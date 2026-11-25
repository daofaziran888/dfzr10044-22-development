"""恢复证据领域验收测试：双库域同名 sales 表、改名冲突、断点续传与旧版文件回收。

场景对应需求：
- 两个库域各登记名为 sales 的湖表，分别含 7 行和 11 行；
- 领取第一个库域的恢复清单后发生湖表改名与新快照确认冲突；
- 中断后重试旧申请可接着恢复，已有结果不重复写入；
- 恢复完成可查回原申请所属湖表、分区和快照，文件同属一个已确认范围；
- 第二个库域的 11 行不能被读入；
- 恢复概览列出改名后的条目；
- 恢复只收到第一份文件即暂停，同时提交旧版文件回收；
- 回收可等待恢复释放引用，也可在读前确认文件副本；
- 值班员可查询旧版文件持有者，以及失败后哪一部分可以补取。
"""
from __future__ import annotations

import pytest

from app.lake_store import LakeStore
from app.recovery.manager import RecoveryManager

DOMAIN_A = "lake_ods"
DOMAIN_B = "lake_dwd"
TABLE = "sales"
RENAMED = "sales_renamed"


def _key(domain: str, table: str) -> str:
    return RecoveryManager.physical_key(domain, table)


@pytest.fixture
def context() -> dict:
    store = LakeStore()
    manager = RecoveryManager(store)

    key_a = _key(DOMAIN_A, TABLE)
    # 库域 A：sales 含 7 行，分布在两个分区的 7 个文件
    store.register_table_identity(DOMAIN_A, TABLE)
    file_ids_a: list[str] = []
    for i in range(4):
        pf = store.register_file(key_a, "dt=20261101", rows=1, bytes_=1280)
        file_ids_a.append(pf.file_id)
    for i in range(3):
        pf = store.register_file(key_a, "dt=20261102", rows=1, bytes_=1280)
        file_ids_a.append(pf.file_id)
    snap_a = store.create_snapshot(key_a, file_ids_a, added_rows=7)
    store.mark_snapshot_available(key_a, snap_a.snapshot_id)

    key_b = _key(DOMAIN_B, TABLE)
    # 库域 B：同名 sales 含 11 行，分布在三个分区的 11 个文件
    store.register_table_identity(DOMAIN_B, TABLE)
    file_ids_b: list[str] = []
    for i in range(5):
        pf = store.register_file(key_b, "dt=20261101", rows=1, bytes_=1280)
        file_ids_b.append(pf.file_id)
    for i in range(4):
        pf = store.register_file(key_b, "dt=20261102", rows=1, bytes_=1280)
        file_ids_b.append(pf.file_id)
    for i in range(2):
        pf = store.register_file(key_b, "dt=20261103", rows=1, bytes_=1280)
        file_ids_b.append(pf.file_id)
    snap_b = store.create_snapshot(key_b, file_ids_b, added_rows=11)
    store.mark_snapshot_available(key_b, snap_b.snapshot_id)

    return {
        "store": store,
        "manager": manager,
        "file_ids_a": file_ids_a,
        "file_ids_b": file_ids_b,
        "snap_a": snap_a.snapshot_id,
        "snap_b": snap_b.snapshot_id,
        "key_a": key_a,
        "key_b": key_b,
    }


class TestRecoveryEvidenceScenario:
    def test_manifest_freezes_confirmed_scope_by_domain(self, context):
        store, manager = context["store"], context["manager"]

        manifest = manager.build_manifest(DOMAIN_A, TABLE)
        assert manifest.database_domain == DOMAIN_A
        assert manifest.table_name == TABLE
        assert manifest.file_table_key == context["key_a"]
        assert manifest.snapshot_id == context["snap_a"]
        assert manifest.snapshot_status == "available"
        assert len(manifest.file_ids) == 7
        assert manifest.total_rows == 7
        assert manifest.partition_keys == ["dt=20261101", "dt=20261102"]
        # 同库域同名表的身份稳定
        again = store.register_table_identity(DOMAIN_A, TABLE)
        assert again.table_uid == manifest.table_uid

    def test_claim_then_rename_and_new_snapshot_conflict_keeps_old_scope(self, context):
        store, manager = context["store"], context["manager"]
        key_a = context["key_a"]
        manifest = manager.build_manifest(DOMAIN_A, TABLE)
        job = manager.claim_manifest(manifest.manifest_id, job_id="recjob-A-1")
        assert job.file_ids == context["file_ids_a"]
        assert job.snapshot_id == context["snap_a"]
        # 领取即持有清单内全部文件
        for fid in manifest.file_ids:
            holder = store.get_file_holder(key_a, fid)
            assert holder is not None
            assert holder["holder"] == "recjob-A-1"

        # 冲突：湖表改名 + 一份新快照确认
        identity = store.rename_table(DOMAIN_A, TABLE, RENAMED)
        cloned = store.clone_table_storage(key_a, _key(DOMAIN_A, RENAMED))
        assert cloned["files"] == 7
        assert identity.table_name == RENAMED
        assert identity.previous_names == [TABLE]
        # 旧名仍能解析回同一稳定标识
        resolved = store.resolve_table(DOMAIN_A, TABLE)
        assert resolved is not None
        assert resolved.table_uid == identity.table_uid

        new_key = _key(DOMAIN_A, RENAMED)
        new_pf = store.register_file(new_key, "dt=20261103", rows=99, bytes_=20480)
        new_snap = store.create_snapshot(
            new_key, [new_pf.file_id], parent_snapshot=context["snap_a"], added_rows=99
        )
        store.mark_snapshot_available(new_key, new_snap.snapshot_id)

        # 旧申请仍冻结在改名前的 7 文件/旧快照上
        old_job = manager.get_job(job.job_id)
        assert old_job.snapshot_id == context["snap_a"]
        assert old_job.file_table_key == key_a
        assert new_pf.file_id not in old_job.file_ids

    def test_pause_resume_is_idempotent_and_skips_read_files(self, context):
        store, manager = context["store"], context["manager"]
        key_a = context["key_a"]
        manifest = manager.build_manifest(DOMAIN_A, TABLE)
        job = manager.claim_manifest(manifest.manifest_id, job_id="recjob-A-2")

        first = manager.read_next_file(job.job_id)
        assert first["success"] and not first["already_read"]
        first_file = first["file_id"]

        # 只收到第一份文件即暂停
        paused = manager.pause_job(job.job_id)
        assert paused.status == "paused"
        # 暂停期间租约不释放
        assert store.get_file_holder(key_a, first_file)["holder"] == job.job_id
        with pytest.raises(RuntimeError):
            manager.read_next_file(job.job_id)

        # 中断后重试旧申请：接着恢复，断点前的文件不再读
        manager.resume_job(job.job_id)
        second = manager.read_next_file(job.job_id)
        assert second["success"]
        assert second["file_id"] != first_file
        assert first_file in manager.get_job(job.job_id).read_checkpoint
        assert len(manager.get_job(job.job_id).read_checkpoint) == 2

    def test_completion_returns_original_scope_and_passes_snapshot_check(self, context):
        store, manager = context["store"], context["manager"]
        key_a = context["key_a"]
        manifest = manager.build_manifest(DOMAIN_A, TABLE)
        job = manager.claim_manifest(manifest.manifest_id, job_id="recjob-A-3")

        # 改名与新快照确认发生在恢复过程中
        store.rename_table(DOMAIN_A, TABLE, RENAMED)
        new_key = _key(DOMAIN_A, RENAMED)
        store.clone_table_storage(key_a, new_key)
        new_pf = store.register_file(new_key, "dt=20261103", rows=99, bytes_=20480)
        new_snap = store.create_snapshot(new_key, [new_pf.file_id], added_rows=99)
        store.mark_snapshot_available(new_key, new_snap.snapshot_id)

        for _ in range(len(job.file_ids)):
            result = manager.read_next_file(job.job_id)
            assert result["success"]
        tail = manager.read_next_file(job.job_id)
        assert tail["already_read"] and tail["file_id"] is None

        scope = manager.validate_snapshot_scope(job.job_id)
        assert scope["scope_ok"]
        assert scope["snapshot_id"] == context["snap_a"]

        summary = manager.complete_job(job.job_id)
        assert summary["status"] == "completed"
        # 可查回原申请所属湖表（稳定标识）、分区与快照
        assert summary["table_uid"] == manifest.table_uid
        assert summary["manifest_table_name"] == TABLE
        assert summary["current_table_name"] == RENAMED
        assert summary["partition_keys"] == ["dt=20261101", "dt=20261102"]
        assert summary["snapshot_id"] == context["snap_a"]
        assert len(summary["files_read"]) == 7
        assert summary["rows_read"] == 7
        # 所有读取文件都在同一个已确认快照范围内
        assert set(summary["files_read"]) <= set(
            store.get_snapshot(key_a, context["snap_a"]).file_ids
        )
        # 完成后引用已释放
        assert store.get_file_holder(key_a, summary["files_read"][0]) is None

    def test_second_domain_eleven_rows_never_read(self, context):
        store, manager = context["store"], context["manager"]
        manifest_a = manager.build_manifest(DOMAIN_A, TABLE)
        job_a = manager.claim_manifest(manifest_a.manifest_id, job_id="recjob-A-4")
        for _ in range(len(job_a.file_ids)):
            manager.read_next_file(job_a.job_id)
        manager.complete_job(job_a.job_id)

        # 库域 B 的 11 行是独立身份、独立清单、独立物理存储
        identity_b = store.resolve_table(DOMAIN_B, TABLE)
        identity_a = store.resolve_table(DOMAIN_A, TABLE)
        assert identity_b.table_uid != identity_a.table_uid
        manifest_b = manager.build_manifest(DOMAIN_B, TABLE)
        assert len(manifest_b.file_ids) == 11
        assert set(manifest_b.file_ids) == set(context["file_ids_b"])
        assert manifest_b.file_table_key == context["key_b"]
        # A 作业读过的文件里没有任何 B 的文件
        assert not set(job_a.read_checkpoint) & set(context["file_ids_b"])
        # 两个库域的物理文件命名空间互不覆盖
        assert len(store.list_files(context["key_a"])) == 7
        assert len(store.list_files(context["key_b"])) == 11

    def test_recovery_overview_lists_renamed_entry(self, context):
        store, manager = context["store"], context["manager"]
        manager.build_manifest(DOMAIN_A, TABLE)
        manager.build_manifest(DOMAIN_B, TABLE)
        store.rename_table(DOMAIN_A, TABLE, RENAMED)

        overview = manager.recovery_overview()
        entry_a = next(e for e in overview["entries"] if e["database_domain"] == DOMAIN_A)
        assert entry_a["table_name"] == RENAMED
        assert entry_a["previous_names"] == [TABLE]
        assert entry_a["renamed"] is True
        assert entry_a["manifest_count"] == 1
        entry_b = next(e for e in overview["entries"] if e["database_domain"] == DOMAIN_B)
        assert entry_b["table_name"] == TABLE and entry_b["renamed"] is False
        assert overview["manifest_count"] == 2

    def test_reclamation_waits_for_release_then_reclaimable(self, context):
        store, manager = context["store"], context["manager"]
        key_a = context["key_a"]
        manifest = manager.build_manifest(DOMAIN_A, TABLE)
        job = manager.claim_manifest(manifest.manifest_id, job_id="recjob-A-5")
        first = manager.read_next_file(job.job_id)
        manager.pause_job(job.job_id)

        # 暂停同时提交旧版文件回收：文件仍被持有，回收进入等待
        order = store.submit_reclamation(key_a, first["file_id"])
        assert order.status == "waiting"
        assert order.blocker_holder == job.job_id
        with pytest.raises(RuntimeError):
            store.execute_reclamation(order.order_id)

        # 恢复完成释放引用后，等待中的回收单变为可回收并可执行
        manager.resume_job(job.job_id)
        for _ in range(len(job.file_ids) - 1):
            manager.read_next_file(job.job_id)
        manager.complete_job(job.job_id)
        refreshed = store.get_reclamation_order(order.order_id)
        assert refreshed.status == "reclaimable"
        done = store.execute_reclamation(order.order_id)
        assert done.status == "reclaimed"
        # 默认文件列表不再暴露已回收文件
        assert first["file_id"] not in [f.file_id for f in store.list_files(key_a)]

    def test_reclamation_via_confirmed_replica_copy(self, context):
        store, manager = context["store"], context["manager"]
        key_a = context["key_a"]
        manifest = manager.build_manifest(DOMAIN_A, TABLE)
        job = manager.claim_manifest(manifest.manifest_id, job_id="recjob-A-6")
        target = manifest.file_ids[1]

        order = store.submit_reclamation(key_a, target)
        assert order.status == "waiting"
        # 读前确认可继续使用的文件副本，无需等待恢复释放
        confirmed = store.confirm_replica_copy(order.order_id)
        assert confirmed.status == "copy_confirmed"
        replica_id = confirmed.replica_copy_id
        assert store.get_file(key_a, replica_id).replica_of == target

        executed = store.execute_reclamation(order.order_id)
        assert executed.status == "reclaimed"
        # 原件已回收但副本可读，恢复读取走副本
        assert store.is_file_readable(key_a, target)
        manager.read_next_file(job.job_id)
        second = manager.read_next_file(job.job_id)
        assert second["file_id"] == target
        assert second["read_source"]["mode"] == "replica"
        assert second["read_source"]["replica_of"] == target

    def test_operator_queries_holder_and_unread_backfill_part(self, context):
        store, manager = context["store"], context["manager"]
        key_a = context["key_a"]
        manifest = manager.build_manifest(DOMAIN_A, TABLE)
        job = manager.claim_manifest(manifest.manifest_id, job_id="recjob-A-7")
        first = manager.read_next_file(job.job_id)

        # 值班员查询旧版具体文件当前由哪一方持有
        holder = store.get_file_holder(key_a, first["file_id"])
        assert holder["lake_table"] == key_a
        assert holder["file_id"] == first["file_id"]
        assert holder["holder"] == "recjob-A-7"
        assert holder["holder_kind"] == "recovery_job"
        free_file = manifest.file_ids[-1]
        assert store.get_file_holder(key_a, free_file)["holder"] == "recjob-A-7"

        # 恢复失败后查询哪一部分可以补取
        manager.fail_job(job.job_id, "模拟读取中断")
        unread = manager.get_unread_files(job.job_id)
        assert first["file_id"] not in unread
        assert set(unread) == set(manifest.file_ids[1:])
        # 续读只补取未读部分，已读结果不会再写一遍
        manager.resume_job(job.job_id)
        next_read = manager.read_next_file(job.job_id)
        assert next_read["file_id"] == unread[0]
        assert len(manager.get_job(job.job_id).read_checkpoint) == 2

    def test_manifest_requires_available_snapshot(self, context):
        store, manager = context["store"], context["manager"]
        key_b = context["key_b"]
        pending_pf = store.register_file(key_b, "dt=20261199", rows=1, bytes_=10)
        pending_snap = store.create_snapshot(key_b, [pending_pf.file_id])
        # generating 状态的快照不能受理
        with pytest.raises(RuntimeError):
            manager.build_manifest(DOMAIN_B, TABLE, pending_snap.snapshot_id)
        # 显式指定不存在快照时报缺失
        with pytest.raises(KeyError):
            manager.build_manifest(DOMAIN_A, TABLE, "snap-not-exist")

    def test_claim_is_single_holder_and_double_claim_rejected(self, context):
        manager = context["manager"]
        manifest = manager.build_manifest(DOMAIN_A, TABLE)
        manager.claim_manifest(manifest.manifest_id, job_id="recjob-only")
        with pytest.raises(RuntimeError):
            manager.claim_manifest(manifest.manifest_id, job_id="recjob-other")
