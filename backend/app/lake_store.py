"""湖表存储引擎：文件注册表、分区清单、快照链、水位存储与批次数据。

本模块提供跨服务共享的不可变数据层：
- 文件注册：每个分区文件有唯一 file_id、所属分区、所属湖表、行数、字节数
- 快照链：每个湖表按时间线记录快照，含 file_ids、manifest、父快照指针
- 水位：每张湖表的已提交/待提交水位记录
- 批次数据：入湖批次的行级数据存储，含校验值
- 隔离区：被质量规则拦截的数据行，按批次隔离
- 写锁：并发提交冲突检测

设计原则：
- 所有写操作在提交前必须拿到全局锁
- 快照只在事务完整落湖后标记为可用
- 归并时先写入新文件清单，再原子地替换旧文件列表
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


# ----------------------------- 文件注册 -----------------------------

@dataclass
class PartitionFile:
    """分区文件：每个文件属于一个分区。"""
    file_id: str
    lake_table: str
    partition_key: str
    file_rows: int
    file_bytes: int
    file_manifest: str  # 文件内容摘要（MD5）
    created_at: float
    # 旧版文件回收后保留登记但标记 reclaimed；副本文件通过 replica_of 指回原件
    file_status: str = "active"  # active | reclaimed
    replica_of: str | None = None


# ----------------------------- 湖表身份 -----------------------------

@dataclass
class TableIdentity:
    """湖表身份：库域内稳定的表标识，改名后标识不变、旧名保留可解析。"""
    table_uid: str
    database_domain: str
    table_name: str
    previous_names: list[str]
    created_at: float
    renamed_at: float | None = None


# ----------------------------- 文件租约 -----------------------------

@dataclass
class FileLease:
    """文件引用租约：恢复作业领取清单即持有清单内全部文件，完成后释放。"""
    lease_id: str
    lake_table: str
    file_id: str
    holder: str
    holder_kind: str  # recovery_job
    acquired_at: float
    released_at: float | None = None
    status: str = "active"  # active | released


# ----------------------------- 旧版文件回收 -----------------------------

@dataclass
class ReclamationOrder:
    """旧版文件回收单：可等待恢复释放引用，或在读前确认可继续使用的文件副本。"""
    order_id: str
    lake_table: str
    file_id: str
    status: str  # waiting | reclaimable | copy_confirmed | reclaimed
    submitted_at: float
    blocker_lease_id: str | None = None
    blocker_holder: str | None = None
    replica_copy_id: str | None = None
    resolved_at: float | None = None
    note: str = ""


# ----------------------------- 快照 -----------------------------

@dataclass
class LakeSnapshot:
    """湖表快照：记录某个时刻整个湖表的分区文件清单。"""
    snapshot_id: str
    lake_table: str
    parent_snapshot: str | None
    file_ids: list[str]
    added_rows: int
    deleted_rows: int
    manifest_checksum: str
    status: str  # generating | available | expired | rolled_back
    created_at: float


# ----------------------------- 水位 -----------------------------

@dataclass
class WatermarkRecord:
    """入湖水位追踪：记录已提交和待提交水位。"""
    lake_table: str
    field_name: str
    committed_value: str
    pending_value: str | None
    strategy: str  # append | merge | overwrite
    last_advanced_at: float


# ----------------------------- 批次数据 -----------------------------

@dataclass
class BatchRecord:
    """入湖批次：一次入湖操作导出的一组数据行。"""
    batch_id: str
    lake_table: str
    source_system: str
    rows: list[dict[str, Any]]  # 实际数据行
    row_count: int
    window_start: str
    window_end: str
    checksum: str
    snapshot_id: str | None  # 落湖后分配的 snap
    status: str  # pending | ingesting | ingested | failed
    created_at: float


# ----------------------------- 写锁 -----------------------------

class WriteLock:
    """全局写入锁：同一湖表同时只能有一个写入者提交。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def acquire(self, lake_table: str) -> threading.Lock:
        with self._lock:
            if lake_table not in self._locks:
                self._locks[lake_table] = threading.Lock()
            table_lock = self._locks[lake_table]
        table_lock.acquire()
        return table_lock

    def release(self, lake_table: str) -> None:
        with self._lock:
            lock = self._locks.get(lake_table)
        if lock is not None:
            try:
                lock.release()
            except RuntimeError:
                pass


# ----------------------------- 存储引擎 -----------------------------

class LakeStore:
    """湖表存储引擎主类。

    管理三大维度：
    - files: 分区文件注册表
    - snapshots: 快照链
    - watermarks: 水位
    - batches: 批次数据
    - quarantine: 隔离区数据
    - write_lock: 并发提交的控制锁
    """

    def __init__(self) -> None:
        # 文件注册表：lake_table -> {file_id -> PartitionFile}
        self._files: dict[str, dict[str, PartitionFile]] = {}

        # 快照链：lake_table -> list[LakeSnapshot]（时间序）
        self._snapshots: dict[str, list[LakeSnapshot]] = {}

        # 水位：lake_table -> WatermarkRecord
        self._watermarks: dict[str, WatermarkRecord] = {}

        # 批次数据：batch_id -> BatchRecord
        self._batches: dict[str, BatchRecord] = {}

        # 隔离区：batch_id -> list[dict]（被拦截的数据行）
        self._quarantine: dict[str, list[dict[str, Any]]] = {}

        # 写锁
        self._write_lock = WriteLock()

        # 归并中断恢复检查点：merge_id -> {partial_file_ids: [...], ...}
        self._merge_checkpoints: dict[str, dict[str, Any]] = {}

        # 湖表身份注册：(库域, 表名/旧名) -> table_uid；table_uid -> TableIdentity
        self._table_name_index: dict[tuple[str, str], str] = {}
        self._table_identities: dict[str, TableIdentity] = {}

        # 文件引用租约：lease_id -> FileLease；(lake_table, file_id) -> 活动租约
        self._file_leases: dict[str, FileLease] = {}
        self._active_lease_index: dict[tuple[str, str], str] = {}

        # 旧版文件回收单：order_id -> ReclamationOrder
        self._reclamation_orders: dict[str, ReclamationOrder] = {}

        # 恢复证据与回收协调的元数据锁
        self._meta_lock = threading.RLock()

    # ---- 文件操作 ----

    def register_file(
        self, lake_table: str, partition_key: str, rows: int, bytes_: int, manifest: str | None = None,
        replica_of: str | None = None,
    ) -> PartitionFile:
        file_id = f"{lake_table}-p{partition_key}-{uuid.uuid4().hex[:8]}"
        checksum = manifest or hashlib.md5(f"{lake_table}:{partition_key}:{time.time()}".encode()).hexdigest()
        pf = PartitionFile(
            file_id=file_id,
            lake_table=lake_table,
            partition_key=partition_key,
            file_rows=rows,
            file_bytes=bytes_,
            file_manifest=checksum,
            created_at=time.time(),
            replica_of=replica_of,
        )
        self._files.setdefault(lake_table, {})[file_id] = pf
        return pf

    def list_files(
        self, lake_table: str, partition_key: str | None = None, *, include_reclaimed: bool = False
    ) -> list[PartitionFile]:
        files = list(self._files.get(lake_table, {}).values())
        if not include_reclaimed:
            files = [f for f in files if f.file_status != "reclaimed"]
        if partition_key is not None:
            files = [f for f in files if f.partition_key == partition_key]
        return files

    def get_file(self, lake_table: str, file_id: str) -> PartitionFile | None:
        return self._files.get(lake_table, {}).get(file_id)

    def is_file_readable(self, lake_table: str, file_id: str) -> bool:
        """文件仍可读取：原件未回收，或已确认可继续使用的副本存在。"""
        pf = self._files.get(lake_table, {}).get(file_id)
        if pf is None:
            return False
        if pf.file_status != "reclaimed":
            return True
        return self.find_replica(lake_table, file_id) is not None

    def find_replica(self, lake_table: str, file_id: str) -> PartitionFile | None:
        """查找指定原件当前可用的文件副本。"""
        return next(
            (
                f for f in self._files.get(lake_table, {}).values()
                if f.replica_of == file_id and f.file_status != "reclaimed"
            ),
            None,
        )

    def remove_files(self, lake_table: str, file_ids: list[str]) -> int:
        table_files = self._files.get(lake_table, {})
        removed = 0
        for fid in file_ids:
            if fid in table_files:
                del table_files[fid]
                removed += 1
        return removed

    def replace_file_list(
        self, lake_table: str, old_file_ids: list[str], new_files: list[PartitionFile]
    ) -> None:
        """原子替换：先注册新文件，再删除旧文件。保证归并不留半份清单。"""
        table_files = self._files.setdefault(lake_table, {})
        for nf in new_files:
            table_files[nf.file_id] = nf
        for fid in old_file_ids:
            table_files.pop(fid, None)

    # ---- 快照操作 ----

    def create_snapshot(
        self,
        lake_table: str,
        file_ids: list[str],
        parent_snapshot: str | None = None,
        added_rows: int = 0,
        deleted_rows: int = 0,
    ) -> LakeSnapshot:
        snap_id = f"snap-{lake_table}-{uuid.uuid4().hex[:8]}"
        manifest = hashlib.md5(
            ",".join(sorted(file_ids)).encode()
        ).hexdigest()
        snap = LakeSnapshot(
            snapshot_id=snap_id,
            lake_table=lake_table,
            parent_snapshot=parent_snapshot,
            file_ids=list(file_ids),
            added_rows=added_rows,
            deleted_rows=deleted_rows,
            manifest_checksum=manifest,
            status="generating",
            created_at=time.time(),
        )
        chain = self._snapshots.setdefault(lake_table, [])
        chain.append(snap)
        return snap

    def mark_snapshot_available(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "available"
                return snap
        return None

    def mark_snapshot_expired(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "expired"
                return snap
        return None

    def get_snapshot(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                return snap
        return None

    def list_snapshots(self, lake_table: str) -> list[LakeSnapshot]:
        return list(self._snapshots.get(lake_table, []))

    def get_snapshot_at_time(self, lake_table: str, timestamp: float) -> LakeSnapshot | None:
        """时间旅行：返回 <=timestamp 的最近可用快照。"""
        candidates = [
            s for s in self._snapshots.get(lake_table, [])
            if s.status == "available" and s.created_at <= timestamp
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.created_at)

    # ---- 水位操作 ----

    def set_watermark(
        self, lake_table: str, field_name: str, committed: str,
        strategy: str = "append"
    ) -> WatermarkRecord:
        wm = WatermarkRecord(
            lake_table=lake_table,
            field_name=field_name,
            committed_value=committed,
            pending_value=None,
            strategy=strategy,
            last_advanced_at=time.time(),
        )
        self._watermarks[lake_table] = wm
        return wm

    def advance_watermark(self, lake_table: str, new_value: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None:
            return None
        wm.pending_value = new_value
        return wm

    def commit_watermark(self, lake_table: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None or wm.pending_value is None:
            return None
        wm.committed_value = wm.pending_value
        wm.pending_value = None
        wm.last_advanced_at = time.time()
        return wm

    def get_watermark(self, lake_table: str) -> WatermarkRecord | None:
        return self._watermarks.get(lake_table)

    def list_watermarks(self) -> list[WatermarkRecord]:
        return list(self._watermarks.values())

    # ---- 批次数据 ----

    def store_batch(
        self, batch_id: str, lake_table: str, source_system: str,
        rows: list[dict[str, Any]], window_start: str, window_end: str,
    ) -> BatchRecord:
        checksum = hashlib.md5(
            str(sorted([str(r) for r in rows])).encode()
        ).hexdigest()
        br = BatchRecord(
            batch_id=batch_id,
            lake_table=lake_table,
            source_system=source_system,
            rows=list(rows),
            row_count=len(rows),
            window_start=window_start,
            window_end=window_end,
            checksum=checksum,
            snapshot_id=None,
            status="pending",
            created_at=time.time(),
        )
        self._batches[batch_id] = br
        return br

    def get_batch(self, batch_id: str) -> BatchRecord | None:
        return self._batches.get(batch_id)

    def list_batches(self, lake_table: str) -> list[BatchRecord]:
        return [b for b in self._batches.values() if b.lake_table == lake_table]

    def update_batch_status(self, batch_id: str, status: str, snapshot_id: str | None = None) -> BatchRecord | None:
        br = self._batches.get(batch_id)
        if br is None:
            return None
        br.status = status
        if snapshot_id is not None:
            br.snapshot_id = snapshot_id
        return br

    # ---- 隔离区 ----

    def quarantine_rows(
        self, batch_id: str, rule_id: str, rule_name: str,
        rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """将被质量规则拦截的行放入隔离区。"""
        quarantined = []
        for row in rows:
            qr = dict(row)
            qr["_quarantine_batch"] = batch_id
            qr["_quarantine_rule"] = rule_id
            qr["_quarantine_rule_name"] = rule_name
            qr["_quarantine_time"] = time.time()
            quarantined.append(qr)
        self._quarantine.setdefault(batch_id, []).extend(quarantined)
        return quarantined

    def list_quarantine(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id is not None:
            return list(self._quarantine.get(batch_id, []))
        result: list[dict[str, Any]] = []
        for rows in self._quarantine.values():
            result.extend(rows)
        return result

    def release_quarantine(self, batch_id: str) -> list[dict[str, Any]]:
        """回炉重入：从隔离区取出数据行。"""
        rows = self._quarantine.pop(batch_id, [])
        cleaned: list[dict[str, Any]] = []
        for r in rows:
            cleaned_row = {k: v for k, v in r.items() if not k.startswith("_quarantine_")}
            cleaned.append(cleaned_row)
        return cleaned

    # ---- 归并检查点 ----

    def save_merge_checkpoint(self, merge_id: str, partial_file_ids: list[str]) -> None:
        self._merge_checkpoints[merge_id] = {
            "partial_file_ids": list(partial_file_ids),
            "saved_at": time.time(),
        }

    def get_merge_checkpoint(self, merge_id: str) -> dict[str, Any] | None:
        return self._merge_checkpoints.get(merge_id)

    def clear_merge_checkpoint(self, merge_id: str) -> None:
        self._merge_checkpoints.pop(merge_id, None)

    # ---- 并发写锁 ----

    def acquire_write_lock(self, lake_table: str) -> threading.Lock:
        return self._write_lock.acquire(lake_table)

    def release_write_lock(self, lake_table: str) -> None:
        self._write_lock.release(lake_table)

    # ---- 统计快照 ----

    def stats_snapshot(self, lake_table: str) -> dict[str, Any]:
        """生成湖表统计画像快照。"""
        files = self.list_files(lake_table)
        snapshots = self.list_snapshots(lake_table)
        wm = self.get_watermark(lake_table)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)
        partition_count = len(set(f.partition_key for f in files))
        return {
            "lake_table": lake_table,
            "file_count": len(files),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partition_count": partition_count,
            "available_snapshots": sum(1 for s in snapshots if s.status == "available"),
            "watermark": wm.committed_value if wm else None,
        }

    # ---- 湖表身份注册（库域 + 稳定表标识 + 改名） ----

    def register_table_identity(self, database_domain: str, table_name: str) -> TableIdentity:
        """登记湖表身份：同一库域内同名表只登记一次，库域之间互不影响。"""
        with self._meta_lock:
            key = (database_domain, table_name)
            existing_uid = self._table_name_index.get(key)
            if existing_uid is not None:
                return self._table_identities[existing_uid]
            uid = f"tbl-{database_domain}-{table_name}-{uuid.uuid4().hex[:8]}"
            identity = TableIdentity(
                table_uid=uid,
                database_domain=database_domain,
                table_name=table_name,
                previous_names=[],
                created_at=time.time(),
            )
            self._table_identities[uid] = identity
            self._table_name_index[key] = uid
            return identity

    def rename_table(self, database_domain: str, old_name: str, new_name: str) -> TableIdentity:
        """湖表改名：稳定标识不变，旧名保留可解析，新名指向同一身份。"""
        with self._meta_lock:
            uid = self._table_name_index.get((database_domain, old_name))
            if uid is None:
                raise KeyError(f"库域 {database_domain} 下不存在湖表 {old_name}")
            taken = self._table_name_index.get((database_domain, new_name))
            if taken is not None and taken != uid:
                raise ValueError(f"新表名 {new_name} 已被同库域其他湖表占用")
            identity = self._table_identities[uid]
            if new_name == identity.table_name:
                return identity
            if old_name not in identity.previous_names:
                identity.previous_names.append(old_name)
            identity.table_name = new_name
            identity.renamed_at = time.time()
            self._table_name_index[(database_domain, new_name)] = uid
            return identity

    def resolve_table(self, database_domain: str, table_name: str) -> TableIdentity | None:
        """按库域和表名（含旧名）解析稳定湖表身份。"""
        uid = self._table_name_index.get((database_domain, table_name))
        if uid is None:
            return None
        return self._table_identities.get(uid)

    def list_table_identities(self) -> list[TableIdentity]:
        return list(self._table_identities.values())

    def clone_table_storage(self, old_key: str, new_key: str) -> dict[str, Any]:
        """改名时把旧物理键下的文件与快照链克隆到新键。

        旧物理键原样保留，已受理的恢复申请继续读旧文件；新键承接后续
        快照确认与写入，快照沿用原 snapshot_id，保证父链可追溯。
        """
        with self._meta_lock:
            old_files = self._files.get(old_key, {})
            if old_key == new_key:
                return {"files": len(old_files), "snapshots": len(self._snapshots.get(old_key, []))}
            new_files = self._files.setdefault(new_key, {})
            for fid, pf in old_files.items():
                new_files[fid] = PartitionFile(
                    file_id=pf.file_id,
                    lake_table=new_key,
                    partition_key=pf.partition_key,
                    file_rows=pf.file_rows,
                    file_bytes=pf.file_bytes,
                    file_manifest=pf.file_manifest,
                    created_at=pf.created_at,
                    file_status=pf.file_status,
                    replica_of=pf.replica_of,
                )
            cloned_snaps: list[LakeSnapshot] = []
            for snap in self._snapshots.get(old_key, []):
                cloned = LakeSnapshot(
                    snapshot_id=snap.snapshot_id,
                    lake_table=new_key,
                    parent_snapshot=snap.parent_snapshot,
                    file_ids=list(snap.file_ids),
                    added_rows=snap.added_rows,
                    deleted_rows=snap.deleted_rows,
                    manifest_checksum=snap.manifest_checksum,
                    status=snap.status,
                    created_at=snap.created_at,
                )
                cloned_snaps.append(cloned)
            self._snapshots[new_key] = cloned_snaps
            return {"files": len(new_files), "snapshots": len(cloned_snaps)}

    # ---- 文件引用租约 ----

    def acquire_file_lease(
        self, lake_table: str, file_id: str, holder: str, holder_kind: str = "recovery_job"
    ) -> FileLease:
        """领取文件引用：同一文件同时只允许一个持有者，重复领取直接报错。"""
        with self._meta_lock:
            index_key = (lake_table, file_id)
            current_id = self._active_lease_index.get(index_key)
            if current_id is not None:
                current = self._file_leases[current_id]
                if current.holder == holder:
                    return current
                raise RuntimeError(
                    f"文件 {file_id} 已由 {current.holder_kind}:{current.holder} 持有"
                )
            lease = FileLease(
                lease_id=f"lease-{uuid.uuid4().hex[:10]}",
                lake_table=lake_table,
                file_id=file_id,
                holder=holder,
                holder_kind=holder_kind,
                acquired_at=time.time(),
            )
            self._file_leases[lease.lease_id] = lease
            self._active_lease_index[index_key] = lease.lease_id
            return lease

    def release_file_lease(self, lake_table: str, file_id: str) -> FileLease | None:
        """释放单文件引用；若有回收单在等待该文件，回收单转为可回收。"""
        with self._meta_lock:
            lease_id = self._active_lease_index.pop((lake_table, file_id), None)
            lease = self._file_leases.get(lease_id) if lease_id else None
            if lease is not None:
                lease.status = "released"
                lease.released_at = time.time()
                for order in self._reclamation_orders.values():
                    if (
                        order.lake_table == lake_table
                        and order.file_id == file_id
                        and order.status == "waiting"
                    ):
                        order.status = "reclaimable"
                        order.resolved_at = time.time()
                        order.note = "恢复作业已释放引用，可执行回收"
            return lease

    def release_holder_leases(self, holder: str) -> list[FileLease]:
        """释放某个持有者（如恢复作业）名下全部文件引用。"""
        with self._meta_lock:
            holder_leases = [
                lease for lease in self._file_leases.values()
                if lease.holder == holder and lease.status == "active"
            ]
            released: list[FileLease] = []
            for lease in holder_leases:
                done = self.release_file_lease(lease.lake_table, lease.file_id)
                if done is not None:
                    released.append(done)
            return released

    def get_active_lease(self, lake_table: str, file_id: str) -> FileLease | None:
        lease_id = self._active_lease_index.get((lake_table, file_id))
        return self._file_leases.get(lease_id) if lease_id else None

    def list_holder_files(self, holder: str) -> list[FileLease]:
        return [
            lease for lease in self._file_leases.values()
            if lease.holder == holder and lease.status == "active"
        ]

    # ---- 旧版文件回收 ----

    def submit_reclamation(self, lake_table: str, file_id: str) -> ReclamationOrder:
        """提交旧版文件回收：文件被持有时回收单进入等待，否则直接可回收。"""
        with self._meta_lock:
            lease = self.get_active_lease(lake_table, file_id)
            order = ReclamationOrder(
                order_id=f"recl-{uuid.uuid4().hex[:10]}",
                lake_table=lake_table,
                file_id=file_id,
                status="waiting" if lease is not None else "reclaimable",
                submitted_at=time.time(),
                blocker_lease_id=lease.lease_id if lease else None,
                blocker_holder=lease.holder if lease else None,
                note="等待恢复作业释放引用" if lease else "无引用占用，可直接回收",
            )
            self._reclamation_orders[order.order_id] = order
            return order

    def confirm_replica_copy(self, order_id: str) -> ReclamationOrder:
        """读前确认可继续使用的文件副本：副本登记后回收单转为副本确认，读取不再阻塞。"""
        with self._meta_lock:
            order = self._reclamation_orders.get(order_id)
            if order is None:
                raise KeyError(f"回收单 {order_id} 不存在")
            if order.status in ("reclaimed", "copy_confirmed"):
                return order
            pf = self.get_file(order.lake_table, order.file_id)
            if pf is None:
                raise RuntimeError("待回收文件已不在注册表中")
            replica = self.register_file(
                order.lake_table,
                pf.partition_key,
                rows=pf.file_rows,
                bytes_=pf.file_bytes,
                manifest=pf.file_manifest,
                replica_of=pf.file_id,
            )
            order.status = "copy_confirmed"
            order.replica_copy_id = replica.file_id
            order.resolved_at = time.time()
            order.note = "已确认可继续使用的文件副本，读取走副本"
            return order

    def execute_reclamation(self, order_id: str) -> ReclamationOrder:
        """执行回收：等待中的回收单必须先释放引用或确认副本。"""
        with self._meta_lock:
            order = self._reclamation_orders.get(order_id)
            if order is None:
                raise KeyError(f"回收单 {order_id} 不存在")
            if order.status == "reclaimed":
                return order
            if order.status == "waiting":
                raise RuntimeError("文件引用仍被持有，需等待释放或先确认文件副本")
            pf = self._files.get(order.lake_table, {}).get(order.file_id)
            if pf is not None:
                pf.file_status = "reclaimed"
            order.status = "reclaimed"
            order.resolved_at = time.time()
            if not order.note:
                order.note = "旧版文件已回收"
            return order

    def get_reclamation_order(self, order_id: str) -> ReclamationOrder | None:
        return self._reclamation_orders.get(order_id)

    def list_reclamation_orders(self, lake_table: str | None = None) -> list[ReclamationOrder]:
        orders = list(self._reclamation_orders.values())
        if lake_table is not None:
            orders = [o for o in orders if o.lake_table == lake_table]
        return orders

    def list_file_reclamation_orders(self, lake_table: str, file_id: str) -> list[ReclamationOrder]:
        return [
            order for order in self._reclamation_orders.values()
            if order.lake_table == lake_table and order.file_id == file_id
        ]

    def get_file_holder(self, lake_table: str, file_id: str) -> dict[str, Any] | None:
        """值班查询：旧版具体文件当前由哪一方持有。"""
        lease = self.get_active_lease(lake_table, file_id)
        if lease is None:
            return None
        return {
            "lake_table": lake_table,
            "file_id": file_id,
            "holder": lease.holder,
            "holder_kind": lease.holder_kind,
            "lease_id": lease.lease_id,
            "acquired_at": lease.acquired_at,
        }


# 全局单例
lake_store = LakeStore()