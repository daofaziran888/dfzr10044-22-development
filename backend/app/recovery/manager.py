"""恢复证据管理器：清单受理、作业领取、断点续读、快照范围校验与恢复概览。

核心保证：
- 清单按「库域 + 湖表标识」受理，受理时冻结快照与文件清单；
- 湖表改名与新快照确认不影响已受理清单，重试旧申请沿用同一身份与范围；
- 恢复读取的每个文件都必须属于同一个已确认（available）快照；
- 已读文件写入断点，中断重试不会再写一遍；
- 库域隔离：只读取本库域本湖表的文件，另一个库域的同名表不会被读入。
"""
from __future__ import annotations

import threading
import time
import uuid
from typing import Any

from app.lake_store import LakeStore, lake_store
from app.recovery.models import RecoveryJob, RecoveryManifest


class RecoveryManager:
    def __init__(self, store: LakeStore) -> None:
        self.store = store
        self._manifests: dict[str, RecoveryManifest] = {}
        self._jobs: dict[str, RecoveryJob] = {}
        self._lock = threading.RLock()

    @staticmethod
    def physical_key(database_domain: str, table_name: str) -> str:
        """存储引擎物理键：库域前缀保证跨库域同名表互不覆盖。"""
        return f"{database_domain}.{table_name}"

    # ---- 恢复证据清单 ----

    def build_manifest(
        self, database_domain: str, table_name: str, snapshot_id: str | None = None
    ) -> RecoveryManifest:
        """受理恢复证据清单：解析湖表身份，冻结一个已确认快照及其文件范围。"""
        with self._lock:
            identity = self.store.resolve_table(database_domain, table_name)
            if identity is None:
                raise KeyError(f"库域 {database_domain} 下不存在湖表 {table_name}")

            physical = self.physical_key(database_domain, table_name)
            snapshots = self.store.list_snapshots(physical)
            if snapshot_id is not None:
                snap = self.store.get_snapshot(physical, snapshot_id)
                if snap is None:
                    raise KeyError(f"快照 {snapshot_id} 不属于湖表 {database_domain}.{table_name}")
            else:
                available = [s for s in snapshots if s.status == "available"]
                if not available:
                    raise RuntimeError(f"湖表 {database_domain}.{table_name} 没有已确认的快照可受理")
                snap = available[-1]
            if snap.status != "available":
                raise RuntimeError(f"快照 {snap.snapshot_id} 尚未确认，不能作为恢复范围")

            frozen_files = [
                fid for fid in snap.file_ids
                if self.store.get_file(physical, fid) is not None
            ]
            partitions = sorted({
                self.store.get_file(physical, fid).partition_key
                for fid in frozen_files
            })
            total_rows = sum(
                self.store.get_file(physical, fid).file_rows for fid in frozen_files
            )
            manifest = RecoveryManifest(
                manifest_id=f"mani-{uuid.uuid4().hex[:10]}",
                database_domain=database_domain,
                table_uid=identity.table_uid,
                table_name=table_name,
                file_table_key=physical,
                snapshot_id=snap.snapshot_id,
                snapshot_status=snap.status,
                file_ids=list(frozen_files),
                partition_keys=partitions,
                total_rows=total_rows,
                status="open",
                created_at=time.time(),
            )
            self._manifests[manifest.manifest_id] = manifest
            return manifest

    def list_manifests(self, database_domain: str | None = None) -> list[RecoveryManifest]:
        items = list(self._manifests.values())
        if database_domain is not None:
            items = [m for m in items if m.database_domain == database_domain]
        return items

    def get_manifest(self, manifest_id: str) -> RecoveryManifest | None:
        return self._manifests.get(manifest_id)

    def claim_manifest(self, manifest_id: str, job_id: str | None = None) -> RecoveryJob:
        """领取恢复清单：清单内全部文件一次性建立引用租约，返回可执行的恢复作业。"""
        with self._lock:
            manifest = self._manifests.get(manifest_id)
            if manifest is None:
                raise KeyError(f"恢复清单 {manifest_id} 不存在")
            if manifest.status == "claimed":
                raise RuntimeError(f"恢复清单 {manifest_id} 已被 {manifest.claimed_by} 领取")

            resolved_job_id = job_id or f"recjob-{uuid.uuid4().hex[:10]}"
            try:
                for fid in manifest.file_ids:
                    self.store.acquire_file_lease(
                        manifest.file_table_key, fid, resolved_job_id
                    )
            except Exception:
                # 任一文件已被他人持有则整单回滚，不留下半份租约
                self.store.release_holder_leases(resolved_job_id)
                raise

            now = time.time()
            job = RecoveryJob(
                job_id=resolved_job_id,
                manifest_id=manifest.manifest_id,
                database_domain=manifest.database_domain,
                table_uid=manifest.table_uid,
                file_table_key=manifest.file_table_key,
                snapshot_id=manifest.snapshot_id,
                file_ids=list(manifest.file_ids),
                created_at=now,
                updated_at=now,
            )
            self._jobs[job.job_id] = job
            manifest.status = "claimed"
            manifest.claimed_by = job.job_id
            return job

    # ---- 恢复作业执行与断点续传 ----

    def get_job(self, job_id: str) -> RecoveryJob | None:
        return self._jobs.get(job_id)

    def list_jobs(self, database_domain: str | None = None) -> list[RecoveryJob]:
        items = list(self._jobs.values())
        if database_domain is not None:
            items = [j for j in items if j.database_domain == database_domain]
        return items

    def read_next_file(self, job_id: str) -> dict[str, Any]:
        """读取清单中下一个未读文件；已读文件靠断点跳过，不会被再写一遍。"""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(f"恢复作业 {job_id} 不存在")
            if job.status == "paused":
                raise RuntimeError("恢复作业已暂停，请先恢复执行")
            if job.status == "completed":
                return {
                    "job_id": job_id,
                    "file_id": None,
                    "already_read": True,
                    "message": "清单全部文件已读取，无新增输入",
                }
            if job.status == "failed":
                raise RuntimeError(f"恢复作业已失败：{job.failure_reason}，请重试续读")

            next_file = next(
                (fid for fid in job.file_ids if fid not in job.read_checkpoint), None
            )
            if next_file is None:
                job.status = "completed"
                job.updated_at = time.time()
                return {
                    "job_id": job_id,
                    "file_id": None,
                    "already_read": True,
                    "message": "清单全部文件已读取，无新增输入",
                }

            if not self.store.is_file_readable(job.file_table_key, next_file):
                job.status = "failed"
                job.failure_reason = f"文件 {next_file} 已回收且无可用副本，无法读取"
                job.updated_at = time.time()
                return {
                    "job_id": job_id,
                    "file_id": next_file,
                    "success": False,
                    "failure_reason": job.failure_reason,
                    "resumable": True,
                }

            pf = self.store.get_file(job.file_table_key, next_file)
            job.read_checkpoint.append(next_file)
            job.rows_read += pf.file_rows
            job.updated_at = time.time()
            return {
                "job_id": job_id,
                "file_id": next_file,
                "partition_key": pf.partition_key,
                "rows": pf.file_rows,
                "read_source": self._read_source(job.file_table_key, next_file),
                "success": True,
                "already_read": False,
                "read_count": len(job.read_checkpoint),
                "remaining_count": len(job.file_ids) - len(job.read_checkpoint),
            }

    def _read_source(self, lake_table: str, file_id: str) -> dict[str, str]:
        """说明本次读取实际走原件还是已确认副本。"""
        pf = self.store.get_file(lake_table, file_id)
        if pf is not None and pf.file_status != "reclaimed":
            return {"mode": "original", "file_id": file_id}
        # 副本是独立 active 文件，直接在全量注册表里找（不能用默认隐藏已回收件的列表）
        replica = self.store.find_replica(lake_table, file_id)
        return {
            "mode": "replica",
            "file_id": replica.file_id if replica else file_id,
            "replica_of": file_id,
        }

    def pause_job(self, job_id: str) -> RecoveryJob:
        """暂停作业：租约继续持有，中断后可原样恢复。"""
        with self._lock:
            job = self._require_active_job(job_id)
            job.status = "paused"
            job.updated_at = time.time()
            return job

    def resume_job(self, job_id: str) -> RecoveryJob:
        """恢复执行：暂停或失败的旧申请都可接着恢复，断点之后的文件才会再读。"""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(f"恢复作业 {job_id} 不存在")
            if job.status == "completed":
                return job
            job.status = "running"
            job.failure_reason = ""
            job.updated_at = time.time()
            return job

    def fail_job(self, job_id: str, reason: str) -> RecoveryJob:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(f"恢复作业 {job_id} 不存在")
            job.status = "failed"
            job.failure_reason = reason
            job.updated_at = time.time()
            return job

    def complete_job(self, job_id: str) -> dict[str, Any]:
        """完成恢复：校验全部文件已读且同属一个已确认快照，随后释放文件引用。"""
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                raise KeyError(f"恢复作业 {job_id} 不存在")
            if job.status != "completed":
                remaining = [fid for fid in job.file_ids if fid not in job.read_checkpoint]
                if remaining:
                    raise RuntimeError(f"仍有 {len(remaining)} 个文件未读取，不能完成恢复")
                scope = self.validate_snapshot_scope(job_id)
                if not scope["scope_ok"]:
                    raise RuntimeError(f"快照范围校验失败：{scope['reason']}")
                job.status = "completed"
                job.updated_at = time.time()

            manifest = self._manifests.get(job.manifest_id)
            # 清单冻结的逻辑表名即使改名后仍可解析回同一稳定身份
            identity = (
                self.store.resolve_table(job.database_domain, manifest.table_name)
                if manifest else None
            )
            # 完成后释放引用，等待中的旧版文件回收单随之可执行
            self.store.release_holder_leases(job.job_id)
            return {
                "job_id": job.job_id,
                "status": job.status,
                "database_domain": job.database_domain,
                "table_uid": job.table_uid,
                "current_table_name": identity.table_name if identity else None,
                "manifest_table_name": manifest.table_name if manifest else None,
                "partition_keys": manifest.partition_keys if manifest else [],
                "snapshot_id": job.snapshot_id,
                "files_read": list(job.read_checkpoint),
                "rows_read": job.rows_read,
            }

    def get_unread_files(self, job_id: str) -> list[str]:
        """恢复失败后值班查询：哪一部分文件还可以补取。"""
        job = self._jobs.get(job_id)
        if job is None:
            raise KeyError(f"恢复作业 {job_id} 不存在")
        return [fid for fid in job.file_ids if fid not in job.read_checkpoint]

    def validate_snapshot_scope(self, job_id: str) -> dict[str, Any]:
        """校验已读文件全部属于作业冻结的同一个已确认快照。"""
        job = self._jobs.get(job_id)
        if job is None:
            raise KeyError(f"恢复作业 {job_id} 不存在")
        snap = self.store.get_snapshot(job.file_table_key, job.snapshot_id)
        if snap is None:
            return {"scope_ok": False, "reason": "冻结快照不存在"}
        if snap.status != "available":
            return {"scope_ok": False, "reason": f"快照 {snap.snapshot_id} 状态为 {snap.status}，未确认"}
        snapshot_files = set(snap.file_ids)
        read_files = set(job.read_checkpoint)
        outside = sorted(read_files - snapshot_files)
        if outside:
            return {"scope_ok": False, "reason": f"文件不属于冻结快照：{outside}"}
        return {
            "scope_ok": True,
            "snapshot_id": snap.snapshot_id,
            "snapshot_status": snap.status,
            "files_in_snapshot": len(snapshot_files),
            "files_read": len(read_files),
        }

    def recovery_overview(self) -> dict[str, Any]:
        """恢复概览：列出各库域湖表的当前条目（含改名后的表名），供下一次恢复使用。"""
        entries: list[dict[str, Any]] = []
        for identity in self.store.list_table_identities():
            physical = self.physical_key(identity.database_domain, identity.table_name)
            available = [
                s for s in self.store.list_snapshots(physical)
                if s.status == "available"
            ]
            manifests = [
                m for m in self._manifests.values() if m.table_uid == identity.table_uid
            ]
            entries.append({
                "database_domain": identity.database_domain,
                "table_uid": identity.table_uid,
                "table_name": identity.table_name,
                "previous_names": list(identity.previous_names),
                "renamed": bool(identity.previous_names),
                "available_snapshots": len(available),
                "file_count": len(self.store.list_files(physical)),
                "manifest_count": len(manifests),
                "open_manifests": sum(1 for m in manifests if m.status == "open"),
            })
        jobs_by_status: dict[str, int] = {}
        for job in self._jobs.values():
            jobs_by_status[job.status] = jobs_by_status.get(job.status, 0) + 1
        return {
            "table_count": len(entries),
            "manifest_count": len(self._manifests),
            "job_count": len(self._jobs),
            "jobs_by_status": jobs_by_status,
            "entries": entries,
        }

    def _require_active_job(self, job_id: str) -> RecoveryJob:
        job = self._jobs.get(job_id)
        if job is None:
            raise KeyError(f"恢复作业 {job_id} 不存在")
        if job.status == "completed":
            raise RuntimeError("恢复作业已完成，不能再变更状态")
        return job


# 全局单例：与 lake_store 配套
recovery_manager = RecoveryManager(lake_store)
