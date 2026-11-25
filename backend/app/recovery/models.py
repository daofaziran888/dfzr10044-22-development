"""恢复证据领域：恢复证据清单与恢复作业的数据模型。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class RecoveryManifest:
    """恢复证据清单：按库域和湖表标识列出可恢复文件的归属范围。

    清单一旦受理即冻结：快照标识、分区清单、文件清单都不再随湖表改名或
    新快照确认而变化，恢复执行时沿清单读取一次已经受理的历史输入。
    """

    manifest_id: str
    database_domain: str
    table_uid: str
    table_name: str  # 清单受理时的逻辑表名（改名后仍可查回原名）
    file_table_key: str  # 存储引擎物理键：库域.表名，保证跨库域同名表隔离
    snapshot_id: str
    snapshot_status: str
    file_ids: list[str]
    partition_keys: list[str]
    total_rows: int
    status: str  # open | claimed
    created_at: float
    claimed_by: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "manifest_id": self.manifest_id,
            "database_domain": self.database_domain,
            "table_uid": self.table_uid,
            "table_name": self.table_name,
            "file_table_key": self.file_table_key,
            "snapshot_id": self.snapshot_id,
            "snapshot_status": self.snapshot_status,
            "file_ids": list(self.file_ids),
            "partition_keys": list(self.partition_keys),
            "total_rows": self.total_rows,
            "file_count": len(self.file_ids),
            "status": self.status,
            "created_at": self.created_at,
            "claimed_by": self.claimed_by,
        }


@dataclass
class RecoveryJob:
    """恢复作业：领取恢复清单后产生，沿冻结清单逐文件读取并记录断点。"""

    job_id: str
    manifest_id: str
    database_domain: str
    table_uid: str
    file_table_key: str  # 清单文件实际挂载的物理键（改名后旧文件仍挂在同一键下）
    snapshot_id: str
    file_ids: list[str]
    read_checkpoint: list[str] = field(default_factory=list)
    rows_read: int = 0
    status: str = "running"  # running | paused | completed | failed
    failure_reason: str = ""
    created_at: float = 0.0
    updated_at: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "manifest_id": self.manifest_id,
            "database_domain": self.database_domain,
            "table_uid": self.table_uid,
            "file_table_key": self.file_table_key,
            "snapshot_id": self.snapshot_id,
            "file_ids": list(self.file_ids),
            "read_checkpoint": list(self.read_checkpoint),
            "read_count": len(self.read_checkpoint),
            "remaining_count": len(self.file_ids) - len(self.read_checkpoint),
            "rows_read": self.rows_read,
            "status": self.status,
            "failure_reason": self.failure_reason,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
