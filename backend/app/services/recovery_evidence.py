"""恢复证据模块服务：恢复证据视图的工作台登记、筛选与动作骨架。

真实的清单受理、领取与断点续读由 app/recovery 领域和 /api/recovery/*
专用接口承载；本服务负责把恢复证据条目纳入统一资源路由与运营概览，
保持与其余 22 个业务模块一致的列表/动作契约。
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService


class RecoveryEvidenceService(BaseModuleService):
    MODULE = "recovery_evidence"
    ENTITY = "恢复证据条目"
    OPERATION_TERM = "恢复"
    LIST_FIELDS = [
        "证据编号", "所属库域", "湖表标识", "湖表名称",
        "快照标识", "分区清单", "文件清单", "证据状态",
        "恢复作业", "受理时间", "已读文件数", "待补文件数",
    ]
    REQUIRED_FIELDS = ["证据编号", "所属库域", "湖表标识", "快照标识"]
    STATUS_ORDER = ["待受理", "已受理", "恢复中", "已恢复", "恢复失败"]
    ACTION_RULES = {
        "受理清单": "已受理",
        "启动恢复": "恢复中",
        "确认恢复": "已恢复",
        "标记失败": "恢复失败",
    }
    NEGATIVE_ACTIONS = ["标记失败"]

    def evidence_snapshot(self) -> dict[str, Any]:
        """恢复证据视图的模块级统计，供运营概览聚合。"""
        rows = self.list_entries(size=1_000_000)[0]
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "total": len(rows),
            "accepted": sum(1 for r in rows if r.get("status") in ("已受理", "恢复中", "已恢复")),
            "recovering": sum(1 for r in rows if r.get("status") == "恢复中"),
            "recovered": sum(1 for r in rows if r.get("status") == "已恢复"),
            "failed": sum(1 for r in rows if r.get("status") == "恢复失败"),
        }
