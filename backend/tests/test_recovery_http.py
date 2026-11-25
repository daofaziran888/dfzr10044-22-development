"""恢复证据模块服务与专用 HTTP 接口的契约测试。

不引入 httpx 依赖：通过 ASGI 协议直接在进程内调用 FastAPI 应用，
只用标准库完成请求/响应的 JSON 编解码。
"""
from __future__ import annotations

import asyncio
import json
from typing import Any
from urllib.parse import urlencode

import pytest

from app.lake_store import lake_store
from app.main import app
from app.recovery import recovery_manager
from app.services.recovery_evidence import RecoveryEvidenceService


class AsgiResponse:
    def __init__(self, status: int, body: bytes) -> None:
        self.status_code = status
        self._body = body

    def json(self) -> Any:
        return json.loads(self._body.decode("utf-8"))


class AsgiTestClient:
    """最小 ASGI 测试客户端：进程内调用，覆盖 GET/POST + JSON 请求体。"""

    def __init__(self, asgi_app) -> None:
        self.app = asgi_app

    def request(
        self, method: str, path: str, payload: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> AsgiResponse:
        if params:
            path = f"{path}?{urlencode(params)}"
        body = json.dumps(payload).encode("utf-8") if payload is not None else b""
        scope = {
            "type": "http",
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": path.split("?", 1)[0],
            "query_string": path.split("?", 1)[1].encode("utf-8") if "?" in path else b"",
            "headers": [(b"content-type", b"application/json")],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
        }
        chunks: list[bytes] = []

        async def receive() -> dict[str, Any]:
            return {"type": "http.request", "body": body, "more_body": False}

        async def send(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.start":
                chunks.append(message.get("status", 200).to_bytes(2, "big"))
            elif message["type"] == "http.response.body":
                chunks.append(message.get("body", b""))

        asyncio.run(self.app(scope, receive, send))
        status = int.from_bytes(chunks[0], "big")
        return AsgiResponse(status, b"".join(chunks[1:]))

    def get(self, path: str, params: dict[str, Any] | None = None) -> AsgiResponse:
        return self.request("GET", path, params=params)

    def post(self, path: str, payload: dict[str, Any] | None = None) -> AsgiResponse:
        return self.request("POST", path, payload=payload or {})


client = AsgiTestClient(app)


@pytest.fixture(autouse=True)
def reset_singletons():
    # 每个用例前清空全局单例里的恢复领域数据，避免用例间串扰
    recovery_manager._manifests.clear()
    recovery_manager._jobs.clear()
    lake_store._reclamation_orders.clear()
    lake_store._file_leases.clear()
    lake_store._active_lease_index.clear()
    lake_store._table_identities.clear()
    lake_store._table_name_index.clear()
    lake_store._files.clear()
    lake_store._snapshots.clear()
    yield


def _seed_sales(domain: str, file_count: int) -> str:
    table = "sales"
    key = recovery_manager.physical_key(domain, table)
    lake_store.register_table_identity(domain, table)
    file_ids = []
    for i in range(file_count):
        pf = lake_store.register_file(key, "dt=20261101", rows=1, bytes_=1280)
        file_ids.append(pf.file_id)
    snap = lake_store.create_snapshot(key, file_ids, added_rows=file_count)
    lake_store.mark_snapshot_available(key, snap.snapshot_id)
    return snap.snapshot_id


class TestRecoveryEvidenceService:
    def test_workbench_listing_contract(self):
        svc = RecoveryEvidenceService()
        items, total = svc.list_entries(size=100)
        assert total >= 3
        assert any(row["所属库域"] == "lake_ods" for row in items)

    def test_evidence_snapshot_stats(self):
        svc = RecoveryEvidenceService()
        stats = svc.evidence_snapshot()
        assert stats["module"] == "recovery_evidence"
        assert stats["total"] >= 3
        assert "recovering" in stats


class TestRecoveryHttpApi:
    def test_full_recovery_flow_over_http(self):
        snap_a = _seed_sales("lake_ods", 7)
        _seed_sales("lake_dwd", 11)

        # 概览包含两个库域的 sales 条目
        overview = client.get("/api/recovery/overview").json()
        assert overview["table_count"] == 2

        # 受理并领取库域 A 的清单
        created = client.post("/api/recovery/manifests", {
            "database_domain": "lake_ods", "table_name": "sales",
        }).json()
        assert created["ok"]
        manifest_id = created["entry"]["manifest_id"]
        assert created["entry"]["file_count"] == 7
        assert created["entry"]["snapshot_id"] == snap_a

        claimed = client.post(f"/api/recovery/manifests/{manifest_id}/claim", {}).json()
        assert claimed["ok"]
        job_id = claimed["entry"]["job_id"]

        # 读取第一份文件后暂停
        first = client.post(f"/api/recovery/jobs/{job_id}/read-next").json()
        assert first["success"]
        client.post(f"/api/recovery/jobs/{job_id}/pause")
        assert client.post(f"/api/recovery/jobs/{job_id}/read-next").status_code == 409

        # 同时提交旧版文件回收，进入等待
        order = client.post("/api/recovery/reclamations", {
            "lake_table": created["entry"]["file_table_key"],
            "file_id": first["file_id"],
        }).json()["entry"]
        assert order["status"] == "waiting"

        # 值班查询持有者
        holder = client.get("/api/recovery/files/holder", {
            "lake_table": order["lake_table"], "file_id": first["file_id"],
        }).json()
        assert holder["holder"] == job_id

        # 恢复执行直到完成
        client.post(f"/api/recovery/jobs/{job_id}/resume")
        for _ in range(6):
            client.post(f"/api/recovery/jobs/{job_id}/read-next")
        completed = client.post(f"/api/recovery/jobs/{job_id}/complete").json()
        assert completed["status"] == "completed"
        assert len(completed["files_read"]) == 7

        # 完成后回收单可执行
        refreshed = client.get("/api/recovery/reclamations").json()["items"][0]
        assert refreshed["status"] == "reclaimable"
        reclaimed = client.post(
            "/api/recovery/reclamations/execute", {"order_id": order["order_id"]}
        ).json()
        assert reclaimed["entry"]["status"] == "reclaimed"

        # 库域 B 的 11 行独立成单，未被 A 读入
        manifest_b = client.post("/api/recovery/manifests", {
            "database_domain": "lake_dwd", "table_name": "sales",
        }).json()
        assert manifest_b["entry"]["file_count"] == 11

    def test_copy_path_reclamation_over_http(self):
        _seed_sales("lake_ods", 3)
        manifest = client.post("/api/recovery/manifests", {
            "database_domain": "lake_ods", "table_name": "sales",
        }).json()["entry"]
        job = client.post(
            f"/api/recovery/manifests/{manifest['manifest_id']}/claim", {}
        ).json()["entry"]
        target = manifest["file_ids"][0]
        key = manifest["file_table_key"]

        order = client.post("/api/recovery/reclamations", {
            "lake_table": key, "file_id": target,
        }).json()["entry"]
        confirmed = client.post(
            "/api/recovery/reclamations/confirm-copy",
            {"order_id": order["order_id"]},
        ).json()["entry"]
        assert confirmed["status"] == "copy_confirmed"
        client.post(
            "/api/recovery/reclamations/execute",
            {"order_id": order["order_id"]},
        )
        read = client.post(f"/api/recovery/jobs/{job['job_id']}/read-next").json()
        assert read["read_source"]["mode"] == "replica"

    def test_rename_overview_reflects_new_name(self):
        _seed_sales("lake_ods", 2)
        renamed = client.post("/api/recovery/tables/rename", {
            "database_domain": "lake_ods", "old_name": "sales", "new_name": "sales_v2",
        }).json()
        assert renamed["ok"]
        assert renamed["entry"]["table_name"] == "sales_v2"
        overview = client.get("/api/recovery/overview").json()
        entry = next(
            e for e in overview["entries"] if e["database_domain"] == "lake_ods"
        )
        assert entry["table_name"] == "sales_v2"
        assert entry["previous_names"] == ["sales"]
