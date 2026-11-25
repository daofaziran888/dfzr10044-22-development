"""恢复证据专用接口：湖表身份、恢复清单、恢复作业、旧版文件回收与值班查询。

与通用资源路由分开注册，路径固定为 /api/recovery/*，
承载恢复证据领域不能被通用 CRUD 表达的状态流转。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.lake_store import lake_store
from app.recovery import recovery_manager

router = APIRouter(prefix="/api/recovery", tags=["恢复证据"])


class IdentityPayload(BaseModel):
    database_domain: str
    table_name: str


class RenamePayload(BaseModel):
    database_domain: str
    old_name: str
    new_name: str


class ManifestPayload(BaseModel):
    database_domain: str
    table_name: str
    snapshot_id: str | None = None


class JobIdPayload(BaseModel):
    job_id: str | None = None
    reason: str | None = None


class ReclamationPayload(BaseModel):
    lake_table: str
    file_id: str


class OrderPayload(BaseModel):
    order_id: str


def _error(exc: Exception) -> HTTPException:
    if isinstance(exc, KeyError):
        return HTTPException(status_code=404, detail=str(exc))
    return HTTPException(status_code=409, detail=str(exc))


# ---- 恢复概览与湖表身份 ----

@router.get("/overview")
def recovery_overview() -> dict[str, Any]:
    return recovery_manager.recovery_overview()


@router.get("/tables")
def list_tables() -> dict[str, Any]:
    return {"items": [t.__dict__ for t in lake_store.list_table_identities()]}


@router.post("/tables/register")
def register_table(payload: IdentityPayload) -> dict[str, Any]:
    try:
        identity = lake_store.register_table_identity(
            payload.database_domain, payload.table_name
        )
    except (KeyError, ValueError, RuntimeError) as exc:
        raise _error(exc)
    return {"ok": True, "entry": identity.__dict__}


@router.post("/tables/rename")
def rename_table(payload: RenamePayload) -> dict[str, Any]:
    try:
        identity = lake_store.rename_table(
            payload.database_domain, payload.old_name, payload.new_name
        )
        # 旧物理键保留给已受理申请，新键克隆文件与快照链承接新写入
        old_key = recovery_manager.physical_key(
            payload.database_domain, payload.old_name
        )
        new_key = recovery_manager.physical_key(
            payload.database_domain, payload.new_name
        )
        cloned = lake_store.clone_table_storage(old_key, new_key)
    except (KeyError, ValueError) as exc:
        raise _error(exc)
    return {"ok": True, "entry": identity.__dict__, "cloned": cloned}


@router.get("/tables/resolve")
def resolve_table(database_domain: str, table_name: str) -> dict[str, Any]:
    identity = lake_store.resolve_table(database_domain, table_name)
    if identity is None:
        raise HTTPException(status_code=404, detail="湖表身份不存在")
    return identity.__dict__


# ---- 恢复证据清单 ----

@router.post("/manifests")
def create_manifest(payload: ManifestPayload) -> dict[str, Any]:
    try:
        manifest = recovery_manager.build_manifest(
            payload.database_domain, payload.table_name, payload.snapshot_id
        )
    except (KeyError, RuntimeError) as exc:
        raise _error(exc)
    return {"ok": True, "entry": manifest.to_dict()}


@router.get("/manifests")
def list_manifests(database_domain: str | None = None) -> dict[str, Any]:
    items = recovery_manager.list_manifests(database_domain)
    return {"items": [m.to_dict() for m in items], "total": len(items)}


@router.get("/manifests/{manifest_id}")
def get_manifest(manifest_id: str) -> dict[str, Any]:
    manifest = recovery_manager.get_manifest(manifest_id)
    if manifest is None:
        raise HTTPException(status_code=404, detail="恢复清单不存在")
    return manifest.to_dict()


@router.post("/manifests/{manifest_id}/claim")
def claim_manifest(manifest_id: str, payload: JobIdPayload | None = None) -> dict[str, Any]:
    try:
        job = recovery_manager.claim_manifest(
            manifest_id, payload.job_id if payload else None
        )
    except (KeyError, RuntimeError) as exc:
        raise _error(exc)
    return {"ok": True, "entry": job.to_dict()}


# ---- 恢复作业 ----

@router.get("/jobs")
def list_jobs(database_domain: str | None = None) -> dict[str, Any]:
    items = recovery_manager.list_jobs(database_domain)
    return {"items": [j.to_dict() for j in items], "total": len(items)}


@router.get("/jobs/{job_id}")
def get_job(job_id: str) -> dict[str, Any]:
    job = recovery_manager.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="恢复作业不存在")
    return job.to_dict()


@router.post("/jobs/{job_id}/read-next")
def read_next_file(job_id: str) -> dict[str, Any]:
    try:
        return recovery_manager.read_next_file(job_id)
    except (KeyError, RuntimeError) as exc:
        raise _error(exc)


@router.post("/jobs/{job_id}/pause")
def pause_job(job_id: str) -> dict[str, Any]:
    try:
        job = recovery_manager.pause_job(job_id)
    except (KeyError, RuntimeError) as exc:
        raise _error(exc)
    return {"ok": True, "entry": job.to_dict()}


@router.post("/jobs/{job_id}/resume")
def resume_job(job_id: str) -> dict[str, Any]:
    try:
        job = recovery_manager.resume_job(job_id)
    except (KeyError, RuntimeError) as exc:
        raise _error(exc)
    return {"ok": True, "entry": job.to_dict()}


@router.post("/jobs/{job_id}/fail")
def fail_job(job_id: str, payload: JobIdPayload) -> dict[str, Any]:
    try:
        job = recovery_manager.fail_job(job_id, payload.reason or "人工标记失败")
    except KeyError as exc:
        raise _error(exc)
    return {"ok": True, "entry": job.to_dict()}


@router.post("/jobs/{job_id}/complete")
def complete_job(job_id: str) -> dict[str, Any]:
    try:
        return recovery_manager.complete_job(job_id)
    except (KeyError, RuntimeError) as exc:
        raise _error(exc)


@router.get("/jobs/{job_id}/unread")
def unread_files(job_id: str) -> dict[str, Any]:
    try:
        files = recovery_manager.get_unread_files(job_id)
    except KeyError as exc:
        raise _error(exc)
    return {"job_id": job_id, "unread_file_ids": files, "count": len(files)}


@router.get("/jobs/{job_id}/scope")
def snapshot_scope(job_id: str) -> dict[str, Any]:
    try:
        return recovery_manager.validate_snapshot_scope(job_id)
    except KeyError as exc:
        raise _error(exc)


# ---- 旧版文件回收 ----

@router.post("/reclamations")
def submit_reclamation(payload: ReclamationPayload) -> dict[str, Any]:
    order = lake_store.submit_reclamation(payload.lake_table, payload.file_id)
    return {"ok": True, "entry": order.__dict__}


@router.get("/reclamations")
def list_reclamations(lake_table: str | None = None) -> dict[str, Any]:
    items = lake_store.list_reclamation_orders(lake_table)
    return {"items": [o.__dict__ for o in items], "total": len(items)}


@router.post("/reclamations/confirm-copy")
def confirm_copy(payload: OrderPayload) -> dict[str, Any]:
    try:
        order = lake_store.confirm_replica_copy(payload.order_id)
    except (KeyError, RuntimeError) as exc:
        raise _error(exc)
    return {"ok": True, "entry": order.__dict__}


@router.post("/reclamations/execute")
def execute_reclamation(payload: OrderPayload) -> dict[str, Any]:
    try:
        order = lake_store.execute_reclamation(payload.order_id)
    except (KeyError, RuntimeError) as exc:
        raise _error(exc)
    return {"ok": True, "entry": order.__dict__}


@router.get("/files/holder")
def file_holder(lake_table: str, file_id: str) -> dict[str, Any]:
    holder = lake_store.get_file_holder(lake_table, file_id)
    if holder is None:
        return {"lake_table": lake_table, "file_id": file_id, "holder": None}
    return holder
