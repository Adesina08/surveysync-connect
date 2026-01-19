from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from app.services import sync_engine

router = APIRouter(prefix="/api/sync-jobs", tags=["sync-jobs"])


SyncModeIn = Literal["append", "upsert", "replace", "insert"]


class SyncJobCreateRequest(BaseModel):
    formId: str
    sessionToken: str
    targetSchema: str
    targetTable: str
    syncMode: SyncModeIn = "upsert"
    primaryKeyField: str | None = None
    createNewTable: bool = False


class SyncJobResponse(BaseModel):
    id: int
    name: str
    source: str
    target: str
    status: str
    created_at: datetime
    updated_at: datetime
    last_error: str | None
    last_synced_at: datetime | None
    config: dict[str, Any] | None = None


def _build_name(form_id: str, schema: str, table: str) -> str:
    name = f"sync_{form_id}_to_{schema}.{table}"
    return name[:200]


def _normalize_sync_mode(mode: str) -> str:
    # frontend may send "insert"; backend uses "append"
    if mode == "insert":
        return "append"
    return mode


@router.post("", response_model=SyncJobResponse)
def create_sync_job(request: SyncJobCreateRequest) -> SyncJobResponse:
    normalized_mode = _normalize_sync_mode(request.syncMode)

    if normalized_mode == "upsert" and not request.primaryKeyField:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="primaryKeyField is required when syncMode is 'upsert'.",
        )

    source = f"surveycto:{request.formId}"
    target = f"postgres:{request.targetSchema}.{request.targetTable}"
    name = _build_name(request.formId, request.targetSchema, request.targetTable)

    config = {
        "formId": request.formId,
        "sessionToken": request.sessionToken,
        "targetSchema": request.targetSchema,
        "targetTable": request.targetTable,
        "syncMode": normalized_mode,
        "primaryKeyField": request.primaryKeyField,
        "createNewTable": request.createNewTable,
    }

    job = sync_engine.create_sync_job(name=name, source=source, target=target, config=config)
    last_sync = sync_engine.get_last_sync(source, target)

    return SyncJobResponse(
        id=job.id,
        name=job.name,
        source=job.source,
        target=job.target,
        status=job.status,
        created_at=job.created_at,
        updated_at=job.updated_at,
        last_error=job.last_error,
        last_synced_at=last_sync.last_synced_at if last_sync else None,
        config=job.config,
    )


@router.get("", response_model=list[SyncJobResponse])
def list_sync_jobs() -> list[SyncJobResponse]:
    jobs = sync_engine.list_sync_jobs()
    responses: list[SyncJobResponse] = []

    for job in jobs:
        last_sync = sync_engine.get_last_sync(job.source, job.target)
        responses.append(
            SyncJobResponse(
                id=job.id,
                name=job.name,
                source=job.source,
                target=job.target,
                status=job.status,
                created_at=job.created_at,
                updated_at=job.updated_at,
                last_error=job.last_error,
                last_synced_at=last_sync.last_synced_at if last_sync else None,
                config=job.config,
            )
        )

    return responses
