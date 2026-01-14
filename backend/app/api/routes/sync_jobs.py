from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from app.services import surveycto_service, sync_engine

router = APIRouter(prefix="/api/sync-jobs", tags=["sync-jobs"])


class SyncJobCreateRequest(BaseModel):
    formId: str
    targetSchema: str
    targetTable: str
    syncMode: Literal["append", "upsert", "replace"] = "upsert"
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


@router.post("", response_model=SyncJobResponse)
def create_sync_job(request: SyncJobCreateRequest) -> SyncJobResponse:
    if request.syncMode == "upsert" and not request.primaryKeyField:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="primaryKeyField is required when syncMode is 'upsert'.",
        )

    source = f"surveycto:{request.formId}"
    target = f"postgres:{request.targetSchema}.{request.targetTable}"
    name = _build_name(request.formId, request.targetSchema, request.targetTable)

    config = {
        "formId": request.formId,
        "targetSchema": request.targetSchema,
        "targetTable": request.targetTable,
        "syncMode": request.syncMode,
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


@router.get("/{job_id}", response_model=SyncJobResponse)
def get_sync_job(job_id: int) -> SyncJobResponse:
    job = sync_engine.get_sync_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Sync job not found")

    last_sync = sync_engine.get_last_sync(job.source, job.target)
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


@router.post("/{job_id}/run", response_model=SyncJobResponse)
async def run_sync_job(
    job_id: int,
    session_token: str = Query(..., description="Session token from /sessions"),
) -> SyncJobResponse:
    job = sync_engine.get_sync_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Sync job not found")
    cfg = job.config or {}

    form_id = cfg.get("formId")
    if not form_id:
        raise HTTPException(status_code=400, detail="Job missing formId in config")

    # download ALL data (date=0) for now
    try:
        rows = await surveycto_service.download_form_wide_json(
            session_token=session_token,
            form_id=str(form_id),
            date="0",
        )
    except surveycto_service.InvalidSessionError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except surveycto_service.AuthenticationError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc
    except surveycto_service.SurveyCTOServiceError as exc:
        raise HTTPException(status_code=502, detail=f"SurveyCTO download failed: {exc}") from exc

    # run sync (writes to Postgres)
    try:
        sync_engine.run_sync_job(job_id=job_id, rows=rows)
    except RuntimeError as exc:
        # typically: "Postgres is not connected"
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    updated = sync_engine.get_sync_job(job_id)
    if not updated:
        raise HTTPException(status_code=500, detail="Job disappeared after execution")

    last_sync = sync_engine.get_last_sync(updated.source, updated.target)
    return SyncJobResponse(
        id=updated.id,
        name=updated.name,
        source=updated.source,
        target=updated.target,
        status=updated.status,
        created_at=updated.created_at,
        updated_at=updated.updated_at,
        last_error=updated.last_error,
        last_synced_at=last_sync.last_synced_at if last_sync else None,
        config=updated.config,
    )
