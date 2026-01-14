from __future__ import annotations

from datetime import datetime
from typing import Literal

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from app.services import sync_engine

# IMPORTANT: match frontend path
router = APIRouter(prefix="/api/sync-jobs", tags=["sync-jobs"])


# ----------------------------
# Request models (match UI)
# ----------------------------

class SyncJobCreateRequest(BaseModel):
    formId: str = Field(..., description="SurveyCTO form id")
    targetSchema: str = Field(..., description="Target Postgres schema", examples=["public"])
    targetTable: str = Field(..., description="Target Postgres table", examples=["responses"])
    syncMode: Literal["append", "upsert", "replace"] = Field(default="upsert")
    primaryKeyField: str | None = Field(default=None, description="Primary key field for upsert")
    createNewTable: bool = Field(default=False, description="Create table if it does not exist")


# ----------------------------
# Response model (existing)
# ----------------------------

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


def _build_name(req: SyncJobCreateRequest) -> str:
    # simple deterministic name the UI can show
    base = f"sync_{req.formId}_to_{req.targetSchema}.{req.targetTable}"
    return base[:200]  # keep it reasonable


def _build_source(req: SyncJobCreateRequest) -> str:
    # sync_engine expects a string; keep a stable scheme prefix
    return f"surveycto:{req.formId}"


def _build_target(req: SyncJobCreateRequest) -> str:
    # encode pg location + options (still string-based for your sync_engine)
    # If your engine later supports JSON config, replace this with a structured store.
    parts = [
        f"postgres:{req.targetSchema}.{req.targetTable}",
        f"mode={req.syncMode}",
    ]
    if req.primaryKeyField:
        parts.append(f"pk={req.primaryKeyField}")
    if req.createNewTable:
        parts.append("create=1")
    return "|".join(parts)


@router.post("", response_model=SyncJobResponse)
def create_sync_job(request: SyncJobCreateRequest) -> SyncJobResponse:
    # Basic validation
    if request.syncMode == "upsert" and not request.primaryKeyField:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="primaryKeyField is required when syncMode is 'upsert'.",
        )

    name = _build_name(request)
    source = _build_source(request)
    target = _build_target(request)

    job = sync_engine.create_sync_job(name, source, target)
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
            )
        )

    return responses
