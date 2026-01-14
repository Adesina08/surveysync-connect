from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services import sync_engine

router = APIRouter(prefix="/api/sync-jobs", tags=["sync-jobs"])


class SyncJobCreateRequest(BaseModel):
    name: str = Field(..., examples=["daily-household-sync"])
    source: str = Field(..., examples=["surveycto:household_survey"])
    target: str = Field(..., examples=["postgres:surveysync.public.responses"])


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


@router.post("", response_model=SyncJobResponse)
def create_sync_job(request: SyncJobCreateRequest) -> SyncJobResponse:
    job = sync_engine.create_sync_job(request.name, request.source, request.target)
    last_sync = sync_engine.get_last_sync(request.source, request.target)
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
