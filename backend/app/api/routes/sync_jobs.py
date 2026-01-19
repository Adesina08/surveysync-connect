from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Path, status
from pydantic import BaseModel, Field

from app.services import sync_engine, sync_runner


router = APIRouter(prefix="/api/sync-jobs", tags=["sync-jobs"])


# -------------------------
# Request/Response models to MATCH src/api/types.ts
# -------------------------

class SyncJobConfig(BaseModel):
    formId: str
    targetSchema: str
    targetTable: str
    syncMode: Literal["insert", "upsert"] = "upsert"
    primaryKeyField: str | None = None
    createNewTable: bool = False
    sessionToken: str = Field(..., description="SurveyCTO session token from /sessions")


class SyncError(BaseModel):
    recordId: str
    field: str | None = None
    message: str


class SyncProgress(BaseModel):
    jobId: str
    status: Literal["pending", "running", "completed", "failed"]
    processedRecords: int
    totalRecords: int
    insertedRecords: int
    updatedRecords: int
    errors: list[SyncError]
    startedAt: str | None = None
    completedAt: str | None = None


def _progress_from_engine(progress: dict[str, Any]) -> SyncProgress:
    # Ensure all fields exist exactly as the frontend expects
    raw_errors = progress.get("errors") or []
    errors: list[SyncError] = []
    for e in raw_errors:
        # tolerate older shapes
        errors.append(
            SyncError(
                recordId=str(e.get("recordId") or e.get("id") or "unknown"),
                field=e.get("field"),
                message=str(e.get("message") or "Unknown error"),
            )
        )

    return SyncProgress(
        jobId=str(progress.get("jobId")),
        status=progress.get("status", "pending"),
        processedRecords=int(progress.get("processedRecords") or 0),
        totalRecords=int(progress.get("totalRecords") or 0),
        insertedRecords=int(progress.get("insertedRecords") or 0),
        updatedRecords=int(progress.get("updatedRecords") or 0),
        errors=errors,
        startedAt=progress.get("startedAt"),
        completedAt=progress.get("completedAt"),
    )


@router.get("", response_model=list[SyncProgress])
def list_sync_jobs() -> list[SyncProgress]:
    jobs = sync_engine.list_sync_jobs_progress()
    return [_progress_from_engine(j) for j in jobs]


@router.post("", response_model=SyncProgress)
def create_sync_job(config: SyncJobConfig) -> SyncProgress:
    if config.syncMode == "upsert" and not config.primaryKeyField:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="primaryKeyField is required when syncMode is 'upsert'.",
        )

    job_id = sync_engine.create_sync_job(config.model_dump())

    progress = sync_engine.get_progress(job_id)
    if not progress:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create sync job")

    return _progress_from_engine(progress)


@router.post("/{job_id}/run", response_model=SyncProgress)
def run_sync_job(job_id: int = Path(..., ge=1)) -> SyncProgress:
    # Mark running early so UI can react
    sync_engine.mark_progress(
        job_id,
        status="running",
        started_at=datetime.now(tz=timezone.utc),
    )

    result = sync_runner.run_sync_job(job_id)

    # Persist progress back in engine store
    if result.status == "completed":
        sync_engine.mark_progress(
            job_id,
            status="completed",
            processed_records=result.processed_records,
            total_records=result.total_records,
            inserted_records=result.inserted_records,
            updated_records=result.updated_records,
            completed_at=result.completed_at,
            errors=result.errors,
        )
    else:
        sync_engine.mark_progress(
            job_id,
            status="failed",
            processed_records=result.processed_records,
            total_records=result.total_records,
            inserted_records=result.inserted_records,
            updated_records=result.updated_records,
            completed_at=result.completed_at,
            errors=result.errors,
        )

    progress = sync_engine.get_progress(job_id)
    if not progress:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sync job not found")

    return _progress_from_engine(progress)
