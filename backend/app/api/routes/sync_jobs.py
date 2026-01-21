from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from fastapi import APIRouter, BackgroundTasks, HTTPException, Path, status
from pydantic import BaseModel, Field

from app.services import sync_engine, sync_runner


router = APIRouter(prefix="/api/sync-jobs", tags=["sync-jobs"])


def _run_and_persist(job_id: int) -> None:
    """Run a sync job and persist its final progress.

    This is meant to be executed as a FastAPI BackgroundTask so the POST request
    can return quickly and the UI can poll GET /api/sync-jobs/{id}.

    IMPORTANT: never allow an exception to escape this function, otherwise the
    job may remain stuck in "running" and the UI will poll forever.
    """
    try:
        result = sync_runner.run_sync_job(job_id)

        # Persist progress back in engine store
        sync_engine.mark_progress(
            job_id,
            status="completed" if result.status == "completed" else "failed",
            processed_records=result.processed_records,
            total_records=result.total_records,
            inserted_records=result.inserted_records,
            updated_records=result.updated_records,
            completed_at=result.completed_at,
            errors=result.errors,
        )
        sync_engine.record_sync_completion(
            job_id,
            "completed" if result.status == "completed" else "failed",
            None if result.status == "completed" else (result.errors[-1]["message"] if result.errors else "failed"),
        )

    except Exception as exc:
        # ✅ Never leave a job in "running" if the background task crashes
        msg = f"Background task crashed: {exc}"
        sync_engine.mark_progress(
            job_id,
            status="failed",
            processed_records=0,
            total_records=0,
            inserted_records=0,
            updated_records=0,
            completed_at=datetime.now(tz=timezone.utc),
            errors=[{"recordId": "backend", "field": None, "message": msg}],
        )
        sync_engine.record_sync_completion(job_id, "failed", msg)


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


@router.delete("/completed")
def clear_completed_jobs() -> dict[str, int]:
    count = sync_engine.clear_completed_jobs()
    return {"deleted": count}


@router.get("/{job_id}", response_model=SyncProgress)
def get_sync_job(job_id: int = Path(..., ge=1)) -> SyncProgress:
    progress = sync_engine.get_progress(job_id)
    if not progress:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sync job not found")
    return _progress_from_engine(progress)


@router.delete("/{job_id}")
def delete_sync_job(job_id: int = Path(..., ge=1)) -> dict[str, str]:
    deleted = sync_engine.delete_sync_job(job_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sync job not found")
    return {"status": "ok"}


@router.post("", response_model=SyncProgress)
def create_sync_job(config: SyncJobConfig, background_tasks: BackgroundTasks) -> SyncProgress:
    if config.syncMode == "upsert" and not config.primaryKeyField:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="primaryKeyField is required when syncMode is 'upsert'.",
        )

    job_id = sync_engine.create_sync_job(config.model_dump())

    # Mark running and start in the background so the UI can poll.
    sync_engine.mark_progress(
        job_id,
        status="running",
        started_at=datetime.now(tz=timezone.utc),
    )
    background_tasks.add_task(_run_and_persist, job_id)

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
        sync_engine.record_sync_completion(job_id, "completed", None)
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
        sync_engine.record_sync_completion(
            job_id,
            "failed",
            result.errors[-1]["message"] if result.errors else "failed",
        )

    progress = sync_engine.get_progress(job_id)
    if not progress:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sync job not found")

    return _progress_from_engine(progress)
