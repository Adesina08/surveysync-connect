from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, BackgroundTasks, HTTPException, status
from pydantic import BaseModel

from app.services import sync_engine, sync_runner

router = APIRouter(prefix="/api/sync-jobs", tags=["sync-jobs"])


class SyncJobCreateRequest(BaseModel):
    formId: str
    targetSchema: str
    targetTable: str
    syncMode: Literal["append", "upsert", "replace"] = "upsert"
    primaryKeyField: str | None = None
    createNewTable: bool = False
    sessionToken: str | None = None  # important for actual sync runner


class SyncProgressResponse(BaseModel):
    jobId: int
    status: str
    processedRecords: int = 0
    totalRecords: int = 0
    insertedRecords: int = 0
    updatedRecords: int = 0
    errors: list[dict[str, Any]] = []
    startedAt: str | None = None
    completedAt: str | None = None


def _build_name(form_id: str, schema: str, table: str) -> str:
    name = f"sync_{form_id}_to_{schema}.{table}"
    return name[:200]


@router.post("", response_model=SyncProgressResponse)
def create_sync_job(request: SyncJobCreateRequest, background: BackgroundTasks) -> SyncProgressResponse:
    if request.syncMode == "upsert" and not request.primaryKeyField:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="primaryKeyField is required when syncMode is 'upsert'.",
        )
    if not request.sessionToken:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="sessionToken is required to run a sync (SurveyCTO).",
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
        "sessionToken": request.sessionToken,
    }

    job = sync_engine.create_sync_job(name=name, source=source, target=target, config=config)
    sync_engine.set_progress_running(job.id)
    background.add_task(sync_runner.run_sync_job, job.id)
    return sync_engine.get_progress(job.id)


@router.get("", response_model=list[SyncProgressResponse])
def list_sync_jobs() -> list[SyncProgressResponse]:
    return sync_engine.list_progress()


@router.get("/{job_id}", response_model=SyncProgressResponse)
def get_progress(job_id: int) -> SyncProgressResponse:
    progress = sync_engine.get_progress(job_id)
    if not progress:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job not found")
    return progress
