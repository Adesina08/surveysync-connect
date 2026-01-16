from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from app.services import postgres_service, postgres_session, surveycto_service, sync_engine

router = APIRouter(prefix="/api/sync-jobs", tags=["sync-jobs"])


# -------------------------
# API models (match frontend expectations)
# -------------------------

class SyncError(BaseModel):
    message: str
    timestamp: str


class SyncProgressResponse(BaseModel):
    jobId: int
    status: Literal["pending", "running", "completed", "failed", "cancelled"]
    processedRecords: int = 0
    totalRecords: int = 0
    insertedRecords: int = 0
    updatedRecords: int = 0
    errors: list[SyncError] = Field(default_factory=list)
    startedAt: Optional[str] = None
    completedAt: Optional[str] = None


class SyncJobCreateRequest(BaseModel):
    formId: str
    targetSchema: str
    targetTable: str
    syncMode: Literal["insert", "upsert", "replace"] = "upsert"
    primaryKeyField: str | None = None
    createNewTable: bool = False
    sessionToken: str | None = None


# -------------------------
# In-memory progress store
# -------------------------

_PROGRESS: Dict[int, SyncProgressResponse] = {}
_TASKS: Dict[int, asyncio.Task] = {}


def _utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _build_name(form_id: str, schema: str, table: str) -> str:
    name = f"sync_{form_id}_to_{schema}.{table}"
    return name[:200]


def _utc_to_epoch_seconds(value: datetime | None) -> str:
    if value is None:
        return "0"
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return str(int(value.astimezone(timezone.utc).timestamp()))


def _build_row_payload(row: dict, columns: list[str]) -> dict:
    return {col: row.get(col) for col in columns}


async def _run_sync(job_id: int, payload: SyncJobCreateRequest) -> None:
    progress = _PROGRESS[job_id]
    progress.status = "running"
    progress.startedAt = _utc_now_iso()
    try:
        if not payload.sessionToken:
            raise ValueError("SurveyCTO session token is required.")

        try:
            creds = postgres_session.get_credentials()
        except RuntimeError as exc:
            raise ValueError(str(exc)) from exc

        source = f"surveycto:{payload.formId}"
        target = f"postgres:{payload.targetSchema}.{payload.targetTable}"
        last_sync = sync_engine.get_last_sync(source, target)
        date_param = _utc_to_epoch_seconds(last_sync.last_synced_at if last_sync else None)

        submissions = await surveycto_service.download_form_wide_json(
            payload.sessionToken,
            payload.formId,
            date=date_param,
        )

        progress.totalRecords = len(submissions)

        if progress.totalRecords == 0:
            progress.status = "completed"
            progress.completedAt = _utc_now_iso()
            sync_engine.record_sync_completion(job_id, status="completed", last_error=None)
            sync_engine.upsert_last_sync(source, target, datetime.now(tz=timezone.utc))
            return

        if payload.createNewTable:
            raise ValueError("Automatic table creation is not implemented. Create the table first.")

        cols = postgres_service.get_table_columns(creds, payload.targetSchema, payload.targetTable)
        column_names = [c.name for c in cols]

        if not column_names:
            raise ValueError("Target table has no columns.")

        primary_key = payload.primaryKeyField or next((c.name for c in cols if c.is_primary_key), None)

        rows = [_build_row_payload(row, column_names) for row in submissions]

        if payload.syncMode == "replace":
            postgres_service.delete_all_rows(creds, payload.targetSchema, payload.targetTable)
            inserted = postgres_service.insert_rows(creds, payload.targetSchema, payload.targetTable, column_names, rows)
            progress.insertedRecords = inserted
            progress.updatedRecords = 0
            progress.processedRecords = progress.totalRecords
        elif payload.syncMode == "insert":
            inserted = postgres_service.insert_rows(creds, payload.targetSchema, payload.targetTable, column_names, rows)
            progress.insertedRecords = inserted
            progress.updatedRecords = 0
            progress.processedRecords = progress.totalRecords
        else:
            if not primary_key:
                raise ValueError("Primary key field is required for upsert mode.")
            inserted, updated = postgres_service.upsert_rows(
                creds,
                payload.targetSchema,
                payload.targetTable,
                column_names,
                rows,
                primary_key,
            )
            progress.insertedRecords = inserted
            progress.updatedRecords = updated
            progress.processedRecords = progress.totalRecords

        progress.status = "completed"
        progress.completedAt = _utc_now_iso()
        sync_engine.record_sync_completion(job_id, status="completed", last_error=None)
        sync_engine.upsert_last_sync(source, target, datetime.now(tz=timezone.utc))

    except Exception as exc:
        progress.status = "failed"
        progress.completedAt = _utc_now_iso()
        progress.errors.append(SyncError(message=str(exc), timestamp=_utc_now_iso()))
        sync_engine.record_sync_completion(job_id, status="failed", last_error=str(exc))


@router.post("", response_model=SyncProgressResponse)
async def create_sync_job(payload: SyncJobCreateRequest) -> SyncProgressResponse:
    if payload.syncMode == "upsert" and not payload.primaryKeyField:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="primaryKeyField is required when syncMode is 'upsert'.",
        )

    source = f"surveycto:{payload.formId}"
    target = f"postgres:{payload.targetSchema}.{payload.targetTable}"
    name = _build_name(payload.formId, payload.targetSchema, payload.targetTable)

    config: dict[str, Any] = {
        "formId": payload.formId,
        "targetSchema": payload.targetSchema,
        "targetTable": payload.targetTable,
        "syncMode": payload.syncMode,
        "primaryKeyField": payload.primaryKeyField,
        "createNewTable": payload.createNewTable,
    }

    job = sync_engine.create_sync_job(name=name, source=source, target=target, config=config)

    progress = SyncProgressResponse(
        jobId=job.id,
        status="pending",
        processedRecords=0,
        totalRecords=0,
        insertedRecords=0,
        updatedRecords=0,
        errors=[],
        startedAt=None,
        completedAt=None,
    )

    _PROGRESS[job.id] = progress
    _TASKS[job.id] = asyncio.create_task(_run_sync(job.id, payload))

    return progress


@router.get("", response_model=list[SyncProgressResponse])
def list_sync_jobs() -> list[SyncProgressResponse]:
    # Return progress for known jobs; if DB has jobs but no progress, you can hydrate later.
    return list(_PROGRESS.values())


@router.get("/{job_id}", response_model=SyncProgressResponse)
def get_sync_job(job_id: int) -> SyncProgressResponse:
    progress = _PROGRESS.get(job_id)
    if not progress:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sync job not found")
    return progress


@router.delete("/completed", response_model=dict)
def clear_completed() -> dict:
    completed_ids = [jid for jid, p in _PROGRESS.items() if p.status in {"completed", "failed", "cancelled"}]
    for jid in completed_ids:
        _PROGRESS.pop(jid, None)
        task = _TASKS.pop(jid, None)
        if task and not task.done():
            task.cancel()
    return {"success": True, "cleared": completed_ids}


@router.delete("/{job_id}", response_model=dict)
def cancel_job(job_id: int) -> dict:
    progress = _PROGRESS.get(job_id)
    if not progress:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sync job not found")

    progress.status = "cancelled"
    progress.completedAt = _utc_now_iso()

    task = _TASKS.get(job_id)
    if task and not task.done():
        task.cancel()

    sync_engine.record_sync_completion(job_id, status="cancelled", last_error=None)
    return {"success": True}
