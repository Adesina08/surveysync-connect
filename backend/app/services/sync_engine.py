from __future__ import annotations

import json
from datetime import datetime, timezone

from app.db.session import get_connection
from app.models.last_sync import LastSyncMetadata
from app.models.sync_job import SyncJob


def create_sync_job(name: str, source: str, target: str, config: dict | None = None) -> SyncJob:
    timestamp = datetime.now(tz=timezone.utc)
    config_json = json.dumps(config or {})

    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO sync_jobs (name, source, target, status, created_at, updated_at, last_error, config_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                name,
                source,
                target,
                "queued",
                timestamp.isoformat(),
                timestamp.isoformat(),
                None,
                config_json,
            ),
        )
        connection.commit()
        job_id = cursor.lastrowid

    return SyncJob(
        id=job_id,
        name=name,
        source=source,
        target=target,
        status="queued",
        created_at=timestamp,
        updated_at=timestamp,
        last_error=None,
        config=config or {},
    )


def list_sync_jobs() -> list[SyncJob]:
    with get_connection() as connection:
        rows = connection.execute(
            "SELECT id, name, source, target, status, created_at, updated_at, last_error, config_json FROM sync_jobs"
        ).fetchall()

    jobs: list[SyncJob] = []
    for row in rows:
        try:
            cfg = json.loads(row["config_json"] or "{}")
        except Exception:
            cfg = {}
        jobs.append(
            SyncJob(
                id=row["id"],
                name=row["name"],
                source=row["source"],
                target=row["target"],
                status=row["status"],
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"]),
                last_error=row["last_error"],
                config=cfg,
            )
        )
    return jobs


def record_sync_completion(job_id: int, status: str, last_error: str | None = None) -> None:
    timestamp = datetime.now(tz=timezone.utc)
    with get_connection() as connection:
        connection.execute(
            """
            UPDATE sync_jobs
            SET status = ?, updated_at = ?, last_error = ?
            WHERE id = ?
            """,
            (status, timestamp.isoformat(), last_error, job_id),
        )
        connection.commit()


def _default_progress(job_id: int) -> dict:
    return {
        "jobId": job_id,
        "status": "pending",
        "processedRecords": 0,
        "totalRecords": 0,
        "insertedRecords": 0,
        "updatedRecords": 0,
        "errors": [],
        "startedAt": None,
        "completedAt": None,
    }


def _row_to_progress(row) -> dict:
    try:
        errors = json.loads(row["errors_json"] or "[]")
    except Exception:
        errors = []
    return {
        "jobId": row["job_id"],
        "status": row["status"],
        "processedRecords": row["processed_records"] or 0,
        "totalRecords": row["total_records"] or 0,
        "insertedRecords": row["inserted_records"] or 0,
        "updatedRecords": row["updated_records"] or 0,
        "errors": errors,
        "startedAt": row["started_at"],
        "completedAt": row["completed_at"],
    }


def set_progress_running(job_id: int) -> dict:
    timestamp = datetime.now(tz=timezone.utc).isoformat()
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO sync_progress (
                job_id,
                status,
                processed_records,
                total_records,
                inserted_records,
                updated_records,
                errors_json,
                started_at,
                completed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                status = excluded.status,
                processed_records = excluded.processed_records,
                total_records = excluded.total_records,
                inserted_records = excluded.inserted_records,
                updated_records = excluded.updated_records,
                errors_json = excluded.errors_json,
                started_at = excluded.started_at,
                completed_at = excluded.completed_at
            """,
            (
                job_id,
                "running",
                0,
                0,
                0,
                0,
                json.dumps([]),
                timestamp,
                None,
            ),
        )
        connection.commit()
    record_sync_completion(job_id, "running", None)
    return get_progress(job_id) or _default_progress(job_id)


def get_progress(job_id: int) -> dict | None:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT job_id, status, processed_records, total_records, inserted_records, updated_records,
                   errors_json, started_at, completed_at
            FROM sync_progress
            WHERE job_id = ?
            """,
            (job_id,),
        ).fetchone()
    if not row:
        return None
    return _row_to_progress(row)


def list_progress() -> list[dict]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT job_id, status, processed_records, total_records, inserted_records, updated_records,
                   errors_json, started_at, completed_at
            FROM sync_progress
            ORDER BY job_id DESC
            """
        ).fetchall()
    return [_row_to_progress(row) for row in rows]


def update_progress(
    job_id: int,
    *,
    status: str | None = None,
    processedRecords: int | None = None,
    totalRecords: int | None = None,
    insertedRecords: int | None = None,
    updatedRecords: int | None = None,
    errors: list[dict] | None = None,
    startedAt: str | None = None,
    completedAt: str | None = None,
) -> dict:
    existing = get_progress(job_id) or _default_progress(job_id)
    payload = {
        "status": status or existing["status"],
        "processed_records": processedRecords if processedRecords is not None else existing["processedRecords"],
        "total_records": totalRecords if totalRecords is not None else existing["totalRecords"],
        "inserted_records": insertedRecords if insertedRecords is not None else existing["insertedRecords"],
        "updated_records": updatedRecords if updatedRecords is not None else existing["updatedRecords"],
        "errors_json": json.dumps(errors if errors is not None else existing["errors"]),
        "started_at": startedAt if startedAt is not None else existing["startedAt"],
        "completed_at": completedAt if completedAt is not None else existing["completedAt"],
    }

    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO sync_progress (
                job_id,
                status,
                processed_records,
                total_records,
                inserted_records,
                updated_records,
                errors_json,
                started_at,
                completed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                status = excluded.status,
                processed_records = excluded.processed_records,
                total_records = excluded.total_records,
                inserted_records = excluded.inserted_records,
                updated_records = excluded.updated_records,
                errors_json = excluded.errors_json,
                started_at = excluded.started_at,
                completed_at = excluded.completed_at
            """,
            (
                job_id,
                payload["status"],
                payload["processed_records"],
                payload["total_records"],
                payload["inserted_records"],
                payload["updated_records"],
                payload["errors_json"],
                payload["started_at"],
                payload["completed_at"],
            ),
        )
        connection.commit()
    if status == "completed":
        record_sync_completion(job_id, status, None)
    return get_progress(job_id) or _default_progress(job_id)


def finish_success(job_id: int, *, inserted: int = 0, updated: int = 0) -> dict:
    timestamp = datetime.now(tz=timezone.utc).isoformat()
    return update_progress(
        job_id,
        status="completed",
        insertedRecords=inserted,
        updatedRecords=updated,
        completedAt=timestamp,
    )


def finish_failed(job_id: int, error_message: str) -> dict:
    timestamp = datetime.now(tz=timezone.utc).isoformat()
    progress = get_progress(job_id) or _default_progress(job_id)
    errors = list(progress["errors"])
    errors.append(
        {
            "recordId": "n/a",
            "message": error_message,
        }
    )
    record_sync_completion(job_id, "failed", error_message)
    return update_progress(
        job_id,
        status="failed",
        errors=errors,
        completedAt=timestamp,
    )


def upsert_last_sync(source: str, target: str, last_synced_at: datetime) -> LastSyncMetadata:
    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO last_sync (source, target, last_synced_at)
            VALUES (?, ?, ?)
            ON CONFLICT(source, target) DO UPDATE SET last_synced_at = excluded.last_synced_at
            """,
            (source, target, last_synced_at.isoformat()),
        )
        connection.commit()
        row = connection.execute(
            "SELECT id, source, target, last_synced_at FROM last_sync WHERE source = ? AND target = ?",
            (source, target),
        ).fetchone()

    return LastSyncMetadata(
        id=row["id"],
        source=row["source"],
        target=row["target"],
        last_synced_at=datetime.fromisoformat(row["last_synced_at"]),
    )


def get_last_sync(source: str, target: str) -> LastSyncMetadata | None:
    with get_connection() as connection:
        row = connection.execute(
            "SELECT id, source, target, last_synced_at FROM last_sync WHERE source = ? AND target = ?",
            (source, target),
        ).fetchone()
    if not row:
        return None
    return LastSyncMetadata(
        id=row["id"],
        source=row["source"],
        target=row["target"],
        last_synced_at=datetime.fromisoformat(row["last_synced_at"]),
    )
