from __future__ import annotations

"""SQLite-backed storage for sync jobs + progress.

The frontend expects the following API shapes:
- POST /api/sync-jobs creates a job and returns a *progress* object.
- GET  /api/sync-jobs returns a list of *progress* objects.
- POST /api/sync-jobs/{id}/run updates progress, then returns progress.

Those endpoints call the functions implemented in this module.
"""

import json
from datetime import datetime, timezone
from typing import Any

from app.db.session import get_connection
from app.models.last_sync import LastSyncMetadata
from app.models.sync_job import SyncJob


def _utcnow() -> datetime:
    return datetime.now(tz=timezone.utc)


def _safe_json_dumps(obj: Any, default: Any = None) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except TypeError:
        return json.dumps(default if default is not None else {}, ensure_ascii=False)


def _safe_json_loads(raw: str | None, default: Any) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return default


def _build_job_fields(config: dict[str, Any]) -> tuple[str, str, str]:
    """Derive name/source/target fields from the frontend config."""
    form_id = str(config.get("formId") or "").strip()
    schema = str(config.get("targetSchema") or "").strip()
    table = str(config.get("targetTable") or "").strip()

    name = f"sync_{form_id}_to_{schema}.{table}".strip()
    if len(name) > 200:
        name = name[:200]

    source = f"surveycto:{form_id}"
    target = f"postgres:{schema}.{table}"
    return name, source, target


# -----------------------------------------------------------------------------
# Sync Jobs
# -----------------------------------------------------------------------------


def create_sync_job(config: dict[str, Any]) -> int:
    """Create a new sync job and a corresponding progress row.

    Returns the created job id.
    """
    timestamp = _utcnow()
    name, source, target = _build_job_fields(config)

    with get_connection() as connection:
        cur = connection.execute(
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
                _safe_json_dumps(config, default={}),
            ),
        )
        job_id = int(cur.lastrowid)

        connection.execute(
            """
            INSERT OR REPLACE INTO sync_progress
              (job_id, status, processed_records, total_records, inserted_records, updated_records, errors_json, started_at, completed_at)
            VALUES
              (?, ?, 0, 0, 0, 0, '[]', NULL, NULL)
            """,
            (job_id, "pending"),
        )
        connection.commit()

    return job_id


def list_sync_jobs() -> list[SyncJob]:
    with get_connection() as connection:
        rows = connection.execute(
            "SELECT id, name, source, target, status, created_at, updated_at, last_error, config_json FROM sync_jobs ORDER BY id DESC"
        ).fetchall()

    jobs: list[SyncJob] = []
    for row in rows:
        jobs.append(
            SyncJob(
                id=int(row["id"]),
                name=row["name"],
                source=row["source"],
                target=row["target"],
                status=row["status"],
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"]),
                last_error=row["last_error"],
                config=_safe_json_loads(row["config_json"], default={}),
            )
        )
    return jobs


# -----------------------------------------------------------------------------
# Progress
# -----------------------------------------------------------------------------


def _progress_row_to_dict(row) -> dict[str, Any]:
    return {
        "jobId": str(row["job_id"]),
        "status": row["status"],
        "processedRecords": int(row["processed_records"] or 0),
        "totalRecords": int(row["total_records"] or 0),
        "insertedRecords": int(row["inserted_records"] or 0),
        "updatedRecords": int(row["updated_records"] or 0),
        "errors": _safe_json_loads(row["errors_json"], default=[]),
        "startedAt": row["started_at"],
        "completedAt": row["completed_at"],
    }


def list_sync_jobs_progress() -> list[dict[str, Any]]:
    """Return all job progress in the exact camelCase keys the UI expects."""
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT
              p.job_id, p.status, p.processed_records, p.total_records,
              p.inserted_records, p.updated_records, p.errors_json,
              p.started_at, p.completed_at
            FROM sync_progress p
            ORDER BY p.job_id DESC
            """
        ).fetchall()

    return [_progress_row_to_dict(r) for r in rows]


def get_progress(job_id: int) -> dict[str, Any] | None:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT
              job_id, status, processed_records, total_records,
              inserted_records, updated_records, errors_json,
              started_at, completed_at
            FROM sync_progress
            WHERE job_id = ?
            """,
            (job_id,),
        ).fetchone()

    if not row:
        return None
    return _progress_row_to_dict(row)


def mark_progress(
    job_id: int,
    *,
    status: str | None = None,
    processed_records: int | None = None,
    total_records: int | None = None,
    inserted_records: int | None = None,
    updated_records: int | None = None,
    errors: list[dict[str, Any]] | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> None:
    """Update progress fields safely (partial updates allowed)."""
    with get_connection() as connection:
        current = connection.execute(
            "SELECT * FROM sync_progress WHERE job_id = ?",
            (job_id,),
        ).fetchone()

        if not current:
            # Create if missing (defensive)
            connection.execute(
                """
                INSERT OR REPLACE INTO sync_progress
                  (job_id, status, processed_records, total_records, inserted_records, updated_records, errors_json, started_at, completed_at)
                VALUES
                  (?, ?, 0, 0, 0, 0, '[]', NULL, NULL)
                """,
                (job_id, status or "pending"),
            )
            connection.commit()
            return

        new_status = status or current["status"]
        new_processed = int(processed_records) if processed_records is not None else int(current["processed_records"] or 0)
        new_total = int(total_records) if total_records is not None else int(current["total_records"] or 0)
        new_inserted = int(inserted_records) if inserted_records is not None else int(current["inserted_records"] or 0)
        new_updated = int(updated_records) if updated_records is not None else int(current["updated_records"] or 0)

        new_errors = errors if errors is not None else _safe_json_loads(current["errors_json"], default=[])

        # Important: UI date formatting crashes if it receives undefined.
        # We always store either NULL or an ISO string.
        new_started_at = (
            started_at.isoformat() if started_at is not None else current["started_at"]
        )
        new_completed_at = (
            completed_at.isoformat() if completed_at is not None else current["completed_at"]
        )

        connection.execute(
            """
            UPDATE sync_progress
            SET status = ?, processed_records = ?, total_records = ?, inserted_records = ?, updated_records = ?,
                errors_json = ?, started_at = ?, completed_at = ?
            WHERE job_id = ?
            """,
            (
                new_status,
                new_processed,
                new_total,
                new_inserted,
                new_updated,
                _safe_json_dumps(new_errors, default=[]),
                new_started_at,
                new_completed_at,
                job_id,
            ),
        )

        # Keep sync_jobs.status roughly aligned for admin/debugging
        connection.execute(
            "UPDATE sync_jobs SET status = ?, updated_at = ? WHERE id = ?",
            (new_status, _utcnow().isoformat(), job_id),
        )
        connection.commit()


# -----------------------------------------------------------------------------
# Last Sync
# -----------------------------------------------------------------------------


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
        id=int(row["id"]),
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
        id=int(row["id"]),
        source=row["source"],
        target=row["target"],
        last_synced_at=datetime.fromisoformat(row["last_synced_at"]),
    )




# -----------------------------------------------------------------------------
# Job lifecycle helpers (used by API routes)
# -----------------------------------------------------------------------------


def record_sync_completion(job_id: int, status: str, last_error: str | None) -> None:
    """Update sync_jobs metadata for admin/debugging.

    Note: progress is stored/updated via mark_progress(); this function just keeps
    sync_jobs (status/last_error) aligned.
    """
    with get_connection() as connection:
        connection.execute(
            "UPDATE sync_jobs SET status = ?, updated_at = ?, last_error = ? WHERE id = ?",
            (status, _utcnow().isoformat(), last_error, job_id),
        )
        connection.commit()


# -----------------------------------------------------------------------------
# SurveyCTO cooldowns (HTTP 417)
# -----------------------------------------------------------------------------


def set_surveycto_cooldown(source: str, cooldown_until: datetime) -> None:
    """Persist a server-directed cooldown window for a SurveyCTO source.

    SurveyCTO may respond with HTTP 417 and a message like
    "Please wait for 106 seconds...". When that happens we store a cooldown so
    the UI can't hammer the API by repeatedly clicking "Try Again".
    """
    if cooldown_until.tzinfo is None:
        cooldown_until = cooldown_until.replace(tzinfo=timezone.utc)

    with get_connection() as connection:
        connection.execute(
            """
            INSERT INTO surveycto_cooldowns (source, cooldown_until, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(source) DO UPDATE SET cooldown_until = excluded.cooldown_until
            """,
            (source, cooldown_until.isoformat(), _utcnow().isoformat()),
        )
        connection.commit()


def get_surveycto_cooldown(source: str) -> datetime | None:
    """Return cooldown_until if the cooldown is still active, else None."""
    with get_connection() as connection:
        row = connection.execute(
            "SELECT cooldown_until FROM surveycto_cooldowns WHERE source = ?",
            (source,),
        ).fetchone()

        if not row:
            return None

        try:
            cooldown_until = datetime.fromisoformat(row["cooldown_until"])
        except Exception:
            # If corrupted, clear
            connection.execute("DELETE FROM surveycto_cooldowns WHERE source = ?", (source,))
            connection.commit()
            return None

        if cooldown_until.tzinfo is None:
            cooldown_until = cooldown_until.replace(tzinfo=timezone.utc)

        now = _utcnow()
        if cooldown_until <= now:
            connection.execute("DELETE FROM surveycto_cooldowns WHERE source = ?", (source,))
            connection.commit()
            return None

        return cooldown_until


def clear_surveycto_cooldown(source: str) -> None:
    with get_connection() as connection:
        connection.execute("DELETE FROM surveycto_cooldowns WHERE source = ?", (source,))
        connection.commit()


def delete_sync_job(job_id: int) -> bool:
    """Delete a job and its progress. Returns True if something was deleted."""
    with get_connection() as connection:
        cur1 = connection.execute("DELETE FROM sync_progress WHERE job_id = ?", (job_id,))
        cur2 = connection.execute("DELETE FROM sync_jobs WHERE id = ?", (job_id,))
        connection.commit()
    return (cur1.rowcount or 0) > 0 or (cur2.rowcount or 0) > 0


def clear_completed_jobs() -> int:
    """Remove completed/failed jobs from storage. Returns number of jobs deleted."""
    with get_connection() as connection:
        ids = [
            int(r["id"])
            for r in connection.execute(
                "SELECT id FROM sync_jobs WHERE status IN ('completed','failed')"
            ).fetchall()
        ]
        if not ids:
            return 0
        connection.executemany("DELETE FROM sync_progress WHERE job_id = ?", [(i,) for i in ids])
        connection.executemany("DELETE FROM sync_jobs WHERE id = ?", [(i,) for i in ids])
        connection.commit()
    return len(ids)

