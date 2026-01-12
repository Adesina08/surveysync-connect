from __future__ import annotations

from datetime import datetime, timezone

from backend.app.db.session import get_connection
from backend.app.models.last_sync import LastSyncMetadata
from backend.app.models.sync_job import SyncJob


def create_sync_job(name: str, source: str, target: str) -> SyncJob:
    timestamp = datetime.now(tz=timezone.utc)
    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO sync_jobs (name, source, target, status, created_at, updated_at, last_error)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (name, source, target, "queued", timestamp.isoformat(), timestamp.isoformat(), None),
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
    )


def list_sync_jobs() -> list[SyncJob]:
    with get_connection() as connection:
        rows = connection.execute(
            "SELECT id, name, source, target, status, created_at, updated_at, last_error FROM sync_jobs"
        ).fetchall()
    return [
        SyncJob(
            id=row["id"],
            name=row["name"],
            source=row["source"],
            target=row["target"],
            status=row["status"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            last_error=row["last_error"],
        )
        for row in rows
    ]


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
