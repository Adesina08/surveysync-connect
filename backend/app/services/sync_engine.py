from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg import sql

from app.db.session import get_connection
from app.models.last_sync import LastSyncMetadata
from app.models.sync_job import SyncJob
from app.services import postgres_session, postgres_service


def create_sync_job(
    name: str,
    source: str,
    target: str,
    config: dict[str, Any] | None = None,
) -> SyncJob:
    timestamp = datetime.now(tz=timezone.utc)
    config_json = json.dumps(config or {}, ensure_ascii=False)

    with get_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO sync_jobs (name, source, target, status, created_at, updated_at, last_error, config_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (name, source, target, "queued", timestamp.isoformat(), timestamp.isoformat(), None, config_json),
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
            """
            SELECT id, name, source, target, status, created_at, updated_at, last_error, config_json
            FROM sync_jobs
            ORDER BY id DESC
            """
        ).fetchall()

    jobs: list[SyncJob] = []
    for row in rows:
        config: dict[str, Any] | None = None
        raw = row["config_json"]
        if raw:
            try:
                parsed = json.loads(raw)
                config = parsed if isinstance(parsed, dict) else None
            except Exception:
                config = None

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
                config=config,
            )
        )
    return jobs


def get_sync_job(job_id: int) -> SyncJob | None:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id, name, source, target, status, created_at, updated_at, last_error, config_json
            FROM sync_jobs
            WHERE id = ?
            """,
            (job_id,),
        ).fetchone()

    if not row:
        return None

    config: dict[str, Any] | None = None
    raw = row["config_json"]
    if raw:
        try:
            parsed = json.loads(raw)
            config = parsed if isinstance(parsed, dict) else None
        except Exception:
            config = None

    return SyncJob(
        id=row["id"],
        name=row["name"],
        source=row["source"],
        target=row["target"],
        status=row["status"],
        created_at=datetime.fromisoformat(row["created_at"]),
        updated_at=datetime.fromisoformat(row["updated_at"]),
        last_error=row["last_error"],
        config=config,
    )


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


def _pg_connect_from_session() -> psycopg.Connection:
    creds = postgres_session.get_credentials()
    return psycopg.connect(
        host=creds.host,
        port=creds.port,
        dbname=creds.database,
        user=creds.username,
        password=creds.password,
        sslmode=creds.sslmode,
        connect_timeout=10,
    )


def run_sync_job(*, job_id: int, rows: list[dict[str, Any]]) -> None:
    job = get_sync_job(job_id)
    if not job or not job.config:
        record_sync_completion(job_id, "failed", "Job not found or missing config_json")
        return

    cfg = job.config
    target_schema = str(cfg.get("targetSchema") or "").strip()
    target_table = str(cfg.get("targetTable") or "").strip()
    mode = str(cfg.get("syncMode") or "upsert").lower()
    pk = cfg.get("primaryKeyField")
    create_new = bool(cfg.get("createNewTable", False))

    if not target_schema or not target_table:
        record_sync_completion(job_id, "failed", "Missing targetSchema/targetTable")
        return
    if mode == "upsert" and not pk:
        record_sync_completion(job_id, "failed", "primaryKeyField required for upsert")
        return

    # mark running
    with get_connection() as connection:
        ts = datetime.now(tz=timezone.utc).isoformat()
        connection.execute("UPDATE sync_jobs SET status = ?, updated_at = ? WHERE id = ?", ("running", ts, job_id))
        connection.commit()

    # infer columns
    all_keys: set[str] = set()
    for r in rows:
        if isinstance(r, dict):
            all_keys.update(r.keys())

    if mode == "upsert" and pk not in all_keys:
        record_sync_completion(job_id, "failed", f"Primary key '{pk}' not found in SurveyCTO data")
        return

    # create table if requested
    if create_new:
        cols = [{"name": k, "type": "TEXT", "nullable": True, "isPrimaryKey": (k == pk)} for k in sorted(all_keys)]
        creds = postgres_session.get_credentials()
        postgres_service.create_table(
            creds=creds,
            schema=target_schema,
            table=target_table,
            columns=cols,
            primary_key=str(pk) if pk else None,
        )

    keys = sorted(all_keys)
    if not keys:
        record_sync_completion(job_id, "failed", "No columns found in SurveyCTO data")
        return

    conn = _pg_connect_from_session()
    try:
        with conn.cursor() as cur:
            if mode == "replace":
                cur.execute(
                    sql.SQL("TRUNCATE TABLE {}.{}").format(
                        sql.Identifier(target_schema),
                        sql.Identifier(target_table),
                    )
                )

            columns_sql = sql.SQL(", ").join(sql.Identifier(k) for k in keys)
            values_sql = sql.SQL(", ").join(sql.Placeholder() for _ in keys)

            stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                sql.Identifier(target_schema),
                sql.Identifier(target_table),
                columns_sql,
                values_sql,
            )

            if mode == "upsert":
                update_cols = [k for k in keys if k != pk]
                update_sql = sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(k), sql.Identifier(k)) for k in update_cols
                )
                stmt = stmt + sql.SQL(" ON CONFLICT ({}) DO UPDATE SET {}").format(
                    sql.Identifier(str(pk)),
                    update_sql if update_cols else sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(str(pk)), sql.Identifier(str(pk))),
                )

            for r in rows:
                vals: list[Any] = []
                for k in keys:
                    v = r.get(k)
                    if isinstance(v, (dict, list)):
                        v = json.dumps(v, ensure_ascii=False)
                    vals.append(v)
                cur.execute(stmt, vals)

        conn.commit()
    except Exception as exc:
        conn.rollback()
        record_sync_completion(job_id, "failed", str(exc))
        return
    finally:
        conn.close()

    now = datetime.now(tz=timezone.utc)
    upsert_last_sync(job.source, job.target, now)
    record_sync_completion(job_id, "completed", None)
