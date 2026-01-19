from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2 import sql, extras

from app.services import sync_engine, surveycto_service


@dataclass
class SyncRunResult:
    job_id: int
    status: str
    processed_records: int
    total_records: int
    inserted_records: int
    updated_records: int
    errors: list[dict[str, Any]]
    started_at: datetime
    completed_at: datetime


def _infer_pg_dsn_from_env() -> str:
    # You likely have this already; adjust as needed.
    # Example env usage: DATABASE_URL or individual PG vars.
    import os

    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return dsn

    host = os.getenv("PGHOST")
    port = os.getenv("PGPORT", "5432")
    db = os.getenv("PGDATABASE")
    user = os.getenv("PGUSER")
    pw = os.getenv("PGPASSWORD")
    sslmode = os.getenv("PGSSLMODE", "require")

    if not (host and db and user and pw):
        raise RuntimeError("Postgres credentials not configured (DATABASE_URL or PG* env vars).")

    return f"host={host} port={port} dbname={db} user={user} password={pw} sslmode={sslmode}"


def run_sync_job(job_id: int) -> SyncRunResult:
    started_at = datetime.now(tz=timezone.utc)
    errors: list[dict[str, Any]] = []

    jobs = {j.id: j for j in sync_engine.list_sync_jobs()}
    job = jobs.get(job_id)
    if not job:
        return SyncRunResult(
            job_id=job_id,
            status="failed",
            processed_records=0,
            total_records=0,
            inserted_records=0,
            updated_records=0,
            errors=[{"message": f"Job {job_id} not found", "timestamp": started_at.isoformat()}],
            started_at=started_at,
            completed_at=datetime.now(tz=timezone.utc),
        )

    cfg = job.config or {}
    form_id = cfg.get("formId")
    session_token = cfg.get("sessionToken")
    schema = cfg.get("targetSchema")
    table = cfg.get("targetTable")
    sync_mode = cfg.get("syncMode", "upsert")
    pk = cfg.get("primaryKeyField") or "KEY"

    if not (form_id and session_token and schema and table):
        msg = "Missing job config: formId/sessionToken/targetSchema/targetTable"
        sync_engine.record_sync_completion(job_id, "failed", msg)
        return SyncRunResult(
            job_id=job_id,
            status="failed",
            processed_records=0,
            total_records=0,
            inserted_records=0,
            updated_records=0,
            errors=[{"message": msg, "timestamp": started_at.isoformat()}],
            started_at=started_at,
            completed_at=datetime.now(tz=timezone.utc),
        )

    source = f"surveycto:{form_id}"
    target = f"postgres:{schema}.{table}"

    last_sync = sync_engine.get_last_sync(source, target)
    since_dt = last_sync.last_synced_at if last_sync else None

    try:
        # Fetch SurveyCTO wide JSON rows (incremental via date=epoch)
        rows = _run_async_fetch(session_token=session_token, form_id=form_id, since_dt=since_dt)
    except surveycto_service.SubmissionsFetchError as exc:
        errors.append({"message": str(exc), "timestamp": datetime.now(tz=timezone.utc).isoformat()})
        sync_engine.record_sync_completion(job_id, "failed", str(exc))
        return SyncRunResult(
            job_id=job_id,
            status="failed",
            processed_records=0,
            total_records=0,
            inserted_records=0,
            updated_records=0,
            errors=errors,
            started_at=started_at,
            completed_at=datetime.now(tz=timezone.utc),
        )
    except Exception as exc:
        errors.append({"message": f"Unexpected error: {exc!r}", "timestamp": datetime.now(tz=timezone.utc).isoformat()})
        sync_engine.record_sync_completion(job_id, "failed", str(exc))
        return SyncRunResult(
            job_id=job_id,
            status="failed",
            processed_records=0,
            total_records=0,
            inserted_records=0,
            updated_records=0,
            errors=errors,
            started_at=started_at,
            completed_at=datetime.now(tz=timezone.utc),
        )

    total = len(rows)
    if total == 0:
        # still mark completion and bump last sync to now (optional)
        sync_engine.record_sync_completion(job_id, "completed", None)
        sync_engine.upsert_last_sync(source, target, datetime.now(tz=timezone.utc))
        return SyncRunResult(
            job_id=job_id,
            status="completed",
            processed_records=0,
            total_records=0,
            inserted_records=0,
            updated_records=0,
            errors=[],
            started_at=started_at,
            completed_at=datetime.now(tz=timezone.utc),
        )

    # Determine columns from data
    col_names = sorted({k for r in rows for k in r.keys() if k and isinstance(k, str)})
    if pk not in col_names:
        col_names.append(pk)  # ensure pk exists in insert list if sync_mode requires it

    dsn = _infer_pg_dsn_from_env()

    inserted = 0
    updated = 0

    try:
        with psycopg2.connect(dsn) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                if sync_mode == "append":
                    inserted = _insert_append(cur, schema, table, col_names, rows)
                else:
                    # upsert by default
                    inserted, updated = _upsert(cur, schema, table, col_names, rows, pk)

            conn.commit()
    except Exception as exc:
        errors.append({"message": f"Postgres write failed: {exc}", "timestamp": datetime.now(tz=timezone.utc).isoformat()})
        sync_engine.record_sync_completion(job_id, "failed", str(exc))
        return SyncRunResult(
            job_id=job_id,
            status="failed",
            processed_records=0,
            total_records=total,
            inserted_records=inserted,
            updated_records=updated,
            errors=errors,
            started_at=started_at,
            completed_at=datetime.now(tz=timezone.utc),
        )

    # Update last sync = now (or max CompletionDate if you want)
    sync_engine.upsert_last_sync(source, target, datetime.now(tz=timezone.utc))
    sync_engine.record_sync_completion(job_id, "completed", None)

    completed_at = datetime.now(tz=timezone.utc)
    return SyncRunResult(
        job_id=job_id,
        status="completed",
        processed_records=total,
        total_records=total,
        inserted_records=inserted,
        updated_records=updated,
        errors=errors,
        started_at=started_at,
        completed_at=completed_at,
    )


def _run_async_fetch(session_token: str, form_id: str, since_dt: datetime | None) -> list[dict]:
    """
    Run the async httpx function from sync_runner (sync context).
    """
    import asyncio

    return asyncio.run(surveycto_service.fetch_submissions_wide_json(session_token, form_id, since_dt))


def _insert_append(cur, schema: str, table: str, cols: list[str], rows: list[dict]) -> int:
    q = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(",").join(sql.Identifier(c) for c in cols),
        sql.SQL(",").join(sql.Placeholder() for _ in cols),
    )
    values = [tuple(_coerce_value(r.get(c)) for c in cols) for r in rows]
    extras.execute_batch(cur, q, values, page_size=500)
    return len(values)


def _upsert(cur, schema: str, table: str, cols: list[str], rows: list[dict], pk: str) -> tuple[int, int]:
    if pk not in cols:
        raise ValueError(f"primary key field '{pk}' not present in data columns")

    insert_cols = cols
    placeholders = sql.SQL(",").join(sql.Placeholder() for _ in insert_cols)

    update_cols = [c for c in insert_cols if c != pk]
    set_clause = sql.SQL(",").join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in update_cols
    )

    q = sql.SQL(
        "INSERT INTO {}.{} ({}) VALUES ({}) "
        "ON CONFLICT ({}) DO UPDATE SET {}"
    ).format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(",").join(sql.Identifier(c) for c in insert_cols),
        placeholders,
        sql.Identifier(pk),
        set_clause,
    )

    values = [tuple(_coerce_value(r.get(c)) for c in insert_cols) for r in rows]
    extras.execute_batch(cur, q, values, page_size=500)

    # psycopg2 can't easily tell inserted vs updated without RETURNING trick; keep it simple
    return (0, len(values))


def _coerce_value(v: Any) -> Any:
    # Keep dict/list as JSON strings; let PG jsonb accept strings if column is jsonb
    if isinstance(v, (dict, list)):
        import json
        return json.dumps(v)
    return v
