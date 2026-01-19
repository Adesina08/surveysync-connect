from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2 import sql, extras

from app.services import sync_engine, surveycto_service


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


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def run_sync_job(job_id: int) -> None:
    jobs = {j.id: j for j in sync_engine.list_sync_jobs()}
    job = jobs.get(job_id)
    if not job:
        return

    cfg = job.config or {}
    form_id = cfg.get("formId")
    session_token = cfg.get("sessionToken")
    schema = cfg.get("targetSchema")
    table = cfg.get("targetTable")
    sync_mode = cfg.get("syncMode", "upsert")
    pk = cfg.get("primaryKeyField") or "KEY"

    if not (form_id and session_token and schema and table):
        msg = "Missing job config: formId/sessionToken/targetSchema/targetTable"
        sync_engine.finish_failed(job_id, msg)
        return

    source = f"surveycto:{form_id}"
    target = f"postgres:{schema}.{table}"

    last_sync = sync_engine.get_last_sync(source, target)
    since_dt = last_sync.last_synced_at if last_sync else None

    try:
        sync_engine.update_progress(job_id, status="running", startedAt=_now_iso())
        rows = _run_async_fetch(session_token=session_token, form_id=form_id, since_dt=since_dt)
        total = len(rows)
        sync_engine.update_progress(job_id, totalRecords=total)

        if total == 0:
            sync_engine.finish_success(job_id, inserted=0, updated=0)
            sync_engine.upsert_last_sync(source, target, datetime.now(tz=timezone.utc))
            return

        col_names = sorted({k for r in rows for k in r.keys() if k and isinstance(k, str)})
        if pk not in col_names:
            col_names.append(pk)

        dsn = _infer_pg_dsn_from_env()
        inserted = 0
        updated = 0

        with psycopg2.connect(dsn) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                if sync_mode == "append":
                    inserted = _insert_append(cur, schema, table, col_names, rows)
                else:
                    inserted, updated = _upsert(cur, schema, table, col_names, rows, pk)
            conn.commit()

        sync_engine.update_progress(
            job_id,
            processedRecords=total,
            insertedRecords=inserted,
            updatedRecords=updated,
        )
        sync_engine.finish_success(job_id, inserted=inserted, updated=updated)
        sync_engine.upsert_last_sync(source, target, datetime.now(tz=timezone.utc))
    except surveycto_service.SubmissionsFetchError as exc:
        sync_engine.finish_failed(job_id, str(exc))
    except Exception as exc:
        sync_engine.finish_failed(job_id, str(exc))


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
