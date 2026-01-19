from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2 import sql
import psycopg2.extras as extras

from app.services import sync_engine, surveycto_service, postgres_session


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
            errors=[{"recordId": "job", "field": None, "message": f"Job {job_id} not found"}],
            started_at=started_at,
            completed_at=datetime.now(tz=timezone.utc),
        )

    cfg = job.config or {}
    form_id = cfg.get("formId")
    session_token = cfg.get("sessionToken")
    schema = cfg.get("targetSchema")
    table = cfg.get("targetTable")
    sync_mode = cfg.get("syncMode", "upsert")

    # Frontend uses syncMode: "insert" | "upsert"
    if sync_mode == "insert":
        sync_mode = "append"

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
            errors=[{"recordId": "config", "field": None, "message": msg}],
            started_at=started_at,
            completed_at=datetime.now(tz=timezone.utc),
        )

    source = f"surveycto:{form_id}"
    target = f"postgres:{schema}.{table}"

    last_sync = sync_engine.get_last_sync(source, target)
    since_dt = last_sync.last_synced_at if last_sync else None

    try:
        rows = _run_async_fetch(session_token=session_token, form_id=form_id, since_dt=since_dt)
    except surveycto_service.SubmissionsFetchError as exc:
        errors.append({"recordId": "surveycto", "field": None, "message": str(exc)})
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
        errors.append({"recordId": "sync", "field": None, "message": f"Unexpected error: {exc!r}"})
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

    col_names = sorted({k for r in rows for k in r.keys() if k and isinstance(k, str)})
    if sync_mode == "upsert" and pk not in col_names:
        raise ValueError(f"primary key field '{pk}' not present in data columns")

    inserted = 0
    updated = 0

    creds = postgres_session.get_credentials()
    if not creds:
        msg = "Postgres not connected (missing stored credentials)."
        errors.append({"recordId": "postgres", "field": None, "message": msg})
        sync_engine.record_sync_completion(job_id, "failed", msg)
        return SyncRunResult(
            job_id=job_id,
            status="failed",
            processed_records=0,
            total_records=total,
            inserted_records=0,
            updated_records=0,
            errors=errors,
            started_at=started_at,
            completed_at=datetime.now(tz=timezone.utc),
        )

    try:
        with psycopg2.connect(
            host=creds.host,
            port=creds.port,
            dbname=creds.database,
            user=creds.username,
            password=creds.password,
            sslmode=creds.sslmode,
            connect_timeout=10,
        ) as conn:
            conn.autocommit = False
            with conn.cursor() as cur:
                if sync_mode == "append":
                    inserted = _insert_append(cur, schema, table, col_names, rows)
                else:
                    inserted, updated = _upsert(cur, schema, table, col_names, rows, pk)

            conn.commit()
    except Exception as exc:
        errors.append({"recordId": "postgres", "field": None, "message": f"Postgres write failed: {exc}"})
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

    # Without RETURNING, we can't accurately split inserted vs updated.
    return (0, len(values))


def _coerce_value(v: Any) -> Any:
    if isinstance(v, (dict, list)):
        import json
        return json.dumps(v)
    return v
