from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import random
import time

import psycopg2
import psycopg2.extras as extras
from psycopg2 import sql

from app.services import postgres_session, surveycto_service, sync_engine


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


def _is_transient_pg_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return (
        "ssl syscall" in msg
        or "server closed the connection" in msg
        or "connection already closed" in msg
        or "terminating connection" in msg
        or "timeout expired" in msg
        or "could not connect" in msg
        or "connection reset by peer" in msg
        or "broken pipe" in msg
        or "eof detected" in msg
    )


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

    # 1) Fetch SurveyCTO FIRST (may wait/retry on 417 inside surveycto_service)
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

    # For upsert, PK must exist in incoming data
    if sync_mode == "upsert" and pk not in col_names:
        msg = f"primary key field '{pk}' not present in data columns"
        errors.append({"recordId": "config", "field": None, "message": msg})
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

    def _connect_pg():
        """
        Robust connect with retries to survive transient provider/network/pooler drops.
        """
        sslmode = getattr(creds, "sslmode", None) or "require"
        last_exc: Exception | None = None

        for attempt in range(1, 5):  # 4 attempts
            try:
                return psycopg2.connect(
                    host=creds.host,
                    port=creds.port,
                    dbname=creds.database,
                    user=creds.username,
                    password=creds.password,
                    sslmode=sslmode,
                    connect_timeout=10,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                    application_name="surveysync-connect",
                )
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                last_exc = exc
                if (not _is_transient_pg_error(exc)) or attempt == 4:
                    raise
                # exponential backoff + jitter
                time.sleep((0.5 * (2 ** (attempt - 1))) + random.uniform(0, 0.25))

        raise last_exc if last_exc else RuntimeError("Postgres connect failed")

    def _write_once() -> tuple[int, int]:
        """
        Write with a fresh connection; retry once on transient TLS/socket drops.
        """
        for attempt in range(1, 3):  # 2 attempts
            conn = _connect_pg()
            try:
                with conn:
                    with conn.cursor() as cur:
                        _ensure_table_ready(cur, schema, table, col_names, sync_mode, pk)

                        if sync_mode == "append":
                            ins = _insert_append(cur, schema, table, col_names, rows)
                            return ins, 0
                        else:
                            return _upsert(cur, schema, table, col_names, rows, pk)
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                if (not _is_transient_pg_error(exc)) or attempt == 2:
                    raise
                time.sleep(1.0)
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

        raise RuntimeError("Unreachable")

    inserted = 0
    updated = 0

    # 2) Write to Postgres (with connect + write retries)
    try:
        inserted, updated = _write_once()
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
        errors=[],
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

    # Without RETURNING we can't split inserted vs updated accurately
    return (0, len(values))


def _ensure_schema(cur, schema: str) -> None:
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))


def _table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        )
        """,
        (schema, table),
    )
    return bool(cur.fetchone()[0])


def _get_existing_columns(cur, schema: str, table: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return {r[0] for r in cur.fetchall()}


def _create_table(cur, schema: str, table: str, cols: list[str], pk: str | None) -> None:
    if not cols:
        cur.execute(
            sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} (dummy TEXT);").format(
                sql.Identifier(schema),
                sql.Identifier(table),
            )
        )
        return

    col_defs: list[sql.Composed] = []
    for c in cols:
        if pk and c == pk:
            col_defs.append(sql.SQL("{} TEXT PRIMARY KEY").format(sql.Identifier(c)))
        else:
            col_defs.append(sql.SQL("{} TEXT").format(sql.Identifier(c)))

    cur.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(col_defs),
        )
    )


def _add_missing_columns(cur, schema: str, table: str, desired_cols: list[str]) -> None:
    existing = _get_existing_columns(cur, schema, table)
    missing = [c for c in desired_cols if c not in existing]
    for c in missing:
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} TEXT").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(c),
            )
        )


def _ensure_table_ready(cur, schema: str, table: str, cols: list[str], sync_mode: str, pk: str) -> None:
    _ensure_schema(cur, schema)

    desired = list(cols)
    if sync_mode == "upsert" and pk not in desired:
        desired.append(pk)

    if not _table_exists(cur, schema, table):
        _create_table(cur, schema, table, desired, pk if sync_mode == "upsert" else None)
    else:
        _add_missing_columns(cur, schema, table, desired)


def _coerce_value(v: Any) -> Any:
    if isinstance(v, (dict, list)):
        import json

        return json.dumps(v)
    return v
