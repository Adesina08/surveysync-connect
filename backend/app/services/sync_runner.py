from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2 import sql

from app.services import postgres_service, surveycto_service, sync_engine


def _safe_ident(name: str) -> str:
    # Basic cleanup: you can expand rules, but psycopg2.sql.Identifier will quote safely.
    return name.strip()


def _infer_column_type(value: Any) -> str:
    # Conservative mapping
    if isinstance(value, (dict, list)):
        return "JSONB"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE PRECISION"
    # default
    return "TEXT"


def _to_db_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def _pick_primary_key(rows: list[dict[str, Any]], preferred: str | None) -> str | None:
    if preferred and all(preferred in r for r in rows):
        return preferred
    # common SurveyCTO key
    if rows and all("KEY" in r for r in rows):
        return "KEY"
    return None


def _max_completion_date(rows: list[dict[str, Any]]) -> datetime:
    # SurveyCTO wide json returns date strings like "Sep 25, 2025 1:11:52 PM"
    # We'll just store “now” as last_sync to ensure we don’t refetch endlessly if parsing fails.
    return datetime.now(timezone.utc)


def run_sync_job(job_id: int, session_token: str) -> dict[str, Any]:
    job = sync_engine.get_sync_job(job_id)
    if not job:
        raise RuntimeError("Sync job not found.")

    cfg = job.config or {}
    form_id = cfg.get("formId")
    target_schema = cfg.get("targetSchema")
    target_table = cfg.get("targetTable")
    sync_mode = cfg.get("syncMode", "upsert")
    pk_field = cfg.get("primaryKeyField")
    create_new_table = bool(cfg.get("createNewTable", False))

    if not form_id or not target_schema or not target_table:
        raise RuntimeError("Sync job config is incomplete.")

    source = f"surveycto:{form_id}"
    target = f"postgres:{target_schema}.{target_table}"

    last_sync = sync_engine.get_last_sync(source, target)
    since_dt = last_sync.last_synced_at if last_sync else None

    # 1) Fetch SurveyCTO submissions
    rows = psycopg2.extras.wait_select  # just to ensure extras import doesn't confuse lint
    rows = []

    # mark running
    sync_engine.record_sync_completion(job_id, "running", None)

    try:
        rows = _run_fetch(session_token=session_token, form_id=form_id, since_dt=since_dt)
        if not rows:
            sync_engine.record_sync_completion(job_id, "success", None)
            # bump last_sync anyway to avoid repeated “same request”
            sync_engine.upsert_last_sync(source, target, datetime.now(timezone.utc))
            return {"rowsFetched": 0, "rowsWritten": 0}

        # 2) Write to Postgres
        written = _run_write(
            rows=rows,
            schema=_safe_ident(target_schema),
            table=_safe_ident(target_table),
            sync_mode=sync_mode,
            preferred_pk=pk_field,
            create_new_table=create_new_table,
        )

        # 3) Update last_sync
        sync_engine.upsert_last_sync(source, target, datetime.now(timezone.utc))
        sync_engine.record_sync_completion(job_id, "success", None)

        return {"rowsFetched": len(rows), "rowsWritten": written}

    except Exception as exc:
        sync_engine.record_sync_completion(job_id, "failed", str(exc))
        raise


def _run_fetch(session_token: str, form_id: str, since_dt: datetime | None) -> list[dict[str, Any]]:
    # Call async function from sync context safely:
    import anyio

    return anyio.run(
        surveycto_service.fetch_wide_json_submissions,
        session_token,
        form_id,
        since_dt,
    )


def _run_write(
    rows: list[dict[str, Any]],
    schema: str,
    table: str,
    sync_mode: str,
    preferred_pk: str | None,
    create_new_table: bool,
) -> int:
    pk = _pick_primary_key(rows, preferred_pk)

    # infer columns from union of keys
    all_cols: list[str] = sorted({k for r in rows for k in r.keys()})
    if not all_cols:
        return 0

    # infer types using first non-null example
    types: dict[str, str] = {}
    for c in all_cols:
        v = next((r.get(c) for r in rows if r.get(c) is not None), None)
        types[c] = _infer_column_type(v)

    with postgres_service.connect() as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            # create schema
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))

            if create_new_table:
                _ensure_table(cur, schema, table, all_cols, types, pk)

            if sync_mode == "append":
                written = _insert_append(cur, schema, table, rows, all_cols)
            elif sync_mode == "replace":
                cur.execute(
                    sql.SQL("TRUNCATE TABLE {}.{}").format(sql.Identifier(schema), sql.Identifier(table))
                )
                written = _insert_append(cur, schema, table, rows, all_cols)
            else:
                # upsert
                if not pk:
                    raise RuntimeError("primaryKeyField/KEY required for upsert but not found in data.")
                written = _upsert(cur, schema, table, rows, all_cols, pk)

        conn.commit()
        return written


def _ensure_table(cur, schema: str, table: str, cols: list[str], types: dict[str, str], pk: str | None) -> None:
    # Build CREATE TABLE
    col_defs = []
    for c in cols:
        col_defs.append(
            sql.SQL("{} {}").format(sql.Identifier(c), sql.SQL(types[c]))
        )
    if pk:
        col_defs.append(sql.SQL("PRIMARY KEY ({})").format(sql.Identifier(pk)))

    cur.execute(
        sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({} )").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(col_defs),
        )
    )

    # Add missing columns
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    existing = {r[0] for r in cur.fetchall()}

    missing = [c for c in cols if c not in existing]
    for c in missing:
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(c),
                sql.SQL(types[c]),
            )
        )


def _insert_append(cur, schema: str, table: str, rows: list[dict[str, Any]], cols: list[str]) -> int:
    q = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, cols)),
        sql.SQL(", ").join(sql.Placeholder() * len(cols)),
    )

    values = [
        tuple(_to_db_value(r.get(c)) for c in cols)
        for r in rows
    ]
    psycopg2.extras.execute_batch(cur, q, values, page_size=500)
    return len(values)


def _upsert(cur, schema: str, table: str, rows: list[dict[str, Any]], cols: list[str], pk: str) -> int:
    insert_cols = cols
    update_cols = [c for c in cols if c != pk]

    q = sql.SQL(
        "INSERT INTO {}.{} ({}) VALUES ({}) "
        "ON CONFLICT ({}) DO UPDATE SET {}"
    ).format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, insert_cols)),
        sql.SQL(", ").join(sql.Placeholder() * len(insert_cols)),
        sql.Identifier(pk),
        sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in update_cols
        ),
    )

    values = [
        tuple(_to_db_value(r.get(c)) for c in insert_cols)
        for r in rows
    ]
    psycopg2.extras.execute_batch(cur, q, values, page_size=500)
    return len(values)
