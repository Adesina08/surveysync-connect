from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_batch

from app.services import surveycto_service, sync_engine


def _sanitize_identifier(name: str) -> str:
    # minimal sanitization; you can improve later
    return "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in name).lower()


def _infer_pg_type(value: Any) -> str:
    if value is None:
        return "TEXT"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE PRECISION"
    return "TEXT"


def _create_table_if_needed(
    pg_conn,
    schema: str,
    table: str,
    primary_key: str | None,
    sample_row: dict[str, Any],
) -> None:
    cur = pg_conn.cursor()

    # Ensure schema exists
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))

    # Build columns from sample row
    columns = []
    for k, v in sample_row.items():
        col = _sanitize_identifier(k)
        col_type = _infer_pg_type(v)
        if primary_key and col == _sanitize_identifier(primary_key):
            columns.append(sql.SQL("{} {} PRIMARY KEY").format(sql.Identifier(col), sql.SQL(col_type)))
        else:
            columns.append(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(col_type)))

    create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(columns),
    )
    cur.execute(create_stmt)
    pg_conn.commit()
    cur.close()


def _ensure_columns_exist(pg_conn, schema: str, table: str, row: dict[str, Any]) -> None:
    """
    Adds missing columns as TEXT (safe default) to avoid failing inserts when SurveyCTO adds fields.
    """
    cur = pg_conn.cursor()
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    existing = {r[0] for r in cur.fetchall()}

    for k in row.keys():
        col = _sanitize_identifier(k)
        if col in existing:
            continue
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} TEXT").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(col),
            )
        )
        existing.add(col)

    pg_conn.commit()
    cur.close()


def _upsert_records(
    pg_conn,
    schema: str,
    table: str,
    records: list[dict[str, Any]],
    primary_key_field: str,
) -> tuple[int, int]:
    if not records:
        return (0, 0)

    pk = _sanitize_identifier(primary_key_field)

    # Make sure table has needed columns
    _ensure_columns_exist(pg_conn, schema, table, records[0])

    cols = [_sanitize_identifier(k) for k in records[0].keys()]

    insert_stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, cols)),
        sql.SQL(", ").join(sql.Placeholder() * len(cols)),
    )

    update_cols = [c for c in cols if c != pk]
    conflict_stmt = sql.SQL(" ON CONFLICT ({}) DO UPDATE SET {}").format(
        sql.Identifier(pk),
        sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in update_cols
        ),
    )

    final_stmt = insert_stmt + conflict_stmt

    values = []
    for r in records:
        row_vals = [r.get(orig_key) for orig_key in records[0].keys()]
        values.append(row_vals)

    cur = pg_conn.cursor()
    execute_batch(cur, final_stmt.as_string(pg_conn), values, page_size=500)
    pg_conn.commit()
    cur.close()

    # We can’t reliably split inserted vs updated without RETURNING; return (processed, 0) for now.
    return (len(records), 0)


def _append_records(pg_conn, schema: str, table: str, records: list[dict[str, Any]]) -> int:
    if not records:
        return 0

    _ensure_columns_exist(pg_conn, schema, table, records[0])

    cols = [_sanitize_identifier(k) for k in records[0].keys()]
    insert_stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(map(sql.Identifier, cols)),
        sql.SQL(", ").join(sql.Placeholder() * len(cols)),
    )

    values = []
    for r in records:
        row_vals = [r.get(orig_key) for orig_key in records[0].keys()]
        values.append(row_vals)

    cur = pg_conn.cursor()
    execute_batch(cur, insert_stmt.as_string(pg_conn), values, page_size=500)
    pg_conn.commit()
    cur.close()
    return len(records)


def run_sync_job(
    *,
    job_id: int,
    session_token: str,
    form_id: str,
    target_host: str,
    target_port: int,
    target_db: str,
    target_user: str,
    target_password: str,
    target_schema: str,
    target_table: str,
    sync_mode: str,
    primary_key_field: str | None,
    create_new_table: bool,
) -> dict[str, Any]:
    job = None
    try:
        job = next((j for j in sync_engine.list_sync_jobs() if j.id == job_id), None)

        source = f"surveycto:{form_id}"
        target = f"postgres:{target_schema}.{target_table}"

        last_sync = sync_engine.get_last_sync(source, target)
        since_dt = last_sync.last_synced_at if last_sync else None

        records = surveycto_service.fetch_wide_json_submissions  # for type checkers
        submissions = None

        # Fetch from SurveyCTO
        submissions = (
            __import__("asyncio")
            .get_event_loop()
            .run_until_complete(
                surveycto_service.fetch_wide_json_submissions(session_token, form_id, since_dt)
            )
        )

        # If SurveyCTO returned no new data, succeed gracefully
        if not submissions:
            sync_engine.record_sync_completion(job_id, "completed", None)
            return {
                "jobId": job_id,
                "status": "completed",
                "processedRecords": 0,
                "totalRecords": 0,
                "insertedRecords": 0,
                "updatedRecords": 0,
                "errors": [],
                "startedAt": datetime.now(timezone.utc).isoformat(),
                "completedAt": datetime.now(timezone.utc).isoformat(),
            }

        # Connect Postgres
        pg_conn = psycopg2.connect(
            host=target_host,
            port=target_port,
            dbname=target_db,
            user=target_user,
            password=target_password,
            sslmode="require",
        )

        try:
            if create_new_table:
                _create_table_if_needed(
                    pg_conn,
                    target_schema,
                    target_table,
                    primary_key_field if sync_mode == "upsert" else None,
                    submissions[0],
                )

            inserted = 0
            updated = 0
            if sync_mode == "upsert":
                if not primary_key_field:
                    raise ValueError("primaryKeyField is required for upsert mode.")
                processed, updated = _upsert_records(
                    pg_conn,
                    target_schema,
                    target_table,
                    submissions,
                    primary_key_field,
                )
                inserted = processed  # placeholder
            else:
                inserted = _append_records(pg_conn, target_schema, target_table, submissions)

            # Update last sync based on now (simple) — you can improve to CompletionDate max later
            sync_engine.upsert_last_sync(source, target, datetime.now(timezone.utc))
            sync_engine.record_sync_completion(job_id, "completed", None)

            return {
                "jobId": job_id,
                "status": "completed",
                "processedRecords": len(submissions),
                "totalRecords": len(submissions),
                "insertedRecords": inserted,
                "updatedRecords": updated,
                "errors": [],
                "startedAt": datetime.now(timezone.utc).isoformat(),
                "completedAt": datetime.now(timezone.utc).isoformat(),
            }
        finally:
            pg_conn.close()

    except Exception as exc:
        sync_engine.record_sync_completion(job_id, "failed", str(exc))
        return {
            "jobId": job_id,
            "status": "failed",
            "processedRecords": 0,
            "totalRecords": 0,
            "insertedRecords": 0,
            "updatedRecords": 0,
            "errors": [{"message": str(exc), "timestamp": datetime.now(timezone.utc).isoformat()}],
            "startedAt": datetime.now(timezone.utc).isoformat(),
            "completedAt": datetime.now(timezone.utc).isoformat(),
        }
