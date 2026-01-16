from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Optional

import psycopg
from psycopg import sql


class PostgresServiceError(Exception):
    pass


class PostgresConnectionError(PostgresServiceError):
    pass


class PostgresQueryError(PostgresServiceError):
    pass


@dataclass
class PostgresCredentials:
    host: str
    port: int
    database: str
    username: str
    password: str
    sslmode: str = "disable"  # disable | prefer | require


@dataclass
class ColumnInfo:
    name: str
    type: str
    nullable: bool
    is_primary_key: bool = False


def _connect(creds: PostgresCredentials) -> psycopg.Connection:
    try:
        conn = psycopg.connect(
            host=creds.host,
            port=creds.port,
            dbname=creds.database,
            user=creds.username,
            password=creds.password,
            sslmode=creds.sslmode,
            connect_timeout=10,
        )
        return conn
    except Exception as exc:
        raise PostgresConnectionError(f"Unable to connect to Postgres: {exc}") from exc


def test_connection(creds: PostgresCredentials) -> None:
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    finally:
        conn.close()


def list_schemas(creds: PostgresCredentials) -> list[str]:
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
                ORDER BY schema_name
                """
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def list_tables(creds: PostgresCredentials, schema: str) -> list[str]:
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """,
                (schema,),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def get_table_columns(creds: PostgresCredentials, schema: str, table: str) -> list[ColumnInfo]:
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    c.column_name,
                    c.udt_name,
                    (c.is_nullable = 'YES') AS is_nullable
                FROM information_schema.columns c
                WHERE c.table_schema = %s
                  AND c.table_name = %s
                ORDER BY c.ordinal_position
                """,
                (schema, table),
            )
            cols = cur.fetchall()

            cur.execute(
                """
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema = %s
                  AND tc.table_name = %s
                """,
                (schema, table),
            )
            pk_cols = {r[0] for r in cur.fetchall()}

            return [
                ColumnInfo(
                    name=name,
                    type=_normalize_pg_type(udt_name),
                    nullable=bool(is_nullable),
                    is_primary_key=(name in pk_cols),
                )
                for (name, udt_name, is_nullable) in cols
            ]
    finally:
        conn.close()


def ensure_schema(creds: PostgresCredentials, schema: str) -> None:
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        conn.commit()
    finally:
        conn.close()


def create_table(
    creds: PostgresCredentials,
    schema: str,
    table: str,
    columns: list[dict[str, Any]],
    primary_key: Optional[str] = None,
) -> None:
    """
    Create table safely with quoted identifiers.
    columns elements: {name, type, nullable, isPrimaryKey}
    """
    ensure_schema(creds, schema)

    col_defs: list[sql.SQL] = []
    pk_col = primary_key

    for c in columns:
        col_name = str(c["name"])
        col_type = str(c.get("type") or "TEXT")
        nullable = bool(c.get("nullable", True))
        is_pk = bool(c.get("isPrimaryKey", False))

        if is_pk and not pk_col:
            pk_col = col_name

        pg_type = _coerce_allowed_type(col_type)

        col_defs.append(
            sql.SQL("{} {} {}").format(
                sql.Identifier(col_name),
                sql.SQL(pg_type),
                sql.SQL("NULL" if nullable else "NOT NULL"),
            )
        )

    if pk_col:
        col_defs.append(sql.SQL("PRIMARY KEY ({})").format(sql.Identifier(pk_col)))

    stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(col_defs),
    )

    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(stmt)
        conn.commit()
    finally:
        conn.close()


def delete_all_rows(creds: PostgresCredentials, schema: str, table: str) -> None:
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("DELETE FROM {}.{}").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                )
            )
        conn.commit()
    finally:
        conn.close()


def insert_rows(
    creds: PostgresCredentials,
    schema: str,
    table: str,
    columns: list[str],
    rows: Iterable[dict[str, Any]],
) -> int:
    if not columns:
        return 0
    data = [tuple(row.get(col) for col in columns) for row in rows]
    if not data:
        return 0
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            stmt = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            )
            cur.executemany(stmt, data)
        conn.commit()
    finally:
        conn.close()
    return len(data)


def upsert_rows(
    creds: PostgresCredentials,
    schema: str,
    table: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    conflict_column: str,
) -> tuple[int, int]:
    if not columns:
        return 0, 0
    if conflict_column not in columns:
        raise PostgresQueryError(f"Primary key column '{conflict_column}' is not in target columns.")

    keys = [row.get(conflict_column) for row in rows if row.get(conflict_column) is not None]
    existing_keys = _fetch_existing_keys(creds, schema, table, conflict_column, keys)

    inserted = 0
    updated = 0
    for row in rows:
        key = row.get(conflict_column)
        if key is None or key not in existing_keys:
            inserted += 1
        else:
            updated += 1

    data = [tuple(row.get(col) for col in columns) for row in rows]
    if not data:
        return 0, 0

    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            update_cols = [c for c in columns if c != conflict_column]
            if update_cols:
                update_stmt = sql.SQL(", ").join(
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                    for c in update_cols
                )
                stmt = sql.SQL(
                    "INSERT INTO {}.{} ({}) VALUES ({}) "
                    "ON CONFLICT ({}) DO UPDATE SET {}"
                ).format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                    sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                    sql.Identifier(conflict_column),
                    update_stmt,
                )
            else:
                stmt = sql.SQL(
                    "INSERT INTO {}.{} ({}) VALUES ({}) "
                    "ON CONFLICT ({}) DO NOTHING"
                ).format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.SQL(", ").join(sql.Identifier(c) for c in columns),
                    sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                    sql.Identifier(conflict_column),
                )
            cur.executemany(stmt, data)
        conn.commit()
    finally:
        conn.close()

    return inserted, updated


# -----------------------
# Type helpers
# -----------------------

_ALLOWED_TYPES = {
    "TEXT",
    "INTEGER",
    "BIGINT",
    "DOUBLE PRECISION",
    "NUMERIC",
    "BOOLEAN",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "JSONB",
}


def _coerce_allowed_type(t: str) -> str:
    """
    Prevent SQL injection via types: only allow known safe types.
    Unknown types => TEXT.
    """
    normalized = t.strip().upper()
    if normalized == "TIMESTAMP WITH TIME ZONE":
        normalized = "TIMESTAMPTZ"
    if normalized == "TIMESTAMP WITHOUT TIME ZONE":
        normalized = "TIMESTAMP"
    if normalized not in _ALLOWED_TYPES:
        return "TEXT"
    return normalized


def _normalize_pg_type(udt_name: str) -> str:
    """
    Convert postgres UDT names to friendly types the UI uses.
    """
    m = {
        "text": "TEXT",
        "varchar": "TEXT",
        "bpchar": "TEXT",
        "int2": "INTEGER",
        "int4": "INTEGER",
        "int8": "BIGINT",
        "float4": "DOUBLE PRECISION",
        "float8": "DOUBLE PRECISION",
        "numeric": "NUMERIC",
        "bool": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "timestamptz": "TIMESTAMPTZ",
        "json": "JSONB",
        "jsonb": "JSONB",
    }
    return m.get((udt_name or "").lower(), "TEXT")


def _fetch_existing_keys(
    creds: PostgresCredentials,
    schema: str,
    table: str,
    column: str,
    keys: list[Any],
) -> set[Any]:
    if not keys:
        return set()
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            stmt = sql.SQL("SELECT {} FROM {}.{} WHERE {} = ANY(%s)").format(
                sql.Identifier(column),
                sql.Identifier(schema),
                sql.Identifier(table),
                sql.Identifier(column),
            )
            cur.execute(stmt, (keys,))
            return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()
