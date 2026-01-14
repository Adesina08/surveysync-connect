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


@dataclass
class ColumnInfo:
    name: str
    type: str
    nullable: bool
    is_primary_key: bool


def get_table_columns(creds: PostgresCredentials, schema: str, table: str) -> list[ColumnInfo]:
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            # columns
            cur.execute(
                """
                SELECT
                    c.column_name,
                    c.data_type,
                    (c.is_nullable = 'YES') AS is_nullable
                FROM information_schema.columns c
                WHERE c.table_schema = %s
                  AND c.table_name = %s
                ORDER BY c.ordinal_position
                """,
                (schema, table),
            )
            cols = cur.fetchall()

            # primary key cols
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
                    type=data_type,
                    nullable=is_nullable,
                    is_primary_key=(name in pk_cols),
                )
                for (name, data_type, is_nullable) in cols
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
    columns: list of {name, type, nullable, isPrimaryKey}
    primary_key: optional pk column name
    """
    ensure_schema(creds, schema)

    # Build columns DDL
    col_sql: list[sql.SQL] = []
    pk_col: Optional[str] = primary_key

    for c in columns:
        col_name = c["name"]
        col_type = c["type"]
        nullable = bool(c.get("nullable", True))
        is_pk = bool(c.get("isPrimaryKey", False))

        if is_pk and not pk_col:
            pk_col = col_name

        # WARNING: type is injected as SQL; restrict to known types or map from UI
        # For now, we allow common types.
        # In production, you should map SurveyCTO types -> safe postgres types.
        allowed = {
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
        normalized_type = col_type.strip().upper()
        if normalized_type not in allowed:
            normalized_type = "TEXT"

        col_def = sql.SQL("{} {} {}").format(
            sql.Identifier(col_name),
            sql.SQL(normalized_type),
            sql.SQL("NULL" if nullable else "NOT NULL"),
        )
        col_sql.append(col_def)

    if pk_col:
        col_sql.append(sql.SQL("PRIMARY KEY ({})").format(sql.Identifier(pk_col)))

    stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(col_sql),
    )

    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(stmt)
        conn.commit()
    finally:
        conn.close()
