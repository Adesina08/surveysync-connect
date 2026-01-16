from __future__ import annotations

from typing import Dict, Optional

import psycopg2
from psycopg2 import sql
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from app.services import postgres_service, postgres_session

router = APIRouter(prefix="/api/pg", tags=["postgres"])


# -------------------------
# Pydantic models (match frontend types.ts)
# -------------------------

class PostgresCredentials(BaseModel):
    host: str
    port: int = 5432
    database: str
    username: str
    password: str
    sslMode: str = Field(default="disable", pattern="^(require|prefer|disable)$")


class PostgresColumn(BaseModel):
    name: str
    type: str
    nullable: bool = True
    isPrimaryKey: bool = False


class PostgresTable(BaseModel):
    name: str
    columns: list[PostgresColumn]
    primaryKey: Optional[str] = None
    rowCount: int = 0


class PostgresSchema(BaseModel):
    name: str
    tables: list[PostgresTable]


class PostgresConnectionResponse(BaseModel):
    success: bool
    schemas: Optional[list[PostgresSchema]] = None
    error: Optional[str] = None


class SurveyCTOField(BaseModel):
    name: str
    type: str
    label: str
    isPrimaryKey: bool = False


class SchemaCompatibility(BaseModel):
    compatible: bool
    missingColumns: list[str]
    extraColumns: list[str]
    typeMismatches: list[dict]
    primaryKeyMatch: bool


class ValidateSchemaRequest(BaseModel):
    formFields: list[SurveyCTOField]
    targetSchema: str
    targetTable: str


class CreateTableRequest(BaseModel):
    schemaName: str
    tableName: str
    columns: list[PostgresColumn]


class CreateTableResponse(BaseModel):
    success: bool
    error: Optional[str] = None


# -------------------------
# Helpers
# -------------------------

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
    normalized = t.strip().upper()
    if normalized == "TIMESTAMP WITH TIME ZONE":
        normalized = "TIMESTAMPTZ"
    if normalized == "TIMESTAMP WITHOUT TIME ZONE":
        normalized = "TIMESTAMP"
    if normalized not in _ALLOWED_TYPES:
        return "TEXT"
    return normalized


def _normalize_pg_type(udt_name: str) -> str:
    mapping = {
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
    return mapping.get((udt_name or "").lower(), "TEXT")


def _ensure_connected() -> None:
    try:
        postgres_service.get_credentials()
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


def _connect():
    _ensure_connected()
    try:
        return postgres_service.connect()
    except psycopg2.Error as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


def _fetch_schemas(conn) -> list[str]:
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


def _fetch_tables(conn, schema: str) -> list[str]:
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


def _fetch_columns(conn, schema: str, table: str) -> list[PostgresColumn]:
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
        PostgresColumn(
            name=name,
            type=_normalize_pg_type(udt_name),
            nullable=bool(is_nullable),
            isPrimaryKey=name in pk_cols,
        )
        for (name, udt_name, is_nullable) in cols
    ]


def _map_surveycto_type_to_pg(field_type: str) -> str:
    t = (field_type or "").strip().lower()
    if t in {"integer", "int"}:
        return "INTEGER"
    if t in {"decimal", "double", "float", "numeric"}:
        return "NUMERIC"
    if t in {"date"}:
        return "DATE"
    if t in {"datetime", "timestamp"}:
        return "TIMESTAMPTZ"
    if t in {"boolean", "bool"}:
        return "BOOLEAN"
    return "TEXT"


# -------------------------
# Routes
# -------------------------

@router.post("/connect", response_model=PostgresConnectionResponse)
def connect(credentials: PostgresCredentials) -> PostgresConnectionResponse:
    if not credentials.host.strip():
        return PostgresConnectionResponse(success=False, error="Host is required")
    if not credentials.database.strip():
        return PostgresConnectionResponse(success=False, error="Database name is required")
    if not credentials.username.strip():
        return PostgresConnectionResponse(success=False, error="Username is required")
    if not credentials.password.strip():
        return PostgresConnectionResponse(success=False, error="Password is required")

    creds = postgres_service.PgCredentials(
        host=credentials.host.strip(),
        port=int(credentials.port),
        database=credentials.database.strip(),
        username=credentials.username.strip(),
        password=credentials.password,
        sslmode=credentials.sslMode,
    )

    try:
        conn = psycopg2.connect(
            host=creds.host,
            port=creds.port,
            dbname=creds.database,
            user=creds.username,
            password=creds.password,
            sslmode=creds.sslmode,
            connect_timeout=10,
        )
    except psycopg2.Error as exc:
        return PostgresConnectionResponse(success=False, error=str(exc))

    postgres_service.set_credentials(creds)
    postgres_session.set_credentials(creds)

    try:
        schemas: list[PostgresSchema] = []
        schema_names = _fetch_schemas(conn)
        for s in schema_names:
            table_names = _fetch_tables(conn, s)
            tables: list[PostgresTable] = []
            for t in table_names:
                cols = _fetch_columns(conn, s, t)
                pk = next((c.name for c in cols if c.isPrimaryKey), None)
                tables.append(
                    PostgresTable(
                        name=t,
                        primaryKey=pk,
                        rowCount=0,
                        columns=cols,
                    )
                )
            schemas.append(PostgresSchema(name=s, tables=tables))

        return PostgresConnectionResponse(success=True, schemas=schemas)
    except Exception as exc:
        return PostgresConnectionResponse(success=False, error=f"Connected, but failed to load schemas: {exc}")
    finally:
        conn.close()


@router.get("/schemas", response_model=list[PostgresSchema])
def list_schemas() -> list[PostgresSchema]:
    conn = _connect()
    try:
        schema_names = _fetch_schemas(conn)
        result: list[PostgresSchema] = []

        for s in schema_names:
            table_names = _fetch_tables(conn, s)
            tables: list[PostgresTable] = []
            for t in table_names:
                cols = _fetch_columns(conn, s, t)
                pk = next((c.name for c in cols if c.isPrimaryKey), None)
                tables.append(
                    PostgresTable(
                        name=t,
                        primaryKey=pk,
                        rowCount=0,
                        columns=cols,
                    )
                )
            result.append(PostgresSchema(name=s, tables=tables))

        return result
    finally:
        conn.close()


@router.get("/schemas/{schema_name}/tables", response_model=list[PostgresTable])
def list_tables(schema_name: str) -> list[PostgresTable]:
    conn = _connect()
    try:
        table_names = _fetch_tables(conn, schema_name)
        tables: list[PostgresTable] = []
        for t in table_names:
            cols = _fetch_columns(conn, schema_name, t)
            pk = next((c.name for c in cols if c.isPrimaryKey), None)
            tables.append(
                PostgresTable(
                    name=t,
                    primaryKey=pk,
                    rowCount=0,
                    columns=cols,
                )
            )
        return tables
    finally:
        conn.close()


@router.post("/validate-schema", response_model=SchemaCompatibility)
def validate_schema(payload: ValidateSchemaRequest) -> SchemaCompatibility:
    conn = _connect()

    try:
        table_names = _fetch_tables(conn, payload.targetSchema)
        if payload.targetTable not in table_names:
            return SchemaCompatibility(
                compatible=False,
                missingColumns=[f.name for f in payload.formFields],
                extraColumns=[],
                typeMismatches=[],
                primaryKeyMatch=False,
            )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to validate schema: {exc}") from exc

    cols = _fetch_columns(conn, payload.targetSchema, payload.targetTable)
    table_col_map: Dict[str, PostgresColumn] = {c.name: c for c in cols}

    form_names = {f.name for f in payload.formFields}
    table_names_set = set(table_col_map.keys())

    missing = sorted(list(form_names - table_names_set))
    extra = sorted(list(table_names_set - form_names))

    type_mismatches: list[dict] = []
    for f in payload.formFields:
        if f.name in table_col_map:
            expected = _map_surveycto_type_to_pg(f.type)
            actual = table_col_map[f.name].type
            if expected != actual and not (expected == "NUMERIC" and actual in {"INTEGER", "BIGINT", "NUMERIC"}):
                type_mismatches.append({"field": f.name, "expected": expected, "actual": actual})

    form_pk = next((f.name for f in payload.formFields if f.isPrimaryKey), None)
    table_pk = next((c.name for c in cols if c.isPrimaryKey), None)
    pk_match = (form_pk is not None) and (table_pk == form_pk)

    compatible = (len(missing) == 0) and (len(type_mismatches) == 0)

    return SchemaCompatibility(
        compatible=compatible,
        missingColumns=missing,
        extraColumns=extra,
        typeMismatches=type_mismatches,
        primaryKeyMatch=pk_match,
    )


@router.post("/tables", response_model=CreateTableResponse)
def create_table(payload: CreateTableRequest) -> CreateTableResponse:
    if not payload.schemaName.strip():
        return CreateTableResponse(success=False, error="schemaName is required")
    if not payload.tableName.strip():
        return CreateTableResponse(success=False, error="tableName is required")

    pk = next((c.name for c in payload.columns if c.isPrimaryKey), None)

    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
                        .format(sql.Identifier(payload.schemaName)))

            col_defs: list[sql.SQL] = []
            for c in payload.columns:
                col_type = _coerce_allowed_type(c.type)
                col_defs.append(
                    sql.SQL("{} {} {}").format(
                        sql.Identifier(c.name),
                        sql.SQL(col_type),
                        sql.SQL("NULL" if c.nullable else "NOT NULL"),
                    )
                )

            if pk:
                col_defs.append(sql.SQL("PRIMARY KEY ({})").format(sql.Identifier(pk)))

            stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                sql.Identifier(payload.schemaName),
                sql.Identifier(payload.tableName),
                sql.SQL(", ").join(col_defs),
            )
            cur.execute(stmt)
        conn.commit()
    except Exception as exc:
        return CreateTableResponse(success=False, error=f"Failed to create table: {exc}")
    finally:
        conn.close()

    return CreateTableResponse(success=True)
