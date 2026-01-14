from __future__ import annotations

from typing import Dict, Optional

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

def _ensure_connected() -> postgres_service.PostgresCredentials:
    try:
        return postgres_session.get_credentials()
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


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

    creds = postgres_service.PostgresCredentials(
        host=credentials.host.strip(),
        port=int(credentials.port),
        database=credentials.database.strip(),
        username=credentials.username.strip(),
        password=credentials.password,
        sslmode=credentials.sslMode,
    )

    try:
        postgres_service.test_connection(creds)
    except postgres_service.PostgresConnectionError as exc:
        return PostgresConnectionResponse(success=False, error=str(exc))

    # ✅ store in shared session (so sync_engine can access it)
    postgres_session.set_credentials(creds)

    # return schemas + tables + columns (frontend expects columns)
    try:
        schemas: list[PostgresSchema] = []
        schema_names = postgres_service.list_schemas(creds)
        for s in schema_names:
            table_names = postgres_service.list_tables(creds, s)
            tables: list[PostgresTable] = []
            for t in table_names:
                cols = postgres_service.get_table_columns(creds, s, t)
                pk = next((c.name for c in cols if c.is_primary_key), None)
                tables.append(
                    PostgresTable(
                        name=t,
                        primaryKey=pk,
                        rowCount=0,
                        columns=[
                            PostgresColumn(
                                name=c.name,
                                type=c.type,
                                nullable=c.nullable,
                                isPrimaryKey=c.is_primary_key,
                            )
                            for c in cols
                        ],
                    )
                )
            schemas.append(PostgresSchema(name=s, tables=tables))

        return PostgresConnectionResponse(success=True, schemas=schemas)
    except Exception as exc:
        return PostgresConnectionResponse(success=False, error=f"Connected, but failed to load schemas: {exc}")


@router.get("/schemas", response_model=list[PostgresSchema])
def list_schemas() -> list[PostgresSchema]:
    creds = _ensure_connected()

    schema_names = postgres_service.list_schemas(creds)
    result: list[PostgresSchema] = []

    for s in schema_names:
        table_names = postgres_service.list_tables(creds, s)
        tables: list[PostgresTable] = []
        for t in table_names:
            cols = postgres_service.get_table_columns(creds, s, t)
            pk = next((c.name for c in cols if c.is_primary_key), None)
            tables.append(
                PostgresTable(
                    name=t,
                    primaryKey=pk,
                    rowCount=0,
                    columns=[
                        PostgresColumn(
                            name=c.name,
                            type=c.type,
                            nullable=c.nullable,
                            isPrimaryKey=c.is_primary_key,
                        )
                        for c in cols
                    ],
                )
            )
        result.append(PostgresSchema(name=s, tables=tables))

    return result


@router.get("/schemas/{schema_name}/tables", response_model=list[PostgresTable])
def list_tables(schema_name: str) -> list[PostgresTable]:
    creds = _ensure_connected()

    table_names = postgres_service.list_tables(creds, schema_name)
    tables: list[PostgresTable] = []
    for t in table_names:
        cols = postgres_service.get_table_columns(creds, schema_name, t)
        pk = next((c.name for c in cols if c.is_primary_key), None)
        tables.append(
            PostgresTable(
                name=t,
                primaryKey=pk,
                rowCount=0,
                columns=[
                    PostgresColumn(
                        name=c.name,
                        type=c.type,
                        nullable=c.nullable,
                        isPrimaryKey=c.is_primary_key,
                    )
                    for c in cols
                ],
            )
        )
    return tables


@router.post("/validate-schema", response_model=SchemaCompatibility)
def validate_schema(payload: ValidateSchemaRequest) -> SchemaCompatibility:
    creds = _ensure_connected()

    try:
        table_names = postgres_service.list_tables(creds, payload.targetSchema)
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

    cols = postgres_service.get_table_columns(creds, payload.targetSchema, payload.targetTable)
    table_col_map: Dict[str, postgres_service.ColumnInfo] = {c.name: c for c in cols}

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
    table_pk = next((c.name for c in cols if c.is_primary_key), None)
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
    creds = _ensure_connected()

    if not payload.schemaName.strip():
        return CreateTableResponse(success=False, error="schemaName is required")
    if not payload.tableName.strip():
        return CreateTableResponse(success=False, error="tableName is required")

    pk = next((c.name for c in payload.columns if c.isPrimaryKey), None)

    try:
        postgres_service.create_table(
            creds=creds,
            schema=payload.schemaName,
            table=payload.tableName,
            columns=[c.model_dump() for c in payload.columns],
            primary_key=pk,
        )
    except postgres_service.PostgresServiceError as exc:
        return CreateTableResponse(success=False, error=str(exc))
    except Exception as exc:
        return CreateTableResponse(success=False, error=f"Failed to create table: {exc}")

    return CreateTableResponse(success=True)
