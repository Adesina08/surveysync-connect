from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field


router = APIRouter(prefix="/api/pg", tags=["postgres"])


# -------------------------
# In-memory "connection"
# -------------------------

@dataclass
class _PgSession:
    connected: bool
    schemas: list[dict]


_PG_SESSION = _PgSession(connected=False, schemas=[])


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

def _ensure_connected() -> None:
    if not _PG_SESSION.connected:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Not connected to database")


def _mock_schemas() -> list[PostgresSchema]:
    """
    Temporary mocked structure until you add a real Postgres driver.
    """
    return [
        PostgresSchema(
            name="public",
            tables=[
                PostgresTable(
                    name="responses",
                    primaryKey="id",
                    rowCount=0,
                    columns=[
                        PostgresColumn(name="id", type="TEXT", nullable=False, isPrimaryKey=True),
                        PostgresColumn(name="created_at", type="TIMESTAMPTZ", nullable=True),
                    ],
                ),
                PostgresTable(
                    name="incoming_responses",
                    primaryKey=None,
                    rowCount=0,
                    columns=[
                        PostgresColumn(name="raw_json", type="JSONB", nullable=True),
                    ],
                ),
            ],
        ),
        PostgresSchema(
            name="staging",
            tables=[
                PostgresTable(
                    name="staging_table",
                    primaryKey=None,
                    rowCount=0,
                    columns=[
                        PostgresColumn(name="id", type="TEXT", nullable=True),
                    ],
                ),
            ],
        ),
    ]


def _find_table(schema_name: str, table_name: str) -> Optional[PostgresTable]:
    for s in _PG_SESSION.schemas:
        if s["name"] == schema_name:
            for t in s["tables"]:
                if t["name"] == table_name:
                    return PostgresTable(**t)
    return None


# -------------------------
# Routes expected by frontend
# -------------------------

@router.post("/connect", response_model=PostgresConnectionResponse)
def connect(credentials: PostgresCredentials) -> PostgresConnectionResponse:
    """
    POST /api/pg/connect
    For now: mocked 'success'. Later: replace with real connection test.
    """
    # Basic input validation (frontend also validates, but keep backend safe)
    if not credentials.host.strip():
        return PostgresConnectionResponse(success=False, error="Host is required")
    if not credentials.database.strip():
        return PostgresConnectionResponse(success=False, error="Database name is required")
    if not credentials.username.strip():
        return PostgresConnectionResponse(success=False, error="Username is required")
    if not credentials.password.strip():
        return PostgresConnectionResponse(success=False, error="Password is required")

    schemas = _mock_schemas()

    # Store "connected" session in memory
    _PG_SESSION.connected = True
    _PG_SESSION.schemas = [s.model_dump() for s in schemas]

    return PostgresConnectionResponse(success=True, schemas=schemas)


@router.get("/schemas", response_model=list[PostgresSchema])
def list_schemas() -> list[PostgresSchema]:
    """
    GET /api/pg/schemas
    """
    _ensure_connected()
    return [PostgresSchema(**s) for s in _PG_SESSION.schemas]


@router.get("/schemas/{schema_name}/tables", response_model=list[PostgresTable])
def list_tables(schema_name: str) -> list[PostgresTable]:
    """
    GET /api/pg/schemas/:schemaName/tables
    """
    _ensure_connected()

    for s in _PG_SESSION.schemas:
        if s["name"] == schema_name:
            return [PostgresTable(**t) for t in s["tables"]]

    # Return empty list if schema doesn't exist (frontend handles it)
    return []


@router.post("/validate-schema", response_model=SchemaCompatibility)
def validate_schema(payload: ValidateSchemaRequest) -> SchemaCompatibility:
    """
    POST /api/pg/validate-schema
    Checks compatibility between SurveyCTO fields and target table.
    (Mocked logic based on in-memory schema.)
    """
    _ensure_connected()

    table = _find_table(payload.targetSchema, payload.targetTable)
    if not table:
        # If table doesn't exist, the schema is "compatible" only if user will create a new one
        return SchemaCompatibility(
            compatible=False,
            missingColumns=[f.name for f in payload.formFields],
            extraColumns=[],
            typeMismatches=[],
            primaryKeyMatch=False,
        )

    form_field_names = {f.name for f in payload.formFields}
    table_col_names = {c.name for c in table.columns}

    missing = sorted(list(form_field_names - table_col_names))
    extra = sorted(list(table_col_names - form_field_names))

    # type mismatch check (very basic / placeholder)
    type_mismatches: list[dict] = []
    col_type_map: Dict[str, str] = {c.name: c.type for c in table.columns}
    for f in payload.formFields:
        if f.name in col_type_map:
            # we can't reliably map without a shared type system yet
            pass

    form_pk = next((f.name for f in payload.formFields if f.isPrimaryKey), None)
    pk_match = (form_pk is not None) and (table.primaryKey == form_pk)

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
    """
    POST /api/pg/tables
    Mock: "creates" a table by adding it to in-memory schemas.
    """
    _ensure_connected()

    if not payload.schemaName.strip():
        return CreateTableResponse(success=False, error="schemaName is required")
    if not payload.tableName.strip():
        return CreateTableResponse(success=False, error="tableName is required")

    # Ensure schema exists in our mock store
    schema = None
    for s in _PG_SESSION.schemas:
        if s["name"] == payload.schemaName:
            schema = s
            break

    if schema is None:
        schema = {"name": payload.schemaName, "tables": []}
        _PG_SESSION.schemas.append(schema)

    # Prevent duplicates
    for t in schema["tables"]:
        if t["name"] == payload.tableName:
            return CreateTableResponse(success=False, error="Table already exists")

    pk = next((c.name for c in payload.columns if c.isPrimaryKey), None)

    schema["tables"].append(
        {
            "name": payload.tableName,
            "columns": [c.model_dump() for c in payload.columns],
            "primaryKey": pk,
            "rowCount": 0,
        }
    )

    return CreateTableResponse(success=True)
