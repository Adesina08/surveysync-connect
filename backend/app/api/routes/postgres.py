from __future__ import annotations

from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.services import postgres_service

router = APIRouter(prefix="/pg", tags=["postgres"])


class DatabasesResponse(BaseModel):
    databases: list[str]


class SchemasResponse(BaseModel):
    database: str
    schemas: list[str]


class TablesResponse(BaseModel):
    database: str
    schema: str
    tables: list[str]


@router.get("/databases", response_model=DatabasesResponse)
def list_databases() -> DatabasesResponse:
    return DatabasesResponse(databases=postgres_service.list_databases())


@router.get("/schemas", response_model=SchemasResponse)
def list_schemas(database: str = Query(..., description="Database name")) -> SchemasResponse:
    return SchemasResponse(database=database, schemas=postgres_service.list_schemas(database))


@router.get("/tables", response_model=TablesResponse)
def list_tables(
    database: str = Query(..., description="Database name"),
    schema: str = Query(..., description="Schema name"),
) -> TablesResponse:
    return TablesResponse(database=database, schema=schema, tables=postgres_service.list_tables(database, schema))
