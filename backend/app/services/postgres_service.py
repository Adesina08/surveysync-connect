from __future__ import annotations


_SAMPLE_DATABASES = ["surveysync", "analytics"]
_SAMPLE_SCHEMAS = {
    "surveysync": ["public", "staging"],
    "analytics": ["public"],
}
_SAMPLE_TABLES = {
    ("surveysync", "public"): ["responses", "enumerators"],
    ("surveysync", "staging"): ["incoming_responses"],
    ("analytics", "public"): ["survey_metrics"],
}


def list_databases() -> list[str]:
    return _SAMPLE_DATABASES


def list_schemas(database: str) -> list[str]:
    return _SAMPLE_SCHEMAS.get(database, [])


def list_tables(database: str, schema: str) -> list[str]:
    return _SAMPLE_TABLES.get((database, schema), [])
