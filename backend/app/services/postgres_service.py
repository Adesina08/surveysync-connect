from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import psycopg2


@dataclass
class PgCredentials:
    host: str
    port: int
    database: str
    username: str
    password: str
    sslmode: str = "disable"


_PG_CREDS: Optional[PgCredentials] = None


def set_credentials(creds: PgCredentials) -> None:
    global _PG_CREDS
    _PG_CREDS = creds


def get_credentials() -> PgCredentials:
    if _PG_CREDS is None:
        raise RuntimeError("Postgres is not connected. Call POST /api/pg/connect first.")
    return _PG_CREDS


def connect() -> psycopg2.extensions.connection:
    creds = get_credentials()
    return psycopg2.connect(
        host=creds.host,
        port=creds.port,
        dbname=creds.database,
        user=creds.username,
        password=creds.password,
        sslmode=creds.sslmode,
    )
