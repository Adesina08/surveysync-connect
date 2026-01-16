from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.services import postgres_service


@dataclass
class PgSession:
    connected: bool = False
    creds: Optional[postgres_service.PgCredentials] = None


_PG_SESSION = PgSession()


def set_credentials(creds: postgres_service.PgCredentials) -> None:
    _PG_SESSION.connected = True
    _PG_SESSION.creds = creds


def clear_credentials() -> None:
    _PG_SESSION.connected = False
    _PG_SESSION.creds = None


def get_credentials() -> postgres_service.PgCredentials:
    if not _PG_SESSION.connected or not _PG_SESSION.creds:
        raise RuntimeError("Postgres is not connected")
    return _PG_SESSION.creds
