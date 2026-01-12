from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class SyncJob:
    id: int
    name: str
    source: str
    target: str
    status: str
    created_at: datetime
    updated_at: datetime
    last_error: str | None
