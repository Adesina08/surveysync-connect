from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class LastSyncMetadata:
    id: int
    source: str
    target: str
    last_synced_at: datetime
