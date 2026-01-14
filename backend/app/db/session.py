from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "internal.db"


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def _column_exists(connection: sqlite3.Connection, table: str, column: str) -> bool:
    rows = connection.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == column for r in rows)


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_connection() as connection:
        # Base tables
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                source TEXT NOT NULL,
                target TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_error TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS last_sync (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                target TEXT NOT NULL,
                last_synced_at TEXT NOT NULL,
                UNIQUE(source, target)
            )
            """
        )

        # ---- Migration: add config_json column if missing ----
        # Safe to run on every startup.
        if not _column_exists(connection, "sync_jobs", "config_json"):
            connection.execute("ALTER TABLE sync_jobs ADD COLUMN config_json TEXT")

        connection.commit()
