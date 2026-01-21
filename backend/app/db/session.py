from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "internal.db"


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == column for r in rows)


def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_connection() as connection:
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
                last_error TEXT,
                config_json TEXT DEFAULT '{}'
            )
            """
        )

        # Add config_json column if missing (migration)
        if not _column_exists(connection, "sync_jobs", "config_json"):
            connection.execute("ALTER TABLE sync_jobs ADD COLUMN config_json TEXT DEFAULT '{}'")

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

        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS sync_progress (
                job_id INTEGER PRIMARY KEY,
                status TEXT NOT NULL,
                processed_records INTEGER DEFAULT 0,
                total_records INTEGER DEFAULT 0,
                inserted_records INTEGER DEFAULT 0,
                updated_records INTEGER DEFAULT 0,
                errors_json TEXT DEFAULT '[]',
                started_at TEXT,
                completed_at TEXT
            )
            """
        )

        # SurveyCTO API cooldowns (HTTP 417 asks clients to wait before retrying)
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS surveycto_cooldowns (
                source TEXT PRIMARY KEY,
                cooldown_until TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        connection.commit()
