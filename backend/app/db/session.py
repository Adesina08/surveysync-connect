from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

DB_PATH = os.getenv("DATABASE_URL", "app.db").replace("sqlite:///", "")


def _ensure_columns(connection: sqlite3.Connection) -> None:
    """
    Lightweight migrations for SQLite.
    """
    # Add config_json if missing
    try:
        connection.execute("ALTER TABLE sync_jobs ADD COLUMN config_json TEXT")
        connection.commit()
    except sqlite3.OperationalError:
        # Column already exists
        pass


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as connection:
        connection.row_factory = sqlite3.Row
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
                config_json TEXT
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
        connection.commit()

        _ensure_columns(connection)


@contextmanager
def get_connection():
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
    finally:
        connection.close()
