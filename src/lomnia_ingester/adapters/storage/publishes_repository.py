import sqlite3
from pathlib import Path
from typing import Any


class PublishesRepository:
    db: sqlite3.Connection

    def __init__(self, path: Path):
        self.db = sqlite3.connect(
            path / "ingester.db", timeout=5, isolation_level=None, check_same_thread=False
        )
        self.init_db()

    def init_db(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS publishes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),

                plugin_id TEXT NOT NULL,

                bucket TEXT,
                key TEXT,
                was_successful INTEGER
            );
            """)
        self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_publishes_plugin_id
            ON executions (plugin_id);
        """)

        self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_publishes_was_successful
            ON executions (was_successful);
        """)

        self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_publishes_created_at
            ON executions (created_at);
        """)

    def on_succesfull_publish(
        self,
        *,
        plugin_id: str,
        bucket: str,
        key: str,
    ) -> None:
        self.db.execute(
            """
            INSERT INTO publishes (
                plugin_id,
                was_successful,
                bucket,
                key
            )
            VALUES (?, 1, ?, ?)
            """,
            (
                plugin_id,
                bucket,
                key,
            ),
        )

    def on_fail_publish(
        self,
        *,
        plugin_id: str,
        bucket: str,
        key: str,
    ) -> None:
        self.db.execute(
            """
            INSERT INTO publishes (
                plugin_id,
                was_successful,
                bucket,
                key
            )
            VALUES (?, 0, ?, ?)
            """,
            (
                plugin_id,
                bucket,
                key,
            ),
        )

    def get_all_publishes(self) -> list[dict[str, Any]]:
        cursor = self.db.execute(
            """
            SELECT
                id,

                created_at,
                updated_at,

                plugin_id,

                bucket,
                key,
                was_successful
            FROM publishes
            ORDER BY created_at DESC
            """
        )

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_all_failed_publishes(self) -> list[dict[str, Any]]:
        cursor = self.db.execute(
            """
            SELECT
                id,

                created_at,
                updated_at,

                plugin_id,

                bucket,
                key,
                was_successful
            FROM publishes
            WHERE NOT was_successfull
            ORDER BY created_at DESC
            """
        )

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
