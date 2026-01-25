import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, TypedDict


class PluginLastRun(TypedDict):
    last_successful_run: str | None
    next_start_date: str | None


class PluginExecutionState(TypedDict):
    plugins: dict[str, PluginLastRun]


class PluginExecutionRepository:
    # { plugins: { <name>: { last_successful_run: ..., next_start_date: ... } } }
    db: sqlite3.Connection

    def __init__(self, path: Path):
        self.db = sqlite3.connect(
            path / "ingester.db", timeout=5, isolation_level=None, check_same_thread=False
        )
        self.init_db()

    def init_db(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS executions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now')),

                plugin_id TEXT NOT NULL,

                next_start_date TEXT,
                extracted_file TEXT,
                transformed_file TEXT,

                was_successful INTEGER,

                started_at TEXT,
                ended_at TEXT,

                entities_count INTEGER,
                run_reason TEXT
            );
            """)
        self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_executions_plugin_id
            ON executions (plugin_id);
        """)

        self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_executions_was_successful
            ON executions (was_successful);
        """)

        self.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_executions_created_at
            ON executions (created_at);
        """)

    def get_next_start_date(self, plugin_id: str) -> Optional[datetime]:
        row = self.db.execute(
            """
            SELECT next_start_date
            FROM executions
            WHERE plugin_id = ?
            AND next_start_date IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (plugin_id,),
        ).fetchone()

        if row is None:
            return None

        return datetime.fromisoformat(row[0])

    def on_succesfull_run(
        self,
        *,
        plugin_name: str,
        next_start_date: datetime,
        started_at: datetime,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()

        self.db.execute(
            """
            INSERT INTO executions (
                plugin_id,
                was_successful,
                next_start_date,
                ended_at,
                started_at
            )
            VALUES (?, 1, ?, ?, ?)
            """,
            (
                plugin_name,
                next_start_date.isoformat(),
                now,
                started_at,
            ),
        )

    def get_all_executions(self) -> list[dict[str, Any]]:
        cursor = self.db.execute(
            """
            SELECT
                id,
                plugin_id,
                created_at,
                updated_at,
                started_at,
                ended_at,
                next_start_date,
                was_successful,
                entities_count,
                run_reason,
                extracted_file,
                transformed_file
            FROM executions
            ORDER BY created_at DESC
            """
        )

        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
