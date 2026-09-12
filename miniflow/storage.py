from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from .models import TaskRecord, TaskStatus, from_timestamp, to_timestamp, utc_now


class SQLiteTaskStore:
    """Durable queue storage with atomic task claiming."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 10000")
        connection.execute("PRAGMA foreign_keys = ON")
        if self.path != ":memory:":
            connection.execute("PRAGMA journal_mode = WAL")
        return connection

    @contextmanager
    def _transaction(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    params_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL CHECK(priority BETWEEN 0 AND 100),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 3,
                    created_at REAL NOT NULL,
                    available_at REAL NOT NULL,
                    started_at REAL,
                    finished_at REAL,
                    worker_id TEXT,
                    result_json TEXT,
                    error TEXT,
                    lease_expires_at REAL
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_claim
                    ON tasks(status, available_at, priority DESC, created_at);
                CREATE INDEX IF NOT EXISTS idx_tasks_created
                    ON tasks(created_at DESC);
                """
            )

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> TaskRecord:
        return TaskRecord(
            id=row["id"],
            name=row["name"],
            params=json.loads(row["params_json"]),
            status=TaskStatus(row["status"]),
            priority=row["priority"],
            attempts=row["attempts"],
            max_retries=row["max_retries"],
            created_at=from_timestamp(row["created_at"]),
            available_at=from_timestamp(row["available_at"]),
            started_at=from_timestamp(row["started_at"]),
            finished_at=from_timestamp(row["finished_at"]),
            worker_id=row["worker_id"],
            result=json.loads(row["result_json"]) if row["result_json"] else None,
            error=row["error"],
            lease_expires_at=from_timestamp(row["lease_expires_at"]),
        )

    def enqueue(
        self,
        name: str,
        params: dict[str, Any],
        *,
        priority: int = 50,
        max_retries: int = 3,
        delay_seconds: float = 0,
    ) -> TaskRecord:
        if not 0 <= priority <= 100:
            raise ValueError("priority must be between 0 and 100")
        if not 0 <= max_retries <= 10:
            raise ValueError("max_retries must be between 0 and 10")
        if not 0 <= delay_seconds <= 31_536_000:
            raise ValueError("delay_seconds must be between 0 and 31536000")

        now = utc_now()
        task_id = uuid4().hex
        available_at = now + timedelta(seconds=delay_seconds)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO tasks (
                    id, name, params_json, status, priority, max_retries,
                    created_at, available_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    name,
                    self._json(params),
                    TaskStatus.QUEUED.value,
                    priority,
                    max_retries,
                    to_timestamp(now),
                    to_timestamp(available_at),
                ),
            )
        record = self.get(task_id)
        assert record is not None
        return record

    def get(self, task_id: str) -> TaskRecord | None:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        return self._row_to_record(row) if row else None

    def list(self, *, status: TaskStatus | None = None, limit: int = 50) -> list[TaskRecord]:
        limit = max(1, min(limit, 200))
        query = "SELECT * FROM tasks"
        params: list[Any] = []
        if status:
            query += " WHERE status = ?"
            params.append(status.value)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [self._row_to_record(row) for row in rows]

    def claim(self, worker_id: str, *, lease_seconds: float = 30) -> TaskRecord | None:
        now = utc_now()
        lease_expires = now + timedelta(seconds=lease_seconds)
        with self._transaction(immediate=True) as connection:
            row = connection.execute(
                """
                SELECT id FROM tasks
                WHERE status IN (?, ?) AND available_at <= ?
                ORDER BY priority DESC, available_at ASC, created_at ASC
                LIMIT 1
                """,
                (
                    TaskStatus.QUEUED.value,
                    TaskStatus.RETRYING.value,
                    to_timestamp(now),
                ),
            ).fetchone()
            if row is None:
                return None
            connection.execute(
                """
                UPDATE tasks
                SET status = ?, worker_id = ?, attempts = attempts + 1,
                    started_at = ?, finished_at = NULL, error = NULL,
                    lease_expires_at = ?
                WHERE id = ?
                """,
                (
                    TaskStatus.RUNNING.value,
                    worker_id,
                    to_timestamp(now),
                    to_timestamp(lease_expires),
                    row["id"],
                ),
            )
            claimed = connection.execute(
                "SELECT * FROM tasks WHERE id = ?", (row["id"],)
            ).fetchone()
        return self._row_to_record(claimed)

    def heartbeat(self, task_id: str, worker_id: str, *, lease_seconds: float = 30) -> bool:
        expires_at = utc_now() + timedelta(seconds=lease_seconds)
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE tasks SET lease_expires_at = ?
                WHERE id = ? AND worker_id = ? AND status = ?
                """,
                (
                    to_timestamp(expires_at),
                    task_id,
                    worker_id,
                    TaskStatus.RUNNING.value,
                ),
            )
        return cursor.rowcount == 1

    def complete(self, task_id: str, worker_id: str, result: Any) -> bool:
        try:
            result_json = self._json(result)
        except (TypeError, ValueError) as exc:
            raise TypeError("Task results must be JSON serializable") from exc
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE tasks
                SET status = ?, result_json = ?, finished_at = ?, lease_expires_at = NULL
                WHERE id = ? AND worker_id = ? AND status = ?
                """,
                (
                    TaskStatus.SUCCEEDED.value,
                    result_json,
                    to_timestamp(utc_now()),
                    task_id,
                    worker_id,
                    TaskStatus.RUNNING.value,
                ),
            )
        return cursor.rowcount == 1

    def fail(
        self,
        task: TaskRecord,
        worker_id: str,
        error: str,
        *,
        retry_base_seconds: float = 1,
    ) -> TaskStatus:
        should_retry = task.attempts <= task.max_retries
        status = TaskStatus.RETRYING if should_retry else TaskStatus.FAILED
        retry_delay = min(retry_base_seconds * (2 ** max(0, task.attempts - 1)), 60)
        available_at = utc_now() + timedelta(seconds=retry_delay)
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE tasks
                SET status = ?, error = ?, available_at = ?,
                    finished_at = ?, lease_expires_at = NULL, worker_id = NULL
                WHERE id = ? AND worker_id = ? AND status = ?
                """,
                (
                    status.value,
                    error[-4000:],
                    to_timestamp(available_at),
                    None if should_retry else to_timestamp(utc_now()),
                    task.id,
                    worker_id,
                    TaskStatus.RUNNING.value,
                ),
            )
        if cursor.rowcount != 1:
            raise RuntimeError(f"Worker {worker_id} no longer owns task {task.id}")
        return status

    def cancel(self, task_id: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE tasks SET status = ?, finished_at = ?
                WHERE id = ? AND status IN (?, ?)
                """,
                (
                    TaskStatus.CANCELLED.value,
                    to_timestamp(utc_now()),
                    task_id,
                    TaskStatus.QUEUED.value,
                    TaskStatus.RETRYING.value,
                ),
            )
        return cursor.rowcount == 1

    def recover_stale(self) -> int:
        """Return tasks abandoned by dead workers to the queue."""
        now = utc_now()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE tasks
                SET status = ?, available_at = ?, worker_id = NULL,
                    lease_expires_at = NULL, error = 'Worker lease expired; task recovered'
                WHERE status = ? AND lease_expires_at < ?
                """,
                (
                    TaskStatus.RETRYING.value,
                    to_timestamp(now),
                    TaskStatus.RUNNING.value,
                    to_timestamp(now),
                ),
            )
        return cursor.rowcount

    def metrics(self) -> dict[str, Any]:
        with self._connect() as connection:
            counts = connection.execute(
                "SELECT status, COUNT(*) AS count FROM tasks GROUP BY status"
            ).fetchall()
            timing = connection.execute(
                """
                SELECT AVG((finished_at - started_at) * 1000.0) AS average_ms
                FROM tasks WHERE status = ? AND started_at IS NOT NULL
                """,
                (TaskStatus.SUCCEEDED.value,),
            ).fetchone()
        by_status = {status.value: 0 for status in TaskStatus}
        by_status.update({row["status"]: row["count"] for row in counts})
        completed = by_status[TaskStatus.SUCCEEDED.value] + by_status[TaskStatus.FAILED.value]
        success_rate = (
            round(by_status[TaskStatus.SUCCEEDED.value] / completed * 100, 1) if completed else None
        )
        return {
            "total": sum(by_status.values()),
            "by_status": by_status,
            "success_rate": success_rate,
            "average_duration_ms": (
                round(timing["average_ms"], 2) if timing["average_ms"] is not None else None
            ),
        }
