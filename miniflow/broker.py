from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from .models import TaskRecord, TaskStatus
from .registry import TaskFunction, TaskRegistry
from .storage import SQLiteTaskStore
from .worker import WorkerPool


class MiniFlow:
    """High-level facade used by the API and by Python applications."""

    def __init__(self, database: str | Path = "miniflow.db") -> None:
        self.store = SQLiteTaskStore(database)
        self.registry = TaskRegistry()
        self.workers: WorkerPool | None = None

    def task(self, name: str | None = None) -> Callable[[TaskFunction], TaskFunction]:
        return self.registry.task(name)

    def enqueue(
        self,
        name: str,
        params: dict[str, Any] | None = None,
        *,
        priority: int = 50,
        max_retries: int = 3,
        delay_seconds: float = 0,
    ) -> TaskRecord:
        self.registry.get(name)
        return self.store.enqueue(
            name,
            params or {},
            priority=priority,
            max_retries=max_retries,
            delay_seconds=delay_seconds,
        )

    def start_workers(
        self,
        concurrency: int = 2,
        *,
        poll_interval: float = 0.2,
        lease_seconds: float = 30,
        retry_base_seconds: float = 1,
    ) -> WorkerPool:
        if self.workers and self.workers.running:
            return self.workers
        self.workers = WorkerPool(
            self.store,
            self.registry,
            concurrency=concurrency,
            poll_interval=poll_interval,
            lease_seconds=lease_seconds,
            retry_base_seconds=retry_base_seconds,
        )
        self.workers.start()
        return self.workers

    def stop_workers(self) -> None:
        if self.workers:
            self.workers.stop()

    def get(self, task_id: str) -> TaskRecord | None:
        return self.store.get(task_id)

    def list(self, status: TaskStatus | None = None, limit: int = 50) -> list[TaskRecord]:
        return self.store.list(status=status, limit=limit)
