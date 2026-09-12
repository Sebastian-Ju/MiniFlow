from __future__ import annotations

import logging
import threading
import time
import traceback
from collections.abc import Callable

from .registry import TaskRegistry
from .storage import SQLiteTaskStore

logger = logging.getLogger(__name__)


class WorkerPool:
    def __init__(
        self,
        store: SQLiteTaskStore,
        registry: TaskRegistry,
        *,
        concurrency: int = 2,
        poll_interval: float = 0.2,
        lease_seconds: float = 30,
        retry_base_seconds: float = 1,
        worker_prefix: str = "worker",
        on_change: Callable[[], None] | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        self.store = store
        self.registry = registry
        self.concurrency = concurrency
        self.poll_interval = poll_interval
        self.lease_seconds = lease_seconds
        self.retry_base_seconds = retry_base_seconds
        self.worker_prefix = worker_prefix
        self.on_change = on_change
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    @property
    def running(self) -> bool:
        return any(thread.is_alive() for thread in self._threads)

    def start(self) -> None:
        if self.running:
            return
        self._stop.clear()
        self.store.recover_stale()
        self._threads = []
        for index in range(self.concurrency):
            worker_id = f"{self.worker_prefix}-{index + 1}"
            thread = threading.Thread(
                target=self._run,
                args=(worker_id,),
                name=worker_id,
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)
        logger.info("Started %d MiniFlow workers", self.concurrency)

    def stop(self, timeout: float = 5) -> None:
        self._stop.set()
        deadline = time.monotonic() + timeout
        for thread in self._threads:
            thread.join(max(0, deadline - time.monotonic()))
        self._threads.clear()

    def _notify(self) -> None:
        if self.on_change:
            try:
                self.on_change()
            except Exception:
                logger.exception("Worker notification hook failed")

    def _run(self, worker_id: str) -> None:
        last_recovery = 0.0
        while not self._stop.is_set():
            now = time.monotonic()
            if now - last_recovery >= max(1, self.lease_seconds / 2):
                self.store.recover_stale()
                last_recovery = now

            task = self.store.claim(worker_id, lease_seconds=self.lease_seconds)
            if task is None:
                self._stop.wait(self.poll_interval)
                continue

            self._notify()
            heartbeat_stop = threading.Event()
            heartbeat = threading.Thread(
                target=self._heartbeat,
                args=(task.id, worker_id, heartbeat_stop),
                daemon=True,
            )
            heartbeat.start()
            try:
                function = self.registry.get(task.name)
                result = function(**task.params)
                self.store.complete(task.id, worker_id, result)
                logger.info("Task %s succeeded", task.id)
            except Exception:
                error = traceback.format_exc()
                status = self.store.fail(
                    task,
                    worker_id,
                    error,
                    retry_base_seconds=self.retry_base_seconds,
                )
                logger.warning("Task %s ended as %s", task.id, status.value)
            finally:
                heartbeat_stop.set()
                heartbeat.join(timeout=1)
                self._notify()

    def _heartbeat(self, task_id: str, worker_id: str, stop: threading.Event) -> None:
        interval = max(0.1, self.lease_seconds / 3)
        while not stop.wait(interval):
            if not self.store.heartbeat(task_id, worker_id, lease_seconds=self.lease_seconds):
                return
