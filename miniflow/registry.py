from __future__ import annotations

from collections.abc import Callable
from threading import RLock
from typing import Any

TaskFunction = Callable[..., Any]


class UnknownTaskError(LookupError):
    pass


class TaskRegistry:
    """Thread-safe allow-list of functions workers may execute."""

    def __init__(self) -> None:
        self._functions: dict[str, TaskFunction] = {}
        self._lock = RLock()

    def register(self, name: str, function: TaskFunction) -> None:
        if not name or not name.replace("_", "").replace("-", "").isalnum():
            raise ValueError("Task names may contain letters, numbers, '_' and '-'")
        with self._lock:
            if name in self._functions:
                raise ValueError(f"Task '{name}' is already registered")
            self._functions[name] = function

    def task(self, name: str | None = None) -> Callable[[TaskFunction], TaskFunction]:
        def decorator(function: TaskFunction) -> TaskFunction:
            self.register(name or function.__name__, function)
            return function

        return decorator

    def get(self, name: str) -> TaskFunction:
        with self._lock:
            try:
                return self._functions[name]
            except KeyError as exc:
                raise UnknownTaskError(f"Unknown task type: {name}") from exc

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._functions)
