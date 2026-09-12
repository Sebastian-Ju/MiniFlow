from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .broker import MiniFlow
from .dag import InvalidDAGError
from .models import TaskStatus
from .registry import UnknownTaskError
from .tasks import register_builtin_tasks


class EnqueueRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    params: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=50, ge=0, le=100)
    max_retries: int = Field(default=3, ge=0, le=10)
    delay_seconds: float = Field(default=0, ge=0, le=31_536_000)
    depends_on: list[str] = Field(default_factory=list, max_length=50)


class DAGNodeRequest(EnqueueRequest):
    key: str = Field(min_length=1, max_length=50, pattern=r"^[A-Za-z0-9_-]+$")
    depends_on: list[str] = Field(default_factory=list, max_length=50)


class DAGRequest(BaseModel):
    nodes: list[DAGNodeRequest] = Field(min_length=1, max_length=50)


def create_app(
    database: str | Path | None = None,
    *,
    concurrency: int | None = None,
    start_workers: bool = True,
) -> FastAPI:
    database = database or os.getenv("MINIFLOW_DB", "miniflow.db")
    concurrency = concurrency or int(os.getenv("MINIFLOW_WORKERS", "2"))
    flow = MiniFlow(database)
    register_builtin_tasks(flow)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        if start_workers:
            flow.start_workers(concurrency)
        yield
        flow.stop_workers()

    app = FastAPI(
        title="MiniFlow",
        version="0.2.0",
        description="A durable task queue with retries, scheduling and live observability.",
        lifespan=lifespan,
    )
    app.state.flow = flow

    static_dir = Path(__file__).parent / "static"
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

    @app.get("/", include_in_schema=False)
    def dashboard() -> FileResponse:
        return FileResponse(static_dir / "index.html")

    @app.get("/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "workers_running": bool(flow.workers and flow.workers.running),
            "registered_tasks": flow.registry.names(),
        }

    @app.post("/api/tasks", status_code=202)
    def enqueue_task(request: EnqueueRequest) -> dict[str, Any]:
        try:
            task = flow.enqueue(
                request.name,
                request.params,
                priority=request.priority,
                max_retries=request.max_retries,
                delay_seconds=request.delay_seconds,
                depends_on=request.depends_on,
            )
        except (UnknownTaskError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return task.to_dict()

    @app.post("/api/dags", status_code=202)
    def enqueue_dag(request: DAGRequest) -> dict[str, Any]:
        try:
            records = flow.enqueue_dag([node.model_dump() for node in request.nodes])
        except (InvalidDAGError, UnknownTaskError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "dag_id": uuid4().hex,
            "nodes": {key: task.to_dict() for key, task in records.items()},
        }

    @app.get("/api/tasks")
    def list_tasks(
        status: TaskStatus | None = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ) -> list[dict[str, Any]]:
        return [task.to_dict() for task in flow.list(status=status, limit=limit)]

    @app.get("/api/tasks/{task_id}")
    def get_task(task_id: str) -> dict[str, Any]:
        task = flow.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="Task not found")
        return task.to_dict()

    @app.delete("/api/tasks/{task_id}")
    def cancel_task(task_id: str) -> dict[str, Any]:
        task = flow.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="Task not found")
        if not flow.store.cancel(task_id):
            raise HTTPException(status_code=409, detail="Only pending tasks can be cancelled")
        return {"id": task_id, "status": TaskStatus.CANCELLED.value}

    @app.get("/api/metrics")
    def metrics() -> dict[str, Any]:
        data = flow.store.metrics()
        data["workers"] = concurrency if start_workers else 0
        data["registered_tasks"] = flow.registry.names()
        return data

    return app


app = create_app()
