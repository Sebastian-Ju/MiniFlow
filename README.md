# MiniFlow

MiniFlow is a small, durable background-task system built to explore the engineering ideas
behind Celery, Sidekiq, and cloud job queues. It accepts work through a REST API, stores jobs
in SQLite, executes them concurrently, retries transient failures, and exposes live operational
metrics in a browser dashboard.

![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115%2B-009688)
![Tests](https://img.shields.io/badge/tests-pytest-0A9EDC)

## Why this is more than a CRUD app

- **Atomic claiming:** `BEGIN IMMEDIATE` prevents two workers from executing the same queued job.
- **Crash recovery:** running jobs have renewable leases; abandoned jobs return to the queue.
- **Exponential backoff:** transient failures are retried with increasing delay.
- **Priority and scheduling:** ready jobs are ordered by priority, then scheduled time.
- **Safe dispatch:** workers execute registered functions only, never arbitrary code from API users.
- **Observability:** statuses, errors, duration, attempts, success rate, and queue depth are visible.

## Quick start

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate

pip install -e ".[dev]"
miniflow serve --reload --workers 4
```

Open <http://127.0.0.1:8000> for the dashboard or <http://127.0.0.1:8000/docs> for the
interactive API documentation.

Docker is also supported:

```bash
docker compose up --build
```

## API example

```bash
curl -X POST http://127.0.0.1:8000/api/tasks \
  -H "Content-Type: application/json" \
  -d '{"name":"word_count","params":{"text":"reliable software expects failure"},"priority":80}'
```

The response contains a task ID. Use it to retrieve the execution result:

```bash
curl http://127.0.0.1:8000/api/tasks/TASK_ID
```

## Registering application tasks

MiniFlow's core can also be embedded in another Python application:

```python
from miniflow import MiniFlow

flow = MiniFlow("jobs.db")


@flow.task("resize_image")
def resize_image(path: str, width: int) -> dict:
    # Application-specific work goes here.
    return {"path": path, "width": width}


flow.start_workers(concurrency=4)
task = flow.enqueue("resize_image", {"path": "photo.jpg", "width": 800})
```

## Architecture

```text
Browser / API client
        |
        v
 FastAPI control plane ----> metrics + task inspection
        |
        v
 SQLite durable queue <---- atomic claim + renewable lease
        |
        v
 Worker pool ----> registered Python task ----> result / retry / failure
```

## Reliability model

MiniFlow provides **at-least-once delivery**. A worker can finish external work and crash before
recording success, so the task may later run again. Production tasks should therefore be
idempotent. SQLite makes this project simple to run and inspect; a production-scale design would
usually replace it with a dedicated broker and run workers in isolated processes or containers.

## Tests

```bash
pytest
ruff check .
```

The tests cover execution, scheduling, priority, cancellation, successful recovery from transient
failure, exhausted retries, API validation, and task lookup.

## Roadmap

- Task dependency DAGs
- Server-sent live dashboard events
- Dead-letter queue and replay
- Separate API and worker processes
- PostgreSQL backend
- Per-queue concurrency and rate limits
- OpenTelemetry traces

## License

MIT
