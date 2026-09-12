# MiniFlow Resume and Interview Guide

Use this document to learn the project, not as a script to memorize word for word. Every claim
below maps to code or a test in this repository.

## Resume entry

**MiniFlow — Durable Background Task Queue** | Python, FastAPI, SQLite, JavaScript, Docker

- Built a durable background-task queue with a FastAPI control plane, SQLite WAL storage, a
  concurrent worker pool, priority scheduling, delayed execution, and exponential-backoff retries.
- Implemented atomic task claiming and renewable worker leases to prevent duplicate concurrent
  execution and recover jobs abandoned after worker crashes.
- Added dependency-DAG workflows using Kahn's topological-sort algorithm, including fan-out/fan-in
  execution, cycle validation, blocked-task release, and downstream failure propagation.
- Developed a live operations dashboard and REST API for task submission, metrics, cancellation,
  and execution inspection; containerized the service and added 14 automated tests with CI.

For a one-page resume, use the strongest three bullets. Keep the fourth only if space permits.

## 30-second introduction

> MiniFlow is a small background-job system inspired by Celery and cloud queues. Clients submit
> registered Python tasks through a REST API, SQLite stores them durably, and concurrent workers
> claim and execute them. I focused on reliability problems that a normal CRUD app does not have:
> atomic claiming, retries, worker leases, crash recovery, and dependency DAGs. I also built a web
> dashboard so I could inspect queue state and execution results in real time.

## 90-second walkthrough

> A request first reaches the FastAPI control plane, which validates the task name and parameters.
> The broker writes a queued task to SQLite. Workers poll the store and use a `BEGIN IMMEDIATE`
> transaction to choose and update one task atomically, so two threads cannot claim the same job.
> While a task runs, its worker renews a lease. If the process dies and the lease expires, another
> worker moves the abandoned task back into retrying state. Exceptions are stored with a bounded
> traceback and retried using exponential backoff.
>
> In version 0.2 I added dependency workflows. The API accepts named nodes and edges, validates the
> graph with Kahn's algorithm, and rejects cycles before enqueueing anything. Root tasks are queued
> immediately; descendants stay blocked until all parents succeed. If a parent permanently fails,
> its descendants are cancelled without execution. The included demo is a fan-out/fan-in graph, and
> tests verify ordering, cycle rejection, and failure propagation.

## Architecture map

| Layer | Responsibility | Main code |
|---|---|---|
| API | Validation, HTTP endpoints, dashboard hosting | `miniflow/api.py` |
| Broker | Public Python API and orchestration | `miniflow/broker.py` |
| DAG validator | Topological ordering and cycle detection | `miniflow/dag.py` |
| Store | Schema, transactions, claiming, state transitions | `miniflow/storage.py` |
| Workers | Polling, execution, heartbeat, retry handling | `miniflow/worker.py` |
| Registry | Safe mapping from names to Python functions | `miniflow/registry.py` |
| Dashboard | Submission and observability UI | `miniflow/static/` |

The most important path to understand is:

`POST /api/tasks` -> `MiniFlow.enqueue` -> `SQLiteTaskStore.enqueue` ->
`WorkerPool._run` -> `SQLiteTaskStore.claim` -> registered function -> `complete` or `fail`.

## Common interview questions

### Why SQLite?

SQLite makes the project easy to clone and run while still providing transactions, persistence,
indexes, and WAL mode. It is not the ideal broker for a large distributed deployment. A production
version would use PostgreSQL with `FOR UPDATE SKIP LOCKED`, or a purpose-built broker such as Redis
or RabbitMQ, and separate worker processes.

### How do you stop two workers from running the same task?

`claim()` opens a `BEGIN IMMEDIATE` transaction, selects the highest-priority ready task, and marks
it running before committing. SQLite permits only one writer at a time, so another worker cannot
interleave a competing claim. The guarantee is against concurrent claims, not exactly-once delivery.

### Is delivery exactly once?

No. MiniFlow provides at-least-once delivery. A worker might finish an external side effect and die
before saving success; after the lease expires, the task may run again. Task functions that affect
external systems should therefore be idempotent or use an idempotency key.

### What is the worker lease for?

It is a time-limited ownership record. A heartbeat extends `lease_expires_at` while a worker runs.
If the worker disappears, `recover_stale()` returns expired tasks to retrying state so work is not
stuck forever.

### How do retries work?

After an exception, the attempt count is compared with `max_retries`. Eligible tasks move to
`retrying`, and their next availability is delayed by `base * 2^(attempt-1)`, capped at 60 seconds.
After retries are exhausted, the task becomes permanently failed.

### Why use a registry instead of importing a function name from the request?

Executing arbitrary user-supplied module paths would be a remote-code-execution risk. The registry
exposes only functions the application explicitly registered and also lets the API reject unknown
tasks before enqueueing them.

### How does DAG validation work?

Kahn's algorithm counts each node's incoming dependencies. It repeatedly removes zero-indegree
nodes and decreases the indegree of their children. If nodes remain after the queue is empty, those
nodes participate in a cycle. Complexity is `O(V + E)` for vertices and dependency edges.

### How are dependent tasks released?

They begin in `blocked` state. Inside the same immediate transaction used for claiming, the store
first cancels blocked tasks with failed or cancelled parents, then changes tasks whose parents all
succeeded to `queued`. Only queued or retrying tasks are eligible for a claim.

### What happens when one DAG branch fails?

After the failing task exhausts retries, it becomes `failed`. The next dependency-resolution pass
cancels every directly blocked child. Cancellation then propagates through later passes, so no
descendant executes with incomplete prerequisites.

### Why not pass a parent's result automatically to its child?

The current DAG models execution dependencies, not dataflow. Keeping parameters explicit makes
serialization and retry behavior predictable. A next version could support templated references
such as `${parent.result.digest}`, with schema validation before a child is released.

### What would you improve next?

Good options are a dead-letter queue with replay, server-sent events instead of dashboard polling,
separate API and worker processes, per-queue rate limits, PostgreSQL support, and OpenTelemetry
traces. Pick one and explain its trade-offs rather than listing everything.

### How did you test concurrency and reliability?

The suite covers normal execution, retry recovery, exhausted retries, delayed jobs, priority claim
order, cancellation, API validation, DAG fan-out/fan-in ordering, cycle rejection, and failure
propagation. A stronger next test would start multiple processes and inject crashes at specific
points around the claim and completion transactions.

## Live demo plan

1. Start the server with `miniflow serve --reload --workers 4`.
2. Open the dashboard and submit `add` with `{"a": 21, "b": 21}`.
3. Click **Queue demo DAG** and point out the root, two branches, and final two-dependency join.
4. Open a task detail and show status, attempts, result, timestamps, worker ID, and dependency IDs.
5. Open `/docs` and show the generated API contract for `/api/tasks` and `/api/dags`.
6. Run `pytest -q` and explain one DAG test instead of only showing a green result.

## Before putting it on your resume

You should be able to do these without reading an answer:

- Draw the architecture and trace one task from submission to completion.
- Explain every task state: blocked, queued, running, retrying, succeeded, failed, and cancelled.
- Explain `BEGIN IMMEDIATE`, WAL, a lease, exponential backoff, idempotency, and at-least-once delivery.
- Write a small registered task and call both APIs.
- Change a DAG test, predict whether it passes, and explain why.
- Name two limitations honestly and propose realistic improvements.

If asked about development process, be truthful: describe AI as a coding assistant used for
scaffolding, review, and iteration, then demonstrate ownership by explaining design decisions,
tests, trade-offs, and code changes in your own words.
