from __future__ import annotations

import time
from pathlib import Path

import pytest

from miniflow import MiniFlow, TaskStatus
from miniflow.dag import InvalidDAGError, topological_order


def wait_for_terminal(flow: MiniFlow, task_id: str, timeout: float = 3):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        task = flow.get(task_id)
        if task and task.status in {TaskStatus.SUCCEEDED, TaskStatus.FAILED}:
            return task
        time.sleep(0.01)
    raise AssertionError(f"Task {task_id} did not finish")


@pytest.fixture
def flow(tmp_path: Path):
    instance = MiniFlow(tmp_path / "test.db")
    yield instance
    instance.stop_workers()


def test_worker_executes_task(flow: MiniFlow):
    @flow.task("multiply")
    def multiply(a: int, b: int) -> int:
        return a * b

    flow.start_workers(2, poll_interval=0.01)
    submitted = flow.enqueue("multiply", {"a": 6, "b": 7})
    finished = wait_for_terminal(flow, submitted.id)

    assert finished.status == TaskStatus.SUCCEEDED
    assert finished.result == 42
    assert finished.attempts == 1


def test_failed_task_retries_then_succeeds(flow: MiniFlow):
    calls = 0

    @flow.task("flaky")
    def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RuntimeError("temporary problem")
        return "recovered"

    flow.start_workers(1, poll_interval=0.005, retry_base_seconds=0.01)
    submitted = flow.enqueue("flaky", max_retries=2)
    finished = wait_for_terminal(flow, submitted.id)

    assert finished.status == TaskStatus.SUCCEEDED
    assert finished.result == "recovered"
    assert finished.attempts == 3


def test_exhausted_retries_mark_task_failed(flow: MiniFlow):
    @flow.task("broken")
    def broken() -> None:
        raise ValueError("permanent problem")

    flow.start_workers(1, poll_interval=0.005, retry_base_seconds=0.01)
    submitted = flow.enqueue("broken", max_retries=1)
    finished = wait_for_terminal(flow, submitted.id)

    assert finished.status == TaskStatus.FAILED
    assert finished.attempts == 2
    assert "permanent problem" in finished.error


def test_scheduled_task_waits_until_available(flow: MiniFlow):
    @flow.task("ping")
    def ping() -> str:
        return "pong"

    flow.start_workers(1, poll_interval=0.005)
    submitted = flow.enqueue("ping", delay_seconds=0.2)
    time.sleep(0.05)
    assert flow.get(submitted.id).status == TaskStatus.QUEUED

    finished = wait_for_terminal(flow, submitted.id)
    assert finished.result == "pong"


def test_priority_controls_claim_order(flow: MiniFlow):
    @flow.task("identity")
    def identity(value: str) -> str:
        return value

    low = flow.enqueue("identity", {"value": "low"}, priority=1)
    high = flow.enqueue("identity", {"value": "high"}, priority=100)

    first = flow.store.claim("test-worker")
    assert first.id == high.id
    flow.store.complete(first.id, "test-worker", "high")
    second = flow.store.claim("test-worker")
    assert second.id == low.id


def test_queued_task_can_be_cancelled(flow: MiniFlow):
    @flow.task("noop")
    def noop() -> None:
        return None

    submitted = flow.enqueue("noop")
    assert flow.store.cancel(submitted.id)
    assert flow.get(submitted.id).status == TaskStatus.CANCELLED


def test_unknown_task_is_rejected(flow: MiniFlow):
    with pytest.raises(LookupError, match="Unknown task"):
        flow.enqueue("does_not_exist")


def test_dependency_dag_runs_in_topological_order(flow: MiniFlow):
    execution_order: list[str] = []

    @flow.task("record")
    def record(value: str) -> str:
        execution_order.append(value)
        return value

    records = flow.enqueue_dag(
        [
            {"key": "root", "name": "record", "params": {"value": "root"}},
            {
                "key": "left",
                "name": "record",
                "params": {"value": "left"},
                "depends_on": ["root"],
            },
            {
                "key": "right",
                "name": "record",
                "params": {"value": "right"},
                "depends_on": ["root"],
            },
            {
                "key": "join",
                "name": "record",
                "params": {"value": "join"},
                "depends_on": ["left", "right"],
            },
        ]
    )
    assert records["join"].status == TaskStatus.BLOCKED
    assert set(records["join"].depends_on) == {records["left"].id, records["right"].id}

    flow.start_workers(2, poll_interval=0.005)
    finished = wait_for_terminal(flow, records["join"].id)

    assert finished.status == TaskStatus.SUCCEEDED
    assert execution_order[0] == "root"
    assert execution_order[-1] == "join"


def test_failed_dependency_cancels_descendant(flow: MiniFlow):
    @flow.task("fail")
    def fail() -> None:
        raise RuntimeError("upstream failed")

    @flow.task("child")
    def child() -> str:
        return "should not run"

    parent = flow.enqueue("fail", max_retries=0)
    descendant = flow.enqueue("child", depends_on=[parent.id])
    flow.start_workers(1, poll_interval=0.005, retry_base_seconds=0.01)
    wait_for_terminal(flow, parent.id)

    deadline = time.monotonic() + 2
    while time.monotonic() < deadline and flow.get(descendant.id).status == TaskStatus.BLOCKED:
        time.sleep(0.01)
    cancelled = flow.get(descendant.id)
    assert cancelled.status == TaskStatus.CANCELLED
    assert cancelled.attempts == 0


def test_topological_order_rejects_cycles():
    with pytest.raises(InvalidDAGError, match="cycle"):
        topological_order({"a": ["b"], "b": ["a"]})
