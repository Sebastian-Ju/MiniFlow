from __future__ import annotations

from fastapi.testclient import TestClient

from miniflow.api import create_app


def test_enqueue_and_get_task(tmp_path):
    app = create_app(tmp_path / "api.db", start_workers=False)
    with TestClient(app) as client:
        response = client.post(
            "/api/tasks",
            json={"name": "add", "params": {"a": 2, "b": 5}, "priority": 70},
        )
        assert response.status_code == 202
        task = response.json()
        assert task["status"] == "queued"

        fetched = client.get(f"/api/tasks/{task['id']}")
        assert fetched.status_code == 200
        assert fetched.json()["priority"] == 70


def test_unknown_task_returns_bad_request(tmp_path):
    app = create_app(tmp_path / "api.db", start_workers=False)
    with TestClient(app) as client:
        response = client.post("/api/tasks", json={"name": "unknown", "params": {}})
    assert response.status_code == 400


def test_health_lists_registered_tasks(tmp_path):
    app = create_app(tmp_path / "api.db", start_workers=False)
    with TestClient(app) as client:
        response = client.get("/health")
    assert response.status_code == 200
    assert "word_count" in response.json()["registered_tasks"]


def test_enqueue_dag_and_reject_cycle(tmp_path):
    app = create_app(tmp_path / "dag-api.db", start_workers=False)
    with TestClient(app) as client:
        response = client.post(
            "/api/dags",
            json={
                "nodes": [
                    {"key": "first", "name": "add", "params": {"a": 1, "b": 2}},
                    {
                        "key": "second",
                        "name": "factorial",
                        "params": {"number": 5},
                        "depends_on": ["first"],
                    },
                ]
            },
        )
        assert response.status_code == 202
        assert response.json()["nodes"]["second"]["status"] == "blocked"

        cycle = client.post(
            "/api/dags",
            json={
                "nodes": [
                    {"key": "a", "name": "add", "depends_on": ["b"]},
                    {"key": "b", "name": "add", "depends_on": ["a"]},
                ]
            },
        )
        assert cycle.status_code == 400
        assert "cycle" in cycle.json()["detail"]
