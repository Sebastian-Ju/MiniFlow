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
