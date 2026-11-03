"""Plan 5 / Phase D: versions, 'Test this block', Run now with input, and run filters."""

from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models.automation import Notification, WorkflowRun, WorkflowVersion

API = "/api/v1/automation"


def _graph(title: str = "hello", x: int = 200) -> dict:
    return {
        "nodes": [
            {"id": "t", "type": "trigger.manual", "config": {}, "position": {"x": 0, "y": 0}},
            {"id": "n", "type": "notify", "config": {"title": title}, "position": {"x": x, "y": 0}},
        ],
        "edges": [{"from": "t", "port": "next", "to": "n"}],
    }


def _create(client: TestClient, headers: dict, graph: dict | None = None) -> dict:
    response = client.post(
        f"{API}/workflows", json={"name": "Builder", "graph": graph or _graph()}, headers=headers
    )
    assert response.status_code == 201, response.text
    return response.json()


# ---------------------------------------------------------------------- versions


def test_versions_are_kept_and_can_be_restored(
    client: TestClient, admin_headers: dict, viewer_headers: dict, db: Session
) -> None:
    wf = _create(client, admin_headers)
    url = f"{API}/workflows/{wf['id']}"

    client.patch(url, json={"graph": _graph("changed")}, headers=admin_headers)
    client.patch(url, json={"graph": _graph("changed", x=400)}, headers=admin_headers)  # layout
    versions = client.get(f"{url}/versions", headers=viewer_headers).json()
    assert [v["version"] for v in versions] == [2, 1]  # moving a block is not a new version
    assert versions[0]["created_by_name"]

    old = client.get(f"{url}/versions/1", headers=viewer_headers).json()
    assert old["graph"]["nodes"][1]["config"]["title"] == "hello"

    assert client.post(f"{url}/versions/1/restore", headers=viewer_headers).status_code == 403
    restored = client.post(f"{url}/versions/1/restore", headers=admin_headers)
    assert restored.status_code == 200, restored.text
    assert restored.json()["version"] == 3
    assert restored.json()["graph"]["nodes"][1]["config"]["title"] == "hello"
    assert [v["version"] for v in client.get(f"{url}/versions", headers=admin_headers).json()] == [
        3,
        2,
        1,
    ]
    assert client.get(f"{url}/versions/9", headers=admin_headers).status_code == 404
    assert db.scalar(select(func.count()).select_from(WorkflowVersion)) == 3


# ---------------------------------------------------------------------- test a block


def test_testing_a_block_shows_the_result_and_keeps_nothing(
    client: TestClient, operator_headers: dict, viewer_headers: dict, db: Session
) -> None:
    graph = {
        "nodes": [
            {"id": "t", "type": "trigger.webhook", "config": {}},
            {
                "id": "if",
                "type": "logic.if",
                "config": {"left": "{{trigger.body.rows}}", "operator": ">", "right": "10"},
            },
            {"id": "n", "type": "notify", "config": {"title": "Rows: {{trigger.body.rows}}"}},
        ],
        "edges": [
            {"from": "t", "port": "next", "to": "if"},
            {"from": "if", "port": "true", "to": "n"},
        ],
    }
    body = {"graph": graph, "node_id": "if", "input": {"trigger": {"body": {"rows": 42}}}}
    result = client.post(f"{API}/workflows/test-node", json=body, headers=operator_headers)
    assert result.status_code == 200, result.text
    assert result.json()["port"] == "true"
    assert result.json()["message"] == "'42' > '10': True"

    body["node_id"] = "n"
    result = client.post(f"{API}/workflows/test-node", json=body, headers=operator_headers).json()
    assert result["port"] == "next"
    assert db.scalar(select(func.count()).select_from(Notification)) == 0  # rolled back
    assert db.scalar(select(func.count()).select_from(WorkflowRun)) == 0

    assert (
        client.post(f"{API}/workflows/test-node", json=body, headers=viewer_headers).status_code
        == 403
    )
    body["node_id"] = "missing"
    assert (
        client.post(f"{API}/workflows/test-node", json=body, headers=operator_headers).status_code
        == 404
    )


def test_testing_an_action_is_a_dry_run(client: TestClient, operator_headers: dict) -> None:
    graph = {
        "nodes": [
            {"id": "t", "type": "trigger.manual", "config": {}},
            {
                "id": "run",
                "type": "pipeline.run_dag",
                "config": {
                    "connection_id": "00000000-0000-0000-0000-000000000000",
                    "dag_id": "etl",
                },
            },
        ],
        "edges": [{"from": "t", "port": "next", "to": "run"}],
    }
    result = client.post(
        f"{API}/workflows/test-node",
        json={"graph": graph, "node_id": "run"},
        headers=operator_headers,
    ).json()
    # The block's connection does not exist: the error is shown, nothing breaks.
    assert result["error"] == "The block's Airflow connection no longer exists"


# ---------------------------------------------------------------------- run now with input


def test_run_now_with_input_and_trigger_filter(
    client: TestClient, admin_headers: dict, operator_headers: dict, db: Session
) -> None:
    wf = _create(client, admin_headers, _graph("Version {{trigger.body.v}}"))
    run = client.post(
        f"{API}/workflows/{wf['id']}/run", json={"input": {"v": "7"}}, headers=operator_headers
    ).json()
    assert run["status"] == "COMPLETED"
    assert [n.title for n in db.scalars(select(Notification))] == ["Version 7"]

    manual = client.get(f"{API}/runs", params={"trigger": "manual"}, headers=operator_headers)
    assert manual.json()["total"] == 1
    other = client.get(f"{API}/runs", params={"trigger": "webhook"}, headers=operator_headers)
    assert other.json()["total"] == 0
