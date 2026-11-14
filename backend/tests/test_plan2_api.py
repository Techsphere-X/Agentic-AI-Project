"""Plan 2 / Phase 4: automation API, RBAC, and the approval flow through HTTP."""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.automation.templates import ALL_TEMPLATES
from app.models.audit_log import AuditLog
from app.orchestration.airflow import mock as mock_module
from app.services import automation_service
from tests.test_plan2_engine import Clock

API = "/api/v1/automation"
CONNS = "/api/v1/airflow/connections"


@pytest.fixture
def partner_clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    clock = Clock(600, 4)
    monkeypatch.setattr(mock_module, "clock", clock)
    return clock


@pytest.fixture
def workflows(db: Session) -> None:
    """The built-in templates, seeded as on first start (disabled)."""
    automation_service.seed_templates(db)


def by_key(client: TestClient, headers: dict) -> dict[str, dict]:
    response = client.get(f"{API}/workflows", headers=headers)
    assert response.status_code == 200, response.text
    return {w["key"]: w for w in response.json()}


def prod_partner(client: TestClient, admin_headers: dict) -> dict:
    conn = client.post(
        CONNS,
        json={
            "name": "mock-prod",
            "environment": "PROD",
            "kind": "MOCK",
            "base_url": "http://mock-airflow",
            "auth_type": "NONE",
        },
        headers=admin_headers,
    ).json()
    client.post(f"{CONNS}/{conn['id']}/sync-dags", headers=admin_headers)
    dags = client.get(f"{CONNS}/{conn['id']}/dags", headers=admin_headers).json()["items"]
    partner = next(d for d in dags if d["dag_id"] == "partner_api_sync")
    client.patch(
        f"/api/v1/airflow/dags/{partner['id']}", json={"is_monitored": True}, headers=admin_headers
    )
    return conn


def test_catalog_and_templates_are_readable(client: TestClient, viewer_headers: dict) -> None:
    types = client.get(f"{API}/node-types", headers=viewer_headers).json()
    assert {t["type"] for t in types} >= {"trigger.incident", "approval.request", "notify"}
    # The editor groups, colours and marks blocks from these fields, so the API must send them.
    by_type = {t["type"]: t for t in types}
    assert by_type["flow.wait"]["stage"] == "decide"
    assert by_type["flow.wait"]["summary"] == "Waits, then continues."
    assert by_type["database.check"]["port_tones"] == {"pass": "good", "fail": "bad"}
    assert by_type["pipeline.run_dag"]["is_action"] is True
    assert "diagnosis.label" in by_type["diagnose.classify_log"]["outputs"]
    templates = client.get(f"{API}/templates", headers=viewer_headers).json()
    assert [t["key"] for t in templates] == [t.key for t in ALL_TEMPLATES]
    parameterized = next(t for t in templates if t["key"] == "run-verify-dag")
    assert parameterized["version"] == 1
    assert "connection_id" in parameterized["parameter_schema"]["properties"]


def test_workflow_rbac_and_validation(
    client: TestClient,
    workflows: None,
    admin_headers: dict,
    operator_headers: dict,
    viewer_headers: dict,
    db: Session,
) -> None:
    wf = by_key(client, viewer_headers)["retry-transient"]
    assert wf["enabled"] is False and wf["version"] == 1
    url = f"{API}/workflows/{wf['id']}"

    for headers in (viewer_headers, operator_headers):
        assert client.patch(url, json={"enabled": True}, headers=headers).status_code == 403
        assert (
            client.post(f"{API}/workflows", json={"template_key": "sla-rerun"}, headers=headers)
        ).status_code == 403

    response = client.patch(url, json={"enabled": True, "mode": "DRY_RUN"}, headers=admin_headers)
    assert response.status_code == 200
    assert (response.json()["enabled"], response.json()["mode"]) == (True, "DRY_RUN")

    bad = client.patch(
        url,
        json={"graph": {"nodes": [{"id": "t", "type": "shell.exec"}], "edges": []}},
        headers=admin_headers,
    )
    assert bad.status_code == 422
    assert bad.json()["error"]["code"] == "invalid_graph"
    assert bad.json()["error"]["details"]["problems"][0]["node"] == "t"

    created = client.post(
        f"{API}/workflows",
        json={"template_key": "sla-rerun", "name": "Copy"},
        headers=admin_headers,
    )
    assert created.status_code == 201 and created.json()["key"] is None
    assert (
        client.post(
            f"{API}/workflows", json={"template_key": "nope"}, headers=admin_headers
        ).status_code
        == 404
    )

    invalid = client.post(
        f"{API}/workflows",
        json={"template_key": "run-verify-dag", "template_parameters": {}},
        headers=admin_headers,
    )
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "invalid_template_parameters"

    preview = client.post(
        f"{API}/templates/run-verify-dag/preview",
        json={
            "template_parameters": {
                "connection_id": "airflow-1",
                "dag_id": "orders_pipeline",
            }
        },
        headers=viewer_headers,
    )
    assert preview.status_code == 200
    assert preview.json()["valid"]
    assert {node["type"] for node in preview.json()["graph"]["nodes"]} >= {
        "pipeline.run_dag",
        "verify.run_success",
    }

    created_template = client.post(
        f"{API}/workflows",
        json={
            "template_key": "run-verify-dag",
            "template_parameters": {
                "connection_id": "airflow-1",
                "dag_id": "orders_pipeline",
            },
        },
        headers=admin_headers,
    )
    assert created_template.status_code == 201
    created_body = created_template.json()
    assert created_body["template_key"] == "run-verify-dag"
    assert created_body["template_version"] == 1
    assert created_body["template_parameters"]["dag_id"] == "orders_pipeline"
    assert created_body["graph"]["nodes"][1]["config"]["dag_id"] == "orders_pipeline"

    actions = set(db.scalars(select(AuditLog.action)).all())
    assert {"workflow.update", "workflow.create", "workflow.seed"} <= actions


def test_prod_approval_flow_over_http(
    client: TestClient,
    workflows: None,
    partner_clock: Clock,
    admin_headers: dict,
    operator_headers: dict,
    viewer_headers: dict,
) -> None:
    prod_partner(client, admin_headers)
    wf = by_key(client, admin_headers)["retry-transient"]
    client.patch(f"{API}/workflows/{wf['id']}", json={"enabled": True}, headers=admin_headers)

    summary = client.post("/api/v1/detection/run", headers=operator_headers).json()
    assert summary["opened"] == 1 and summary["automation_queued"] == 1
    assert summary["automation"]["waiting"] == 1

    counts = client.get(f"{API}/summary", headers=viewer_headers).json()
    assert counts["pending_approvals"] == 1 and counts["active_runs"] == 1
    assert counts["unread_notifications"] == 1 and counts["enabled_workflows"] == 1

    approvals = client.get(f"{API}/approvals", params={"status": "PENDING"}, headers=viewer_headers)
    approval = approvals.json()["items"][0]
    assert approval["workflow_name"] == wf["name"]
    assert approval["incident"]["dag_id"] == "partner_api_sync"
    assert approval["proposed_action"]["environment"] == "PROD"

    incident_id = approval["incident_id"]
    for_incident = client.get(
        f"{API}/approvals", params={"incident_id": incident_id}, headers=viewer_headers
    ).json()
    assert [a["id"] for a in for_incident["items"]] == [approval["id"]]
    detail = client.get(f"/api/v1/incidents/{incident_id}", headers=viewer_headers).json()
    assert detail["diagnosis"]["category"] == "TRANSIENT_NETWORK"
    assert "approval_requested" in [e["event"] for e in detail["events"]]

    # Viewers can't decide; operators can.
    decide = f"{API}/approvals/{approval['id']}/approve"
    assert client.post(decide, json={}, headers=viewer_headers).status_code == 403
    response = client.post(decide, json={"comment": "  ok  "}, headers=operator_headers)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "APPROVED" and body["comment"] == "ok"
    assert body["decided_by_name"] == "Operator User"
    assert client.post(decide, json={}, headers=operator_headers).status_code == 409

    runs = client.get(f"{API}/runs", params={"incident_id": incident_id}, headers=viewer_headers)
    run = runs.json()["items"][0]
    assert run["status"] == "WAITING" and run["current_node"] == "verify"
    run_detail = client.get(f"{API}/runs/{run['id']}", headers=viewer_headers).json()
    assert [s["node_id"] for s in run_detail["steps"]][-2:] == ["retry", "verify"]
    assert run_detail["approvals"][0]["status"] == "APPROVED"

    # Cancel: viewer forbidden, operator allowed, then 409.
    cancel = f"{API}/runs/{run['id']}/cancel"
    assert client.post(cancel, headers=viewer_headers).status_code == 403
    cancelled = client.post(cancel, headers=operator_headers)
    assert cancelled.status_code == 200 and cancelled.json()["status"] == "CANCELLED"
    assert client.post(cancel, headers=operator_headers).status_code == 409


def test_notifications_and_tick(
    client: TestClient, workflows: None, operator_headers: dict, viewer_headers: dict
) -> None:
    assert client.post(f"{API}/tick", headers=viewer_headers).status_code == 403
    tick = client.post(f"{API}/tick", headers=operator_headers)
    assert tick.status_code == 200 and tick.json()["advanced"] == 0

    page = client.get(f"{API}/notifications", headers=viewer_headers).json()
    assert page["total"] == 0
    read = client.post(f"{API}/notifications/read-all", headers=viewer_headers)
    assert read.status_code == 200 and read.json() == {"updated": 0}


def test_run_filters(client: TestClient, viewer_headers: dict) -> None:
    response = client.get(
        f"{API}/runs", params={"status": ["WAITING", "PENDING"]}, headers=viewer_headers
    )
    assert response.status_code == 200 and response.json()["total"] == 0
    assert (
        client.get(f"{API}/runs", params={"status": "BOGUS"}, headers=viewer_headers).status_code
        == 422
    )
