"""Plan 3 / Phase 1: canvas support in the backend (positions, validate, delete, monitor blocks)."""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.automation.graph import GraphError, to_raw, validate_graph
from app.core.exceptions import ConflictError
from app.detection.types import IncidentType
from app.models.airflow import DeploymentEnvironment
from app.models.audit_log import AuditLog
from app.models.automation import Workflow
from app.models.incident import Incident
from app.models.user import User
from app.orchestration.airflow import mock as mock_module
from app.services import automation_service
from tests.test_plan2_engine import Clock, detect, make_env

API = "/api/v1/automation"


def graph(**positions: dict) -> dict:
    return {
        "nodes": [
            {"id": "t", "type": "trigger.incident", "config": {}, "position": positions.get("t")},
            {"id": "n", "type": "notify", "config": {}, "position": positions.get("n")},
        ],
        "edges": [{"from": "t", "port": "next", "to": "n"}],
    }


# ---------------------------------------------------------------------- positions


def test_positions_round_trip() -> None:
    raw = to_raw(validate_graph(graph(t={"x": 10, "y": 20.26}, n=None)))
    assert raw["nodes"][0]["position"] == {"x": 10.0, "y": 20.3}
    assert "position" not in raw["nodes"][1]


@pytest.mark.parametrize(
    "position",
    ["left", {"x": 1}, {"x": "a", "y": 1}, {"x": 1, "y": float("inf")}, {"x": 1e9, "y": 0}],
)
def test_bad_positions_are_rejected(position: object) -> None:
    with pytest.raises(GraphError) as exc:
        validate_graph(graph(t=position))
    assert exc.value.problems[0]["node"] == "t"
    assert "position" in exc.value.problems[0]["message"]


def test_layout_only_changes_keep_the_version(db: Session, admin: User) -> None:
    workflow = automation_service.create_workflow(db, actor=admin, name="W", graph=graph())
    automation_service.update_workflow(
        db, workflow.id, {"graph": graph(t={"x": 5, "y": 5})}, actor=admin
    )
    assert workflow.version == 1 and workflow.graph["nodes"][0]["position"] == {"x": 5.0, "y": 5.0}
    changed = graph(t={"x": 5, "y": 5})
    changed["nodes"][1]["config"] = {"level": "WARNING"}
    automation_service.update_workflow(db, workflow.id, {"graph": changed}, actor=admin)
    assert workflow.version == 2


# ---------------------------------------------------------------------- validate & delete


def test_validate_endpoint_never_saves(
    client: TestClient, viewer_headers: dict, db: Session
) -> None:
    ok = client.post(f"{API}/workflows/validate", json={"graph": graph()}, headers=viewer_headers)
    assert ok.status_code == 200
    body = ok.json()
    assert body["valid"] and body["problems"] == [] and body["warnings"] == []
    assert body["graph"]["nodes"][0]["config"] == {"events": ["opened"], "incident_types": []}

    unknown = graph()
    unknown["nodes"][1]["config"] = {"message": "{{diagnosis.label}}"}  # nothing classifies first
    warned = client.post(
        f"{API}/workflows/validate", json={"graph": unknown}, headers=viewer_headers
    ).json()
    assert warned["valid"]  # warnings never block saving
    assert warned["warnings"][0]["node"] == "n"
    assert "{{diagnosis.label}}" in warned["warnings"][0]["message"]

    broken = graph()
    broken["edges"] = []
    bad = client.post(f"{API}/workflows/validate", json={"graph": broken}, headers=viewer_headers)
    assert bad.status_code == 200
    assert not bad.json()["valid"] and bad.json()["problems"][0]["node"] == "n"
    assert db.scalars(select(Workflow)).all() == []


def test_delete_workflow(
    client: TestClient, admin_headers: dict, operator_headers: dict, db: Session
) -> None:
    created = client.post(
        f"{API}/workflows", json={"name": "Temp", "graph": graph()}, headers=admin_headers
    ).json()
    url = f"{API}/workflows/{created['id']}"
    assert client.delete(url, headers=operator_headers).status_code == 403
    assert client.delete(url, headers=admin_headers).status_code == 204
    assert client.get(url, headers=admin_headers).status_code == 404
    assert "workflow.delete" in db.scalars(select(AuditLog.action)).all()


def test_workflow_with_runs_cannot_be_deleted(
    db: Session, admin: User, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = Clock(600, 4)
    monkeypatch.setattr(mock_module, "clock", clock)
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    workflow = automation_service.create_workflow(db, actor=admin, name="W", graph=graph())
    workflow.enabled = True
    db.commit()
    assert detect(db, clock).automation_queued == 1
    db.commit()
    with pytest.raises(ConflictError) as exc:
        automation_service.delete_workflow(db, workflow.id, actor=admin)
    assert exc.value.code == "workflow_has_runs"


# ---------------------------------------------------------------------- monitor blocks


def test_failure_monitor_off_keeps_sla_monitor(
    db: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = Clock(300, 3)  # orders_pipeline just failed
    monkeypatch.setattr(mock_module, "clock", clock)
    _, dags = make_env(
        db,
        DeploymentEnvironment.DEV,
        "orders_pipeline",
        "legacy_inventory_sync",
        sla={"legacy_inventory_sync": 60},
    )
    dags["orders_pipeline"].detect_failures = False
    dags["legacy_inventory_sync"].detect_failures = False  # SLA-only DAG
    db.commit()
    summary = detect(db, clock)
    incidents = db.scalars(select(Incident)).all()
    assert [(i.dag_id, i.type) for i in incidents] == [
        ("legacy_inventory_sync", IncidentType.SLA_MISSED)
    ]
    assert summary.opened == 1

    dags["orders_pipeline"].detect_failures = True
    dags["orders_pipeline"].detection_watermark = None
    db.commit()
    assert detect(db, clock).opened == 1  # the failure monitor is back


def test_dag_patch_and_open_counts(
    client: TestClient,
    admin_headers: dict,
    operator_headers: dict,
    viewer_headers: dict,
    db: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mock_module, "clock", Clock(300, 3))
    conn = client.post(
        "/api/v1/airflow/connections",
        json={"name": "m", "kind": "MOCK", "base_url": "http://mock-airflow", "auth_type": "NONE"},
        headers=admin_headers,
    ).json()
    client.post(f"/api/v1/airflow/connections/{conn['id']}/sync-dags", headers=admin_headers)
    dags = client.get(
        f"/api/v1/airflow/connections/{conn['id']}/dags", headers=admin_headers
    ).json()["items"]
    orders = next(d for d in dags if d["dag_id"] == "orders_pipeline")
    assert orders["detect_failures"] is True

    url = f"/api/v1/airflow/dags/{orders['id']}"
    assert (
        client.patch(url, json={"detect_failures": False}, headers=viewer_headers).status_code
        == 403
    )
    patched = client.patch(
        url, json={"is_monitored": True, "detect_failures": False}, headers=operator_headers
    ).json()
    assert (patched["is_monitored"], patched["detect_failures"]) == (True, False)
    assert "monitored_dag.failure_monitor" in db.scalars(select(AuditLog.action)).all()

    client.patch(url, json={"detect_failures": True}, headers=operator_headers)
    client.post("/api/v1/detection/run", headers=operator_headers)
    counts = client.get("/api/v1/incidents/open-counts", headers=viewer_headers).json()
    assert counts == [
        {
            "connection_id": conn["id"],
            "dag_id": "orders_pipeline",
            "type": "DAG_RUN_FAILED",
            "count": 1,
        }
    ]
