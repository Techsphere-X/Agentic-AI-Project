"""Plan 1 / Phase 4: incident + detection API, state machine, RBAC, and the E2E mock flow."""

from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.audit_log import AuditLog
from app.orchestration.airflow import mock as mock_module
from app.services import detection_service

CONNS = "/api/v1/airflow/connections"


class MockClock:
    """Mock-Airflow time aligned to 5-minute buckets near real time."""

    def __init__(self) -> None:
        current = int(datetime.now(UTC).timestamp()) // 300
        # Latest bucket b <= current with b % 3 == 1: run b is running, run b-1 failed.
        self.bucket = current - ((current - 1) % 3)
        self.seconds_into_bucket = 30

    def __call__(self) -> datetime:
        return datetime.fromtimestamp(self.bucket * 300 + self.seconds_into_bucket, UTC)

    def advance_one_run(self) -> None:
        self.bucket += 1  # previous "running" run is now a terminal success


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> MockClock:
    c = MockClock()
    monkeypatch.setattr(mock_module, "clock", c)
    return c


@pytest.fixture
def setup(client: TestClient, admin_headers: dict, clock: MockClock) -> dict:
    conn = client.post(
        CONNS,
        json={
            "name": "mock",
            "kind": "MOCK",
            "base_url": "http://mock-airflow",
            "auth_type": "NONE",
        },
        headers=admin_headers,
    ).json()
    client.post(f"{CONNS}/{conn['id']}/sync-dags", headers=admin_headers)
    dags = {
        d["dag_id"]: d
        for d in client.get(f"{CONNS}/{conn['id']}/dags", headers=admin_headers).json()["items"]
    }
    client.patch(
        f"/api/v1/airflow/dags/{dags['orders_pipeline']['id']}",
        json={"is_monitored": True},
        headers=admin_headers,
    )
    r = client.patch(
        f"/api/v1/airflow/dags/{dags['legacy_inventory_sync']['id']}",
        json={"is_monitored": True, "sla_minutes": 60},
        headers=admin_headers,
    )
    assert r.json()["sla_minutes"] == 60
    return {"conn": conn, "dags": dags}


def run_detection(client: TestClient, headers: dict) -> dict:
    response = client.post("/api/v1/detection/run", headers=headers)
    assert response.status_code == 200, response.text
    return response.json()


def by_dag(client: TestClient, headers: dict, **query: str) -> dict[str, dict]:
    items = client.get("/api/v1/incidents", params=query, headers=headers).json()["items"]
    return {i["dag_id"]: i for i in items}


# ---------------------------------------------------------------------- E2E flow


def test_end_to_end_mock_flow(
    client: TestClient,
    setup: dict,
    clock: MockClock,
    operator_headers: dict,
    viewer_headers: dict,
    db: Session,
) -> None:
    summary = run_detection(client, operator_headers)
    assert (summary["opened"], summary["errors"]) == (2, [])

    incidents = by_dag(client, viewer_headers)
    failed, sla = incidents["orders_pipeline"], incidents["legacy_inventory_sync"]
    assert failed["type"] == "DAG_RUN_FAILED" and failed["severity"] == "CRITICAL"
    assert sla["type"] == "SLA_MISSED" and sla["severity"] == "LOW"

    detail = client.get(f"/api/v1/incidents/{failed['id']}", headers=viewer_headers).json()
    log = next(e for e in detail["evidence"] if e["kind"] == "TASK_LOG")
    assert "UniqueViolation" in log["content"] and "hunter2" not in log["content"]
    assert [e["event"] for e in detail["events"]] == ["opened", "evidence_added", "diagnosed"]

    summary_counts = client.get("/api/v1/incidents/summary", headers=viewer_headers).json()
    assert summary_counts["open_total"] == 2
    assert summary_counts["by_severity"]["CRITICAL"] == 1

    # Viewer is read-only.
    for action in ("acknowledge", "reopen"):
        r = client.post(f"/api/v1/incidents/{failed['id']}/{action}", headers=viewer_headers)
        assert r.status_code == 403
    assert client.post("/api/v1/detection/run", headers=viewer_headers).status_code == 403

    ack = client.post(f"/api/v1/incidents/{failed['id']}/acknowledge", headers=operator_headers)
    assert ack.json()["status"] == "ACKNOWLEDGED"
    assert ack.json()["events"][-1]["actor_name"] == "Operator User"

    # Next run of orders_pipeline succeeds -> auto-resolved.
    clock.advance_one_run()
    second = run_detection(client, operator_headers)
    assert second["auto_resolved"] == 1
    resolved = client.get(f"/api/v1/incidents/{failed['id']}", headers=viewer_headers).json()
    assert resolved["status"] == "RESOLVED" and resolved["resolution"] == "AUTO_RECOVERED"
    assert resolved["events"][-1]["actor_name"] == "System"

    # The SLA incident is still open (legacy never succeeds): resolve manually with a note.
    r = client.post(
        f"/api/v1/incidents/{sla['id']}/resolve",
        json={"note": "DAG is deprecated; SLA removed next."},
        headers=operator_headers,
    )
    assert r.json()["resolution"] == "MANUAL"
    assert client.get("/api/v1/incidents/summary", headers=viewer_headers).json()["open_total"] == 0

    actions = set(db.scalars(select(AuditLog.action)).all())
    assert {
        "incident.opened",
        "incident.acknowledged",
        "incident.auto_resolved",
        "incident.resolved",
        "detection.cycle",
        "monitored_dag.sla_update",
    } <= actions


# ---------------------------------------------------------------------- state machine


def test_invalid_transitions_and_note_required(
    client: TestClient, setup: dict, operator_headers: dict
) -> None:
    run_detection(client, operator_headers)
    incident = by_dag(client, operator_headers)["legacy_inventory_sync"]
    url = f"/api/v1/incidents/{incident['id']}"

    assert client.post(f"{url}/reopen", headers=operator_headers).status_code == 409
    assert (
        client.post(f"{url}/resolve", json={"note": ""}, headers=operator_headers).status_code
        == 422
    )
    client.post(f"{url}/resolve", json={"note": "done"}, headers=operator_headers)
    conflict = client.post(f"{url}/acknowledge", headers=operator_headers)
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "invalid_transition"

    reopened = client.post(f"{url}/reopen", headers=operator_headers).json()
    assert reopened["status"] == "OPEN" and reopened["resolution"] is None


def test_reopen_conflicts_with_newer_open_incident(
    client: TestClient, setup: dict, operator_headers: dict
) -> None:
    run_detection(client, operator_headers)
    old = by_dag(client, operator_headers)["legacy_inventory_sync"]
    client.post(
        f"/api/v1/incidents/{old['id']}/resolve", json={"note": "x"}, headers=operator_headers
    )
    run_detection(client, operator_headers)  # still breached -> a new incident opens
    r = client.post(f"/api/v1/incidents/{old['id']}/reopen", headers=operator_headers)
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "duplicate_open_incident"


# ---------------------------------------------------------------------- filters, status, validation


def test_filters_and_paging(client: TestClient, setup: dict, operator_headers: dict) -> None:
    run_detection(client, operator_headers)
    base = "/api/v1/incidents"
    get = lambda **q: client.get(base, params=q, headers=operator_headers).json()  # noqa: E731
    assert get(type="SLA_MISSED")["total"] == 1
    assert get(severity="CRITICAL")["total"] == 1
    assert get(status="RESOLVED")["total"] == 0
    assert get(status=["OPEN", "ACKNOWLEDGED"])["total"] == 2
    assert get(search="orders")["total"] == 1
    assert get(dag_id="legacy_inventory_sync")["total"] == 1
    assert get(connection_id=setup["conn"]["id"], limit=1)["total"] == 2
    assert len(get(limit=1)["items"]) == 1


def test_detection_status_and_busy(
    client: TestClient, setup: dict, operator_headers: dict, viewer_headers: dict, db: Session
) -> None:
    status = client.get("/api/v1/detection/status", headers=viewer_headers).json()
    assert status["last_cycle"] is None and status["loop_running"] is False

    run_detection(client, operator_headers)
    status = client.get("/api/v1/detection/status", headers=viewer_headers).json()
    assert status["last_cycle"]["opened"] == 2
    assert status["lease"]["holder"] == status["this_worker"]

    # Another worker holds the lease -> manual run is rejected.
    lease = detection_service.get_lease(db)
    lease.holder = "other-worker"
    db.commit()
    busy = client.post("/api/v1/detection/run", headers=operator_headers)
    assert busy.status_code == 409
    assert busy.json()["error"]["code"] == "detection_busy"


def test_sla_validation_and_clearing(
    client: TestClient, setup: dict, operator_headers: dict
) -> None:
    dag_id = setup["dags"]["quality_checks"]["id"]
    url = f"/api/v1/airflow/dags/{dag_id}"
    assert (
        client.patch(url, json={"sla_minutes": 1}, headers=operator_headers).json()["sla_minutes"]
        == 1
    )
    assert (
        client.patch(url, json={"sla_minutes": 30}, headers=operator_headers).json()["sla_minutes"]
        == 30
    )
    cleared = client.patch(url, json={"sla_minutes": None}, headers=operator_headers).json()
    assert cleared["sla_minutes"] is None
    assert cleared["is_monitored"] is False  # untouched by an SLA-only patch


def test_viewer_cannot_set_sla(client: TestClient, setup: dict, viewer_headers: dict) -> None:
    dag_id = setup["dags"]["quality_checks"]["id"]
    r = client.patch(
        f"/api/v1/airflow/dags/{dag_id}", json={"sla_minutes": 30}, headers=viewer_headers
    )
    assert r.status_code == 403


def test_unknown_incident_404(client: TestClient, viewer_headers: dict) -> None:
    r = client.get("/api/v1/incidents/00000000-0000-0000-0000-000000000000", headers=viewer_headers)
    assert r.status_code == 404
