"""The "Diagnose failure" workflow block (diagnose.classify_log): outcome outputs and settings."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.automation.graph import catalog, validate_graph
from app.detection.types import EvidenceKind
from app.models.airflow import DeploymentEnvironment
from app.models.automation import Notification, WorkflowRun
from app.models.incident import Incident, IncidentEvidence
from app.models.user import User
from app.orchestration.airflow import mock as mock_module
from app.services import automation_service, diagnosis_service
from app.services.automation_nodes import diagnosis_outcome
from tests.test_plan2_engine import Clock, detect, make_env, only, ports, tick

OUTCOMES = ("retryable", "needs_fix", "unknown")


@pytest.fixture
def partner_clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    clock = Clock(600, 4)  # partner_api_sync: transient network failure
    monkeypatch.setattr(mock_module, "clock", clock)
    return clock


@pytest.fixture
def orders_clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    clock = Clock(300, 3)  # orders_pipeline: bad data (UniqueViolation)
    monkeypatch.setattr(mock_module, "clock", clock)
    return clock


def graph(config: dict[str, Any] | None = None, wired: tuple[str, ...] = OUTCOMES) -> dict:
    nodes = [
        {"id": "t", "type": "trigger.incident", "config": {}},
        {"id": "c", "type": "diagnose.classify_log", "config": config or {}},
    ]
    edges = [{"from": "t", "port": "next", "to": "c"}]
    for port in wired:
        nodes.append({
            "id": f"n_{port}",
            "type": "notify",
            "config": {"title": f"{port}: {{{{diagnosis.label}}}} / {{{{diagnosis.outcome}}}}"},
        })
        edges.append({"from": "c", "port": port, "to": f"n_{port}"})
    return {"nodes": nodes, "edges": edges}


def install(db: Session, admin: User, raw: dict) -> None:
    workflow = automation_service.create_workflow(db, actor=admin, name="Diagnose", graph=raw)
    workflow.enabled = True
    db.commit()


def run_and_titles(db: Session, clock: Clock) -> tuple[WorkflowRun, list[str]]:
    tick(db, clock)
    return only(db, WorkflowRun), [n.title for n in db.scalars(select(Notification)).all()]


# ---------------------------------------------------------------------- catalog / validation


def test_catalog_entry_has_outcome_outputs_and_settings() -> None:
    entry = {e["type"]: e for e in catalog()}["diagnose.classify_log"]
    assert entry["label"] == "Classify the failure"
    assert entry["ports"] == ["retryable", "needs_fix", "unknown", "next"]
    assert entry["port_labels"]["needs_fix"] == "needs a fix"
    assert {"min_confidence_pct", "refresh"} <= set(entry["config_schema"]["properties"])


def test_old_and_new_wiring_validate() -> None:
    assert validate_graph(graph()).nodes["c"].config == {"min_confidence_pct": None,
                                                          "refresh": False}
    assert validate_graph(graph(wired=("next",))).next_nodes("c", "next") == ["n_next"]
    with pytest.raises(Exception, match=r"Minimum confidence \(%\)"):
        validate_graph(graph({"min_confidence_pct": 150}))


def test_outcome_rules() -> None:
    def d(category: str, confidence: float = 0.9, source: str = "regex") -> dict:
        return {"category": category, "confidence": confidence, "source": source}

    assert diagnosis_outcome(d("TIMEOUT"), None) == "retryable"
    assert diagnosis_outcome(d("AUTH"), None) == "needs_fix"
    assert diagnosis_outcome(d("UNKNOWN", 0.0), None) == "unknown"
    assert diagnosis_outcome(d("TIMEOUT", 0.88, "model"), 90) == "unknown"
    assert diagnosis_outcome(d("AUTH", 1.0, "operator"), 100) == "needs_fix"


# ---------------------------------------------------------------------- engine


def test_transient_failure_routes_retryable(
    db: Session, admin: User, partner_clock: Clock
) -> None:
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    install(db, admin, graph())
    detect(db, partner_clock)
    run, titles = run_and_titles(db, partner_clock)
    assert ports(run)[:2] == ["t.next", "c.retryable"]
    assert titles == ["retryable: Network glitch / retryable"]
    assert run.steps[1].message.endswith("→ retry may help")


def test_bad_data_routes_needs_fix(db: Session, admin: User, orders_clock: Clock) -> None:
    make_env(db, DeploymentEnvironment.DEV, "orders_pipeline")
    install(db, admin, graph())
    detect(db, orders_clock)
    run, titles = run_and_titles(db, orders_clock)
    assert ports(run)[1] == "c.needs_fix"
    assert titles == ["needs_fix: Bad data (constraint violation) / needs_fix"]


def test_low_confidence_routes_unknown(db: Session, admin: User, partner_clock: Clock) -> None:
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    install(db, admin, graph({"min_confidence_pct": 96}))  # regex network rule is 0.9
    detect(db, partner_clock)
    run, titles = run_and_titles(db, partner_clock)
    assert ports(run)[1] == "c.unknown"
    assert titles == ["unknown: Network glitch / unknown"]


def test_unconnected_outcome_falls_back_to_any_outcome(
    db: Session, admin: User, partner_clock: Clock
) -> None:
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    install(db, admin, graph(wired=("needs_fix", "next")))
    detect(db, partner_clock)
    run, titles = run_and_titles(db, partner_clock)
    assert ports(run)[1] == "c.next"
    assert titles == ["next: Network glitch / retryable"]


def test_refresh_uses_new_logs(db: Session, admin: User, orders_clock: Clock) -> None:
    make_env(db, DeploymentEnvironment.DEV, "orders_pipeline")
    install(db, admin, graph({"refresh": True}))
    detect(db, orders_clock)
    incident = only(db, Incident)
    db.add(IncidentEvidence(
        incident_id=incident.id,
        kind=EvidenceKind.TASK_LOG,
        source="later",
        content="requests.exceptions.ConnectionError: Connection refused",
        collected_at=datetime.now(UTC) + timedelta(days=1),
    ))
    db.commit()
    run, _ = run_and_titles(db, orders_clock)
    assert ports(run)[1] == "c.retryable"
    assert incident.diagnosis["category"] == "TRANSIENT_NETWORK"


def test_operator_correction_passes_threshold_and_survives_refresh(
    db: Session, admin: User, orders_clock: Clock
) -> None:
    make_env(db, DeploymentEnvironment.DEV, "orders_pipeline")
    install(db, admin, graph({"refresh": True, "min_confidence_pct": 100}))
    detect(db, orders_clock)
    incident = only(db, Incident)
    diagnosis_service.set_override(
        db, incident.id, "TRANSIENT_NETWORK", "vendor outage", actor=admin, ip_address=None
    )
    run, titles = run_and_titles(db, orders_clock)
    assert ports(run)[1] == "c.retryable"
    assert titles == ["retryable: Network glitch / retryable"]
    assert incident.diagnosis["source"] == "operator"
