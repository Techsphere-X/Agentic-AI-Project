"""Tests for the Adaptive SLA block (check.sla): historical baseline estimation, buffer margins, and notification routing."""

import uuid
from datetime import UTC, datetime, timedelta
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.automation.graph import GraphError, catalog, to_raw, validate_graph
from app.models.airflow import AirflowConnection, DeploymentEnvironment
from app.models.automation import Notification, Workflow, WorkflowRun
from app.models.user import User
from app.orchestration.airflow.base import AdapterConfig, AirflowDagRun
from app.orchestration.airflow.mock import MockAirflowAdapter
from app.services import automation_service
from tests.test_plan2_engine import make_env


def test_catalog_includes_sla_block():
    entry = next((e for e in catalog() if e["type"] == "check.sla"), None)
    assert entry is not None
    assert entry["label"] == "Estimate & check SLA"
    assert entry["category"] == "logic"
    assert "exceeded" in entry["ports"]
    assert "met" in entry["ports"]
    assert "margin_pct" in entry["config_schema"]["properties"]
    assert "sample_size" in entry["config_schema"]["properties"]


def test_graph_validation_sla_block():
    graph = {
        "nodes": [
            {"id": "t", "type": "trigger.dag_run", "config": {}},
            {"id": "s", "type": "check.sla", "config": {"margin_pct": 25, "sample_size": 5}},
            {"id": "n1", "type": "notify", "config": {"title": "Exceeded"}},
            {"id": "n2", "type": "notify", "config": {"title": "Normal"}},
        ],
        "edges": [
            {"from": "t", "port": "next", "to": "s"},
            {"from": "s", "port": "exceeded", "to": "n1"},
            {"from": "s", "port": "met", "to": "n2"},
        ],
    }
    validated = validate_graph(graph)
    assert validated.nodes["s"].config["margin_pct"] == 25
    assert validated.nodes["s"].config["sample_size"] == 5
    assert validated.next_nodes("s", "exceeded") == ["n1"]
    assert validated.next_nodes("s", "met") == ["n2"]


def test_graph_validation_invalid_margin():
    graph = {
        "nodes": [
            {"id": "t", "type": "trigger.dag_run", "config": {}},
            {"id": "s", "type": "check.sla", "config": {"margin_pct": 999}},
        ],
        "edges": [{"from": "t", "port": "next", "to": "s"}],
    }
    with pytest.raises(GraphError):
        validate_graph(graph)


class CustomMockAdapter(MockAirflowAdapter):
    def __init__(self, runs: list[AirflowDagRun]):
        super().__init__(AdapterConfig(base_url="http://mock-airflow"), simulate_latency=False)
        self._runs = runs

    async def list_dag_runs(self, dag_id: str, *, since: datetime | None = None, limit: int = 100) -> list[AirflowDagRun]:
        return self._runs

    async def get_dag_run(self, dag_id: str, run_id: str) -> AirflowDagRun | None:
        return next((r for r in self._runs if r.run_id == run_id), None)


def test_sla_block_execution_exceeded(db: Session, admin: User, monkeypatch: pytest.MonkeyPatch):
    conn, dags = make_env(db, DeploymentEnvironment.DEV, "orders_pipeline")
    conn.is_default = True
    db.commit()

    now = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)

    # 3 historical runs of 10 minutes each: baseline median = 10 min.
    # Buffer margin = 20% -> threshold = 12 min.
    # Current run: duration = 20 min (> 12 min).
    history = [
        AirflowDagRun(
            run_id="run_current",
            state="success",
            start_date=now - timedelta(minutes=20),
            end_date=now,
        ),
        AirflowDagRun(
            run_id="run_past_1",
            state="success",
            start_date=now - timedelta(minutes=60),
            end_date=now - timedelta(minutes=50),
        ),
        AirflowDagRun(
            run_id="run_past_2",
            state="success",
            start_date=now - timedelta(minutes=120),
            end_date=now - timedelta(minutes=110),
        ),
        AirflowDagRun(
            run_id="run_past_3",
            state="success",
            start_date=now - timedelta(minutes=180),
            end_date=now - timedelta(minutes=170),
        ),
    ]

    custom_adapter = CustomMockAdapter(history)
    from app.services import airflow_service
    monkeypatch.setattr(airflow_service, "adapter_for", lambda c: custom_adapter)

    graph = {
        "nodes": [
            {"id": "t", "type": "trigger.manual", "config": {}},
            {
                "id": "check_sla_node",
                "type": "check.sla",
                "config": {
                    "dag_id": "orders_pipeline",
                    "metric": "duration",
                    "margin_pct": 20,
                    "sample_size": 10,
                    "calculation_method": "median",
                },
            },
            {
                "id": "notify_exceeded",
                "type": "notify",
                "config": {
                    "channel": "in_app",
                    "level": "WARNING",
                    "title": "SLA Exceeded on {{sla.dag_id}}",
                    "message": "Duration {{sla.actual_minutes}} min exceeded threshold {{sla.threshold_minutes}} min: {{sla.summary}}",
                },
            },
            {
                "id": "notify_met",
                "type": "notify",
                "config": {
                    "channel": "in_app",
                    "level": "INFO",
                    "title": "SLA Normal",
                    "message": "SLA met",
                },
            },
        ],
        "edges": [
            {"from": "t", "port": "next", "to": "check_sla_node"},
            {"from": "check_sla_node", "port": "exceeded", "to": "notify_exceeded"},
            {"from": "check_sla_node", "port": "met", "to": "notify_met"},
        ],
    }

    wf = Workflow(
        id=uuid.uuid4(),
        name="sla-test-exceeded",
        graph=to_raw(validate_graph(graph)),
        enabled=True,
    )
    db.add(wf)
    db.commit()

    run = automation_service.start_manual_run(db, wf.id, actor=admin, now=now)
    advanced = automation_service.advance(db, run, now=now)

    assert advanced.status.value == "COMPLETED"
    sla_ctx = advanced.context.get("sla", {})
    assert sla_ctx.get("exceeded") is True
    assert sla_ctx.get("actual_minutes") == 20.0
    assert sla_ctx.get("estimated_baseline_minutes") == 10.0
    assert sla_ctx.get("threshold_minutes") == 12.0
    assert sla_ctx.get("sample_count") == 4

    # Check notification was sent to in-app via the exceeded port
    notifs = db.scalars(select(Notification).where(Notification.run_id == run.id)).all()
    assert len(notifs) == 1
    assert "SLA Exceeded on orders_pipeline" in notifs[0].title
    assert "20.0 min exceeded threshold 12.0 min" in notifs[0].body


def test_sla_block_execution_met(db: Session, admin: User, monkeypatch: pytest.MonkeyPatch):
    conn, dags = make_env(db, DeploymentEnvironment.DEV, "orders_pipeline")
    conn.is_default = True
    db.commit()

    now = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)

    # 2 historical runs of 10 minutes each: baseline median = 10 min.
    # Buffer margin = 20% -> threshold = 12 min.
    # Current run: duration = 9 min (within threshold 12 min).
    history = [
        AirflowDagRun(
            run_id="run_current",
            state="success",
            start_date=now - timedelta(minutes=9),
            end_date=now,
        ),
        AirflowDagRun(
            run_id="run_past_1",
            state="success",
            start_date=now - timedelta(minutes=60),
            end_date=now - timedelta(minutes=50),
        ),
        AirflowDagRun(
            run_id="run_past_2",
            state="success",
            start_date=now - timedelta(minutes=120),
            end_date=now - timedelta(minutes=110),
        ),
    ]

    custom_adapter = CustomMockAdapter(history)
    from app.services import airflow_service
    monkeypatch.setattr(airflow_service, "adapter_for", lambda c: custom_adapter)

    graph = {
        "nodes": [
            {"id": "t", "type": "trigger.manual", "config": {}},
            {
                "id": "check_sla_node",
                "type": "check.sla",
                "config": {
                    "dag_id": "orders_pipeline",
                    "metric": "duration",
                    "margin_pct": 20,
                    "sample_size": 10,
                },
            },
            {
                "id": "notify_exceeded",
                "type": "notify",
                "config": {"channel": "in_app", "level": "WARNING", "title": "Exceeded", "message": "bad"},
            },
            {
                "id": "notify_met",
                "type": "notify",
                "config": {"channel": "in_app", "level": "INFO", "title": "SLA Normal on {{sla.dag_id}}", "message": "All good"},
            },
        ],
        "edges": [
            {"from": "t", "port": "next", "to": "check_sla_node"},
            {"from": "check_sla_node", "port": "exceeded", "to": "notify_exceeded"},
            {"from": "check_sla_node", "port": "met", "to": "notify_met"},
        ],
    }

    wf = Workflow(
        id=uuid.uuid4(),
        name="sla-test-met",
        graph=to_raw(validate_graph(graph)),
        enabled=True,
    )
    db.add(wf)
    db.commit()

    run = automation_service.start_manual_run(db, wf.id, actor=admin, now=now)
    advanced = automation_service.advance(db, run, now=now)

    assert advanced.status.value == "COMPLETED"
    sla_ctx = advanced.context.get("sla", {})
    assert sla_ctx.get("exceeded") is False
    assert sla_ctx.get("actual_minutes") == 9.0

    notifs = db.scalars(select(Notification).where(Notification.run_id == run.id)).all()
    assert len(notifs) == 1
    assert "SLA Normal on orders_pipeline" in notifs[0].title


def test_in_flight_sla_detection_exceeded(db: Session, admin: User, monkeypatch: pytest.MonkeyPatch):
    conn, dags = make_env(db, DeploymentEnvironment.DEV, "orders_pipeline")
    conn.is_default = True
    db.commit()

    now = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)

    # 3 past runs of 10 min each -> baseline median = 10 min.
    # Buffer = 20% -> threshold = 12 min.
    # Active in-flight run: running for 25 min (> 12 min).
    history = [
        AirflowDagRun(
            run_id="run_in_flight_long",
            state="running",
            start_date=now - timedelta(minutes=25),
            end_date=None,
        ),
        AirflowDagRun(
            run_id="past_1",
            state="success",
            start_date=now - timedelta(minutes=60),
            end_date=now - timedelta(minutes=50),
        ),
        AirflowDagRun(
            run_id="past_2",
            state="success",
            start_date=now - timedelta(minutes=120),
            end_date=now - timedelta(minutes=110),
        ),
        AirflowDagRun(
            run_id="past_3",
            state="success",
            start_date=now - timedelta(minutes=180),
            end_date=now - timedelta(minutes=170),
        ),
    ]

    custom_adapter = CustomMockAdapter(history)
    from app.services import airflow_service
    monkeypatch.setattr(airflow_service, "adapter_for", lambda c: custom_adapter)

    graph = {
        "nodes": [
            {"id": "t", "type": "trigger.schedule", "config": {"every_minutes": 5}},
            {
                "id": "check_sla_node",
                "type": "check.sla",
                "config": {
                    "dag_id": "orders_pipeline",
                    "metric": "duration",
                    "margin_pct": 20,
                    "sample_size": 10,
                    "only_in_flight": True,
                },
            },
            {
                "id": "notify_exceeded",
                "type": "notify",
                "config": {
                    "channel": "in_app",
                    "level": "WARNING",
                    "title": "In-flight SLA Breach on {{sla.dag_id}} ({{sla.actual_minutes}} min)",
                    "message": "{{sla.summary}}",
                },
            },
            {
                "id": "notify_met",
                "type": "notify",
                "config": {"channel": "in_app", "level": "INFO", "title": "Normal", "message": "Normal"},
            },
        ],
        "edges": [
            {"from": "t", "port": "next", "to": "check_sla_node"},
            {"from": "check_sla_node", "port": "exceeded", "to": "notify_exceeded"},
            {"from": "check_sla_node", "port": "met", "to": "notify_met"},
        ],
    }

    wf = Workflow(
        id=uuid.uuid4(),
        name="in-flight-sla-test-exceeded",
        graph=to_raw(validate_graph(graph)),
        enabled=True,
    )
    db.add(wf)
    db.commit()

    run = automation_service.start_manual_run(db, wf.id, actor=admin, now=now)
    advanced = automation_service.advance(db, run, now=now)

    assert advanced.status.value == "COMPLETED"
    sla_ctx = advanced.context.get("sla", {})
    assert sla_ctx.get("exceeded") is True
    assert sla_ctx.get("is_in_flight") is True
    assert sla_ctx.get("run_id") == "run_in_flight_long"
    assert sla_ctx.get("actual_minutes") == 25.0
    assert sla_ctx.get("threshold_minutes") == 12.0

    notifs = db.scalars(select(Notification).where(Notification.run_id == run.id)).all()
    assert len(notifs) == 1
    assert "In-flight SLA Breach on orders_pipeline (25.0 min)" in notifs[0].title
    assert "in-flight run 'run_in_flight_long' has been running for 25.0 min" in notifs[0].body


def test_in_flight_sla_detection_idle_dag_no_false_alarm(db: Session, admin: User, monkeypatch: pytest.MonkeyPatch):
    conn, dags = make_env(db, DeploymentEnvironment.DEV, "orders_pipeline")
    conn.is_default = True
    db.commit()

    now = datetime(2026, 10, 12, 12, 0, tzinfo=UTC)

    # Past completed run took 50 min (which was long), but nothing is currently running.
    history = [
        AirflowDagRun(
            run_id="past_completed_long",
            state="success",
            start_date=now - timedelta(minutes=100),
            end_date=now - timedelta(minutes=50),
        ),
        AirflowDagRun(
            run_id="past_1",
            state="success",
            start_date=now - timedelta(minutes=160),
            end_date=now - timedelta(minutes=150),
        ),
    ]

    custom_adapter = CustomMockAdapter(history)
    from app.services import airflow_service
    monkeypatch.setattr(airflow_service, "adapter_for", lambda c: custom_adapter)

    graph = {
        "nodes": [
            {"id": "t", "type": "trigger.schedule", "config": {"every_minutes": 5}},
            {
                "id": "check_sla_node",
                "type": "check.sla",
                "config": {
                    "dag_id": "orders_pipeline",
                    "metric": "duration",
                    "margin_pct": 20,
                    "sample_size": 10,
                    "only_in_flight": True,
                },
            },
            {
                "id": "notify_exceeded",
                "type": "notify",
                "config": {"channel": "in_app", "level": "WARNING", "title": "Breach", "message": "bad"},
            },
            {
                "id": "notify_met",
                "type": "notify",
                "config": {"channel": "in_app", "level": "INFO", "title": "Idle / Within SLA", "message": "all good"},
            },
        ],
        "edges": [
            {"from": "t", "port": "next", "to": "check_sla_node"},
            {"from": "check_sla_node", "port": "exceeded", "to": "notify_exceeded"},
            {"from": "check_sla_node", "port": "met", "to": "notify_met"},
        ],
    }

    wf = Workflow(
        id=uuid.uuid4(),
        name="in-flight-sla-test-idle",
        graph=to_raw(validate_graph(graph)),
        enabled=True,
    )
    db.add(wf)
    db.commit()

    run = automation_service.start_manual_run(db, wf.id, actor=admin, now=now)
    advanced = automation_service.advance(db, run, now=now)

    assert advanced.status.value == "COMPLETED"
    sla_ctx = advanced.context.get("sla", {})
    assert sla_ctx.get("exceeded") is False
    assert sla_ctx.get("is_in_flight") is False
    assert sla_ctx.get("actual_minutes") == 0.0

    notifs = db.scalars(select(Notification).where(Notification.run_id == run.id)).all()
    assert len(notifs) == 1
    assert "Idle / Within SLA" in notifs[0].title


def test_in_flight_template_validation():
    from app.automation.templates import IN_FLIGHT_SLA_TEMPLATE
    validated = validate_graph(IN_FLIGHT_SLA_TEMPLATE.graph)
    assert validated.trigger.type == "trigger.schedule"
    assert validated.nodes["sla_check"].type == "check.sla"
    assert validated.nodes["sla_check"].config["only_in_flight"] is True
    assert validated.next_nodes("sla_check", "exceeded") == ["notify_sla"]

