"""'When a DAG run finishes' workflows: one run per finished DAG run, a per-run check status
recorded by the 'Record the run's status' block, and the 'Data check failed' incident."""

from datetime import timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from app.automation.graph import GraphError, validate_graph
from app.automation.templates import TEMPLATES_BY_KEY
from app.automation.types import DagRunCheckStatus, RunStatus, TriggerEvent
from app.connectors import database as db_connector
from app.core.exceptions import BadRequestError
from app.detection import rules
from app.detection.types import IncidentResolution, IncidentStatus, IncidentType
from app.models.automation import DagRunCheck, Workflow, WorkflowRun
from app.models.database_connection import DatabaseConnection, DatabaseEngine
from app.models.incident import Incident
from app.models.user import User
from app.orchestration.airflow import factory
from app.services import automation_service, detection_service
from tests.fake_airflow import FakeAirflow
from tests.test_plan1_detection import T0, _conn, _dag, at, iso, run


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeAirflow:
    server = FakeAirflow(major=3)
    monkeypatch.setattr(factory, "live_transport", server.transport())
    return server


@pytest.fixture
def warehouse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'warehouse.db'}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE orders (id INTEGER PRIMARY KEY, run TEXT)"))
    monkeypatch.setattr(db_connector, "engine_factory", lambda url, args: create_engine(engine.url))
    return engine


def _database(db: Session) -> str:
    conn = DatabaseConnection(
        name="dwh",
        environment="DEV",
        engine=DatabaseEngine.POSTGRESQL,
        host="dwh.test",
        port=5432,
        database="analytics",
        username="etl",
    )
    db.add(conn)
    db.commit()
    return str(conn.id)


def _trigger(**config: object) -> dict:
    return {"id": "trigger", "type": "trigger.dag_run", "config": config}


def _workflow(db: Session, graph: dict, name: str = "Validate") -> Workflow:
    validate_graph(graph)
    wf = Workflow(name=name, graph=graph, enabled=True)
    db.add(wf)
    db.commit()
    return wf


def _record_only(result: str = "passed", **trigger: object) -> dict:
    return {
        "nodes": [
            _trigger(**trigger),
            {"id": "record", "type": "check.record", "config": {"result": result}},
        ],
        "edges": [{"from": "trigger", "port": "next", "to": "record"}],
    }


def _validate_graph(dwh: str) -> dict:
    return {
        "nodes": [
            _trigger(states=["success"]),
            {
                "id": "check",
                "type": "database.check",
                "config": {
                    "connection_id": dwh,
                    "sql": "SELECT count(*) FROM orders",
                    "operator": ">",
                    "expected": "0",
                },
            },
            {"id": "ok", "type": "check.record", "config": {"result": "passed"}},
            {
                "id": "bad",
                "type": "check.record",
                "config": {"result": "failed", "note": "{{last_error}}"},
            },
        ],
        "edges": [
            {"from": "trigger", "port": "next", "to": "check"},
            {"from": "check", "port": "pass", "to": "ok"},
            {"from": "check", "port": "fail", "to": "bad"},
        ],
    }


def _checks(db: Session) -> list[DagRunCheck]:
    db.expire_all()
    return list(db.scalars(select(DagRunCheck).order_by(DagRunCheck.created_at)).all())


def _tick(db: Session, now) -> None:
    summary = automation_service.tick(db, now=now)
    assert summary.errors == []
    db.expire_all()


# ---------------------------------------------------------------------- rules (pure)


def test_finished_runs_after_watermark() -> None:
    runs = [
        run("a", "success", at(9)),
        run("b", "failed", at(10)),
        run("c", "running", at(11)),
        run("d", "success", at(11, 30)),
    ]
    assert [r.run_id for r in rules.finished_runs(runs, at(9))] == ["b", "d"]
    assert [r.run_id for r in rules.finished_runs(runs, at(11, 30))] == []
    # First cycle: only the newest finished run, no history replay.
    assert [r.run_id for r in rules.finished_runs(runs, None)] == ["d"]
    assert rules.finished_runs([], None) == []


# ---------------------------------------------------------------------- validation


def test_record_block_needs_the_dag_run_trigger() -> None:
    graph = _record_only()
    graph["nodes"][0] = {"id": "trigger", "type": "trigger.manual", "config": {}}
    with pytest.raises(GraphError, match="needs the 'When a DAG run finishes' trigger"):
        validate_graph(graph)


def test_incident_blocks_are_rejected_under_the_dag_run_trigger() -> None:
    graph = {
        "nodes": [_trigger(), {"id": "fix", "type": "action.clear_failed_tasks", "config": {}}],
        "edges": [{"from": "trigger", "port": "next", "to": "fix"}],
    }
    with pytest.raises(GraphError, match="needs an incident trigger"):
        validate_graph(graph)


def test_trigger_needs_a_state() -> None:
    with pytest.raises(GraphError):
        validate_graph(_record_only(states=[]))


def test_dag_run_workflows_cannot_be_run_by_hand(db: Session, admin: User) -> None:
    wf = _workflow(db, _record_only())
    with pytest.raises(BadRequestError, match="starts when a DAG run finishes"):
        automation_service.start_manual_run(db, wf.id, actor=admin)


def test_validate_every_run_template_builds() -> None:
    template = TEMPLATES_BY_KEY["validate-every-run"]
    graph = validate_graph(
        template.build({"dag_id": "orders", "database_connection_id": "c1", "sql": "SELECT 1"})
    )
    assert graph.trigger.type == "trigger.dag_run"
    assert graph.trigger.config == {"states": ["success"], "dag_ids": ["orders"]}


# ---------------------------------------------------------------------- enqueue


def test_each_finished_run_starts_the_workflow_once(db: Session, fake: FakeAirflow) -> None:
    conn = _conn(db)
    _dag(db, conn)
    wf = _workflow(db, _record_only())
    fake.add_run("etl", "r1", "success", iso(at(10)), iso(at(10, 5)))

    summary = detection_service.run_cycle(db, now=T0)
    assert summary.automation_queued == 1
    [check] = _checks(db)
    assert (check.run_id, check.run_state, check.status) == ("r1", "success", "PENDING")
    workflow_run = db.get(WorkflowRun, check.workflow_run_id)
    assert workflow_run.workflow_id == wf.id
    assert workflow_run.trigger_event == TriggerEvent.DAG_RUN
    assert workflow_run.context["dag_run"]["run_id"] == "r1"

    # The same run is never queued twice.
    again = detection_service.run_cycle(db, now=T0 + timedelta(minutes=2))
    assert again.automation_queued == 0

    fake.add_run("etl", "r2", "failed", iso(at(12, 5)), failed_task="load", log="x")
    fake.add_run("etl", "r3", "success", iso(at(12, 10)), iso(at(12, 15)))
    detection_service.run_cycle(db, now=at(12, 20))
    assert [(c.run_id, c.run_state) for c in _checks(db)] == [
        ("r1", "success"),
        ("r2", "failed"),
        ("r3", "success"),
    ]


def test_trigger_filters_by_state_and_dag(db: Session, fake: FakeAirflow) -> None:
    conn = _conn(db)
    _dag(db, conn, "etl")
    _dag(db, conn, "other")
    _workflow(db, _record_only(states=["failed"], dag_ids=["etl"]))
    fake.add_run("etl", "ok", "success", iso(at(10)), iso(at(10, 5)))
    fake.add_run("other", "bad_other", "failed", iso(at(10)), failed_task="t", log="x")
    detection_service.run_cycle(db, now=T0)
    assert _checks(db) == []

    fake.add_run("etl", "bad", "failed", iso(at(11)), failed_task="t", log="x")
    detection_service.run_cycle(db, now=at(11, 10))
    assert [c.run_id for c in _checks(db)] == ["bad"]


def test_disabled_workflows_are_not_started(db: Session, fake: FakeAirflow) -> None:
    conn = _conn(db)
    _dag(db, conn)
    wf = _workflow(db, _record_only())
    wf.enabled = False
    db.commit()
    fake.add_run("etl", "r1", "success", iso(at(10)), iso(at(10, 5)))
    detection_service.run_cycle(db, now=T0)
    assert _checks(db) == []


# ---------------------------------------------------------------------- status


def test_data_check_records_status_and_drives_the_incident(
    db: Session, fake: FakeAirflow, warehouse
) -> None:
    conn = _conn(db)
    _dag(db, conn)
    _workflow(db, _validate_graph(_database(db)))
    alert = _workflow(
        db,
        {
            "nodes": [
                {
                    "id": "trigger",
                    "type": "trigger.incident",
                    "config": {"events": ["opened"], "incident_types": ["DATA_CHECK_FAILED"]},
                },
                {"id": "tell", "type": "notify", "config": {"title": "{{incident.title}}"}},
            ],
            "edges": [{"from": "trigger", "port": "next", "to": "tell"}],
        },
        name="Alert",
    )

    # Run 1 succeeded in Airflow but loaded nothing: the check fails.
    fake.add_run("etl", "r1", "success", iso(at(10)), iso(at(10, 5)))
    detection_service.run_cycle(db, now=T0)
    _tick(db, T0)
    [check] = _checks(db)
    assert check.status == DagRunCheckStatus.FAILED
    assert check.message == "Data check failed: got 0, expected > 0."
    [incident] = db.scalars(select(Incident)).all()
    assert incident.type == IncidentType.DATA_CHECK_FAILED
    assert incident.title == "etl: data check failed"
    assert incident.run_id == "r1" and incident.status == IncidentStatus.OPEN

    # The incident starts incident workflows.
    _tick(db, T0)
    alert_run = db.scalars(select(WorkflowRun).where(WorkflowRun.workflow_id == alert.id)).one()
    assert alert_run.status == RunStatus.COMPLETED and alert_run.incident_id == incident.id

    # Run 2 loaded rows: it passes and resolves the incident.
    with warehouse.begin() as c:
        c.execute(text("INSERT INTO orders (run) VALUES ('r2')"))
    fake.add_run("etl", "r2", "success", iso(at(12)), iso(at(12, 5)))
    detection_service.run_cycle(db, now=at(12, 10))
    _tick(db, at(12, 10))
    assert [c.status for c in _checks(db)] == [DagRunCheckStatus.FAILED, DagRunCheckStatus.PASSED]
    db.refresh(incident)
    assert incident.status == IncidentStatus.RESOLVED
    assert incident.resolution == IncidentResolution.AUTO_RECOVERED


def test_failed_checks_recur_on_the_open_incident(db: Session, fake: FakeAirflow) -> None:
    conn = _conn(db)
    _dag(db, conn)
    _workflow(db, _record_only(result="failed"))
    fake.add_run("etl", "r1", "success", iso(at(10)), iso(at(10, 5)))
    detection_service.run_cycle(db, now=T0)
    _tick(db, T0)
    fake.add_run("etl", "r2", "success", iso(at(11)), iso(at(11, 5)))
    detection_service.run_cycle(db, now=at(11, 10))
    _tick(db, at(11, 10))
    [incident] = db.scalars(select(Incident)).all()
    assert (incident.occurrence_count, incident.last_run_id) == (2, "r2")


def test_no_incident_when_turned_off(db: Session, fake: FakeAirflow) -> None:
    conn = _conn(db)
    _dag(db, conn)
    graph = _record_only(result="failed")
    graph["nodes"][1]["config"]["open_incident"] = False
    _workflow(db, graph)
    fake.add_run("etl", "r1", "success", iso(at(10)), iso(at(10, 5)))
    detection_service.run_cycle(db, now=T0)
    _tick(db, T0)
    assert [c.status for c in _checks(db)] == [DagRunCheckStatus.FAILED]
    assert db.scalars(select(Incident)).all() == []


def test_unrecorded_and_cancelled_runs(db: Session, fake: FakeAirflow, admin: User) -> None:
    conn = _conn(db)
    _dag(db, conn)
    # Ends without a 'Record the run's status' block: NOT_CHECKED.
    _workflow(
        db,
        {
            "nodes": [_trigger(), {"id": "tell", "type": "notify", "config": {"title": "x"}}],
            "edges": [{"from": "trigger", "port": "next", "to": "tell"}],
        },
    )
    fake.add_run("etl", "r1", "success", iso(at(10)), iso(at(10, 5)))
    detection_service.run_cycle(db, now=T0)
    _tick(db, T0)
    [check] = _checks(db)
    assert check.status == DagRunCheckStatus.NOT_CHECKED

    # Cancelled before it ran: ERROR.
    fake.add_run("etl", "r2", "success", iso(at(11)), iso(at(11, 5)))
    detection_service.run_cycle(db, now=at(11, 10))
    pending = _checks(db)[-1]
    automation_service.cancel_run(db, pending.workflow_run_id, actor=admin)
    assert _checks(db)[-1].status == DagRunCheckStatus.ERROR


def test_dry_run_records_nothing(db: Session, fake: FakeAirflow) -> None:
    conn = _conn(db)
    _dag(db, conn)
    wf = _workflow(db, _record_only(result="failed"))
    wf.mode = "DRY_RUN"
    db.commit()
    fake.add_run("etl", "r1", "success", iso(at(10)), iso(at(10, 5)))
    detection_service.run_cycle(db, now=T0)
    _tick(db, T0)
    assert [c.status for c in _checks(db)] == [DagRunCheckStatus.NOT_CHECKED]
    assert db.scalars(select(Incident)).all() == []


# ---------------------------------------------------------------------- API


def test_run_checks_api(
    client: TestClient, viewer_headers: dict, db: Session, fake: FakeAirflow
) -> None:
    conn = _conn(db)
    _dag(db, conn)
    _workflow(db, _record_only())
    fake.add_run("etl", "r1", "success", iso(at(10)), iso(at(10, 5)))
    detection_service.run_cycle(db, now=T0)
    _tick(db, T0)
    fake.add_run("etl", "r2", "success", iso(at(11)), iso(at(11, 5)))
    detection_service.run_cycle(db, now=at(11, 10))

    page = client.get("/api/v1/automation/run-checks", headers=viewer_headers).json()
    assert page["total"] == 2
    assert [(c["run_id"], c["status"]) for c in page["items"]] == [
        ("r2", "PENDING"),
        ("r1", "PASSED"),
    ]
    assert page["items"][0]["workflow_name"] == "Validate"
    passed = client.get(
        "/api/v1/automation/run-checks?status=PASSED&dag_id=etl", headers=viewer_headers
    ).json()
    assert [c["run_id"] for c in passed["items"]] == ["r1"]

    latest = client.get("/api/v1/automation/run-checks/latest", headers=viewer_headers).json()
    assert [(c["dag_id"], c["run_id"]) for c in latest] == [("etl", "r2")]

    types = client.get("/api/v1/automation/node-types", headers=viewer_headers).json()
    by_type = {t["type"]: t for t in types}
    assert by_type["trigger.dag_run"]["needs_dag_run"] is True
    assert by_type["check.record"]["category"] == "output"
