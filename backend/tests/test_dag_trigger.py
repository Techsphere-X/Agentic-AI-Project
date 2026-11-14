"""'When this DAG runs': one DAG, an output per event; the workflow decides about incidents,
fix blocks work on the DAG run itself, and a fix does not restart its own workflow."""

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.automation.graph import GraphError, validate_graph
from app.automation.migrate import migrate_graph
from app.automation.types import RunStatus
from app.detection.types import IncidentStatus, IncidentType
from app.models.automation import Workflow, WorkflowRun
from app.models.incident import Incident
from app.models.user import User
from app.orchestration.airflow import factory
from app.services import automation_service, detection_service
from tests.fake_airflow import FakeAirflow
from tests.test_plan1_detection import T0, _conn, _dag, at, iso

NETWORK_LOG = "requests.exceptions.ConnectionError: Connection reset by peer"


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeAirflow:
    server = FakeAirflow(major=3)
    monkeypatch.setattr(factory, "live_transport", server.transport())
    return server


def _trigger(conn: object, states: list[str], dag_id: str = "etl") -> dict:
    return {
        "id": "trigger",
        "type": "trigger.dag",
        "config": {"connection_id": str(conn.id), "dag_id": dag_id, "states": states},
    }


def _notify(node_id: str, title: str = "x") -> dict:
    return {"id": node_id, "type": "notify", "config": {"title": title}}


def _workflow(db: Session, graph: dict, name: str = "orders") -> Workflow:
    validate_graph(graph)
    wf = Workflow(name=name, graph=graph, enabled=True)
    db.add(wf)
    db.commit()
    return wf


def _runs(db: Session) -> list[WorkflowRun]:
    db.expire_all()
    return list(db.scalars(select(WorkflowRun).order_by(WorkflowRun.created_at)).all())


def _tick(db: Session, now) -> None:
    summary = automation_service.tick(db, now=now)
    assert summary.errors == []
    db.expire_all()


def _ports(run: WorkflowRun) -> list[tuple[str, str | None]]:
    return [(s.node_id, s.port) for s in run.steps]


# ---------------------------------------------------------------------- validation


def test_incident_blocks_need_an_open_incident_first() -> None:
    conn = type("C", (), {"id": "c1"})()
    update = {"id": "ack", "type": "incident.update", "config": {"operation": "acknowledge"}}
    graph = {
        "nodes": [_trigger(conn, ["failed"]), update],
        "edges": [{"from": "trigger", "port": "failed", "to": "ack"}],
    }
    with pytest.raises(GraphError, match="put an 'Open incident' block before it"):
        validate_graph(graph)

    graph["nodes"].append({"id": "open", "type": "incident.open", "config": {}})
    graph["edges"] = [
        {"from": "trigger", "port": "failed", "to": "open"},
        {"from": "open", "port": "opened", "to": "ack"},
    ]
    validate_graph(graph)


def test_replaced_start_blocks_are_reported() -> None:
    graph = {"nodes": [{"id": "t", "type": "trigger.dag_run", "config": {}}], "edges": []}
    with pytest.raises(GraphError, match="replaced by 'When this DAG runs'"):
        validate_graph(graph)


def test_open_incident_needs_the_dag_trigger() -> None:
    graph = {
        "nodes": [
            {"id": "t", "type": "trigger.manual", "config": {}},
            {"id": "open", "type": "incident.open", "config": {}},
        ],
        "edges": [{"from": "t", "port": "next", "to": "open"}],
    }
    with pytest.raises(GraphError, match="needs 'When this DAG runs'"):
        validate_graph(graph)


# ---------------------------------------------------------------------- detection & incidents


def test_a_workflow_on_failed_owns_the_dags_failures(db: Session, fake: FakeAirflow) -> None:
    conn = _conn(db)
    _dag(db, conn, "etl")
    _dag(db, conn, "other")
    _workflow(
        db,
        {
            "nodes": [_trigger(conn, ["failed"]), _notify("tell")],
            "edges": [{"from": "trigger", "port": "failed", "to": "tell"}],
        },
    )
    fake.add_run("etl", "r1", "failed", iso(at(10)), failed_task="load", log=NETWORK_LOG)
    fake.add_run("other", "o1", "failed", iso(at(10)), failed_task="load", log=NETWORK_LOG)

    summary = detection_service.run_cycle(db, now=T0)
    assert summary.automation_queued == 1
    # 'etl' belongs to the workflow: no automatic incident. 'other' still gets one.
    assert [i.dag_id for i in db.scalars(select(Incident))] == ["other"]


def test_a_workflow_only_on_success_leaves_failure_incidents_alone(
    db: Session, fake: FakeAirflow
) -> None:
    conn = _conn(db)
    _dag(db, conn, "etl")
    _workflow(
        db,
        {
            "nodes": [_trigger(conn, ["success"]), _notify("tell")],
            "edges": [{"from": "trigger", "port": "success", "to": "tell"}],
        },
    )
    fake.add_run("etl", "r1", "failed", iso(at(10)), failed_task="load", log=NETWORK_LOG)
    summary = detection_service.run_cycle(db, now=T0)
    assert summary.automation_queued == 0
    assert [i.dag_id for i in db.scalars(select(Incident))] == ["etl"]


def test_open_incident_then_already_open(db: Session, fake: FakeAirflow) -> None:
    conn = _conn(db)
    _dag(db, conn, "etl")
    _workflow(
        db,
        {
            "nodes": [
                _trigger(conn, ["failed"]),
                {"id": "open", "type": "incident.open", "config": {"severity": "CRITICAL"}},
                {"id": "ack", "type": "incident.update", "config": {"operation": "acknowledge"}},
                _notify("again", "Still failing: {{incident.title}}"),
            ],
            "edges": [
                {"from": "trigger", "port": "failed", "to": "open"},
                {"from": "open", "port": "opened", "to": "ack"},
                {"from": "open", "port": "already_open", "to": "again"},
            ],
        },
    )
    fake.add_run("etl", "r1", "failed", iso(at(10)), failed_task="load", log=NETWORK_LOG)
    detection_service.run_cycle(db, now=T0)
    _tick(db, T0)
    [incident] = db.scalars(select(Incident)).all()
    assert incident.type == IncidentType.DAG_RUN_FAILED
    assert incident.severity == "CRITICAL"
    assert (incident.run_id, incident.status) == ("r1", IncidentStatus.ACKNOWLEDGED)
    assert any(NETWORK_LOG in (e.content or "") for e in incident.evidence)  # the run's log is kept
    [first] = _runs(db)
    assert first.incident_id == incident.id
    assert _ports(first) == [("trigger", "failed"), ("open", "opened"), ("ack", "next")]

    fake.add_run("etl", "r2", "failed", iso(at(11)), failed_task="load", log=NETWORK_LOG)
    detection_service.run_cycle(db, now=at(11, 10))
    _tick(db, at(11, 10))
    second = _runs(db)[-1]
    assert _ports(second)[1] == ("open", "already_open")
    db.refresh(incident)
    assert (incident.occurrence_count, incident.last_run_id) == (2, "r2")


# ---------------------------------------------------------------------- fix blocks


def test_classify_reads_the_dag_runs_logs_without_an_incident(
    db: Session, fake: FakeAirflow
) -> None:
    conn = _conn(db)
    _dag(db, conn, "etl")
    _workflow(
        db,
        {
            "nodes": [
                _trigger(conn, ["failed"]),
                {"id": "classify", "type": "diagnose.classify_log", "config": {}},
                _notify("retry", "{{diagnosis.label}}"),
            ],
            "edges": [
                {"from": "trigger", "port": "failed", "to": "classify"},
                {"from": "classify", "port": "retryable", "to": "retry"},
            ],
        },
    )
    fake.add_run("etl", "r1", "failed", iso(at(10)), failed_task="load", log=NETWORK_LOG)
    detection_service.run_cycle(db, now=T0)
    _tick(db, T0)
    [run] = _runs(db)
    assert run.status == RunStatus.COMPLETED
    assert _ports(run)[1] == ("classify", "retryable")
    assert run.context["diagnosis"]["category"] == "TRANSIENT_NETWORK"


def test_a_fix_is_verified_in_the_same_run_and_does_not_restart_the_workflow(
    db: Session, fake: FakeAirflow
) -> None:
    conn = _conn(db)
    _dag(db, conn, "etl")
    _workflow(
        db,
        {
            "nodes": [
                _trigger(conn, ["in_progress", "success", "failed"]),
                {"id": "retry", "type": "action.clear_failed_tasks", "config": {}},
                {"id": "verify", "type": "verify.run_success", "config": {}},
                _notify("fixed", "fixed"),
                _notify("started", "started"),
            ],
            "edges": [
                {"from": "trigger", "port": "failed", "to": "retry"},
                {"from": "trigger", "port": "in_progress", "to": "started"},
                {"from": "retry", "port": "success", "to": "verify"},
                {"from": "verify", "port": "success", "to": "fixed"},
            ],
        },
    )
    fake.add_run("etl", "r1", "failed", iso(at(10)), failed_task="load", log=NETWORK_LOG)
    detection_service.run_cycle(db, now=T0)
    _tick(db, T0)
    [run] = _runs(db)
    assert run.status == RunStatus.WAITING  # verify waits for the cleared run
    assert ("retry", "success") in _ports(run)

    # The cleared run is queued, then succeeds: neither state starts the workflow again.
    assert detection_service.run_cycle(db, now=at(12, 1)).automation_queued == 0
    fake.runs["etl"][0]["state"] = "success"
    fake.runs["etl"][0]["end_date"] = iso(at(12, 2))
    assert detection_service.run_cycle(db, now=at(12, 5)).automation_queued == 0

    _tick(db, at(12, 5))
    [run] = _runs(db)
    assert run.status == RunStatus.COMPLETED
    assert _ports(run)[-2:] == [("verify", "success"), ("fixed", "next")]


def test_run_now_replays_the_dags_latest_run(db: Session, fake: FakeAirflow, admin: User) -> None:
    conn = _conn(db)
    _dag(db, conn, "etl")
    wf = _workflow(
        db,
        {
            "nodes": [_trigger(conn, ["success", "failed"]), _notify("tell")],
            "edges": [{"from": "trigger", "port": "success", "to": "tell"}],
        },
    )
    fake.add_run("etl", "old", "failed", iso(at(8)), failed_task="load", log="x")
    fake.add_run("etl", "new", "success", iso(at(10)), iso(at(10, 5)))
    run = automation_service.start_manual_run(db, wf.id, actor=admin, now=T0)
    run = db.get(WorkflowRun, run.id)
    assert run.context["dag_run"]["run_id"] == "new"
    assert _ports(run)[0] == ("trigger", "success")


# ---------------------------------------------------------------------- migration


def _connections(dag_id: str) -> str | None:
    return {"etl": "c1", "billing": "c2"}.get(dag_id)


def test_migrate_one_dag_keeps_the_workflow() -> None:
    graph = {
        "nodes": [
            {
                "id": "t",
                "type": "trigger.dag_run",
                "config": {"states": ["success", "failed"], "dag_ids": ["etl"]},
            },
            {"id": "n", "type": "notify", "config": {"title": "x"}},
        ],
        "edges": [{"from": "t", "port": "next", "to": "n"}],
    }
    result = migrate_graph(graph, _connections)
    [(dag_id, out)] = result.graphs
    assert dag_id == "etl" and result.disable is None
    assert out["nodes"][0] == {
        "id": "t",
        "type": "trigger.dag",
        "config": {"connection_id": "c1", "dag_id": "etl", "states": ["success", "failed"]},
    }
    assert {(e["port"], e["to"]) for e in out["edges"]} == {("success", "n"), ("failed", "n")}
    validate_graph(out)


def test_migrate_several_dags_splits_and_watch_opens_an_incident() -> None:
    graph = {
        "nodes": [
            {
                "id": "t",
                "type": "trigger.dag_watch",
                "position": {"x": 0, "y": 0},
                "config": {
                    "dags": [
                        {"key": "etl", "connection_id": "c9", "dag_id": "etl"},
                        {"key": "billing", "connection_id": "c2", "dag_id": "billing"},
                    ]
                },
            },
            {"id": "ack", "type": "incident.update", "config": {"operation": "acknowledge"}},
            {"id": "tell", "type": "notify", "config": {"title": "x"}},
        ],
        "edges": [
            {"from": "t", "port": "etl", "to": "ack"},
            {"from": "t", "port": "billing", "to": "tell"},
        ],
    }
    result = migrate_graph(graph, _connections)
    assert [d for d, _ in result.graphs] == ["etl", "billing"]
    etl = result.graphs[0][1]
    assert etl["nodes"][0]["config"]["connection_id"] == "c9"  # the watch's own connection
    assert {n["id"] for n in etl["nodes"]} == {"t", "ack", "open_incident"}
    assert {(e["from"], e["port"], e["to"]) for e in etl["edges"]} == {
        ("t", "failed", "open_incident"),
        ("open_incident", "opened", "ack"),
        ("open_incident", "already_open", "ack"),
    }
    for _, out in result.graphs:
        validate_graph(out)


def test_migrate_every_dag_or_unknown_dag_turns_the_workflow_off() -> None:
    every = {
        "nodes": [{"id": "t", "type": "trigger.dag_run", "config": {"dag_ids": []}}],
        "edges": [],
    }
    assert "Choose a DAG" in migrate_graph(every, _connections).disable
    unknown = {
        "nodes": [{"id": "t", "type": "trigger.dag_run", "config": {"dag_ids": ["nope"]}}],
        "edges": [],
    }
    assert "not in any synced DAG list" in migrate_graph(unknown, _connections).disable
    manual = {"nodes": [{"id": "t", "type": "trigger.manual", "config": {}}], "edges": []}
    assert migrate_graph(manual, _connections) is None
