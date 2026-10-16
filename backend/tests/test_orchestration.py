"""Orchestration blocks: manual/scheduled workflows that run DAGs and SQL without an incident.

Airflow is the mock adapter (its clock and the engine's `now` share one Clock); databases are a
SQLite file standing in for PostgreSQL via the connector's `engine_factory` hook.
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from app.automation.graph import GraphError, validate_graph
from app.automation.types import ApprovalStatus, RunStatus, WorkflowMode
from app.connectors import database as db_connector
from app.core.exceptions import BadRequestError, ConflictError
from app.models.airflow import AirflowConnection, ConnectionKind, DeploymentEnvironment
from app.models.automation import Approval, Notification, Workflow, WorkflowRun
from app.models.database_connection import DatabaseConnection, DatabaseEngine
from app.models.user import User
from app.orchestration.airflow import mock as mock_module
from app.orchestration.airflow.base import AuthType
from app.services import automation_service
from app.services.automation_nodes import compare


class Clock:
    def __init__(self) -> None:
        self.now = datetime.now(UTC).replace(microsecond=0)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **delta: float) -> None:
        self.now += timedelta(**delta)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    c = Clock()
    monkeypatch.setattr(mock_module, "clock", c)
    return c


@pytest.fixture
def warehouse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A SQLite database every database connection resolves to."""
    engine = create_engine(f"sqlite:///{tmp_path / 'warehouse.db'}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE orders (id INTEGER PRIMARY KEY, day TEXT)"))
        conn.execute(text("CREATE TABLE daily_totals (day TEXT, total INTEGER)"))
    monkeypatch.setattr(db_connector, "engine_factory", lambda url, args: create_engine(engine.url))
    return engine


def writes() -> mock_module._WriteState:
    return mock_module._state.get("http://mock-airflow", mock_module._WriteState())


def count(engine, table: str) -> int:
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()


def airflow(db: Session, env: DeploymentEnvironment = DeploymentEnvironment.DEV) -> str:
    conn = AirflowConnection(
        name=f"mock-{env.lower()}",
        environment=env,
        kind=ConnectionKind.MOCK,
        base_url="http://mock-airflow",
        auth_type=AuthType.NONE,
    )
    db.add(conn)
    db.commit()
    return str(conn.id)


def database(db: Session, env: DeploymentEnvironment = DeploymentEnvironment.DEV) -> str:
    conn = DatabaseConnection(
        name=f"dwh-{env.lower()}",
        environment=env,
        engine=DatabaseEngine.POSTGRESQL,
        host="dwh.test",
        port=5432,
        database="analytics",
        username="etl",
    )
    db.add(conn)
    db.commit()
    return str(conn.id)


def chain(*nodes: dict, trigger: dict | None = None, ports: dict | None = None) -> dict:
    """trigger -> nodes[0] -> nodes[1] ...; each link uses the node's first port unless given."""
    trigger = trigger or {"id": "trigger", "type": "trigger.manual", "config": {}}
    all_nodes = [trigger, *nodes]
    first_port = {
        "trigger": "next",
        "pipeline.run_dag": "success",
        "pipeline.wait_for_dag": "success",
        "database.run_sql": "success",
        "database.check": "pass",
        "approval.request": "approved",
    }
    edges = []
    for a, b in zip(all_nodes, all_nodes[1:], strict=False):
        port = (ports or {}).get(a["id"]) or first_port.get(a["type"]) or first_port.get(a["id"])
        edges.append({"from": a["id"], "port": port or "next", "to": b["id"]})
    return {"nodes": all_nodes, "edges": edges}


def workflow(
    db: Session, graph: dict, *, enabled: bool = False, mode=WorkflowMode.LIVE
) -> Workflow:
    validate_graph(graph)
    wf = Workflow(name="Nightly load", graph=graph, enabled=enabled, mode=mode)
    db.add(wf)
    db.commit()
    return wf


def run_now(db: Session, wf: Workflow, admin: User, clock: Clock, **kw) -> WorkflowRun:
    run = automation_service.start_manual_run(db, wf.id, actor=admin, now=clock(), **kw)
    db.expire_all()
    return db.get(WorkflowRun, run.id)


def tick(db: Session, clock: Clock) -> automation_service.TickSummary:
    summary = automation_service.tick(db, now=clock())
    assert summary.errors == []
    db.expire_all()
    return summary


def steps(run: WorkflowRun) -> list[tuple[str, str | None]]:
    return [(s.node_id, s.port) for s in run.steps]


# ---------------------------------------------------------------------- validation


def test_incident_blocks_need_an_incident_trigger() -> None:
    graph = chain({"id": "fix", "type": "action.clear_failed_tasks", "config": {}})
    with pytest.raises(GraphError) as exc:
        validate_graph(graph)
    assert "needs an incident trigger" in exc.value.problems[0]["message"]


def test_targets_must_be_chosen_and_parameters_valid() -> None:
    blank = chain({"id": "dag", "type": "pipeline.run_dag", "config": {}})
    with pytest.raises(GraphError, match="Choose an Airflow connection"):
        validate_graph(blank)
    bad_json = chain(
        {
            "id": "dag",
            "type": "pipeline.run_dag",
            "config": {"connection_id": "x", "dag_id": "d", "parameters": "[1]"},
        }
    )
    with pytest.raises(GraphError, match="JSON object"):
        validate_graph(bad_json)
    no_sql = chain({"id": "q", "type": "database.check", "config": {"connection_id": "x"}})
    with pytest.raises(GraphError, match="Enter the SQL"):
        validate_graph(no_sql)


@pytest.mark.parametrize(
    ("value", "op", "expected", "result"),
    [
        (5, ">", "0", True),
        (0, ">", "0", False),
        ("12.5", ">=", "12.5", True),
        ("ok", "=", "ok", True),
        ("ok", "!=", "ok", False),
        ("abc", ">", "1", False),
        (None, "=", "0", False),
    ],
)
def test_compare(value, op, expected, result) -> None:
    assert compare(value, op, expected) is result


# ---------------------------------------------------------------------- running


def test_pipeline_then_sql_then_check_then_notify(
    db: Session, admin: User, clock: Clock, warehouse
) -> None:
    af, dwh = airflow(db), database(db)
    wf = workflow(
        db,
        chain(
            {
                "id": "extract",
                "type": "pipeline.run_dag",
                "config": {
                    "connection_id": af,
                    "dag_id": "orders_pipeline",
                    "parameters": '{"day": "2026-09-01"}',
                    "wait_for_completion": False,
                },
            },
            {
                "id": "load",
                "type": "database.run_sql",
                "config": {"connection_id": dwh, "sql": "INSERT INTO orders (day) VALUES ('d1')"},
            },
            {
                "id": "has_rows",
                "type": "database.check",
                "config": {
                    "connection_id": dwh,
                    "sql": "SELECT count(*) FROM orders",
                    "operator": ">=",
                    "expected": "1",
                },
            },
            {
                "id": "tell",
                "type": "notify",
                "config": {"title": "Loaded {{results.has_rows.value}} order(s)"},
            },
        ),
    )
    run = run_now(db, wf, admin, clock)

    assert run.status == RunStatus.COMPLETED, run.error
    assert run.incident_id is None and run.trigger_event == "manual"
    assert steps(run) == [
        ("trigger", "next"),
        ("extract", "success"),
        ("load", "success"),
        ("has_rows", "pass"),
        ("tell", "next"),
    ]
    assert count(warehouse, "orders") == 1
    assert len(writes().triggered["orders_pipeline"]) == 1
    note = db.scalars(select(Notification)).one()
    assert note.title == "Loaded 1 order(s)" and note.incident_id is None
    assert run.steps[2].message == "1 row(s) affected"


def test_run_dag_waits_for_the_run_to_finish(db: Session, admin: User, clock: Clock) -> None:
    af = airflow(db)
    wf = workflow(
        db,
        chain(
            {
                "id": "extract",
                "type": "pipeline.run_dag",
                "config": {"connection_id": af, "dag_id": "partner_api_sync"},
            }
        ),
    )
    run = run_now(db, wf, admin, clock)
    assert run.status == RunStatus.WAITING
    assert "waiting for it to finish" in run.steps[-1].message

    clock.advance(seconds=mock_module.RERUN_SECONDS + 1)
    tick(db, clock)
    run = db.get(WorkflowRun, run.id)
    assert run.status == RunStatus.COMPLETED
    assert steps(run)[-1] == ("extract", "success")
    assert run.context["results"]["extract"]["state"] == "success"


def test_run_dag_runs_in_progress_branch_before_polling_to_success(
    db: Session, admin: User, clock: Clock
) -> None:
    af = airflow(db)
    wf = workflow(
        db,
        {
            "nodes": [
                {"id": "trigger", "type": "trigger.manual", "config": {}},
                {
                    "id": "extract",
                    "type": "pipeline.run_dag",
                    "config": {"connection_id": af, "dag_id": "partner_api_sync"},
                },
                {"id": "progress", "type": "notify", "config": {"message": "started"}},
                {"id": "success", "type": "notify", "config": {"message": "done"}},
            ],
            "edges": [
                {"from": "trigger", "port": "next", "to": "extract"},
                {"from": "extract", "port": "in_progress", "to": "progress"},
                {"from": "extract", "port": "success", "to": "success"},
            ],
        },
    )

    run = run_now(db, wf, admin, clock)
    assert run.status == RunStatus.WAITING
    assert steps(run)[:2] == [("trigger", "next"), ("extract", None)]
    assert run.pending_nodes == ["progress", "extract"]
    assert len(writes().triggered["partner_api_sync"]) == 1

    clock.advance(seconds=mock_module.RERUN_SECONDS + 1)
    tick(db, clock)
    run = db.get(WorkflowRun, run.id)
    assert run.status == RunStatus.COMPLETED
    assert {node_id: port for node_id, port in steps(run)} == {
        "trigger": "next",
        "extract": "success",
        "progress": "next",
        "success": "next",
    }
    assert len(writes().triggered["partner_api_sync"]) == 1


def test_check_keeps_checking_until_the_data_arrives(
    db: Session, admin: User, clock: Clock, warehouse
) -> None:
    dwh = database(db)
    wf = workflow(
        db,
        chain(
            {
                "id": "ready",
                "type": "database.check",
                "config": {
                    "connection_id": dwh,
                    "sql": "SELECT count(*) FROM orders",
                    "keep_checking_minutes": 30,
                },
            }
        ),
    )
    run = run_now(db, wf, admin, clock)
    assert run.status == RunStatus.WAITING
    assert "Not yet: 0 is not > 0" in run.steps[-1].message

    with warehouse.begin() as conn:
        conn.execute(text("INSERT INTO orders (day) VALUES ('d1')"))
    clock.advance(minutes=1)
    tick(db, clock)
    run = db.get(WorkflowRun, run.id)
    assert run.status == RunStatus.COMPLETED
    assert steps(run)[-1] == ("ready", "pass")


def test_check_gives_up_after_its_window(db: Session, admin: User, clock: Clock, warehouse) -> None:
    dwh = database(db)
    wf = workflow(
        db,
        chain(
            {
                "id": "ready",
                "type": "database.check",
                "config": {
                    "connection_id": dwh,
                    "sql": "SELECT count(*) FROM orders",
                    "keep_checking_minutes": 5,
                },
            }
        ),
    )
    run = run_now(db, wf, admin, clock)
    clock.advance(minutes=6)
    tick(db, clock)
    run = db.get(WorkflowRun, run.id)
    assert steps(run)[-1] == ("ready", "fail")
    assert run.context["last_error"] == "Data check failed: got 0, expected > 0."


def test_wait_for_dag_sees_a_recent_success(db: Session, admin: User, clock: Clock) -> None:
    af = airflow(db)
    # partner_api_sync runs every 10 minutes in the mock; most runs succeed.
    wf = workflow(
        db,
        chain(
            {
                "id": "upstream",
                "type": "pipeline.wait_for_dag",
                "config": {
                    "connection_id": af,
                    "dag_id": "partner_api_sync",
                    "success_within_minutes": 120,
                },
            }
        ),
    )
    run = run_now(db, wf, admin, clock)
    assert run.status == RunStatus.COMPLETED
    assert steps(run)[-1] == ("upstream", "success")


def test_read_only_sql_cannot_write(db: Session, admin: User, clock: Clock, warehouse) -> None:
    dwh = database(db, DeploymentEnvironment.PROD)
    wf = workflow(
        db,
        chain(
            {
                "id": "q",
                "type": "database.run_sql",
                "config": {
                    "connection_id": dwh,
                    "sql": "INSERT INTO orders (day) VALUES ('x')",
                    "read_only": True,
                },
            }
        ),
    )
    run = run_now(db, wf, admin, clock)
    # Read-only needs no approval even on PROD, but the database refuses the write.
    assert steps(run)[-1] == ("q", "failed")
    assert count(warehouse, "orders") == 0


# ---------------------------------------------------------------------- safety


def test_prod_write_needs_approval(db: Session, admin: User, clock: Clock, warehouse) -> None:
    dwh = database(db, DeploymentEnvironment.PROD)
    sql = {
        "id": "load",
        "type": "database.run_sql",
        "config": {"connection_id": dwh, "sql": "INSERT INTO orders (day) VALUES ('d1')"},
    }
    # Without an approval step the policy blocks the write.
    run = run_now(db, workflow(db, chain(sql)), admin, clock)
    assert steps(run)[-1] == ("load", "failed")
    assert "require an approved approval step" in run.steps[-1].message
    assert count(warehouse, "orders") == 0

    # With one, the run waits for a human, then writes.
    approve = {"id": "ok", "type": "approval.request", "config": {}}
    wf = Workflow(name="Guarded load", graph=chain(approve, sql), mode=WorkflowMode.LIVE)
    db.add(wf)
    db.commit()
    run = run_now(db, wf, admin, clock)
    assert run.status == RunStatus.WAITING
    approval = db.scalars(select(Approval)).one()
    assert approval.title == "Guarded load: run SQL on dwh-prod"
    assert approval.incident_id is None
    automation_service.decide(db, approval.id, approve=True, actor=admin, comment=None)
    tick(db, clock)
    run = db.get(WorkflowRun, run.id)
    assert run.status == RunStatus.COMPLETED
    assert db.get(Approval, approval.id).status == ApprovalStatus.APPROVED
    assert count(warehouse, "orders") == 1


def test_dry_run_changes_nothing(db: Session, admin: User, clock: Clock, warehouse) -> None:
    af, dwh = airflow(db), database(db)
    wf = workflow(
        db,
        chain(
            {
                "id": "extract",
                "type": "pipeline.run_dag",
                "config": {"connection_id": af, "dag_id": "orders_pipeline"},
            },
            {
                "id": "load",
                "type": "database.run_sql",
                "config": {"connection_id": dwh, "sql": "INSERT INTO orders (day) VALUES ('x')"},
            },
        ),
    )
    run = run_now(db, wf, admin, clock, dry_run=True)
    assert run.status == RunStatus.COMPLETED
    assert [s.message for s in run.steps[1:]] == [
        "Dry run: would run orders_pipeline",
        "Dry run: would run SQL on dwh-dev",
    ]
    assert count(warehouse, "orders") == 0
    assert "orders_pipeline" not in writes().triggered


def test_run_now_rules(
    db: Session, admin: User, clock: Clock, client: TestClient, viewer_headers: dict
) -> None:
    automation_service.seed_templates(db)
    incident_wf = db.scalars(select(Workflow).where(Workflow.key == "retry-transient")).one()
    with pytest.raises(BadRequestError, match="starts from incidents"):
        automation_service.start_manual_run(db, incident_wf.id, actor=admin)

    wf = workflow(db, chain({"id": "pause", "type": "flow.wait", "config": {"minutes": 5}}))
    run = run_now(db, wf, admin, clock)
    assert run.status == RunStatus.WAITING
    with pytest.raises(ConflictError, match="already running"):
        automation_service.start_manual_run(db, wf.id, actor=admin)

    response = client.post(f"/api/v1/automation/workflows/{wf.id}/run", headers=viewer_headers)
    assert response.status_code == 403

    clock.advance(minutes=5)
    tick(db, clock)
    assert db.get(WorkflowRun, run.id).status == RunStatus.COMPLETED


def test_run_now_endpoint(
    db: Session, client: TestClient, operator_headers: dict, warehouse
) -> None:
    dwh = database(db)
    wf = workflow(
        db,
        chain(
            {
                "id": "q",
                "type": "database.run_sql",
                "config": {"connection_id": dwh, "sql": "SELECT 42 AS answer", "read_only": True},
            }
        ),
    )
    response = client.post(f"/api/v1/automation/workflows/{wf.id}/run", headers=operator_headers)
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["status"] == "COMPLETED"
    assert body["steps"][-1]["output"]["value"] == 42
    assert body["steps"][-1]["message"] == "Returned 1 row(s)"


# ---------------------------------------------------------------------- schedule


def test_schedule_starts_one_run_per_slot(db: Session, clock: Clock) -> None:
    wf = workflow(
        db,
        chain(
            {"id": "pause", "type": "flow.wait", "config": {"minutes": 1}},
            trigger={"id": "trigger", "type": "trigger.schedule", "config": {"every_minutes": 15}},
        ),
        enabled=True,
    )
    runs = lambda: db.scalars(select(WorkflowRun).where(WorkflowRun.workflow_id == wf.id)).all()  # noqa: E731

    assert tick(db, clock).enqueued == 1
    assert [r.trigger_event for r in runs()] == ["schedule"]
    clock.advance(minutes=2)  # the wait finishes; same 15-minute slot
    assert tick(db, clock).enqueued == 0
    assert all(r.status == RunStatus.COMPLETED for r in runs())

    clock.advance(minutes=15)  # next slot
    assert tick(db, clock).enqueued == 1
    assert len(runs()) == 2

    wf.enabled = False
    db.commit()
    clock.advance(minutes=30)
    assert tick(db, clock).enqueued == 0


def test_orchestration_blocks_are_in_the_catalog(client: TestClient, viewer_headers: dict) -> None:
    types = {
        t["type"]: t
        for t in client.get("/api/v1/automation/node-types", headers=viewer_headers).json()
    }
    for name in (
        "trigger.manual",
        "trigger.schedule",
        "pipeline.run_dag",
        "pipeline.wait_for_dag",
        "database.run_sql",
        "database.check",
        "flow.wait",
    ):
        assert name in types
    props = types["pipeline.run_dag"]["config_schema"]["properties"]
    assert props["connection_id"]["x-widget"] == "airflow_connection"
    assert props["dag_id"]["x-widget"] == "dag"
    assert types["pipeline.run_dag"]["ports"] == ["success", "in_progress", "failed"]
    assert types["pipeline.run_dag"]["port_labels"]["in_progress"] == "in progress"
    assert types["action.clear_failed_tasks"]["needs_incident"] is True
    assert types["database.check"]["needs_incident"] is False


# ---------------------------------------------------------------------- one output, several blocks


def _notify(node_id: str, y: float) -> dict:
    return {
        "id": node_id,
        "type": "notify",
        "config": {"message": node_id},
        "position": {"x": 300, "y": y},
    }


def fan_out(*targets: dict, extra_edges: tuple = ()) -> dict:
    trigger = {"id": "trigger", "type": "trigger.manual", "config": {}}
    edges = [{"from": "trigger", "port": "next", "to": t["id"]} for t in targets]
    return {"nodes": [trigger, *targets], "edges": edges + list(extra_edges)}


def test_linked_blocks_run_in_order_top_to_bottom(db: Session, admin: User, clock: Clock) -> None:
    # Linked lower block first: the canvas position decides the order, not the link order.
    run = run_now(
        db, workflow(db, fan_out(_notify("lower", 200), _notify("upper", 50))), admin, clock
    )
    assert run.status == RunStatus.COMPLETED
    assert [n for n, _ in steps(run)] == ["trigger", "upper", "lower"]
    assert sorted(db.scalars(select(Notification.body)).all()) == ["lower", "upper"]


def test_a_failing_branch_does_not_stop_the_others(db: Session, admin: User, clock: Clock) -> None:
    broken = {
        "id": "broken",
        "type": "pipeline.run_dag",
        "config": {"connection_id": "00000000-0000-4000-8000-000000000000", "dag_id": "gone"},
        "position": {"x": 300, "y": 0},
    }
    run = run_now(db, workflow(db, fan_out(broken, _notify("tell", 100))), admin, clock)
    assert [(s.node_id, s.status) for s in run.steps] == [
        ("trigger", "COMPLETED"),
        ("broken", "FAILED"),
        ("tell", "COMPLETED"),
    ]
    assert run.status == RunStatus.FAILED
    assert run.error == "The block's Airflow connection no longer exists"


def test_a_block_reached_twice_runs_once(db: Session, admin: User, clock: Clock) -> None:
    join = _notify("join", 300)
    graph = fan_out(
        _notify("a", 0),
        _notify("b", 100),
        join,
        extra_edges=(
            {"from": "a", "port": "next", "to": "join"},
            {"from": "b", "port": "next", "to": "join"},
        ),
    )
    graph["edges"] = [
        e for e in graph["edges"] if not (e["from"] == "trigger" and e["to"] == "join")
    ]
    run = run_now(db, workflow(db, graph), admin, clock)
    assert run.status == RunStatus.COMPLETED
    assert [n for n, _ in steps(run)] == ["trigger", "a", "b", "join"]


def test_a_wait_pauses_the_run_and_the_rest_follows(db: Session, admin: User, clock: Clock) -> None:
    wait = {
        "id": "wait",
        "type": "flow.wait",
        "config": {"minutes": 1},
        "position": {"x": 300, "y": 0},
    }
    run = run_now(db, workflow(db, fan_out(wait, _notify("after", 100))), admin, clock)
    assert run.status == RunStatus.WAITING
    assert run.current_node == "wait"
    assert run.pending_nodes == ["wait", "after"]

    clock.advance(minutes=2)
    tick(db, clock)
    run = db.get(WorkflowRun, run.id)
    assert run.status == RunStatus.COMPLETED
    assert [n for n, _ in steps(run)] == ["trigger", "wait", "after"]


def test_runs_from_before_the_to_do_list_still_resume(
    db: Session, admin: User, clock: Clock
) -> None:
    wait = {"id": "wait", "type": "flow.wait", "config": {"minutes": 1}}
    run = run_now(db, workflow(db, chain(wait, _notify("after", 0))), admin, clock)
    run.pending_nodes = None  # as stored by the previous version: only current_node
    db.commit()

    clock.advance(minutes=2)
    tick(db, clock)
    run = db.get(WorkflowRun, run.id)
    assert run.status == RunStatus.COMPLETED
    assert [n for n, _ in steps(run)] == ["trigger", "wait", "after"]


def test_one_approval_covers_every_linked_block(
    db: Session, admin: User, clock: Clock, warehouse
) -> None:
    dwh = database(db, DeploymentEnvironment.PROD)
    af = airflow(db, DeploymentEnvironment.PROD)
    approve = {"id": "ok", "type": "approval.request", "config": {}}
    load = {
        "id": "load",
        "type": "database.run_sql",
        "config": {"connection_id": dwh, "sql": "INSERT INTO orders (day) VALUES ('d1')"},
        "position": {"x": 600, "y": 0},
    }
    rerun = {
        "id": "rerun",
        "type": "pipeline.run_dag",
        "config": {"connection_id": af, "dag_id": "partner_api_sync", "wait_for_completion": False},
        "position": {"x": 600, "y": 100},
    }
    graph = {
        "nodes": [{"id": "trigger", "type": "trigger.manual", "config": {}}, approve, rerun, load],
        "edges": [
            {"from": "trigger", "port": "next", "to": "ok"},
            {"from": "ok", "port": "approved", "to": "rerun"},
            {"from": "ok", "port": "approved", "to": "load"},
        ],
    }
    wf = Workflow(name="Guarded load", graph=graph, mode=WorkflowMode.LIVE)
    db.add(wf)
    db.commit()
    run = run_now(db, wf, admin, clock)
    assert run.status == RunStatus.WAITING
    approval = db.scalars(select(Approval)).one()
    assert approval.title == "Guarded load: run SQL on dwh-prod, then run partner_api_sync"
    assert [a["node_id"] for a in approval.proposed_action["actions"]] == ["load", "rerun"]
    assert approval.proposed_action["environment"] == "PROD"

    automation_service.decide(db, approval.id, approve=True, actor=admin, comment=None)
    tick(db, clock)
    run = db.get(WorkflowRun, run.id)
    assert run.status == RunStatus.COMPLETED, run.error
    assert steps(run) == [
        ("trigger", "next"),
        ("ok", "approved"),
        ("load", "success"),
        ("rerun", "success"),
    ]
    assert count(warehouse, "orders") == 1
