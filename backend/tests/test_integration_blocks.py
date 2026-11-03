"""Plan 5 / Phase E: HTTP request, Run another workflow, For each row."""

import json
from pathlib import Path

import httpx
import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from app.automation.graph import validate_graph
from app.automation.types import RunStatus
from app.connectors import database as db_connector
from app.models.automation import Notification, Workflow, WorkflowRun
from app.models.user import User
from app.orchestration.airflow import mock as mock_module
from app.services import automation_nodes, automation_service
from tests.test_orchestration import Clock, database, run_now, tick

calls: list[httpx.Request] = []


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    c = Clock()
    monkeypatch.setattr(mock_module, "clock", c)
    return c


@pytest.fixture
def api(monkeypatch: pytest.MonkeyPatch) -> list[httpx.Request]:
    """A fake external API: /ok answers 200 with JSON, anything else 500."""
    calls.clear()

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path == "/ok":
            return httpx.Response(200, json={"ticket": "T-1", "got": request.content.decode()})
        return httpx.Response(500, text="boom")

    monkeypatch.setattr(automation_nodes, "webhook_transport", httpx.MockTransport(handler))
    return calls


@pytest.fixture
def warehouse(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    engine = create_engine(f"sqlite:///{tmp_path / 'warehouse.db'}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE tables_to_check (name TEXT, min_rows INTEGER)"))
        conn.execute(text("INSERT INTO tables_to_check VALUES ('orders', 5), ('users', 1)"))
    monkeypatch.setattr(db_connector, "engine_factory", lambda url, args: create_engine(engine.url))
    return engine


def _wf(db: Session, graph: dict, name: str, *, enabled: bool = False) -> Workflow:
    validate_graph(graph)
    wf = Workflow(name=name, graph=graph, enabled=enabled)
    db.add(wf)
    db.commit()
    return wf


def _manual(*nodes: dict, edges: list[tuple[str, str, str]]) -> dict:
    return {
        "nodes": [{"id": "t", "type": "trigger.manual", "config": {}}, *nodes],
        "edges": [{"from": a, "port": p, "to": b} for a, p, b in edges],
    }


def _http(url: str, method: str = "POST", environment: str = "DEV", **extra: object) -> dict:
    config = {"method": method, "url": url, "environment": environment, **extra}
    return {"id": "call", "type": "http.request", "config": config}


def _notify(node_id: str, title: str) -> dict:
    return {"id": node_id, "type": "notify", "config": {"title": title}}


def _titles(db: Session) -> list[str]:
    db.expire_all()
    return sorted(n.title for n in db.scalars(select(Notification)))


# ---------------------------------------------------------------------- HTTP request


def test_http_request_sends_and_exposes_the_answer(
    db: Session, admin: User, clock: Clock, api: list
) -> None:
    graph = _manual(
        _http(
            "https://api.example.com/ok",
            headers="Authorization: Bearer x\nX-Run: {{workflow.name}}",
            body='{"name": "{{workflow.name}}"}',
        ),
        _notify("ok", "Ticket {{results.call.body.ticket}} ({{results.call.status}})"),
        edges=[("t", "next", "call"), ("call", "success", "ok")],
    )
    run = run_now(db, _wf(db, graph, "Ticket"), admin, clock)
    assert run.status == RunStatus.COMPLETED
    [request] = api
    assert request.method == "POST"
    assert request.headers["X-Run"] == "Ticket"
    assert json.loads(request.content) == {"name": "Ticket"}
    assert _titles(db) == ["Ticket T-1 (200)"]


def test_http_request_failures_take_the_failed_output(
    db: Session, admin: User, clock: Clock, api: list
) -> None:
    graph = _manual(
        _http("https://api.example.com/broken"),
        _notify("bad", "Got {{results.call.status}}: {{last_error}}"),
        edges=[("t", "next", "call"), ("call", "failed", "bad")],
    )
    run_now(db, _wf(db, graph, "Broken"), admin, clock)
    assert _titles(db) == ["Got 500: Could not POST https://api.example.com/broken: HTTP 500"]


def test_http_changes_in_prod_need_an_approval_but_gets_do_not(
    db: Session, admin: User, clock: Clock, api: list
) -> None:
    post = _manual(
        _http("https://api.example.com/ok", environment="PROD"),
        edges=[("t", "next", "call")],
    )
    run = run_now(db, _wf(db, post, "Prod post"), admin, clock)
    assert (
        run.steps[-1].port == "failed" and "require an approved approval" in run.steps[-1].message
    )
    assert api == []

    get = _manual(
        _http("https://api.example.com/ok", method="GET", environment="PROD"),
        edges=[("t", "next", "call")],
    )
    run = run_now(db, _wf(db, get, "Prod get"), admin, clock)
    assert run.steps[-1].port == "success" and len(api) == 1


def test_http_hosts_can_be_limited(
    db: Session, admin: User, clock: Clock, api: list, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.core.config import get_settings

    monkeypatch.setattr(get_settings(), "AUTOMATION_WEBHOOK_ALLOWED_HOSTS", ["hooks.example.com"])
    graph = _manual(_http("https://api.example.com/ok"), edges=[("t", "next", "call")])
    run = run_now(db, _wf(db, graph, "Blocked"), admin, clock)
    assert "not an allowed host" in run.steps[-1].message and api == []


# ---------------------------------------------------------------------- other workflows


def _child(db: Session) -> Workflow:
    return _wf(
        db,
        _manual(_notify("hi", "Hi {{trigger.body.who}}"), edges=[("t", "next", "hi")]),
        "Greeter",
    )


def test_run_another_workflow_and_wait_for_it(db: Session, admin: User, clock: Clock) -> None:
    child = _child(db)
    parent = _wf(
        db,
        _manual(
            {
                "id": "call",
                "type": "flow.call_workflow",
                "config": {"workflow_id": str(child.id), "input": '{"who": "{{workflow.name}}"}'},
            },
            _notify("after", "Child {{results.call.status}}"),
            edges=[("t", "next", "call"), ("call", "success", "after")],
        ),
        "Caller",
    )
    run = run_now(db, parent, admin, clock)
    assert run.status == RunStatus.WAITING  # the child runs on the next tick

    clock.advance(seconds=60)
    tick(db, clock)
    clock.advance(seconds=60)
    tick(db, clock)
    run = db.get(WorkflowRun, run.id)
    assert run.status == RunStatus.COMPLETED
    child_run = db.scalars(select(WorkflowRun).where(WorkflowRun.workflow_id == child.id)).one()
    assert child_run.trigger_event == "called"
    assert child_run.context["parent"]["run_id"] == str(run.id)
    assert _titles(db) == ["Child COMPLETED", "Hi Caller"]


def test_a_workflow_cannot_run_itself_or_a_dag_workflow(
    db: Session, admin: User, clock: Clock
) -> None:
    looped = _wf(db, _manual(_notify("x", "x"), edges=[("t", "next", "x")]), "Loop")
    looped.graph = _manual(
        {"id": "call", "type": "flow.call_workflow", "config": {"workflow_id": str(looped.id)}},
        edges=[("t", "next", "call")],
    )
    db.commit()
    run = run_now(db, looped, admin, clock)
    assert run.status == RunStatus.FAILED and "cannot run itself" in run.error

    dag = _wf(
        db,
        {
            "nodes": [
                {
                    "id": "t",
                    "type": "trigger.dag",
                    "config": {"connection_id": "c1", "dag_id": "etl"},
                }
            ],
            "edges": [],
        },
        "DAG flow",
    )
    caller = _wf(
        db,
        _manual(
            {"id": "call", "type": "flow.call_workflow", "config": {"workflow_id": str(dag.id)}},
            edges=[("t", "next", "call")],
        ),
        "Caller",
    )
    run = run_now(db, caller, admin, clock)
    assert "does not start by hand or by webhook" in run.error


def test_for_each_row_runs_the_workflow_per_row(
    db: Session, admin: User, clock: Clock, warehouse
) -> None:
    child = _wf(
        db,
        _manual(
            _notify("check", "Check {{trigger.body.name}} >= {{trigger.body.min_rows}}"),
            edges=[("t", "next", "check")],
        ),
        "Check one table",
    )
    parent = _wf(
        db,
        _manual(
            {
                "id": "each",
                "type": "flow.for_each",
                "config": {
                    "connection_id": database(db),
                    "sql": "SELECT name, min_rows FROM tables_to_check ORDER BY name",
                    "workflow_id": str(child.id),
                },
            },
            _notify("done", "Checked {{results.each.rows}} tables"),
            edges=[("t", "next", "each"), ("each", "done", "done")],
        ),
        "Check all tables",
    )
    run = run_now(db, parent, admin, clock)
    for _ in range(6):  # each row: the child runs, then the parent sees it finished
        clock.advance(seconds=60)
        tick(db, clock)
    run = db.get(WorkflowRun, run.id)
    assert run.status == RunStatus.COMPLETED
    assert _titles(db) == ["Check orders >= 5", "Check users >= 1", "Checked 2 tables"]
    assert automation_service.MAX_CALL_DEPTH == 3
