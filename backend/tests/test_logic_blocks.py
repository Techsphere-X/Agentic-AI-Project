"""Plan 5 / Phase B: If, Switch, Wait for all, Set value, and per-block retries / error output."""

import pytest
from sqlalchemy.orm import Session

from app.automation.graph import GraphError, catalog, node_ports, validate_graph
from app.automation.types import RunStatus
from app.models.automation import Notification, WorkflowRun
from app.models.user import User
from app.orchestration.airflow import mock as mock_module
from app.services.automation_nodes import compare_values
from tests.test_orchestration import Clock, run_now, tick, workflow

MISSING_CONNECTION = "00000000-0000-0000-0000-000000000000"


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    c = Clock()
    monkeypatch.setattr(mock_module, "clock", c)
    return c


def _manual(*nodes: dict, edges: list[tuple[str, str, str]]) -> dict:
    return {
        "nodes": [{"id": "t", "type": "trigger.manual", "config": {}}, *nodes],
        "edges": [{"from": a, "port": p, "to": b} for a, p, b in edges],
    }


def _notify(node_id: str, title: str = "x") -> dict:
    return {"id": node_id, "type": "notify", "config": {"title": title}}


def _ports(run: WorkflowRun) -> list[tuple[str, str | None]]:
    return [(s.node_id, s.port) for s in run.steps]


def _titles(db: Session) -> list[str]:
    return sorted(n.title for n in db.query(Notification).all())


# ---------------------------------------------------------------------- If / Switch


@pytest.mark.parametrize(
    ("left", "op", "right", "expected"),
    [
        ("12", ">", "9", True),  # numbers compare as numbers, not text
        ("12", "<", "9", False),
        ("failed", "=", "failed", True),
        ("failed", "!=", "success", True),
        ("Connection reset by peer", "contains", "reset", True),
        ("", "is_empty", "", True),
        ("x", "not_empty", "", True),
        ("abc", ">", "1", False),  # text cannot be greater than a number
    ],
)
def test_compare_values(left: str, op: str, right: str, expected: bool) -> None:
    assert compare_values(left, op, right) is expected


def test_if_branches_on_a_value(db: Session, admin: User, clock: Clock) -> None:
    graph = _manual(
        {"id": "set", "type": "logic.set", "config": {"name": "rows", "value": "42"}},
        {
            "id": "if",
            "type": "logic.if",
            "config": {"left": "{{vars.rows}}", "operator": ">", "right": "10"},
        },
        _notify("many", "Many rows: {{vars.rows}}"),
        _notify("few", "Few rows"),
        edges=[
            ("t", "next", "set"),
            ("set", "next", "if"),
            ("if", "true", "many"),
            ("if", "false", "few"),
        ],
    )
    run = run_now(db, workflow(db, graph), admin, clock)
    assert run.status == RunStatus.COMPLETED
    assert ("if", "true") in _ports(run)
    assert _titles(db) == ["Many rows: 42"]


def test_switch_has_an_output_per_case(db: Session, admin: User, clock: Clock) -> None:
    switch = {
        "id": "sw",
        "type": "logic.switch",
        "config": {
            "value": "{{vars.env}}",
            "cases": [{"label": "Production", "equals": "PROD"}, {"equals": "DEV"}],
        },
    }
    graph = _manual(
        {"id": "set", "type": "logic.set", "config": {"name": "env", "value": "PROD"}},
        switch,
        _notify("prod", "prod"),
        _notify("other", "other"),
        edges=[
            ("t", "next", "set"),
            ("set", "next", "sw"),
            ("sw", "Production", "prod"),
            ("sw", "otherwise", "other"),
        ],
    )
    validated = validate_graph(graph)
    assert node_ports("logic.switch", validated.nodes["sw"].config) == (
        "Production",
        "DEV",
        "otherwise",
    )
    run = run_now(db, workflow(db, graph), admin, clock)
    assert ("sw", "Production") in _ports(run)
    assert _titles(db) == ["prod"]

    entry = next(e for e in catalog() if e["type"] == "logic.switch")
    assert entry["dynamic_ports"] == "cases" and entry["ports"] == []


def test_switch_rejects_duplicate_outputs() -> None:
    graph = _manual(
        {
            "id": "sw",
            "type": "logic.switch",
            "config": {"cases": [{"key": "a", "equals": "1"}, {"key": "a", "equals": "2"}]},
        },
        edges=[("t", "next", "sw")],
    )
    with pytest.raises(GraphError, match="Two cases use the output 'a'"):
        validate_graph(graph)


# ---------------------------------------------------------------------- Wait for all


def test_wait_for_all_runs_once_after_every_branch(db: Session, admin: User, clock: Clock) -> None:
    graph = _manual(
        _notify("a", "a"),
        {"id": "slow", "type": "flow.wait", "config": {"minutes": 5}},
        {"id": "join", "type": "logic.join", "config": {}},
        _notify("done", "done"),
        edges=[
            ("t", "next", "a"),
            ("t", "next", "slow"),
            ("a", "next", "join"),
            ("slow", "next", "join"),
            ("join", "next", "done"),
        ],
    )
    run = run_now(db, workflow(db, graph), admin, clock)
    # 'a' arrived, 'slow' is still waiting: the join waits for it.
    assert run.status == RunStatus.WAITING
    assert "join" not in {s.node_id for s in run.steps}

    clock.advance(minutes=5)
    tick(db, clock)
    run = db.get(WorkflowRun, run.id)
    assert run.status == RunStatus.COMPLETED
    assert [s.node_id for s in run.steps].count("join") == 1
    assert _titles(db) == ["a", "done"]


def test_wait_for_all_needs_two_incoming_links() -> None:
    graph = _manual(
        {"id": "join", "type": "logic.join", "config": {}},
        edges=[("t", "next", "join")],
    )
    with pytest.raises(GraphError, match="link at least two blocks into it"):
        validate_graph(graph)


# ---------------------------------------------------------------------- retries & errors


def _broken_run_dag(settings: dict | None = None) -> dict:
    node = {
        "id": "run",
        "type": "pipeline.run_dag",
        "config": {"connection_id": MISSING_CONNECTION, "dag_id": "etl"},
    }
    if settings:
        node["settings"] = settings
    return node


def test_error_output_continues_instead_of_failing(db: Session, admin: User, clock: Clock) -> None:
    graph = _manual(
        _broken_run_dag({"on_error": "output"}),
        _notify("tell", "Could not run: {{last_error}}"),
        edges=[("t", "next", "run"), ("run", "error", "tell")],
    )
    run = run_now(db, workflow(db, graph), admin, clock)
    assert run.status == RunStatus.COMPLETED
    assert ("run", "error") in _ports(run)
    assert _titles(db) == ["Could not run: The block's Airflow connection no longer exists"]


def test_error_port_only_exists_when_chosen() -> None:
    graph = _manual(
        _broken_run_dag(),
        _notify("tell"),
        edges=[("t", "next", "run"), ("run", "error", "tell")],
    )
    with pytest.raises(GraphError, match="no output port 'error'"):
        validate_graph(graph)


def test_retries_then_fails(db: Session, admin: User, clock: Clock) -> None:
    graph = _manual(
        _broken_run_dag({"retries": 2, "retry_delay_seconds": 30}),
        edges=[("t", "next", "run")],
    )
    run = run_now(db, workflow(db, graph), admin, clock)
    assert run.status == RunStatus.WAITING
    assert "Attempt 1 failed" in run.steps[-1].message

    clock.advance(seconds=30)
    tick(db, clock)
    run = db.get(WorkflowRun, run.id)
    assert run.status == RunStatus.WAITING and "Attempt 2 failed" in run.steps[-1].message

    clock.advance(seconds=30)
    tick(db, clock)
    run = db.get(WorkflowRun, run.id)
    assert run.status == RunStatus.FAILED
    assert [s.node_id for s in run.steps].count("run") == 1  # one step, tried three times
    assert run.context["attempts"] == {"run": 2}


def test_run_settings_are_validated_and_kept() -> None:
    graph = _manual(_broken_run_dag({"retries": 9}), edges=[("t", "next", "run")])
    with pytest.raises(GraphError, match="Run settings"):
        validate_graph(graph)
    graph = _manual(_broken_run_dag({"retries": 1}), edges=[("t", "next", "run")])
    assert validate_graph(graph).nodes["run"].settings == {
        "retries": 1,
        "retry_delay_seconds": 60,
        "on_error": "fail",
    }
