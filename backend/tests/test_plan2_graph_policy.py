"""Plan 2 / Phase 2: graph validation, templates, policy, rendering, migrations."""

import copy
from typing import Any

import pytest
from sqlalchemy.orm import Session

from app.automation import policy
from app.automation.graph import GraphError, catalog, graph_warnings, to_raw, validate_graph
from app.automation.render import render
from app.automation.templates import ALL_TEMPLATES, TEMPLATES
from app.models.automation import Workflow
from app.services import automation_service


def simple() -> dict[str, Any]:
    return {
        "nodes": [
            {"id": "t", "type": "trigger.incident", "config": {}},
            {"id": "c", "type": "diagnose.classify_log"},
            {"id": "f", "type": "condition.filter", "config": {"max_occurrences": 2}},
            {"id": "n", "type": "notify", "config": {"message": "hi"}},
        ],
        "edges": [
            {"from": "t", "port": "next", "to": "c"},
            {"from": "c", "port": "next", "to": "f"},
            {"from": "f", "port": "true", "to": "n"},
        ],
    }


@pytest.mark.parametrize("template", TEMPLATES, ids=lambda t: t.key)
def test_every_template_is_valid(template) -> None:  # noqa: ANN001
    graph = validate_graph(template.graph)
    assert graph.trigger.type.startswith("trigger.")
    # Round trip is stable (defaults applied once).
    assert to_raw(validate_graph(to_raw(graph))) == to_raw(graph)


def test_parameterized_template_builds_a_valid_graph() -> None:
    template = next(t for t in ALL_TEMPLATES if t.key == "run-verify-dag")
    graph = validate_graph(
        template.build({"connection_id": "conn", "dag_id": "orders_pipeline"})
    )
    assert graph.trigger.type == "trigger.manual"
    assert graph.nodes["run"].config["dag_id"] == "orders_pipeline"


def test_parameterized_template_rejects_missing_parameters() -> None:
    template = next(t for t in ALL_TEMPLATES if t.key == "run-verify-dag")
    with pytest.raises(ValueError):
        template.build({})


def test_defaults_are_applied() -> None:
    graph = validate_graph(simple())
    assert graph.nodes["t"].config == {"events": ["opened"], "incident_types": []}
    assert graph.nodes["n"].config["channel"] == "in_app"
    assert graph.next_nodes("f", "true") == ["n"]
    assert graph.next_nodes("f", "false") == []


def _problems(raw: Any) -> list[dict[str, str]]:
    with pytest.raises(GraphError) as exc:
        validate_graph(raw)
    return exc.value.problems


def mutate(fn) -> dict[str, Any]:  # noqa: ANN001
    raw = copy.deepcopy(simple())
    fn(raw)
    return raw


@pytest.mark.parametrize(
    ("raw", "fragment"),
    [
        ("nope", "must be an object"),
        ({"nodes": []}, "at least one node"),
        (
            mutate(lambda g: g["nodes"].append({"id": "x", "type": "shell.exec"})),
            "Unknown node type",
        ),
        (mutate(lambda g: g["nodes"].append({"id": "t", "type": "notify"})), "Duplicate node id"),
        (mutate(lambda g: g["nodes"].append({"id": "bad id!", "type": "notify"})), "invalid id"),
        (
            mutate(lambda g: g["nodes"][2].update(config={"min_severity": "HUGE"})),
            "At least this severe",
        ),
        (mutate(lambda g: g["nodes"][3].update(config={"url": "ftp://x"})), "url"),
        (mutate(lambda g: g["nodes"][2].update(config={"bogus": 1})), "bogus"),
        (
            mutate(lambda g: g["edges"].append({"from": "f", "port": "maybe", "to": "n"})),
            "no output port",
        ),
        (
            mutate(lambda g: g["edges"].append({"from": "f", "port": "true", "to": "n"})),
            "already connected to 'n'",
        ),
        (
            mutate(lambda g: g["edges"].append({"from": "f", "port": "false", "to": "ghost"})),
            "unknown node",
        ),
        (mutate(lambda g: g["edges"].append({"from": "f", "port": "false", "to": "t"})), "trigger"),
        (mutate(lambda g: g["edges"].append({"from": "f", "port": "false", "to": "c"})), "loop"),
        (mutate(lambda g: g["edges"].pop()), "not connected to the trigger"),
        (
            mutate(lambda g: g["nodes"].append({"id": "t2", "type": "trigger.incident_stale"})),
            "exactly one trigger",
        ),
        (mutate(lambda g: g["nodes"].pop(0) and g.update(edges=[])), "exactly one trigger"),
    ],
)
def test_invalid_graphs(raw: Any, fragment: str) -> None:
    problems = _problems(raw)
    assert any(fragment.lower() in p["message"].lower() for p in problems), problems


def test_one_output_can_link_to_several_blocks() -> None:
    raw = simple()
    raw["nodes"].append({"id": "u", "type": "incident.update", "config": {"operation": "resolve"}})
    raw["edges"].append({"from": "f", "port": "true", "to": "u"})
    graph = validate_graph(raw)
    assert graph.next_nodes("f", "true") == ["n", "u"]  # no positions: link order
    assert to_raw(validate_graph(to_raw(graph))) == to_raw(graph)


def test_linked_blocks_run_top_to_bottom_on_the_canvas() -> None:
    raw = simple()
    raw["nodes"].append({"id": "u", "type": "incident.update", "config": {"operation": "resolve"}})
    raw["edges"].append({"from": "f", "port": "true", "to": "u"})
    raw["nodes"][3]["position"] = {"x": 0, "y": 200}  # n: lower
    raw["nodes"][4]["position"] = {"x": 0, "y": 50}  # u: higher
    assert validate_graph(raw).next_nodes("f", "true") == ["u", "n"]


def test_catalog_lists_ports_and_schemas() -> None:
    entries = {e["type"]: e for e in catalog()}
    assert entries["approval.request"]["ports"] == ["approved", "rejected"]
    assert "timeout_minutes" in entries["approval.request"]["config_schema"]["properties"]
    assert entries["check.dag_state"]["ports"] == ["ready", "paused", "busy"]


# ---------------------------------------------------------------------- policy


def check(**overrides: Any) -> policy.PolicyDecision:
    values: dict[str, Any] = {
        "environment": "DEV",
        "dry_run": False,
        "approved": False,
        "actions_last_24h": 0,
        "max_per_day": 3,
    }
    values.update(overrides)
    return policy.check_action("action.clear_failed_tasks", **values)


def test_policy_rules() -> None:
    assert check().allowed
    prod = check(environment="PROD")
    assert not prod.allowed and "approval" in prod.reasons[0]
    assert check(environment="PROD", approved=True).allowed
    limited = check(actions_last_24h=3)
    assert not limited.allowed and "Rate limit" in limited.reasons[0]
    dry = check(environment="PROD", dry_run=True, actions_last_24h=99)
    assert dry.allowed and dry.simulate


def test_render_is_safe() -> None:
    values = {"incident": {"title": "t1", "count": 3}, "x": None}
    assert render("{{incident.title}} x{{ incident.count }}", values) == "t1 x3"
    assert render("{{missing.key}}|{{x}}|{{incident.title.upper}}", values) == "||"
    assert render("{{__class__}} {incident.title}", values) == " {incident.title}"


# ---------------------------------------------------------------------- seeding


def test_seed_templates_is_idempotent_and_disabled(db: Session) -> None:
    created = automation_service.seed_templates(db)
    assert {w.key for w in created} == {t.key for t in TEMPLATES}
    assert all(not w.enabled for w in created)
    assert automation_service.seed_templates(db) == []
    assert db.query(Workflow).count() == len(TEMPLATES)


# ---------------------------------------------------------------------- editor metadata


def test_catalog_describes_every_port_and_setting() -> None:
    for entry in catalog():
        assert entry["stage"] in {"when", "check", "diagnose", "decide", "act", "verify", "notify"}
        assert set(entry["port_tones"]) == set(entry["ports"]), entry["type"]
        assert len(entry["summary"]) <= 60, entry["type"]  # one line in the editor
        assert set(entry["port_labels"]) == set(entry["ports"]), entry["type"]
        assert all(t in {"good", "bad", "neutral"} for t in entry["port_tones"].values())
        for name, prop in entry["config_schema"].get("properties", {}).items():
            assert prop.get("x-hidden") or "x-label" in prop, f"{entry['type']}.{name}"
    by_type = {e["type"]: e for e in catalog()}
    assert by_type["database.check"]["port_tones"] == {"pass": "good", "fail": "bad"}
    assert by_type["pipeline.run_dag"]["is_action"] is True
    assert "diagnosis.label" in by_type["diagnose.classify_log"]["outputs"]


def test_validation_reports_every_problem_at_once() -> None:
    raw = simple()
    raw["nodes"][2]["config"] = {"min_severity": "HUGE"}  # a settings error on 'f'
    raw["nodes"].append({"id": "x", "type": "notify", "config": {"title": ""}})  # and on 'x'
    problems = _problems(raw)
    assert {p.get("node") for p in problems} >= {"f", "x"}
    severity = next(p for p in problems if p.get("node") == "f")
    assert severity["message"].startswith("At least this severe:")
    assert severity["field"] == "min_severity"


def test_loop_and_trigger_mismatch_are_reported_together() -> None:
    raw = simple()
    raw["nodes"][0] = {"id": "t", "type": "trigger.manual", "config": {}}
    raw["edges"].append({"from": "f", "port": "false", "to": "c"})
    messages = " | ".join(p["message"] for p in _problems(raw))
    assert "loop" in messages
    assert "needs an incident trigger" in messages


def test_unknown_placeholders_are_warned_about() -> None:
    raw = simple()
    raw["nodes"][3]["config"] = {
        "message": "{{diagnosis.label}} {{incident.title}} {{results.f.value}} {{nope.x}}"
    }
    warnings = graph_warnings(validate_graph(raw))
    text = " | ".join(w["message"] for w in warnings)
    assert "{{nope.x}}" in text
    assert "{{results.f.value}}" not in text  # 'f' runs before the notification
    assert "{{diagnosis.label}}" not in text  # provided by "Classify the failure"
    assert "{{incident.title}}" not in text
    raw["nodes"][3]["config"] = {"message": "{{results.later.value}}"}
    raw["nodes"].append({"id": "later", "type": "flow.wait", "config": {}})
    raw["edges"].append({"from": "n", "port": "next", "to": "later"})
    (warning,) = graph_warnings(validate_graph(raw))
    assert "does not run before this one" in warning["message"]
