"""Plan 2 / Phase 2: graph validation, templates, policy, rendering, migrations."""

import copy
from typing import Any

import pytest
from sqlalchemy.orm import Session

from app.automation import policy
from app.automation.graph import (
    DagWatch,
    Graph,
    GraphError,
    catalog,
    graph_warnings,
    monitoring_warnings,
    overlap_warnings,
    to_raw,
    validate_graph,
)
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
    assert graph.nodes["t"].config == {
        "events": ["opened"],
        "incident_types": [],
        "dag_ids": [],
        "environments": [],
        "min_severity": None,
    }
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


def test_result_keys_a_block_does_not_record_are_warned_about() -> None:
    raw = {
        "nodes": [
            {"id": "t", "type": "trigger.incident", "config": {}},
            {"id": "logs", "type": "analyze.task_logs", "config": {}},
            {"id": "n", "type": "notify", "config": {}},
        ],
        "edges": [
            {"from": "t", "port": "next", "to": "logs"},
            {"from": "logs", "port": "analyzed", "to": "n"},
        ],
    }
    raw["nodes"][2]["config"] = {"message": "{{results.logs.headline}} {{results.logs.analyzed}}"}
    (warning,) = graph_warnings(validate_graph(raw))
    assert "{{results.logs.analyzed}}" in warning["message"]
    assert "does not record 'analyzed'" in warning["message"]


def trigger_graph(node_type: str = "trigger.incident", **config: Any) -> Graph:
    return validate_graph({"nodes": [{"id": "t", "type": node_type, "config": config}]})


WATCHED = DagWatch(detect_failures=True, has_sla=True)


def test_an_unscoped_trigger_has_no_monitoring_warnings() -> None:
    assert monitoring_warnings(trigger_graph(), {}) == []
    assert monitoring_warnings(trigger_graph("trigger.manual"), {}) == []


def test_a_dag_detection_will_not_report_is_warned_about() -> None:
    graph = trigger_graph(dag_ids=["orders"])
    (warning,) = monitoring_warnings(graph, {})
    assert warning == {
        "message": "'orders' is not monitored, so this workflow will not start for it",
        "node": "t",
    }
    assert monitoring_warnings(graph, {"orders": WATCHED}) == []
    dag_run = trigger_graph("trigger.dag_run", dag_ids=["orders"])
    assert "not monitored" in monitoring_warnings(dag_run, {})[0]["message"]


def test_detection_settings_are_checked_against_the_incident_types() -> None:
    no_failures = {"orders": DagWatch(detect_failures=False, has_sla=True)}
    no_sla = {"orders": DagWatch(detect_failures=True, has_sla=False)}

    failures = trigger_graph(dag_ids=["orders"], incident_types=["DAG_RUN_FAILED"])
    assert "Failure detection is off" in monitoring_warnings(failures, no_failures)[0]["message"]
    assert monitoring_warnings(failures, no_sla) == []

    sla = trigger_graph(dag_ids=["orders"], incident_types=["SLA_MISSED"])
    assert monitoring_warnings(sla, no_failures) == []
    assert "has no SLA" in monitoring_warnings(sla, no_sla)[0]["message"]

    assert monitoring_warnings(trigger_graph(dag_ids=["orders"]), no_sla) == []  # any type


@pytest.mark.parametrize(
    ("ours", "theirs", "overlap"),
    [
        ({}, {}, True),  # two catch-alls
        ({"dag_ids": ["a"]}, {}, True),  # a dedicated workflow and a catch-all
        ({"dag_ids": ["a"]}, {"dag_ids": ["a", "b"]}, True),
        ({"dag_ids": ["a"]}, {"dag_ids": ["b"]}, False),
        ({"events": ["opened"]}, {"events": ["recurred"]}, False),
        ({"incident_types": ["SLA_MISSED"]}, {"incident_types": ["DAG_RUN_FAILED"]}, False),
        ({"environments": ["DEV"]}, {"environments": ["PROD"]}, False),
    ],
)
def test_overlapping_triggers_are_warned_about(ours: dict, theirs: dict, overlap: bool) -> None:
    other = trigger_graph(**theirs).trigger
    warnings = overlap_warnings(trigger_graph(**ours), [("Other", other)])
    assert bool(warnings) is overlap


def test_different_trigger_types_do_not_overlap() -> None:
    other = trigger_graph("trigger.dag_run").trigger
    assert overlap_warnings(trigger_graph(), [("Other", other)]) == []
    manual = trigger_graph("trigger.manual")
    assert overlap_warnings(manual, [("Other", manual.trigger)]) == []


def test_dag_failure_template_is_scoped_to_one_dag() -> None:
    template = next(t for t in ALL_TEMPLATES if t.key == "dag-failure-handler")
    graph = validate_graph(template.build({"dag_id": "orders", "send_via": "channel"}))

    assert graph.trigger.config["dag_ids"] == ["orders"]
    assert graph.trigger.config["incident_types"] == ["DAG_RUN_FAILED"]
    assert graph.next_nodes("trigger", "next") == ["logs"]
    assert graph.next_nodes("logs", "analyzed") == ["diagnose"]
    assert graph.next_nodes("logs", "no_logs") == ["diagnose"]
    assert graph.nodes["notify_fail"].config["send_via"] == "channel"
    assert graph_warnings(graph) == []
    with pytest.raises(ValueError):
        template.build({})


def test_a_missing_choice_is_marked_and_keeps_the_other_checks() -> None:
    raw = {
        "nodes": [
            {"id": "t", "type": "trigger.manual", "config": {}},
            {"id": "r", "type": "pipeline.run_dag", "config": {}},
            {"id": "lost", "type": "notify", "config": {}},
        ],
        "edges": [{"from": "t", "port": "next", "to": "r"}],
    }
    problems = _problems(raw)
    missing = {"message": "Choose an Airflow connection", "node": "r", "kind": "incomplete"}
    assert missing in problems
    assert any(p.get("node") == "lost" and "not connected" in p["message"] for p in problems)
