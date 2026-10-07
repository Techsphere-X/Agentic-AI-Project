"""The "Choose the fix" block (decide.choose_fix): the decision table and the engine wiring."""

import json
from collections.abc import Callable
from dataclasses import replace
from typing import Any

import anyio
import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.automation.graph import catalog, validate_graph
from app.automation.remediation import (
    Advice,
    AdvisorUnavailable,
    Facts,
    Settings,
    advise,
    choose_fix,
)
from app.automation.templates import TEMPLATES_BY_KEY
from app.automation.types import RunStatus, TriggerEvent
from app.connectors.decision_llm import DecisionLLM
from app.core.config import get_settings
from app.detection.types import IncidentResolution, IncidentStatus
from app.models.airflow import DeploymentEnvironment
from app.models.audit_log import AuditLog
from app.models.automation import WorkflowRun
from app.models.incident import Incident
from app.models.user import User
from app.orchestration.airflow import mock as mock_module
from app.orchestration.airflow.base import AdapterConfig
from app.orchestration.airflow.mock import MockAirflowAdapter
from app.services import automation_nodes, automation_service
from tests.test_plan2_engine import (
    BASE_URL,
    Clock,
    detect,
    enable,
    events,
    make_env,
    only,
    ports,
    tick,
    writes,
)

FIXES = ("retry", "rerun", "wait", "pause", "escalate", "ignore")

# ---------------------------------------------------------------------- decision table

BASE = Facts(
    incident_type="DAG_RUN_FAILED",
    incident_status="OPEN",
    dag_id="orders",
    environment="PROD",
    severity="MEDIUM",
    occurrences=1,
    category="TRANSIENT_NETWORK",
    confidence=0.9,
    source="regex",
    has_failed_run=True,
    fix_attempts=0,
    failed_verifications=0,
    actions_today=0,
    max_actions_per_day=3,
    dag_state="ready",
)


def pick(settings: Settings | None = None, **changes: Any) -> tuple[str, str]:
    choice = choose_fix(replace(BASE, **changes), settings or Settings())
    assert choice.fix in choice.allowed
    return choice.fix, choice.rule


@pytest.mark.parametrize(
    ("changes", "expected"),
    [
        ({}, ("retry", "retry_helps")),
        ({"category": "TIMEOUT"}, ("retry", "retry_helps")),
        ({"category": "RESOURCE"}, ("retry", "retry_helps")),
        ({"has_failed_run": False}, ("rerun", "retry_helps")),
        ({"category": "UPSTREAM_MISSING"}, ("wait", "missing_input")),
        ({"incident_type": "SLA_MISSED", "has_failed_run": False}, ("rerun", "sla_missed")),
        ({"category": "DATA_INTEGRITY"}, ("escalate", "needs_fix")),
        ({"category": "SCHEMA"}, ("escalate", "needs_fix")),
        ({"category": "CODE_BUG"}, ("escalate", "needs_fix")),
        ({"category": "AUTH"}, ("escalate", "needs_fix")),
        ({"category": "UNKNOWN", "confidence": 0.0}, ("retry", "unknown_try_once")),
        ({"category": "UNKNOWN", "fix_attempts": 1}, ("escalate", "unknown")),
        ({"fix_attempts": 2, "failed_verifications": 2}, ("escalate", "out_of_attempts")),
        ({"actions_today": 3}, ("escalate", "daily_limit")),
        ({"incident_status": "RESOLVED"}, ("ignore", "already_resolved")),
        ({"dag_state": "paused"}, ("ignore", "dag_paused")),
        ({"dag_state": "busy"}, ("retry", "retry_helps")),
        ({"dag_state": "busy", "has_failed_run": False}, ("wait", "run_active")),
        ({"dag_state": "busy", "category": "AUTH"}, ("escalate", "needs_fix")),
        ({"category": "NOT_A_CATEGORY"}, ("retry", "unknown_try_once")),
    ],
)
def test_default_rules(changes: dict[str, Any], expected: tuple[str, str]) -> None:
    assert pick(**changes) == expected


def test_settings_change_the_choice() -> None:
    assert pick(Settings(max_attempts=3), fix_attempts=2) == ("retry", "retry_helps")
    assert pick(Settings(retry_unknown_once=False), category="UNKNOWN") == ("escalate", "unknown")
    # Below the minimum confidence the cause counts as unknown; operator corrections always pass.
    assert pick(Settings(min_confidence_pct=95)) == ("retry", "unknown_try_once")
    assert pick(Settings(min_confidence_pct=95), confidence=0.5, source="operator")[0] == "retry"
    bad = Settings(pause_on_bad_data=True)
    assert pick(bad, category="DATA_INTEGRITY") == ("pause", "stop_bad_data")
    assert pick(bad, category="CODE_BUG") == ("escalate", "needs_fix")
    assert pick(Settings(pause_after_failures=3), occurrences=3) == ("pause", "keeps_failing")


def test_allowed_fixes_are_the_guard_rail() -> None:
    def allowed(settings: Settings | None = None, **changes: Any) -> tuple[str, ...]:
        return choose_fix(replace(BASE, **changes), settings or Settings()).allowed

    assert allowed() == ("retry", "rerun", "wait", "escalate")
    assert allowed(has_failed_run=False) == ("rerun", "wait", "escalate")
    # Never a retry or rerun when it cannot help, the attempts are spent, or the budget is used.
    for changes in ({"category": "DATA_INTEGRITY"}, {"fix_attempts": 2}, {"actions_today": 3}):
        assert allowed(**changes) == ("escalate",)
    assert allowed(Settings(pause_on_bad_data=True), category="AUTH") == ("escalate", "pause")
    # A run in progress rules out starting another one, not retrying the failed one.
    assert allowed(dag_state="busy") == ("retry", "wait", "escalate")
    # Fixed states leave exactly one answer.
    assert allowed(incident_status="RESOLVED") == ("ignore",)
    assert allowed(dag_state="paused") == ("ignore",)


# ---------------------------------------------------------------------- catalog / validation


def test_catalog_entry() -> None:
    entry = {e["type"]: e for e in catalog()}["decide.choose_fix"]
    assert entry["label"] == "Choose the fix"
    assert entry["category"] == "logic"
    assert tuple(entry["ports"]) == FIXES
    assert entry["needs_incident"] is True
    assert {"max_attempts", "pause_on_bad_data", "check_dag_state"} <= set(
        entry["config_schema"]["properties"]
    )


def test_choose_fix_template_validates() -> None:
    graph = validate_graph(TEMPLATES_BY_KEY["choose-fix"].build())
    wired = {port for (src, port) in graph.edges if src == "decide"}
    assert wired == set(FIXES)


# ---------------------------------------------------------------------- engine


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


def decision_graph(wired: tuple[str, ...] = FIXES, **config: Any) -> dict[str, Any]:
    nodes = [
        {"id": "t", "type": "trigger.incident", "config": {}},
        {"id": "d", "type": "decide.choose_fix", "config": config},
    ]
    edges = [{"from": "t", "port": "next", "to": "d"}]
    for port in wired:
        nodes.append(
            {
                "id": f"n_{port}",
                "type": "notify",
                "config": {"title": f"{port}: {{{{decision.rule}}}}"},
            }
        )
        edges.append({"from": "d", "port": port, "to": f"n_{port}"})
    return {"nodes": nodes, "edges": edges}


def install(db: Session, admin: User, raw: dict[str, Any]) -> None:
    workflow = automation_service.create_workflow(db, actor=admin, name="Decide", graph=raw)
    workflow.enabled = True
    db.commit()


def test_transient_failure_chooses_retry(db: Session, admin: User, partner_clock: Clock) -> None:
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    install(db, admin, decision_graph())
    detect(db, partner_clock)
    tick(db, partner_clock)
    run = only(db, WorkflowRun)
    assert ports(run)[:2] == ["t.next", "d.retry"]
    decision = run.context["decision"]
    assert (decision["fix"], decision["rule"]) == ("retry", "retry_helps")
    assert decision["facts"]["category"] == "TRANSIENT_NETWORK"
    # The mock's next scheduled run is in progress: that rules out a rerun, not a retry.
    assert decision["facts"]["dag_state"] == "busy"
    assert "rerun" not in decision["allowed"]
    assert run.context["results"]["d"]["fix"] == "retry"
    assert "fix_chosen" in events(only(db, Incident))
    audit = db.scalar(select(AuditLog).where(AuditLog.action == "automation.fix_chosen"))
    assert audit is not None and audit.details["rule"] == "retry_helps"


def test_bad_data_is_handed_to_a_person(db: Session, admin: User, orders_clock: Clock) -> None:
    make_env(db, DeploymentEnvironment.DEV, "orders_pipeline")
    install(db, admin, decision_graph())
    detect(db, orders_clock)
    tick(db, orders_clock)
    run = only(db, WorkflowRun)
    assert ports(run)[1] == "d.escalate"
    assert run.context["decision"]["rule"] == "needs_fix"
    assert "Retrying will not help" in run.context["last_error"]


def test_unconnected_choice_goes_to_a_person(
    db: Session, admin: User, partner_clock: Clock
) -> None:
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    install(db, admin, decision_graph(wired=("escalate",)))
    detect(db, partner_clock)
    tick(db, partner_clock)
    run = only(db, WorkflowRun)
    assert ports(run)[1] == "d.escalate"
    assert run.context["decision"]["chosen"] == "retry"
    assert "Nothing is connected to 'retry'" in run.context["decision"]["reason"]


def test_paused_dag_is_left_alone(db: Session, admin: User, partner_clock: Clock) -> None:
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    install(db, admin, decision_graph())
    detect(db, partner_clock)
    adapter = MockAirflowAdapter(AdapterConfig(base_url=BASE_URL), simulate_latency=False)
    anyio.run(adapter.set_dag_paused, "partner_api_sync", True)
    tick(db, partner_clock)
    assert ports(only(db, WorkflowRun))[1] == "d.ignore"


def test_template_retries_verifies_and_resolves(db: Session, partner_clock: Clock) -> None:
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    enable(db, "choose-fix")
    detect(db, partner_clock)
    tick(db, partner_clock)
    run = only(db, WorkflowRun)
    incident = only(db, Incident)
    assert ports(run) == [
        "trigger.next",
        "diagnose.next",
        "decide.retry",
        "approve_retry.approved",
        "retry.success",
        "verify.waiting",
    ]
    assert list(writes().cleared) == [("partner_api_sync", incident.last_run_id)]

    partner_clock.advance(seconds=mock_module.RERUN_SECONDS + 1)
    tick(db, partner_clock)
    assert run.status == RunStatus.COMPLETED
    assert incident.status == IncidentStatus.RESOLVED
    assert incident.resolution == IncidentResolution.AUTO_REMEDIATED
    assert "usually passes" in (incident.resolution_note or "")


def test_earlier_fix_attempts_are_counted(db: Session, partner_clock: Clock) -> None:
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    workflow = enable(db, "choose-fix")
    detect(db, partner_clock)
    tick(db, partner_clock)
    partner_clock.advance(seconds=mock_module.RERUN_SECONDS + 1)
    tick(db, partner_clock)
    incident = only(db, Incident)

    # Run the same workflow on the same incident again: one fix is already on record.
    second = automation_service._add_run(
        db, workflow, TriggerEvent.RECURRED, "again", incident_id=incident.id
    )
    db.commit()
    tick(db, partner_clock)
    db.refresh(second)
    facts = second.context["decision"]["facts"]
    assert facts["fix_attempts"] == 1
    assert second.context["decision"]["fix"] == "ignore"  # resolved by the first run


# ---------------------------------------------------------------------- local AI advice


def advice(fix: str = "rerun", confidence: float = 0.9) -> Advice:
    return Advice(fix, "the partner API was down briefly", confidence, "qwen3:4b")


def test_ai_off_and_fixed_states_never_ask() -> None:
    def never() -> Advice:
        raise AssertionError("the model must not be asked")

    choice = choose_fix(BASE, Settings())
    assert advise(choice, "off", never).ai["status"] == "off"
    fixed = choose_fix(replace(BASE, category="AUTH"), Settings())
    assert fixed.allowed == ("escalate",)
    assert advise(fixed, "decide", never).ai["status"] == "skipped"
    assert advise(choice, "decide", None).ai["status"] == "disabled"


def test_ai_suggests_or_decides_within_the_allowed_fixes() -> None:
    choice = choose_fix(BASE, Settings())  # rules: retry; allowed retry/rerun/wait/escalate
    suggested = advise(choice, "suggest", advice)
    assert (suggested.fix, suggested.ai["status"], suggested.ai["routed"]) == (
        "retry",
        "used",
        False,
    )
    assert suggested.ai["fix"] == "rerun"
    decided = advise(choice, "decide", advice)
    assert (decided.fix, decided.ai["routed"]) == ("rerun", True)
    assert decided.reason.startswith("AI (qwen3:4b): ")


def test_ai_answers_the_rules_cannot_accept_fall_back_to_the_rules() -> None:
    choice = choose_fix(BASE, Settings())
    rejected = advise(choice, "decide", lambda: advice("pause"))  # pause is not allowed here
    assert (rejected.fix, rejected.ai["status"]) == ("retry", "rejected")
    low = advise(choice, "decide", lambda: advice(confidence=0.4), min_confidence_pct=60)
    assert (low.fix, low.ai["status"]) == ("retry", "low_confidence")

    def down() -> Advice:
        raise AdvisorUnavailable("ConnectError")

    unavailable = advise(choice, "decide", down)
    assert (unavailable.fix, unavailable.ai["status"]) == ("retry", "unavailable")
    assert unavailable.reason == choice.reason


def llm(handler: Callable[[httpx.Request], httpx.Response]) -> DecisionLLM:
    settings = get_settings().model_copy(
        update={"DECISION_LLM_ENABLED": True, "DECISION_LLM_BREAKER_FAILURES": 2}
    )
    return DecisionLLM(settings, transport=httpx.MockTransport(handler))


def ollama_reply(content: str) -> httpx.Response:
    return httpx.Response(200, json={"message": {"role": "assistant", "content": content}})


def test_ollama_client_constrains_and_parses_the_answer() -> None:
    sent: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))
        return ollama_reply('{"fix": "wait", "reason": "Input is late.", "confidence": 1.4}')

    choice = choose_fix(BASE, Settings())
    log = "password=hunter2\n" + "\n".join(f"line {i}" for i in range(400))
    result = llm(handler).advise(BASE, choice, {"label": "Network glitch"}, [log])
    assert (result.fix, result.reason, result.confidence) == ("wait", "Input is late.", 1.0)
    body = sent[0]
    assert body["think"] is False and body["stream"] is False
    assert body["format"]["properties"]["fix"]["enum"] == list(choice.allowed)
    prompt = body["messages"][1]["content"]
    assert "hunter2" not in prompt and "line 399" in prompt and "line 100" not in prompt
    # The rules' own pick is withheld so the model judges independently.
    assert "rules_choice" not in prompt and choice.rule not in prompt


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(500, json={"error": "boom"}),
        ollama_reply("not json"),
        ollama_reply('{"fix": "retry"}'),
    ],
)
def test_ollama_client_failures_are_unavailable(response: httpx.Response) -> None:
    client = llm(lambda _request: response)
    with pytest.raises(AdvisorUnavailable):
        client.advise(BASE, choose_fix(BASE, Settings()), {}, [])


def test_ollama_breaker_opens_after_repeated_failures() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("refused", request=request)

    client = llm(handler)
    choice = choose_fix(BASE, Settings())
    for _ in range(3):
        with pytest.raises(AdvisorUnavailable):
            client.advise(BASE, choice, {}, [])
    assert calls == 2  # the third call failed fast


class FakeAdvisor:
    def __init__(self, fix: str) -> None:
        self.fix = fix
        self.calls = 0

    def advise(
        self, facts: Facts, choice: Any, diagnosis: dict[str, Any], logs: list[str]
    ) -> Advice:
        self.calls += 1
        return Advice(self.fix, "the next run is already going; let it finish", 0.8, "fake")


def test_ai_decides_the_route_in_a_workflow(
    db: Session, admin: User, partner_clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake = FakeAdvisor("wait")
    monkeypatch.setattr(automation_nodes, "decision_advisor", lambda _settings: fake)
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    install(db, admin, decision_graph(ai_mode="decide"))
    detect(db, partner_clock)
    tick(db, partner_clock)
    run = only(db, WorkflowRun)
    assert ports(run)[1] == "d.wait"
    decision = run.context["decision"]
    assert (decision["rules_fix"], decision["fix"]) == ("retry", "wait")
    assert decision["ai"]["status"] == "used" and decision["ai"]["routed"] is True
    assert fake.calls == 1


def test_ai_suggestion_does_not_change_the_route(
    db: Session, admin: User, partner_clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(automation_nodes, "decision_advisor", lambda _settings: FakeAdvisor("wait"))
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    install(db, admin, decision_graph(ai_mode="suggest"))
    detect(db, partner_clock)
    tick(db, partner_clock)
    run = only(db, WorkflowRun)
    assert ports(run)[1] == "d.retry"
    assert run.context["decision"]["ai"]["fix"] == "wait"
