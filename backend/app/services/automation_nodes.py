"""Executors for workflow node types (see app/automation/graph.py for the catalog).

Each executor takes a NodeContext and the node, performs its work (policy-checked for actions),
and returns a NodeResult naming the output port to follow, or a time to wait until.
"""

import json
import logging
import operator
import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlsplit

import httpx
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.automation import policy, remediation
from app.automation.graph import ACTION_TYPES, Graph, Node
from app.automation.render import render
from app.automation.types import (
    ApprovalStatus,
    DagRunCheckStatus,
    NotificationLevel,
    TriggerEvent,
)
from app.connectors import database as db_connector
from app.connectors.decision_llm import DecisionLLM, get_decision_llm
from app.connectors.notify import Message
from app.core.config import Settings
from app.core.exceptions import AppError
from app.detection import severity
from app.detection.evidence import scrub
from app.detection.types import (
    IncidentResolution,
    IncidentSeverity,
    IncidentStatus,
    IncidentType,
)
from app.diagnosis.log_classifier import RETRYABLE, FailureCategory
from app.models.airflow import AirflowConnection, AirflowTriggerReservation, MonitoredDag
from app.models.automation import (
    Approval,
    DagRunCheck,
    Notification,
    WorkflowRun,
    WorkflowStep,
)
from app.models.database_connection import DatabaseConnection
from app.models.incident import Incident
from app.models.notification_channel import NotificationChannel
from app.orchestration.airflow.base import (
    AirflowAdapter,
    AirflowAdapterError,
    ConnectionStatus,
    ConnectionTestResult,
)
from app.orchestration.airflow.factory import run_async
from app.services import (
    airflow_service,
    audit_service,
    database_connection_service,
    diagnosis_service,
    incident_service,
    notification_channel_service,
)

logger = logging.getLogger(__name__)

# Test hook: inject an httpx transport for webhook delivery.
webhook_transport: httpx.BaseTransport | None = None

ACTIVE_RUN_STATES = frozenset({"queued", "running", "restarting", "up_for_retry", "scheduled"})


@dataclass
class NodeResult:
    port: str | None = None
    output: dict[str, Any] = field(default_factory=dict)
    message: str | None = None
    wait_until: datetime | None = None


class NodeError(Exception):
    """The node could not run at all (fails the workflow run)."""


class ActionFailed(Exception):
    """An action ran but did not succeed (the node follows its 'failed' port)."""


@dataclass
class NodeContext:
    db: Session
    run: WorkflowRun
    graph: Graph
    incident: Incident | None  # None for manual and scheduled runs
    now: datetime
    settings: Settings
    _adapter: AirflowAdapter | None = None
    _adapters: dict[uuid.UUID, AirflowAdapter] = field(default_factory=dict)
    _dag_state_cache: dict[tuple[uuid.UUID, str], NodeResult] = field(default_factory=dict)

    @property
    def the_incident(self) -> Incident:
        """The run's incident; incident-only blocks are rejected at validation otherwise."""
        if self.incident is None:
            raise NodeError("This block needs a workflow started by an incident")
        return self.incident

    @property
    def connection(self) -> AirflowConnection:
        conn = self.db.get(AirflowConnection, self.the_incident.connection_id)
        if conn is None:
            raise NodeError("The incident's Airflow connection no longer exists")
        return conn

    @property
    def dag(self) -> MonitoredDag | None:
        if self.incident is None:
            return None
        return self.db.get(MonitoredDag, self.incident.monitored_dag_id)

    @property
    def environment(self) -> str:
        return str(self.connection.environment) if self.incident is not None else ""

    # Blocks that name their own target (pipeline.* / database.*).
    def airflow_connection(self, connection_id: str) -> AirflowConnection:
        conn = self.db.get(AirflowConnection, _uuid(connection_id))
        if conn is None:
            raise NodeError("The block's Airflow connection no longer exists")
        return conn

    def airflow_adapter(self, conn: AirflowConnection) -> AirflowAdapter:
        if self.incident is not None and conn.id == self.incident.connection_id:
            return self.adapter()
        if conn.id not in self._adapters:
            try:
                self._adapters[conn.id] = airflow_service.adapter_for(conn)
            except AppError as exc:
                raise NodeError(f"{conn.name}: {exc.message}") from exc
        return self._adapters[conn.id]

    def database_connection(self, connection_id: str) -> DatabaseConnection:
        conn = self.db.get(DatabaseConnection, _uuid(connection_id))
        if conn is None:
            raise NodeError("The block's database connection no longer exists")
        return conn

    def record_result(self, node_id: str, result: dict[str, Any]) -> None:
        """Expose a block's outcome to later blocks as {{results.<node id>.<key>}}."""
        self.set_context("results", {**self.get_context("results", {}), node_id: result})

    @property
    def dry_run(self) -> bool:
        return self.run.dry_run or self.settings.AUTOMATION_FORCE_DRY_RUN

    def adapter(self) -> AirflowAdapter:
        if self._adapter is None:
            self._adapter = airflow_service.adapter_for(self.connection)
        return self._adapter

    def set_context(self, key: str, value: Any) -> None:
        # Reassign so SQLAlchemy sees the JSON change.
        self.run.context = {**(self.run.context or {}), key: value}

    def get_context(self, key: str, default: Any = None) -> Any:
        return (self.run.context or {}).get(key, default)

    def node_state(self, node_id: str) -> dict[str, Any]:
        return dict(self.get_context("nodes", {}).get(node_id, {}))

    def set_node_state(self, node_id: str, state: dict[str, Any]) -> None:
        self.set_context("nodes", {**self.get_context("nodes", {}), node_id: state})

    def event(self, event: str, /, **details: Any) -> None:
        if self.incident is None:
            return  # manual/scheduled runs: the run's steps are the record
        incident_service.add_event(
            self.db,
            self.incident,
            event,
            details={"workflow": self.run.workflow_name, "run_id": str(self.run.id), **details},
        )

    def audit(self, action: str, /, **details: Any) -> None:
        audit_service.record(
            self.db,
            action=action,
            entity_type="workflow_run",
            entity_id=self.run.id,
            details={
                "workflow": self.run.workflow_name,
                **(
                    {"incident_id": str(self.incident.id), "dag_id": self.incident.dag_id}
                    if self.incident is not None
                    else {"trigger": self.run.trigger_event}
                ),
                "dry_run": self.dry_run,
                **details,
            },
        )

    def render_values(self) -> dict[str, Any]:
        incident = self.incident
        return {
            "incident": {
                "id": str(incident.id),
                "title": incident.title,
                "dag_id": incident.dag_id,
                "type": str(incident.type),
                "severity": str(incident.severity),
                "status": str(incident.status),
                "occurrence_count": incident.occurrence_count,
                "run_id": incident.last_run_id or incident.run_id or "",
            }
            if incident is not None
            else {},
            "diagnosis": self.get_context("diagnosis", {}),
            "analysis": self.get_context("analysis", {}),
            "ai_solution": self.get_context("ai_solution", {}),
            "decision": self.get_context("decision", {}),
            "results": self.get_context("results", {}),
            "workflow": {"name": self.run.workflow_name, "trigger": self.run.trigger_event},
            "dag_run": self.get_context("dag_run", {}),
            "environment": self.environment,
            "last_error": self.get_context("last_error", ""),
            "links": {
                "run": self.link(f"/automation/runs/{self.run.id}"),
                "approvals": self.link("/automation/approvals"),
            },
        }

    def link(self, path: str) -> str:
        """Absolute link into the app, for messages people open outside it."""
        return self.settings.PUBLIC_APP_URL.rstrip("/") + path


def _uuid(value: str) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(value))
    except ValueError:
        return None


# ---------------------------------------------------------------------- triggers & logic


_TRIGGER_TEXT = {"manual": "Started manually", "schedule": "Started by the schedule"}


def _trigger(ctx: NodeContext, node: Node) -> NodeResult:
    event = ctx.run.trigger_event
    if event == TriggerEvent.DAG_RUN:
        dag_run = ctx.get_context("dag_run", {})
        return NodeResult(
            "next",
            {"dag_id": dag_run.get("dag_id"), "run_id": dag_run.get("run_id")},
            f"Run {dag_run.get('run_id')} of {dag_run.get('dag_id')} {dag_run.get('state')}",
        )
    return NodeResult("next", message=_TRIGGER_TEXT.get(event, f"Triggered by incident {event}"))


def _filter(ctx: NodeContext, node: Node) -> NodeResult:
    cfg = node.config
    incident = ctx.the_incident
    checks: list[str] = []
    failed: list[str] = []

    def check(ok: bool, label: str) -> None:
        (checks if ok else failed).append(label)

    if cfg["environments"]:
        check(ctx.environment in cfg["environments"], f"environment {ctx.environment}")
    if cfg["incident_types"]:
        check(str(incident.type) in cfg["incident_types"], f"type {incident.type}")
    if cfg["min_severity"]:
        check(
            incident.severity.rank >= IncidentSeverity(cfg["min_severity"]).rank,
            f"severity {incident.severity} >= {cfg['min_severity']}",
        )
    if cfg["dag_ids"]:
        check(incident.dag_id in cfg["dag_ids"], f"DAG {incident.dag_id}")
    if cfg["tags_any"]:
        tags = set(ctx.dag.tags if ctx.dag else [])
        check(bool(tags & set(cfg["tags_any"])), f"tags {sorted(tags)}")
    if cfg["min_occurrences"] is not None:
        check(
            incident.occurrence_count >= cfg["min_occurrences"],
            f"occurrences {incident.occurrence_count} >= {cfg['min_occurrences']}",
        )
    if cfg["max_occurrences"] is not None:
        check(
            incident.occurrence_count <= cfg["max_occurrences"],
            f"occurrences {incident.occurrence_count} <= {cfg['max_occurrences']}",
        )
    if cfg["diagnosis_categories"]:
        category = ctx.get_context("diagnosis", {}).get("category")
        check(category in cfg["diagnosis_categories"], f"diagnosis {category or 'none'}")

    matched = not failed
    message = ("Matched: " if matched else "Did not match: ") + ", ".join(
        failed if not matched else checks or ["no criteria"]
    )
    return NodeResult("true" if matched else "false", {"passed": checks, "failed": failed}, message)


_OUTCOME_LABELS = {"retryable": "retry may help", "needs_fix": "needs a fix", "unknown": "unknown"}


def diagnosis_outcome(diagnosis: dict[str, Any], min_confidence_pct: int | None) -> str:
    """retryable / needs_fix / unknown. Operator corrections always pass the minimum."""
    category = str(diagnosis.get("category"))
    if category == FailureCategory.UNKNOWN:
        return "unknown"
    if (
        min_confidence_pct is not None
        and diagnosis.get("source") != "operator"
        and float(diagnosis.get("confidence") or 0) * 100 < min_confidence_pct
    ):
        return "unknown"
    return "retryable" if category in RETRYABLE else "needs_fix"


def _load_diagnosis(ctx: NodeContext, *, refresh: bool = False) -> dict[str, Any]:
    """The stored diagnosis (computed when evidence arrived, or an operator correction), so the
    workflow and the incident page always agree; computed here only if it is missing, or
    recomputed from the latest logs when asked."""
    incident = ctx.the_incident
    if refresh:
        stored = diagnosis_service.diagnose_and_store(ctx.db, incident, record_event=False)
    else:
        stored = diagnosis_service.ensure(ctx.db, incident, record_event=False)
    # Only failed-run incidents store one; others (SLA) are classified on the fly as before.
    return dict(stored) if stored else incident_service.diagnose(incident).as_dict()


def _classify(ctx: NodeContext, node: Node) -> NodeResult:
    cfg = node.config
    diagnosis = _load_diagnosis(ctx, refresh=bool(cfg.get("refresh")))
    outcome = diagnosis_outcome(diagnosis, cfg.get("min_confidence_pct"))
    diagnosis["outcome"] = outcome
    ctx.set_context("diagnosis", diagnosis)
    ctx.event(
        "diagnosed",
        category=diagnosis["category"],
        label=diagnosis["label"],
        retryable=diagnosis["retryable"],
        matched_line=diagnosis.get("matched_line"),
        source=diagnosis.get("source", "regex"),
        model_version=diagnosis.get("model_version"),
        outcome=outcome,
    )
    source = diagnosis.get("source", "regex")
    if source == "model" and diagnosis.get("model_version"):
        source = f"model {diagnosis['model_version']}"
    message = (
        f"Diagnosis: {diagnosis['label']} ({source}, {round(diagnosis['confidence'] * 100)}%)"
        f" → {_OUTCOME_LABELS[outcome]}"
    )
    # Graphs that only wire 'any outcome' (all graphs made before the outcome outputs) keep
    # following it.
    port = outcome if ctx.graph.next_nodes(node.id, outcome) else "next"
    return NodeResult(port, diagnosis, message)


def _extract_log_error(raw_log: str, max_lines: int = 100) -> tuple[str, str, str, str]:
    """Extract (headline, stack_trace, snippet, task_id) from raw log text."""
    lines = [line.strip() for line in raw_log.splitlines() if line.strip()]
    if not lines:
        return "", "", "", ""
    snippet = "\n".join(lines[-max_lines:])

    task_id = ""
    for line in lines[:30]:
        m = re.search(r"task_id[=:]\s*['\"]?([A-Za-z0-9_.-]+)", line, re.I) or re.search(
            r"Task (?:instance: )?([A-Za-z0-9_.-]+)", line, re.I
        )
        if m:
            task_id = m.group(1)
            break

    stack_trace_lines: list[str] = []
    headline = ""
    in_traceback = False
    for line in lines:
        if "Traceback (most recent call last):" in line:
            in_traceback = True
            stack_trace_lines = [line]
            continue
        if in_traceback:
            stack_trace_lines.append(line)
            if (
                line
                and not line.startswith("File ")
                and not line.startswith("  ")
                and (
                    "Error:" in line
                    or "Exception:" in line
                    or re.match(r"^[A-Za-z0-9_.]+(?:Error|Exception|Exit|Interrupt):", line)
                )
            ):
                headline = line
                in_traceback = False

    stack_trace = "\n".join(stack_trace_lines) if stack_trace_lines else ""

    if not headline:
        for line in reversed(lines):
            lower = line.lower()
            if any(term in lower for term in ("error:", "exception:", "failed:", "fatal:", "critical:")):
                headline = line
                break
        if not headline and lines:
            headline = lines[-1]

    return headline[:300], stack_trace, snippet, task_id


def _analyze_task_logs(ctx: NodeContext, node: Node) -> NodeResult:
    cfg = node.config
    incident = ctx.the_incident
    logs = diagnosis_service.task_logs(ctx.db, incident)
    if not logs:
        output = {
            "headline": "",
            "stack_trace": "",
            "log_snippet": "",
            "task_id": "",
            "dag_id": incident.dag_id,
            "run_id": incident.last_run_id or incident.run_id or "",
            "incident_id": str(incident.id),
        }
        ctx.set_context("analysis", output)
        ctx.record_result(node.id, output)
        return NodeResult("no_logs", output, "No task logs found for incident")

    raw_log = logs[0]
    if cfg.get("redact_secrets", True):
        raw_log = scrub(raw_log)

    max_lines = cfg.get("max_log_lines", 100)
    headline, stack_trace, snippet, task_id = _extract_log_error(raw_log, max_lines=max_lines)

    output = {
        "headline": headline,
        "stack_trace": stack_trace,
        "log_snippet": snippet,
        "task_id": task_id,
        "dag_id": incident.dag_id,
        "run_id": incident.last_run_id or incident.run_id or "",
        "incident_id": str(incident.id),
    }
    ctx.set_context("analysis", output)
    ctx.record_result(node.id, output)
    ctx.event("logs_analyzed", headline=headline, task_id=task_id)
    return NodeResult("analyzed", output, f"Analyzed logs: {headline}")


def _dag_state(ctx: NodeContext, node: Node) -> NodeResult:
    dag_id = ctx.the_incident.dag_id
    cache_key = (ctx.the_incident.connection_id, dag_id)
    cached = ctx._dag_state_cache.get(cache_key)
    if cached is not None:
        logger.debug("Reused cached DAG state for %s", dag_id)
        return cached
    try:
        dag = run_async(lambda: ctx.adapter().get_dag(dag_id))
        if dag is None:
            raise NodeError(f"DAG {dag_id} no longer exists in Airflow")
        if dag.is_paused:
            result = NodeResult("paused", {"is_paused": True}, f"{dag_id} is paused")
            logger.info("Dependency blocked DAG action for %s: DAG is paused", dag_id)
            ctx._dag_state_cache[cache_key] = result
            return result
        runs = run_async(
            lambda: ctx.adapter().list_dag_runs(dag_id, since=ctx.now - timedelta(hours=24))
        )
    except AirflowAdapterError as exc:
        raise NodeError(f"Could not read DAG state: {exc.result.message}") from exc
    active = [r.run_id for r in runs if r.state in ACTIVE_RUN_STATES]
    if active:
        result = NodeResult(
            "busy", {"active_runs": active}, f"A run is already active: {active[0]}"
        )
        logger.info("Dependency blocked DAG action for %s: active run %s", dag_id, active[0])
    else:
        result = NodeResult("ready", {"is_paused": False}, f"{dag_id} is idle and not paused")
    ctx._dag_state_cache[cache_key] = result
    return result


_FIX_ACTIONS = ("action.clear_failed_tasks", "action.trigger_dag_run")


def _fix_history(ctx: NodeContext) -> tuple[int, int]:
    """(fixes carried out, verifications that failed) on this incident, by any live run."""
    rows = ctx.db.execute(
        select(WorkflowStep.node_type, WorkflowStep.port)
        .join(WorkflowRun, WorkflowRun.id == WorkflowStep.run_id)
        .where(
            WorkflowRun.incident_id == ctx.the_incident.id,
            WorkflowRun.dry_run.is_(False),
            WorkflowStep.node_type.in_((*_FIX_ACTIONS, "verify.run_success")),
        )
    ).all()
    attempts = sum(1 for t, p in rows if t in _FIX_ACTIONS and p == "success")
    failed = sum(1 for t, p in rows if t == "verify.run_success" and p == "failed")
    return attempts, failed


def _fix_facts(ctx: NodeContext, node: Node, diagnosis: dict[str, Any]) -> remediation.Facts:
    incident = ctx.the_incident
    dag_state = "unknown"
    if node.config.get("check_dag_state", False):
        try:
            dag_state = _dag_state(ctx, node).port or "unknown"
        except (NodeError, AppError) as exc:
            logger.info("Choose the fix: DAG state unavailable for %s: %s", incident.dag_id, exc)
    attempts, failed = _fix_history(ctx)
    return remediation.Facts(
        incident_type=str(incident.type),
        incident_status=str(incident.status),
        dag_id=incident.dag_id,
        environment=ctx.environment,
        severity=str(incident.severity),
        occurrences=incident.occurrence_count,
        category=str(diagnosis.get("category") or FailureCategory.UNKNOWN),
        confidence=float(diagnosis.get("confidence") or 0),
        source=str(diagnosis.get("source") or "regex"),
        has_failed_run=incident.type == IncidentType.DAG_RUN_FAILED
        and bool(incident.last_run_id or incident.run_id),
        fix_attempts=attempts,
        failed_verifications=failed,
        actions_today=_actions_last_24h(ctx),
        max_actions_per_day=ctx.settings.AUTOMATION_MAX_ACTIONS_PER_DAG_PER_DAY,
        dag_state=dag_state,
    )


def decision_advisor(settings: Settings) -> DecisionLLM | None:
    """The local LLM for "Choose the fix" blocks, or None when it is switched off."""
    return get_decision_llm() if settings.DECISION_LLM_ENABLED else None


def _choose_fix(ctx: NodeContext, node: Node) -> NodeResult:
    cfg = node.config
    diagnosis = ctx.get_context("diagnosis") or _load_diagnosis(ctx)
    facts = _fix_facts(ctx, node, diagnosis)
    choice = remediation.choose_fix(
        facts,
        remediation.Settings(
            max_attempts=cfg["max_attempts"],
            min_confidence_pct=cfg["min_confidence_pct"],
            retry_unknown_once=cfg["retry_unknown_once"],
            pause_on_bad_data=cfg["pause_on_bad_data"],
            pause_after_failures=cfg["pause_after_failures"],
        ),
    )
    advisor = decision_advisor(ctx.settings) if cfg["ai_mode"] != "off" else None
    ask: Callable[[], remediation.Advice] | None = None
    if advisor is not None:

        def ask_advisor() -> remediation.Advice:
            logs = diagnosis_service.task_logs(ctx.db, ctx.the_incident)
            return advisor.advise(facts, choice, diagnosis, logs)

        ask = ask_advisor

    advised = remediation.advise(choice, cfg["ai_mode"], ask, cfg["ai_min_confidence_pct"])
    fix, reason = advised.fix, advised.reason
    port = fix
    if not ctx.graph.next_nodes(node.id, fix) and ctx.graph.next_nodes(node.id, "escalate"):
        port = "escalate"
        reason += f" Nothing is connected to '{fix}', so it goes to a person."
    decision = {
        "fix": port,
        "chosen": fix,
        "label": remediation.FIX_LABELS[port],
        "rule": choice.rule,
        "rules_fix": choice.fix,
        "reason": reason,
        "allowed": list(choice.allowed),
        "ai": advised.ai,
        "facts": facts.as_dict(),
    }
    ctx.set_context("decision", decision)
    ctx.record_result(node.id, decision)
    if port == "escalate":
        ctx.set_context("last_error", reason)
    ctx.event("fix_chosen", fix=port, rule=choice.rule, reason=reason, ai=advised.ai)
    ctx.audit(
        "automation.fix_chosen",
        fix=port,
        rule=choice.rule,
        reason=reason,
        ai=advised.ai,
        facts=facts.as_dict(),
    )
    return NodeResult(port, decision, f"Chose to {remediation.FIX_LABELS[port]}: {reason}")


def _ai_generate_fix(ctx: NodeContext, node: Node) -> NodeResult:
    cfg = node.config
    min_conf = cfg.get("min_confidence_pct", 70)
    instruction = cfg.get("instruction", "")
    incident = ctx.the_incident
    analysis = ctx.get_context("analysis") or {}
    diagnosis = ctx.get_context("diagnosis") or _load_diagnosis(ctx)

    headline = analysis.get("headline") or diagnosis.get("label") or incident.title
    stack_trace = analysis.get("stack_trace") or analysis.get("log_snippet") or ""

    advisor = decision_advisor(ctx.settings)
    if advisor is None or not advisor.enabled:
        output = {
            "headline": headline,
            "explanation": "AI advisor is disabled or unavailable.",
            "recommended_action": "escalate",
            "confidence": 0.0,
            "model": "disabled",
            "reasoning": "Decision LLM is not enabled in settings.",
            "instruction": instruction,
        }
        ctx.set_context("ai_solution", output)
        ctx.record_result(node.id, output)
        return NodeResult("uncertain", output, "AI advisor disabled; following uncertain")

    try:
        facts = _fix_facts(ctx, node, diagnosis)
        choice = remediation.choose_fix(facts, remediation.Settings())
        logs = [stack_trace] if stack_trace else diagnosis_service.task_logs(ctx.db, incident)
        advice = advisor.advise(facts, choice, diagnosis, logs)
        conf_pct = round(advice.confidence * 100)
        output = {
            "headline": headline,
            "explanation": advice.reason,
            "recommended_action": advice.fix,
            "confidence": advice.confidence,
            "model": advice.model,
            "instruction": instruction,
        }
        ctx.set_context("ai_solution", output)
        ctx.record_result(node.id, output)
        port = "solution_ready" if conf_pct >= min_conf else "uncertain"
        msg = f"AI ({advice.model}): {advice.fix} - {advice.reason} ({conf_pct}%)"
        ctx.event("ai_solution_generated", fix=advice.fix, confidence=advice.confidence)
        return NodeResult(port, output, msg)
    except Exception as exc:
        logger.warning("AI solution generation error: %s", exc)
        output = {
            "headline": headline,
            "explanation": f"AI generation error: {exc}",
            "recommended_action": "escalate",
            "confidence": 0.0,
            "model": "error",
            "instruction": instruction,
        }
        ctx.set_context("ai_solution", output)
        ctx.record_result(node.id, output)
        return NodeResult("uncertain", output, f"AI generation error: {exc}")


# ---------------------------------------------------------------------- approval


def _proposed_actions(ctx: NodeContext, node: Node) -> list[dict[str, Any]]:
    """Every block on the "approved" output, in the order they will run."""
    return [
        {"node_id": target.id, "type": target.type, "config": target.config}
        for target in (ctx.graph.nodes[n] for n in ctx.graph.next_nodes(node.id, "approved"))
    ]


_ACTION_TEXT = {
    "action.clear_failed_tasks": "retry the failed tasks of run {run_id}",
    "action.trigger_dag_run": "start a new run of {dag_id}",
    "action.set_dag_paused": "pause {dag_id}",
    "pipeline.run_dag": "run {dag_id}",
    "database.run_sql": "run SQL on {database}",
}


def describe_action(
    action: dict[str, Any], incident: Incident | None, *, database: str = "the database"
) -> str:
    template = _ACTION_TEXT.get(action.get("type", ""), "continue the workflow")
    config = action.get("config") or {}
    if action.get("type") == "action.set_dag_paused" and not config.get("paused", True):
        template = "unpause {dag_id}"
    if action.get("type") == "database.run_sql" and config.get("read_only"):
        template = "run a read-only query on {database}"
    return template.format(
        run_id=(incident.last_run_id or incident.run_id) if incident else "",
        dag_id=config.get("dag_id") or (incident.dag_id if incident else ""),
        database=database,
    )


def _action_environment(ctx: NodeContext, action: dict[str, Any]) -> tuple[str, str]:
    """(environment, database name) of the connection the proposed action would touch.

    Blocks with their own target use its connection; incident blocks use the incident's.
    Without either, assume PROD so an approval is never skipped by accident.
    """
    config = action.get("config") or {}
    kind = str(action.get("type", "")).split(".")[0]
    if kind == "pipeline" and config.get("connection_id"):
        conn = ctx.db.get(AirflowConnection, _uuid(config["connection_id"]))
        if conn is not None:
            return str(conn.environment), ""
    if kind == "database" and config.get("connection_id"):
        db_conn = ctx.db.get(DatabaseConnection, _uuid(config["connection_id"]))
        if db_conn is not None:
            return str(db_conn.environment), db_conn.name
    if ctx.incident is not None:
        return ctx.environment, ""
    return "PROD", ""


def _approval(ctx: NodeContext, node: Node) -> NodeResult:
    state = ctx.node_state(node.id)
    approval_id = state.get("approval_id")

    if approval_id is None:
        required_envs = node.config["required_environments"]
        actions = _proposed_actions(ctx, node)
        targets = [(a, *_action_environment(ctx, a)) for a in actions or [{}]]
        # One approval covers every linked block: it is needed if any of them needs it.
        envs = [env for _, env, _ in targets]
        environment = next((env for env in envs if env in required_envs), envs[0])
        if ctx.dry_run:
            return NodeResult("approved", {"auto": "dry_run"}, "Dry run: approval skipped")
        if required_envs and environment not in required_envs:
            return NodeResult(
                "approved",
                {"auto": "not_required", "environment": environment},
                f"No approval needed in {environment}",
            )
        what = ", then ".join(
            dict.fromkeys(
                describe_action(a, ctx.incident, database=database or "the database")
                for a, _, database in targets
            )
        )
        diagnosis = ctx.get_context("diagnosis", {})
        analysis = ctx.get_context("analysis", {})
        ai_sol = ctx.get_context("ai_solution", {})
        summary = f"The workflow wants to {what} on {environment}."
        if analysis and analysis.get("headline"):
            summary += f" Error: {analysis['headline']}."
        elif diagnosis:
            summary += f" Diagnosis: {diagnosis.get('label')}."
            if diagnosis.get("matched_line"):
                summary += f" Evidence: {diagnosis['matched_line']}"
        if ai_sol and ai_sol.get("explanation"):
            summary += f" AI Solution: {ai_sol['explanation']}"
        expires = ctx.now + timedelta(minutes=node.config["timeout_minutes"])
        subject = ctx.incident.dag_id if ctx.incident is not None else ctx.run.workflow_name
        approval = Approval(
            id=uuid.uuid4(),
            run_id=ctx.run.id,
            incident_id=ctx.incident.id if ctx.incident is not None else None,
            node_id=node.id,
            title=f"{subject}: {what}"[:300],
            summary=summary,
            proposed_action={
                **(actions[0] if actions else {}),
                **({"actions": actions} if len(actions) > 1 else {}),
                "description": what,
                "environment": environment,
            },
            status=ApprovalStatus.PENDING,
            expires_at=expires,
        )
        ctx.db.add(approval)
        ctx.db.add(
            Notification(
                run_id=ctx.run.id,
                incident_id=ctx.incident.id if ctx.incident is not None else None,
                level=NotificationLevel.WARNING,
                title=f"Approval needed: {approval.title}"[:300],
                body=summary,
                created_at=ctx.now,
            )
        )
        ctx.set_node_state(node.id, {"approval_id": str(approval.id)})
        ctx.event("approval_requested", approval_id=str(approval.id), action=what)
        ctx.audit("approval.requested", approval_id=str(approval.id), action=what)
        output = {"approval_id": str(approval.id), "expires_at": expires.isoformat()}
        message = f"Waiting for approval to {what}"
        if node.config.get("send_via"):
            ok, detail = _send_via(
                ctx,
                node.config["send_via"],
                node.config.get("to") or [],
                Message(
                    title=f"Approval needed: {approval.title}",
                    text=f"{summary}\nExpires {expires:%Y-%m-%d %H:%M} UTC.",
                    level="WARNING",
                    link=ctx.link("/automation/approvals"),
                    link_label="Review and approve",
                ),
            )
            output.update(sent=ok, detail=detail)
            message += f" ({detail})"
        return NodeResult(output=output, message=message, wait_until=expires)

    approval = ctx.db.get(Approval, uuid.UUID(approval_id))
    if approval is None:
        raise NodeError("Approval record disappeared")
    if approval.status == ApprovalStatus.PENDING:
        if ctx.now < approval.expires_at:
            return NodeResult(
                output={"approval_id": approval_id},
                message="Still waiting for approval",
                wait_until=approval.expires_at,
            )
        approval.status = ApprovalStatus.EXPIRED
        approval.decided_at = ctx.now
        ctx.event("approval_expired", approval_id=approval_id)
        ctx.audit("approval.expired", approval_id=approval_id)
    output = {
        "approval_id": approval_id,
        "status": str(approval.status),
        "decided_by": approval.decided_by_name,
        "comment": approval.comment,
    }
    if approval.status == ApprovalStatus.APPROVED:
        ctx.set_context("approval_granted", True)
        return NodeResult("approved", output, f"Approved by {approval.decided_by_name}")
    ctx.set_context("last_error", f"Approval {str(approval.status).lower()}.")
    return NodeResult("rejected", output, f"Approval {str(approval.status).lower()}")


# ---------------------------------------------------------------------- actions


def _actions_last_24h(ctx: NodeContext) -> int:
    """Successful automated actions on the incident's DAG in the last 24h (rate limit)."""
    incident = ctx.the_incident
    since = ctx.now - timedelta(hours=24)
    return (
        ctx.db.scalar(
            select(func.count(WorkflowStep.id))
            .join(WorkflowRun, WorkflowRun.id == WorkflowStep.run_id)
            .join(Incident, Incident.id == WorkflowRun.incident_id)
            .where(
                Incident.connection_id == incident.connection_id,
                Incident.dag_id == incident.dag_id,
                WorkflowStep.node_type.in_(ACTION_TYPES),
                WorkflowStep.port == "success",
                WorkflowStep.finished_at >= since,
                WorkflowRun.dry_run.is_(False),
            )
        )
        or 0
    )


def _run_action(
    ctx: NodeContext,
    node: Node,
    description: str,
    execute: Callable[[], dict[str, Any]],
    simulated_target: dict[str, Any] | None = None,
    *,
    environment: str | None = None,
) -> NodeResult:
    """Policy-check and run an action. `environment` is given for blocks with their own target;
    those are not rate limited (their workflow's trigger sets the pace), incident fixes are."""
    decision = policy.check_action(
        node.type,
        environment=environment if environment is not None else ctx.environment,
        dry_run=ctx.dry_run,
        approved=bool(ctx.get_context("approval_granted")),
        actions_last_24h=_actions_last_24h(ctx) if environment is None else 0,
        max_per_day=ctx.settings.AUTOMATION_MAX_ACTIONS_PER_DAG_PER_DAY,
    )
    if not decision.allowed:
        reason = "; ".join(decision.reasons)
        ctx.set_context("last_error", f"Not allowed to {description}: {reason}.")
        ctx.event("action_denied", action=description, reasons=decision.reasons)
        ctx.audit("automation.action_denied", action=node.type, reasons=decision.reasons)
        return NodeResult("failed", {"policy": decision.as_dict()}, f"Blocked by policy: {reason}")

    if decision.simulate:
        ctx.set_context("action_target", {**(simulated_target or {}), "simulated": True})
        ctx.event("action_simulated", action=description)
        return NodeResult(
            "success",
            {"policy": decision.as_dict(), "simulated": True},
            f"Dry run: would {description}",
        )

    try:
        output = execute()
    except (AirflowAdapterError, ActionFailed) as exc:
        message = exc.result.message if isinstance(exc, AirflowAdapterError) else str(exc)
        ctx.set_context("last_error", f"Could not {description}: {message}")
        ctx.event("action_failed", action=description, error=message)
        ctx.audit("automation.action_failed", action=node.type, error=message)
        return NodeResult("failed", {"error": message}, f"Could not {description}: {message}")

    ctx.event("action_executed", action=description, **output)
    ctx.audit("automation.action_executed", action=node.type, **output)
    return NodeResult("success", {"policy": decision.as_dict(), **output}, f"Done: {description}")


def _trigger_dag_once(
    ctx: NodeContext,
    *,
    node_id: str,
    conn: AirflowConnection,
    dag_id: str,
    note: str,
    conf: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Atomically reserve a DAG before triggering it, so workers cannot race a duplicate call."""
    reservation = ctx.db.scalar(
        select(AirflowTriggerReservation).where(
            AirflowTriggerReservation.connection_id == conn.id,
            AirflowTriggerReservation.dag_id == dag_id,
        )
    )
    if reservation is not None:
        if reservation.airflow_run_id:
            remote = run_async(
                lambda: ctx.airflow_adapter(conn).get_dag_run(dag_id, reservation.airflow_run_id)
            )
            if remote is not None and remote.state in ACTIVE_RUN_STATES:
                if reservation.workflow_run_id == ctx.run.id and reservation.node_id == node_id:
                    logger.info(
                        "Skipped duplicate DAG trigger for %s/%s; workflow run %s "
                        "already started %s",
                        conn.name,
                        dag_id,
                        ctx.run.id,
                        reservation.airflow_run_id,
                    )
                    return {
                        "dag_id": dag_id,
                        "run_id": reservation.airflow_run_id,
                        "deduplicated": True,
                    }
                logger.info(
                    "Skipped duplicate DAG trigger for %s/%s; active reservation belongs to %s",
                    conn.name,
                    dag_id,
                    reservation.workflow_run_id,
                )
                raise ActionFailed(f"DAG {dag_id} already has an active trigger")
            ctx.db.delete(reservation)
            ctx.db.flush()
        elif (
            ctx.now - reservation.created_at
        ).total_seconds() < ctx.settings.AUTOMATION_TRIGGER_RESERVATION_SECONDS:
            logger.info(
                "Skipped duplicate DAG trigger for %s/%s; reservation is pending",
                conn.name,
                dag_id,
            )
            raise ActionFailed(f"DAG {dag_id} has a trigger reservation pending")
        else:
            ctx.db.delete(reservation)
            ctx.db.flush()

    try:
        active = run_async(
            lambda: ctx.airflow_adapter(conn).list_dag_runs(
                dag_id, since=ctx.now - timedelta(hours=24)
            )
        )
        if any(run.state in ACTIVE_RUN_STATES for run in active):
            logger.info(
                "Skipped duplicate DAG trigger for %s/%s; Airflow already has an active run",
                conn.name,
                dag_id,
            )
            raise ActionFailed(f"DAG {dag_id} already has an active run")
    except AirflowAdapterError:
        raise

    reservation = AirflowTriggerReservation(
        connection_id=conn.id,
        dag_id=dag_id,
        workflow_run_id=ctx.run.id,
        node_id=node_id,
    )
    try:
        with ctx.db.begin_nested():
            ctx.db.add(reservation)
            ctx.db.flush()
    except IntegrityError as exc:
        logger.info(
            "Skipped duplicate DAG trigger for %s/%s; another worker reserved it",
            conn.name,
            dag_id,
        )
        raise ActionFailed(f"DAG {dag_id} trigger was reserved by another worker") from exc

    run = run_async(lambda: ctx.airflow_adapter(conn).trigger_dag_run(dag_id, note=note, conf=conf))
    reservation.airflow_run_id = run.run_id
    ctx.db.flush()
    return {"dag_id": dag_id, "run_id": run.run_id}


def _clear_failed_tasks(ctx: NodeContext, node: Node) -> NodeResult:
    incident = ctx.the_incident
    run_id = incident.last_run_id or incident.run_id
    if not run_id:
        ctx.set_context("last_error", "There is no failed run to retry.")
        return NodeResult("failed", message="There is no failed run to retry")

    def execute() -> dict[str, Any]:
        cleared = run_async(
            lambda: ctx.adapter().clear_task_instances(
                incident.dag_id,
                run_id,
                only_failed=True,
                include_downstream=node.config["include_downstream"],
            )
        )
        if not cleared:
            raise AirflowAdapterError(_adapter_result(f"No failed tasks to clear in run {run_id}"))
        ctx.set_context(
            "action_target", {"dag_id": incident.dag_id, "run_id": run_id, "action": "clear"}
        )
        return {"run_id": run_id, "cleared_tasks": cleared}

    return _run_action(
        ctx,
        node,
        f"retry the failed tasks of run {run_id}",
        execute,
        {"dag_id": incident.dag_id, "run_id": run_id, "action": "clear"},
    )


def _trigger_dag_run(ctx: NodeContext, node: Node) -> NodeResult:
    incident = ctx.the_incident
    dag_id = incident.dag_id

    def execute() -> dict[str, Any]:
        note = f"Triggered by automation '{ctx.run.workflow_name}' for incident {incident.id}"
        result = _trigger_dag_once(
            ctx, node_id=node.id, conn=ctx.connection, dag_id=dag_id, note=note
        )
        ctx.set_context(
            "action_target", {"dag_id": dag_id, "run_id": result["run_id"], "action": "trigger"}
        )
        return {
            "run_id": result["run_id"],
            **({"deduplicated": True} if result.get("deduplicated") else {}),
        }

    return _run_action(ctx, node, f"start a new run of {dag_id}", execute, {"dag_id": dag_id})


def _set_dag_paused(ctx: NodeContext, node: Node) -> NodeResult:
    dag_id = ctx.the_incident.dag_id
    paused = node.config["paused"]

    def execute() -> dict[str, Any]:
        run_async(lambda: ctx.adapter().set_dag_paused(dag_id, paused))
        dag = ctx.dag
        if dag is not None:
            dag.is_paused = paused
        ctx.set_context("action_target", {"dag_id": dag_id, "action": "pause"})
        return {"dag_id": dag_id, "paused": paused}

    verb = "pause" if paused else "unpause"
    return _run_action(ctx, node, f"{verb} {dag_id}", execute, {"dag_id": dag_id})


def _adapter_result(message: str) -> ConnectionTestResult:
    return ConnectionTestResult(status=ConnectionStatus.AIRFLOW_ERROR, message=message)


# ---------------------------------------------------------------------- verify


def _verify(ctx: NodeContext, node: Node) -> NodeResult:
    target = ctx.get_context("action_target") or {}
    if target.get("simulated") or ctx.dry_run:
        return NodeResult("success", {"simulated": True}, "Dry run: verification skipped")
    run_id = target.get("run_id")
    if not run_id:
        ctx.set_context("last_error", "Nothing to verify: the previous action started no run.")
        return NodeResult("failed", message="Nothing to verify")

    deadline = _deadline(ctx, node, node.config["timeout_minutes"])
    dag_id = target["dag_id"]
    if target.get("connection_id"):
        adapter = ctx.airflow_adapter(ctx.airflow_connection(target["connection_id"]))
    else:
        adapter = ctx.adapter()
    return _poll_dag_run(ctx, adapter, dag_id, run_id, deadline, after="the fix")


def _deadline(ctx: NodeContext, node: Node, minutes: int) -> datetime:
    """The node's deadline, fixed on its first execution and kept across waits."""
    state = ctx.node_state(node.id)
    if "deadline" not in state:
        state["deadline"] = (ctx.now + timedelta(minutes=minutes)).isoformat()
    state["checks"] = int(state.get("checks", 0)) + 1
    ctx.set_node_state(node.id, state)
    return datetime.fromisoformat(state["deadline"])


def _poll_dag_run(
    ctx: NodeContext,
    adapter: AirflowAdapter,
    dag_id: str,
    run_id: str,
    deadline: datetime,
    *,
    after: str,
) -> NodeResult:
    """Wait for a DAG run to finish: 'success', 'failed' (incl. timeout), or wait again."""
    try:
        run = run_async(lambda: adapter.get_dag_run(dag_id, run_id))
    except AirflowAdapterError as exc:
        if ctx.now >= deadline:
            ctx.set_context("last_error", f"Could not check run {run_id}: {exc.result.message}")
            ctx.event("verification_failed", run=run_id, reason=exc.result.message)
            return NodeResult("failed", {"error": exc.result.message}, "Could not verify")
        return _verify_wait(ctx, deadline, f"Airflow unavailable, retrying: {exc.result.message}")

    if run is None:
        ctx.set_context("last_error", f"Run {run_id} no longer exists.")
        ctx.event("verification_failed", run=run_id, reason="run not found")
        return NodeResult("failed", message=f"Run {run_id} not found")
    if run.state == "success":
        ctx.event("verified", run=run_id, state=run.state)
        return NodeResult("success", {"run_id": run_id, "state": run.state}, "Run succeeded")
    if run.state == "failed":
        ctx.set_context("last_error", f"Run {run_id} of {dag_id} failed after {after}.")
        ctx.event("verification_failed", run=run_id, state=run.state)
        return NodeResult("failed", {"run_id": run_id, "state": run.state}, "Run failed again")
    if ctx.now >= deadline:
        ctx.set_context("last_error", f"Run {run_id} did not finish in time (state {run.state}).")
        ctx.event("verification_failed", run=run_id, state=run.state, reason="timeout")
        return NodeResult("failed", {"run_id": run_id, "state": run.state}, "Timed out")
    return _verify_wait(ctx, deadline, f"Run {run_id} is {run.state}; checking again")


def _verify_wait(ctx: NodeContext, deadline: datetime, message: str) -> NodeResult:
    poll = timedelta(seconds=ctx.settings.AUTOMATION_VERIFY_POLL_SECONDS)
    return NodeResult(message=message, wait_until=min(ctx.now + poll, deadline))


# ---------------------------------------------------------------------- incident & notify


def _incident_update(ctx: NodeContext, node: Node) -> NodeResult:
    incident = ctx.incident
    operation = node.config["operation"]
    note = render(node.config["note"] or "", ctx.render_values()).strip() or None

    if operation == "note":
        ctx.event("automation_note", note=note or "")
        return NodeResult("next", {"note": note}, "Note added")

    if ctx.dry_run:
        return NodeResult("next", {"simulated": True}, f"Dry run: would {operation} the incident")

    if operation == "resolve":
        if incident.status == IncidentStatus.RESOLVED:
            return NodeResult("next", {"skipped": True}, "Incident was already resolved")
        incident_service.mark_resolved(incident, IncidentResolution.AUTO_REMEDIATED, note=note)
        ctx.event("auto_remediated", note=note or "")
        ctx.audit("incident.auto_remediated", note=note)
        return NodeResult("next", {"status": "RESOLVED"}, "Incident resolved")

    if operation == "escalate":
        if incident.status == IncidentStatus.RESOLVED:
            return NodeResult("next", {"skipped": True}, "Incident is resolved; not escalated")
        levels = list(IncidentSeverity)
        before = incident.severity
        after = levels[min(before.rank + 1, len(levels) - 1)]
        if after == before:
            return NodeResult("next", {"severity": str(after)}, f"Already {after}")
        incident.severity = after
        ctx.event("escalated", severity={"from": str(before), "to": str(after)})
        ctx.audit("incident.escalated", severity_from=str(before), severity_to=str(after))
        return NodeResult("next", {"from": str(before), "to": str(after)}, f"Escalated to {after}")

    # acknowledge
    if incident.status != IncidentStatus.OPEN:
        return NodeResult("next", {"skipped": True}, f"Incident is {incident.status}")
    incident.status = IncidentStatus.ACKNOWLEDGED
    incident.acknowledged_at = ctx.now
    ctx.event("acknowledged")
    return NodeResult("next", {"status": "ACKNOWLEDGED"}, "Incident acknowledged")


def _host_allowed(url: str, allowed: list[str]) -> bool:
    host = (urlsplit(url).hostname or "").lower()
    return not allowed or host in {h.lower() for h in allowed}


def _send_via(
    ctx: NodeContext, channel_id: str, recipients: list[str], message: Message
) -> tuple[bool, str]:
    """Send through a catalog channel. Delivery problems never fail the run: they are
    reported in the step and on the channel's status instead."""
    channel = ctx.db.get(NotificationChannel, _uuid(channel_id))
    if channel is None:
        ctx.event("notify_failed", error="channel no longer exists")
        return False, "The notification channel no longer exists"
    if ctx.dry_run:
        return True, f"Dry run: would send via {channel.name}"
    result = notification_channel_service.deliver(channel, message, recipients or None)
    if result.ok:
        ctx.event("notified", channel=channel.name, detail=result.detail)
        return True, f"Sent via {channel.name}: {result.detail}"
    ctx.event("notify_failed", channel=channel.name, error=result.detail)
    ctx.audit("automation.notify_failed", channel=channel.name, error=result.detail)
    return False, f"Could not send via {channel.name}: {result.detail}"


def _notify(ctx: NodeContext, node: Node) -> NodeResult:
    cfg = node.config
    values = ctx.render_values()
    fallback = ctx.incident.title if ctx.incident is not None else ctx.run.workflow_name
    title = render(cfg["title"], values).strip()[:300] or fallback
    body = render(cfg["message"], values).strip()
    level = NotificationLevel(cfg["level"])

    if cfg["channel"] == "in_app" or cfg.get("send_via"):
        prefix = "[Dry run] " if ctx.dry_run else ""
        ctx.db.add(
            Notification(
                run_id=ctx.run.id,
                incident_id=ctx.incident.id if ctx.incident is not None else None,
                level=level,
                title=(prefix + title)[:300],
                body=body,
                created_at=ctx.now,
            )
        )
        ctx.event("notified", channel="in_app", title=title)
        if not cfg.get("send_via"):
            return NodeResult("next", {"channel": "in_app", "title": title}, "In-app notification")
        ok, detail = _send_via(
            ctx,
            cfg["send_via"],
            cfg.get("to") or [],
            Message(
                title=title,
                text=body,
                level=str(level),
                link=values["links"]["run"],
                link_label="Open the run",
            ),
        )
        return NodeResult("next", {"title": title, "sent": ok, "detail": detail}, detail)

    url = cfg["url"] or ""
    if ctx.dry_run:
        return NodeResult("next", {"simulated": True, "url": url}, "Dry run: webhook not sent")
    if not url or not _host_allowed(url, ctx.settings.AUTOMATION_WEBHOOK_ALLOWED_HOSTS):
        ctx.event("notify_failed", channel="webhook", error="host not allowed")
        return NodeResult("next", {"error": "Webhook host is not allowed"}, "Webhook blocked")
    payload = {
        "title": title,
        "message": body,
        "level": str(level),
        "incident": values["incident"],
        "diagnosis": values["diagnosis"],
        "workflow": values["workflow"],
        "run_id": str(ctx.run.id),
        "environment": values["environment"],
    }
    try:
        with httpx.Client(
            timeout=ctx.settings.AUTOMATION_WEBHOOK_TIMEOUT_SECONDS,
            follow_redirects=False,
            transport=webhook_transport,
        ) as client:
            response = client.post(url, json=payload)
        ok = response.is_success
        error = None if ok else f"HTTP {response.status_code}"
    except httpx.HTTPError as exc:
        ok, error = False, type(exc).__name__
    host = urlsplit(url).hostname
    if ok:
        ctx.event("notified", channel="webhook", host=host)
        return NodeResult("next", {"channel": "webhook", "host": host}, "Webhook delivered")
    ctx.event("notify_failed", channel="webhook", host=host, error=error)
    return NodeResult("next", {"channel": "webhook", "error": error}, f"Webhook failed: {error}")


# ---------------------------------------------------------------------- orchestration


def _wait(ctx: NodeContext, node: Node) -> NodeResult:
    minutes = node.config["minutes"]
    state = ctx.node_state(node.id)
    if "until" not in state:
        state["until"] = (ctx.now + timedelta(minutes=minutes)).isoformat()
        ctx.set_node_state(node.id, state)
    until = datetime.fromisoformat(state["until"])
    if ctx.now >= until:
        return NodeResult("next", message=f"Waited {minutes} min")
    return NodeResult(message=f"Waiting until {until:%H:%M} UTC", wait_until=until)


def _run_dag(ctx: NodeContext, node: Node) -> NodeResult:
    cfg = node.config
    conn = ctx.airflow_connection(cfg["connection_id"])
    dag_id = cfg["dag_id"]
    state = ctx.node_state(node.id)

    if "run_id" in state:  # woken up: the run was started earlier, check on it
        run_id = state["run_id"]
        deadline = datetime.fromisoformat(state["deadline"])
        result = _poll_dag_run(
            ctx, ctx.airflow_adapter(conn), dag_id, run_id, deadline, after="it started"
        )
        if result.port:
            ctx.record_result(
                node.id,
                {"dag_id": dag_id, "run_id": run_id, "state": result.output.get("state", "")},
            )
        return result

    target = {"connection_id": str(conn.id), "dag_id": dag_id, "action": "trigger"}

    def execute() -> dict[str, Any]:
        conf = json.loads(cfg["parameters"] or "{}")
        note = f"Started by workflow '{ctx.run.workflow_name}'"
        result = _trigger_dag_once(
            ctx,
            node_id=node.id,
            conn=conn,
            dag_id=dag_id,
            note=note,
            conf=conf,
        )
        ctx.set_context("action_target", {**target, "run_id": result["run_id"]})
        return {**result, "dag_id": dag_id}

    result = _run_action(
        ctx, node, f"run {dag_id}", execute, target, environment=str(conn.environment)
    )
    run_id = result.output.get("run_id", "")
    simulated = bool(result.output.get("simulated"))
    if result.port != "success" or simulated or not cfg["wait_for_completion"]:
        started = "simulated" if simulated else "queued"
        ctx.record_result(
            node.id,
            {
                "dag_id": dag_id,
                "run_id": run_id,
                "state": started if result.port == "success" else "failed",
            },
        )
        return result

    deadline = ctx.now + timedelta(minutes=cfg["timeout_minutes"])
    ctx.set_node_state(node.id, {"run_id": run_id, "deadline": deadline.isoformat()})
    poll = timedelta(seconds=ctx.settings.AUTOMATION_VERIFY_POLL_SECONDS)
    return NodeResult(
        output=result.output,
        message=f"Started {run_id}; waiting for it to finish",
        wait_until=min(ctx.now + poll, deadline),
    )


def _wait_for_dag(ctx: NodeContext, node: Node) -> NodeResult:
    cfg = node.config
    conn = ctx.airflow_connection(cfg["connection_id"])
    dag_id = cfg["dag_id"]
    window = timedelta(minutes=cfg["success_within_minutes"])
    deadline = _deadline(ctx, node, cfg["timeout_minutes"])

    def give_up(reason: str, output: dict[str, Any]) -> NodeResult:
        ctx.set_context("last_error", reason)
        ctx.record_result(node.id, {"dag_id": dag_id, "succeeded": False})
        return NodeResult("timeout", output, reason)

    try:
        # Look back a day further than the window: a long run may have started before it.
        runs = run_async(
            lambda: ctx.airflow_adapter(conn).list_dag_runs(
                dag_id, since=ctx.now - window - timedelta(days=1)
            )
        )
    except AirflowAdapterError as exc:
        if ctx.now >= deadline:
            return give_up(f"Could not read runs of {dag_id}: {exc.result.message}", {})
        return _verify_wait(ctx, deadline, f"Airflow unavailable, retrying: {exc.result.message}")

    def finished(run: Any) -> datetime | None:
        return run.end_date or run.sort_date

    recent = [
        r
        for r in runs
        if r.state == "success" and finished(r) is not None and finished(r) >= ctx.now - window
    ]
    if recent:
        latest = max(recent, key=lambda r: finished(r))
        output = {"run_id": latest.run_id, "finished_at": finished(latest).isoformat()}
        ctx.record_result(node.id, {"dag_id": dag_id, "succeeded": True, **output})
        return NodeResult("success", output, f"{dag_id} succeeded ({latest.run_id})")
    if ctx.now >= deadline:
        minutes = cfg["success_within_minutes"]
        return give_up(f"{dag_id} had no successful run within {minutes} min.", {})
    return _verify_wait(ctx, deadline, f"No recent successful run of {dag_id} yet")


def _sql_summary(result: db_connector.SqlResult) -> dict[str, Any]:
    if not result.returns_rows:
        return {"rows_affected": result.row_count, "value": result.row_count}
    return {
        "columns": result.columns,
        "rows": result.rows,
        "row_count": result.row_count,
        "truncated": result.truncated,
        "value": result.rows[0][0] if result.rows and result.rows[0] else None,
    }


def _sql_message(summary: dict[str, Any]) -> str:
    if "rows_affected" in summary:
        return f"{summary['rows_affected']} row(s) affected"
    more = "+" if summary.get("truncated") else ""
    return f"Returned {summary['row_count'] - (1 if more else 0)}{more} row(s)"


def _query(
    conn: DatabaseConnection, sql: str, *, read_only: bool, timeout_seconds: int, max_rows: int
) -> db_connector.SqlResult:
    """Run SQL on a catalog connection; any problem becomes ActionFailed."""
    if not conn.is_active:
        raise ActionFailed(f"Database connection {conn.name} is turned off")
    try:
        target = database_connection_service.target_for(conn)
        return db_connector.execute(
            target, sql, read_only=read_only, timeout_seconds=timeout_seconds, max_rows=max_rows
        )
    except AppError as exc:
        raise ActionFailed(exc.message) from exc
    except db_connector.SqlError as exc:
        raise ActionFailed(str(exc)) from exc


def _run_sql(ctx: NodeContext, node: Node) -> NodeResult:
    cfg = node.config
    conn = ctx.database_connection(cfg["connection_id"])
    description = f"run SQL on {conn.name}"

    def execute() -> dict[str, Any]:
        result = _query(
            conn,
            cfg["sql"],
            read_only=cfg["read_only"],
            timeout_seconds=cfg["timeout_seconds"],
            max_rows=20,
        )
        summary = _sql_summary(result)
        ctx.record_result(node.id, summary)
        return summary

    if cfg["read_only"]:
        # A read-only transaction cannot change anything: no policy check, runs in dry runs too.
        try:
            output = execute()
        except ActionFailed as exc:
            ctx.set_context("last_error", f"Could not {description}: {exc}")
            return NodeResult("failed", {"error": str(exc)}, f"Query failed: {exc}")
        return NodeResult("success", output, _sql_message(output))

    result = _run_action(
        ctx, node, description, execute, {"database": conn.name}, environment=str(conn.environment)
    )
    if result.port == "success" and not result.output.get("simulated"):
        result.message = _sql_message(result.output)
    return result


_COMPARE = {
    ">": operator.gt,
    ">=": operator.ge,
    "=": operator.eq,
    "!=": operator.ne,
    "<": operator.lt,
    "<=": operator.le,
}


def compare(value: Any, op: str, expected: str) -> bool:
    """Numbers compare numerically; anything else only supports = and != (as text)."""
    if value is None:
        return False
    try:
        return _COMPARE[op](float(value), float(expected))
    except (TypeError, ValueError):
        if op in ("=", "!="):
            return (str(value) == expected) == (op == "=")
        return False


def _check_data(ctx: NodeContext, node: Node) -> NodeResult:
    cfg = node.config
    conn = ctx.database_connection(cfg["connection_id"])
    op, expected, keep_checking = cfg["operator"], cfg["expected"], cfg["keep_checking_minutes"]
    deadline = _deadline(ctx, node, keep_checking) if keep_checking else ctx.now

    try:
        result = _query(conn, cfg["sql"], read_only=True, timeout_seconds=60, max_rows=1)
    except ActionFailed as exc:
        if ctx.now < deadline:
            return _verify_wait(ctx, deadline, f"Query failed, retrying: {exc}")
        ctx.set_context("last_error", f"Data check on {conn.name} could not run: {exc}")
        ctx.record_result(node.id, {"value": None, "passed": False, "error": str(exc)})
        return NodeResult("fail", {"error": str(exc)}, f"Query failed: {exc}")

    if not result.returns_rows:
        raise NodeError("The data check query must return a value (use SELECT)")
    value = result.rows[0][0] if result.rows and result.rows[0] else None
    passed = compare(value, op, expected)
    output = {"value": value, "passed": passed, "rule": f"{op} {expected}"}
    ctx.record_result(node.id, output)
    if passed:
        return NodeResult("pass", output, f"Passed: {value} {op} {expected}")
    if ctx.now < deadline:
        return _verify_wait(ctx, deadline, f"Not yet: {value} is not {op} {expected}")
    ctx.set_context("last_error", f"Data check failed: got {value}, expected {op} {expected}.")
    return NodeResult("fail", output, f"Failed: {value} is not {op} {expected}")


# ---------------------------------------------------------------------- DAG run checks


def _record_check(ctx: NodeContext, node: Node) -> NodeResult:
    cfg = node.config
    dag_run = ctx.get_context("dag_run") or {}
    check = ctx.db.scalar(select(DagRunCheck).where(DagRunCheck.workflow_run_id == ctx.run.id))
    if check is None:
        raise NodeError("This run has no DAG run to record a status for")
    passed = cfg["result"] == "passed"
    note = render(cfg["note"] or "", ctx.render_values()).strip()
    label = "passed" if passed else "failed"
    if ctx.dry_run:
        return NodeResult("next", {"simulated": True}, f"Dry run: would mark the run as {label}")

    check.status = DagRunCheckStatus.PASSED if passed else DagRunCheckStatus.FAILED
    check.message = (note or ("Checks passed" if passed else "Checks failed"))[:2000]
    check.checked_at = ctx.now
    output: dict[str, Any] = {"status": str(check.status), "run_id": check.run_id}
    message = f"Marked run {check.run_id} as {label}"

    fingerprint = f"{IncidentType.DATA_CHECK_FAILED.value}:{check.connection_id}:{check.dag_id}"
    open_incident = ctx.db.scalar(
        select(Incident).where(
            Incident.fingerprint == fingerprint,
            Incident.status.in_(incident_service.OPEN_STATUSES),
        )
    )
    if passed and open_incident is not None:
        incident_service.mark_resolved(
            open_incident, IncidentResolution.AUTO_RECOVERED, note=f"Run {check.run_id} passed"
        )
        incident_service.add_event(
            ctx.db, open_incident, "auto_resolved", details={"recovered_by_run": check.run_id}
        )
        output["resolved_incident"] = str(open_incident.id)
        message += "; the data check incident is resolved"
    elif not passed and cfg["open_incident"] and check.monitored_dag_id is not None:
        incident = _data_check_incident(ctx, check, open_incident, fingerprint, dag_run, note)
        output["incident_id"] = str(incident.id)
        message += f"; incident {'updated' if open_incident else 'opened'}"
    ctx.audit("dag_run_check.recorded", dag_id=check.dag_id, run_id=check.run_id, status=label)
    return NodeResult("next", output, message)


def _data_check_incident(
    ctx: NodeContext,
    check: DagRunCheck,
    incident: Incident | None,
    fingerprint: str,
    dag_run: dict[str, Any],
    note: str,
) -> Incident:
    """Open (or record again) the DAG's "Data check failed" incident and queue its workflows."""
    from app.services import automation_service  # imports this module

    summary = note or f"Run {check.run_id} failed its data checks"
    if incident is not None:
        incident.occurrence_count += 1
        incident.last_seen_at = ctx.now
        incident.occurred_at = ctx.now
        incident.last_run_id = check.run_id
        incident.summary = summary
        incident_service.add_event(
            ctx.db,
            incident,
            "recurred",
            details={"run_id": check.run_id, "count": incident.occurrence_count},
        )
        event = TriggerEvent.RECURRED
    else:
        dag = ctx.db.get(MonitoredDag, check.monitored_dag_id)
        score = severity.score(
            IncidentType.DATA_CHECK_FAILED,
            environment=dag_run.get("environment", ""),
            tags=dag.tags if dag else None,
            recent_failures=0,
        )
        incident = Incident(
            id=uuid.uuid4(),
            connection_id=check.connection_id,
            monitored_dag_id=check.monitored_dag_id,
            dag_id=check.dag_id,
            run_id=check.run_id,
            last_run_id=check.run_id,
            type=IncidentType.DATA_CHECK_FAILED,
            severity=score.severity,
            title=f"{check.dag_id}: data check failed",
            summary=summary,
            fingerprint=fingerprint,
            occurred_at=ctx.now,
            first_seen_at=ctx.now,
            last_seen_at=ctx.now,
        )
        ctx.db.add(incident)
        ctx.db.flush()
        incident_service.add_event(
            ctx.db,
            incident,
            "opened",
            details={
                "severity": incident.severity,
                "severity_reasons": score.reasons,
                "run_id": check.run_id,
                "workflow": ctx.run.workflow_name,
            },
        )
        audit_service.record(
            ctx.db,
            action="incident.opened",
            entity_type="incident",
            entity_id=incident.id,
            details={
                "dag_id": check.dag_id,
                "type": IncidentType.DATA_CHECK_FAILED,
                "severity": incident.severity,
                "run_id": check.run_id,
            },
        )
        event = TriggerEvent.OPENED
    ctx.db.flush()
    automation_service.enqueue_for_incident(ctx.db, incident, event, now=ctx.now)
    return incident


EXECUTORS: dict[str, Callable[[NodeContext, Node], NodeResult]] = {
    "trigger.incident": _trigger,
    "trigger.incident_stale": _trigger,
    "trigger.manual": _trigger,
    "trigger.schedule": _trigger,
    "trigger.dag_run": _trigger,
    "flow.wait": _wait,
    "pipeline.run_dag": _run_dag,
    "pipeline.wait_for_dag": _wait_for_dag,
    "database.run_sql": _run_sql,
    "database.check": _check_data,
    "condition.filter": _filter,
    "analyze.task_logs": _analyze_task_logs,
    "diagnose.classify_log": _classify,
    "check.dag_state": _dag_state,
    "ai.generate_fix": _ai_generate_fix,
    "decide.choose_fix": _choose_fix,
    "approval.request": _approval,
    "action.clear_failed_tasks": _clear_failed_tasks,
    "action.trigger_dag_run": _trigger_dag_run,
    "action.set_dag_paused": _set_dag_paused,
    "verify.run_success": _verify,
    "incident.update": _incident_update,
    "notify": _notify,
    "check.record": _record_check,
}
