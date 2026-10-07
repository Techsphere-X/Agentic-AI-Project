"""Built-in workflows and parameterized workflow templates."""

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, Field, field_validator

RETRYABLE_NOW = ["TRANSIENT_NETWORK", "TIMEOUT"]
RETRY_WITH_APPROVAL = ["RESOURCE", "UPSTREAM_MISSING", "UNKNOWN"]
DO_NOT_RETRY = ["DATA_INTEGRITY", "SCHEMA", "CODE_BUG", "AUTH"]


@dataclass(frozen=True)
class Template:
    key: str
    name: str
    description: str
    graph: dict[str, Any] | None = None
    version: int = 1
    parameter_model: type[BaseModel] | None = None
    builder: Callable[[BaseModel], dict[str, Any]] | None = None
    supported_trigger_types: tuple[str, ...] = ()

    def validate_parameters(self, parameters: dict[str, Any] | None = None) -> BaseModel:
        model = self.parameter_model or EmptyParameters
        return model.model_validate(parameters or {})

    def build(self, parameters: dict[str, Any] | None = None) -> dict[str, Any]:
        values = self.validate_parameters(parameters)
        if self.builder is not None:
            return self.builder(values)
        if self.graph is None:
            raise ValueError(f"Template '{self.key}' has no graph")
        return self.graph

    @property
    def parameter_schema(self) -> dict[str, Any]:
        return (self.parameter_model or EmptyParameters).model_json_schema()


class EmptyParameters(BaseModel):
    model_config = {"extra": "forbid"}


class RunVerifyDagParameters(BaseModel):
    connection_id: str = Field(
        min_length=1,
        json_schema_extra={"x-label": "Airflow connection", "x-widget": "airflow_connection"},
    )
    dag_id: str = Field(
        min_length=1,
        max_length=250,
        json_schema_extra={"x-label": "DAG", "x-widget": "dag"},
    )
    parameters: str = Field(
        default="{}",
        max_length=10000,
        json_schema_extra={"x-label": "Run parameters (JSON)", "x-widget": "json"},
    )
    timeout_minutes: int = Field(default=60, ge=1, le=1440)

    @field_validator("parameters")
    @classmethod
    def _json_object(cls, value: str) -> str:
        try:
            parsed = json.loads(value or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError(f"Run parameters are not valid JSON: {exc.msg}") from exc
        if not isinstance(parsed, dict):
            raise ValueError("Run parameters must be a JSON object")
        return value or "{}"


def _n(node_id: str, node_type: str, name: str | None = None, **config: Any) -> dict[str, Any]:
    node: dict[str, Any] = {"id": node_id, "type": node_type, "config": config}
    if name:
        node["name"] = name
    return node


def _e(src: str, port: str, dst: str) -> dict[str, str]:
    return {"from": src, "port": port, "to": dst}


def _run_verify_dag_graph(parameters: RunVerifyDagParameters) -> dict[str, Any]:
    return {
        "nodes": [
            _n("trigger", "trigger.manual"),
            _n(
                "run",
                "pipeline.run_dag",
                connection_id=parameters.connection_id,
                dag_id=parameters.dag_id,
                parameters=parameters.parameters,
                timeout_minutes=parameters.timeout_minutes,
            ),
            _n("verify", "verify.run_success", timeout_minutes=parameters.timeout_minutes),
            _n(
                "notify",
                "notify",
                level="INFO",
                title="DAG run succeeded: {{workflow.name}}",
                message=f"{parameters.dag_id} completed successfully.",
            ),
        ],
        "edges": [
            _e("trigger", "next", "run"),
            _e("run", "success", "verify"),
            _e("verify", "success", "notify"),
        ],
    }


def _retry_graph(categories: list[str], approval_envs: list[str]) -> dict[str, Any]:
    """failed run -> classify -> filter -> approval -> clear -> verify -> resolve / escalate."""
    return {
        "nodes": [
            _n(
                "trigger",
                "trigger.incident",
                events=["opened", "recurred"],
                incident_types=["DAG_RUN_FAILED"],
            ),
            _n("classify", "diagnose.classify_log"),
            _n(
                "is_retryable",
                "condition.filter",
                "Retryable and not flapping?",
                diagnosis_categories=categories,
                max_occurrences=2,
            ),
            _n("approval", "approval.request", required_environments=approval_envs),
            _n("retry", "action.clear_failed_tasks", include_downstream=True),
            _n("verify", "verify.run_success", timeout_minutes=30),
            _n(
                "resolve",
                "incident.update",
                operation="resolve",
                note="Retried failed tasks ({{diagnosis.label}}); the run then succeeded.",
            ),
            _n(
                "notify_ok",
                "notify",
                level="INFO",
                title="Fixed automatically: {{incident.title}}",
                message="{{diagnosis.label}} on {{incident.dag_id}}. Retried and verified.",
            ),
            _n("escalate", "incident.update", operation="escalate"),
            _n(
                "notify_fail",
                "notify",
                level="WARNING",
                title="Automatic retry did not fix {{incident.title}}",
                message="{{diagnosis.label}} on {{incident.dag_id}}. {{last_error}} "
                "Please investigate.",
            ),
        ],
        "edges": [
            _e("trigger", "next", "classify"),
            _e("classify", "next", "is_retryable"),
            _e("is_retryable", "true", "approval"),
            _e("approval", "approved", "retry"),
            _e("retry", "success", "verify"),
            _e("retry", "failed", "escalate"),
            _e("verify", "success", "resolve"),
            _e("verify", "failed", "escalate"),
            _e("resolve", "next", "notify_ok"),
            _e("escalate", "next", "notify_fail"),
        ],
    }


def _choose_fix_graph() -> dict[str, Any]:
    """failure / SLA miss -> diagnose -> choose the fix -> the matching fix, verified."""
    return {
        "nodes": [
            _n(
                "trigger",
                "trigger.incident",
                events=["opened", "recurred"],
                incident_types=["DAG_RUN_FAILED", "SLA_MISSED"],
            ),
            _n("diagnose", "diagnose.classify_log"),
            _n("decide", "decide.choose_fix"),
            # retry the failed tasks
            _n("approve_retry", "approval.request", required_environments=["PROD"]),
            _n("retry", "action.clear_failed_tasks", include_downstream=True),
            # start a new run (also after waiting for missing input)
            _n("wait", "flow.wait", "Give the input time to arrive", minutes=30),
            _n("approve_rerun", "approval.request", required_environments=["PROD"]),
            _n("rerun", "action.trigger_dag_run"),
            _n("verify", "verify.run_success", timeout_minutes=60),
            _n(
                "resolve",
                "incident.update",
                operation="resolve",
                note="{{decision.reason}} The run then succeeded.",
            ),
            _n(
                "notify_ok",
                "notify",
                level="INFO",
                title="Fixed automatically: {{incident.title}}",
                message="{{diagnosis.label}} on {{incident.dag_id}}: {{decision.label}}, verified.",
            ),
            # pause (only chosen when the block's settings allow it; always asks)
            _n("approve_pause", "approval.request", required_environments=[], timeout_minutes=240),
            _n("pause", "action.set_dag_paused", paused=True),
            _n("paused_note", "incident.update", operation="note", note="{{decision.reason}}"),
            # hand to a person
            _n("escalate", "incident.update", operation="escalate"),
            _n(
                "notify_fail",
                "notify",
                level="WARNING",
                title="Needs a person: {{incident.title}}",
                message="{{diagnosis.label}} on {{incident.dag_id}}. {{last_error}}",
            ),
            _n("ignore_note", "incident.update", operation="note", note="{{decision.reason}}"),
        ],
        "edges": [
            _e("trigger", "next", "diagnose"),
            _e("diagnose", "next", "decide"),
            _e("decide", "retry", "approve_retry"),
            _e("approve_retry", "approved", "retry"),
            _e("retry", "success", "verify"),
            _e("retry", "failed", "escalate"),
            _e("decide", "rerun", "approve_rerun"),
            _e("decide", "wait", "wait"),
            _e("wait", "next", "approve_rerun"),
            _e("approve_rerun", "approved", "rerun"),
            _e("rerun", "success", "verify"),
            _e("rerun", "failed", "escalate"),
            _e("verify", "success", "resolve"),
            _e("verify", "failed", "escalate"),
            _e("resolve", "next", "notify_ok"),
            _e("decide", "pause", "approve_pause"),
            _e("approve_pause", "approved", "pause"),
            _e("pause", "success", "paused_note"),
            _e("pause", "failed", "escalate"),
            _e("decide", "escalate", "escalate"),
            _e("escalate", "next", "notify_fail"),
            _e("decide", "ignore", "ignore_note"),
        ],
    }


TEMPLATES: tuple[Template, ...] = (
    Template(
        key="retry-transient",
        name="Auto-retry transient failures",
        description=(
            "Network glitch or timeout: retry the failed tasks, check the run succeeds, and "
            "close the incident. Asks for approval in PROD."
        ),
        graph=_retry_graph(RETRYABLE_NOW, ["PROD"]),
    ),
    Template(
        key="retry-with-approval",
        name="Retry other failures with approval",
        description=(
            "Out-of-memory, missing input or unknown failures: ask a human, then retry and verify."
        ),
        graph=_retry_graph(RETRY_WITH_APPROVAL, []),
    ),
    Template(
        key="no-retry-data-code",
        name="Don't retry data, code or login errors",
        description=(
            "Bad data, schema changes, code bugs and login problems would just fail again: "
            "record why and alert the owner instead."
        ),
        graph={
            "nodes": [
                _n(
                    "trigger",
                    "trigger.incident",
                    events=["opened"],
                    incident_types=["DAG_RUN_FAILED"],
                ),
                _n("classify", "diagnose.classify_log"),
                _n(
                    "not_retryable",
                    "condition.filter",
                    "Needs a human fix?",
                    diagnosis_categories=DO_NOT_RETRY,
                ),
                _n(
                    "note",
                    "incident.update",
                    operation="note",
                    note="Not retried automatically: {{diagnosis.label}} "
                    "({{diagnosis.matched_line}}).",
                ),
                _n(
                    "notify",
                    "notify",
                    level="WARNING",
                    title="Needs a fix: {{incident.title}}",
                    message="{{diagnosis.label}} on {{incident.dag_id}}. Retrying will not help: "
                    "{{diagnosis.matched_line}}",
                ),
            ],
            "edges": [
                _e("trigger", "next", "classify"),
                _e("classify", "next", "not_retryable"),
                _e("not_retryable", "true", "note"),
                _e("note", "next", "notify"),
            ],
        },
    ),
    Template(
        key="sla-rerun",
        name="Re-run a DAG that missed its SLA",
        description=(
            "When a DAG misses its freshness SLA and nothing is running, start a new run and "
            "check it succeeds. Asks for approval in PROD."
        ),
        graph={
            "nodes": [
                _n("trigger", "trigger.incident", events=["opened"], incident_types=["SLA_MISSED"]),
                _n("state", "check.dag_state"),
                _n("approval", "approval.request", required_environments=["PROD"]),
                _n("run", "action.trigger_dag_run"),
                _n("verify", "verify.run_success", timeout_minutes=60),
                _n(
                    "resolve",
                    "incident.update",
                    operation="resolve",
                    note="Started a new run after the SLA miss; it succeeded.",
                ),
                _n(
                    "notify_paused",
                    "notify",
                    level="WARNING",
                    title="SLA missed on a paused DAG: {{incident.dag_id}}",
                    message="{{incident.dag_id}} is paused in Airflow, so it cannot meet its SLA. "
                    "Unpause it or remove the SLA.",
                ),
                _n(
                    "notify_fail",
                    "notify",
                    level="WARNING",
                    title="Re-run did not fix {{incident.title}}",
                    message="{{last_error}}",
                ),
            ],
            "edges": [
                _e("trigger", "next", "state"),
                _e("state", "ready", "approval"),
                _e("state", "paused", "notify_paused"),
                _e("approval", "approved", "run"),
                _e("run", "success", "verify"),
                _e("run", "failed", "notify_fail"),
                _e("verify", "success", "resolve"),
                _e("verify", "failed", "notify_fail"),
            ],
        },
    ),
    Template(
        key="flapping-breaker",
        name="Stop a DAG that keeps failing",
        description=(
            "Three or more failures: stop retrying, raise the alarm, and offer to pause the DAG "
            "(always asks for approval)."
        ),
        graph={
            "nodes": [
                _n(
                    "trigger",
                    "trigger.incident",
                    events=["recurred"],
                    incident_types=["DAG_RUN_FAILED"],
                ),
                _n("flapping", "condition.filter", "Failed 3+ times?", min_occurrences=3),
                _n("escalate", "incident.update", operation="escalate"),
                _n(
                    "notify",
                    "notify",
                    level="CRITICAL",
                    title="{{incident.dag_id}} keeps failing",
                    message="{{incident.dag_id}} failed {{incident.occurrence_count}} times. "
                    "Approve pausing it to stop the damage.",
                ),
                _n("approval", "approval.request", required_environments=[], timeout_minutes=240),
                _n("pause", "action.set_dag_paused", paused=True),
                _n(
                    "note",
                    "incident.update",
                    operation="note",
                    note="DAG paused by automation after repeated failures.",
                ),
            ],
            "edges": [
                _e("trigger", "next", "flapping"),
                _e("flapping", "true", "escalate"),
                _e("escalate", "next", "notify"),
                _e("notify", "next", "approval"),
                _e("approval", "approved", "pause"),
                _e("pause", "success", "note"),
            ],
        },
    ),
    Template(
        key="choose-fix",
        name="Choose and apply the fix",
        description=(
            "One workflow for every failure and SLA miss: diagnose it, pick the fix (retry, new "
            "run, wait for input, or hand to a person) from the cause and earlier attempts, "
            "apply it and check it worked. Use instead of the retry workflows, not with them."
        ),
        graph=_choose_fix_graph(),
    ),
    Template(
        key="stale-escalation",
        name="Remind about ignored incidents",
        description="An incident nobody acknowledged for 30 minutes: raise severity and notify.",
        graph={
            "nodes": [
                _n("trigger", "trigger.incident_stale", minutes=30),
                _n("escalate", "incident.update", operation="escalate"),
                _n(
                    "notify",
                    "notify",
                    level="WARNING",
                    title="Still unacknowledged: {{incident.title}}",
                    message="Open for 30+ minutes with no one looking. Severity raised to "
                    "{{incident.severity}}.",
                ),
            ],
            "edges": [_e("trigger", "next", "escalate"), _e("escalate", "next", "notify")],
        },
    ),
)

RUN_VERIFY_TEMPLATE = Template(
    key="run-verify-dag",
    name="Run and verify a DAG",
    description="Start a selected Airflow DAG, wait for completion, and notify on success.",
    graph=_run_verify_dag_graph(
        RunVerifyDagParameters(connection_id="template-connection", dag_id="template-dag")
    ),
    parameter_model=RunVerifyDagParameters,
    builder=_run_verify_dag_graph,
    supported_trigger_types=("trigger.manual",),
)

ALL_TEMPLATES: tuple[Template, ...] = (*TEMPLATES, RUN_VERIFY_TEMPLATE)
TEMPLATES_BY_KEY = {t.key: t for t in ALL_TEMPLATES}
