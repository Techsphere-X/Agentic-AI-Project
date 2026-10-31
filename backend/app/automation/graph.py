"""Workflow graph: node catalog (types, ports, config models) and validation.

A workflow graph is `{"nodes": [{id, type, name?, config}], "edges": [{from, port, to}]}`.
Execution starts at the single trigger node and follows the edge for each node's output port;
a port with no edge ends the run. This is also the contract for the Plan 3 canvas.

Pure: Pydantic only, no database or framework imports.
"""

import json
import math
import re
from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator
from pydantic_core import PydanticCustomError

from app.detection.types import IncidentSeverity, IncidentType
from app.diagnosis.log_classifier import CATEGORY_LABELS, FailureCategory

MAX_NODES = 100
MAX_COORDINATE = 100_000
NODE_ID_PATTERN = r"^[A-Za-z0-9_-]{1,100}$"
# Problem `kind`: a choice only the user can make (a connection, a DAG, the SQL), not a mistake.
INCOMPLETE = "incomplete"


class _Config(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NoConfig(_Config):
    pass


class _IncidentScope(_Config):
    """Which incidents start the workflow. Empty / unset = any, so a workflow with no scope
    set reacts to every incident, as before."""

    dag_ids: list[str] = Field(
        default_factory=list,
        json_schema_extra={"x-label": "DAGs", "x-hint": "Empty = every monitored DAG"},
    )
    environments: list[Literal["DEV", "STAGING", "PROD"]] = Field(
        default_factory=list,
        json_schema_extra={
            "x-label": "Environments",
            "x-hint": "Empty = every environment",
            "x-options": {"DEV": "Dev", "STAGING": "Staging", "PROD": "Production"},
        },
    )
    min_severity: IncidentSeverity | None = Field(
        default=None,
        json_schema_extra={
            "x-label": "At least this severe",
            "x-options": {"LOW": "Low", "MEDIUM": "Medium", "HIGH": "High", "CRITICAL": "Critical"},
        },
    )

    @field_validator("dag_ids")
    @classmethod
    def _clean(cls, value: list[str]) -> list[str]:
        return [v.strip() for v in value if v.strip()]


class IncidentTriggerConfig(_IncidentScope):
    events: list[Literal["opened", "recurred"]] = Field(
        default=["opened"],
        min_length=1,
        json_schema_extra={
            "x-label": "Start when the incident",
            "x-options": {"opened": "Opens", "recurred": "Happens again"},
        },
    )
    incident_types: list[IncidentType] = Field(
        default_factory=list,
        json_schema_extra={
            "x-label": "Incident types",
            "x-options": {
                "DAG_RUN_FAILED": "DAG run failed",
                "SLA_MISSED": "SLA missed",
                "DATA_CHECK_FAILED": "Data check failed",
            },
        },
    )  # empty = any


class StaleTriggerConfig(_IncidentScope):
    minutes: int = Field(
        default=30,
        ge=5,
        le=10080,
        json_schema_extra={
            "x-label": "Open and unacknowledged for (minutes)",
        },
    )


class DagRunTriggerConfig(_Config):
    states: list[Literal["success", "in_progress", "failed"]] = Field(
        default=["success", "failed"],
        min_length=1,
        json_schema_extra={
            "x-label": "When the run",
            "x-options": {
                "success": "Succeeded",
                "in_progress": "In progress",
                "failed": "Failed",
            },
        },
    )
    dag_ids: list[str] = Field(
        default_factory=list,
        json_schema_extra={"x-label": "DAGs", "x-hint": "Empty = every monitored DAG"},
    )

    @field_validator("dag_ids")
    @classmethod
    def _clean(cls, value: list[str]) -> list[str]:
        return [v.strip() for v in value if v.strip()]


class FilterConfig(_Config):
    """All criteria that are set must match; unset (empty/None) criteria are ignored."""

    environments: list[Literal["DEV", "STAGING", "PROD"]] = Field(
        default_factory=list,
        json_schema_extra={
            "x-label": "Environments",
            "x-options": {"DEV": "Dev", "STAGING": "Staging", "PROD": "Production"},
        },
    )
    incident_types: list[IncidentType] = Field(
        default_factory=list,
        json_schema_extra={
            "x-label": "Incident types",
            "x-options": {
                "DAG_RUN_FAILED": "DAG run failed",
                "SLA_MISSED": "SLA missed",
                "DATA_CHECK_FAILED": "Data check failed",
            },
        },
    )
    min_severity: IncidentSeverity | None = Field(
        default=None,
        json_schema_extra={
            "x-label": "At least this severe",
            "x-options": {"LOW": "Low", "MEDIUM": "Medium", "HIGH": "High", "CRITICAL": "Critical"},
        },
    )
    dag_ids: list[str] = Field(default_factory=list, json_schema_extra={"x-label": "DAGs"})
    tags_any: list[str] = Field(
        default_factory=list,
        json_schema_extra={"x-label": "DAG has any of these tags"},
    )
    min_occurrences: int | None = Field(
        default=None,
        ge=1,
        json_schema_extra={"x-label": "Failed at least (times)"},
    )
    max_occurrences: int | None = Field(
        default=None,
        ge=1,
        json_schema_extra={"x-label": "Failed at most (times)"},
    )
    diagnosis_categories: list[FailureCategory] = Field(
        default_factory=list,
        json_schema_extra={"x-label": "Failure causes", "x-options": dict(CATEGORY_LABELS)},
    )


# `x-widget` / `x-label` / `x-hint` / `x-hidden` are UI hints for the canvas settings form.
def _ui(
    label: str,
    widget: str | None = None,
    hint: str | None = None,
    options: dict[str, str] | None = None,
) -> dict[str, Any]:
    extra: dict[str, Any] = {"x-label": label}
    if widget:
        extra["x-widget"] = widget
    if hint:
        extra["x-hint"] = hint
    if options:
        extra["x-options"] = options  # option value -> label shown in the form
    return extra


_ENVIRONMENTS = {"DEV": "Dev", "STAGING": "Staging", "PROD": "Production"}
_INCIDENT_TYPES = {
    "DAG_RUN_FAILED": "DAG run failed",
    "SLA_MISSED": "SLA missed",
    "DATA_CHECK_FAILED": "Data check failed",
}
_SEVERITIES = {"LOW": "Low", "MEDIUM": "Medium", "HIGH": "High", "CRITICAL": "Critical"}


_SEND_VIA_HINT = "Empty = in-app only"
_TO_HINT = "Email addresses; empty = the channel's default recipients"
MAX_RECIPIENTS = 20


def _recipients(values: list[str]) -> list[str]:
    cleaned = [v.strip() for v in values if v.strip()]
    if len(cleaned) > MAX_RECIPIENTS:
        raise ValueError(f"At most {MAX_RECIPIENTS} recipients")
    bad = [v for v in cleaned if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", v)]
    if bad:
        raise ValueError(f"Not an email address: {', '.join(bad)}")
    return cleaned


class ApprovalConfig(_Config):
    # Approval is required when the connection environment is listed; empty = always required.
    required_environments: list[Literal["DEV", "STAGING", "PROD"]] = Field(
        default_factory=lambda: ["PROD"],
        json_schema_extra=_ui(
            "Ask in these environments",
            hint="Elsewhere it is approved automatically",
            options=_ENVIRONMENTS,
        ),
    )
    timeout_minutes: int = Field(
        default=60, ge=1, le=10080, json_schema_extra=_ui("Expires after (minutes)")
    )
    send_via: str = Field(
        default="",
        json_schema_extra=_ui("Also send the request via", "notification_channel", _SEND_VIA_HINT),
    )
    to: list[str] = Field(default_factory=list, json_schema_extra=_ui("To", hint=_TO_HINT))

    @field_validator("to")
    @classmethod
    def _check_to(cls, value: list[str]) -> list[str]:
        return _recipients(value)


class DiagnoseConfig(_Config):
    min_confidence_pct: int | None = Field(
        default=None,
        ge=0,
        le=100,
        json_schema_extra=_ui(
            "Minimum confidence (%)",
            hint="Below this the outcome is 'unknown'",
        ),
    )
    refresh: bool = Field(
        default=False,
        json_schema_extra=_ui(
            "Re-check the latest logs",
        ),
    )


class AnalyzeLogsConfig(_Config):
    max_log_lines: int = Field(
        default=100,
        ge=10,
        le=500,
        json_schema_extra=_ui(
            "Max log lines to analyze",
        ),
    )
    redact_secrets: bool = Field(
        default=True,
        json_schema_extra=_ui("Redact secrets and tokens"),
    )


class AiGenerateFixConfig(_Config):
    min_confidence_pct: int = Field(
        default=70,
        ge=0,
        le=100,
        json_schema_extra=_ui(
            "Minimum confidence (%)",
        ),
    )
    instruction: str = Field(
        default="",
        max_length=1000,
        json_schema_extra=_ui(
            "Custom instructions",
        ),
    )


class ChooseFixConfig(_Config):
    max_attempts: int = Field(
        default=2,
        ge=1,
        le=10,
        json_schema_extra=_ui(
            "Automatic fixes before handing over",
        ),
    )
    min_confidence_pct: int | None = Field(
        default=None,
        ge=0,
        le=100,
        json_schema_extra=_ui(
            "Minimum diagnosis confidence (%)",
            hint="Below this the cause counts as unknown",
        ),
    )
    retry_unknown_once: bool = Field(
        default=True, json_schema_extra=_ui("Try once when the cause is unknown")
    )
    pause_on_bad_data: bool = Field(
        default=False,
        json_schema_extra=_ui(
            "Pause the DAG on bad data or schema changes",
        ),
    )
    pause_after_failures: int | None = Field(
        default=None,
        ge=2,
        le=100,
        json_schema_extra=_ui("Pause after this many failures", hint="Empty = never"),
    )
    check_dag_state: bool = Field(
        default=True,
        json_schema_extra=_ui(
            "Ask Airflow first",
        ),
    )
    ai_mode: Literal["off", "suggest", "decide"] = Field(
        default="off",
        json_schema_extra=_ui(
            "Local AI",
            hint="The AI only ever chooses among the fixes the rules allow",
            options={
                "off": "Off (rules only)",
                "suggest": "Suggest: record the AI's pick next to the rules' choice",
                "decide": "Decide: the AI picks",
            },
        ),
    )
    ai_min_confidence_pct: int | None = Field(
        default=None,
        ge=0,
        le=100,
        json_schema_extra=_ui(
            "Minimum AI confidence (%)",
        ),
    )


class SlaCheckConfig(_Config):
    dag_id: str = Field(
        default="",
        max_length=250,
        json_schema_extra=_ui(
            "DAG",
            "dag",
            hint="Empty = the trigger's DAG",
        ),
    )
    metric: Literal["duration", "freshness"] = Field(
        default="duration",
        json_schema_extra=_ui(
            "Measure",
            options={
                "duration": "Run duration (how long the run takes)",
                "freshness": "Freshness (time since the last successful run)",
            },
        ),
    )
    margin_pct: int = Field(
        default=20,
        ge=0,
        le=500,
        json_schema_extra=_ui(
            "Buffer margin (%)",
        ),
    )
    sample_size: int = Field(
        default=10,
        ge=2,
        le=50,
        json_schema_extra=_ui(
            "Historical sample size",
        ),
    )
    calculation_method: Literal["median", "mean", "p95"] = Field(
        default="median",
        json_schema_extra=_ui(
            "Expected time from past runs",
            options={"median": "Median", "mean": "Average", "p95": "95th percentile (slowest 5%)"},
        ),
    )
    fallback_minutes: float = Field(
        default=60.0,
        ge=0.1,
        le=10080.0,
        json_schema_extra=_ui(
            "Fallback SLA (minutes)",
        ),
    )
    only_in_flight: bool = Field(
        default=False,
        json_schema_extra=_ui(
            "Only check in-flight runs",
        ),
    )


class ClearTasksConfig(_Config):
    include_downstream: bool = Field(
        default=True, json_schema_extra=_ui("Also retry the tasks after them (downstream)")
    )


class SetPausedConfig(_Config):
    paused: bool = Field(
        default=True, json_schema_extra=_ui("Pause the DAG", hint="Off = resume (unpause) it")
    )


class VerifyConfig(_Config):
    timeout_minutes: int = Field(
        default=30, ge=1, le=1440, json_schema_extra=_ui("Wait for the run up to (minutes)")
    )


class IncidentUpdateConfig(_Config):
    operation: Literal["resolve", "escalate", "acknowledge", "note"] = Field(
        json_schema_extra=_ui(
            "What to do",
            options={
                "resolve": "Resolve it",
                "escalate": "Escalate it",
                "acknowledge": "Acknowledge it",
                "note": "Only add a note",
            },
        )
    )
    note: str | None = Field(
        default=None,
        max_length=2000,
        json_schema_extra=_ui("Note", "template"),
    )


class RecordCheckConfig(_Config):
    result: Literal["passed", "failed"] = Field(
        default="passed",
        json_schema_extra={
            "x-label": "Mark the run as",
            "x-options": {"passed": "Passed", "failed": "Failed"},
        },
    )
    note: str = Field(
        default="",
        max_length=2000,
        json_schema_extra={
            "x-label": "Note",
            "x-widget": "template",
        },
    )
    open_incident: bool = Field(
        default=True,
        json_schema_extra={
            "x-label": "Open an incident when failed",
            "x-hint": "A later passed run resolves it",
        },
    )


class NotifyConfig(_Config):
    send_via: str = Field(
        default="", json_schema_extra=_ui("Send via", "notification_channel", _SEND_VIA_HINT)
    )
    to: list[str] = Field(default_factory=list, json_schema_extra=_ui("To", hint=_TO_HINT))
    level: Literal["INFO", "WARNING", "CRITICAL"] = Field(
        default="INFO",
        json_schema_extra=_ui(
            "Importance", options={"INFO": "Info", "WARNING": "Warning", "CRITICAL": "Critical"}
        ),
    )
    title: str = Field(
        default="{{workflow.name}}",
        min_length=1,
        max_length=300,
        json_schema_extra=_ui("Title", "template"),
    )
    message: str = Field(default="", max_length=4000, json_schema_extra=_ui("Message", "template"))
    # Before notification channels: a raw JSON webhook typed into the block. Kept so saved
    # workflows keep working; hidden in the form unless used.
    channel: Literal["in_app", "webhook"] = Field(
        default="in_app", json_schema_extra={"x-hidden": True}
    )
    url: str | None = Field(default=None, max_length=1000, json_schema_extra={"x-hidden": True})

    @field_validator("to")
    @classmethod
    def _check_to(cls, value: list[str]) -> list[str]:
        return _recipients(value)

    @field_validator("url")
    @classmethod
    def _http_url(cls, value: str | None) -> str | None:
        if value is not None and not value.startswith(("http://", "https://")):
            raise ValueError("url must start with http:// or https://")
        return value


# ---------------------------------------------------------------------- orchestration configs
# `x-widget` / `x-label` / `x-hint` are UI hints for the canvas settings form.


class ScheduleTriggerConfig(_Config):
    every_minutes: int = Field(default=60, ge=5, le=10080, json_schema_extra=_ui("Every (minutes)"))


def _incomplete(message: str) -> PydanticCustomError:
    return PydanticCustomError(INCOMPLETE, message)


class _AirflowTarget(_Config):
    connection_id: str = Field(
        default="", json_schema_extra=_ui("Airflow connection", "airflow_connection")
    )
    dag_id: str = Field(default="", max_length=250, json_schema_extra=_ui("DAG", "dag"))

    @model_validator(mode="after")
    def _chosen(self) -> "_AirflowTarget":
        if not self.connection_id:
            raise _incomplete("Choose an Airflow connection")
        if not self.dag_id:
            raise _incomplete("Choose a DAG")
        return self


class RunDagConfig(_AirflowTarget):
    parameters: str = Field(
        default="{}",
        max_length=10000,
        json_schema_extra=_ui("Run parameters (JSON)", "json", "Passed to the run as its conf"),
    )
    wait_for_completion: bool = Field(
        default=True, json_schema_extra=_ui("Wait until the run finishes")
    )
    timeout_minutes: int = Field(
        default=60, ge=1, le=1440, json_schema_extra=_ui("Give up after (minutes)")
    )

    @field_validator("parameters")
    @classmethod
    def _json_object(cls, value: str) -> str:
        try:
            parsed = json.loads(value or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError(f"Run parameters are not valid JSON: {exc.msg}") from exc
        if not isinstance(parsed, dict):
            raise ValueError('Run parameters must be a JSON object, e.g. {"date": "2026-01-31"}')
        return value or "{}"


class WaitForDagConfig(_AirflowTarget):
    success_within_minutes: int = Field(
        default=60,
        ge=1,
        le=10080,
        json_schema_extra=_ui("Counts if it succeeded within (minutes)"),
    )
    timeout_minutes: int = Field(
        default=60, ge=1, le=1440, json_schema_extra=_ui("Give up after (minutes)")
    )


class _DatabaseTarget(_Config):
    connection_id: str = Field(
        default="", json_schema_extra=_ui("Database connection", "database_connection")
    )
    sql: str = Field(default="", max_length=20000, json_schema_extra=_ui("SQL", "sql"))

    @model_validator(mode="after")
    def _chosen(self) -> "_DatabaseTarget":
        if not self.connection_id:
            raise _incomplete("Choose a database connection")
        if not self.sql.strip():
            raise _incomplete("Enter the SQL to run")
        return self


class RunSqlConfig(_DatabaseTarget):
    read_only: bool = Field(
        default=False,
        json_schema_extra=_ui("Read only", hint="Needs no approval"),
    )
    timeout_seconds: int = Field(
        default=60, ge=1, le=3600, json_schema_extra=_ui("Timeout (seconds)")
    )


class CheckDataConfig(_DatabaseTarget):
    operator: Literal[">", ">=", "=", "!=", "<", "<="] = Field(
        default=">", json_schema_extra=_ui("Result must be")
    )
    expected: str = Field(default="0", max_length=200, json_schema_extra=_ui("Compared with"))
    keep_checking_minutes: int = Field(
        default=0,
        ge=0,
        le=1440,
        json_schema_extra=_ui("Keep checking for (minutes)", hint="0 = check once"),
    )


class ReconcileConfig(_Config):
    source_connection_id: str = Field(
        default="", json_schema_extra=_ui("Source database", "database_connection")
    )
    source_sql: str = Field(
        default="", max_length=20000, json_schema_extra=_ui("Source SQL", "sql")
    )
    destination_connection_id: str = Field(
        default="", json_schema_extra=_ui("Destination database", "database_connection")
    )
    destination_sql: str = Field(
        default="", max_length=20000, json_schema_extra=_ui("Destination SQL", "sql")
    )
    comparison_mode: Literal["row_count", "rows"] = Field(
        default="row_count", json_schema_extra=_ui("Compare", "select")
    )
    key_columns: list[str] = Field(
        default_factory=list,
        json_schema_extra=_ui("Key columns (optional)"),
    )
    timeout_seconds: int = Field(
        default=60, ge=1, le=3600, json_schema_extra=_ui("Timeout (seconds)")
    )
    max_rows: int = Field(
        default=1000,
        ge=1,
        le=10000,
        json_schema_extra=_ui("Maximum rows to compare"),
    )

    @model_validator(mode="after")
    def _chosen(self) -> "ReconcileConfig":
        if not self.source_connection_id or not self.destination_connection_id:
            raise _incomplete("Choose both source and destination database connections")
        if not self.source_sql.strip() or not self.destination_sql.strip():
            raise _incomplete("Enter both source and destination SQL queries")
        if self.comparison_mode == "row_count" and self.key_columns:
            raise ValueError("Key columns are only used when comparing rows")
        return self


class WaitConfig(_Config):
    minutes: int = Field(default=5, ge=1, le=1440, json_schema_extra=_ui("Minutes"))


@dataclass(frozen=True)
class NodeType:
    type: str
    label: str
    category: Literal[
        "trigger",
        "logic",
        "pipeline",
        "database",
        "diagnosis",
        "approval",
        "action",
        "verify",
        "output",
    ]
    description: str
    ports: tuple[str, ...]
    config_model: type[_Config] = NoConfig
    port_labels: dict[str, str] = field(default_factory=dict)
    # Works on the incident that started the run (only valid under an incident trigger).
    needs_incident: bool = False
    # Works on the finished DAG run that started the workflow (only under trigger.dag_run).
    needs_dag_run: bool = False
    # Changes something outside the platform: policy-checked, simulated in dry runs.
    is_action: bool = False
    # Palette stage (When → Check → Diagnose → Decide → Act → Verify → Notify); defaults
    # from the category.
    stage: "Stage | None" = None
    # {{placeholders}} this block makes available to later blocks; "{id}" is the block's id.
    outputs: tuple[str, ...] = ()

    def catalog_entry(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "label": self.label,
            "category": self.category,
            "stage": self.stage or _CATEGORY_STAGES[self.category],
            "description": self.description,
            "summary": _SUMMARIES.get(self.type, self.description),
            "ports": list(self.ports),
            "port_labels": {p: self.port_labels.get(p, _PORT_LABELS.get(p, p)) for p in self.ports},
            "port_tones": {p: port_tone(p) for p in self.ports},
            "needs_incident": self.needs_incident,
            "needs_dag_run": self.needs_dag_run,
            "is_action": self.is_action,
            "outputs": list(self.outputs),
            "config_schema": self.config_model.model_json_schema(),
        }


Stage = Literal["when", "check", "diagnose", "decide", "act", "verify", "notify"]

_CATEGORY_STAGES: dict[str, Stage] = {
    "trigger": "when",
    "logic": "check",
    "pipeline": "act",
    "database": "act",
    "diagnosis": "diagnose",
    "approval": "decide",
    "action": "act",
    "verify": "verify",
    "output": "notify",
}

# Outcome colours on the canvas: "bad" ports are taken when something failed or the answer was no.
_GOOD_PORTS = frozenset(
    {
        "success",
        "pass",
        "matched",
        "approved",
        "true",
        "ready",
        "met",
        "analyzed",
        "retryable",
        "solution_ready",
    }
)
_BAD_PORTS = frozenset(
    {
        "failed",
        "fail",
        "rejected",
        "false",
        "timeout",
        "mismatched",
        "no_logs",
        "uncertain",
        "exceeded",
        "needs_fix",
        "escalate",
        "paused",
        "busy",
    }
)
# Port labels used unless a block names its own.
_PORT_LABELS = {
    "next": "then",
    "success": "succeeded",
    "failed": "failed",
    "pass": "passed",
    "fail": "failed",
    "approved": "approved",
    "rejected": "rejected",
    "timeout": "timed out",
}


def port_tone(port: str) -> Literal["good", "bad", "neutral"]:
    if port in _GOOD_PORTS:
        return "good"
    return "bad" if port in _BAD_PORTS else "neutral"


NODE_TYPES: dict[str, NodeType] = {
    t.type: t
    for t in (
        # ------------------------------------------------------------ triggers
        NodeType(
            "trigger.incident",
            "When an incident opens",
            "trigger",
            "Starts when detection opens an incident, or records it again (a recurrence).",
            ("next",),
            IncidentTriggerConfig,
            needs_incident=True,
        ),
        NodeType(
            "trigger.incident_stale",
            "When an incident is ignored",
            "trigger",
            "Starts when an incident stays open and unacknowledged for too long.",
            ("next",),
            StaleTriggerConfig,
            needs_incident=True,
        ),
        NodeType(
            "trigger.dag_run",
            "When a DAG run finishes",
            "trigger",
            "Starts when a monitored DAG run succeeds, fails, or is in progress, so the workflow "
            "can check or report its state.",
            ("next", "success", "in_progress", "failed"),
            DagRunTriggerConfig,
            {"success": "succeeded", "in_progress": "in progress", "failed": "failed"},
            needs_dag_run=True,
        ),
        NodeType(
            "trigger.manual",
            "When run manually",
            "trigger",
            "Starts when someone presses “Run now” on the workflow.",
            ("next",),
        ),
        NodeType(
            "trigger.schedule",
            "On a schedule",
            "trigger",
            "Starts every N minutes while the workflow is turned on.",
            ("next",),
            ScheduleTriggerConfig,
        ),
        # ------------------------------------------------------------ logic
        NodeType(
            "condition.filter",
            "Filter incidents",
            "logic",
            "Continues on 'matches' when the incident meets every criterion you set, otherwise on "
            "'doesn't match'.",
            ("true", "false"),
            FilterConfig,
            {"true": "matches", "false": "doesn't match"},
            needs_incident=True,
        ),
        NodeType(
            "check.dag_state",
            "Check DAG state",
            "logic",
            "Asks Airflow whether the incident's DAG is paused or already has an active run.",
            ("ready", "paused", "busy"),
            port_labels={"ready": "ready (idle)", "paused": "paused", "busy": "already running"},
            needs_incident=True,
        ),
        NodeType(
            "check.sla",
            "Check SLA",
            "logic",
            "Estimates how long the DAG should take from its past runs in Airflow and branches on "
            "whether this run is over that limit.",
            ("exceeded", "met"),
            SlaCheckConfig,
            port_labels={"exceeded": "over SLA", "met": "within SLA"},
            outputs=(
                "sla.summary",
                "sla.dag_id",
                "sla.actual_minutes",
                "sla.threshold_minutes",
                "sla.estimated_baseline_minutes",
            ),
        ),
        NodeType(
            "flow.wait",
            "Wait",
            "logic",
            "Pauses the workflow for a number of minutes, then continues.",
            ("next",),
            WaitConfig,
            stage="decide",
        ),
        # ------------------------------------------------------------ pipelines
        NodeType(
            "pipeline.run_dag",
            "Run a DAG",
            "pipeline",
            "Starts a run of the chosen DAG, optionally with parameters, and can report when it "
            "is in progress or has finished.",
            ("success", "in_progress", "failed"),
            RunDagConfig,
            {"success": "succeeded", "in_progress": "in progress", "failed": "failed"},
            is_action=True,
            outputs=("results.{id}.dag_id", "results.{id}.run_id", "results.{id}.state"),
        ),
        NodeType(
            "pipeline.wait_for_dag",
            "Wait for a DAG run",
            "pipeline",
            "Waits until the chosen DAG has a recent successful run (e.g. an upstream pipeline).",
            ("success", "timeout"),
            WaitForDagConfig,
            stage="decide",
            outputs=("results.{id}.dag_id", "results.{id}.run_id", "results.{id}.finished_at"),
        ),
        # ------------------------------------------------------------ databases
        NodeType(
            "database.run_sql",
            "Run SQL",
            "database",
            "Runs SQL on the chosen database (e.g. refresh a table). Write statements are "
            "policy-checked like any other action.",
            ("success", "failed"),
            RunSqlConfig,
            is_action=True,
            outputs=("results.{id}.value", "results.{id}.row_count", "results.{id}.rows_affected"),
        ),
        NodeType(
            "database.check",
            "Check data (SQL)",
            "database",
            "Runs a read-only query that returns one value and compares it, e.g. row count > 0. "
            "Can keep checking until it passes.",
            ("pass", "fail"),
            CheckDataConfig,
            stage="check",
            outputs=("results.{id}.value", "results.{id}.passed", "results.{id}.rule"),
        ),
        NodeType(
            "database.reconcile",
            "Compare two databases",
            "database",
            "Runs read-only queries against two databases and compares their row counts or rows.",
            ("matched", "mismatched", "failed"),
            ReconcileConfig,
            {
                "matched": "results match",
                "mismatched": "results differ",
                "failed": "query failed",
            },
            stage="check",
            outputs=(
                "results.{id}.source_row_count",
                "results.{id}.destination_row_count",
                "results.{id}.difference_count",
            ),
        ),
        # ------------------------------------------------------------ incident handling
        NodeType(
            "analyze.task_logs",
            "Fetch task logs",
            "diagnosis",
            "Fetches the failed task's logs and pulls out the error headline and stack trace for "
            "later blocks. It does not classify the failure; use 'Classify the failure' for that.",
            ("analyzed", "no_logs"),
            AnalyzeLogsConfig,
            {"analyzed": "logs found", "no_logs": "no logs"},
            outputs=(
                "analysis.headline",
                "analysis.stack_trace",
                "analysis.log_snippet",
                "analysis.task_id",
            ),
            needs_incident=True,
        ),
        NodeType(
            "diagnose.classify_log",
            "Classify the failure",
            "diagnosis",
            "Labels the cause of the failure (network glitch, bad data, …) from the logs, using "
            "the rules, the AI model and operator corrections, and branches on whether a retry "
            "may help. 'Always' runs for every outcome whose own output is not connected.",
            ("retryable", "needs_fix", "unknown", "next"),
            DiagnoseConfig,
            {
                "retryable": "retry may help",
                "needs_fix": "needs a fix",
                "unknown": "unknown",
                "next": "always",
            },
            needs_incident=True,
            outputs=(
                "diagnosis.label",
                "diagnosis.category",
                "diagnosis.confidence",
                "diagnosis.matched_line",
            ),
        ),
        NodeType(
            "ai.generate_fix",
            "Suggest a fix (AI)",
            "diagnosis",
            "Sends the error and incident context to the local AI model, which explains the root "
            "cause and proposes a fix plan.",
            ("solution_ready", "uncertain"),
            AiGenerateFixConfig,
            {"solution_ready": "fix suggested", "uncertain": "not sure"},
            needs_incident=True,
            outputs=(
                "ai_solution.explanation",
                "ai_solution.recommended_action",
                "ai_solution.confidence",
            ),
        ),
        NodeType(
            "decide.choose_fix",
            "Choose a fix automatically",
            "logic",
            "Picks how to handle the failure from the diagnosis, earlier fix attempts, the daily "
            "action limit and the DAG's state, and records why. A local AI can suggest or make "
            "the pick among the fixes the rules allow. If the chosen output is not connected, "
            "'hand to a person' is followed.",
            ("retry", "rerun", "wait", "pause", "escalate", "ignore"),
            ChooseFixConfig,
            {
                "retry": "retry failed tasks",
                "rerun": "start a new run",
                "wait": "wait for input",
                "pause": "pause the DAG",
                "escalate": "hand to a person",
                "ignore": "leave it alone",
            },
            needs_incident=True,
            stage="decide",
            outputs=("decision.label", "decision.reason", "decision.fix"),
        ),
        NodeType(
            "approval.request",
            "Ask for approval",
            "approval",
            "Waits for an operator to approve. Auto-approves outside the listed environments.",
            ("approved", "rejected"),
            ApprovalConfig,
        ),
        NodeType(
            "action.clear_failed_tasks",
            "Retry failed tasks",
            "action",
            "Clears the failed tasks of the incident's latest failing run so Airflow reruns them.",
            ("success", "failed"),
            ClearTasksConfig,
            needs_incident=True,
            is_action=True,
        ),
        NodeType(
            "action.trigger_dag_run",
            "Re-run the incident's DAG",
            "action",
            "Triggers a new run of the incident's DAG. To run any other DAG, use 'Run a DAG'.",
            ("success", "failed"),
            needs_incident=True,
            is_action=True,
        ),
        NodeType(
            "action.set_dag_paused",
            "Pause or resume a DAG",
            "action",
            "Pauses (or resumes) the incident's DAG in Airflow.",
            ("success", "failed"),
            SetPausedConfig,
            needs_incident=True,
            is_action=True,
        ),
        NodeType(
            "verify.run_success",
            "Verify the run succeeded",
            "verify",
            "Waits for the run touched by the last action to finish and checks it succeeded.",
            ("success", "failed"),
            VerifyConfig,
        ),
        NodeType(
            "incident.update",
            "Update the incident",
            "output",
            "Resolves, escalates, acknowledges, or adds a note to the incident.",
            ("next",),
            IncidentUpdateConfig,
            needs_incident=True,
        ),
        NodeType(
            "check.record",
            "Record the run result",
            "output",
            "Marks the DAG run that started the workflow as passed or failed. A failed run can "
            "open a 'Data check failed' incident; a passed one resolves it.",
            ("next",),
            RecordCheckConfig,
            needs_dag_run=True,
        ),
        NodeType(
            "notify",
            "Send a notification",
            "output",
            "Sends an in-app notification, or a message through a channel (email, Slack, Teams). "
            "Use 'Insert variable' to add values like {{incident.title}} or "
            "{{results.<block id>.value}}.",
            ("next",),
            NotifyConfig,
        ),
    )
}

# One-line descriptions for the editor. The full `description` stays for the palette tooltip and
# the workflow assistant's prompt, which needs the details (e.g. what "always" means).
_SUMMARIES: dict[str, str] = {
    "trigger.incident": "Starts when an incident opens or happens again.",
    "trigger.incident_stale": "Starts when an incident stays unacknowledged too long.",
    "trigger.dag_run": "Starts when a monitored DAG run finishes.",
    "trigger.manual": "Starts when someone presses Run now.",
    "trigger.schedule": "Starts every N minutes while turned on.",
    "condition.filter": "Continues only for incidents that match.",
    "check.dag_state": "Is the incident's DAG paused or already running?",
    "check.sla": "Is this run slower than its past runs allow?",
    "flow.wait": "Waits, then continues.",
    "pipeline.run_dag": "Starts a run of the chosen DAG.",
    "pipeline.wait_for_dag": "Waits for the chosen DAG to succeed.",
    "database.run_sql": "Runs SQL on the chosen database.",
    "database.check": "Runs a query and compares its value.",
    "database.reconcile": "Compares query results from two databases.",
    "analyze.task_logs": "Pulls the error out of the failed task's logs.",
    "diagnose.classify_log": "Labels the cause and whether a retry may help.",
    "ai.generate_fix": "The local AI explains the cause and suggests a fix.",
    "decide.choose_fix": "Picks the fix from the diagnosis and past attempts.",
    "approval.request": "Waits for someone to approve.",
    "action.clear_failed_tasks": "Re-runs the failed tasks.",
    "action.trigger_dag_run": "Starts a new run of the incident's DAG.",
    "action.set_dag_paused": "Pauses or resumes the incident's DAG.",
    "verify.run_success": "Waits for the last action's run and checks it.",
    "incident.update": "Resolves, escalates, acknowledges or notes the incident.",
    "check.record": "Marks the finished run as passed or failed.",
    "notify": "Sends a message in the app or through a channel.",
}

# Every key a block records under {{results.<block id>.<key>}} (see `record_result` callers).
# `outputs` is only what the editor offers; a block not listed here is not checked.
_RESULT_KEYS: dict[str, frozenset[str]] = {
    "check.sla": frozenset(
        {
            "dag_id",
            "run_id",
            "metric",
            "actual_minutes",
            "estimated_baseline_minutes",
            "threshold_minutes",
            "margin_pct",
            "sample_count",
            "calculation_method",
            "exceeded",
            "is_in_flight",
            "summary",
        }
    ),
    "pipeline.run_dag": frozenset({"dag_id", "run_id", "state"}),
    "pipeline.wait_for_dag": frozenset({"dag_id", "succeeded", "run_id", "finished_at"}),
    "database.run_sql": frozenset(
        {"columns", "rows", "row_count", "truncated", "value", "rows_affected"}
    ),
    "database.check": frozenset({"value", "passed", "rule", "error"}),
    "database.reconcile": frozenset(
        {
            "source_row_count",
            "destination_row_count",
            "source_truncated",
            "destination_truncated",
            "comparison_mode",
            "matched",
            "difference_count",
            "differences",
            "phase",
            "error",
        }
    ),
    "analyze.task_logs": frozenset(
        {"headline", "stack_trace", "log_snippet", "task_id", "dag_id", "run_id", "incident_id"}
    ),
    "ai.generate_fix": frozenset(
        {
            "headline",
            "explanation",
            "recommended_action",
            "confidence",
            "model",
            "reasoning",
            "instruction",
        }
    ),
    "decide.choose_fix": frozenset(
        {"fix", "chosen", "label", "rule", "rules_fix", "reason", "allowed", "ai", "facts"}
    ),
}

TRIGGER_TYPES = frozenset(t for t, nt in NODE_TYPES.items() if nt.category == "trigger")
INCIDENT_TRIGGER_TYPES = frozenset(t for t in TRIGGER_TYPES if NODE_TYPES[t].needs_incident)
ACTION_TYPES = frozenset(t for t, nt in NODE_TYPES.items() if nt.is_action)


class GraphError(ValueError):
    def __init__(self, problems: list[dict[str, str]], graph: "Graph | None" = None) -> None:
        super().__init__("; ".join(p["message"] for p in problems))
        self.problems = problems
        # Set when only user choices are missing: the structure is sound, the settings are not.
        self.graph = graph


@dataclass(frozen=True)
class Node:
    id: str
    type: str
    name: str | None
    config: dict[str, Any]
    # Canvas layout; the engine only uses it to order blocks linked from the same output.
    position: dict[str, float] | None = None


@dataclass(frozen=True)
class Graph:
    nodes: dict[str, Node]
    edges: dict[tuple[str, str], list[str]]  # (from, port) -> [to, ...] in link order
    trigger_id: str

    @property
    def trigger(self) -> Node:
        return self.nodes[self.trigger_id]

    def next_nodes(self, node_id: str, port: str) -> list[str]:
        """Blocks linked from an output, in run order: top to bottom (then left to right) on
        the canvas; blocks without a position keep link order, after the positioned ones."""
        targets = self.edges.get((node_id, port), [])

        def key(indexed: tuple[int, str]) -> tuple[int, float, float, int]:
            index, target = indexed
            pos = self.nodes[target].position
            return (0, pos["y"], pos["x"], index) if pos else (1, 0.0, 0.0, index)

        return [t for _, t in sorted(enumerate(targets), key=key)]


def _problem(message: str, **where: str) -> dict[str, str]:
    return {"message": message, **where}


def validate_graph(raw: Any) -> Graph:
    """Validate a raw graph and return it with defaults applied. Raises GraphError."""
    if not isinstance(raw, dict):
        raise GraphError([_problem("Graph must be an object with 'nodes' and 'edges'")])
    raw_nodes, raw_edges = raw.get("nodes"), raw.get("edges", [])
    if not isinstance(raw_nodes, list) or not raw_nodes:
        raise GraphError([_problem("Graph needs at least one node")])
    if not isinstance(raw_edges, list):
        raise GraphError([_problem("'edges' must be a list")])
    if len(raw_nodes) > MAX_NODES:
        raise GraphError([_problem(f"At most {MAX_NODES} nodes are allowed")])

    problems: list[dict[str, str]] = []
    nodes: dict[str, Node] = {}
    for index, item in enumerate(raw_nodes):
        if not isinstance(item, dict):
            problems.append(_problem(f"Node #{index + 1} must be an object"))
            continue
        node_id = str(item.get("id", ""))
        if not re.fullmatch(NODE_ID_PATTERN, node_id):
            problems.append(_problem(f"Node #{index + 1} has an invalid id", node=node_id))
            continue
        if node_id in nodes:
            problems.append(_problem(f"Duplicate node id '{node_id}'", node=node_id))
            continue
        node_type = NODE_TYPES.get(str(item.get("type")))
        if node_type is None:
            problems.append(_problem(f"Unknown node type '{item.get('type')}'", node=node_id))
            continue
        config: dict[str, Any]
        try:
            config = node_type.config_model.model_validate(item.get("config") or {}).model_dump(
                mode="json"
            )
        except ValidationError as exc:
            errors = exc.errors()
            problems.extend(_config_problem(node_type, node_id, err) for err in errors)
            if any(err["type"] != INCOMPLETE for err in errors):
                continue
            # The block is only waiting for a choice, so its links can still be checked.
            config = dict(item.get("config") or {})
        position, problem = _position(item.get("position"))
        if problem:
            problems.append(_problem(problem, node=node_id))
            continue
        name = item.get("name")
        nodes[node_id] = Node(
            id=node_id,
            type=node_type.type,
            name=str(name)[:200] if name else None,
            config=config,
            position=position,
        )

    raw_ids = {str(n.get("id")) for n in raw_nodes if isinstance(n, dict)}
    edges: dict[tuple[str, str], list[str]] = {}
    for index, item in enumerate(raw_edges):
        label = f"Edge #{index + 1}"
        if not isinstance(item, dict):
            problems.append(_problem(f"{label} must be an object"))
            continue
        src, port, dst = str(item.get("from", "")), str(item.get("port", "")), str(item.get("to"))
        edge = f"{src}.{port}->{dst}"
        if src not in nodes or dst not in nodes:
            if src not in raw_ids or dst not in raw_ids:  # else the node itself was reported
                problems.append(_problem(f"{label} references an unknown node", edge=edge))
            continue
        if port not in NODE_TYPES[nodes[src].type].ports:
            problems.append(
                _problem(f"{label}: node '{src}' has no output port '{port}'", edge=edge)
            )
            continue
        if nodes[dst].type in TRIGGER_TYPES:
            problems.append(_problem(f"{label} cannot point at a trigger", edge=edge))
            continue
        if dst in edges.get((src, port), []):
            problems.append(
                _problem(f"{label}: port '{port}' of '{src}' is already connected to '{dst}'")
            )
            continue
        edges.setdefault((src, port), []).append(dst)

    triggers = [n.id for n in nodes.values() if n.type in TRIGGER_TYPES]
    # Every block and link is valid, so reachability is meaningful.
    blocks_ok = all(p.get("kind") == INCOMPLETE for p in problems)
    if len(triggers) != 1:
        problems.append(_problem(f"A workflow needs exactly one trigger (found {len(triggers)})"))
        raise GraphError(problems)

    # Report every remaining problem at once, so the editor can show them all.
    graph = Graph(nodes=nodes, edges=edges, trigger_id=triggers[0])
    problems.extend(_structure_problems(graph, check_reachable=blocks_ok))
    if graph.trigger.type not in INCIDENT_TRIGGER_TYPES:
        problems.extend(
            _problem(
                f"'{NODE_TYPES[n.type].label}' works on an incident, so it needs an incident "
                "trigger",
                node=n.id,
            )
            for n in graph.nodes.values()
            if NODE_TYPES[n.type].needs_incident
        )
    if graph.trigger.type != "trigger.dag_run":
        problems.extend(
            _problem(
                f"'{NODE_TYPES[n.type].label}' works on a finished DAG run, so it needs the "
                "'When a DAG run finishes' trigger",
                node=n.id,
            )
            for n in graph.nodes.values()
            if NODE_TYPES[n.type].needs_dag_run
        )
    if problems:
        only_choices = all(p.get("kind") == INCOMPLETE for p in problems)
        raise GraphError(problems, graph if only_choices else None)
    return graph


def _config_problem(node_type: NodeType, node_id: str, err: Any) -> dict[str, str]:
    where = {"node": node_id}
    if err["loc"]:
        where["field"] = ".".join(str(p) for p in err["loc"])
    if err["type"] == INCOMPLETE:
        where["kind"] = INCOMPLETE
    return _problem(_config_message(node_type, err), **where)


def _config_message(node_type: NodeType, err: Any) -> str:
    """A settings error in the form's words: "Expires after (minutes): must be ≥ 1"."""
    message = str(err["msg"]).removeprefix("Value error, ")
    loc = [str(p) for p in err["loc"]]
    if not loc:
        return message
    prop = node_type.config_model.model_json_schema().get("properties", {}).get(loc[0], {})
    return f"{prop.get('x-label', loc[0])}: {message}"


def _position(raw: Any) -> tuple[dict[str, float] | None, str | None]:
    if raw is None:
        return None, None
    if not isinstance(raw, dict):
        return None, "position must be an object with x and y"
    try:
        x, y = float(raw["x"]), float(raw["y"])
    except (KeyError, TypeError, ValueError):
        return None, "position needs numeric x and y"
    if not (math.isfinite(x) and math.isfinite(y)) or max(abs(x), abs(y)) > MAX_COORDINATE:
        return None, "position is out of range"
    return {"x": round(x, 1), "y": round(y, 1)}, None


def _structure_problems(graph: Graph, *, check_reachable: bool) -> list[dict[str, str]]:
    """A loop (the first one found) and blocks the trigger cannot reach."""
    adjacency: dict[str, list[str]] = {n: [] for n in graph.nodes}
    for (src, _), targets in graph.edges.items():
        adjacency[src].extend(targets)

    problems: list[dict[str, str]] = []
    visiting: set[str] = set()
    done: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in done or problems:
            return
        if node_id in visiting:
            problems.append(_problem("The workflow contains a loop", node=node_id))
            return
        visiting.add(node_id)
        for nxt in adjacency[node_id]:
            visit(nxt)
        visiting.discard(node_id)
        done.add(node_id)

    for node_id in [graph.trigger_id, *graph.nodes]:
        visit(node_id)  # also catches cycles among unreachable nodes

    if check_reachable:
        problems.extend(
            _problem(
                "This block is not connected to the trigger: link it from an earlier block", node=n
            )
            for n in sorted(set(graph.nodes) - _downstream(graph, graph.trigger_id))
        )
    return problems


def _downstream(graph: Graph, start: str) -> set[str]:
    """`start` and every block reachable from it."""
    seen: set[str] = set()
    stack = [start]
    while stack:
        current = stack.pop()
        if current not in seen:
            seen.add(current)
            stack.extend(t for (src, _), ts in graph.edges.items() if src == current for t in ts)
    return seen


# Values every run offers to {{placeholders}} (see NodeContext.render_values).
_GLOBAL_VALUES = frozenset({"workflow", "environment", "last_error", "links", "results"})
_INCIDENT_VALUES = frozenset({"incident"})
_PLACEHOLDER = re.compile(r"\{\{\s*([A-Za-z0-9_.]+)\s*\}\}")


def graph_warnings(graph: Graph) -> list[dict[str, str]]:
    """{{placeholders}} that will render empty: no earlier block or the trigger provides them.

    Warnings do not block saving; the editor shows them next to the block.
    """
    warnings: list[dict[str, str]] = []
    for node in graph.nodes.values():
        schema = NODE_TYPES[node.type].config_model.model_json_schema().get("properties", {})
        before = {n for n in graph.nodes if node.id in _downstream(graph, n) and n != node.id}
        offered = set(_GLOBAL_VALUES)
        if NODE_TYPES[graph.trigger.type].needs_incident:
            offered |= _INCIDENT_VALUES
        if graph.trigger.type == "trigger.dag_run":
            offered.add("dag_run")
        for upstream in before:
            offered |= {o.split(".")[0] for o in NODE_TYPES[graph.nodes[upstream].type].outputs}
        for field_name, prop in schema.items():
            text = node.config.get(field_name)
            if prop.get("x-widget") != "template" or not isinstance(text, str):
                continue
            for key in _PLACEHOLDER.findall(text):
                parts = key.split(".")
                label = prop.get("x-label", field_name)
                if parts[0] not in offered:
                    warnings.append(
                        _problem(
                            f"{label}: {{{{{key}}}}} will be empty here; no earlier block "
                            "provides it",
                            node=node.id,
                        )
                    )
                elif parts[0] == "results" and (len(parts) < 2 or parts[1] not in before):
                    block = parts[1] if len(parts) > 1 else "?"
                    warnings.append(
                        _problem(
                            f"{label}: {{{{{key}}}}} will be empty; block '{block}' does not "
                            "run before this one",
                            node=node.id,
                        )
                    )
                elif parts[0] == "results" and len(parts) > 2:
                    known = _RESULT_KEYS.get(graph.nodes[parts[1]].type)
                    if known is not None and parts[2] not in known:
                        warnings.append(
                            _problem(
                                f"{label}: {{{{{key}}}}} will be empty; block '{parts[1]}' does "
                                f"not record '{parts[2]}'",
                                node=node.id,
                            )
                        )
    return warnings


@dataclass(frozen=True)
class DagWatch:
    """What detection does for a DAG it polls."""

    detect_failures: bool
    has_sla: bool


def monitoring_warnings(graph: Graph, watched: dict[str, DagWatch]) -> list[dict[str, str]]:
    """DAGs named on the trigger that detection will never report, so it would never start.

    `watched` holds the DAGs detection polls. An unscoped trigger names none, so it is fine.
    """
    trigger = graph.trigger
    warnings: list[dict[str, str]] = []
    if trigger.type not in _OVERLAP_SETTINGS:
        return warnings
    types = trigger.config.get("incident_types") or []

    def warn(message: str) -> None:
        warnings.append(_problem(message, node=trigger.id))

    for dag_id in trigger.config["dag_ids"]:
        watch = watched.get(dag_id)
        if watch is None:
            warn(f"'{dag_id}' is not monitored, so this workflow will not start for it")
        elif trigger.type != "trigger.dag_run":
            if not watch.detect_failures and (not types or "DAG_RUN_FAILED" in types):
                warn(f"Failure detection is off for '{dag_id}', so its failures are not reported")
            # Most DAGs have no SLA, so this only matters when SLA misses were asked for.
            if not watch.has_sla and "SLA_MISSED" in types:
                warn(f"'{dag_id}' has no SLA, so it can never miss one")
    return warnings


# The settings two triggers must share to fire on one event; empty = any. `min_severity` is left
# out: any two limits can meet, on a severe enough incident.
_OVERLAP_SETTINGS: dict[str, tuple[str, ...]] = {
    "trigger.incident": ("events", "incident_types", "dag_ids", "environments"),
    "trigger.incident_stale": ("dag_ids", "environments"),
    "trigger.dag_run": ("states", "dag_ids"),
}


def overlap_warnings(graph: Graph, others: list[tuple[str, Node]]) -> list[dict[str, str]]:
    """Other workflows, given as (name, trigger), that would start on the same event."""
    trigger = graph.trigger
    settings = _OVERLAP_SETTINGS.get(trigger.type, ())

    def meets(other: Node, name: str) -> bool:
        ours, theirs = trigger.config[name], other.config[name]
        return not ours or not theirs or bool(set(ours) & set(theirs))

    return [
        _problem(
            f"'{name}' can start on the same event, so both workflows would run",
            node=trigger.id,
        )
        for name, other in others
        if settings and other.type == trigger.type and all(meets(other, s) for s in settings)
    ]


def to_raw(graph: Graph) -> dict[str, Any]:
    """Serialize a validated graph (defaults applied) back to JSON form."""
    return {
        "nodes": [
            {
                "id": n.id,
                "type": n.type,
                **({"name": n.name} if n.name else {}),
                "config": n.config,
                **({"position": n.position} if n.position else {}),
            }
            for n in graph.nodes.values()
        ],
        "edges": [
            {"from": s, "port": p, "to": d}
            for (s, p), targets in graph.edges.items()
            for d in targets
        ],
    }


def catalog() -> list[dict[str, Any]]:
    return [t.catalog_entry() for t in NODE_TYPES.values()]
