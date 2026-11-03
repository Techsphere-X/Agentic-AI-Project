"""Workflow graph: node catalog (types, ports, config models) and validation.

A workflow graph is `{"nodes": [{id, type, name?, config}], "edges": [{from, port, to}]}`.
Execution starts at the single trigger node and follows the edge for each node's output port;
a port with no edge ends the run. This is also the contract for the Plan 3 canvas.

Pure: Pydantic only, no database or framework imports.
"""

import json
import math
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from app.automation import cron
from app.detection.types import IncidentSeverity, IncidentType
from app.diagnosis.log_classifier import CATEGORY_LABELS, FailureCategory

MAX_NODES = 100
MAX_COORDINATE = 100_000
NODE_ID_PATTERN = r"^[A-Za-z0-9_-]{1,100}$"


class _Config(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NoConfig(_Config):
    pass


class IncidentTriggerConfig(_Config):
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


class StaleTriggerConfig(_Config):
    minutes: int = Field(
        default=30,
        ge=5,
        le=10080,
        json_schema_extra={
            "x-label": "Open and unacknowledged for (minutes)",
        },
    )


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
    mode: Literal["every", "cron"] = Field(
        default="every",
        json_schema_extra=_ui(
            "Repeat",
            options={"every": "Every N minutes", "cron": "At set times (cron)"},
        ),
    )
    every_minutes: int = Field(
        default=60,
        ge=5,
        le=10080,
        json_schema_extra={**_ui("Every (minutes)"), "x-show-when": {"mode": "every"}},
    )
    cron: str = Field(
        default="0 6 * * *",
        max_length=100,
        json_schema_extra={
            **_ui(
                "Cron schedule",
                "cron",
                "minute hour day month weekday, e.g. 0 6 * * 1-5 = 06:00 on weekdays",
            ),
            "x-show-when": {"mode": "cron"},
        },
    )
    timezone: str = Field(
        default="UTC",
        max_length=64,
        json_schema_extra={**_ui("Time zone", "timezone"), "x-show-when": {"mode": "cron"}},
    )

    @model_validator(mode="after")
    def _cron(self) -> "ScheduleTriggerConfig":
        if self.mode == "cron":
            try:
                cron.parse(self.cron)
                cron.zone(self.timezone)
            except cron.CronError as exc:
                raise ValueError(str(exc)) from exc
        return self


class _AirflowTarget(_Config):
    connection_id: str = Field(
        default="", json_schema_extra=_ui("Airflow connection", "airflow_connection")
    )
    dag_id: str = Field(default="", max_length=250, json_schema_extra=_ui("DAG", "dag"))

    @model_validator(mode="after")
    def _chosen(self) -> "_AirflowTarget":
        if not self.connection_id:
            raise ValueError("Choose an Airflow connection")
        if not self.dag_id:
            raise ValueError("Choose a DAG")
        return self


DAG_EVENTS = {"in_progress": "In progress", "success": "Succeeded", "failed": "Failed"}


class DagTriggerConfig(_AirflowTarget):
    """'When this DAG runs': one DAG; each chosen event starts its own workflow run."""

    states: list[Literal["in_progress", "success", "failed"]] = Field(
        default_factory=lambda: ["in_progress", "success", "failed"],
        min_length=1,
        json_schema_extra=_ui(
            "Start on",
            hint="Each event starts its own run, through the matching output",
            options=DAG_EVENTS,
        ),
    )


class OpenIncidentConfig(_Config):
    incident_type: IncidentType = Field(
        default=IncidentType.DAG_RUN_FAILED,
        json_schema_extra=_ui("Incident type", options=_INCIDENT_TYPES),
    )
    severity: IncidentSeverity = Field(
        default=IncidentSeverity.HIGH,
        json_schema_extra=_ui("Severity", options=_SEVERITIES),
    )
    title: str = Field(
        default="{{dag_run.dag_id}} {{dag_run.state}}",
        min_length=1,
        max_length=300,
        json_schema_extra=_ui("Title", "template"),
    )
    note: str = Field(default="", max_length=2000, json_schema_extra=_ui("Note", "template"))


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
            raise ValueError("Choose a database connection")
        if not self.sql.strip():
            raise ValueError("Enter the SQL to run")
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
            raise ValueError("Choose both source and destination database connections")
        if not self.source_sql.strip() or not self.destination_sql.strip():
            raise ValueError("Enter both source and destination SQL queries")
        if self.comparison_mode == "row_count" and self.key_columns:
            raise ValueError("Key columns are only used when comparing rows")
        return self


# ---------------------------------------------------------------------- logic configs

COMPARE_OPERATORS = {
    "=": "equals",
    "!=": "does not equal",
    ">": "is greater than",
    ">=": "is at least",
    "<": "is less than",
    "<=": "is at most",
    "contains": "contains",
    "is_empty": "is empty",
    "not_empty": "is not empty",
}
VALUE_NAME_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]{0,63}$"
MAX_SWITCH_CASES = 20


class IfConfig(_Config):
    left: str = Field(
        default="",
        max_length=2000,
        json_schema_extra=_ui("Value", "template", "e.g. {{results.count_rows.value}}"),
    )
    operator: Literal["=", "!=", ">", ">=", "<", "<=", "contains", "is_empty", "not_empty"] = Field(
        default="=", json_schema_extra=_ui("Is", options=COMPARE_OPERATORS)
    )
    right: str = Field(
        default="",
        max_length=2000,
        json_schema_extra=_ui("Compared with", "template", "Not used for 'is empty'"),
    )


class SwitchCase(_Config):
    key: str = Field(default="", max_length=100)  # output port id; filled from the label
    label: str = Field(default="", max_length=100)
    equals: str = Field(max_length=500)


class SwitchConfig(_Config):
    value: str = Field(
        default="",
        max_length=2000,
        json_schema_extra=_ui("Value", "template", "e.g. {{dag_run.state}}"),
    )
    cases: list[SwitchCase] = Field(
        default_factory=list,
        max_length=MAX_SWITCH_CASES,
        json_schema_extra=_ui(
            "Cases",
            "cases",
            "Each case gets its own output; anything else goes to 'otherwise'",
        ),
    )

    @model_validator(mode="after")
    def _keys(self) -> "SwitchConfig":
        taken: set[str] = {"otherwise"}
        for case in self.cases:
            if not case.key:
                base = re.sub(r"[^A-Za-z0-9_-]", "_", case.label or case.equals)[:90] or "case"
                key, n = base, 2
                while key in taken:
                    key, n = f"{base}_{n}", n + 1
                case.key = key
            if not re.fullmatch(NODE_ID_PATTERN, case.key):
                raise ValueError(f"Invalid output id '{case.key}'")
            if case.key in taken:
                raise ValueError(f"Two cases use the output '{case.key}'")
            taken.add(case.key)
        return self


def _switch_ports(config: dict[str, Any]) -> list[tuple[str, str]]:
    cases = [(c["key"], c.get("label") or c["equals"]) for c in config.get("cases") or []]
    return [*cases, ("otherwise", "otherwise")]


class SetValueConfig(_Config):
    name: str = Field(
        default="value",
        max_length=64,
        json_schema_extra=_ui("Name", hint="Later blocks use {{vars.<name>}}"),
    )
    value: str = Field(default="", max_length=4000, json_schema_extra=_ui("Value", "template"))

    @field_validator("name")
    @classmethod
    def _name(cls, value: str) -> str:
        if not re.fullmatch(VALUE_NAME_PATTERN, value):
            raise ValueError("Use letters, digits and _ (not starting with a digit)")
        return value


class NodeSettings(BaseModel):
    """How the engine runs a block (not part of the block's own settings)."""

    model_config = ConfigDict(extra="forbid")
    retries: int = Field(default=0, ge=0, le=5)
    retry_delay_seconds: int = Field(default=60, ge=5, le=3600)
    # "fail": the block's failure fails the run. "output": it continues on the 'error' output.
    on_error: Literal["fail", "output"] = "fail"


# ---------------------------------------------------------------------- integration configs

HTTP_METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE")
MAX_FOR_EACH_ROWS = 100


class HttpRequestConfig(_Config):
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = Field(
        default="POST", json_schema_extra=_ui("Method", options={m: m for m in HTTP_METHODS})
    )
    url: str = Field(
        default="",
        max_length=2000,
        json_schema_extra=_ui("URL", "template", "http(s)://…; may use {{placeholders}}"),
    )
    headers: str = Field(
        default="",
        max_length=4000,
        json_schema_extra=_ui("Headers", "template", "One per line: Name: value"),
    )
    body: str = Field(
        default="",
        max_length=20000,
        json_schema_extra=_ui("Body", "template", "Sent as is (e.g. JSON); not sent with GET"),
    )
    environment: Literal["DEV", "STAGING", "PROD"] = Field(
        default="PROD",
        json_schema_extra=_ui(
            "Target environment",
            hint="Changes in PROD need an approval first; GET only reads and needs none",
            options=_ENVIRONMENTS,
        ),
    )
    timeout_seconds: int = Field(
        default=30, ge=1, le=120, json_schema_extra=_ui("Timeout (seconds)")
    )

    @field_validator("url")
    @classmethod
    def _url(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Enter the URL to call")
        if "{{" not in value and not value.startswith(("http://", "https://")):
            raise ValueError("The URL must start with http:// or https://")
        return value


class _CallWorkflow(_Config):
    workflow_id: str = Field(
        default="",
        json_schema_extra=_ui(
            "Workflow to run", "workflow", "One that starts by hand or by webhook"
        ),
    )

    @model_validator(mode="after")
    def _chosen(self) -> "_CallWorkflow":
        if not self.workflow_id:
            raise ValueError("Choose the workflow to run")
        return self


class CallWorkflowConfig(_CallWorkflow):
    input: str = Field(
        default="{}",
        max_length=20000,
        json_schema_extra=_ui(
            "Input (JSON)",
            "template",
            "The called workflow reads it as {{trigger.body.<field>}}",
        ),
    )
    wait: bool = Field(default=True, json_schema_extra=_ui("Wait until it finishes"))
    timeout_minutes: int = Field(
        default=60, ge=1, le=1440, json_schema_extra=_ui("Give up after (minutes)")
    )


class ForEachConfig(_CallWorkflow):
    connection_id: str = Field(
        default="", json_schema_extra=_ui("Database connection", "database_connection")
    )
    sql: str = Field(
        default="",
        max_length=20000,
        json_schema_extra=_ui("Rows (read-only SQL)", "sql", "One run of the workflow per row"),
    )
    max_rows: int = Field(
        default=20,
        ge=1,
        le=MAX_FOR_EACH_ROWS,
        json_schema_extra=_ui("At most this many rows"),
    )

    @model_validator(mode="after")
    def _query(self) -> "ForEachConfig":
        if not self.connection_id:
            raise ValueError("Choose a database connection")
        if not self.sql.strip():
            raise ValueError("Enter the SQL that returns the rows")
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
        "integration",
        "verify",
        "output",
    ]
    description: str
    ports: tuple[str, ...]
    config_model: type[_Config] = NoConfig
    port_labels: dict[str, str] = field(default_factory=dict)
    # Works on the incident (an incident trigger, or after an 'Open incident' block).
    needs_incident: bool = False
    # Works on "the DAG run": the incident's run, or the run that started 'When this DAG runs'.
    needs_target: bool = False
    # Works on the DAG run that started the workflow (only under "When this DAG runs").
    needs_dag_run: bool = False
    # Changes something outside the platform: policy-checked, simulated in dry runs.
    is_action: bool = False
    # Palette stage (When → Check → Diagnose → Decide → Act → Verify → Notify); defaults
    # from the category.
    stage: "Stage | None" = None
    # {{placeholders}} this block makes available to later blocks; "{id}" is the block's id.
    outputs: tuple[str, ...] = ()
    # Ports that depend on the block's config (e.g. one output per watched DAG): config ->
    # [(port, label)]. `ports` is then empty, and `dynamic_ports` names the rule for the editor.
    port_rule: Callable[[dict[str, Any]], list[tuple[str, str]]] | None = None
    dynamic_ports: str | None = None

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
            "needs_target": self.needs_target,
            "needs_dag_run": self.needs_dag_run,
            "is_action": self.is_action,
            "outputs": list(self.outputs),
            "dynamic_ports": self.dynamic_ports,
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
    "integration": "act",
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
        "done",
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
            "When any DAG has an incident",
            "trigger",
            "Starts when detection opens an incident on any monitored DAG, or records it again "
            "(a recurrence). To react to particular DAGs only, use 'Watch selected DAGs'.",
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
            "trigger.dag",
            "When this DAG runs",
            "trigger",
            "Starts when the chosen DAG has a run in progress, a run that succeeded, or a run "
            "that failed. Each event starts its own run through the matching output, so every "
            "outcome of the DAG can get its own steps. No incident is opened automatically for "
            "this DAG while the workflow is on: add 'Open incident' where you want one.",
            ("in_progress", "success", "failed"),
            DagTriggerConfig,
            {"in_progress": "in progress", "success": "succeeded", "failed": "failed"},
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
            "trigger.webhook",
            "When a webhook is called",
            "trigger",
            "Starts when another system sends a POST request to the workflow's secret URL (shown "
            "in the block's settings once the workflow is saved). The request's JSON body is "
            "available as {{trigger.body.<field>}}.",
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
            "Asks Airflow whether the workflow's DAG is paused or already has an active run.",
            ("ready", "paused", "busy"),
            port_labels={"ready": "ready (idle)", "paused": "paused", "busy": "already running"},
            needs_target=True,
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
            "logic.if",
            "If",
            "logic",
            "Compares a value (any {{placeholder}}, e.g. a query result or the DAG run's state) "
            "and continues on 'true' or 'false'. Numbers compare as numbers.",
            ("true", "false"),
            IfConfig,
            {"true": "true", "false": "false"},
            stage="check",
        ),
        NodeType(
            "logic.switch",
            "Switch",
            "logic",
            "Picks an output by value: each case you add gets its own output, anything else "
            "goes to 'otherwise'.",
            (),
            SwitchConfig,
            stage="check",
            port_rule=_switch_ports,
            dynamic_ports="cases",
        ),
        NodeType(
            "logic.join",
            "Wait for all",
            "logic",
            "Continues once every branch leading into it has arrived (branches that will not "
            "run are not waited for). Use it to bring parallel steps back together.",
            ("next",),
            stage="decide",
        ),
        NodeType(
            "logic.set",
            "Set value",
            "logic",
            "Stores a value under a name, for later blocks as {{vars.<name>}}.",
            ("next",),
            SetValueConfig,
            stage="decide",
            outputs=("vars",),
        ),
        NodeType(
            "http.request",
            "HTTP request",
            "integration",
            "Calls another system's API (GET, POST, …) with {{placeholders}} in the URL, headers "
            "and body. Continues on 'succeeded' for a 2xx answer, otherwise on 'failed'. Its "
            "answer is {{results.<id>.status}} and {{results.<id>.body}}.",
            ("success", "failed"),
            HttpRequestConfig,
            is_action=True,
            outputs=("results.{id}.status", "results.{id}.body"),
        ),
        NodeType(
            "flow.call_workflow",
            "Run another workflow",
            "integration",
            "Starts another workflow (one that starts by hand or by webhook) with input, and "
            "can wait for it to finish. Reuse common steps this way.",
            ("success", "failed"),
            CallWorkflowConfig,
            outputs=("results.{id}.run_id", "results.{id}.status"),
        ),
        NodeType(
            "flow.for_each",
            "For each row",
            "integration",
            "Runs a query and starts the chosen workflow once per row (one after another, at "
            "most 100), each with the row as {{trigger.body.<column>}}. 'Failed' when any of "
            "those runs failed.",
            ("done", "failed"),
            ForEachConfig,
            {"done": "all done", "failed": "some failed"},
            outputs=("results.{id}.rows", "results.{id}.failed"),
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
            needs_target=True,
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
            needs_target=True,
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
            needs_target=True,
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
            needs_target=True,
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
            "Clears the failed tasks of the DAG run (the one that started the workflow, or the "
            "incident's latest failing run) so Airflow reruns them.",
            ("success", "failed"),
            ClearTasksConfig,
            needs_target=True,
            is_action=True,
        ),
        NodeType(
            "action.trigger_dag_run",
            "Re-run the DAG",
            "action",
            "Triggers a new run of the workflow's DAG. To run any other DAG, use 'Run a DAG'.",
            ("success", "failed"),
            needs_target=True,
            is_action=True,
        ),
        NodeType(
            "action.set_dag_paused",
            "Pause or resume a DAG",
            "action",
            "Pauses (or resumes) the workflow's DAG in Airflow.",
            ("success", "failed"),
            SetPausedConfig,
            needs_target=True,
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
            "incident.open",
            "Open incident",
            "output",
            "Opens an incident for the DAG run that started the workflow, with its logs as "
            "evidence. If one is already open for this DAG and type, the run is recorded on it "
            "('already open'). Blocks after it can update the incident.",
            ("opened", "already_open"),
            OpenIncidentConfig,
            {"opened": "opened", "already_open": "already open"},
            needs_dag_run=True,
            outputs=("incident.id", "incident.title", "incident.severity"),
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
    "trigger.incident": "Starts for an incident on any monitored DAG.",
    "trigger.incident_stale": "Starts when an incident stays unacknowledged too long.",
    "trigger.dag": "Starts when the DAG's run is in progress, succeeds or fails.",
    "trigger.manual": "Starts when someone presses Run now.",
    "trigger.schedule": "Starts every N minutes, or at set times (cron).",
    "trigger.webhook": "Starts when its secret URL is called.",
    "condition.filter": "Continues only for incidents that match.",
    "check.dag_state": "Is the DAG paused or already running?",
    "check.sla": "Is this run slower than its past runs allow?",
    "logic.if": "Continues on true or false.",
    "logic.switch": "One output per case, plus otherwise.",
    "logic.join": "Waits until every branch into it has arrived.",
    "logic.set": "Stores a value for {{vars.<name>}}.",
    "flow.wait": "Waits, then continues.",
    "http.request": "Calls another system's API.",
    "flow.call_workflow": "Starts another workflow, and can wait for it.",
    "flow.for_each": "Runs a workflow once per row of a query.",
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
    "action.trigger_dag_run": "Starts a new run of the workflow's DAG.",
    "action.set_dag_paused": "Pauses or resumes the workflow's DAG.",
    "verify.run_success": "Waits for the last action's run and checks it.",
    "incident.open": "Opens an incident for the DAG run.",
    "incident.update": "Resolves, escalates, acknowledges or notes the incident.",
    "check.record": "Marks the finished run as passed or failed.",
    "notify": "Sends a message in the app or through a channel.",
}

TRIGGER_TYPES = frozenset(t for t, nt in NODE_TYPES.items() if nt.category == "trigger")
INCIDENT_TRIGGER_TYPES = frozenset(t for t in TRIGGER_TYPES if NODE_TYPES[t].needs_incident)
# Triggers that give the run a DAG run to work on (an incident's run, or the DAG's own run).
TARGET_TRIGGER_TYPES = INCIDENT_TRIGGER_TYPES | {"trigger.dag"}
# Start blocks replaced by "When this DAG runs" (saved workflows are migrated).
LEGACY_TRIGGERS = frozenset({"trigger.dag_watch", "trigger.dag_run"})
ACTION_TYPES = frozenset(t for t, nt in NODE_TYPES.items() if nt.is_action)


class GraphError(ValueError):
    def __init__(self, problems: list[dict[str, str]]) -> None:
        super().__init__("; ".join(p["message"] for p in problems))
        self.problems = problems


@dataclass(frozen=True)
class Node:
    id: str
    type: str
    name: str | None
    config: dict[str, Any]
    # Canvas layout; the engine only uses it to order blocks linked from the same output.
    position: dict[str, float] | None = None
    # How the engine runs it: retries and the 'error' output (NodeSettings), when set.
    settings: dict[str, Any] | None = None


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


def node_ports(
    node_type: str, config: dict[str, Any], settings: dict[str, Any] | None = None
) -> tuple[str, ...]:
    """Output ports of a block: fixed by its type, or (with a `port_rule`) from its config;
    plus 'error' when its settings send failures there."""
    nt = NODE_TYPES[node_type]
    ports = (
        tuple(port for port, _ in nt.port_rule(config)) if nt.port_rule is not None else nt.ports
    )
    if (settings or {}).get("on_error") == "output":
        ports = (*ports, "error")
    return ports


def node_port_labels(
    node_type: str, config: dict[str, Any], settings: dict[str, Any] | None = None
) -> dict[str, str]:
    nt = NODE_TYPES[node_type]
    if nt.port_rule is not None:
        labels = dict(nt.port_rule(config))
    else:
        labels = {p: nt.port_labels.get(p, _PORT_LABELS.get(p, p)) for p in nt.ports}
    if (settings or {}).get("on_error") == "output":
        labels["error"] = "error"
    return labels


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
        if str(item.get("type")) in LEGACY_TRIGGERS:
            problems.append(
                _problem(
                    "This start block was replaced by 'When this DAG runs': pick the DAG there",
                    node=node_id,
                )
            )
            continue
        if node_type is None:
            problems.append(_problem(f"Unknown node type '{item.get('type')}'", node=node_id))
            continue
        try:
            config = node_type.config_model.model_validate(item.get("config") or {})
        except ValidationError as exc:
            for err in exc.errors():
                field_name = ".".join(str(p) for p in err["loc"]) or "config"
                problems.append(
                    _problem(_config_message(node_type, err), node=node_id, field=field_name)
                )
            continue
        position, problem = _position(item.get("position"))
        if problem:
            problems.append(_problem(problem, node=node_id))
            continue
        settings = None
        if item.get("settings") and node_type.category != "trigger":
            try:
                parsed = NodeSettings.model_validate(item["settings"])
            except ValidationError as exc:
                problems.append(_problem(f"Run settings: {exc.errors()[0]['msg']}", node=node_id))
                continue
            if parsed != NodeSettings():
                settings = parsed.model_dump()
        name = item.get("name")
        nodes[node_id] = Node(
            id=node_id,
            type=node_type.type,
            name=str(name)[:200] if name else None,
            config=config.model_dump(mode="json"),
            position=position,
            settings=settings,
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
        if port not in node_ports(nodes[src].type, nodes[src].config, nodes[src].settings):
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
    blocks_ok = not problems  # every block and link is valid, so reachability is meaningful
    if len(triggers) != 1:
        problems.append(_problem(f"A workflow needs exactly one trigger (found {len(triggers)})"))
        raise GraphError(problems)

    # Report every remaining problem at once, so the editor can show them all.
    graph = Graph(nodes=nodes, edges=edges, trigger_id=triggers[0])
    problems.extend(_structure_problems(graph, check_reachable=blocks_ok))
    problems.extend(_trigger_fit_problems(graph))
    for n in graph.nodes.values():
        if n.type == "logic.join":
            incoming = {src for (src, _), targets in graph.edges.items() if n.id in targets}
            if len(incoming) < 2:
                problems.append(
                    _problem(
                        "'Wait for all' brings branches together: link at least two blocks into it",
                        node=n.id,
                    )
                )
    if problems:
        raise GraphError(problems)
    return graph


def _trigger_fit_problems(graph: Graph) -> list[dict[str, str]]:
    """Blocks that cannot work under the workflow's trigger."""
    trigger = graph.trigger.type
    opened = [n.id for n in graph.nodes.values() if n.type == "incident.open"]
    problems = []
    for n in graph.nodes.values():
        nt = NODE_TYPES[n.type]
        if nt.category == "trigger":
            continue
        if nt.needs_incident and trigger not in INCIDENT_TRIGGER_TYPES:
            # After an 'Open incident' block the run has an incident to work on.
            if not any(n.id in _downstream(graph, o) and n.id != o for o in opened):
                problems.append(
                    _problem(
                        f"'{nt.label}' works on an incident: start from an incident, or put an "
                        "'Open incident' block before it",
                        node=n.id,
                    )
                )
        if nt.needs_target and trigger not in TARGET_TRIGGER_TYPES:
            problems.append(
                _problem(
                    f"'{nt.label}' works on a DAG run: start with 'When this DAG runs' or an "
                    "incident trigger",
                    node=n.id,
                )
            )
        if nt.needs_dag_run and trigger != "trigger.dag":
            problems.append(
                _problem(
                    f"'{nt.label}' works on the DAG run that started the workflow, so it needs "
                    "'When this DAG runs'",
                    node=n.id,
                )
            )
    return problems


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


def downstream(graph: Graph, start: str) -> set[str]:
    """`start` and every block reachable from it."""
    return _downstream(graph, start)


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
_GLOBAL_VALUES = frozenset(
    {"workflow", "environment", "last_error", "links", "results", "vars", "trigger"}
)
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
        if graph.trigger.type == "trigger.dag":
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
    return warnings


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
                **({"settings": n.settings} if n.settings else {}),
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
