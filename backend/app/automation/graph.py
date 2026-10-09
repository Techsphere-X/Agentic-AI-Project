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

from app.detection.types import IncidentSeverity, IncidentType
from app.diagnosis.log_classifier import FailureCategory

MAX_NODES = 100
MAX_COORDINATE = 100_000
NODE_ID_PATTERN = r"^[A-Za-z0-9_-]{1,100}$"


class _Config(BaseModel):
    model_config = ConfigDict(extra="forbid")


class NoConfig(_Config):
    pass


class IncidentTriggerConfig(_Config):
    events: list[Literal["opened", "recurred"]] = Field(default=["opened"], min_length=1)
    incident_types: list[IncidentType] = Field(default_factory=list)  # empty = any


class StaleTriggerConfig(_Config):
    minutes: int = Field(default=30, ge=5, le=10080)


class DagRunTriggerConfig(_Config):
    states: list[Literal["success", "failed"]] = Field(
        default=["success", "failed"], min_length=1, json_schema_extra={"x-label": "When the run"}
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

    environments: list[Literal["DEV", "STAGING", "PROD"]] = Field(default_factory=list)
    incident_types: list[IncidentType] = Field(default_factory=list)
    min_severity: IncidentSeverity | None = None
    dag_ids: list[str] = Field(default_factory=list)
    tags_any: list[str] = Field(default_factory=list)
    min_occurrences: int | None = Field(default=None, ge=1)
    max_occurrences: int | None = Field(default=None, ge=1)
    diagnosis_categories: list[FailureCategory] = Field(default_factory=list)


# `x-widget` / `x-label` / `x-hint` / `x-hidden` are UI hints for the canvas settings form.
def _ui(label: str, widget: str | None = None, hint: str | None = None) -> dict[str, Any]:
    extra: dict[str, Any] = {"x-label": label}
    if widget:
        extra["x-widget"] = widget
    if hint:
        extra["x-hint"] = hint
    return extra


_SEND_VIA_HINT = "A channel from the Catalog; empty = in-app only"
_TO_HINT = "Email channels: addresses, comma separated; empty = the channel's default recipients"
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
        default_factory=lambda: ["PROD"]
    )
    timeout_minutes: int = Field(default=60, ge=1, le=10080)
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
            hint="Below this the outcome is 'unknown'. Operator corrections always pass; "
            "model scores top out around 90%. Empty = no minimum",
        ),
    )
    refresh: bool = Field(
        default=False,
        json_schema_extra=_ui(
            "Re-check the latest logs",
            hint="Diagnose again with logs that arrived later (operator corrections are kept)",
        ),
    )


class ChooseFixConfig(_Config):
    max_attempts: int = Field(
        default=2,
        ge=1,
        le=10,
        json_schema_extra=_ui(
            "Automatic fixes before handing over",
            hint="Retries and reruns already carried out on this incident, by any workflow",
        ),
    )
    min_confidence_pct: int | None = Field(
        default=None,
        ge=0,
        le=100,
        json_schema_extra=_ui(
            "Minimum diagnosis confidence (%)",
            hint="Below this the cause counts as unknown. Operator corrections always pass. "
            "Empty = no minimum",
        ),
    )
    retry_unknown_once: bool = Field(
        default=True, json_schema_extra=_ui("Try once when the cause is unknown")
    )
    pause_on_bad_data: bool = Field(
        default=False,
        json_schema_extra=_ui(
            "Pause the DAG on bad data or schema changes",
            hint="Stops bad data spreading downstream until someone fixes it",
        ),
    )
    pause_after_failures: int | None = Field(
        default=None,
        ge=2,
        le=100,
        json_schema_extra=_ui(
            "Pause after this many failures", hint="Empty = never pause for repeated failures"
        ),
    )
    check_dag_state: bool = Field(
        default=True,
        json_schema_extra=_ui(
            "Ask Airflow first",
            hint="Leave paused DAGs alone and wait while a run is active",
        ),
    )
    ai_mode: Literal["off", "suggest", "decide"] = Field(
        default="off",
        json_schema_extra=_ui(
            "Local AI",
            hint="suggest = record the AI's pick next to the rules' choice; decide = the AI picks. "
            "It only ever chooses among the fixes the rules allow",
        ),
    )
    ai_min_confidence_pct: int | None = Field(
        default=None,
        ge=0,
        le=100,
        json_schema_extra=_ui(
            "Minimum AI confidence (%)",
            hint="Below this the rules' choice stands. Empty = no minimum",
        ),
    )


class ClearTasksConfig(_Config):
    include_downstream: bool = True


class SetPausedConfig(_Config):
    paused: bool = True


class VerifyConfig(_Config):
    timeout_minutes: int = Field(default=30, ge=1, le=1440)


class IncidentUpdateConfig(_Config):
    operation: Literal["resolve", "escalate", "acknowledge", "note"]
    note: str | None = Field(default=None, max_length=2000)


class RecordCheckConfig(_Config):
    result: Literal["passed", "failed"] = Field(
        default="passed", json_schema_extra={"x-label": "Mark the run as"}
    )
    note: str = Field(
        default="",
        max_length=2000,
        json_schema_extra={
            "x-label": "Note",
            "x-hint": "Shown next to the run, e.g. {{last_error}}",
        },
    )
    open_incident: bool = Field(
        default=True,
        json_schema_extra={
            "x-label": "Open an incident when failed",
            "x-hint": "A 'Data check failed' incident, so incident workflows and alerts run; "
            "a later passed run resolves it",
        },
    )


class NotifyConfig(_Config):
    send_via: str = Field(
        default="", json_schema_extra=_ui("Send via", "notification_channel", _SEND_VIA_HINT)
    )
    to: list[str] = Field(default_factory=list, json_schema_extra=_ui("To", hint=_TO_HINT))
    level: Literal["INFO", "WARNING", "CRITICAL"] = "INFO"
    title: str = Field(default="{{workflow.name}}", min_length=1, max_length=300)
    message: str = Field(default="", max_length=4000)
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
    every_minutes: int = Field(
        default=60, ge=5, le=10080, json_schema_extra=_ui("Every (minutes)", hint="5 min to 7 days")
    )


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
        json_schema_extra=_ui(
            "Counts if it succeeded within (minutes)", hint="How recent the successful run must be"
        ),
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
        json_schema_extra=_ui(
            "Read only", hint="Runs in a read-only transaction; needs no approval"
        ),
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
        json_schema_extra=_ui(
            "Keep checking for (minutes)", hint="0 = check once; otherwise wait until it passes"
        ),
    )


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

    def catalog_entry(self) -> dict[str, Any]:
        return {
            "type": self.type,
            "label": self.label,
            "category": self.category,
            "description": self.description,
            "ports": list(self.ports),
            "port_labels": self.port_labels,
            "needs_incident": self.needs_incident,
            "needs_dag_run": self.needs_dag_run,
            "config_schema": self.config_model.model_json_schema(),
        }


NODE_TYPES: dict[str, NodeType] = {
    t.type: t
    for t in (
        # ------------------------------------------------------------ triggers
        NodeType(
            "trigger.incident",
            "When an incident opens or recurs",
            "trigger",
            "Starts when detection opens an incident or records it again.",
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
            "Starts for every finished run (succeeded or failed) of the monitored DAGs, so the "
            "workflow can check it and record its status.",
            ("next",),
            DagRunTriggerConfig,
            needs_dag_run=True,
        ),
        NodeType(
            "trigger.manual",
            "Run on demand",
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
            "Only if…",
            "logic",
            "Continues on 'true' when every configured criterion matches.",
            ("true", "false"),
            FilterConfig,
            {"true": "matches", "false": "does not match"},
            needs_incident=True,
        ),
        NodeType(
            "check.dag_state",
            "Check the DAG",
            "logic",
            "Asks Airflow whether the incident's DAG is paused or already has an active run.",
            ("ready", "paused", "busy"),
            port_labels={"ready": "not paused, idle", "paused": "paused", "busy": "run active"},
            needs_incident=True,
        ),
        NodeType(
            "flow.wait",
            "Wait",
            "logic",
            "Pauses the workflow for a number of minutes, then continues.",
            ("next",),
            WaitConfig,
        ),
        # ------------------------------------------------------------ pipelines
        NodeType(
            "pipeline.run_dag",
            "Run a DAG",
            "pipeline",
            "Starts a run of the chosen DAG, optionally with parameters, and can wait for it "
            "to finish.",
            ("success", "failed"),
            RunDagConfig,
            {"success": "succeeded", "failed": "failed"},
            is_action=True,
        ),
        NodeType(
            "pipeline.wait_for_dag",
            "Wait for a DAG",
            "pipeline",
            "Waits until the chosen DAG has a recent successful run (e.g. an upstream pipeline).",
            ("success", "timeout"),
            WaitForDagConfig,
            {"success": "it succeeded", "timeout": "gave up"},
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
        ),
        NodeType(
            "database.check",
            "Check data",
            "database",
            "Runs a read-only query that returns one value and compares it, e.g. row count > 0. "
            "Can keep checking until it passes.",
            ("pass", "fail"),
            CheckDataConfig,
            {"pass": "passes", "fail": "fails"},
        ),
        # ------------------------------------------------------------ incident handling
        NodeType(
            "diagnose.classify_log",
            "Diagnose failure",
            "diagnosis",
            "Labels the cause of the failure (network glitch, bad data, …) from the logs, using "
            "the rules, the AI model and operator corrections, and branches on whether a retry "
            "may help. 'Any outcome' is used when the outcome's own output is not connected.",
            ("retryable", "needs_fix", "unknown", "next"),
            DiagnoseConfig,
            {
                "retryable": "retry may help",
                "needs_fix": "needs a fix",
                "unknown": "unknown",
                "next": "any outcome",
            },
            needs_incident=True,
        ),
        NodeType(
            "decide.choose_fix",
            "Choose the fix",
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
        ),
        NodeType(
            "approval.request",
            "Ask a human",
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
            "Start a new run",
            "action",
            "Triggers a new run of the incident's DAG.",
            ("success", "failed"),
            needs_incident=True,
            is_action=True,
        ),
        NodeType(
            "action.set_dag_paused",
            "Pause / unpause DAG",
            "action",
            "Pauses (or unpauses) the incident's DAG in Airflow.",
            ("success", "failed"),
            SetPausedConfig,
            needs_incident=True,
            is_action=True,
        ),
        NodeType(
            "verify.run_success",
            "Check it worked",
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
            "Record the run's status",
            "output",
            "Marks the DAG run that started the workflow as passed or failed. A failed run can "
            "open a 'Data check failed' incident; a passed one resolves it.",
            ("next",),
            RecordCheckConfig,
            needs_dag_run=True,
        ),
        NodeType(
            "notify",
            "Tell someone",
            "output",
            "Sends an in-app notification or a webhook. Placeholders like {{incident.title}} "
            "or {{results.<block id>.value}}.",
            ("next",),
            NotifyConfig,
        ),
    )
}

TRIGGER_TYPES = frozenset(t for t, nt in NODE_TYPES.items() if nt.category == "trigger")
INCIDENT_TRIGGER_TYPES = frozenset(t for t in TRIGGER_TYPES if NODE_TYPES[t].needs_incident)
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
        try:
            config = node_type.config_model.model_validate(item.get("config") or {})
        except ValidationError as exc:
            for err in exc.errors():
                loc = ".".join(str(p) for p in err["loc"]) or "config"
                problems.append(_problem(f"{loc}: {err['msg']}", node=node_id))
            continue
        position, problem = _position(item.get("position"))
        if problem:
            problems.append(_problem(problem, node=node_id))
            continue
        name = item.get("name")
        nodes[node_id] = Node(
            id=node_id,
            type=node_type.type,
            name=str(name)[:200] if name else None,
            config=config.model_dump(mode="json"),
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
    if len(triggers) != 1 and not problems:
        problems.append(_problem(f"A workflow needs exactly one trigger (found {len(triggers)})"))
    if problems:
        raise GraphError(problems)

    graph = Graph(nodes=nodes, edges=edges, trigger_id=triggers[0])
    _check_acyclic_and_reachable(graph)
    if graph.trigger.type not in INCIDENT_TRIGGER_TYPES:
        needs = [
            _problem(
                f"'{NODE_TYPES[n.type].label}' works on an incident, so it needs an incident "
                "trigger",
                node=n.id,
            )
            for n in graph.nodes.values()
            if NODE_TYPES[n.type].needs_incident
        ]
        if needs:
            raise GraphError(needs)
    if graph.trigger.type != "trigger.dag_run":
        needs = [
            _problem(
                f"'{NODE_TYPES[n.type].label}' works on a finished DAG run, so it needs the "
                "'When a DAG run finishes' trigger",
                node=n.id,
            )
            for n in graph.nodes.values()
            if NODE_TYPES[n.type].needs_dag_run
        ]
        if needs:
            raise GraphError(needs)
    return graph


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


def _check_acyclic_and_reachable(graph: Graph) -> None:
    adjacency: dict[str, list[str]] = {n: [] for n in graph.nodes}
    for (src, _), targets in graph.edges.items():
        adjacency[src].extend(targets)

    visiting: set[str] = set()
    done: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in done:
            return
        if node_id in visiting:
            raise GraphError([_problem("The workflow contains a loop", node=node_id)])
        visiting.add(node_id)
        for nxt in adjacency[node_id]:
            visit(nxt)
        visiting.discard(node_id)
        done.add(node_id)

    visit(graph.trigger_id)
    for node_id in graph.nodes:
        visit(node_id)  # also catches cycles among unreachable nodes

    reachable: set[str] = set()
    stack = [graph.trigger_id]
    while stack:
        current = stack.pop()
        if current not in reachable:
            reachable.add(current)
            stack.extend(adjacency[current])
    unreachable = sorted(set(graph.nodes) - reachable)
    if unreachable:
        raise GraphError(
            [_problem(f"Node '{n}' is not connected to the trigger", node=n) for n in unreachable]
        )


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
