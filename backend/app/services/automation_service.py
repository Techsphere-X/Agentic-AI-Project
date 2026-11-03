"""Automation engine: enqueue workflow runs (incident events, schedules, "Run now"), advance
them step by step, and handle approvals, cancellation and workflow CRUD."""

import hmac
import logging
import os
import secrets
import socket
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from pydantic import ValidationError
from sqlalchemy import func, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app.automation import cron
from app.automation.graph import (
    ACTION_TYPES,
    INCIDENT_TRIGGER_TYPES,
    GraphError,
    downstream,
    to_raw,
    validate_graph,
)
from app.automation.templates import TEMPLATES, TEMPLATES_BY_KEY
from app.automation.types import (
    ACTIVE_RUN_STATUSES,
    ApprovalStatus,
    DagRunCheckStatus,
    RunStatus,
    StepStatus,
    TriggerEvent,
    WorkflowMode,
)
from app.core.config import get_settings
from app.core.exceptions import AppError, BadRequestError, ConflictError, NotFoundError
from app.db.base import utcnow
from app.detection.types import IncidentStatus
from app.models.airflow import AirflowConnection, AirflowTriggerReservation, MonitoredDag
from app.models.automation import (
    Approval,
    DagRunCheck,
    Notification,
    Workflow,
    WorkflowRun,
    WorkflowStep,
    WorkflowVersion,
)
from app.models.incident import Incident
from app.models.user import User
from app.orchestration.airflow.base import AirflowAdapterError, AirflowDagRun
from app.orchestration.airflow.factory import run_async
from app.services import airflow_service, audit_service, automation_nodes, incident_service
from app.services.automation_nodes import EXECUTORS, NodeContext, NodeError, NodeResult

logger = logging.getLogger(__name__)

HOLDER = f"{socket.gethostname()}:{os.getpid()}"
CLAIM_SECONDS = 300
TICK_BATCH = 100
# Nodes that must not start once the incident is resolved (nothing left to fix).
_NEEDS_OPEN_INCIDENT = ACTION_TYPES | {"approval.request"}


@dataclass
class TickSummary:
    enqueued: int = 0
    advanced: int = 0
    completed: int = 0
    waiting: int = 0
    failed: int = 0
    duration_ms: int = 0
    errors: list[dict[str, str]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------- workflows


def seed_templates(db: Session, actor: User | None = None) -> list[Workflow]:
    """Insert built-in templates that are not present yet (disabled)."""
    existing = set(db.scalars(select(Workflow.key).where(Workflow.key.is_not(None))).all())
    created = []
    for template in TEMPLATES:
        if template.key in existing:
            continue
        workflow = Workflow(
            key=template.key,
            name=template.name,
            description=template.description,
            enabled=False,
            mode=WorkflowMode.LIVE,
            graph=to_raw(validate_graph(template.build())),
            created_by=actor.id if actor else None,
        )
        db.add(workflow)
        created.append(workflow)
    if created:
        db.flush()
        audit_service.record(
            db,
            action="workflow.seed",
            entity_type="workflow",
            actor=actor,
            details={"keys": [w.key for w in created]},
        )
    db.commit()
    return created


def list_workflows(db: Session) -> list[tuple[Workflow, int, datetime | None]]:
    stats = dict(
        (row[0], (row[1], row[2]))
        for row in db.execute(
            select(
                WorkflowRun.workflow_id, func.count(), func.max(WorkflowRun.created_at)
            ).group_by(WorkflowRun.workflow_id)
        ).all()
    )
    workflows = db.scalars(select(Workflow).order_by(Workflow.created_at, Workflow.name)).all()
    return [(w, *stats.get(w.id, (0, None))) for w in workflows]


def get_workflow(db: Session, workflow_id: uuid.UUID) -> Workflow:
    workflow = db.get(Workflow, workflow_id)
    if workflow is None:
        raise NotFoundError("Workflow not found")
    return workflow


def _validated(raw: Any) -> dict[str, Any]:
    try:
        return to_raw(validate_graph(raw))
    except GraphError as exc:
        raise BadRequestError(
            "The workflow graph is invalid",
            code="invalid_graph",
            status_code=422,
            details={"problems": exc.problems},
        ) from exc


def build_template(
    template_key: str, parameters: dict[str, Any] | None = None
) -> tuple[Any, dict[str, Any], dict[str, Any]]:
    template = TEMPLATES_BY_KEY.get(template_key)
    if template is None:
        raise NotFoundError(f"Unknown template '{template_key}'")
    try:
        values = template.validate_parameters(parameters)
    except ValidationError as exc:
        problems = [
            {"field": ".".join(str(part) for part in error["loc"]), "message": error["msg"]}
            for error in exc.errors()
        ]
        raise BadRequestError(
            "Template parameters are invalid",
            code="invalid_template_parameters",
            status_code=422,
            details={"problems": problems},
        ) from exc
    graph = _validated(template.build(values.model_dump(mode="json")))
    return template, graph, values.model_dump(mode="json")


def create_workflow(
    db: Session,
    *,
    actor: User,
    template_key: str | None = None,
    name: str | None = None,
    description: str | None = None,
    graph: dict[str, Any] | None = None,
    template_parameters: dict[str, Any] | None = None,
    ip_address: str | None = None,
) -> Workflow:
    template = None
    parameters = None
    if template_key:
        template, graph, parameters = build_template(template_key, template_parameters)
        name = name or template.name
        description = description if description is not None else template.description
    if not name or graph is None:
        raise BadRequestError("Provide a template_key, or a name and a graph")
    workflow = Workflow(
        name=name,
        description=description,
        enabled=False,
        mode=WorkflowMode.LIVE,
        graph=_validated(graph),
        template_key=template.key if template else None,
        template_version=template.version if template else None,
        template_parameters=parameters,
        created_by=actor.id,
    )
    db.add(workflow)
    db.flush()
    _save_version(db, workflow, actor)
    audit_service.record(
        db,
        action="workflow.create",
        entity_type="workflow",
        entity_id=workflow.id,
        actor=actor,
        details={"name": name, "template": template_key},
        ip_address=ip_address,
    )
    db.commit()
    return workflow


def update_workflow(
    db: Session,
    workflow_id: uuid.UUID,
    changes: dict[str, Any],
    *,
    actor: User,
    ip_address: str | None = None,
) -> Workflow:
    workflow = get_workflow(db, workflow_id)
    changed: dict[str, Any] = {}
    for key in ("name", "description", "enabled", "mode"):
        if key in changes and changes[key] != getattr(workflow, key):
            setattr(workflow, key, changes[key])
            changed[key] = changes[key]
    if "graph" in changes and changes["graph"] is not None:
        graph = _validated(changes["graph"])
        if graph != workflow.graph:
            if _without_layout(graph) != _without_layout(workflow.graph):
                workflow.version += 1  # behaviour changed; layout-only edits keep the version
                changed["graph_version"] = workflow.version
                workflow.graph = graph
                _save_version(db, workflow, actor)
            workflow.graph = graph
    if workflow.enabled:
        _monitor_trigger_dag(db, workflow.graph, actor=actor, ip_address=ip_address)
    if changed:
        audit_service.record(
            db,
            action="workflow.update",
            entity_type="workflow",
            entity_id=workflow.id,
            actor=actor,
            details={"name": workflow.name, **changed},
            ip_address=ip_address,
        )
    db.commit()
    return workflow


def _trigger_dag(graph: dict[str, Any]) -> dict[str, Any] | None:
    """The config of a 'When this DAG runs' trigger, or None."""
    trigger = next((n for n in graph.get("nodes", []) if n.get("type") == "trigger.dag"), None)
    return (trigger or {}).get("config") or None


def _monitor_trigger_dag(
    db: Session, graph: dict[str, Any], *, actor: User, ip_address: str | None
) -> None:
    """'When this DAG runs' needs the DAG polled: switch monitoring on for it."""
    config = _trigger_dag(graph)
    if config is None:
        return
    try:
        connection_id = uuid.UUID(config["connection_id"])
    except (KeyError, ValueError):
        connection_id = None
    dag = connection_id and db.scalar(
        select(MonitoredDag).where(
            MonitoredDag.connection_id == connection_id,
            MonitoredDag.dag_id == config.get("dag_id"),
        )
    )
    if not dag:
        raise BadRequestError(
            f"DAG '{config.get('dag_id')}' is not in the synced DAG list of its Airflow "
            "connection; sync the connection or choose another DAG"
        )
    if dag.is_monitored:
        return
    dag.is_monitored = True
    audit_service.record(
        db,
        action="monitored_dag.toggle",
        entity_type="monitored_dag",
        entity_id=dag.id,
        actor=actor,
        details={
            "connection_id": str(dag.connection_id),
            "dag_id": dag.dag_id,
            "is_monitored": True,
            "reason": "used by an enabled 'When this DAG runs' workflow",
        },
        ip_address=ip_address,
    )


def dag_trigger_states(db: Session) -> dict[tuple[uuid.UUID, str], set[str]]:
    """(connection id, DAG id) -> the events enabled 'When this DAG runs' workflows start on.

    Detection polls these DAGs' runs for the workflows, and a DAG whose workflows start on
    'failed' owns its failures: no automatic failure incident is opened for it."""
    states: dict[tuple[uuid.UUID, str], set[str]] = {}
    for graph in db.scalars(select(Workflow.graph).where(Workflow.enabled.is_(True))):
        config = _trigger_dag(graph or {})
        if config is None:
            continue
        try:
            key = (uuid.UUID(config["connection_id"]), config["dag_id"])
        except (KeyError, ValueError):
            continue
        states.setdefault(key, set()).update(config.get("states") or [])
    return states


def _save_version(db: Session, workflow: Workflow, actor: User | None) -> None:
    if db.scalar(
        select(WorkflowVersion.id).where(
            WorkflowVersion.workflow_id == workflow.id,
            WorkflowVersion.version == workflow.version,
        )
    ):
        return
    db.add(
        WorkflowVersion(
            workflow_id=workflow.id,
            version=workflow.version,
            name=workflow.name,
            graph=workflow.graph,
            created_by=actor.id if actor is not None else None,
        )
    )


def list_versions(db: Session, workflow_id: uuid.UUID) -> list[WorkflowVersion]:
    get_workflow(db, workflow_id)
    return list(
        db.scalars(
            select(WorkflowVersion)
            .where(WorkflowVersion.workflow_id == workflow_id)
            .options(selectinload(WorkflowVersion.creator))
            .order_by(WorkflowVersion.version.desc())
        )
    )


def get_version(db: Session, workflow_id: uuid.UUID, version: int) -> WorkflowVersion:
    found = db.scalar(
        select(WorkflowVersion).where(
            WorkflowVersion.workflow_id == workflow_id, WorkflowVersion.version == version
        )
    )
    if found is None:
        raise NotFoundError(f"Version {version} not found")
    return found


def restore_version(
    db: Session, workflow_id: uuid.UUID, version: int, *, actor: User, ip_address: str | None
) -> Workflow:
    """Bring back an older graph as a new version (history is never rewritten)."""
    old = get_version(db, workflow_id, version)
    workflow = update_workflow(
        db, workflow_id, {"graph": old.graph}, actor=actor, ip_address=ip_address
    )
    audit_service.record(
        db,
        action="workflow.version_restored",
        entity_type="workflow",
        entity_id=workflow.id,
        actor=actor,
        details={"restored": version, "now": workflow.version},
        ip_address=ip_address,
    )
    db.commit()
    return workflow


def test_node(
    db: Session,
    raw_graph: dict[str, Any],
    node_id: str,
    *,
    workflow_id: uuid.UUID | None = None,
    input: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """'Test this block': run one block as a dry run and undo everything it wrote.

    Its context is `input`, or what the workflow's latest run knew; actions are simulated.
    """
    try:
        graph = validate_graph(raw_graph)
    except GraphError as exc:
        raise BadRequestError(f"The workflow is not valid: {exc}") from exc
    node = graph.nodes.get(node_id)
    if node is None:
        raise NotFoundError(f"Block '{node_id}' not found")
    context: dict[str, Any] = {}
    incident = None
    latest = None
    if workflow_id is not None:
        latest = db.scalar(
            select(WorkflowRun)
            .where(WorkflowRun.workflow_id == workflow_id)
            .order_by(WorkflowRun.created_at.desc())
            .limit(1)
        )
    if input is not None:
        context = dict(input)
    elif latest is not None:
        context = {k: v for k, v in (latest.context or {}).items() if k != "nodes"}
        incident = latest.incident
    savepoint = db.begin_nested()
    try:
        run = WorkflowRun(
            id=uuid.uuid4(),
            workflow_id=workflow_id,
            workflow_version=0,
            graph=raw_graph,
            incident_id=incident.id if incident else None,
            trigger_event="test",
            dedup_key=f"test:{uuid.uuid4()}",
            status=RunStatus.RUNNING,
            dry_run=True,
            context=context,
        )
        if workflow_id is not None:
            db.add(run)
            db.flush()
        ctx = NodeContext(
            db=db, run=run, graph=graph, incident=incident, now=utcnow(), settings=get_settings()
        )
        try:
            result = EXECUTORS[node.type](ctx, node)
        except NodeError as exc:
            return {"port": None, "output": {}, "message": None, "error": str(exc)}
        except Exception as exc:  # a test must never break the editor
            logger.warning("Testing block %s failed: %s", node_id, exc)
            return {"port": None, "output": {}, "message": None, "error": str(exc)[:500]}
        return {
            "port": result.port,
            "output": result.output,
            "message": result.message,
            "waits": result.wait_until is not None,
        }
    finally:
        savepoint.rollback()


def _without_layout(graph: dict[str, Any]) -> dict[str, Any]:
    return {
        "nodes": [{k: v for k, v in n.items() if k != "position"} for n in graph.get("nodes", [])],
        "edges": graph.get("edges", []),
    }


def check_graph(raw: Any) -> tuple[dict[str, Any] | None, list[dict[str, str]]]:
    """Validate without saving: (normalized graph, []) or (None, problems)."""
    try:
        return to_raw(validate_graph(raw)), []
    except GraphError as exc:
        return None, exc.problems


def delete_workflow(
    db: Session, workflow_id: uuid.UUID, *, actor: User, ip_address: str | None = None
) -> None:
    workflow = get_workflow(db, workflow_id)
    runs = db.scalar(select(func.count()).where(WorkflowRun.workflow_id == workflow.id)) or 0
    if runs:
        raise ConflictError(
            f"This workflow has {runs} run(s) kept as audit history. Turn it off instead.",
            code="workflow_has_runs",
            details={"runs": runs},
        )
    audit_service.record(
        db,
        action="workflow.delete",
        entity_type="workflow",
        entity_id=workflow.id,
        actor=actor,
        details={"name": workflow.name, "key": workflow.key},
        ip_address=ip_address,
    )
    db.delete(workflow)
    db.commit()


# ---------------------------------------------------------------------- enqueue


def _incident_matches(trigger: Any, incident: Incident) -> bool:
    """Whether an incident (whatever the event) is one this incident trigger reacts to."""
    if trigger.type == "trigger.incident":
        types = trigger.config["incident_types"]
        if types and incident.type not in types:
            return False
    return True


def _trigger_matches(
    workflow: Workflow, incident: Incident, event: TriggerEvent, now: datetime
) -> bool:
    try:
        trigger = validate_graph(workflow.graph).trigger
    except GraphError:
        logger.warning("Workflow %s has an invalid graph; skipped", workflow.id)
        return False
    if trigger.type == "trigger.incident":
        return event.value in trigger.config["events"] and _incident_matches(trigger, incident)
    if trigger.type == "trigger.incident_stale" and event == TriggerEvent.STALE:
        return incident.first_seen_at <= now - timedelta(minutes=trigger.config["minutes"])
    return False


def _dedup_key(incident: Incident, event: TriggerEvent) -> str:
    if event == TriggerEvent.STALE:  # once per occurrence: a recurrence ignored again counts
        return f"{incident.id}:stale:{incident.occurrence_count}"
    return f"{incident.id}:{event.value}:{incident.occurrence_count}"


def enqueue_for_incident(
    db: Session,
    incident: Incident,
    event: TriggerEvent,
    *,
    now: datetime | None = None,
    workflows: list[Workflow] | None = None,
) -> list[WorkflowRun]:
    """Create PENDING runs for enabled workflows triggered by this incident event.

    Called inside the caller's transaction (e.g. a detection cycle); the tick executes them.
    """
    settings = get_settings()
    if not settings.AUTOMATION_ENABLED:
        return []
    now = now or utcnow()
    runs = []
    if workflows is None:
        workflows = list(db.scalars(select(Workflow).where(Workflow.enabled.is_(True))))
    for workflow in workflows:
        if not _trigger_matches(workflow, incident, event, now):
            continue
        busy = db.scalar(
            select(WorkflowRun.id).where(
                WorkflowRun.workflow_id == workflow.id,
                WorkflowRun.incident_id == incident.id,
                WorkflowRun.status.in_(ACTIVE_RUN_STATUSES),
            )
        )
        if busy:
            continue  # one active run per workflow and incident
        dedup = _dedup_key(incident, event)
        if db.scalar(
            select(WorkflowRun.id).where(
                WorkflowRun.workflow_id == workflow.id, WorkflowRun.dedup_key == dedup
            )
        ):
            continue
        run = _add_run(db, workflow, event, dedup, incident_id=incident.id)
        if run is not None:
            runs.append(run)
    return runs


def dag_run_context(
    conn: AirflowConnection, dag: MonitoredDag, dag_run: AirflowDagRun
) -> dict[str, Any]:
    """What a 'When this DAG runs' run knows about the DAG run that started it."""
    return {
        "connection_id": str(conn.id),
        "monitored_dag_id": str(dag.id),
        "dag_id": dag.dag_id,
        "run_id": dag_run.run_id,
        "state": dag_run.normalized_state,
        "normalized_state": dag_run.normalized_state,
        "airflow_state": dag_run.state,
        "environment": str(conn.environment),
    }


def enqueue_for_dag_event(
    db: Session,
    conn: AirflowConnection,
    dag: MonitoredDag,
    dag_run: AirflowDagRun,
    *,
    now: datetime | None = None,
) -> list[WorkflowRun]:
    """'When this DAG runs': a PENDING run for each enabled workflow on this DAG whose chosen
    events include the run's state. Each (Airflow run, state) starts a workflow at most once,
    and a run that the workflow itself is fixing does not start it again."""
    if not get_settings().AUTOMATION_ENABLED:
        return []
    now = now or utcnow()
    state = dag_run.normalized_state
    if state not in ("in_progress", "success", "failed"):
        return []
    runs = []
    dedup = f"dagrun:{conn.id}:{dag.dag_id}:{dag_run.run_id}:{state}"[:200]
    for workflow in db.scalars(select(Workflow).where(Workflow.enabled.is_(True))).all():
        config = _trigger_dag(workflow.graph or {})
        if (
            config is None
            or config.get("connection_id") != str(conn.id)
            or config.get("dag_id") != dag.dag_id
            or state not in (config.get("states") or [])
        ):
            continue
        if db.scalar(
            select(WorkflowRun.id).where(
                WorkflowRun.workflow_id == workflow.id, WorkflowRun.dedup_key == dedup
            )
        ):
            continue
        if _started_by(db, workflow, conn, dag.dag_id, dag_run.run_id) or _owned_by_fix(
            db, workflow, conn, dag.dag_id, dag_run.run_id, now
        ):
            continue  # its own fix touched this run: the fixing run follows it with 'Verify'
        run = _add_run(db, workflow, TriggerEvent.DAG_RUN, dedup)
        if run is None:
            continue
        run.context = {"dag_run": dag_run_context(conn, dag, dag_run)}
        if (
            dag_run.is_terminal
            and _dag_run_check(db, workflow.id, conn, dag.dag_id, dag_run.run_id) is None
        ):
            db.add(
                DagRunCheck(
                    connection_id=conn.id,
                    monitored_dag_id=dag.id,
                    dag_id=dag.dag_id,
                    run_id=dag_run.run_id,
                    run_state=dag_run.state,
                    workflow_run_id=run.id,
                    status=DagRunCheckStatus.PENDING,
                )
            )
        runs.append(run)
    return runs


FIX_CLAIM_HOURS = 1


def _owned_by_fix(
    db: Session,
    workflow: Workflow,
    conn: AirflowConnection,
    dag_id: str,
    run_id: str,
    now: datetime,
) -> bool:
    """A run of this workflow (active, or finished within the last hour) is fixing that
    Airflow run: it cleared its failed tasks, so its next states belong to that run."""
    claim = {"connection_id": str(conn.id), "dag_id": dag_id, "run_id": run_id}
    recent = db.scalars(
        select(WorkflowRun.context).where(
            WorkflowRun.workflow_id == workflow.id,
            or_(
                WorkflowRun.status.in_(ACTIVE_RUN_STATUSES),
                WorkflowRun.finished_at >= now - timedelta(hours=FIX_CLAIM_HOURS),
            ),
        )
    )
    return any(claim in ((context or {}).get("fix_claims") or []) for context in recent)


def _dag_run_check(
    db: Session,
    workflow_id: uuid.UUID,
    conn: AirflowConnection,
    dag_id: str,
    run_id: str,
) -> DagRunCheck | None:
    return db.scalar(
        select(DagRunCheck)
        .join(WorkflowRun, WorkflowRun.id == DagRunCheck.workflow_run_id)
        .where(
            WorkflowRun.workflow_id == workflow_id,
            DagRunCheck.connection_id == conn.id,
            DagRunCheck.dag_id == dag_id,
            DagRunCheck.run_id == run_id,
        )
    )


def _refresh_dag_run_check(
    check: DagRunCheck,
    dag_run: AirflowDagRun,
    conn: AirflowConnection,
    dag: MonitoredDag,
) -> None:
    check.run_state = dag_run.state
    workflow_run = check.workflow_run
    context = dict(workflow_run.context or {})
    dag_context = dict(context.get("dag_run") or {})
    dag_context.update(
        {
            "connection_id": str(conn.id),
            "monitored_dag_id": str(dag.id),
            "dag_id": dag.dag_id,
            "run_id": dag_run.run_id,
            "state": dag_run.normalized_state,
            "normalized_state": dag_run.normalized_state,
            "airflow_state": dag_run.state,
            "environment": str(conn.environment),
        }
    )
    context["dag_run"] = dag_context
    workflow_run.context = context


def refresh_dag_run_checks(
    db: Session,
    conn: AirflowConnection,
    dag: MonitoredDag,
    dag_run: AirflowDagRun,
) -> int:
    """Refresh existing in-progress workflow checks without starting new workflows."""
    checks = db.scalars(
        select(DagRunCheck).where(
            DagRunCheck.connection_id == conn.id,
            DagRunCheck.monitored_dag_id == dag.id,
            DagRunCheck.dag_id == dag.dag_id,
            DagRunCheck.run_id == dag_run.run_id,
        )
    ).all()
    for check in checks:
        _refresh_dag_run_check(check, dag_run, conn, dag)
    return len(checks)


def _started_by(
    db: Session, workflow: Workflow, conn: AirflowConnection, dag_id: str, run_id: str
) -> bool:
    return (
        db.scalar(
            select(AirflowTriggerReservation.id)
            .join(WorkflowRun, WorkflowRun.id == AirflowTriggerReservation.workflow_run_id)
            .where(
                AirflowTriggerReservation.connection_id == conn.id,
                AirflowTriggerReservation.dag_id == dag_id,
                AirflowTriggerReservation.airflow_run_id == run_id,
                WorkflowRun.workflow_id == workflow.id,
            )
        )
        is not None
    )


def _add_run(
    db: Session,
    workflow: Workflow,
    event: TriggerEvent,
    dedup: str,
    *,
    incident_id: uuid.UUID | None = None,
    dry_run: bool | None = None,
) -> WorkflowRun | None:
    """Add a PENDING run; None if the dedup key was taken meanwhile (another worker)."""
    settings = get_settings()
    run = WorkflowRun(
        id=uuid.uuid4(),
        workflow_id=workflow.id,
        workflow_version=workflow.version,
        graph=workflow.graph,
        incident_id=incident_id,
        trigger_event=event.value,
        dedup_key=dedup,
        status=RunStatus.PENDING,
        dry_run=(workflow.mode == WorkflowMode.DRY_RUN if dry_run is None else dry_run)
        or settings.AUTOMATION_FORCE_DRY_RUN,
        current_node=validate_graph(workflow.graph).trigger_id,
        context={},
    )
    try:
        with db.begin_nested():
            db.add(run)
    except IntegrityError:
        return None
    return run


def _has_active_run(db: Session, workflow_id: uuid.UUID) -> bool:
    return bool(
        db.scalar(
            select(WorkflowRun.id).where(
                WorkflowRun.workflow_id == workflow_id,
                WorkflowRun.status.in_(ACTIVE_RUN_STATUSES),
            )
        )
    )


def _latest_dag_run_context(db: Session, config: dict[str, Any], now: datetime) -> dict[str, Any]:
    """'Run now' on a 'When this DAG runs' workflow: replay it on the DAG's latest run."""
    conn = db.get(AirflowConnection, uuid.UUID(config["connection_id"]))
    dag = conn and db.scalar(
        select(MonitoredDag).where(
            MonitoredDag.connection_id == conn.id, MonitoredDag.dag_id == config["dag_id"]
        )
    )
    if not conn or not dag:
        raise BadRequestError(f"DAG '{config['dag_id']}' is not in the synced DAG list")
    try:
        adapter = airflow_service.adapter_for(conn)
        runs = run_async(
            lambda: adapter.list_dag_runs(dag.dag_id, since=now - timedelta(days=30), limit=20)
        )
    except (AppError, AirflowAdapterError) as exc:
        message = exc.message if isinstance(exc, AppError) else exc.result.message
        raise BadRequestError(f"Could not read the runs of {dag.dag_id}: {message}") from exc
    runs = [r for r in runs if r.normalized_state in ("in_progress", "success", "failed")]
    if not runs:
        raise BadRequestError(f"{dag.dag_id} has no runs in the last 30 days to start from")
    latest = max(runs, key=lambda r: r.sort_date or datetime.min.replace(tzinfo=UTC))
    return dag_run_context(conn, dag, latest)


def start_manual_run(
    db: Session,
    workflow_id: uuid.UUID,
    *,
    actor: User,
    dry_run: bool | None = None,
    now: datetime | None = None,
    input: dict[str, Any] | None = None,
) -> WorkflowRun:
    """ "Run now": start a run of a manual or scheduled workflow and advance it right away.

    Works while the workflow is turned off, so it can be tried before it is switched on.
    """
    if not get_settings().AUTOMATION_ENABLED:
        raise BadRequestError("Automation is disabled on this server (AUTOMATION_ENABLED)")
    workflow = get_workflow(db, workflow_id)
    try:
        trigger = validate_graph(workflow.graph).trigger
    except GraphError as exc:
        raise BadRequestError(f"The workflow is not valid: {exc}") from exc
    incident_id = None
    context: dict[str, Any] = {}
    if trigger.type == "trigger.dag":
        context = {"dag_run": _latest_dag_run_context(db, trigger.config, now or utcnow())}
    if trigger.type in INCIDENT_TRIGGER_TYPES:
        # Run on the newest open incident this trigger would react to.
        candidates = db.scalars(
            select(Incident)
            .where(Incident.status == IncidentStatus.OPEN)
            .order_by(Incident.created_at.desc())
            .limit(500)
        )
        incident = next((i for i in candidates if _incident_matches(trigger, i)), None)
        if incident is None:
            raise BadRequestError("No open incident matches this workflow's trigger")
        incident_id = incident.id
    if _has_active_run(db, workflow.id):
        raise ConflictError("This workflow is already running; wait for it or cancel it first")
    run = _add_run(
        db,
        workflow,
        TriggerEvent.MANUAL,
        f"manual:{uuid.uuid4()}",
        incident_id=incident_id,
        dry_run=dry_run,
    )
    assert run is not None  # a fresh random dedup key cannot collide
    if input is not None:
        context["trigger"] = {"body": input}
    if context:
        run.context = context
    audit_service.record(
        db,
        action="automation.run_manual",
        entity_type="workflow_run",
        entity_id=run.id,
        actor=actor,
        details={"workflow": workflow.name, "dry_run": run.dry_run},
    )
    db.commit()
    return run_now(db, run.id, now=now) or run


# A cron time is run if the engine sees it within this long (it ticks every few seconds).
CRON_CATCH_UP = timedelta(minutes=15)


def _enqueue_scheduled(db: Session, now: datetime) -> int:
    """One run per schedule slot for each enabled scheduled workflow that is not busy."""
    count = 0
    for workflow in db.scalars(select(Workflow).where(Workflow.enabled.is_(True))).all():
        try:
            trigger = validate_graph(workflow.graph).trigger
        except GraphError:
            continue
        if trigger.type != "trigger.schedule":
            continue
        config = trigger.config
        if config.get("mode") == "cron":
            # The latest set time within the last few minutes (a late tick still catches it).
            fire = cron.latest_fire(config["cron"], config["timezone"], now, CRON_CATCH_UP)
            if fire is None:
                continue
            dedup = f"schedule:cron:{fire.isoformat()}"
        else:
            slot = int(now.timestamp()) // (config["every_minutes"] * 60)
            dedup = f"schedule:{config['every_minutes']}:{slot}"
        if _has_active_run(db, workflow.id) or db.scalar(
            select(WorkflowRun.id).where(
                WorkflowRun.workflow_id == workflow.id, WorkflowRun.dedup_key == dedup
            )
        ):
            continue
        if _add_run(db, workflow, TriggerEvent.SCHEDULE, dedup) is not None:
            count += 1
    db.commit()
    return count


def _enqueue_stale(db: Session, now: datetime) -> int:
    """'When an incident is ignored': graphs are checked once per tick, then only those
    workflows are matched against each open incident."""
    count = 0
    stale_workflows, thresholds = [], []
    for workflow in db.scalars(select(Workflow).where(Workflow.enabled.is_(True))).all():
        try:
            trigger = validate_graph(workflow.graph).trigger
        except GraphError:
            continue
        if trigger.type == "trigger.incident_stale":
            stale_workflows.append(workflow)
            thresholds.append(trigger.config["minutes"])
    if not thresholds:
        return 0
    cutoff = now - timedelta(minutes=min(thresholds))
    incidents = db.scalars(
        select(Incident).where(
            Incident.status == IncidentStatus.OPEN, Incident.first_seen_at <= cutoff
        )
    ).all()
    for incident in incidents:
        count += len(
            enqueue_for_incident(
                db, incident, TriggerEvent.STALE, now=now, workflows=stale_workflows
            )
        )
    db.commit()
    return count


# ---------------------------------------------------------------------- called workflows

MAX_CALL_DEPTH = 3
CALLABLE_TRIGGERS = frozenset({"trigger.manual", "trigger.webhook"})


def _start_child_run(
    db: Session, parent: WorkflowRun, workflow_id: str, body: dict[str, Any]
) -> WorkflowRun:
    """'Run another workflow' / 'For each row': a run of `workflow_id` with `body` as its
    input. It inherits dry run; the engine advances it like any other run."""
    try:
        workflow = db.get(Workflow, uuid.UUID(workflow_id))
    except ValueError:
        workflow = None
    if workflow is None:
        raise NodeError("The workflow to run no longer exists")
    if workflow.id == parent.workflow_id:
        raise NodeError("A workflow cannot run itself")
    depth = int(((parent.context or {}).get("parent") or {}).get("depth", 0)) + 1
    if depth > MAX_CALL_DEPTH:
        raise NodeError(f"Workflows can run each other at most {MAX_CALL_DEPTH} levels deep")
    try:
        trigger = validate_graph(workflow.graph).trigger
    except GraphError as exc:
        raise NodeError(f"'{workflow.name}' is not valid: {exc}") from exc
    if trigger.type not in CALLABLE_TRIGGERS:
        raise NodeError(f"'{workflow.name}' does not start by hand or by webhook")
    run = _add_run(
        db, workflow, TriggerEvent.CALLED, f"called:{uuid.uuid4()}", dry_run=parent.dry_run
    )
    assert run is not None  # a fresh random dedup key cannot collide
    run.context = {
        "trigger": {"body": body},
        "parent": {"run_id": str(parent.id), "workflow": parent.workflow_name, "depth": depth},
    }
    db.flush()
    return run


automation_nodes.start_child_run = _start_child_run


# ---------------------------------------------------------------------- webhooks

WEBHOOK_MAX_BODY_BYTES = 64 * 1024
WEBHOOK_MAX_PER_MINUTE = 30


def webhook_token(db: Session, workflow_id: uuid.UUID, *, actor: User, rotate: bool = False) -> str:
    """The secret in the workflow's webhook URL, made on first use; `rotate` replaces it so
    the old URL stops working."""
    workflow = get_workflow(db, workflow_id)
    if workflow.webhook_token is None or rotate:
        workflow.webhook_token = secrets.token_urlsafe(32)
        audit_service.record(
            db,
            action="workflow.webhook_token_rotated" if rotate else "workflow.webhook_token_created",
            entity_type="workflow",
            entity_id=workflow.id,
            actor=actor,
            details={"name": workflow.name},
        )
        db.commit()
    return workflow.webhook_token


def receive_webhook(
    db: Session,
    workflow_id: uuid.UUID,
    token: str,
    body: Any,
    *,
    ip_address: str | None = None,
    now: datetime | None = None,
) -> WorkflowRun:
    """Start a run of an enabled 'When a webhook is called' workflow. Every failure looks the
    same from outside (not found), so the URL cannot be probed."""
    now = now or utcnow()
    workflow = db.get(Workflow, workflow_id)
    if (
        workflow is None
        or not workflow.webhook_token
        or not hmac.compare_digest(workflow.webhook_token, token)
        or not workflow.enabled
        or not get_settings().AUTOMATION_ENABLED
    ):
        raise NotFoundError("Not found")
    try:
        trigger = validate_graph(workflow.graph).trigger
    except GraphError as exc:
        raise NotFoundError("Not found") from exc
    if trigger.type != "trigger.webhook":
        raise NotFoundError("Not found")
    recent = db.scalar(
        select(func.count())
        .select_from(WorkflowRun)
        .where(
            WorkflowRun.workflow_id == workflow.id,
            WorkflowRun.trigger_event == TriggerEvent.WEBHOOK.value,
            WorkflowRun.created_at >= now - timedelta(minutes=1),
        )
    )
    if (recent or 0) >= WEBHOOK_MAX_PER_MINUTE:
        raise ConflictError(f"At most {WEBHOOK_MAX_PER_MINUTE} calls a minute", code="rate_limited")
    run = _add_run(db, workflow, TriggerEvent.WEBHOOK, f"webhook:{uuid.uuid4()}")
    assert run is not None  # a fresh random dedup key cannot collide
    run.context = {"trigger": {"body": body, "received_at": now.isoformat()}}
    audit_service.record(
        db,
        action="automation.webhook_received",
        entity_type="workflow_run",
        entity_id=run.id,
        details={"workflow": workflow.name},
        ip_address=ip_address,
    )
    db.commit()
    return run


# ---------------------------------------------------------------------- execution


def _claim(db: Session, run_id: uuid.UUID, now: datetime, holder: str) -> bool:
    result = db.execute(
        update(WorkflowRun)
        .where(
            WorkflowRun.id == run_id,
            WorkflowRun.status.in_(ACTIVE_RUN_STATUSES),
            or_(WorkflowRun.claimed_until.is_(None), WorkflowRun.claimed_until < now),
        )
        .values(claimed_by=holder, claimed_until=now + timedelta(seconds=CLAIM_SECONDS))
    )
    db.commit()
    return result.rowcount == 1  # type: ignore[attr-defined]


def _finish(
    db: Session, run: WorkflowRun, status: RunStatus, now: datetime, *, error: str | None = None
) -> None:
    run.status = status
    run.finished_at = now
    run.wake_at = None
    run.error = error
    for approval in run.approvals:
        if approval.status == ApprovalStatus.PENDING:
            approval.status = ApprovalStatus.EXPIRED
            approval.decided_at = now
    check = db.scalar(select(DagRunCheck).where(DagRunCheck.workflow_run_id == run.id))
    if check is not None and check.status == DagRunCheckStatus.PENDING:
        if status == RunStatus.COMPLETED:
            check.status = DagRunCheckStatus.NOT_CHECKED
            check.message = check.message or "The workflow ended without recording a result"
        else:
            check.status = DagRunCheckStatus.ERROR
            check.message = error or f"The workflow was {status.value.lower()}"
        check.checked_at = now
    if run.incident is not None:
        incident_service.add_event(
            db,
            run.incident,
            "automation_finished",
            details={
                "workflow": run.workflow_name,
                "run_id": str(run.id),
                "status": str(status),
                "outcome": (run.context or {}).get("outcome"),
                **({"error": error} if error else {}),
            },
        )
    audit_service.record(
        db,
        action="automation.run_finished",
        entity_type="workflow_run",
        entity_id=run.id,
        details={"workflow": run.workflow_name, "status": status, "error": error},
    )


_DONE_STEPS = frozenset({StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.SKIPPED})


def _waiting_step(run: WorkflowRun, node_id: str) -> WorkflowStep | None:
    for step in reversed(run.steps):
        if step.node_id == node_id and step.status == StepStatus.WAITING:
            return step
    return None


def advance(db: Session, run: WorkflowRun, now: datetime | None = None) -> WorkflowRun:
    """Execute nodes until the run waits or ends. The caller must hold the run's claim."""
    settings = get_settings()
    now = now or utcnow()
    try:
        _advance(db, run, now, settings.AUTOMATION_MAX_STEPS_PER_RUN)
    except Exception as exc:
        db.rollback()
        logger.exception("Workflow run %s failed", run.id)
        run = db.get(WorkflowRun, run.id)  # type: ignore[assignment]
        _finish(db, run, RunStatus.FAILED, now, error=str(exc)[:500] or type(exc).__name__)
    finally:
        run.claimed_until = None
        run.claimed_by = None
        db.commit()
    return run


def _advance(db: Session, run: WorkflowRun, now: datetime, max_steps: int) -> None:
    if run.status.is_terminal:
        return
    incident = run.incident
    if run.incident_id is not None and incident is None:
        _finish(db, run, RunStatus.FAILED, now, error="The incident no longer exists")
        return
    graph = validate_graph(run.graph)
    if run.status == RunStatus.PENDING:
        run.status = RunStatus.RUNNING
        run.started_at = now
        if incident is not None:
            incident_service.add_event(
                db,
                incident,
                "automation_started",
                details={
                    "workflow": run.workflow_name,
                    "run_id": str(run.id),
                    "dry_run": run.dry_run,
                },
            )
        audit_service.record(
            db,
            action="automation.run_started",
            entity_type="workflow_run",
            entity_id=run.id,
            details={
                "workflow": run.workflow_name,
                **({"incident_id": str(incident.id)} if incident is not None else {}),
                "trigger": run.trigger_event,
                "dry_run": run.dry_run,
            },
        )
    else:
        run.status = RunStatus.RUNNING
    run.wake_at = None
    ctx = NodeContext(
        db=db, run=run, graph=graph, incident=incident, now=now, settings=get_settings()
    )

    # To-do list of blocks. Runs started before it existed only have `current_node`.
    queue = (
        list(run.pending_nodes)
        if run.pending_nodes is not None
        else [run.current_node or graph.trigger_id]
    )
    done = {s.node_id for s in run.steps if s.status in _DONE_STEPS}

    for _ in range(max_steps):
        if not queue:
            break
        node = graph.nodes[queue[0]]
        if node.id in done:  # reached again through another branch: blocks run once
            _set_queue(run, queue[1:])
            queue = queue[1:]
            continue
        if node.type == "logic.join" and any(
            node.id in downstream(graph, other) for other in queue[1:]
        ):
            # 'Wait for all': another queued branch can still arrive; run it later.
            queue = [*queue[1:], node.id]
            _set_queue(run, queue)
            continue
        step = _waiting_step(run, node.id)

        if (
            incident is not None
            and node.type in _NEEDS_OPEN_INCIDENT
            and incident.status == IncidentStatus.RESOLVED
        ):
            if step is not None:
                step.status = StepStatus.SKIPPED
                step.finished_at = now
                step.message = "Incident already resolved"
            run.context = {**(run.context or {}), "outcome": "incident_resolved"}
            _finish(db, run, RunStatus.COMPLETED, now)
            return

        if step is None:
            step = WorkflowStep(
                run_id=run.id,
                node_id=node.id,
                node_type=node.type,
                status=StepStatus.RUNNING,
                started_at=now,
            )
            db.add(step)
            run.steps.append(step)
        failure: str | None = None
        try:
            result: NodeResult = EXECUTORS[node.type](ctx, node)
        except NodeError as exc:
            failure = str(exc)
            result = NodeResult(None, {"error": failure}, failure)
        settings = node.settings or {}
        failed = failure is not None or (
            result.wait_until is None and result.port in _FAILURE_PORTS
        )
        if failed and _retry_left(run, node.id, settings):
            # Try the block again later (its own waiting state starts over).
            attempt = _count_attempt(run, node.id)
            delay = settings.get("retry_delay_seconds", 60)
            ctx.set_node_state(node.id, {})
            step.status = StepStatus.WAITING
            step.output = result.output
            step.message = f"Attempt {attempt} failed ({result.message}); trying again in {delay} s"
            run.status = RunStatus.WAITING
            run.wake_at = now + timedelta(seconds=delay)
            _set_queue(run, queue)
            db.commit()
            return
        if failure is not None and settings.get("on_error") == "output":
            ctx.set_context("last_error", failure)
            result = NodeResult("error", {"error": failure}, f"Error: {failure}")
        elif failure is not None:
            # The block's outputs are not followed, but other queued branches still run.
            step.status = StepStatus.FAILED
            step.message = failure
            step.finished_at = now
            done.add(node.id)
            queue = queue[1:]
            _set_queue(run, queue)
            db.commit()
            continue

        step.message = result.message
        step.output = result.output
        if result.wait_until is not None:
            step.status = StepStatus.WAITING
            run.status = RunStatus.WAITING
            run.wake_at = result.wait_until
            if result.port:
                following = graph.next_nodes(node.id, result.port)
                _set_queue(run, queue[1:] + [n for n in following if n not in done] + [node.id])
            else:
                _set_queue(run, queue)
            db.commit()
            return

        step.status = StepStatus.COMPLETED
        step.port = result.port
        step.finished_at = now
        done.add(node.id)
        following = graph.next_nodes(node.id, result.port or "")
        if not following:
            run.context = {**(run.context or {}), "outcome": f"{node.id}.{result.port}"}
        queue = queue[1:] + [n for n in following if n not in done and n not in queue]
        _set_queue(run, queue)
        db.commit()

    if queue:
        _finish(db, run, RunStatus.FAILED, now, error=f"Stopped after {max_steps} steps")
        return

    errors = [s.message or "Step failed" for s in run.steps if s.status == StepStatus.FAILED]
    if errors:
        _finish(db, run, RunStatus.FAILED, now, error="; ".join(errors))
    else:
        _finish(db, run, RunStatus.COMPLETED, now)


# Outputs a block takes when it did not succeed; with retries set, the block is tried again.
_FAILURE_PORTS = frozenset({"failed", "fail", "timeout"})


def _retry_left(run: WorkflowRun, node_id: str, settings: dict[str, Any]) -> bool:
    attempts = ((run.context or {}).get("attempts") or {}).get(node_id, 0)
    return attempts < settings.get("retries", 0)


def _count_attempt(run: WorkflowRun, node_id: str) -> int:
    attempts = dict((run.context or {}).get("attempts") or {})
    attempts[node_id] = attempts.get(node_id, 0) + 1
    run.context = {**(run.context or {}), "attempts": attempts}
    return attempts[node_id]


def _set_queue(run: WorkflowRun, queue: list[str]) -> None:
    run.pending_nodes = list(queue)
    if queue:
        run.current_node = queue[0]


def _candidates(db: Session, now: datetime) -> list[uuid.UUID]:
    resolved = select(Incident.id).where(Incident.status == IncidentStatus.RESOLVED)
    return list(
        db.scalars(
            select(WorkflowRun.id)
            .where(
                or_(
                    WorkflowRun.status == RunStatus.PENDING,
                    (WorkflowRun.status == RunStatus.WAITING)
                    & or_(WorkflowRun.wake_at <= now, WorkflowRun.incident_id.in_(resolved)),
                    # A RUNNING run whose claim expired was interrupted (e.g. worker crash).
                    (WorkflowRun.status == RunStatus.RUNNING) & (WorkflowRun.claimed_until < now),
                )
            )
            .order_by(WorkflowRun.created_at)
            .limit(TICK_BATCH)
        ).all()
    )


def run_now(db: Session, run_id: uuid.UUID, *, now: datetime | None = None) -> WorkflowRun | None:
    """Claim and advance one run. Returns None if someone else holds it."""
    now = now or utcnow()
    if not _claim(db, run_id, now, HOLDER):
        return None
    run = db.get(WorkflowRun, run_id)
    assert run is not None
    db.refresh(run)
    return advance(db, run, now)


def tick(db: Session, *, now: datetime | None = None) -> TickSummary:
    """Enqueue stale and scheduled triggers and advance every due run."""
    started = time.perf_counter()
    now = now or utcnow()
    summary = TickSummary()
    if not get_settings().AUTOMATION_ENABLED:
        return summary
    summary.enqueued = _enqueue_stale(db, now) + _enqueue_scheduled(db, now)
    for run_id in _candidates(db, now):
        try:
            run = run_now(db, run_id, now=now)
        except Exception as exc:  # one broken run must not stop the tick
            db.rollback()
            logger.exception("Automation tick failed for run %s", run_id)
            summary.errors.append({"run_id": str(run_id), "error": str(exc)[:300]})
            continue
        if run is None:
            continue
        summary.advanced += 1
        if run.status == RunStatus.COMPLETED:
            summary.completed += 1
        elif run.status == RunStatus.WAITING:
            summary.waiting += 1
        elif run.status == RunStatus.FAILED:
            summary.failed += 1
    summary.duration_ms = int((time.perf_counter() - started) * 1000)
    return summary


# ---------------------------------------------------------------------- runs


def list_runs(
    db: Session,
    *,
    statuses: list[RunStatus] | None,
    workflow_id: uuid.UUID | None,
    incident_id: uuid.UUID | None,
    limit: int,
    offset: int,
    trigger_events: list[str] | None = None,
) -> tuple[list[WorkflowRun], int]:
    query = select(WorkflowRun)
    if trigger_events:
        query = query.where(WorkflowRun.trigger_event.in_(trigger_events))
    if statuses:
        query = query.where(WorkflowRun.status.in_(statuses))
    if workflow_id:
        query = query.where(WorkflowRun.workflow_id == workflow_id)
    if incident_id:
        query = query.where(WorkflowRun.incident_id == incident_id)
    total = db.scalar(select(func.count()).select_from(query.subquery())) or 0
    items = db.scalars(
        query.options(selectinload(WorkflowRun.workflow), selectinload(WorkflowRun.incident))
        .order_by(WorkflowRun.created_at.desc(), WorkflowRun.id)
        .limit(limit)
        .offset(offset)
    ).all()
    return list(items), total


def get_run(db: Session, run_id: uuid.UUID) -> WorkflowRun:
    run = db.scalar(
        select(WorkflowRun)
        .where(WorkflowRun.id == run_id)
        .options(
            selectinload(WorkflowRun.steps),
            selectinload(WorkflowRun.approvals).selectinload(Approval.decider),
            selectinload(WorkflowRun.workflow),
            selectinload(WorkflowRun.incident),
        )
    )
    if run is None:
        raise NotFoundError("Workflow run not found")
    return run


def cancel_run(
    db: Session, run_id: uuid.UUID, *, actor: User, ip_address: str | None = None
) -> WorkflowRun:
    run = get_run(db, run_id)
    if run.status.is_terminal:
        raise ConflictError(
            f"The run is already {run.status.value.lower()}", code="invalid_transition"
        )
    now = utcnow()
    if run.status == RunStatus.RUNNING and run.claimed_until and run.claimed_until > now:
        raise ConflictError("The run is executing right now; try again shortly", code="run_busy")
    _finish(db, run, RunStatus.CANCELLED, now)
    audit_service.record(
        db,
        action="automation.run_cancelled",
        entity_type="workflow_run",
        entity_id=run.id,
        actor=actor,
        details={"workflow": run.workflow_name},
        ip_address=ip_address,
    )
    db.commit()
    return run


MAX_DELETE_RUNS = 200


def delete_runs(
    db: Session, run_ids: list[uuid.UUID], *, actor: User, ip_address: str | None = None
) -> int:
    """Delete finished runs with their steps, approvals and DAG run checks.

    Active runs (pending, running, waiting) must be cancelled first. Notifications a run sent
    are kept and unlinked. Each deletion is audited.
    """
    ids = list(dict.fromkeys(run_ids))
    if not ids:
        raise BadRequestError("Choose at least one run to delete")
    if len(ids) > MAX_DELETE_RUNS:
        raise BadRequestError(f"Delete at most {MAX_DELETE_RUNS} runs at a time")
    runs = db.scalars(
        select(WorkflowRun)
        .options(selectinload(WorkflowRun.workflow))
        .where(WorkflowRun.id.in_(ids))
    ).all()
    missing = len(ids) - len(runs)
    if missing:
        raise NotFoundError(f"{missing} run(s) not found" if len(ids) > 1 else "Run not found")
    active = [r for r in runs if not r.status.is_terminal]
    if active:
        raise ConflictError(
            f"{len(active)} run(s) are still active; cancel them before deleting",
            code="run_active",
            details={"run_ids": [str(r.id) for r in active]},
        )
    for run in runs:
        audit_service.record(
            db,
            action="automation.run_deleted",
            entity_type="workflow_run",
            entity_id=run.id,
            actor=actor,
            details={
                "workflow": run.workflow_name,
                "status": run.status.value,
                "trigger": run.trigger_event,
                "started_at": run.started_at.isoformat() if run.started_at else None,
            },
            ip_address=ip_address,
        )
        db.delete(run)
    db.commit()
    return len(runs)


# ---------------------------------------------------------------------- DAG run checks


def list_run_checks(
    db: Session,
    *,
    dag_id: str | None,
    statuses: list[DagRunCheckStatus] | None,
    limit: int,
    offset: int,
) -> tuple[list[DagRunCheck], int]:
    query = select(DagRunCheck)
    if dag_id:
        query = query.where(DagRunCheck.dag_id == dag_id)
    if statuses:
        query = query.where(DagRunCheck.status.in_(statuses))
    total = db.scalar(select(func.count()).select_from(query.subquery())) or 0
    items = db.scalars(
        query.options(selectinload(DagRunCheck.workflow_run).selectinload(WorkflowRun.workflow))
        .order_by(DagRunCheck.created_at.desc(), DagRunCheck.id)
        .limit(limit)
        .offset(offset)
    ).all()
    return list(items), total


def latest_checks(db: Session) -> dict[tuple[uuid.UUID, str], DagRunCheck]:
    """Newest check per (connection, DAG), for status badges on monitored DAGs."""
    newest = (
        select(
            DagRunCheck.connection_id,
            DagRunCheck.dag_id,
            func.max(DagRunCheck.created_at).label("created_at"),
        )
        .group_by(DagRunCheck.connection_id, DagRunCheck.dag_id)
        .subquery()
    )
    rows = db.scalars(
        select(DagRunCheck).join(
            newest,
            (DagRunCheck.connection_id == newest.c.connection_id)
            & (DagRunCheck.dag_id == newest.c.dag_id)
            & (DagRunCheck.created_at == newest.c.created_at),
        )
    ).all()
    return {(c.connection_id, c.dag_id): c for c in rows}


# ---------------------------------------------------------------------- approvals


def list_approvals(
    db: Session,
    *,
    statuses: list[ApprovalStatus] | None,
    incident_id: uuid.UUID | None = None,
    limit: int,
    offset: int,
) -> tuple[list[Approval], int]:
    query = select(Approval)
    if statuses:
        query = query.where(Approval.status.in_(statuses))
    if incident_id:
        query = query.where(Approval.incident_id == incident_id)
    total = db.scalar(select(func.count()).select_from(query.subquery())) or 0
    items = db.scalars(
        query.options(
            selectinload(Approval.decider),
            selectinload(Approval.incident),
            selectinload(Approval.run).selectinload(WorkflowRun.workflow),
        )
        .order_by(Approval.created_at.desc(), Approval.id)
        .limit(limit)
        .offset(offset)
    ).all()
    return list(items), total


def get_approval(db: Session, approval_id: uuid.UUID) -> Approval:
    approval = db.get(Approval, approval_id)
    if approval is None:
        raise NotFoundError("Approval not found")
    return approval


def decide(
    db: Session,
    approval_id: uuid.UUID,
    *,
    approve: bool,
    comment: str | None,
    actor: User,
    ip_address: str | None = None,
    now: datetime | None = None,
) -> Approval:
    """Approve or reject, then resume the waiting run right away."""
    now = now or utcnow()
    # Lock out a concurrent decision: only a PENDING, unexpired approval can change.
    status = ApprovalStatus.APPROVED if approve else ApprovalStatus.REJECTED
    result = db.execute(
        update(Approval)
        .where(
            Approval.id == approval_id,
            Approval.status == ApprovalStatus.PENDING,
            Approval.expires_at > now,
        )
        .values(status=status, decided_by=actor.id, decided_at=now, comment=comment)
    )
    if result.rowcount != 1:  # type: ignore[attr-defined]
        db.rollback()
        approval = get_approval(db, approval_id)
        raise ConflictError(
            f"This approval is already {approval.status.value.lower()}"
            if approval.status != ApprovalStatus.PENDING
            else "This approval has expired",
            code="approval_closed",
            details={"status": approval.status},
        )
    approval = get_approval(db, approval_id)
    db.refresh(approval)
    verb = "approved" if approve else "rejected"
    audit_service.record(
        db,
        action=f"approval.{verb}",
        entity_type="approval",
        entity_id=approval.id,
        actor=actor,
        details={"title": approval.title, "comment": comment, "run_id": str(approval.run_id)},
        ip_address=ip_address,
    )
    if approval.incident is not None:
        incident_service.add_event(
            db,
            approval.incident,
            verb,
            actor=actor,
            details={"approval_id": str(approval.id), "title": approval.title, "note": comment},
        )
    run = approval.run
    run.wake_at = now
    db.commit()
    run_now(db, run.id, now=now)  # if claimed elsewhere, the next tick resumes it
    db.refresh(approval)
    return approval


# ---------------------------------------------------------------------- notifications & summary


def list_notifications(
    db: Session, *, unread_only: bool, limit: int, offset: int
) -> tuple[list[Notification], int]:
    query = select(Notification)
    if unread_only:
        query = query.where(Notification.read_at.is_(None))
    total = db.scalar(select(func.count()).select_from(query.subquery())) or 0
    items = db.scalars(
        query.order_by(Notification.created_at.desc(), Notification.id).limit(limit).offset(offset)
    ).all()
    return list(items), total


def mark_all_read(db: Session) -> int:
    result = db.execute(
        update(Notification).where(Notification.read_at.is_(None)).values(read_at=utcnow())
    )
    db.commit()
    return result.rowcount  # type: ignore[attr-defined,no-any-return]


def summary(db: Session) -> dict[str, int]:
    return {
        "pending_approvals": db.scalar(
            select(func.count()).where(Approval.status == ApprovalStatus.PENDING)
        )
        or 0,
        "active_runs": db.scalar(
            select(func.count()).where(WorkflowRun.status.in_(ACTIVE_RUN_STATUSES))
        )
        or 0,
        "unread_notifications": db.scalar(
            select(func.count()).where(Notification.read_at.is_(None))
        )
        or 0,
        "enabled_workflows": db.scalar(select(func.count()).where(Workflow.enabled.is_(True))) or 0,
    }
