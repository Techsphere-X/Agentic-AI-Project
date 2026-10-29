"""Plan 2 / Phase 3: the automation engine end to end against a mock Airflow connection.

Time is controlled: the mock Airflow clock and the engine's `now` are the same Clock.
"""

import json
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import anyio
import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.automation.types import ApprovalStatus, RunStatus, TriggerEvent, WorkflowMode
from app.core.config import get_settings
from app.core.exceptions import ConflictError
from app.detection.types import (
    IncidentResolution,
    IncidentSeverity,
    IncidentStatus,
    IncidentType,
)
from app.models.airflow import (
    AirflowConnection,
    ConnectionKind,
    DeploymentEnvironment,
    MonitoredDag,
)
from app.models.audit_log import AuditLog
from app.models.automation import Approval, Notification, Workflow, WorkflowRun
from app.models.incident import Incident
from app.models.user import User
from app.orchestration.airflow import mock as mock_module
from app.orchestration.airflow.base import AdapterConfig, AuthType
from app.orchestration.airflow.mock import MOCK_DAGS, MockAirflowAdapter
from app.services import airflow_service, automation_nodes, automation_service, detection_service

BASE_URL = "http://mock-airflow"


class Clock:
    """30s into a schedule bucket whose previous run failed; shared by mock Airflow and engine."""

    def __init__(self, period_seconds: int, fail_every: int) -> None:
        current = int(datetime.now(UTC).timestamp()) // period_seconds
        bucket = current - ((current - 1) % fail_every)  # bucket-1 is a failing run
        self.now = datetime.fromtimestamp(bucket * period_seconds + 30, UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **delta: float) -> None:
        self.now += timedelta(**delta)


@pytest.fixture
def partner_clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    clock = Clock(600, 4)  # partner_api_sync: every 10 min, every 4th fails (transient)
    monkeypatch.setattr(mock_module, "clock", clock)
    return clock


@pytest.fixture
def orders_clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    clock = Clock(300, 3)  # orders_pipeline: every 5 min, every 3rd fails (bad data)
    monkeypatch.setattr(mock_module, "clock", clock)
    return clock


def make_env(
    db: Session, environment: DeploymentEnvironment, *dag_ids: str, sla: dict | None = None
) -> tuple[AirflowConnection, dict[str, MonitoredDag]]:
    conn = AirflowConnection(
        name=f"mock-{environment.lower()}",
        environment=environment,
        kind=ConnectionKind.MOCK,
        base_url=BASE_URL,
        auth_type=AuthType.NONE,
    )
    db.add(conn)
    db.flush()
    summaries = {d.dag_id: d for d in MOCK_DAGS}
    dags = {}
    for dag_id in dag_ids:
        dag = MonitoredDag(
            connection_id=conn.id,
            dag_id=dag_id,
            is_monitored=True,
            is_paused=summaries[dag_id].is_paused,
            tags=summaries[dag_id].tags,
            sla_minutes=(sla or {}).get(dag_id),
        )
        db.add(dag)
        dags[dag_id] = dag
    db.commit()
    return conn, dags


def enable(db: Session, key: str, mode: WorkflowMode = WorkflowMode.LIVE) -> Workflow:
    automation_service.seed_templates(db)
    workflow = db.scalar(select(Workflow).where(Workflow.key == key))
    assert workflow is not None
    workflow.enabled = True
    workflow.mode = mode
    db.commit()
    return workflow


def detect(db: Session, clock: Clock) -> detection_service.CycleSummary:
    summary = detection_service.run_cycle(db, now=clock())
    assert summary.errors == []
    return summary


def tick(db: Session, clock: Clock) -> automation_service.TickSummary:
    summary = automation_service.tick(db, now=clock())
    assert summary.errors == []
    db.expire_all()
    return summary


def only(db: Session, model: type) -> object:
    rows = db.scalars(select(model)).all()
    assert len(rows) == 1, rows
    return rows[0]


def ports(run: WorkflowRun) -> list[str]:
    return [f"{s.node_id}.{s.port or s.status.value.lower()}" for s in run.steps]


def events(incident: Incident) -> list[str]:
    return [e.event for e in incident.events]


def audit_actions(db: Session) -> set[str]:
    return set(db.scalars(select(AuditLog.action)).all())


def writes() -> mock_module._WriteState:
    return mock_module._state.get(BASE_URL, mock_module._WriteState())


def make_incident(
    db: Session,
    conn: AirflowConnection,
    dag: MonitoredDag,
    now: datetime,
    *,
    occurrences: int = 1,
    first_seen: datetime | None = None,
    status: IncidentStatus = IncidentStatus.OPEN,
    key: str = "",
) -> Incident:
    incident = Incident(
        id=uuid.uuid4(),
        connection_id=conn.id,
        monitored_dag_id=dag.id,
        dag_id=dag.dag_id,
        run_id="scheduled__x",
        last_run_id="scheduled__x",
        type=IncidentType.DAG_RUN_FAILED,
        severity=IncidentSeverity.MEDIUM,
        status=status,
        title=f"{dag.dag_id}: DAG run failed",
        fingerprint=f"DAG_RUN_FAILED:{conn.id}:{dag.dag_id}{key}",
        occurrence_count=occurrences,
        occurred_at=now,
        first_seen_at=first_seen or now,
        last_seen_at=now,
    )
    db.add(incident)
    db.commit()
    return incident


# ---------------------------------------------------------------------- happy path


def test_retry_transient_in_dev_fixes_and_resolves(db: Session, partner_clock: Clock) -> None:
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    enable(db, "retry-transient")

    summary = detect(db, partner_clock)
    assert (summary.opened, summary.automation_queued) == (1, 1)
    incident = only(db, Incident)
    run = only(db, WorkflowRun)
    assert run.status == RunStatus.PENDING

    tick(db, partner_clock)
    assert run.status == RunStatus.WAITING
    assert ports(run) == [
        "trigger.next",
        "classify.next",
        "is_retryable.true",
        "approval.approved",
        "retry.success",
        "verify.waiting",
    ]
    assert run.context["diagnosis"]["category"] == "TRANSIENT_NETWORK"
    assert run.steps[3].output["auto"] == "not_required"
    assert list(writes().cleared) == [("partner_api_sync", incident.last_run_id)]

    partner_clock.advance(seconds=mock_module.RERUN_SECONDS + 1)
    tick(db, partner_clock)
    assert run.status == RunStatus.COMPLETED
    assert run.context["outcome"] == "notify_ok.next"
    assert incident.status == IncidentStatus.RESOLVED
    assert incident.resolution == IncidentResolution.AUTO_REMEDIATED
    assert "Network glitch" in (incident.resolution_note or "")
    for event in ("automation_started", "diagnosed", "action_executed", "verified"):
        assert event in events(incident)
    assert events(incident)[-1] == "automation_finished"
    note = only(db, Notification)
    assert note.title.startswith("Fixed automatically")
    assert {
        "automation.run_started",
        "automation.action_executed",
        "incident.auto_remediated",
        "automation.run_finished",
    } <= audit_actions(db)

    # Nothing new on the next detection cycle.
    assert detect(db, partner_clock).automation_queued == 0


def test_newer_failure_during_the_run_keeps_the_incident_open(
    db: Session, partner_clock: Clock
) -> None:
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    enable(db, "retry-transient")
    detect(db, partner_clock)
    incident = only(db, Incident)
    run = only(db, WorkflowRun)
    tick(db, partner_clock)
    assert run.status == RunStatus.WAITING  # retried, verifying

    # The DAG fails again (another run) while the retry is being verified.
    partner_clock.advance(seconds=10)
    incident.occurrence_count += 1
    incident.last_seen_at = partner_clock()
    db.commit()

    partner_clock.advance(seconds=mock_module.RERUN_SECONDS + 1)
    tick(db, partner_clock)
    assert run.status == RunStatus.COMPLETED
    resolve = next(s for s in run.steps if s.node_id == "resolve")
    assert resolve.output == {"skipped": True, "reason": "newer failure"}
    assert incident.status == IncidentStatus.OPEN
    assert incident.resolution is None
    assert "automation_note" in events(incident)


# ---------------------------------------------------------------------- approvals


@pytest.fixture
def prod_waiting(db: Session, partner_clock: Clock) -> tuple[Incident, WorkflowRun, Approval]:
    make_env(db, DeploymentEnvironment.PROD, "partner_api_sync")
    enable(db, "retry-transient")
    detect(db, partner_clock)
    tick(db, partner_clock)
    run = only(db, WorkflowRun)
    approval = only(db, Approval)
    assert run.status == RunStatus.WAITING and approval.status == ApprovalStatus.PENDING
    assert "retry the failed tasks" in approval.title
    assert "Network glitch" in (approval.summary or "")
    assert approval.proposed_action["type"] == "action.clear_failed_tasks"
    assert writes().cleared == {}  # nothing happened in PROD yet
    assert only(db, Notification).title.startswith("Approval needed")
    return only(db, Incident), run, approval


def test_prod_waits_for_approval_then_fixes(
    db: Session, prod_waiting: tuple, partner_clock: Clock, operator: User
) -> None:
    incident, run, approval = prod_waiting
    automation_service.decide(
        db, approval.id, approve=True, comment="go", actor=operator, now=partner_clock()
    )
    db.expire_all()
    assert approval.status == ApprovalStatus.APPROVED and approval.decided_by == operator.id
    assert run.status == RunStatus.WAITING and ports(run)[-2:] == [
        "retry.success",
        "verify.waiting",
    ]
    assert run.context["approval_granted"] is True
    assert writes().cleared

    partner_clock.advance(seconds=61)
    tick(db, partner_clock)
    assert run.status == RunStatus.COMPLETED
    assert incident.resolution == IncidentResolution.AUTO_REMEDIATED
    assert {"approval.requested", "approval.approved"} <= audit_actions(db)
    approved_event = next(e for e in incident.events if e.event == "approved")
    assert approved_event.actor_user_id == operator.id


def test_prod_rejection_takes_no_action(
    db: Session, prod_waiting: tuple, partner_clock: Clock, operator: User
) -> None:
    incident, run, approval = prod_waiting
    automation_service.decide(
        db, approval.id, approve=False, comment="not now", actor=operator, now=partner_clock()
    )
    db.expire_all()
    assert run.status == RunStatus.COMPLETED
    assert run.context["outcome"] == "approval.rejected"
    assert incident.status == IncidentStatus.OPEN
    assert writes().cleared == {}
    with pytest.raises(ConflictError):
        automation_service.decide(
            db, approval.id, approve=True, comment=None, actor=operator, now=partner_clock()
        )


def test_approval_expires(db: Session, prod_waiting: tuple, partner_clock: Clock) -> None:
    _, run, approval = prod_waiting
    partner_clock.advance(minutes=61)
    tick(db, partner_clock)
    assert approval.status == ApprovalStatus.EXPIRED
    assert run.status == RunStatus.COMPLETED and run.context["outcome"] == "approval.rejected"
    assert "approval.expired" in audit_actions(db)
    assert writes().cleared == {}


def test_cancel_expires_pending_approval(
    db: Session, prod_waiting: tuple, partner_clock: Clock, operator: User
) -> None:
    _, run, approval = prod_waiting
    automation_service.cancel_run(db, run.id, actor=operator)
    db.expire_all()
    assert run.status == RunStatus.CANCELLED and approval.status == ApprovalStatus.EXPIRED
    with pytest.raises(ConflictError):
        automation_service.cancel_run(db, run.id, actor=operator)
    with pytest.raises(ConflictError):
        automation_service.decide(
            db, approval.id, approve=True, comment=None, actor=operator, now=partner_clock()
        )


def test_resolved_incident_stops_waiting_run(
    db: Session, prod_waiting: tuple, partner_clock: Clock
) -> None:
    incident, run, approval = prod_waiting
    incident.status = IncidentStatus.RESOLVED  # e.g. auto-recovered by detection
    db.commit()
    tick(db, partner_clock)
    assert run.status == RunStatus.COMPLETED
    assert run.context["outcome"] == "incident_resolved"
    assert approval.status == ApprovalStatus.EXPIRED


# ---------------------------------------------------------------------- safety


def test_data_errors_are_not_retried(db: Session, orders_clock: Clock) -> None:
    make_env(db, DeploymentEnvironment.DEV, "orders_pipeline")
    enable(db, "retry-transient")
    enable(db, "no-retry-data-code")
    assert detect(db, orders_clock).automation_queued == 2
    tick(db, orders_clock)

    runs = {r.workflow.key: r for r in db.scalars(select(WorkflowRun)).all()}
    assert runs["retry-transient"].context["outcome"] == "is_retryable.false"
    assert runs["no-retry-data-code"].context["outcome"] == "notify.next"
    assert all(r.status == RunStatus.COMPLETED for r in runs.values())
    incident = only(db, Incident)
    assert incident.status == IncidentStatus.OPEN
    note = next(e for e in incident.events if e.event == "automation_note")
    assert "Bad data" in note.details["note"] and "UniqueViolation" in note.details["note"]
    assert "hunter2" not in json.dumps(note.details)  # the log line is scrubbed evidence
    notification = only(db, Notification)
    assert notification.title.startswith("Needs a fix")
    assert writes().cleared == {}


def test_dry_run_changes_nothing(db: Session, partner_clock: Clock) -> None:
    make_env(db, DeploymentEnvironment.PROD, "partner_api_sync")
    enable(db, "retry-transient", WorkflowMode.DRY_RUN)
    detect(db, partner_clock)
    tick(db, partner_clock)
    run = only(db, WorkflowRun)
    assert run.dry_run and run.status == RunStatus.COMPLETED
    assert run.steps[3].output["auto"] == "dry_run"  # no approval requested
    assert run.steps[4].output["simulated"] is True
    assert writes().cleared == {}
    incident = only(db, Incident)
    assert incident.status == IncidentStatus.OPEN
    assert "action_simulated" in events(incident)
    assert only(db, Notification).title.startswith("[Dry run]")
    assert db.scalars(select(Approval)).all() == []


def test_force_dry_run_setting(
    db: Session, partner_clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "AUTOMATION_FORCE_DRY_RUN", True)
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    enable(db, "retry-transient")
    detect(db, partner_clock)
    tick(db, partner_clock)
    assert only(db, WorkflowRun).dry_run is True
    assert writes().cleared == {}


def test_rate_limit_blocks_and_escalates(
    db: Session, partner_clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "AUTOMATION_MAX_ACTIONS_PER_DAG_PER_DAY", 0)
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    enable(db, "retry-transient")
    detect(db, partner_clock)
    tick(db, partner_clock)
    run = only(db, WorkflowRun)
    assert "retry.failed" in ports(run) and run.context["outcome"] == "notify_fail.next"
    assert "Rate limit" in run.steps[4].output["policy"]["reasons"][0]
    incident = only(db, Incident)
    escalated = next(e for e in incident.events if e.event == "escalated")
    before, after = (IncidentSeverity(escalated.details["severity"][k]) for k in ("from", "to"))
    assert after.rank == before.rank + 1 and incident.severity == after
    assert {"automation.action_denied", "incident.escalated"} <= audit_actions(db)
    assert "Rate limit" in only(db, Notification).body
    assert writes().cleared == {}


def test_prod_action_without_approval_step_is_denied(
    db: Session, partner_clock: Clock, admin: User
) -> None:
    make_env(db, DeploymentEnvironment.PROD, "partner_api_sync")
    workflow = automation_service.create_workflow(
        db,
        actor=admin,
        name="Unsafe retry",
        graph={
            "nodes": [
                {"id": "t", "type": "trigger.incident", "config": {}},
                {"id": "retry", "type": "action.clear_failed_tasks"},
            ],
            "edges": [{"from": "t", "port": "next", "to": "retry"}],
        },
    )
    workflow.enabled = True
    db.commit()
    detect(db, partner_clock)
    tick(db, partner_clock)
    run = only(db, WorkflowRun)
    assert ports(run) == ["t.next", "retry.failed"]
    assert "require an approved approval" in run.steps[1].output["policy"]["reasons"][0]
    assert writes().cleared == {}


def test_actions_count_toward_the_daily_limit(
    db: Session, partner_clock: Clock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "AUTOMATION_MAX_ACTIONS_PER_DAG_PER_DAY", 1)
    conn, dags = make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    enable(db, "retry-transient")
    detect(db, partner_clock)
    tick(db, partner_clock)  # 1st action executes
    # A second incident on the same DAG within 24h is blocked.
    first = only(db, Incident)
    first.status = IncidentStatus.RESOLVED
    db.commit()
    second = make_incident(db, conn, dags["partner_api_sync"], partner_clock())
    second.evidence = list(first.evidence)  # same transient log
    db.commit()
    automation_service.enqueue_for_incident(db, second, TriggerEvent.OPENED, now=partner_clock())
    db.commit()
    tick(db, partner_clock)
    run = db.scalars(select(WorkflowRun).where(WorkflowRun.incident_id == second.id)).one()
    assert "retry.failed" in ports(run)


# ---------------------------------------------------------------------- other templates


def _unpause(dag_id: str) -> None:
    adapter = MockAirflowAdapter(AdapterConfig(base_url=BASE_URL), simulate_latency=False)
    anyio.run(adapter.set_dag_paused, dag_id, False)


def test_sla_rerun_triggers_a_run(db: Session, partner_clock: Clock) -> None:
    _unpause("legacy_inventory_sync")
    make_env(
        db, DeploymentEnvironment.DEV, "legacy_inventory_sync", sla={"legacy_inventory_sync": 60}
    )
    enable(db, "sla-rerun")
    assert detect(db, partner_clock).automation_queued == 1
    tick(db, partner_clock)
    run = only(db, WorkflowRun)
    assert ports(run)[:3] == ["trigger.next", "state.ready", "approval.approved"]
    assert run.status == RunStatus.WAITING
    assert len(writes().triggered["legacy_inventory_sync"]) == 1

    partner_clock.advance(seconds=61)
    tick(db, partner_clock)
    assert run.status == RunStatus.COMPLETED
    assert only(db, Incident).resolution == IncidentResolution.AUTO_REMEDIATED


def test_sla_rerun_on_paused_dag_only_notifies(db: Session, partner_clock: Clock) -> None:
    make_env(
        db, DeploymentEnvironment.DEV, "legacy_inventory_sync", sla={"legacy_inventory_sync": 60}
    )
    enable(db, "sla-rerun")
    detect(db, partner_clock)
    tick(db, partner_clock)
    assert only(db, WorkflowRun).context["outcome"] == "notify_paused.next"
    assert "paused" in only(db, Notification).title
    assert writes().triggered == {}


def test_active_dag_trigger_is_deduplicated_across_workflow_runs(
    db: Session, partner_clock: Clock, admin: User
) -> None:
    conn, dags = make_env(db, DeploymentEnvironment.DEV, "legacy_inventory_sync")
    workflow = automation_service.create_workflow(
        db,
        actor=admin,
        name="Trigger once",
        graph={
            "nodes": [
                {"id": "trigger", "type": "trigger.incident", "config": {"events": ["opened"]}},
                {
                    "id": "run",
                    "type": "action.trigger_dag_run",
                    "config": {},
                },
            ],
            "edges": [{"from": "trigger", "port": "next", "to": "run"}],
        },
    )
    workflow.enabled = True
    db.commit()
    first = make_incident(db, conn, dags["legacy_inventory_sync"], partner_clock(), key=":first")
    second = make_incident(db, conn, dags["legacy_inventory_sync"], partner_clock(), key=":second")
    automation_service.enqueue_for_incident(db, first, TriggerEvent.OPENED, now=partner_clock())
    automation_service.enqueue_for_incident(db, second, TriggerEvent.OPENED, now=partner_clock())
    db.commit()

    tick(db, partner_clock)

    assert len(writes().triggered["legacy_inventory_sync"]) == 1
    runs = db.scalars(select(WorkflowRun).order_by(WorkflowRun.created_at)).all()
    assert runs[0].status == RunStatus.COMPLETED
    assert runs[1].status == RunStatus.COMPLETED
    assert runs[1].steps[-1].port == "failed"
    assert "active" in (runs[1].steps[-1].output.get("error", "").lower())


def test_terminal_reserved_run_allows_retry_but_active_duplicate_is_blocked(
    db: Session, partner_clock: Clock, admin: User
) -> None:
    conn, _ = make_env(db, DeploymentEnvironment.DEV, "legacy_inventory_sync")
    workflow = automation_service.create_workflow(
        db,
        actor=admin,
        name="Trigger retry",
        graph={"nodes": [{"id": "trigger", "type": "trigger.manual", "config": {}}], "edges": []},
    )
    first_run = WorkflowRun(
        id=uuid.uuid4(),
        workflow_id=workflow.id,
        workflow_version=workflow.version,
        graph=workflow.graph,
        status=RunStatus.RUNNING,
        trigger_event=TriggerEvent.MANUAL.value,
        dedup_key=f"retry:{uuid.uuid4()}",
    )
    second_run = WorkflowRun(
        id=uuid.uuid4(),
        workflow_id=workflow.id,
        workflow_version=workflow.version,
        graph=workflow.graph,
        status=RunStatus.RUNNING,
        trigger_event=TriggerEvent.MANUAL.value,
        dedup_key=f"retry:{uuid.uuid4()}",
    )
    db.add_all([first_run, second_run])
    db.flush()
    adapter = airflow_service.adapter_for(conn)

    def context(run: WorkflowRun) -> SimpleNamespace:
        return SimpleNamespace(
            db=db,
            run=run,
            now=partner_clock(),
            settings=get_settings(),
            airflow_adapter=lambda _: adapter,
        )

    first = automation_nodes._trigger_dag_once(
        context(first_run),
        node_id="run",
        conn=conn,
        dag_id="legacy_inventory_sync",
        note="first",
    )
    partner_clock.advance(seconds=61)
    retry = automation_nodes._trigger_dag_once(
        context(first_run),
        node_id="run",
        conn=conn,
        dag_id="legacy_inventory_sync",
        note="retry",
    )

    assert first["run_id"] != retry["run_id"]
    assert len(writes().triggered["legacy_inventory_sync"]) == 2
    with pytest.raises(automation_nodes.ActionFailed, match="active trigger"):
        automation_nodes._trigger_dag_once(
            context(second_run),
            node_id="run",
            conn=conn,
            dag_id="legacy_inventory_sync",
            note="duplicate",
        )



def test_flapping_breaker_pauses_after_approval(
    db: Session, orders_clock: Clock, operator: User
) -> None:
    conn, dags = make_env(db, DeploymentEnvironment.DEV, "orders_pipeline")
    enable(db, "flapping-breaker")
    incident = make_incident(db, conn, dags["orders_pipeline"], orders_clock(), occurrences=3)
    automation_service.enqueue_for_incident(db, incident, TriggerEvent.RECURRED, now=orders_clock())
    db.commit()
    tick(db, orders_clock)
    run = only(db, WorkflowRun)
    assert run.status == RunStatus.WAITING  # approval required even in DEV
    assert incident.severity == IncidentSeverity.HIGH
    approval = only(db, Approval)
    assert approval.title.endswith("pause orders_pipeline")

    automation_service.decide(
        db, approval.id, approve=True, comment=None, actor=operator, now=orders_clock()
    )
    db.expire_all()
    assert run.status == RunStatus.COMPLETED and run.context["outcome"] == "note.next"
    assert writes().paused == {"orders_pipeline": True}
    assert dags["orders_pipeline"].is_paused is True


def test_flapping_breaker_ignores_first_failures(db: Session, orders_clock: Clock) -> None:
    conn, dags = make_env(db, DeploymentEnvironment.DEV, "orders_pipeline")
    enable(db, "flapping-breaker")
    incident = make_incident(db, conn, dags["orders_pipeline"], orders_clock(), occurrences=2)
    automation_service.enqueue_for_incident(db, incident, TriggerEvent.RECURRED, now=orders_clock())
    db.commit()
    tick(db, orders_clock)
    assert only(db, WorkflowRun).context["outcome"] == "flapping.false"
    assert db.scalars(select(Approval)).all() == []


def test_stale_escalation(db: Session, orders_clock: Clock) -> None:
    conn, dags = make_env(db, DeploymentEnvironment.DEV, "orders_pipeline")
    enable(db, "stale-escalation")
    now = orders_clock()
    old = make_incident(
        db, conn, dags["orders_pipeline"], now, first_seen=now - timedelta(minutes=31)
    )
    fresh = make_incident(
        db, conn, dags["orders_pipeline"], now, first_seen=now - timedelta(minutes=5), key=":2"
    )
    acked = make_incident(
        db,
        conn,
        dags["orders_pipeline"],
        now,
        first_seen=now - timedelta(hours=2),
        status=IncidentStatus.ACKNOWLEDGED,
        key=":3",
    )

    summary = tick(db, orders_clock)
    assert summary.enqueued == 1 and summary.completed == 1
    assert old.severity == IncidentSeverity.HIGH
    assert fresh.severity == acked.severity == IncidentSeverity.MEDIUM
    assert "Severity raised to HIGH" in only(db, Notification).body
    assert tick(db, orders_clock).enqueued == 0  # once per incident


# ---------------------------------------------------------------------- mechanics


def test_enqueue_is_deduplicated_and_claims_are_exclusive(db: Session, orders_clock: Clock) -> None:
    conn, dags = make_env(db, DeploymentEnvironment.DEV, "orders_pipeline")
    enable(db, "retry-transient")
    incident = make_incident(db, conn, dags["orders_pipeline"], orders_clock())
    first = automation_service.enqueue_for_incident(db, incident, TriggerEvent.OPENED)
    db.commit()
    assert len(first) == 1
    assert automation_service.enqueue_for_incident(db, incident, TriggerEvent.OPENED) == []
    assert automation_service.enqueue_for_incident(db, incident, TriggerEvent.STALE) == []

    now = orders_clock()
    assert automation_service._claim(db, first[0].id, now, "worker-a")
    assert not automation_service._claim(db, first[0].id, now, "worker-b")
    # tick skips a run claimed elsewhere
    assert tick(db, orders_clock).advanced == 0
    later = now + timedelta(seconds=automation_service.CLAIM_SECONDS + 1)
    assert automation_service._claim(db, first[0].id, later, "worker-b")  # claim expired


def test_disabled_workflows_do_not_run(db: Session, partner_clock: Clock) -> None:
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    automation_service.seed_templates(db)
    assert detect(db, partner_clock).automation_queued == 0


def test_graph_snapshot_is_used_for_running_runs(
    db: Session, prod_waiting: tuple, admin: User
) -> None:
    _, run, _ = prod_waiting
    workflow = run.workflow
    automation_service.update_workflow(
        db,
        workflow.id,
        {
            "graph": {
                "nodes": [{"id": "t", "type": "trigger.incident", "config": {}}],
                "edges": [],
            }
        },
        actor=admin,
    )
    db.expire_all()
    assert workflow.version == 2
    assert run.workflow_version == 1 and any(n["id"] == "retry" for n in run.graph["nodes"])


def test_webhook_notification(
    db: Session, partner_clock: Clock, admin: User, monkeypatch: pytest.MonkeyPatch
) -> None:
    received: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received.append({"url": str(request.url), **json.loads(request.content)})
        return httpx.Response(200)

    monkeypatch.setattr(automation_nodes, "webhook_transport", httpx.MockTransport(handler))
    make_env(db, DeploymentEnvironment.DEV, "partner_api_sync")
    workflow = automation_service.create_workflow(
        db,
        actor=admin,
        name="Webhook",
        graph={
            "nodes": [
                {"id": "t", "type": "trigger.incident", "config": {}},
                {"id": "c", "type": "diagnose.classify_log"},
                {
                    "id": "hook",
                    "type": "notify",
                    "config": {
                        "channel": "webhook",
                        "url": "https://hooks.test/abc",
                        "title": "{{incident.dag_id}}: {{diagnosis.label}}",
                    },
                },
                {
                    "id": "blocked",
                    "type": "notify",
                    "config": {"channel": "webhook", "url": "https://evil.test/x"},
                },
            ],
            "edges": [
                {"from": "t", "port": "next", "to": "c"},
                {"from": "c", "port": "next", "to": "hook"},
                {"from": "hook", "port": "next", "to": "blocked"},
            ],
        },
    )
    workflow.enabled = True
    db.commit()
    monkeypatch.setattr(get_settings(), "AUTOMATION_WEBHOOK_ALLOWED_HOSTS", ["hooks.test"])
    detect(db, partner_clock)
    tick(db, partner_clock)

    assert len(received) == 1  # evil.test was blocked
    payload = received[0]
    assert payload["title"] == "partner_api_sync: Network glitch"
    assert payload["incident"]["dag_id"] == "partner_api_sync"
    assert payload["diagnosis"]["category"] == "TRANSIENT_NETWORK"
    run = only(db, WorkflowRun)
    assert run.status == RunStatus.COMPLETED
    assert run.steps[-1].output["error"] == "Webhook host is not allowed"
