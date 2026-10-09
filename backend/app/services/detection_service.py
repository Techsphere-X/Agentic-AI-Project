"""One detection cycle: poll Airflow for monitored DAGs, open/recur/auto-resolve incidents."""

import logging
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta

from sqlalchemy import select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.automation.types import TriggerEvent
from app.core.config import get_settings
from app.core.exceptions import AppError
from app.db.base import utcnow
from app.detection import evidence, rules, severity
from app.detection.evidence import EvidenceRecord
from app.detection.types import EvidenceKind, IncidentResolution, IncidentType
from app.models.airflow import AirflowConnection, MonitoredDag
from app.models.incident import DetectionLease, Incident, IncidentEvidence
from app.models.user import User
from app.orchestration.airflow.base import (
    AirflowAdapter,
    AirflowAdapterError,
    AirflowDagRun,
)
from app.orchestration.airflow.factory import run_async
from app.services import (
    airflow_service,
    audit_service,
    automation_service,
    diagnosis_service,
    incident_service,
)
from app.services.incident_service import OPEN_STATUSES

logger = logging.getLogger(__name__)

LEASE_NAME = "detector"
RUN_LIMIT = 100


@dataclass
class CycleSummary:
    trigger: str
    started_at: datetime
    finished_at: datetime | None = None
    duration_ms: int = 0
    connections: int = 0
    dags: int = 0
    opened: int = 0
    recurred: int = 0
    auto_resolved: int = 0
    automation_queued: int = 0
    errors: list[dict[str, str]] = field(default_factory=list)
    automation: dict[str, object] | None = None  # tick summary, set by the scheduler

    def as_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["started_at"] = self.started_at.isoformat()
        data["finished_at"] = self.finished_at.isoformat() if self.finished_at else None
        return data


# ---------------------------------------------------------------------- lease


def try_acquire_lease(
    db: Session, holder: str, ttl_seconds: int, now: datetime, *, name: str = LEASE_NAME
) -> bool:
    """Atomically take (or renew) the named lease (`detector`, `automation`). Process-safe."""
    expires = now + timedelta(seconds=ttl_seconds)
    result = db.execute(
        update(DetectionLease)
        .where(
            DetectionLease.name == name,
            (DetectionLease.expires_at < now) | (DetectionLease.holder == holder),
        )
        .values(holder=holder, expires_at=expires, renewed_at=now)
    )
    if result.rowcount == 1:  # type: ignore[attr-defined]
        db.commit()
        return True
    exists = db.scalar(select(DetectionLease.name).where(DetectionLease.name == name))
    if exists:
        db.rollback()
        return False
    try:
        db.add(DetectionLease(name=name, holder=holder, expires_at=expires, renewed_at=now))
        db.commit()
        return True
    except IntegrityError:
        db.rollback()
        return False


def get_lease(db: Session, name: str = LEASE_NAME) -> DetectionLease | None:
    return db.get(DetectionLease, name)


# ---------------------------------------------------------------------- helpers


def _fingerprint(incident_type: IncidentType, conn: AirflowConnection, dag: MonitoredDag) -> str:
    return f"{incident_type.value}:{conn.id}:{dag.dag_id}"


def _open_incident(db: Session, fingerprint: str) -> Incident | None:
    return db.scalar(
        select(Incident).where(
            Incident.fingerprint == fingerprint, Incident.status.in_(OPEN_STATUSES)
        )
    )


def _run_already_recorded(db: Session, conn: AirflowConnection, dag_id: str, run_id: str) -> bool:
    """Idempotency: a failed run is attached to at most one incident, ever."""
    return (
        db.scalar(
            select(IncidentEvidence.id)
            .join(Incident, Incident.id == IncidentEvidence.incident_id)
            .where(
                Incident.connection_id == conn.id,
                Incident.dag_id == dag_id,
                IncidentEvidence.kind == EvidenceKind.RUN_METADATA,
                IncidentEvidence.source == evidence.run_source(run_id),
            )
            .limit(1)
        )
        is not None
    )


def _collect_failure_evidence(
    adapter: AirflowAdapter, dag_id: str, run: AirflowDagRun
) -> tuple[list[EvidenceRecord], str]:
    settings = get_settings()
    records = [evidence.run_metadata(run)]
    try:
        instances = run_async(lambda: adapter.list_task_instances(dag_id, run.run_id))
    except AirflowAdapterError as exc:
        records.append(
            evidence.unavailable(run.run_id, EvidenceKind.TASK_INSTANCE, exc.result.message)
        )
        return records, evidence.failure_summary(run, [])

    ti_record = evidence.task_instances(run.run_id, instances)
    if ti_record:
        records.append(ti_record)
    failed = [ti for ti in instances if ti.state == "failed"][: settings.EVIDENCE_MAX_LOGS]
    for ti in failed:
        try:
            log = run_async(
                lambda ti=ti: adapter.get_task_log(
                    dag_id,
                    run.run_id,
                    ti.task_id,
                    ti.try_number,
                    max_bytes=settings.EVIDENCE_LOG_MAX_BYTES,
                )
            )
            records.append(evidence.task_log(run.run_id, ti, log))
        except AirflowAdapterError as exc:
            logger.warning(
                "Could not fetch log for %s/%s/%s: %r",
                dag_id,
                run.run_id,
                ti.task_id,
                exc.__cause__,
            )
            records.append(
                evidence.unavailable(run.run_id, EvidenceKind.TASK_LOG, exc.result.message)
            )
    return records, evidence.failure_summary(run, instances)


def _attach(db: Session, incident: Incident, records: list[EvidenceRecord], now: datetime) -> None:
    for record in records:
        db.add(
            IncidentEvidence(
                incident_id=incident.id,
                kind=record.kind,
                source=record.source,
                content=record.content,
                data=record.data,
                truncated=record.truncated,
                collected_at=now,
            )
        )
    if records:
        incident_service.add_event(
            db,
            incident,
            "evidence_added",
            details={"sources": [r.source for r in records]},
        )
    if any(r.kind == EvidenceKind.TASK_LOG and r.content for r in records):
        # Diagnose once, when the evidence arrives; pages and workflows read the stored result.
        diagnosis_service.diagnose_and_store(db, incident)


# ---------------------------------------------------------------------- per-DAG processing


def _handle_finding(
    db: Session,
    *,
    conn: AirflowConnection,
    dag: MonitoredDag,
    finding: rules.Finding,
    score: severity.SeverityScore,
    records: list[EvidenceRecord],
    summary_text: str,
    now: datetime,
    summary: CycleSummary,
) -> None:
    fingerprint = _fingerprint(finding.type, conn, dag)
    incident = _open_incident(db, fingerprint)

    if incident is not None:
        if finding.type == IncidentType.SLA_MISSED:
            incident.last_seen_at = now  # still breached; not a new occurrence
            return
        incident.occurrence_count += 1
        incident.last_seen_at = now
        incident.occurred_at = finding.occurred_at
        incident.last_run_id = finding.run_id
        incident.summary = summary_text
        escalated = severity.max_severity(incident.severity, score.severity)
        details: dict[str, object] = {"run_id": finding.run_id, "count": incident.occurrence_count}
        if escalated != incident.severity:
            details["severity"] = {"from": incident.severity, "to": escalated}
            details["severity_reasons"] = score.reasons
            incident.severity = escalated
        incident_service.add_event(db, incident, "recurred", details=details)
        _attach(db, incident, records, now)
        summary.recurred += 1
        _enqueue(db, incident, TriggerEvent.RECURRED, now, summary)
        return

    if finding.type == IncidentType.DAG_RUN_FAILED:
        title = f"{dag.dag_id}: DAG run failed"
    else:
        title = f"{dag.dag_id}: no successful run within {finding.sla_minutes} min SLA"
    incident = Incident(
        id=uuid.uuid4(),
        connection_id=conn.id,
        monitored_dag_id=dag.id,
        dag_id=dag.dag_id,
        run_id=finding.run_id,
        last_run_id=finding.run_id,
        type=finding.type,
        severity=score.severity,
        title=title,
        summary=summary_text,
        fingerprint=fingerprint,
        occurred_at=finding.occurred_at,
        first_seen_at=now,
        last_seen_at=now,
    )
    db.add(incident)
    db.flush()  # makes the partial unique index apply to later findings in this batch
    incident_service.add_event(
        db,
        incident,
        "opened",
        details={
            "severity": incident.severity,
            "severity_reasons": score.reasons,
            "run_id": finding.run_id,
        },
    )
    _attach(db, incident, records, now)
    audit_service.record(
        db,
        action="incident.opened",
        entity_type="incident",
        entity_id=incident.id,
        details={
            "dag_id": dag.dag_id,
            "type": finding.type,
            "severity": incident.severity,
            "run_id": finding.run_id,
        },
    )
    summary.opened += 1
    _enqueue(db, incident, TriggerEvent.OPENED, now, summary)


def _enqueue(
    db: Session, incident: Incident, event: TriggerEvent, now: datetime, summary: CycleSummary
) -> None:
    """Queue automation runs in this transaction; the automation tick executes them."""
    db.flush()
    summary.automation_queued += len(
        automation_service.enqueue_for_incident(db, incident, event, now=now)
    )


def _process_dag(
    db: Session,
    adapter: AirflowAdapter,
    conn: AirflowConnection,
    dag: MonitoredDag,
    runs: list[AirflowDagRun],
    now: datetime,
    summary: CycleSummary,
) -> None:
    recent_failures = rules.count_recent_failures(runs, now)

    failures = rules.detect_failed_runs(dag.dag_id, runs, dag.detection_watermark)
    for finding in failures if dag.detect_failures else []:
        assert finding.run is not None
        if _run_already_recorded(db, conn, dag.dag_id, finding.run.run_id):
            continue
        records, text = _collect_failure_evidence(adapter, dag.dag_id, finding.run)
        score = severity.score(
            finding.type,
            environment=conn.environment,
            tags=dag.tags,
            recent_failures=recent_failures,
        )
        _handle_finding(
            db,
            conn=conn,
            dag=dag,
            finding=finding,
            score=score,
            records=records,
            summary_text=text,
            now=now,
            summary=summary,
        )

    sla_finding = rules.detect_sla_miss(dag.dag_id, runs, dag.sla_minutes, now)
    if sla_finding is not None:
        last_ok = sla_finding.run
        records = [evidence.run_metadata(last_ok, note="last successful run")] if last_ok else []
        text = (
            f"Last successful run finished at {(last_ok.end_date or last_ok.sort_date).isoformat()}"  # type: ignore[union-attr]
            if last_ok
            else f"No successful run in the last {get_settings().DETECTION_LOOKBACK_HOURS}h"
        ) + f"; SLA is {dag.sla_minutes} min."
        score = severity.score(
            sla_finding.type, environment=conn.environment, tags=dag.tags, recent_failures=0
        )
        _handle_finding(
            db,
            conn=conn,
            dag=dag,
            finding=sla_finding,
            score=score,
            records=records,
            summary_text=text,
            now=now,
            summary=summary,
        )

    open_incidents = db.scalars(
        select(Incident).where(
            Incident.connection_id == conn.id,
            Incident.dag_id == dag.dag_id,
            Incident.status.in_(OPEN_STATUSES),
        )
    ).all()
    refs = [rules.OpenIncidentRef(i.id, i.type, i.occurred_at) for i in open_incidents]
    by_id = {i.id: i for i in open_incidents}
    for recovery in rules.find_recoveries(runs, refs, sla_minutes=dag.sla_minutes, now=now):
        incident = by_id[recovery.incident_id]
        incident_service.mark_resolved(incident, IncidentResolution.AUTO_RECOVERED)
        run_id = recovery.run.run_id if recovery.run else None
        incident_service.add_event(
            db, incident, "auto_resolved", details={"recovered_by_run": run_id}
        )
        audit_service.record(
            db,
            action="incident.auto_resolved",
            entity_type="incident",
            entity_id=incident.id,
            details={"dag_id": incident.dag_id, "type": incident.type, "recovered_by_run": run_id},
        )
        summary.auto_resolved += 1

    # "When a DAG run finishes" workflows: one run each per finished DAG run.
    for run in rules.finished_runs(runs, dag.detection_watermark):
        db.flush()
        summary.automation_queued += len(
            automation_service.enqueue_for_dag_run(db, conn, dag, run, now=now)
        )

    dag.detection_watermark = rules.next_watermark(runs, dag.detection_watermark)


# ---------------------------------------------------------------------- cycle


def run_cycle(
    db: Session,
    *,
    trigger: str = "schedule",
    actor: User | None = None,
    now: datetime | None = None,
) -> CycleSummary:
    settings = get_settings()
    started = time.perf_counter()
    now = now or utcnow()
    summary = CycleSummary(trigger=trigger, started_at=now)
    since = now - timedelta(hours=settings.DETECTION_LOOKBACK_HOURS)

    connections = db.scalars(
        select(AirflowConnection)
        .where(AirflowConnection.is_active.is_(True))
        .order_by(AirflowConnection.name)
    ).all()
    for conn in connections:
        dags = db.scalars(
            select(MonitoredDag)
            .where(
                MonitoredDag.connection_id == conn.id,
                MonitoredDag.is_monitored.is_(True),
                MonitoredDag.is_present.is_(True),
            )
            .order_by(MonitoredDag.dag_id)
        ).all()
        if not dags:
            continue
        summary.connections += 1
        try:
            adapter = airflow_service.adapter_for(conn)
        except AppError as exc:
            summary.errors.append({"connection": conn.name, "error": exc.message})
            continue

        for dag in dags:
            try:
                runs = run_async(
                    lambda a=adapter, d=dag.dag_id: a.list_dag_runs(d, since=since, limit=RUN_LIMIT)
                )
            except AirflowAdapterError as exc:
                # Connection-level problem: record health, skip the rest of this connection.
                airflow_service.store_health(conn, exc.result, now)
                db.commit()
                summary.errors.append({"connection": conn.name, "error": exc.result.message})
                break
            try:
                _process_dag(db, adapter, conn, dag, runs, now, summary)
                summary.dags += 1
                db.commit()
            except Exception as exc:  # one bad DAG must not stop the cycle
                db.rollback()
                logger.exception("Detection failed for %s/%s", conn.name, dag.dag_id)
                summary.errors.append(
                    {"connection": conn.name, "dag_id": dag.dag_id, "error": str(exc)[:500]}
                )

    summary.finished_at = utcnow()
    summary.duration_ms = int((time.perf_counter() - started) * 1000)
    audit_service.record(
        db,
        action="detection.cycle",
        entity_type="detection",
        actor=actor,
        details={k: v for k, v in summary.as_dict().items() if k != "started_at"},
    )
    db.commit()
    return summary
