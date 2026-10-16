import uuid
from collections import Counter
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session, selectinload

from app.core.exceptions import ConflictError, NotFoundError
from app.db.base import utcnow
from app.detection.types import (
    EvidenceKind,
    IncidentResolution,
    IncidentSeverity,
    IncidentStatus,
    IncidentType,
)
from app.diagnosis.log_classifier import Diagnosis, classify_logs
from app.models.incident import Incident, IncidentEvent, IncidentOccurrence
from app.models.user import User
from app.services import audit_service

OPEN_STATUSES = (IncidentStatus.OPEN, IncidentStatus.ACKNOWLEDGED)


def add_event(
    db: Session,
    incident: Incident,
    event: str,
    *,
    actor: User | None = None,
    details: dict[str, Any] | None = None,
) -> IncidentEvent:
    entry = IncidentEvent(
        incident_id=incident.id,
        event=event,
        actor_user_id=actor.id if actor else None,
        details=details or {},
        created_at=utcnow(),
    )
    db.add(entry)
    return entry


def add_occurrence(
    db: Session,
    incident: Incident,
    *,
    occurred_at,
    run_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> IncidentOccurrence:
    occurrence = IncidentOccurrence(
        incident_id=incident.id,
        connection_id=incident.connection_id,
        monitored_dag_id=incident.monitored_dag_id,
        dag_id=incident.dag_id,
        type=incident.type,
        title=incident.title,
        severity=incident.severity,
        status=incident.status,
        run_id=run_id,
        first_seen_at=occurred_at,
        occurred_at=occurred_at,
        summary=incident.summary,
        details=details or {},
    )
    db.add(occurrence)
    return occurrence


def list_incidents(
    db: Session,
    *,
    statuses: list[IncidentStatus] | None,
    severity: IncidentSeverity | None,
    incident_type: IncidentType | None,
    connection_id: uuid.UUID | None,
    dag_id: str | None,
    search: str | None,
    limit: int,
    offset: int,
) -> tuple[list[Incident], int]:
    query = select(Incident)
    if statuses:
        query = query.where(Incident.status.in_(statuses))
    if severity:
        query = query.where(Incident.severity == severity)
    if incident_type:
        query = query.where(Incident.type == incident_type)
    if connection_id:
        query = query.where(Incident.connection_id == connection_id)
    if dag_id:
        query = query.where(Incident.dag_id == dag_id)
    if search:
        pattern = f"%{search.lower()}%"
        query = query.where(
            or_(func.lower(Incident.title).like(pattern), func.lower(Incident.dag_id).like(pattern))
        )
    total = db.scalar(select(func.count()).select_from(query.subquery())) or 0
    items = db.scalars(
        query.order_by(Incident.last_seen_at.desc(), Incident.id).limit(limit).offset(offset)
    ).all()
    return list(items), total


def open_counts(db: Session) -> list[dict[str, Any]]:
    """Open incidents grouped by connection, DAG and type (for the pipeline canvas)."""
    rows = db.execute(
        select(Incident.connection_id, Incident.dag_id, Incident.type, func.count())
        .where(Incident.status.in_(OPEN_STATUSES))
        .group_by(Incident.connection_id, Incident.dag_id, Incident.type)
    ).all()
    return [{"connection_id": c, "dag_id": d, "type": t, "count": n} for c, d, t, n in rows]


def summary(db: Session) -> dict[str, Any]:
    rows = db.execute(
        select(Incident.severity, func.count())
        .where(Incident.status.in_(OPEN_STATUSES))
        .group_by(Incident.severity)
    ).all()
    counts = Counter({severity: count for severity, count in rows})
    return {
        "open_total": sum(counts.values()),
        "by_severity": {s.value: counts.get(s, 0) for s in IncidentSeverity},
    }


def diagnose(incident: Incident) -> Diagnosis:
    """Classify the incident's failure from its task logs (newest first)."""
    logs = [
        e.content
        for e in sorted(incident.evidence, key=lambda e: e.collected_at, reverse=True)
        if e.kind == EvidenceKind.TASK_LOG and e.content
    ]
    return classify_logs(logs)


def get_incident(db: Session, incident_id: uuid.UUID, *, with_details: bool = False) -> Incident:
    query = select(Incident).where(Incident.id == incident_id)
    if with_details:
        query = query.options(
            selectinload(Incident.evidence),
            selectinload(Incident.events).selectinload(IncidentEvent.actor),
            selectinload(Incident.occurrences),
        )
    incident = db.scalar(query)
    if incident is None:
        raise NotFoundError("Incident not found")
    return incident


def list_occurrences(
    db: Session, incident_id: uuid.UUID, *, limit: int, offset: int
) -> tuple[list[IncidentOccurrence], int]:
    query = select(IncidentOccurrence).where(IncidentOccurrence.incident_id == incident_id)
    total = db.scalar(select(func.count()).select_from(query.subquery())) or 0
    items = db.scalars(
        query.order_by(IncidentOccurrence.occurred_at.desc(), IncidentOccurrence.id)
        .limit(limit)
        .offset(offset)
    ).all()
    return list(items), total


def _invalid(incident: Incident, action: str) -> ConflictError:
    return ConflictError(
        f"Cannot {action} an incident that is {incident.status.value.lower()}",
        code="invalid_transition",
        details={"status": incident.status},
    )


def _audit(
    db: Session, incident: Incident, action: str, actor: User | None, ip: str | None, **details: Any
) -> None:
    audit_service.record(
        db,
        action=action,
        entity_type="incident",
        entity_id=incident.id,
        actor=actor,
        details={"dag_id": incident.dag_id, "type": incident.type, **details},
        ip_address=ip,
    )


def acknowledge(
    db: Session, incident_id: uuid.UUID, *, actor: User, ip_address: str | None
) -> Incident:
    incident = get_incident(db, incident_id)
    if incident.status != IncidentStatus.OPEN:
        raise _invalid(incident, "acknowledge")
    incident.status = IncidentStatus.ACKNOWLEDGED
    incident.acknowledged_by = actor.id
    incident.acknowledged_at = utcnow()
    add_event(db, incident, "acknowledged", actor=actor)
    _audit(db, incident, "incident.acknowledged", actor, ip_address)
    db.commit()
    return incident


def resolve(
    db: Session, incident_id: uuid.UUID, note: str, *, actor: User, ip_address: str | None
) -> Incident:
    incident = get_incident(db, incident_id)
    if incident.status not in OPEN_STATUSES:
        raise _invalid(incident, "resolve")
    mark_resolved(incident, IncidentResolution.MANUAL, actor=actor, note=note)
    add_event(db, incident, "resolved", actor=actor, details={"note": note})
    _audit(db, incident, "incident.resolved", actor, ip_address, note=note)
    db.commit()
    return incident


def mark_resolved(
    incident: Incident,
    resolution: IncidentResolution,
    *,
    actor: User | None = None,
    note: str | None = None,
) -> None:
    incident.status = IncidentStatus.RESOLVED
    incident.resolution = resolution
    incident.resolved_by = actor.id if actor else None
    incident.resolved_at = utcnow()
    incident.resolution_note = note


def reopen(db: Session, incident_id: uuid.UUID, *, actor: User, ip_address: str | None) -> Incident:
    incident = get_incident(db, incident_id)
    if incident.status != IncidentStatus.RESOLVED:
        raise _invalid(incident, "reopen")
    duplicate = db.scalar(
        select(Incident.id).where(
            Incident.fingerprint == incident.fingerprint,
            Incident.status.in_(OPEN_STATUSES),
            Incident.id != incident.id,
        )
    )
    if duplicate:
        raise ConflictError(
            "Another open incident already tracks this problem",
            code="duplicate_open_incident",
            details={"incident_id": str(duplicate)},
        )
    incident.status = IncidentStatus.OPEN
    incident.resolution = None
    incident.resolved_by = None
    incident.resolved_at = None
    incident.resolution_note = None
    incident.acknowledged_by = None
    incident.acknowledged_at = None
    add_event(db, incident, "reopened", actor=actor)
    _audit(db, incident, "incident.reopened", actor, ip_address)
    db.commit()
    return incident
