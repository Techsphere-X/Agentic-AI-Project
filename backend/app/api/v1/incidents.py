import uuid
from typing import Annotated

from fastapi import APIRouter, Query

from app.core.dependencies import ClientIp, DbSession, OperatorUser, PageParams, ViewerUser
from app.detection.types import IncidentSeverity, IncidentStatus, IncidentType
from app.schemas.common import Page
from app.schemas.incident import (
    DiagnosisOverride,
    DiagnosisRead,
    IncidentDetail,
    IncidentOccurrenceRead,
    IncidentRead,
    IncidentSummary,
    OpenCount,
    ResolveRequest,
)
from app.services import diagnosis_service, incident_service

router = APIRouter(prefix="/incidents", tags=["incidents"])


def _detail(db: DbSession, incident_id: uuid.UUID) -> IncidentDetail:
    # Stored at detection time; never calls the model here (old incidents: regex backfill,
    # done before the timeline is loaded so its 'diagnosed' event shows up).
    diagnosis = diagnosis_service.stored_or_backfill(
        db, incident_service.get_incident(db, incident_id)
    )
    incident = incident_service.get_incident(db, incident_id, with_details=True)
    detail = IncidentDetail.model_validate(incident)
    if diagnosis is not None:
        detail.diagnosis = DiagnosisRead.model_validate(diagnosis)
    return detail


@router.get("", response_model=Page[IncidentRead])
def list_incidents(
    db: DbSession,
    _: ViewerUser,
    page: PageParams,
    status: Annotated[list[IncidentStatus] | None, Query()] = None,
    severity: IncidentSeverity | None = None,
    type: IncidentType | None = None,  # noqa: A002 - public query parameter name
    connection_id: uuid.UUID | None = None,
    dag_id: str | None = None,
    search: str | None = None,
) -> Page[IncidentRead]:
    items, total = incident_service.list_incidents(
        db,
        statuses=status,
        severity=severity,
        incident_type=type,
        connection_id=connection_id,
        dag_id=dag_id,
        search=search,
        limit=page.limit,
        offset=page.offset,
    )
    return Page[IncidentRead](
        items=[IncidentRead.model_validate(i) for i in items],
        total=total,
        limit=page.limit,
        offset=page.offset,
    )


@router.get("/summary", response_model=IncidentSummary)
def incident_summary(db: DbSession, _: ViewerUser) -> IncidentSummary:
    return IncidentSummary(**incident_service.summary(db))


@router.get("/open-counts", response_model=list[OpenCount])
def open_counts(db: DbSession, _: ViewerUser) -> list[OpenCount]:
    return [OpenCount(**row) for row in incident_service.open_counts(db)]


@router.get("/{incident_id}", response_model=IncidentDetail)
def get_incident(incident_id: uuid.UUID, db: DbSession, _: ViewerUser) -> IncidentDetail:
    return _detail(db, incident_id)


@router.get("/{incident_id}/occurrences", response_model=Page[IncidentOccurrenceRead])
def list_occurrences(
    incident_id: uuid.UUID,
    db: DbSession,
    _: ViewerUser,
    page: PageParams,
) -> Page[IncidentOccurrenceRead]:
    incident_service.get_incident(db, incident_id)
    items, total = incident_service.list_occurrences(
        db, incident_id, limit=page.limit, offset=page.offset
    )
    return Page[IncidentOccurrenceRead](
        items=[IncidentOccurrenceRead.model_validate(item) for item in items],
        total=total,
        limit=page.limit,
        offset=page.offset,
    )


@router.post("/{incident_id}/acknowledge", response_model=IncidentDetail)
def acknowledge(
    incident_id: uuid.UUID, db: DbSession, operator: OperatorUser, ip: ClientIp
) -> IncidentDetail:
    incident_service.acknowledge(db, incident_id, actor=operator, ip_address=ip)
    return _detail(db, incident_id)


@router.post("/{incident_id}/resolve", response_model=IncidentDetail)
def resolve(
    incident_id: uuid.UUID,
    body: ResolveRequest,
    db: DbSession,
    operator: OperatorUser,
    ip: ClientIp,
) -> IncidentDetail:
    incident_service.resolve(db, incident_id, body.note.strip(), actor=operator, ip_address=ip)
    return _detail(db, incident_id)


@router.post("/{incident_id}/reopen", response_model=IncidentDetail)
def reopen(
    incident_id: uuid.UUID, db: DbSession, operator: OperatorUser, ip: ClientIp
) -> IncidentDetail:
    incident_service.reopen(db, incident_id, actor=operator, ip_address=ip)
    return _detail(db, incident_id)


@router.put("/{incident_id}/diagnosis", response_model=IncidentDetail)
def correct_diagnosis(
    incident_id: uuid.UUID,
    body: DiagnosisOverride,
    db: DbSession,
    operator: OperatorUser,
    ip: ClientIp,
) -> IncidentDetail:
    """Operator correction: wins over regex/model and is never overwritten by re-diagnosis."""
    note = body.note.strip() if body.note else None
    diagnosis_service.set_override(
        db, incident_id, body.category, note or None, actor=operator, ip_address=ip
    )
    return _detail(db, incident_id)
