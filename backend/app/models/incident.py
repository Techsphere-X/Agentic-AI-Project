import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import JSON, Enum, ForeignKey, Index, String, Text, Uuid, text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base, TimestampMixin, UTCDateTime, utcnow
from app.detection.types import (
    EvidenceKind,
    IncidentResolution,
    IncidentSeverity,
    IncidentStatus,
    IncidentType,
)
from app.models.user import User


def _enum(enum_cls: type[StrEnum], name: str) -> Enum:
    return Enum(enum_cls, native_enum=False, length=32, create_constraint=True, name=name)


class Incident(TimestampMixin, Base):
    __tablename__ = "incidents"
    __table_args__ = (
        # One open (non-resolved) incident per fingerprint; resolved ones are history.
        Index(
            "uq_incidents_open_fingerprint",
            "fingerprint",
            unique=True,
            sqlite_where=text("status != 'RESOLVED'"),
            postgresql_where=text("status != 'RESOLVED'"),
        ),
        Index("ix_incidents_status_last_seen", "status", "last_seen_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    connection_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("airflow_connections.id", ondelete="CASCADE"), index=True
    )
    monitored_dag_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("monitored_dags.id", ondelete="CASCADE"), index=True
    )
    dag_id: Mapped[str] = mapped_column(String(250))
    run_id: Mapped[str | None] = mapped_column(String(250))  # first failing run
    last_run_id: Mapped[str | None] = mapped_column(String(250))  # latest failing run
    type: Mapped[IncidentType] = mapped_column(_enum(IncidentType, "incident_type"))
    severity: Mapped[IncidentSeverity] = mapped_column(_enum(IncidentSeverity, "incident_severity"))
    status: Mapped[IncidentStatus] = mapped_column(
        _enum(IncidentStatus, "incident_status"), default=IncidentStatus.OPEN
    )
    title: Mapped[str] = mapped_column(String(300))
    summary: Mapped[str | None] = mapped_column(Text)
    fingerprint: Mapped[str] = mapped_column(String(400))
    occurrence_count: Mapped[int] = mapped_column(default=1)
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime())  # latest failure/breach time
    first_seen_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    last_seen_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    acknowledged_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    acknowledged_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    resolved_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    resolved_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    resolution: Mapped[IncidentResolution | None] = mapped_column(
        _enum(IncidentResolution, "incident_resolution")
    )
    resolution_note: Mapped[str | None] = mapped_column(Text)
    # Stored diagnosis (regex / model / operator); see app.services.diagnosis_service.
    diagnosis: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    evidence: Mapped[list["IncidentEvidence"]] = relationship(
        back_populates="incident",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="IncidentEvidence.collected_at",
    )
    events: Mapped[list["IncidentEvent"]] = relationship(
        back_populates="incident",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="IncidentEvent.created_at",
    )
    occurrences: Mapped[list["IncidentOccurrence"]] = relationship(
        back_populates="incident",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="IncidentOccurrence.occurred_at",
    )

    @property
    def is_open(self) -> bool:
        return self.status != IncidentStatus.RESOLVED


class IncidentEvidence(Base):
    __tablename__ = "incident_evidence"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    incident_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("incidents.id", ondelete="CASCADE"), index=True
    )
    kind: Mapped[EvidenceKind] = mapped_column(_enum(EvidenceKind, "evidence_kind"))
    # e.g. "run:<run_id>" or "run:<run_id> task:<task_id> try:<n>"; also the idempotency key.
    source: Mapped[str] = mapped_column(String(600), index=True)
    content: Mapped[str | None] = mapped_column(Text)
    data: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    truncated: Mapped[bool] = mapped_column(default=False)
    collected_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)

    incident: Mapped[Incident] = relationship(back_populates="evidence")


class IncidentEvent(Base):
    """Timeline entry. `actor_user_id` is null for system actions."""

    __tablename__ = "incident_events"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    incident_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("incidents.id", ondelete="CASCADE"), index=True
    )
    event: Mapped[str] = mapped_column(String(32))
    actor_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)

    incident: Mapped[Incident] = relationship(back_populates="events")
    actor: Mapped[User | None] = relationship()

    @property
    def actor_name(self) -> str:
        return self.actor.full_name if self.actor else "System"


class IncidentOccurrence(Base):
    """Immutable snapshot of one detection occurrence within a logical incident."""

    __tablename__ = "incident_occurrences"
    __table_args__ = (Index("ix_incident_occurrences_incident_occurred", "incident_id", "occurred_at"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    incident_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("incidents.id", ondelete="CASCADE"), index=True
    )
    connection_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("airflow_connections.id", ondelete="CASCADE"), index=True
    )
    monitored_dag_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("monitored_dags.id", ondelete="CASCADE"), index=True
    )
    dag_id: Mapped[str] = mapped_column(String(250))
    type: Mapped[IncidentType] = mapped_column(_enum(IncidentType, "incident_occurrence_type"))
    title: Mapped[str] = mapped_column(String(300))
    severity: Mapped[IncidentSeverity] = mapped_column(
        _enum(IncidentSeverity, "incident_occurrence_severity")
    )
    status: Mapped[IncidentStatus] = mapped_column(
        _enum(IncidentStatus, "incident_occurrence_status")
    )
    run_id: Mapped[str | None] = mapped_column(String(250))
    first_seen_at: Mapped[datetime] = mapped_column(UTCDateTime())
    occurred_at: Mapped[datetime] = mapped_column(UTCDateTime(), index=True)
    summary: Mapped[str | None] = mapped_column(Text)
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)

    incident: Mapped[Incident] = relationship(back_populates="occurrences")


class DetectionLease(Base):
    """Single-row lock so only one worker runs detection at a time."""

    __tablename__ = "detection_leases"

    name: Mapped[str] = mapped_column(String(64), primary_key=True)
    holder: Mapped[str] = mapped_column(String(200))
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime())
    renewed_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
