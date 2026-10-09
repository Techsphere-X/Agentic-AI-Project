import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import JSON, Enum, ForeignKey, Index, String, Text, UniqueConstraint, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.automation.types import (
    ApprovalStatus,
    DagRunCheckStatus,
    NotificationLevel,
    RunStatus,
    StepStatus,
    WorkflowMode,
)
from app.db.base import Base, TimestampMixin, UTCDateTime, utcnow
from app.models.incident import Incident
from app.models.user import User


def _enum(enum_cls: type[StrEnum], name: str) -> Enum:
    return Enum(enum_cls, native_enum=False, length=32, create_constraint=True, name=name)


class Workflow(TimestampMixin, Base):
    """An automation: a graph of typed nodes ({nodes, edges}) started by a trigger node."""

    __tablename__ = "automation_workflows"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    key: Mapped[str | None] = mapped_column(String(100), unique=True)  # template key
    template_key: Mapped[str | None] = mapped_column(String(100))
    template_version: Mapped[int | None]
    template_parameters: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    name: Mapped[str] = mapped_column(String(200))
    description: Mapped[str | None] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(default=False)
    mode: Mapped[WorkflowMode] = mapped_column(
        _enum(WorkflowMode, "workflow_mode"), default=WorkflowMode.LIVE
    )
    graph: Mapped[dict[str, Any]] = mapped_column(JSON)
    version: Mapped[int] = mapped_column(default=1)
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )


class WorkflowRun(TimestampMixin, Base):
    __tablename__ = "workflow_runs"
    __table_args__ = (
        UniqueConstraint("workflow_id", "dedup_key"),
        Index("ix_workflow_runs_status_wake_at", "status", "wake_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    workflow_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("automation_workflows.id", ondelete="CASCADE"), index=True
    )
    workflow_version: Mapped[int]
    graph: Mapped[dict[str, Any]] = mapped_column(JSON)  # snapshot at start
    incident_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("incidents.id", ondelete="CASCADE"), index=True
    )
    trigger_event: Mapped[str] = mapped_column(String(32))
    dedup_key: Mapped[str] = mapped_column(String(200))
    status: Mapped[RunStatus] = mapped_column(
        _enum(RunStatus, "workflow_run_status"), default=RunStatus.PENDING
    )
    dry_run: Mapped[bool] = mapped_column(default=False)
    current_node: Mapped[str | None] = mapped_column(String(100))
    # Blocks still to run, in order (head = current_node). None on runs from before fan-out.
    pending_nodes: Mapped[list[str] | None] = mapped_column(JSON)
    context: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    wake_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    claimed_until: Mapped[datetime | None] = mapped_column(UTCDateTime())
    claimed_by: Mapped[str | None] = mapped_column(String(200))
    error: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())

    workflow: Mapped[Workflow] = relationship()
    incident: Mapped[Incident | None] = relationship()
    steps: Mapped[list["WorkflowStep"]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="WorkflowStep.started_at",
    )
    approvals: Mapped[list["Approval"]] = relationship(
        back_populates="run",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="Approval.created_at",
    )

    @property
    def workflow_name(self) -> str:
        return self.workflow.name if self.workflow else ""


class WorkflowStep(Base):
    __tablename__ = "workflow_steps"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("workflow_runs.id", ondelete="CASCADE"), index=True
    )
    node_id: Mapped[str] = mapped_column(String(100))
    node_type: Mapped[str] = mapped_column(String(64))
    status: Mapped[StepStatus] = mapped_column(_enum(StepStatus, "workflow_step_status"))
    port: Mapped[str | None] = mapped_column(String(32))
    output: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    message: Mapped[str | None] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime())

    run: Mapped[WorkflowRun] = relationship(back_populates="steps")


class Approval(TimestampMixin, Base):
    __tablename__ = "approvals"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("workflow_runs.id", ondelete="CASCADE"), index=True
    )
    incident_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("incidents.id", ondelete="CASCADE"), index=True
    )
    node_id: Mapped[str] = mapped_column(String(100))
    title: Mapped[str] = mapped_column(String(300))
    summary: Mapped[str | None] = mapped_column(Text)
    proposed_action: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    status: Mapped[ApprovalStatus] = mapped_column(
        _enum(ApprovalStatus, "approval_status"), default=ApprovalStatus.PENDING, index=True
    )
    expires_at: Mapped[datetime] = mapped_column(UTCDateTime())
    decided_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    decided_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    comment: Mapped[str | None] = mapped_column(Text)

    run: Mapped[WorkflowRun] = relationship(back_populates="approvals")
    incident: Mapped[Incident | None] = relationship()
    decider: Mapped[User | None] = relationship()

    @property
    def decided_by_name(self) -> str | None:
        return self.decider.full_name if self.decider else None

    @property
    def workflow_name(self) -> str:
        return self.run.workflow_name if self.run else ""


class DagRunCheck(TimestampMixin, Base):
    """Validation status of one finished DAG run, recorded by the workflow it started."""

    __tablename__ = "dag_run_checks"
    __table_args__ = (
        Index("ix_dag_run_checks_dag_created", "connection_id", "dag_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    connection_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("airflow_connections.id", ondelete="CASCADE")
    )
    monitored_dag_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("monitored_dags.id", ondelete="CASCADE"), index=True
    )
    dag_id: Mapped[str] = mapped_column(String(250))
    run_id: Mapped[str] = mapped_column(String(250))
    run_state: Mapped[str] = mapped_column(String(32))  # Airflow state: success / failed
    workflow_run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("workflow_runs.id", ondelete="CASCADE"), unique=True
    )
    status: Mapped[DagRunCheckStatus] = mapped_column(
        _enum(DagRunCheckStatus, "dag_run_check_status"), default=DagRunCheckStatus.PENDING
    )
    message: Mapped[str | None] = mapped_column(Text)
    checked_at: Mapped[datetime | None] = mapped_column(UTCDateTime())

    workflow_run: Mapped[WorkflowRun] = relationship()

    @property
    def workflow_name(self) -> str:
        return self.workflow_run.workflow_name if self.workflow_run else ""


class Notification(Base):
    __tablename__ = "notifications"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    run_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("workflow_runs.id", ondelete="SET NULL"), index=True
    )
    incident_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("incidents.id", ondelete="SET NULL"), index=True
    )
    level: Mapped[NotificationLevel] = mapped_column(
        _enum(NotificationLevel, "notification_level"), default=NotificationLevel.INFO
    )
    title: Mapped[str] = mapped_column(String(300))
    body: Mapped[str | None] = mapped_column(Text)
    read_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow, index=True)
