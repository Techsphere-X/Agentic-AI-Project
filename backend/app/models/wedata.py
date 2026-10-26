import uuid
from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    Enum,
    ForeignKey,
    Index,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.config import get_settings
from app.db.base import Base, TimestampMixin, UTCDateTime, utcnow
from app.models.airflow import ConnectionKind, DeploymentEnvironment
from app.orchestration.wedata.base import WeDataConnectionStatus


def _enum(enum_cls: type[StrEnum], name: str, length: int = 32) -> Enum:
    return Enum(enum_cls, native_enum=False, length=length, create_constraint=True, name=name)


class WeDataConnection(TimestampMixin, Base):
    __tablename__ = "wedata_connections"
    __table_args__ = (
        Index(
            "uq_wedata_connections_single_default",
            "is_default",
            unique=True,
            sqlite_where=text("is_default = 1"),
            postgresql_where=text("is_default = true"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    environment: Mapped[DeploymentEnvironment] = mapped_column(
        _enum(DeploymentEnvironment, "wedata_deployment_environment")
    )
    kind: Mapped[ConnectionKind] = mapped_column(_enum(ConnectionKind, "wedata_connection_kind"))
    project_id: Mapped[str] = mapped_column(String(100))
    region: Mapped[str] = mapped_column(String(50), default="ap-guangzhou")
    endpoint: Mapped[str] = mapped_column(String(200), default="wedata.tencentcloudapi.com")
    secret_id: Mapped[str | None] = mapped_column(String(200))
    encrypted_secret_key: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(default=True)
    is_default: Mapped[bool] = mapped_column(default=False)
    last_health_status: Mapped[WeDataConnectionStatus] = mapped_column(
        _enum(WeDataConnectionStatus, "wedata_connection_status"),
        default=WeDataConnectionStatus.UNKNOWN,
    )
    last_health_message: Mapped[str | None] = mapped_column(Text)
    last_latency_ms: Mapped[int | None]
    last_checked_at: Mapped[datetime | None] = mapped_column(UTCDateTime())
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )

    tasks: Mapped[list["MonitoredWeDataTask"]] = relationship(
        back_populates="connection", cascade="all, delete-orphan", passive_deletes=True
    )

    @property
    def has_secret(self) -> bool:
        return bool(self.encrypted_secret_key)

    @property
    def status_stale(self) -> bool:
        reference = self.last_checked_at or self.updated_at
        if reference is None:
            return False
        return (utcnow() - reference).total_seconds() > 120

    @property
    def is_live(self) -> bool:
        return (
            self.is_active
            and self.last_health_status == WeDataConnectionStatus.HEALTHY
            and not self.status_stale
        )


class MonitoredWeDataTask(TimestampMixin, Base):
    __tablename__ = "wedata_monitored_tasks"
    __table_args__ = (
        UniqueConstraint(
            "connection_id", "task_id", name="uq_wedata_monitored_tasks_conn_task"
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    connection_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("wedata_connections.id", ondelete="CASCADE"), index=True
    )
    task_id: Mapped[str] = mapped_column(String(200))
    task_name: Mapped[str] = mapped_column(String(200))
    workflow_id: Mapped[str | None] = mapped_column(String(100))
    workflow_name: Mapped[str | None] = mapped_column(String(200))
    task_type: Mapped[str | None] = mapped_column(String(50))
    schedule_cron: Mapped[str | None] = mapped_column(String(100))
    is_paused: Mapped[bool] = mapped_column(default=False)
    is_monitored: Mapped[bool] = mapped_column(default=False, index=True)
    is_present_in_wedata: Mapped[bool] = mapped_column(default=True)
    last_synced_at: Mapped[datetime] = mapped_column(UTCDateTime(), default=utcnow)
    last_run_state: Mapped[str | None] = mapped_column(String(50))
    last_run_id: Mapped[str | None] = mapped_column(String(200))
    last_run_at: Mapped[datetime | None] = mapped_column(UTCDateTime())

    connection: Mapped[WeDataConnection] = relationship(back_populates="tasks")
