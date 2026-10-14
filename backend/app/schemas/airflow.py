import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.models.airflow import ConnectionKind, DeploymentEnvironment
from app.orchestration.airflow.base import (
    ApiVersion,
    AuthType,
    ConnectionStatus,
    normalize_base_url,
)


def _validate_target(model: "_TargetFields") -> None:
    """Shared cross-field validation for anything that points at an Airflow instance."""
    if model.base_url is not None and model.kind is not None:
        model.base_url = normalize_base_url(
            model.base_url, allow_query=model.kind == ConnectionKind.MOCK
        )
    if model.kind == ConnectionKind.LIVE and model.auth_type == AuthType.BASIC:
        if not model.username:
            raise ValueError("username is required for BASIC auth")


class _TargetFields(BaseModel):
    kind: ConnectionKind | None = None
    base_url: str | None = None
    auth_type: AuthType | None = None
    username: str | None = Field(default=None, max_length=200)
    secret: str | None = Field(default=None, max_length=4096)


class AirflowConnTestRequest(_TargetFields):
    """Test settings that have not been saved yet."""

    kind: ConnectionKind = ConnectionKind.LIVE
    base_url: str = Field(max_length=500)
    auth_type: AuthType = AuthType.BASIC

    @model_validator(mode="after")
    def _validate(self) -> "AirflowConnTestRequest":
        _validate_target(self)
        return self


class AirflowConnCreate(_TargetFields):
    name: str = Field(min_length=1, max_length=100)
    environment: DeploymentEnvironment = DeploymentEnvironment.DEV
    kind: ConnectionKind = ConnectionKind.LIVE
    base_url: str = Field(max_length=500)
    auth_type: AuthType = AuthType.BASIC
    is_active: bool = True
    is_default: bool = False

    @model_validator(mode="after")
    def _validate(self) -> "AirflowConnCreate":
        _validate_target(self)
        if self.kind == ConnectionKind.LIVE and self.auth_type != AuthType.NONE and not self.secret:
            raise ValueError("secret (password or token) is required for BASIC and TOKEN auth")
        return self


class AirflowConnUpdate(_TargetFields):
    """Partial update. Omitting `secret` keeps the stored one."""

    name: str | None = Field(default=None, min_length=1, max_length=100)
    environment: DeploymentEnvironment | None = None
    base_url: str | None = Field(default=None, max_length=500)
    is_active: bool | None = None
    is_default: bool | None = None


class AirflowConnRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    environment: DeploymentEnvironment
    kind: ConnectionKind
    base_url: str
    auth_type: AuthType
    username: str | None
    has_secret: bool
    api_version: ApiVersion | None
    airflow_version: str | None
    is_active: bool
    is_default: bool
    last_health_status: ConnectionStatus
    last_health_message: str | None
    last_latency_ms: int | None
    last_checked_at: datetime | None
    # Server-computed: show this connection's DAGs only while `is_live`.
    is_live: bool
    status_stale: bool
    created_by: uuid.UUID | None
    created_at: datetime
    updated_at: datetime


class ConnectionTestResultRead(BaseModel):
    status: ConnectionStatus
    ok: bool
    message: str
    latency_ms: int | None
    api_version: ApiVersion | None
    airflow_version: str | None
    checked_at: datetime


class DagRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    connection_id: uuid.UUID
    dag_id: str
    description: str | None
    schedule_summary: str | None
    is_paused: bool | None
    is_monitored: bool
    is_present: bool
    tags: list[str]
    last_synced_at: datetime | None
    sla_minutes: int | None
    detect_failures: bool


class DagSyncResult(BaseModel):
    total: int
    created: int
    updated: int
    missing: int
    synced_at: datetime


class DagMonitorUpdate(BaseModel):
    """Partial update; `sla_minutes: null` removes the SLA."""

    is_monitored: bool | None = None
    sla_minutes: int | None = Field(default=None, ge=1, le=10080)
    detect_failures: bool | None = None
