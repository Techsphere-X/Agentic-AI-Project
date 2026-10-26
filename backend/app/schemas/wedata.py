import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.models.airflow import ConnectionKind, DeploymentEnvironment
from app.orchestration.wedata.base import WeDataConnectionStatus


class WeDataConnTestRequest(BaseModel):
    kind: ConnectionKind = ConnectionKind.LIVE
    project_id: str = Field(min_length=1, max_length=100)
    region: str = Field(default="ap-guangzhou", max_length=50)
    endpoint: str = Field(default="wedata.tencentcloudapi.com", max_length=200)
    secret_id: str | None = Field(default=None, max_length=200)
    secret_key: str | None = Field(default=None, max_length=4096)


class WeDataConnCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    environment: DeploymentEnvironment = DeploymentEnvironment.DEV
    kind: ConnectionKind = ConnectionKind.LIVE
    project_id: str = Field(min_length=1, max_length=100)
    region: str = Field(default="ap-guangzhou", max_length=50)
    endpoint: str = Field(default="wedata.tencentcloudapi.com", max_length=200)
    secret_id: str | None = Field(default=None, max_length=200)
    secret_key: str | None = Field(default=None, max_length=4096)
    is_active: bool = True
    is_default: bool = False


class WeDataConnUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=100)
    environment: DeploymentEnvironment | None = None
    project_id: str | None = Field(default=None, max_length=100)
    region: str | None = Field(default=None, max_length=50)
    endpoint: str | None = Field(default=None, max_length=200)
    secret_id: str | None = Field(default=None, max_length=200)
    secret_key: str | None = Field(default=None, max_length=4096)
    is_active: bool | None = None
    is_default: bool | None = None


class WeDataConnRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    environment: DeploymentEnvironment
    kind: ConnectionKind
    project_id: str
    region: str
    endpoint: str
    secret_id: str | None
    has_secret: bool
    is_active: bool
    is_default: bool
    last_health_status: WeDataConnectionStatus
    last_health_message: str | None
    last_latency_ms: int | None
    last_checked_at: datetime | None
    is_live: bool
    status_stale: bool
    created_by: uuid.UUID | None
    created_at: datetime
    updated_at: datetime


class WeDataTestResultRead(BaseModel):
    status: WeDataConnectionStatus
    ok: bool
    message: str
    latency_ms: int | None = None
    project_id: str | None = None
    project_name: str | None = None
    checked_at: datetime


class WeDataTaskRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    task_id: str
    task_name: str
    workflow_id: str | None
    workflow_name: str | None
    task_type: str | None
    schedule_cron: str | None
    is_paused: bool
    is_monitored: bool
    is_present_in_wedata: bool
    last_synced_at: datetime
    last_run_state: str | None
    last_run_id: str | None
    last_run_at: datetime | None


class WeDataTaskMonitorUpdate(BaseModel):
    is_monitored: bool


class WeDataTaskSyncResult(BaseModel):
    total: int
    created: int
    updated: int
    missing: int
    synced_at: datetime


class WeDataInstanceRunRead(BaseModel):
    run_id: str
    task_id: str
    state: str
    raw_state: str | None = None
    cur_run_date: str | None = None
    start_date: str | None = None
    end_date: str | None = None
