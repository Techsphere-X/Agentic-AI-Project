import uuid
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class ConnectorRunMetrics(BaseModel):
    id: str = Field(description="Connector unique identifier")
    name: str = Field(description="Display name of the connector")
    type: Literal["airflow", "database", "channel", "workflow_engine"] = Field(
        description="Type of connector"
    )
    environment: str | None = Field(default=None, description="Deployment environment (DEV, STAGING, PROD)")
    kind: str | None = Field(default=None, description="Connector kind or engine (e.g. LIVE, MOCK, POSTGRESQL, SLACK)")
    status: str = Field(default="UNKNOWN", description="Current health status (e.g. HEALTHY, DEGRADED, UNKNOWN)")
    latency_ms: int | None = Field(default=None, description="Last recorded latency in milliseconds")
    total_runs: int = Field(default=0, description="Total recorded runs / checks")
    success_runs: int = Field(default=0, description="Total successful runs / checks")
    failed_runs: int = Field(default=0, description="Total failed runs / checks")
    success_rate: float = Field(default=100.0, description="Calculated success rate percentage (0.0 - 100.0)")
    active_runs: int = Field(default=0, description="Currently running executions, if any")
    incident_count: int = Field(default=0, description="Open or associated incident count")
    last_run_at: datetime | None = Field(default=None, description="Timestamp of the most recent run or check")
    description: str | None = Field(default=None, description="Short summary or endpoint description")


class ConnectorMetricsResponse(BaseModel):
    total_connectors: int = Field(description="Total number of active connectors")
    total_runs: int = Field(description="Aggregated total runs across all connectors")
    total_success: int = Field(description="Aggregated successful runs")
    total_failed: int = Field(description="Aggregated failed runs")
    overall_success_rate: float = Field(description="Overall fleet success rate percentage (0.0 - 100.0)")
    total_executions: int = Field(description="Total workflow executions")
    successful_executions: int = Field(description="Successfully completed workflow executions")
    failed_executions: int = Field(description="Failed workflow executions")
    execution_success_rate: float = Field(description="Workflow execution success rate percentage")
    connectors: list[ConnectorRunMetrics] = Field(default_factory=list, description="Ranked / listed connector metrics")
