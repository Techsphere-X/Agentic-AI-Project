import time

from fastapi import APIRouter
from sqlalchemy import select, text
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import get_settings
from app.core.dependencies import DbSession, ViewerUser
from app.models.airflow import AirflowConnection
from app.models.database_connection import DatabaseConnection
from app.orchestration.airflow.base import ConnectionStatus
from app.schemas.connector_metrics import ConnectorMetricsResponse
from app.schemas.health import (
    AirflowConnectionHealth,
    DatabaseConnectionHealth,
    DatabaseHealth,
    HealthStatusResponse,
    LivenessResponse,
)
from app.services import connector_metrics_service

router = APIRouter(prefix="/health", tags=["health"])

_OK_STATUSES = {ConnectionStatus.HEALTHY, ConnectionStatus.UNKNOWN}


@router.get("/live", response_model=LivenessResponse)
def live() -> LivenessResponse:
    return LivenessResponse()


@router.get("", response_model=HealthStatusResponse)
def health(db: DbSession, _: ViewerUser) -> HealthStatusResponse:
    settings = get_settings()
    started = time.perf_counter()
    connections: list[AirflowConnection] = []
    db_connections: list[DatabaseConnection] = []
    try:
        db.execute(text("SELECT 1"))
        database = DatabaseHealth(
            status="ok", latency_ms=int((time.perf_counter() - started) * 1000)
        )
        connections = list(
            db.scalars(
                select(AirflowConnection)
                .where(AirflowConnection.is_active.is_(True))
                .order_by(AirflowConnection.is_default.desc(), AirflowConnection.name)
            ).all()
        )
        db_connections = list(
            db.scalars(
                select(DatabaseConnection)
                .where(DatabaseConnection.is_active.is_(True))
                .order_by(DatabaseConnection.name)
            ).all()
        )
    except SQLAlchemyError:
        database = DatabaseHealth(status="error", message="Database query failed")

    airflow = [
        AirflowConnectionHealth(
            id=c.id,
            name=c.name,
            is_default=c.is_default,
            status=c.last_health_status,
            message=c.last_health_message,
            last_checked_at=c.last_checked_at,
        )
        for c in connections
    ]
    databases = [
        DatabaseConnectionHealth(
            id=c.id,
            name=c.name,
            engine=c.engine,
            status=c.last_health_status,
            message=c.last_health_message,
            last_checked_at=c.last_checked_at,
        )
        for c in db_connections
    ]
    degraded = (
        database.status != "ok"
        or any(a.status not in _OK_STATUSES for a in airflow)
        or any(d.status not in _OK_STATUSES for d in databases)
        or any(c.status_stale for c in connections)  # the connection monitor is not reporting
    )
    return HealthStatusResponse(
        status="degraded" if degraded else "ok",
        version=settings.VERSION,
        environment=settings.ENVIRONMENT,
        database=database,
        airflow=airflow,
        databases=databases,
    )


@router.get("/connector-metrics", response_model=ConnectorMetricsResponse)
def get_connector_metrics(db: DbSession, _: ViewerUser) -> ConnectorMetricsResponse:
    """Return aggregated run KPIs, successes, failures, and reliability rankings per connector."""
    return connector_metrics_service.get_connector_metrics(db)
