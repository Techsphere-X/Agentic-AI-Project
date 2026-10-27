"""Service for computing aggregated run metrics, successes, and failures per connector."""

import logging
from datetime import datetime
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from app.db.base import utcnow
from app.models.airflow import AirflowConnection, ConnectionKind, MonitoredDag
from app.models.automation import StepStatus, WorkflowRun, WorkflowStep
from app.models.database_connection import DatabaseConnection
from app.models.incident import Incident
from app.models.notification_channel import NotificationChannel
from app.orchestration.airflow.base import AirflowAdapterError
from app.orchestration.airflow.factory import run_async
from app.schemas.connector_metrics import (
    ConnectorMetricsResponse,
    ConnectorRunMetrics,
)
from app.services.airflow_service import adapter_for

logger = logging.getLogger(__name__)


def _rate(success: int, total: int) -> float:
    if total <= 0:
        return 100.0
    return round((success / total) * 100.0, 1)


def get_connector_metrics(db: Session) -> ConnectorMetricsResponse:
    """Collect total runs, successes, failures, and rates for each connector."""
    connector_items: list[ConnectorRunMetrics] = []
    execution_counts = {
        status.value if hasattr(status, "value") else str(status): count
        for status, count in db.execute(
            select(WorkflowRun.status, func.count()).group_by(WorkflowRun.status)
        ).all()
    }
    total_executions = sum(execution_counts.values())
    successful_executions = execution_counts.get("COMPLETED", 0)
    failed_executions = execution_counts.get("FAILED", 0)
    execution_success_rate = (
        round((successful_executions / total_executions) * 100.0, 1)
        if total_executions
        else 0.0
    )

    # ------------------------------------------------------------------ Airflow Connections
    airflow_conns = list(
        db.scalars(
            select(AirflowConnection)
            .where(AirflowConnection.is_active.is_(True))
            .order_by(AirflowConnection.is_default.desc(), AirflowConnection.name)
        ).all()
    )

    for conn in airflow_conns:
        success_runs = 0
        failed_runs = 0
        last_run_at = conn.last_checked_at

        # 1. Fetch DAG runs from Airflow adapter
        dags = list(
            db.scalars(
                select(MonitoredDag).where(MonitoredDag.connection_id == conn.id)
            ).all()
        )

        try:
            adapter = adapter_for(conn)
            for dag in dags:
                try:
                    runs = run_async(
                        lambda d_id=dag.dag_id: adapter.list_dag_runs(d_id, limit=25)
                    )
                    for r in runs:
                        if r.state == "success":
                            success_runs += 1
                        elif r.state == "failed":
                            failed_runs += 1
                        if r.end_date and (last_run_at is None or r.end_date > last_run_at):
                            last_run_at = r.end_date
                except Exception as dag_err:
                    logger.debug("Failed to list DAG runs for %s on %s: %s", dag.dag_id, conn.name, dag_err)
        except Exception as conn_err:
            logger.debug("Could not initialize adapter for %s: %s", conn.name, conn_err)

        # 2. Count incidents
        incident_count = (
            db.scalar(
                select(func.count())
                .select_from(Incident)
                .where(Incident.connection_id == conn.id)
            )
            or 0
        )

        # 3. Count workflow steps executed on this connection (if any)
        # Note: if adapter runs were 0, check workflow steps for fallback
        if success_runs == 0 and failed_runs == 0:
            step_counts = db.execute(
                select(WorkflowStep.status, func.count())
                .where(
                    WorkflowStep.node_type.in_(["pipeline.run_dag", "action.clear_failed_tasks", "check.sla"])
                )
                .group_by(WorkflowStep.status)
            ).all()
            for status, count in step_counts:
                if status == StepStatus.COMPLETED:
                    success_runs += count
                elif status == StepStatus.FAILED:
                    failed_runs += count

        total_runs = success_runs + failed_runs
        success_rate = _rate(success_runs, total_runs)

        status_val = (
            conn.last_health_status.value
            if hasattr(conn.last_health_status, "value")
            else str(conn.last_health_status)
        )
        env_val = (
            conn.environment.value
            if hasattr(conn.environment, "value")
            else str(conn.environment)
        )
        kind_val = (
            conn.kind.value if hasattr(conn.kind, "value") else str(conn.kind)
        )

        connector_items.append(
            ConnectorRunMetrics(
                id=str(conn.id),
                name=conn.name,
                type="airflow",
                environment=env_val,
                kind=kind_val,
                status=status_val,
                latency_ms=conn.last_latency_ms,
                total_runs=total_runs,
                success_runs=success_runs,
                failed_runs=failed_runs,
                success_rate=success_rate,
                active_runs=0,
                incident_count=incident_count,
                last_run_at=last_run_at or conn.last_checked_at or utcnow(),
                description=f"{len(dags)} DAG(s) monitored · {conn.base_url}",
            )
        )

    # ------------------------------------------------------------------ Database Connections
    db_conns = list(
        db.scalars(
            select(DatabaseConnection)
            .where(DatabaseConnection.is_active.is_(True))
            .order_by(DatabaseConnection.name)
        ).all()
    )

    for d in db_conns:
        # Check workflow queries run against this DB
        conn_str = str(d.id)
        # Database connection health is reported separately from workflow execution
        # history because workflow steps do not carry a connection id.
        success_runs = 0
        failed_runs = 0

        total_runs = success_runs + failed_runs
        success_rate = _rate(success_runs, total_runs)
        status_val = (
            d.last_health_status.value
            if hasattr(d.last_health_status, "value")
            else str(d.last_health_status)
        )
        env_val = (
            d.environment.value
            if hasattr(d.environment, "value")
            else str(d.environment)
        )
        engine_val = (
            d.engine.value if hasattr(d.engine, "value") else str(d.engine)
        )

        connector_items.append(
            ConnectorRunMetrics(
                id=str(d.id),
                name=d.name,
                type="database",
                environment=env_val,
                kind=engine_val,
                status=status_val,
                latency_ms=d.last_latency_ms,
                total_runs=total_runs,
                success_runs=success_runs,
                failed_runs=failed_runs,
                success_rate=success_rate,
                active_runs=0,
                incident_count=0,
                last_run_at=d.last_checked_at or utcnow(),
                description=f"{engine_val} · {d.host}:{d.port}/{d.database}",
            )
        )

    # ------------------------------------------------------------------ Notification Channels
    channels = list(
        db.scalars(
            select(NotificationChannel)
            .where(NotificationChannel.is_active.is_(True))
            .order_by(NotificationChannel.name)
        ).all()
    )

    for ch in channels:
        notify_completed = (
            db.scalar(
                select(func.count())
                .select_from(WorkflowStep)
                .where(
                    WorkflowStep.node_type == "notify",
                    WorkflowStep.status == StepStatus.COMPLETED,
                )
            )
            or 0
        )
        notify_failed = (
            db.scalar(
                select(func.count())
                .select_from(WorkflowStep)
                .where(
                    WorkflowStep.node_type == "notify",
                    WorkflowStep.status == StepStatus.FAILED,
                )
            )
            or 0
        )

        total_runs = notify_completed + notify_failed
        success_rate = _rate(notify_completed, total_runs)
        kind_val = ch.kind.value if hasattr(ch.kind, "value") else str(ch.kind)
        channel_status = ch.last_status.value if hasattr(ch.last_status, "value") else str(ch.last_status)

        connector_items.append(
            ConnectorRunMetrics(
                id=str(ch.id),
                name=ch.name,
                type="channel",
                environment="DEV",
                kind=kind_val,
                status=channel_status if ch.is_active else "UNKNOWN",
                latency_ms=None,
                total_runs=total_runs,
                success_runs=notify_completed,
                failed_runs=notify_failed,
                success_rate=success_rate,
                active_runs=0,
                incident_count=0,
                last_run_at=ch.updated_at or utcnow(),
                description=f"{kind_val} notification destination",
            )
        )

    # Calculate overall aggregates
    overall_total = sum(c.total_runs for c in connector_items)
    overall_success = sum(c.success_runs for c in connector_items)
    overall_failed = sum(c.failed_runs for c in connector_items)
    overall_rate = _rate(overall_success, overall_total)

    # Sort connectors by total runs descending by default
    connector_items.sort(key=lambda c: (c.total_runs, c.failed_runs), reverse=True)

    return ConnectorMetricsResponse(
        total_connectors=len(connector_items),
        total_runs=overall_total,
        total_success=overall_success,
        total_failed=overall_failed,
        overall_success_rate=overall_rate,
        total_executions=total_executions,
        successful_executions=successful_executions,
        failed_executions=failed_executions,
        execution_success_rate=execution_success_rate,
        connectors=connector_items,
    )
