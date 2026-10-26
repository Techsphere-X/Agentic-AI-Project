"""Connection monitor: keeps each active connection's health and DAG list current.

Database connections from the catalog are health-checked in the same cycle (see
`database_connection_service.check_all`).

Every cycle lists DAGs live from every active connection (concurrently, so one slow or dead
Airflow does not delay the others), then records the outcome: success refreshes
`monitored_dags` and marks the connection HEALTHY; failure stores the failure status. The UI
only shows a connection's DAGs while `airflow_service.is_live` holds, so users never see a stale
DAG list presented as current.
"""

import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime

import anyio
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.core.exceptions import AppError
from app.db.base import utcnow
from app.models.airflow import AirflowConnection
from app.models.user import User
from app.orchestration.airflow.base import (
    AirflowAdapter,
    AirflowAdapterError,
    AirflowDagSummary,
    ConnectionStatus,
    ConnectionTestResult,
)
from app.orchestration.airflow.factory import run_async
from app.services import airflow_service, audit_service, database_connection_service

logger = logging.getLogger(__name__)

LEASE_NAME = "connections"

# Adapter construction errors (bad stored config) mapped to the closest connection status.
_CONFIG_ERROR_STATUS = {
    "secret_undecryptable": ConnectionStatus.UNAUTHORIZED,
    "host_not_allowed": ConnectionStatus.INVALID_ENDPOINT,
}


@dataclass
class _Fetch:
    """Outcome of one live DAG listing: either `dags` or a failure `result`."""

    dags: list[AirflowDagSummary] | None = None
    result: ConnectionTestResult | None = None
    latency_ms: int | None = None


@dataclass
class CycleSummary:
    trigger: str
    started_at: datetime
    connections: int = 0
    healthy: int = 0
    failing: int = 0
    skipped: int = 0
    databases_healthy: int = 0
    databases_failing: int = 0
    transitions: list[dict[str, str]] = field(default_factory=list)
    errors: list[dict[str, str]] = field(default_factory=list)


async def _fetch(adapter: AirflowAdapter) -> _Fetch:
    started = time.perf_counter()
    logger.debug("Polling Airflow DAG list")
    try:
        dags = await adapter.list_dags()
    except AirflowAdapterError as exc:
        return _Fetch(result=exc.result)
    except Exception:  # an adapter bug must not stop the other connections
        logger.exception("Unexpected error listing DAGs")
        return _Fetch(
            result=ConnectionTestResult(
                status=ConnectionStatus.AIRFLOW_ERROR,
                message="Unexpected error while contacting Airflow. See server logs.",
            )
        )
    return _Fetch(dags=list(dags), latency_ms=int((time.perf_counter() - started) * 1000))


def _fetch_all(adapters: dict[str, AirflowAdapter]) -> dict[str, _Fetch]:
    results: dict[str, _Fetch] = {}

    async def one(key: str, adapter: AirflowAdapter) -> None:
        results[key] = await _fetch(adapter)

    async def all_() -> None:
        async with anyio.create_task_group() as tg:
            for key, adapter in adapters.items():
                tg.start_soon(one, key, adapter)

    if adapters:
        run_async(all_)
    return results


def _record(
    db: Session,
    conn: AirflowConnection,
    fetch: _Fetch,
    now: datetime,
    actor: User | None,
    summary: CycleSummary | None,
) -> None:
    before = conn.last_health_status
    if fetch.dags is not None:
        airflow_service.mark_healthy(conn, fetch.latency_ms, now)
        airflow_service.apply_remote_dags(db, conn, fetch.dags, now)
    else:
        assert fetch.result is not None
        airflow_service.store_health(conn, fetch.result, now)
    after = conn.last_health_status
    if before != after:
        audit_service.record(
            db,
            action="airflow_conn.status_changed",
            entity_type="airflow_connection",
            entity_id=conn.id,
            actor=actor,
            details={"from": before, "to": after, "message": conn.last_health_message},
        )
        if summary is not None:
            summary.transitions.append({"connection": conn.name, "from": before, "to": after})
        log = logger.info if after == ConnectionStatus.HEALTHY else logger.warning
        log("Airflow connection %s: %s -> %s", conn.name, before, after)


def _adapters(
    conns: Sequence[AirflowConnection],
) -> tuple[dict[str, AirflowAdapter], dict[str, _Fetch]]:
    """Build adapters; connections whose stored config is unusable get a failure right away."""
    adapters: dict[str, AirflowAdapter] = {}
    failed: dict[str, _Fetch] = {}
    for conn in conns:
        try:
            adapters[str(conn.id)] = airflow_service.adapter_for(conn)
        except AppError as exc:
            status = _CONFIG_ERROR_STATUS.get(exc.code, ConnectionStatus.AIRFLOW_ERROR)
            failed[str(conn.id)] = _Fetch(
                result=ConnectionTestResult(status=status, message=exc.message)
            )
    return adapters, failed


def refresh_connection(
    db: Session, conn: AirflowConnection, *, actor: User | None = None
) -> AirflowConnection:
    """Check one connection now (the UI's "Retry now"). Commits."""
    adapters, fetches = _adapters([conn])
    fetches.update(_fetch_all(adapters))
    _record(db, conn, fetches[str(conn.id)], utcnow(), actor, None)
    db.commit()
    db.refresh(conn)
    return conn


def run_cycle(db: Session, *, trigger: str = "schedule", actor: User | None = None) -> CycleSummary:
    summary = CycleSummary(trigger=trigger, started_at=utcnow())
    conns = db.scalars(
        select(AirflowConnection)
        .where(AirflowConnection.is_active.is_(True))
        .order_by(AirflowConnection.name)
    ).all()
    now = utcnow()
    due = [conn for conn in conns if airflow_service.dag_sync_due(conn, now)]
    summary.skipped = len(conns) - len(due)
    adapters, fetches = _adapters(due)
    fetches.update(_fetch_all(adapters))
    finished = utcnow()  # when the answers arrived; slow peers must not look older than they are

    for conn in conns:
        summary.connections += 1
        if str(conn.id) not in fetches:
            logger.debug("Skipped Airflow polling for %s; DAG state is still fresh", conn.name)
            continue
        fetch = fetches[str(conn.id)]
        try:
            _record(db, conn, fetch, finished, actor, summary)
            db.commit()
        except SQLAlchemyError as exc:  # e.g. a concurrent manual sync inserted the same DAG
            db.rollback()
            logger.warning("Could not record status for %s: %s", conn.name, exc)
            summary.errors.append({"connection": conn.name, "error": str(exc)[:500]})
            continue
        if fetch.dags is not None:
            summary.healthy += 1
        else:
            summary.failing += 1

    try:
        databases = database_connection_service.check_all(db, actor=actor)
    except SQLAlchemyError as exc:
        db.rollback()
        logger.warning("Could not record database connection status: %s", exc)
        summary.errors.append({"connection": "databases", "error": str(exc)[:500]})
    else:
        summary.databases_healthy = databases.healthy
        summary.databases_failing = databases.failing
        summary.transitions.extend(databases.transitions)
    return summary
