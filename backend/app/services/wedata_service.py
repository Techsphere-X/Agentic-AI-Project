"""Service layer for Tencent Cloud WeData connection management, task sync, and monitoring."""

import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import func, or_, select, update
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.crypto import SecretDecryptionError, decrypt_secret, encrypt_secret
from app.core.exceptions import BadRequestError, ConflictError, NotFoundError, UpstreamError
from app.db.base import utcnow
from app.models.airflow import ConnectionKind
from app.models.user import User
from app.models.wedata import MonitoredWeDataTask, WeDataConnection
from app.orchestration.wedata.base import (
    WeDataAdapter,
    WeDataAdapterConfig,
    WeDataAdapterError,
    WeDataConnectionStatus,
    WeDataConnectionTestResult,
    WeDataInstanceRun,
    WeDataTaskLog,
    WeDataTaskSummary,
)
from app.orchestration.wedata.factory import build_adapter, run_async
from app.schemas.wedata import (
    WeDataConnCreate,
    WeDataConnTestRequest,
    WeDataConnUpdate,
    WeDataTaskSyncResult,
)
from app.services import audit_service

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WeDataProbeOutcome:
    result: WeDataConnectionTestResult
    checked_at: datetime


def is_live(conn: WeDataConnection) -> bool:
    """True when the connection is active, healthy, and recently checked."""
    return (
        conn.is_active
        and conn.last_health_status == WeDataConnectionStatus.HEALTHY
        and not conn.status_stale
    )


def adapter_for(conn: WeDataConnection) -> WeDataAdapter:
    """Build a WeDataAdapter instance from a stored WeDataConnection."""
    secret_key = None
    if conn.encrypted_secret_key:
        try:
            secret_key = decrypt_secret(conn.encrypted_secret_key)
        except SecretDecryptionError:
            raise UpstreamError("Could not decrypt WeData secret key.")

    config = WeDataAdapterConfig(
        project_id=conn.project_id,
        region=conn.region,
        endpoint=conn.endpoint,
        secret_id=conn.secret_id,
        secret_key=secret_key,
    )
    return build_adapter(mock=conn.kind == ConnectionKind.MOCK, config=config)


# ---------------------------------------------------------------------- connection CRUD


def list_connections(
    db: Session, *, limit: int = 50, offset: int = 0
) -> tuple[list[WeDataConnection], int]:
    stmt = (
        select(WeDataConnection)
        .order_by(WeDataConnection.is_default.desc(), WeDataConnection.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    items = list(db.scalars(stmt).all())
    total = db.scalar(select(func.count()).select_from(WeDataConnection)) or 0
    return items, total


def get_connection(db: Session, conn_id: uuid.UUID) -> WeDataConnection:
    conn = db.get(WeDataConnection, conn_id)
    if conn is None:
        raise NotFoundError("WeData connection not found")
    return conn


def create_connection(
    db: Session, body: WeDataConnCreate, actor: User, ip: str | None
) -> WeDataConnection:
    settings = get_settings()
    if body.kind == ConnectionKind.MOCK and settings.ENVIRONMENT == "production":
        raise BadRequestError("Mock connections are disabled in production")

    existing = db.scalar(select(WeDataConnection).where(WeDataConnection.name == body.name))
    if existing:
        raise ConflictError(f"Connection '{body.name}' already exists")

    if body.is_default:
        db.execute(update(WeDataConnection).values(is_default=False))

    encrypted_key = encrypt_secret(body.secret_key) if body.secret_key else None
    conn = WeDataConnection(
        name=body.name,
        environment=body.environment,
        kind=body.kind,
        project_id=body.project_id,
        region=body.region,
        endpoint=body.endpoint,
        secret_id=body.secret_id,
        encrypted_secret_key=encrypted_key,
        is_active=body.is_active,
        is_default=body.is_default,
        created_by=actor.id,
    )
    db.add(conn)
    db.flush()

    audit_service.record(
        db,
        action="wedata_conn.create",
        entity_type="wedata_connection",
        entity_id=conn.id,
        actor=actor,
        ip_address=ip,
        details={"name": conn.name, "project_id": conn.project_id, "region": conn.region},
    )
    db.commit()
    db.refresh(conn)
    return conn


def update_connection(
    db: Session, conn_id: uuid.UUID, body: WeDataConnUpdate, actor: User, ip: str | None
) -> WeDataConnection:
    conn = get_connection(db, conn_id)
    updates = body.model_dump(exclude_unset=True)

    if "name" in updates and updates["name"] != conn.name:
        existing = db.scalar(select(WeDataConnection).where(WeDataConnection.name == updates["name"]))
        if existing and existing.id != conn.id:
            raise ConflictError(f"Connection '{updates['name']}' already exists")
        conn.name = updates["name"]

    if "secret_key" in updates:
        raw_key = updates.pop("secret_key")
        conn.encrypted_secret_key = encrypt_secret(raw_key) if raw_key else None

    if updates.get("is_default") is True and not conn.is_default:
        db.execute(update(WeDataConnection).values(is_default=False))

    for k, v in updates.items():
        if hasattr(conn, k):
            setattr(conn, k, v)

    audit_service.record(
        db,
        action="wedata_conn.update",
        entity_type="wedata_connection",
        entity_id=conn.id,
        actor=actor,
        ip_address=ip,
        details={"updated_fields": list(updates.keys())},
    )
    db.commit()
    db.refresh(conn)
    return conn


def delete_connection(db: Session, conn_id: uuid.UUID, actor: User, ip: str | None) -> None:
    conn = get_connection(db, conn_id)
    audit_service.record(
        db,
        action="wedata_conn.delete",
        entity_type="wedata_connection",
        entity_id=conn.id,
        actor=actor,
        ip_address=ip,
        details={"name": conn.name, "project_id": conn.project_id},
    )
    db.delete(conn)
    db.commit()


# ---------------------------------------------------------------------- probe & sync


def test_target(body: WeDataConnTestRequest) -> WeDataProbeOutcome:
    config = WeDataAdapterConfig(
        project_id=body.project_id,
        region=body.region,
        endpoint=body.endpoint,
        secret_id=body.secret_id,
        secret_key=body.secret_key,
    )
    adapter = build_adapter(mock=body.kind == ConnectionKind.MOCK, config=config)
    result = run_async(adapter.probe)
    return WeDataProbeOutcome(result=result, checked_at=utcnow())


def probe_connection(db: Session, conn: WeDataConnection) -> WeDataProbeOutcome:
    adapter = adapter_for(conn)
    result = run_async(adapter.probe)
    now = utcnow()

    conn.last_health_status = result.status
    conn.last_health_message = result.message
    conn.last_latency_ms = result.latency_ms
    conn.last_checked_at = now
    db.commit()
    db.refresh(conn)

    return WeDataProbeOutcome(result=result, checked_at=now)


def sync_tasks(
    db: Session, conn: WeDataConnection, actor: User | None = None, ip: str | None = None
) -> WeDataTaskSyncResult:
    """Fetch tasks from WeData and update the local monitored tasks registry."""
    adapter = adapter_for(conn)
    try:
        remote_tasks = run_async(adapter.list_tasks)
    except WeDataAdapterError as e:
        conn.last_health_status = e.result.status
        conn.last_health_message = e.result.message
        db.commit()
        raise UpstreamError(f"WeData sync failed: {e.result.message}")

    now = utcnow()
    existing_tasks = {
        t.task_id: t
        for t in db.scalars(
            select(MonitoredWeDataTask).where(MonitoredWeDataTask.connection_id == conn.id)
        ).all()
    }

    created = 0
    updated = 0
    seen_ids = set()

    for rt in remote_tasks:
        seen_ids.add(rt.task_id)
        if rt.task_id in existing_tasks:
            local = existing_tasks[rt.task_id]
            local.task_name = rt.task_name
            local.workflow_id = rt.workflow_id
            local.workflow_name = rt.workflow_name
            local.task_type = rt.task_type
            local.schedule_cron = rt.schedule_cron
            local.is_paused = rt.is_paused
            local.is_present_in_wedata = True
            local.last_synced_at = now
            updated += 1
        else:
            new_task = MonitoredWeDataTask(
                connection_id=conn.id,
                task_id=rt.task_id,
                task_name=rt.task_name,
                workflow_id=rt.workflow_id,
                workflow_name=rt.workflow_name,
                task_type=rt.task_type,
                schedule_cron=rt.schedule_cron,
                is_paused=rt.is_paused,
                is_monitored=False,
                is_present_in_wedata=True,
                last_synced_at=now,
            )
            db.add(new_task)
            created += 1

    missing = 0
    for tid, local in existing_tasks.items():
        if tid not in seen_ids:
            local.is_present_in_wedata = False
            missing += 1

    conn.last_health_status = WeDataConnectionStatus.HEALTHY
    conn.last_checked_at = now
    db.commit()

    if actor:
        audit_service.record(
            db,
            action="wedata.tasks_synced",
            entity_type="wedata_connection",
            entity_id=conn.id,
            actor=actor,
            ip_address=ip,
            details={"created": created, "updated": updated, "missing": missing},
        )
        db.commit()

    return WeDataTaskSyncResult(
        total=len(remote_tasks),
        created=created,
        updated=updated,
        missing=missing,
        synced_at=now,
    )


def list_tasks(
    db: Session, conn_id: uuid.UUID, *, monitored_only: bool = False
) -> list[MonitoredWeDataTask]:
    stmt = (
        select(MonitoredWeDataTask)
        .where(MonitoredWeDataTask.connection_id == conn_id)
        .order_by(MonitoredWeDataTask.task_name.asc())
    )
    if monitored_only:
        stmt = stmt.where(MonitoredWeDataTask.is_monitored.is_(True))
    return list(db.scalars(stmt).all())


def set_task_monitored(
    db: Session, conn_id: uuid.UUID, task_id: str, is_monitored: bool, actor: User, ip: str | None
) -> MonitoredWeDataTask:
    task = db.scalar(
        select(MonitoredWeDataTask).where(
            MonitoredWeDataTask.connection_id == conn_id,
            MonitoredWeDataTask.task_id == task_id,
        )
    )
    if task is None:
        raise NotFoundError("WeData task not found")

    task.is_monitored = is_monitored
    audit_service.record(
        db,
        action="wedata.task_monitored_changed",
        entity_type="wedata_task",
        entity_id=task.id,
        actor=actor,
        ip_address=ip,
        details={"task_id": task.task_id, "is_monitored": is_monitored},
    )
    db.commit()
    db.refresh(task)
    return task


# ---------------------------------------------------------------------- instances & actions


def list_task_instances(
    db: Session, conn_id: uuid.UUID, task_id: str, *, limit: int = 20
) -> list[WeDataInstanceRun]:
    conn = get_connection(db, conn_id)
    adapter = adapter_for(conn)
    return run_async(lambda: adapter.list_task_instances(task_id, limit=limit))


def get_task_log(
    db: Session, conn_id: uuid.UUID, task_id: str, run_id: str, *, max_bytes: int = 65536
) -> WeDataTaskLog:
    conn = get_connection(db, conn_id)
    adapter = adapter_for(conn)
    return run_async(lambda: adapter.get_task_log(task_id, run_id, max_bytes=max_bytes))


def rerun_task_instance(
    db: Session, conn_id: uuid.UUID, task_id: str, run_id: str, actor: User, ip: str | None
) -> bool:
    conn = get_connection(db, conn_id)
    adapter = adapter_for(conn)
    success = run_async(lambda: adapter.rerun_instance(task_id, run_id))
    audit_service.record(
        db,
        action="wedata.task_instance_rerun",
        entity_type="wedata_task",
        entity_id=conn.id,
        actor=actor,
        ip_address=ip,
        details={"task_id": task_id, "run_id": run_id, "success": success},
    )
    db.commit()
    return success


@dataclass
class WeDataCheckSummary:
    healthy: int = 0
    failing: int = 0
    transitions: list[dict[str, str]] = None

    def __post_init__(self):
        if self.transitions is None:
            self.transitions = []


def check_all(db: Session, *, actor: User | None = None) -> WeDataCheckSummary:
    """Check health for all active WeData connections during monitor cycle."""
    summary = WeDataCheckSummary()
    conns = db.scalars(
        select(WeDataConnection)
        .where(WeDataConnection.is_active.is_(True))
        .order_by(WeDataConnection.name)
    ).all()
    for conn in conns:
        before = conn.last_health_status
        try:
            outcome = probe_connection(db, conn)
            status = outcome.result.status
        except Exception as e:
            status = WeDataConnectionStatus.WEDATA_ERROR
            conn.last_health_status = status
            conn.last_health_message = str(e)
            conn.last_checked_at = utcnow()
            db.commit()

        if status == WeDataConnectionStatus.HEALTHY:
            summary.healthy += 1
        else:
            summary.failing += 1

        if before != status:
            audit_service.record(
                db,
                action="wedata_conn.status_changed",
                entity_type="wedata_connection",
                entity_id=conn.id,
                actor=actor,
                details={"from": before, "to": status, "message": conn.last_health_message},
            )
            summary.transitions.append(
                {"connection": conn.name, "from": str(before), "to": str(status)}
            )
            log = logger.info if status == WeDataConnectionStatus.HEALTHY else logger.warning
            log("WeData connection %s: %s -> %s", conn.name, before, status)
    return summary

