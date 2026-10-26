import uuid

from fastapi import APIRouter, status

from app.core.dependencies import (
    AdminUser,
    ClientIp,
    DbSession,
    OperatorUser,
    PageParams,
    ViewerUser,
)
from app.schemas.common import Page
from app.schemas.wedata import (
    WeDataConnCreate,
    WeDataConnRead,
    WeDataConnTestRequest,
    WeDataConnUpdate,
    WeDataInstanceRunRead,
    WeDataTaskMonitorUpdate,
    WeDataTaskRead,
    WeDataTaskSyncResult,
    WeDataTestResultRead,
)
from app.services import wedata_service
from app.services.wedata_service import WeDataProbeOutcome

router = APIRouter(prefix="/wedata", tags=["wedata"])


def _test_result(outcome: WeDataProbeOutcome) -> WeDataTestResultRead:
    r = outcome.result
    return WeDataTestResultRead(
        status=r.status,
        ok=r.ok,
        message=r.message,
        latency_ms=r.latency_ms,
        project_id=r.project_id,
        project_name=r.project_name,
        checked_at=outcome.checked_at,
    )


# ---------------------------------------------------------------------- connections


@router.get("/connections", response_model=Page[WeDataConnRead])
def list_connections(db: DbSession, _: ViewerUser, page: PageParams) -> Page[WeDataConnRead]:
    items, total = wedata_service.list_connections(db, limit=page.limit, offset=page.offset)
    return Page[WeDataConnRead](
        items=[WeDataConnRead.model_validate(c) for c in items],
        total=total,
        limit=page.limit,
        offset=page.offset,
    )


@router.post("/connections", response_model=WeDataConnRead, status_code=status.HTTP_201_CREATED)
def create_connection(
    body: WeDataConnCreate, db: DbSession, admin: AdminUser, ip: ClientIp
) -> WeDataConnRead:
    conn = wedata_service.create_connection(db, body, actor=admin, ip=ip)
    return WeDataConnRead.model_validate(conn)


@router.get("/connections/{connection_id}", response_model=WeDataConnRead)
def get_connection(connection_id: uuid.UUID, db: DbSession, _: ViewerUser) -> WeDataConnRead:
    return WeDataConnRead.model_validate(wedata_service.get_connection(db, connection_id))


@router.patch("/connections/{connection_id}", response_model=WeDataConnRead)
def update_connection(
    connection_id: uuid.UUID,
    body: WeDataConnUpdate,
    db: DbSession,
    admin: AdminUser,
    ip: ClientIp,
) -> WeDataConnRead:
    conn = wedata_service.update_connection(db, connection_id, body, actor=admin, ip=ip)
    return WeDataConnRead.model_validate(conn)


@router.delete("/connections/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_connection(
    connection_id: uuid.UUID, db: DbSession, admin: AdminUser, ip: ClientIp
) -> None:
    wedata_service.delete_connection(db, connection_id, actor=admin, ip=ip)


# ---------------------------------------------------------------------- testing & probe


@router.post("/test-connection", response_model=WeDataTestResultRead)
def test_unsaved_connection(
    body: WeDataConnTestRequest, _: OperatorUser
) -> WeDataTestResultRead:
    outcome = wedata_service.test_target(body)
    return _test_result(outcome)


@router.post("/connections/{connection_id}/test", response_model=WeDataTestResultRead)
def test_saved_connection(
    connection_id: uuid.UUID, db: DbSession, _: OperatorUser
) -> WeDataTestResultRead:
    conn = wedata_service.get_connection(db, connection_id)
    outcome = wedata_service.probe_connection(db, conn)
    return _test_result(outcome)


# ---------------------------------------------------------------------- tasks & sync


@router.post("/connections/{connection_id}/sync", response_model=WeDataTaskSyncResult)
def sync_tasks(
    connection_id: uuid.UUID, db: DbSession, operator: OperatorUser, ip: ClientIp
) -> WeDataTaskSyncResult:
    conn = wedata_service.get_connection(db, connection_id)
    return wedata_service.sync_tasks(db, conn, actor=operator, ip=ip)


@router.get("/connections/{connection_id}/tasks", response_model=list[WeDataTaskRead])
def list_tasks(
    connection_id: uuid.UUID, db: DbSession, _: ViewerUser, monitored_only: bool = False
) -> list[WeDataTaskRead]:
    tasks = wedata_service.list_tasks(db, connection_id, monitored_only=monitored_only)
    return [WeDataTaskRead.model_validate(t) for t in tasks]


@router.patch("/connections/{connection_id}/tasks/{task_id}", response_model=WeDataTaskRead)
def set_task_monitored(
    connection_id: uuid.UUID,
    task_id: str,
    body: WeDataTaskMonitorUpdate,
    db: DbSession,
    operator: OperatorUser,
    ip: ClientIp,
) -> WeDataTaskRead:
    task = wedata_service.set_task_monitored(
        db, connection_id, task_id, body.is_monitored, actor=operator, ip=ip
    )
    return WeDataTaskRead.model_validate(task)


# ---------------------------------------------------------------------- instances & remediation


@router.get(
    "/connections/{connection_id}/tasks/{task_id}/instances",
    response_model=list[WeDataInstanceRunRead],
)
def list_task_instances(
    connection_id: uuid.UUID, task_id: str, db: DbSession, _: ViewerUser, limit: int = 20
) -> list[WeDataInstanceRunRead]:
    runs = wedata_service.list_task_instances(db, connection_id, task_id, limit=limit)
    return [
        WeDataInstanceRunRead(
            run_id=r.run_id,
            task_id=r.task_id,
            state=r.normalized_state,
            raw_state=r.state,
            cur_run_date=r.cur_run_date,
            start_date=r.start_date.isoformat() if r.start_date else None,
            end_date=r.end_date.isoformat() if r.end_date else None,
        )
        for r in runs
    ]


@router.get("/connections/{connection_id}/tasks/{task_id}/instances/{run_id}/log")
def get_task_log(
    connection_id: uuid.UUID,
    task_id: str,
    run_id: str,
    db: DbSession,
    _: ViewerUser,
    max_bytes: int = 65536,
) -> dict[str, object]:
    log = wedata_service.get_task_log(db, connection_id, task_id, run_id, max_bytes=max_bytes)
    return {"content": log.content, "truncated": log.truncated, "available": log.available}


@router.post("/connections/{connection_id}/tasks/{task_id}/instances/{run_id}/rerun")
def rerun_task_instance(
    connection_id: uuid.UUID,
    task_id: str,
    run_id: str,
    db: DbSession,
    operator: OperatorUser,
    ip: ClientIp,
) -> dict[str, object]:
    success = wedata_service.rerun_task_instance(db, connection_id, task_id, run_id, actor=operator, ip=ip)
    return {"success": success}
