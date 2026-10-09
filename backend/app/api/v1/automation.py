import uuid
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Query, Request

from app.automation.graph import catalog
from app.automation.templates import ALL_TEMPLATES
from app.automation.types import ApprovalStatus, DagRunCheckStatus, RunStatus
from app.core.config import get_settings
from app.core.dependencies import (
    AdminUser,
    ClientIp,
    DbSession,
    OperatorUser,
    PageParams,
    ViewerUser,
)
from app.models.automation import Workflow
from app.schemas.automation import (
    ApprovalRead,
    AutomationStatus,
    AutomationSummary,
    DagRunCheckRead,
    DecisionRequest,
    GraphCheck,
    GraphCheckResult,
    ManualRunRequest,
    MarkedRead,
    NodeTypeRead,
    NotificationRead,
    TemplatePreview,
    TemplateRead,
    TickSummaryRead,
    WorkflowCreate,
    WorkflowRead,
    WorkflowRunDetail,
    WorkflowRunRead,
    WorkflowUpdate,
)
from app.schemas.common import Page
from app.services import automation_service

router = APIRouter(prefix="/automation", tags=["automation"])


def _workflow(
    workflow: Workflow, run_count: int = 0, last_run_at: datetime | None = None
) -> WorkflowRead:
    return WorkflowRead.model_validate(workflow).model_copy(
        update={"run_count": run_count, "last_run_at": last_run_at}
    )


# ---------------------------------------------------------------------- catalog


@router.get("/node-types", response_model=list[NodeTypeRead])
def node_types(_: ViewerUser) -> list[NodeTypeRead]:
    return [NodeTypeRead(**entry) for entry in catalog()]


@router.get("/templates", response_model=list[TemplateRead])
def templates(_: ViewerUser) -> list[TemplateRead]:
    return [
        TemplateRead(
            key=t.key,
            name=t.name,
            description=t.description,
            version=t.version,
            graph=t.graph,
            parameter_schema=t.parameter_schema,
            supported_trigger_types=list(
                t.supported_trigger_types
                or tuple(
                    node["type"]
                    for node in (t.graph or {}).get("nodes", [])
                    if str(node.get("type", "")).startswith("trigger.")
                )
            ),
        )
        for t in ALL_TEMPLATES
    ]


@router.post("/templates/{template_key}/preview", response_model=GraphCheckResult)
def preview_template(
    template_key: str, body: TemplatePreview, _: ViewerUser
) -> GraphCheckResult:
    _, graph, _ = automation_service.build_template(template_key, body.template_parameters)
    return GraphCheckResult(valid=True, problems=[], graph=graph)


# ---------------------------------------------------------------------- workflows


@router.get("/workflows", response_model=list[WorkflowRead])
def list_workflows(db: DbSession, _: ViewerUser) -> list[WorkflowRead]:
    return [_workflow(*row) for row in automation_service.list_workflows(db)]


@router.post("/workflows", response_model=WorkflowRead, status_code=201)
def create_workflow(
    body: WorkflowCreate, db: DbSession, admin: AdminUser, ip: ClientIp
) -> WorkflowRead:
    workflow = automation_service.create_workflow(
        db,
        actor=admin,
        template_key=body.template_key,
        name=body.name,
        description=body.description,
        graph=body.graph,
        template_parameters=body.template_parameters,
        ip_address=ip,
    )
    return _workflow(workflow)


@router.post("/workflows/validate", response_model=GraphCheckResult)
def validate_workflow_graph(body: GraphCheck, _: ViewerUser) -> GraphCheckResult:
    graph, problems = automation_service.check_graph(body.graph)
    return GraphCheckResult(valid=not problems, problems=problems, graph=graph)


@router.delete("/workflows/{workflow_id}", status_code=204)
def delete_workflow(workflow_id: uuid.UUID, db: DbSession, admin: AdminUser, ip: ClientIp) -> None:
    automation_service.delete_workflow(db, workflow_id, actor=admin, ip_address=ip)


@router.get("/workflows/{workflow_id}", response_model=WorkflowRead)
def get_workflow(workflow_id: uuid.UUID, db: DbSession, _: ViewerUser) -> WorkflowRead:
    return _workflow(automation_service.get_workflow(db, workflow_id))


@router.patch("/workflows/{workflow_id}", response_model=WorkflowRead)
def update_workflow(
    workflow_id: uuid.UUID, body: WorkflowUpdate, db: DbSession, admin: AdminUser, ip: ClientIp
) -> WorkflowRead:
    workflow = automation_service.update_workflow(
        db, workflow_id, body.model_dump(exclude_unset=True), actor=admin, ip_address=ip
    )
    return _workflow(workflow)


# ---------------------------------------------------------------------- runs


@router.get("/runs", response_model=Page[WorkflowRunRead])
def list_runs(
    db: DbSession,
    _: ViewerUser,
    page: PageParams,
    status: Annotated[list[RunStatus] | None, Query()] = None,
    workflow_id: uuid.UUID | None = None,
    incident_id: uuid.UUID | None = None,
) -> Page[WorkflowRunRead]:
    items, total = automation_service.list_runs(
        db,
        statuses=status,
        workflow_id=workflow_id,
        incident_id=incident_id,
        limit=page.limit,
        offset=page.offset,
    )
    return Page[WorkflowRunRead](
        items=[WorkflowRunRead.model_validate(r) for r in items],
        total=total,
        limit=page.limit,
        offset=page.offset,
    )


@router.get("/run-checks", response_model=Page[DagRunCheckRead])
def list_run_checks(
    db: DbSession,
    _: ViewerUser,
    page: PageParams,
    status: Annotated[list[DagRunCheckStatus] | None, Query()] = None,
    dag_id: str | None = None,
) -> Page[DagRunCheckRead]:
    """Validation status of finished DAG runs, newest first."""
    items, total = automation_service.list_run_checks(
        db, dag_id=dag_id, statuses=status, limit=page.limit, offset=page.offset
    )
    return Page[DagRunCheckRead](
        items=[DagRunCheckRead.model_validate(c) for c in items],
        total=total,
        limit=page.limit,
        offset=page.offset,
    )


@router.get("/run-checks/latest", response_model=list[DagRunCheckRead])
def latest_run_checks(db: DbSession, _: ViewerUser) -> list[DagRunCheckRead]:
    """The newest check of each DAG (status badges)."""
    checks = automation_service.latest_checks(db).values()
    return [DagRunCheckRead.model_validate(c) for c in checks]


@router.post("/workflows/{workflow_id}/run", response_model=WorkflowRunDetail, status_code=201)
def run_workflow(
    workflow_id: uuid.UUID,
    db: DbSession,
    operator: OperatorUser,
    body: ManualRunRequest | None = None,
) -> WorkflowRunDetail:
    """Run now, for workflows started on demand or on a schedule."""
    run = automation_service.start_manual_run(
        db, workflow_id, actor=operator, dry_run=body.dry_run if body else None
    )
    db.expire_all()
    return WorkflowRunDetail.model_validate(automation_service.get_run(db, run.id))


@router.get("/runs/{run_id}", response_model=WorkflowRunDetail)
def get_run(run_id: uuid.UUID, db: DbSession, _: ViewerUser) -> WorkflowRunDetail:
    return WorkflowRunDetail.model_validate(automation_service.get_run(db, run_id))


@router.post("/runs/{run_id}/cancel", response_model=WorkflowRunDetail)
def cancel_run(
    run_id: uuid.UUID, db: DbSession, operator: OperatorUser, ip: ClientIp
) -> WorkflowRunDetail:
    automation_service.cancel_run(db, run_id, actor=operator, ip_address=ip)
    db.expire_all()
    return WorkflowRunDetail.model_validate(automation_service.get_run(db, run_id))


@router.get("/status", response_model=AutomationStatus)
def status(request: Request, _: ViewerUser) -> AutomationStatus:
    loop = request.app.state.automation_loop
    settings = get_settings()
    last = loop.last_summary
    return AutomationStatus(
        enabled=settings.AUTOMATION_ENABLED,
        force_dry_run=settings.AUTOMATION_FORCE_DRY_RUN,
        loop_running=loop.running_loop,
        interval_seconds=loop.interval_seconds,
        last_tick=TickSummaryRead(**last.as_dict())
        if isinstance(last, automation_service.TickSummary)
        else None,
        last_error=loop.last_error,
    )


@router.post("/tick", response_model=TickSummaryRead)
def tick(db: DbSession, _: OperatorUser) -> TickSummaryRead:
    return TickSummaryRead(**automation_service.tick(db).as_dict())


# ---------------------------------------------------------------------- approvals


@router.get("/approvals", response_model=Page[ApprovalRead])
def list_approvals(
    db: DbSession,
    _: ViewerUser,
    page: PageParams,
    status: Annotated[list[ApprovalStatus] | None, Query()] = None,
    incident_id: uuid.UUID | None = None,
) -> Page[ApprovalRead]:
    items, total = automation_service.list_approvals(
        db, statuses=status, incident_id=incident_id, limit=page.limit, offset=page.offset
    )
    return Page[ApprovalRead](
        items=[ApprovalRead.model_validate(a) for a in items],
        total=total,
        limit=page.limit,
        offset=page.offset,
    )


def _decide(
    approval_id: uuid.UUID,
    body: DecisionRequest,
    db: DbSession,
    operator: OperatorUser,
    ip: ClientIp,
    *,
    approve: bool,
) -> ApprovalRead:
    comment = body.comment.strip() if body.comment else None
    approval = automation_service.decide(
        db, approval_id, approve=approve, comment=comment or None, actor=operator, ip_address=ip
    )
    return ApprovalRead.model_validate(approval)


@router.post("/approvals/{approval_id}/approve", response_model=ApprovalRead)
def approve(
    approval_id: uuid.UUID,
    body: DecisionRequest,
    db: DbSession,
    operator: OperatorUser,
    ip: ClientIp,
) -> ApprovalRead:
    return _decide(approval_id, body, db, operator, ip, approve=True)


@router.post("/approvals/{approval_id}/reject", response_model=ApprovalRead)
def reject(
    approval_id: uuid.UUID,
    body: DecisionRequest,
    db: DbSession,
    operator: OperatorUser,
    ip: ClientIp,
) -> ApprovalRead:
    return _decide(approval_id, body, db, operator, ip, approve=False)


# ---------------------------------------------------------------------- notifications & summary


@router.get("/notifications", response_model=Page[NotificationRead])
def list_notifications(
    db: DbSession, _: ViewerUser, page: PageParams, unread_only: bool = False
) -> Page[NotificationRead]:
    items, total = automation_service.list_notifications(
        db, unread_only=unread_only, limit=page.limit, offset=page.offset
    )
    return Page[NotificationRead](
        items=[NotificationRead.model_validate(n) for n in items],
        total=total,
        limit=page.limit,
        offset=page.offset,
    )


@router.post("/notifications/read-all", response_model=MarkedRead)
def read_all(db: DbSession, _: ViewerUser) -> MarkedRead:
    return MarkedRead(updated=automation_service.mark_all_read(db))


@router.get("/summary", response_model=AutomationSummary)
def summary(db: DbSession, _: ViewerUser) -> AutomationSummary:
    return AutomationSummary(**automation_service.summary(db))
