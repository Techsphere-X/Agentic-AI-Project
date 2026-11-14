"""Webhooks: other systems start 'When a webhook is called' workflows through a secret URL.

No login: the token in the URL is the credential. Any problem answers 404, so the URL cannot
be probed; calls are rate limited per workflow and audited.
"""

import json
import uuid
from typing import Any

from fastapi import APIRouter, Request

from app.core.dependencies import ClientIp, DbSession
from app.core.exceptions import BadRequestError
from app.services import automation_service

router = APIRouter(prefix="/hooks", tags=["webhooks"])


@router.post("/{workflow_id}/{token}", status_code=202)
async def call_webhook(
    workflow_id: uuid.UUID, token: str, request: Request, db: DbSession, ip: ClientIp
) -> dict[str, Any]:
    raw = await request.body()
    if len(raw) > automation_service.WEBHOOK_MAX_BODY_BYTES:
        raise BadRequestError("The body is too large (at most 64 KB)", status_code=413)
    try:
        body = json.loads(raw) if raw.strip() else {}
    except ValueError as exc:
        raise BadRequestError("The body must be JSON") from exc
    run = automation_service.receive_webhook(db, workflow_id, token, body, ip_address=ip)
    return {"run_id": str(run.id), "status": run.status.value}
