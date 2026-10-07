"""Notification channels: catalog CRUD, test messages, and delivery from workflow blocks.

HTTP channels (Slack/Teams/webhook) go through an httpx MockTransport; email goes to a fake
SMTP server. Nothing leaves the machine.
"""

import json
import smtplib

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.automation.graph import GraphError, validate_graph
from app.automation.types import RunStatus
from app.connectors import notify
from app.models.audit_log import AuditLog
from app.models.automation import Approval, Notification, Workflow, WorkflowRun
from app.models.notification_channel import ChannelStatus, NotificationChannel
from app.models.user import User
from app.services import automation_service

CHANNELS = "/api/v1/channels"
SLACK_URL = "https://hooks.slack.com/services/T000/B000/secret-token"
SLACK = {"name": "Ops Slack", "kind": "SLACK", "secret": SLACK_URL}
EMAIL = {
    "name": "Company email",
    "kind": "EMAIL",
    "email": {
        "smtp_host": "smtp.test",
        "smtp_port": 587,
        "security": "starttls",
        "username": "bot@example.com",
        "from_address": "bot@example.com",
        "default_recipients": ["oncall@example.com"],
    },
    "secret": "app-password",
}


class Http:
    """Records requests; `status` sets the reply."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.status = 200

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(self.status, text="ok" if self.status < 400 else "no_service")

    def last_json(self) -> dict:
        return json.loads(self.requests[-1].content)


class FakeSMTP:
    sent: list[dict] = []

    def __init__(self, host: str, port: int, *, timeout: float, ssl: bool) -> None:
        self.info = {"host": host, "port": port, "ssl": ssl, "tls": False, "login": None}

    def __enter__(self) -> "FakeSMTP":
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def starttls(self, context: object = None) -> None:
        self.info["tls"] = True

    def login(self, user: str, password: str) -> None:
        if password == "wrong":
            raise smtplib.SMTPAuthenticationError(535, b"5.7.8 bad credentials")
        self.info["login"] = user

    def send_message(self, message) -> None:
        FakeSMTP.sent.append(
            {
                **self.info,
                "to": message["To"],
                "subject": message["Subject"],
                "body": message.get_content(),
            }
        )


@pytest.fixture
def http(monkeypatch: pytest.MonkeyPatch) -> Http:
    fake = Http()
    monkeypatch.setattr(notify, "http_transport", httpx.MockTransport(fake.handler))
    return fake


@pytest.fixture
def smtp(monkeypatch: pytest.MonkeyPatch) -> type[FakeSMTP]:
    FakeSMTP.sent = []
    monkeypatch.setattr(notify, "smtp_factory", FakeSMTP)
    return FakeSMTP


def create(client: TestClient, headers: dict, body: dict) -> dict:
    response = client.post(CHANNELS, json=body, headers=headers)
    assert response.status_code == 201, response.text
    return response.json()


# ---------------------------------------------------------------------- catalog


def test_create_hides_the_secret(client: TestClient, admin_headers: dict, db: Session) -> None:
    channel = create(client, admin_headers, SLACK)
    assert channel["target"] == "hooks.slack.com/…"
    assert channel["has_secret"] is True
    assert "secret-token" not in client.get(CHANNELS, headers=admin_headers).text
    assert db.scalars(select(NotificationChannel)).one().encrypted_secret != SLACK_URL
    audit = db.scalars(select(AuditLog).where(AuditLog.action.like("notification_channel.%"))).all()
    assert audit and all("secret-token" not in str(a.details) for a in audit)


def test_validation_and_rbac(
    client: TestClient, admin_headers: dict, operator_headers: dict, viewer_headers: dict
) -> None:
    bad = client.post(
        CHANNELS, json={**SLACK, "secret": "https://evil.test/x"}, headers=admin_headers
    )
    assert bad.status_code == 400 and "hooks.slack.com" in bad.json()["error"]["message"]
    no_link = client.post(CHANNELS, json={"name": "t", "kind": "TEAMS"}, headers=admin_headers)
    assert no_link.status_code == 400
    assert client.post(CHANNELS, json=SLACK, headers=operator_headers).status_code == 403
    channel = create(client, admin_headers, SLACK)
    assert client.get(CHANNELS, headers=viewer_headers).status_code == 200
    assert (
        client.post(f"{CHANNELS}/{channel['id']}/test", headers=viewer_headers).status_code == 403
    )
    dup = client.post(CHANNELS, json={**SLACK, "name": "ops slack"}, headers=admin_headers)
    assert dup.status_code == 409


def test_slack_test_message(client: TestClient, admin_headers: dict, http: Http) -> None:
    channel = create(client, admin_headers, SLACK)
    result = client.post(f"{CHANNELS}/{channel['id']}/test", headers=admin_headers).json()
    assert result == {"ok": True, "message": "Sent to Ops Slack", "status": "OK"}
    assert str(http.requests[-1].url) == SLACK_URL
    payload = http.last_json()
    assert payload["text"].startswith("Test message from Agentic Ops")
    assert payload["blocks"][-1]["elements"][0]["url"].endswith("/connections/channels")


def test_failed_send_is_reported(client: TestClient, admin_headers: dict, http: Http) -> None:
    channel = create(client, admin_headers, SLACK)
    http.status = 404
    result = client.post(f"{CHANNELS}/{channel['id']}/test", headers=admin_headers).json()
    assert result["ok"] is False and result["status"] == "FAILED"
    assert result["message"] == "HTTP 404: no_service"


def test_teams_and_webhook_payloads(client: TestClient, admin_headers: dict, http: Http) -> None:
    teams = create(
        client,
        admin_headers,
        {"name": "Teams", "kind": "TEAMS", "secret": "https://prod.westus.logic.azure.com/wf"},
    )
    client.post(f"{CHANNELS}/{teams['id']}/test", headers=admin_headers)
    card = http.last_json()["attachments"][0]
    assert card["contentType"] == "application/vnd.microsoft.card.adaptive"
    assert card["content"]["body"][0]["text"] == "Test message from Agentic Ops"
    assert card["content"]["actions"][0]["type"] == "Action.OpenUrl"

    hook = create(
        client, admin_headers, {"name": "Hook", "kind": "WEBHOOK", "secret": "https://n8n.test/h"}
    )
    client.post(f"{CHANNELS}/{hook['id']}/test", headers=admin_headers)
    assert http.last_json()["title"] == "Test message from Agentic Ops"


def test_email_test_message(client: TestClient, admin_headers: dict, smtp: type[FakeSMTP]) -> None:
    channel = create(client, admin_headers, EMAIL)
    assert channel["target"] == "smtp.test:587"
    result = client.post(
        f"{CHANNELS}/{channel['id']}/test", json={"to": ["me@example.com"]}, headers=admin_headers
    ).json()
    assert result["ok"] is True and result["message"] == "Emailed me@example.com"
    sent = smtp.sent[-1]
    assert sent["tls"] is True and sent["login"] == "bot@example.com"
    assert sent["to"] == "me@example.com"
    assert sent["subject"] == "Test message from Agentic Ops"

    # No address given: the channel's default recipients.
    client.post(f"{CHANNELS}/{channel['id']}/test", headers=admin_headers)
    assert smtp.sent[-1]["to"] == "oncall@example.com"


def test_email_login_failure(client: TestClient, admin_headers: dict, smtp: type[FakeSMTP]) -> None:
    channel = create(client, admin_headers, {**EMAIL, "secret": "wrong"})
    result = client.post(f"{CHANNELS}/{channel['id']}/test", headers=admin_headers).json()
    assert result["ok"] is False
    assert result["message"] == "Email login failed: check the username and password"


# ---------------------------------------------------------------------- workflow blocks


def _channel(db: Session, admin: User, body: dict) -> NotificationChannel:
    from app.schemas.channel import ChannelCreate
    from app.services import notification_channel_service

    return notification_channel_service.create_channel(
        db, ChannelCreate(**body), actor=admin, ip_address=None
    )


def _workflow(db: Session, *nodes: dict, ports: dict | None = None) -> Workflow:
    all_nodes = [{"id": "trigger", "type": "trigger.manual", "config": {}}, *nodes]
    edges = [
        {"from": a["id"], "port": (ports or {}).get(a["id"], "next"), "to": b["id"]}
        for a, b in zip(all_nodes, all_nodes[1:], strict=False)
    ]
    graph = {"nodes": all_nodes, "edges": edges}
    validate_graph(graph)
    wf = Workflow(name="Nightly load", graph=graph)
    db.add(wf)
    db.commit()
    return wf


def _run(db: Session, wf: Workflow, admin: User, **kw) -> WorkflowRun:
    run = automation_service.start_manual_run(db, wf.id, actor=admin, **kw)
    db.expire_all()
    return db.get(WorkflowRun, run.id)


def test_notify_block_sends_via_slack(db: Session, admin: User, http: Http) -> None:
    ch = _channel(db, admin, SLACK)
    wf = _workflow(
        db,
        {
            "id": "tell",
            "type": "notify",
            "config": {
                "send_via": str(ch.id),
                "title": "{{workflow.name}} finished",
                "level": "WARNING",
            },
        },
    )
    run = _run(db, wf, admin)
    assert run.status == RunStatus.COMPLETED
    step = run.steps[-1]
    assert step.message == "Sent via Ops Slack: Sent to Ops Slack"
    assert step.output["sent"] is True
    payload = http.last_json()
    assert payload["text"] == "Nightly load finished"
    assert payload["blocks"][0]["text"]["text"].startswith(":warning:")
    assert payload["blocks"][-1]["elements"][0]["url"].endswith(f"/automation/runs/{run.id}")
    # The in-app notification is still written.
    assert db.scalars(select(Notification)).one().title == "Nightly load finished"
    assert db.get(NotificationChannel, ch.id).last_status == ChannelStatus.OK


def test_failed_delivery_does_not_fail_the_run(db: Session, admin: User, http: Http) -> None:
    ch = _channel(db, admin, SLACK)
    http.status = 500
    run = _run(
        db,
        _workflow(db, {"id": "tell", "type": "notify", "config": {"send_via": str(ch.id)}}),
        admin,
    )
    assert run.status == RunStatus.COMPLETED
    assert run.steps[-1].output["sent"] is False
    assert run.steps[-1].message.startswith("Could not send via Ops Slack: HTTP 500")
    assert db.get(NotificationChannel, ch.id).last_status == ChannelStatus.FAILED


def test_dry_run_sends_nothing(db: Session, admin: User, http: Http) -> None:
    ch = _channel(db, admin, SLACK)
    wf = _workflow(db, {"id": "tell", "type": "notify", "config": {"send_via": str(ch.id)}})
    run = _run(db, wf, admin, dry_run=True)
    assert run.steps[-1].message == "Dry run: would send via Ops Slack"
    assert http.requests == []


def test_notify_email_to_listed_recipients(db: Session, admin: User, smtp: type[FakeSMTP]) -> None:
    ch = _channel(db, admin, EMAIL)
    wf = _workflow(
        db,
        {
            "id": "tell",
            "type": "notify",
            "config": {
                "send_via": str(ch.id),
                "to": ["a@example.com", "b@example.com"],
                "title": "Load done",
                "message": "All good",
            },
        },
    )
    run = _run(db, wf, admin)
    sent = smtp.sent[-1]
    assert sent["to"] == "a@example.com, b@example.com"
    assert sent["subject"] == "Load done"
    assert "All good" in sent["body"] and f"/automation/runs/{run.id}" in sent["body"]


def test_approval_request_is_sent_with_a_link(
    db: Session, admin: User, smtp: type[FakeSMTP]
) -> None:
    ch = _channel(db, admin, EMAIL)
    wf = _workflow(
        db,
        {
            "id": "ok",
            "type": "approval.request",
            "config": {
                "required_environments": [],
                "send_via": str(ch.id),
                "to": ["boss@example.com"],
            },
        },
        {"id": "tell", "type": "notify", "config": {}},
        ports={"ok": "approved"},
    )
    run = _run(db, wf, admin)
    assert run.status == RunStatus.WAITING
    approval = db.scalars(select(Approval)).one()
    sent = smtp.sent[-1]
    assert sent["to"] == "boss@example.com"
    assert sent["subject"] == f"Approval needed: {approval.title}"
    assert "/automation/approvals" in sent["body"]
    assert "(Sent via Company email: Emailed boss@example.com)" in run.steps[-1].message


def test_recipients_must_be_email_addresses() -> None:
    graph = {
        "nodes": [
            {"id": "trigger", "type": "trigger.manual", "config": {}},
            {"id": "tell", "type": "notify", "config": {"to": ["not-an-address"]}},
        ],
        "edges": [{"from": "trigger", "port": "next", "to": "tell"}],
    }
    with pytest.raises(GraphError, match="Not an email address: not-an-address"):
        validate_graph(graph)
