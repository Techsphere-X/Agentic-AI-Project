"""Plan 5 / Phase C: cron schedules (with a time zone) and 'When a webhook is called'."""

from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.automation import cron
from app.automation.graph import GraphError, validate_graph
from app.automation.types import RunStatus
from app.models.automation import Notification, Workflow, WorkflowRun
from app.services import automation_service


def _schedule(**config: object) -> dict:
    return {
        "nodes": [
            {"id": "t", "type": "trigger.schedule", "config": config},
            {"id": "n", "type": "notify", "config": {"title": "tick"}},
        ],
        "edges": [{"from": "t", "port": "next", "to": "n"}],
    }


def _webhook_graph() -> dict:
    return {
        "nodes": [
            {"id": "t", "type": "trigger.webhook", "config": {}},
            {"id": "n", "type": "notify", "config": {"title": "Deploy {{trigger.body.version}}"}},
        ],
        "edges": [{"from": "t", "port": "next", "to": "n"}],
    }


def _workflow(db: Session, graph: dict, *, enabled: bool = True) -> Workflow:
    validate_graph(graph)
    wf = Workflow(name="hooked", graph=graph, enabled=enabled)
    db.add(wf)
    db.commit()
    return wf


def _runs(db: Session, wf: Workflow) -> list[WorkflowRun]:
    db.expire_all()
    return list(db.scalars(select(WorkflowRun).where(WorkflowRun.workflow_id == wf.id)))


# ---------------------------------------------------------------------- cron


def at(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 11, day, hour, minute, tzinfo=UTC)  # 2 Nov 2026 is a Monday


def test_cron_fields() -> None:
    c = cron.parse("*/15 9-17 * * mon-fri")
    assert c.minutes == {0, 15, 30, 45}
    assert c.hours == set(range(9, 18))
    assert c.weekdays == {1, 2, 3, 4, 5}
    assert cron.parse("@daily") == cron.parse("0 0 * * *")
    assert cron.parse("0 0 * * 7").weekdays == {0}  # 7 is Sunday too


@pytest.mark.parametrize(
    ("expression", "message"),
    [
        ("* * * *", "5 fields"),
        ("61 * * * *", "minute must be between 0 and 59"),
        ("0 25 * * *", "hour must be between"),
        ("0 0 * foo *", "month"),
        ("10-5 * * * *", "goes backwards"),
    ],
)
def test_cron_errors(expression: str, message: str) -> None:
    with pytest.raises(cron.CronError, match=message):
        cron.parse(expression)


def test_next_times_in_a_time_zone() -> None:
    # 06:00 in India (UTC+05:30) on weekdays, asked on Saturday 7 Nov.
    times = cron.next_times("0 6 * * 1-5", "Asia/Kolkata", at(7, 12), count=2)
    assert times == [at(9, 0, 30), at(10, 0, 30)]


def test_day_of_month_or_weekday() -> None:
    # Both day fields set: either matches (the 1st, or any Monday).
    times = cron.next_times("0 0 1 * mon", "UTC", at(3, 0), count=3)
    assert times == [at(9, 0), at(16, 0), at(23, 0)]


def test_bad_schedule_settings_are_rejected() -> None:
    with pytest.raises(GraphError, match="5 fields"):
        validate_graph(_schedule(mode="cron", cron="every day"))
    with pytest.raises(GraphError, match="Unknown time zone"):
        validate_graph(_schedule(mode="cron", cron="0 6 * * *", timezone="Mars/Olympus"))
    validate_graph(_schedule(mode="every", cron="not checked"))  # only cron mode reads it


def test_cron_schedule_runs_once_per_set_time(db: Session) -> None:
    wf = _workflow(db, _schedule(mode="cron", cron="*/15 * * * *"))
    automation_service.tick(db, now=at(3, 10, 1))
    assert len(_runs(db, wf)) == 1
    automation_service.tick(db, now=at(3, 10, 7))  # same set time (10:00)
    assert len(_runs(db, wf)) == 1
    automation_service.tick(db, now=at(3, 10, 15))
    assert len(_runs(db, wf)) == 2


def test_cron_does_not_replay_an_old_time_when_turned_on(db: Session) -> None:
    wf = _workflow(db, _schedule(mode="cron", cron="0 6 * * *"))
    automation_service.tick(db, now=at(3, 15))  # 06:00 was hours ago
    assert _runs(db, wf) == []
    automation_service.tick(db, now=at(4, 6, 2))
    assert len(_runs(db, wf)) == 1


def test_schedule_preview_api(client: TestClient, viewer_headers: dict) -> None:
    ok = client.get(
        "/api/v1/automation/schedule-preview",
        params={"cron": "0 6 * * *", "timezone": "UTC", "count": 2},
        headers=viewer_headers,
    )
    assert ok.status_code == 200 and len(ok.json()["next"]) == 2
    bad = client.get(
        "/api/v1/automation/schedule-preview", params={"cron": "nope"}, headers=viewer_headers
    )
    assert bad.status_code == 400 and "5 fields" in bad.json()["error"]["message"]


# ---------------------------------------------------------------------- webhooks


def test_every_block_type_can_run() -> None:
    from app.automation.graph import NODE_TYPES
    from app.services.automation_nodes import EXECUTORS

    assert set(NODE_TYPES) <= set(EXECUTORS)


def _hook_path(client: TestClient, wf: Workflow, admin_headers: dict) -> str:
    response = client.get(f"/api/v1/automation/workflows/{wf.id}/webhook", headers=admin_headers)
    assert response.status_code == 200, response.text
    return response.json()["path"]


def test_webhook_starts_a_run_with_its_body(
    db: Session, client: TestClient, admin_headers: dict, viewer_headers: dict
) -> None:
    wf = _workflow(db, _webhook_graph())
    path = _hook_path(client, wf, admin_headers)
    assert path.startswith(f"/api/v1/hooks/{wf.id}/")
    # Viewers cannot see the secret URL.
    viewer = client.get(f"/api/v1/automation/workflows/{wf.id}/webhook", headers=viewer_headers)
    assert viewer.status_code == 403
    # The token is not part of the workflow others can read.
    assert (
        "webhook_token"
        not in client.get(f"/api/v1/automation/workflows/{wf.id}", headers=viewer_headers).json()
    )

    response = client.post(path, json={"version": "1.4.2"})  # no login needed
    assert response.status_code == 202, response.text
    [run] = _runs(db, wf)
    assert run.trigger_event == "webhook"
    assert run.context["trigger"]["body"] == {"version": "1.4.2"}

    automation_service.tick(db, now=datetime.now(UTC) + timedelta(seconds=1))
    db.expire_all()
    assert db.get(WorkflowRun, run.id).status == RunStatus.COMPLETED
    assert [n.title for n in db.scalars(select(Notification))] == ["Deploy 1.4.2"]


def test_webhook_refuses_wrong_or_old_tokens_and_off_workflows(
    db: Session, client: TestClient, admin_headers: dict
) -> None:
    wf = _workflow(db, _webhook_graph())
    path = _hook_path(client, wf, admin_headers)
    assert client.post(path + "x", json={}).status_code == 404
    assert client.post(f"/api/v1/hooks/{wf.id}/", json={}).status_code in (404, 405)

    rotated = client.post(
        f"/api/v1/automation/workflows/{wf.id}/webhook/rotate", headers=admin_headers
    ).json()["path"]
    assert rotated != path
    assert client.post(path, json={}).status_code == 404  # the old URL stopped working
    assert client.post(rotated, json={}).status_code == 202

    wf.enabled = False
    db.commit()
    assert client.post(rotated, json={}).status_code == 404
    assert client.post(rotated, content=b"not json").status_code in (400, 404)


def test_webhook_needs_json_and_is_rate_limited(
    db: Session, client: TestClient, admin_headers: dict, monkeypatch: pytest.MonkeyPatch
) -> None:
    wf = _workflow(db, _webhook_graph())
    path = _hook_path(client, wf, admin_headers)
    assert client.post(path, content=b"{oops").status_code == 400
    assert client.post(path, content=b"x" * (64 * 1024 + 1)).status_code == 413

    monkeypatch.setattr(automation_service, "WEBHOOK_MAX_PER_MINUTE", 2)
    assert client.post(path, json={}).status_code == 202
    assert client.post(path, json={}).status_code == 202
    assert client.post(path, json={}).status_code == 409


def test_webhook_url_only_starts_webhook_workflows(
    db: Session, client: TestClient, admin_headers: dict
) -> None:
    wf = _workflow(db, _schedule(mode="every", every_minutes=60))
    path = _hook_path(client, wf, admin_headers)
    assert client.post(path, json={}).status_code == 404
