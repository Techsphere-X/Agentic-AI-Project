"""Tests for Tencent Cloud WeData orchestration adapter, services, and REST API."""

import uuid

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.audit_log import AuditLog
from app.models.wedata import MonitoredWeDataTask, WeDataConnection
from app.orchestration.wedata.base import (
    WeDataAdapterConfig,
    WeDataConnectionStatus,
)
from app.orchestration.wedata.client import build_tc3_headers
from app.orchestration.wedata.factory import build_adapter
from app.orchestration.wedata.mock import MockWeDataAdapter
from app.services import connection_monitor_service

CONNS = "/api/v1/wedata/connections"
SAMPLE_MOCK_CONN = {
    "name": "tencent-wedata-prod",
    "environment": "DEV",
    "kind": "MOCK",
    "project_id": "1460947878944567296",
    "region": "ap-guangzhou",
    "endpoint": "wedata.tencentcloudapi.com",
    "secret_id": "AKIDtest1234567890",
    "secret_key": "test_secret_key_abcdefgh",
    "is_active": True,
    "is_default": False,
}


def test_tc3_signature_headers():
    headers = build_tc3_headers(
        secret_id="AKIDtest123",
        secret_key="my_secret_key",
        region="ap-guangzhou",
        endpoint="wedata.tencentcloudapi.com",
        action="DescribeTasks",
        payload_str='{"ProjectId":"123"}',
        timestamp=1700000000,
    )
    assert headers["Host"] == "wedata.tencentcloudapi.com"
    assert headers["X-TC-Action"] == "DescribeTasks"
    assert headers["X-TC-Timestamp"] == "1700000000"
    assert "TC3-HMAC-SHA256 Credential=AKIDtest123/2023-11-14/wedata/tc3_request" in headers["Authorization"]
    assert "Signature=" in headers["Authorization"]


@pytest.mark.asyncio
async def test_mock_wedata_adapter():
    config = WeDataAdapterConfig(
        project_id="1460947878944567296",
        region="ap-guangzhou",
        endpoint="wedata.tencentcloudapi.com",
    )
    adapter = build_adapter(mock=True, config=config)
    assert isinstance(adapter, MockWeDataAdapter)

    # Test probe
    res = await adapter.probe()
    assert res.ok is True
    assert res.status == WeDataConnectionStatus.HEALTHY

    # Test list tasks
    tasks = await adapter.list_tasks()
    assert len(tasks) >= 3
    task_ids = {t.task_id for t in tasks}
    assert "20251025_task_spark_orders" in task_ids

    # Test instances
    instances = await adapter.list_task_instances("20251025_task_spark_orders")
    assert len(instances) > 0
    assert instances[0].normalized_state in {"failed", "success"}

    # Test logs
    log = await adapter.get_task_log("20251025_task_spark_orders", instances[0].run_id)
    assert log.available is True
    assert "org.apache.spark" in log.content or "SparkSession" in log.content

    # Test rerun
    success = await adapter.rerun_instance("20251025_task_spark_orders", instances[0].run_id)
    assert success is True


@pytest.mark.asyncio
async def test_mock_wedata_simulation():
    config = WeDataAdapterConfig(
        project_id="123",
        endpoint="wedata.tencentcloudapi.com?simulate=UNAUTHORIZED",
    )
    adapter = build_adapter(mock=True, config=config)
    res = await adapter.probe()
    assert res.ok is False
    assert res.status == WeDataConnectionStatus.UNAUTHORIZED


def test_wedata_connection_crud_and_sync(
    client: TestClient,
    admin_headers: dict,
    operator_headers: dict,
    viewer_headers: dict,
    db: Session,
):
    # 1. Viewer cannot create connection
    resp = client.post(CONNS, json=SAMPLE_MOCK_CONN, headers=viewer_headers)
    assert resp.status_code == 403

    # 2. Admin creates connection
    resp = client.post(CONNS, json=SAMPLE_MOCK_CONN, headers=admin_headers)
    assert resp.status_code == 201
    data = resp.json()
    conn_id = data["id"]
    assert data["name"] == "tencent-wedata-prod"
    assert data["has_secret"] is True
    assert data["region"] == "ap-guangzhou"

    # 3. List connections
    resp = client.get(CONNS, headers=viewer_headers)
    assert resp.status_code == 200
    assert resp.json()["total"] == 1

    # 4. Test unsaved target
    test_req = {
        "kind": "MOCK",
        "project_id": "1460947878944567296",
        "region": "ap-guangzhou",
        "endpoint": "wedata.tencentcloudapi.com",
    }
    resp = client.post("/api/v1/wedata/test-connection", json=test_req, headers=operator_headers)
    assert resp.status_code == 200
    assert resp.json()["ok"] is True

    # 5. Probe saved connection
    resp = client.post(f"{CONNS}/{conn_id}/test", headers=operator_headers)
    assert resp.status_code == 200
    assert resp.json()["ok"] is True

    # 6. Sync tasks
    resp = client.post(f"{CONNS}/{conn_id}/sync", headers=operator_headers)
    assert resp.status_code == 200
    sync_result = resp.json()
    assert sync_result["total"] >= 3
    assert sync_result["created"] >= 3

    # 7. List tasks
    resp = client.get(f"{CONNS}/{conn_id}/tasks", headers=viewer_headers)
    assert resp.status_code == 200
    tasks = resp.json()
    assert len(tasks) >= 3
    spark_task = next(t for t in tasks if t["task_name"] == "ods_to_dwd_orders_spark")
    assert spark_task["is_monitored"] is False

    # 8. Toggle task monitoring
    patch_resp = client.patch(
        f"{CONNS}/{conn_id}/tasks/{spark_task['task_id']}",
        json={"is_monitored": True},
        headers=operator_headers,
    )
    assert patch_resp.status_code == 200
    assert patch_resp.json()["is_monitored"] is True

    # 9. List instances
    inst_resp = client.get(
        f"{CONNS}/{conn_id}/tasks/{spark_task['task_id']}/instances",
        headers=viewer_headers,
    )
    assert inst_resp.status_code == 200
    instances = inst_resp.json()
    assert len(instances) > 0

    # 10. Fetch instance log
    log_resp = client.get(
        f"{CONNS}/{conn_id}/tasks/{spark_task['task_id']}/instances/{instances[0]['run_id']}/log",
        headers=viewer_headers,
    )
    assert log_resp.status_code == 200
    assert log_resp.json()["available"] is True

    # 11. Rerun instance
    rerun_resp = client.post(
        f"{CONNS}/{conn_id}/tasks/{spark_task['task_id']}/instances/{instances[0]['run_id']}/rerun",
        headers=operator_headers,
    )
    assert rerun_resp.status_code == 200
    assert rerun_resp.json()["success"] is True

    # 12. Verify audit log recorded
    logs = db.scalars(
        select(AuditLog).where(AuditLog.action.startswith("wedata"))
    ).all()
    actions = {l.action for l in logs}
    assert "wedata_conn.create" in actions
    assert "wedata.tasks_synced" in actions
    assert "wedata.task_monitored_changed" in actions

    # 13. Update connection
    update_resp = client.patch(
        f"{CONNS}/{conn_id}",
        json={"name": "tencent-wedata-renamed"},
        headers=admin_headers,
    )
    assert update_resp.status_code == 200
    assert update_resp.json()["name"] == "tencent-wedata-renamed"

    # 14. Delete connection
    del_resp = client.delete(f"{CONNS}/{conn_id}", headers=admin_headers)
    assert del_resp.status_code == 204

    # Confirm deleted
    assert client.get(f"{CONNS}/{conn_id}", headers=viewer_headers).status_code == 404
