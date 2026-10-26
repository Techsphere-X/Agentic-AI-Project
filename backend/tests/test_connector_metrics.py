"""Test suite for connector run success and failure metrics API."""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models.airflow import AirflowConnection, ConnectionKind, DeploymentEnvironment
from app.models.database_connection import DatabaseConnection, DatabaseEngine
from app.models.notification_channel import ChannelKind, NotificationChannel
from app.models.user import User
from app.orchestration.airflow.base import AuthType, ConnectionStatus


def test_connector_metrics_endpoint_returns_metrics(
    client: TestClient, viewer_headers: dict[str, str], db: Session
) -> None:
    # Ensure at least one connection of each type exists
    conn = AirflowConnection(
        name="Test Airflow Conn",
        environment=DeploymentEnvironment.DEV,
        kind=ConnectionKind.MOCK,
        base_url="http://mock-airflow:8080",
        auth_type=AuthType.NONE,
        is_active=True,
        last_health_status=ConnectionStatus.HEALTHY,
        last_latency_ms=15,
    )
    db_conn = DatabaseConnection(
        name="Test Database Conn",
        environment=DeploymentEnvironment.DEV,
        engine=DatabaseEngine.POSTGRESQL,
        host="localhost",
        port=5432,
        database="analytics",
        username="postgres",
        is_active=True,
        last_health_status=ConnectionStatus.HEALTHY,
        last_latency_ms=25,
    )
    channel = NotificationChannel(
        name="Test Alert Channel",
        kind=ChannelKind.SLACK,
        settings={"channel": "#ops-alerts"},
        is_active=True,
    )
    db.add_all([conn, db_conn, channel])
    db.commit()

    resp = client.get("/api/v1/health/connector-metrics", headers=viewer_headers)
    assert resp.status_code == 200
    data = resp.json()

    assert "total_connectors" in data
    assert "total_runs" in data
    assert "total_success" in data
    assert "total_failed" in data
    assert "overall_success_rate" in data
    assert "connectors" in data

    assert data["total_connectors"] >= 3
    assert isinstance(data["connectors"], list)

    names = {c["name"] for c in data["connectors"]}
    assert "Test Airflow Conn" in names
    assert "Test Database Conn" in names
    assert "Test Alert Channel" in names

    # Verify connector structure
    item = next(c for c in data["connectors"] if c["name"] == "Test Airflow Conn")
    assert item["type"] == "airflow"
    assert item["status"] == "HEALTHY"
    assert item["total_runs"] >= 0
    assert item["success_runs"] >= 0
    assert item["failed_runs"] >= 0
    assert 0.0 <= item["success_rate"] <= 100.0
