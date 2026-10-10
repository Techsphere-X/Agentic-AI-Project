"""Tests for the decoupled Log Analysis (analyze.task_logs) and AI Solution (ai.generate_fix) blocks."""

import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

from datetime import datetime, timezone

from app.automation.graph import catalog, validate_graph
from app.automation.remediation import Advice
from app.detection.types import EvidenceKind, IncidentSeverity, IncidentStatus, IncidentType
from app.models.airflow import (
    AirflowConnection,
    AuthType,
    ConnectionKind,
    DeploymentEnvironment,
    MonitoredDag,
)
from app.models.automation import Workflow, WorkflowRun
from app.models.incident import Incident, IncidentEvidence
from app.models.user import User
from app.services import automation_nodes, automation_service
from app.services.automation_nodes import _extract_log_error


def test_catalog_entries() -> None:
    cat = {e["type"]: e for e in catalog()}

    analyze_entry = cat["analyze.task_logs"]
    assert analyze_entry["label"] == "Analyze logs"
    assert analyze_entry["category"] == "diagnosis"
    assert analyze_entry["ports"] == ["analyzed", "no_logs"]
    assert analyze_entry["needs_incident"] is True
    assert "max_log_lines" in analyze_entry["config_schema"]["properties"]

    ai_entry = cat["ai.generate_fix"]
    assert ai_entry["label"] == "AI solution"
    assert ai_entry["category"] == "logic"
    assert ai_entry["ports"] == ["solution_ready", "uncertain"]
    assert ai_entry["needs_incident"] is True
    assert "min_confidence_pct" in ai_entry["config_schema"]["properties"]


def test_graph_validation() -> None:
    graph = {
        "nodes": [
            {"id": "t", "type": "trigger.incident", "config": {}},
            {"id": "a", "type": "analyze.task_logs", "config": {}},
            {"id": "ai", "type": "ai.generate_fix", "config": {"min_confidence_pct": 75}},
            {"id": "n_ok", "type": "notify", "config": {"message": "{{analysis.headline}}"}},
            {"id": "n_human", "type": "approval.request", "config": {}},
        ],
        "edges": [
            {"from": "t", "port": "next", "to": "a"},
            {"from": "a", "port": "analyzed", "to": "ai"},
            {"from": "a", "port": "no_logs", "to": "n_human"},
            {"from": "ai", "port": "solution_ready", "to": "n_ok"},
            {"from": "ai", "port": "uncertain", "to": "n_human"},
        ],
    }
    validated = validate_graph(graph)
    assert len(validated.nodes) == 5
    assert len(validated.edges) == 5


def test_extract_log_error_traceback() -> None:
    raw_log = """
[2026-10-10 12:00:00] {taskinstance.py:1150} INFO - Starting task_id=process_orders
[2026-10-10 12:00:02] {database.py:45} INFO - Connecting to PostgreSQL database...
Traceback (most recent call last):
  File "/opt/airflow/dags/etl.py", line 42, in process_orders
    conn = db.connect()
  File "/opt/airflow/plugins/db.py", line 19, in connect
psycopg2.OperationalError: could not connect to server: Connection timed out
[2026-10-10 12:00:32] {taskinstance.py:1200} ERROR - Task failed with exception
"""
    headline, stack_trace, snippet, task_id = _extract_log_error(raw_log, max_lines=50)
    assert "psycopg2.OperationalError: could not connect to server: Connection timed out" in headline
    assert "Traceback (most recent call last):" in stack_trace
    assert task_id == "process_orders"
    assert "Connecting to PostgreSQL database..." in snippet


def test_extract_log_error_no_traceback() -> None:
    raw_log = """
[2026-10-10 12:00:00] INFO - Task instance: sync_customers
[2026-10-10 12:00:01] WARNING - Retrying connection (1/3)
[2026-10-10 12:00:05] ERROR: Remote API returned HTTP 503 Service Unavailable
"""
    headline, stack_trace, snippet, task_id = _extract_log_error(raw_log, max_lines=50)
    assert "ERROR: Remote API returned HTTP 503 Service Unavailable" in headline
    assert stack_trace == ""
    assert task_id == "sync_customers"


def _incident(db: Session, *logs: str) -> Incident:
    conn = AirflowConnection(
        name=f"mock-{uuid.uuid4().hex[:6]}",
        environment=DeploymentEnvironment.DEV,
        kind=ConnectionKind.MOCK,
        base_url="http://mock-airflow",
        auth_type=AuthType.NONE,
    )
    db.add(conn)
    db.flush()
    dag = MonitoredDag(connection_id=conn.id, dag_id="daily_etl", is_monitored=True)
    db.add(dag)
    db.flush()
    now = datetime.now(timezone.utc)
    inc = Incident(
        connection_id=conn.id,
        monitored_dag_id=dag.id,
        dag_id=dag.dag_id,
        type=IncidentType.DAG_RUN_FAILED,
        status=IncidentStatus.OPEN,
        severity=IncidentSeverity.HIGH,
        title="daily_etl: run failed",
        fingerprint=f"fp-{uuid.uuid4()}",
        run_id="run_123",
        last_run_id="run_123",
        occurrence_count=1,
        occurred_at=now,
    )
    db.add(inc)
    db.flush()
    for i, log in enumerate(logs):
        db.add(
            IncidentEvidence(
                incident_id=inc.id,
                kind=EvidenceKind.TASK_LOG,
                source=f"task:{i}",
                content=log,
            )
        )
    db.commit()
    return inc


def test_analyze_task_logs_executor_no_logs(db: Session) -> None:
    inc = _incident(db)
    wf = Workflow(
        name="Test Analysis",
        enabled=True,
        graph={
            "nodes": [
                {"id": "t", "type": "trigger.incident", "config": {}},
                {"id": "a", "type": "analyze.task_logs", "config": {}},
            ],
            "edges": [{"from": "t", "port": "next", "to": "a"}],
        },
    )
    db.add(wf)
    db.commit()

    run = automation_service.enqueue_for_incident(
        db, inc, automation_nodes.TriggerEvent.OPENED
    )[0]
    res = automation_service.tick(db)
    assert res.advanced >= 1

    db.refresh(run)
    analysis = run.context.get("analysis")
    assert analysis is not None
    assert analysis["headline"] == ""
    assert analysis["dag_id"] == "daily_etl"


def test_analyze_task_logs_executor_with_logs(db: Session) -> None:
    log_content = "[2026-10-10] ERROR: Out of memory killed process\ntask_id=heavy_job"
    inc = _incident(db, log_content)

    wf = Workflow(
        name="Test Analysis Logs",
        enabled=True,
        graph={
            "nodes": [
                {"id": "t", "type": "trigger.incident", "config": {}},
                {"id": "a", "type": "analyze.task_logs", "config": {}},
            ],
            "edges": [{"from": "t", "port": "next", "to": "a"}],
        },
    )
    db.add(wf)
    db.commit()

    run = automation_service.enqueue_for_incident(
        db, inc, automation_nodes.TriggerEvent.OPENED
    )[0]
    automation_service.tick(db)

    db.refresh(run)
    analysis = run.context.get("analysis")
    assert analysis is not None
    assert "Out of memory" in analysis["headline"]
    assert analysis["task_id"] == "heavy_job"


def test_ai_generate_fix_executor(db: Session, monkeypatch: pytest.MonkeyPatch) -> None:
    inc = _incident(db)
    wf = Workflow(
        name="Test AI Solution",
        enabled=True,
        graph={
            "nodes": [
                {"id": "t", "type": "trigger.incident", "config": {}},
                {"id": "ai", "type": "ai.generate_fix", "config": {"min_confidence_pct": 80}},
            ],
            "edges": [{"from": "t", "port": "next", "to": "ai"}],
        },
    )
    db.add(wf)
    db.commit()

    # 1. When advisor is disabled -> uncertain
    monkeypatch.setattr(automation_nodes, "decision_advisor", lambda settings: None)
    run = automation_service.enqueue_for_incident(
        db, inc, automation_nodes.TriggerEvent.OPENED
    )[0]
    automation_service.tick(db)
    db.refresh(run)
    sol = run.context.get("ai_solution")
    assert sol is not None
    assert sol["confidence"] == 0.0

    # 2. When advisor is mocked with high confidence -> solution_ready
    mock_advisor = MagicMock()
    mock_advisor.enabled = True
    mock_advisor.advise.return_value = Advice(
        fix="retry",
        reason="Network timeout was transient; safe to retry.",
        confidence=0.92,
        model="qwen3:4b",
    )
    monkeypatch.setattr(automation_nodes, "decision_advisor", lambda settings: mock_advisor)

    inc2 = _incident(db)
    run2 = automation_service.enqueue_for_incident(
        db, inc2, automation_nodes.TriggerEvent.OPENED
    )[0]
    automation_service.tick(db)
    db.refresh(run2)
    sol2 = run2.context.get("ai_solution")
    assert sol2 is not None
    assert sol2["recommended_action"] == "retry"
    assert sol2["confidence"] == 0.92
    assert "safe to retry" in sol2["explanation"]
