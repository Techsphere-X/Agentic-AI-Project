"""'Watch selected DAGs' trigger: one output per watched DAG; the all-DAGs trigger unchanged."""

from datetime import timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.automation.graph import GraphError, catalog, node_ports, validate_graph
from app.automation.types import RunStatus, TriggerEvent
from app.core.exceptions import BadRequestError
from app.db.base import utcnow
from app.detection.types import IncidentSeverity, IncidentStatus, IncidentType
from app.models.airflow import (
    AirflowConnection,
    ConnectionKind,
    DeploymentEnvironment,
    MonitoredDag,
)
from app.models.audit_log import AuditLog
from app.models.automation import Workflow, WorkflowRun
from app.models.incident import Incident
from app.models.user import User
from app.orchestration.airflow.base import AuthType
from app.services import automation_service


@pytest.fixture
def conn(db: Session) -> AirflowConnection:
    conn = AirflowConnection(
        name="mock-dev",
        environment=DeploymentEnvironment.DEV,
        kind=ConnectionKind.MOCK,
        base_url="http://mock-airflow",
        auth_type=AuthType.NONE,
    )
    db.add(conn)
    db.flush()
    for dag_id in ("orders", "billing", "other"):
        db.add(MonitoredDag(connection_id=conn.id, dag_id=dag_id, is_monitored=False))
    db.commit()
    return conn


def watch_graph(conn: AirflowConnection, *dag_ids: str, wire: dict[str, str] | None = None) -> dict:
    nodes = [
        {
            "id": "start",
            "type": "trigger.dag_watch",
            "config": {
                "dags": [{"key": d, "connection_id": str(conn.id), "dag_id": d} for d in dag_ids]
            },
        }
    ]
    edges = []
    for port, note in (wire or {}).items():
        nodes.append(
            {
                "id": f"n_{port}",
                "type": "incident.update",
                "config": {"operation": "note", "note": note},
            }
        )
        edges.append({"from": "start", "port": port, "to": f"n_{port}"})
    return {"nodes": nodes, "edges": edges}


def incident(db: Session, conn: AirflowConnection, dag_id: str) -> Incident:
    dag = db.scalar(select(MonitoredDag).where(MonitoredDag.dag_id == dag_id))
    now = utcnow()
    row = Incident(
        connection_id=conn.id,
        monitored_dag_id=dag.id,
        dag_id=dag_id,
        type=IncidentType.DAG_RUN_FAILED,
        severity=IncidentSeverity.HIGH,
        status=IncidentStatus.OPEN,
        title=f"{dag_id} failed",
        fingerprint=f"{conn.id}:{dag_id}:failed",
        occurred_at=now,
        first_seen_at=now - timedelta(minutes=1),
        last_seen_at=now,
    )
    db.add(row)
    db.commit()
    return row


def enabled_workflow(db: Session, admin: User, graph: dict) -> Workflow:
    wf = automation_service.create_workflow(db, actor=admin, name="Watch", graph=graph)
    return automation_service.update_workflow(db, wf.id, {"enabled": True}, actor=admin)


# ---------------------------------------------------------------------- graph


def test_ports_come_from_watched_dags(conn: AirflowConnection) -> None:
    graph = validate_graph(watch_graph(conn, "orders", "billing", wire={"billing": "b"}))
    assert node_ports("trigger.dag_watch", graph.trigger.config) == ("orders", "billing")
    entry = next(e for e in catalog() if e["type"] == "trigger.dag_watch")
    assert entry["dynamic_ports"] == "dags" and entry["ports"] == []
    assert entry["needs_incident"] is True


def test_watch_needs_dags_and_rejects_duplicates_and_unknown_ports(conn: AirflowConnection) -> None:
    with pytest.raises(GraphError, match="at least one DAG"):
        validate_graph(watch_graph(conn))
    dup = watch_graph(conn, "orders")
    dup["nodes"][0]["config"]["dags"].append(
        {"key": "o2", "connection_id": str(conn.id), "dag_id": "orders"}
    )
    with pytest.raises(GraphError, match="watched twice"):
        validate_graph(dup)
    bad = watch_graph(conn, "orders", wire={"orders": "x"})
    bad["edges"][0]["port"] = "billing"
    with pytest.raises(GraphError, match="no output port 'billing'"):
        validate_graph(bad)


def test_missing_keys_are_filled_from_dag_id(conn: AirflowConnection) -> None:
    raw = watch_graph(conn, "orders")
    raw["nodes"][0]["config"]["dags"] = [
        {"connection_id": str(conn.id), "dag_id": "my.dag"},
        {"connection_id": "other-conn", "dag_id": "my.dag"},
    ]
    keys = [d["key"] for d in validate_graph(raw).trigger.config["dags"]]
    assert keys == ["my_dag", "my_dag_2"]


# ---------------------------------------------------------------------- enqueue & run


def test_enabling_turns_on_monitoring(db: Session, admin: User, conn: AirflowConnection) -> None:
    enabled_workflow(db, admin, watch_graph(conn, "orders"))
    dags = {d.dag_id: d for d in db.scalars(select(MonitoredDag))}
    assert dags["orders"].is_monitored and dags["orders"].detect_failures
    assert not dags["billing"].is_monitored
    assert db.scalar(select(AuditLog).where(AuditLog.action == "monitored_dag.toggle"))


def test_enabling_with_unknown_dag_fails(db: Session, admin: User, conn: AirflowConnection) -> None:
    wf = automation_service.create_workflow(
        db, actor=admin, name="Watch", graph=watch_graph(conn, "orders")
    )
    graph = watch_graph(conn, "orders")
    graph["nodes"][0]["config"]["dags"][0]["dag_id"] = "missing_dag"
    with pytest.raises(BadRequestError, match="missing_dag"):
        automation_service.update_workflow(
            db, wf.id, {"enabled": True, "graph": graph}, actor=admin
        )


def test_run_follows_the_incident_dag_output(
    db: Session, admin: User, conn: AirflowConnection
) -> None:
    wf = enabled_workflow(
        db, admin, watch_graph(conn, "orders", "billing", wire={"orders": "O", "billing": "B"})
    )
    assert (
        automation_service.enqueue_for_incident(
            db, incident(db, conn, "other"), TriggerEvent.OPENED
        )
        == []
    )

    inc = incident(db, conn, "billing")
    [run] = automation_service.enqueue_for_incident(db, inc, TriggerEvent.OPENED)
    db.commit()
    automation_service.tick(db, now=utcnow())
    db.expire_all()
    run = db.get(WorkflowRun, run.id)
    assert run.workflow_id == wf.id and run.status == RunStatus.COMPLETED
    assert [(s.node_id, s.port) for s in run.steps][:2] == [
        ("start", "billing"),
        ("n_billing", "next"),
    ]


def test_all_dags_trigger_still_fires_for_any_dag(
    db: Session, admin: User, conn: AirflowConnection
) -> None:
    enabled_workflow(db, admin, {"nodes": [{"id": "t", "type": "trigger.incident"}], "edges": []})
    runs = automation_service.enqueue_for_incident(
        db, incident(db, conn, "other"), TriggerEvent.OPENED
    )
    assert len(runs) == 1


def test_run_now_uses_an_open_incident_on_a_watched_dag(
    db: Session, admin: User, conn: AirflowConnection
) -> None:
    wf = enabled_workflow(db, admin, watch_graph(conn, "orders"))
    with pytest.raises(BadRequestError, match="No open incident"):
        automation_service.start_manual_run(db, wf.id, actor=admin)
    incident(db, conn, "billing")  # not watched
    with pytest.raises(BadRequestError, match="No open incident"):
        automation_service.start_manual_run(db, wf.id, actor=admin)
    inc = incident(db, conn, "orders")
    run = automation_service.start_manual_run(db, wf.id, actor=admin)
    assert run.incident_id == inc.id
