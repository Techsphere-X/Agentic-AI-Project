"""The demo seed script: valid, enabled workflows, and safe to run twice."""

import importlib.util
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.automation.graph import validate_graph
from app.models.airflow import AirflowConnection, MonitoredDag
from app.models.automation import Workflow
from app.models.database_connection import DatabaseConnection
from app.models.user import User

_spec = importlib.util.spec_from_file_location(
    "seed_demo", Path(__file__).resolve().parents[1] / "scripts" / "seed_demo.py"
)
seed_demo = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(seed_demo)  # type: ignore[union-attr]

DEMO_WORKFLOWS = {
    "Retry network failures",
    "Fix with approval",
    "Validate every e-commerce run",
    "Compare source and warehouse orders",
}


def _demo_workflows(db: Session) -> dict[str, Workflow]:
    rows = db.scalars(select(Workflow).where(Workflow.name.in_(DEMO_WORKFLOWS))).all()
    return {w.name: w for w in rows}


def test_seed_creates_valid_enabled_workflows(db: Session, admin: User) -> None:
    seed_demo.seed(db, seed_demo.DemoConfig(mock_airflow=True))

    workflows = _demo_workflows(db)
    assert set(workflows) == DEMO_WORKFLOWS
    for workflow in workflows.values():
        assert workflow.enabled
        validate_graph(workflow.graph)
    assert db.scalar(select(func.count()).select_from(DatabaseConnection)) == 2
    assert db.scalar(select(AirflowConnection.name)) == seed_demo.AIRFLOW_NAME


def test_reconcile_compares_values_not_row_counts(db: Session, admin: User) -> None:
    seed_demo.seed(db, seed_demo.DemoConfig(mock_airflow=True))

    workflow = _demo_workflows(db)["Compare source and warehouse orders"]
    reconcile = next(n for n in workflow.graph["nodes"] if n["type"] == "database.reconcile")
    assert reconcile["config"]["comparison_mode"] == "rows"
    assert workflow.template_parameters["comparison_mode"] == "rows"


def test_incident_workflows_resolve_and_report_unknown(db: Session, admin: User) -> None:
    seed_demo.seed(db, seed_demo.DemoConfig(mock_airflow=True))
    workflows = _demo_workflows(db)

    def edges(name: str) -> set[tuple[str, str, str]]:
        return {(e["from"], e["port"], e["to"]) for e in workflows[name].graph["edges"]}

    assert ("verify", "success", "resolve") in edges("Fix with approval")
    assert ("classify", "unknown", "tell_unknown") in edges("Retry network failures")


def test_seed_twice_creates_no_duplicates(db: Session, admin: User) -> None:
    cfg = seed_demo.DemoConfig(mock_airflow=True)
    seed_demo.seed(db, cfg)
    versions = {name: w.version for name, w in _demo_workflows(db).items()}
    seed_demo.seed(db, cfg)

    assert db.scalar(select(func.count()).select_from(AirflowConnection)) == 1
    assert db.scalar(select(func.count()).select_from(DatabaseConnection)) == 2
    assert db.scalar(
        select(func.count()).select_from(Workflow).where(Workflow.name.in_(DEMO_WORKFLOWS))
    ) == len(DEMO_WORKFLOWS)
    assert {name: w.version for name, w in _demo_workflows(db).items()} == versions


def test_monitors_known_sandbox_dags(db: Session, admin: User) -> None:
    seed_demo.seed(db, seed_demo.DemoConfig(mock_airflow=True))
    conn = db.scalar(select(AirflowConnection))
    db.add(MonitoredDag(connection_id=conn.id, dag_id=seed_demo.ETL_DAG, detect_failures=False))
    db.commit()

    seed_demo.seed(db, seed_demo.DemoConfig(mock_airflow=True))

    dag = db.scalar(select(MonitoredDag).where(MonitoredDag.dag_id == seed_demo.ETL_DAG))
    assert dag.is_monitored and dag.detect_failures and dag.sla_minutes == 60
