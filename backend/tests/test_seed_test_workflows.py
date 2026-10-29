"""The workflow test set seed script: 8 valid enabled workflows, old demo off, safe to rerun."""

import importlib.util
import sys
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.automation.graph import validate_graph
from app.models.airflow import AirflowConnection, MonitoredDag
from app.models.automation import Workflow
from app.models.user import User

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


seed_demo = _load("seed_demo")
seed_tests = _load("seed_test_workflows")


def _seed(db: Session) -> None:
    seed_demo.seed(db, seed_demo.DemoConfig(mock_airflow=True))
    seed_tests.seed(db)


def _workflow(db: Session, name: str) -> Workflow:
    workflow = db.scalar(select(Workflow).where(Workflow.name == name))
    assert workflow is not None, name
    return workflow


def test_creates_eight_valid_enabled_workflows(db: Session, admin: User) -> None:
    _seed(db)

    for name in seed_tests.ALL_WORKFLOWS:
        workflow = _workflow(db, name)
        assert workflow.enabled
        validate_graph(workflow.graph)
    for name in seed_tests.OLD_DEMO_WORKFLOWS:
        assert not _workflow(db, name).enabled


def test_every_block_is_reachable(db: Session, admin: User) -> None:
    _seed(db)

    for name in seed_tests.ALL_WORKFLOWS:
        graph = _workflow(db, name).graph
        targets = {e["to"] for e in graph["edges"]}
        unreachable = [
            n["id"] for n in graph["nodes"] if n["id"] != "trigger" and n["id"] not in targets
        ]
        assert unreachable == [], name


def test_data_quality_runs_all_twenty_checks_in_order(db: Session, admin: User) -> None:
    _seed(db)
    expected = [c.id for c in seed_tests.quality_checks(60)]
    assert len(expected) == len(set(expected)) == 20

    for name in (seed_tests.DQ_1, seed_tests.DQ_2):
        graph = _workflow(db, name).graph
        checks = [
            n["id"]
            for n in graph["nodes"]
            if n["type"] in ("database.check", "database.reconcile")
        ]
        assert checks == expected, name
        # Each check passes to the next one; every failure leaves the chain.
        passes = {(e["from"], e["to"]) for e in graph["edges"] if e["port"] in ("pass", "matched")}
        for current, nxt in zip(expected, expected[1:], strict=False):
            assert (current, nxt) in passes, (name, current)
        for node in graph["nodes"]:
            if node["type"] == "database.reconcile":
                assert node["config"]["comparison_mode"] == "rows"


def test_after_run_records_the_result_and_manual_does_not(db: Session, admin: User) -> None:
    _seed(db)

    after = {n["type"] for n in _workflow(db, seed_tests.DQ_1).graph["nodes"]}
    manual = {n["type"] for n in _workflow(db, seed_tests.DQ_2).graph["nodes"]}
    assert "check.record" in after and "check.record" not in manual
    fresh = {
        name: next(n for n in _workflow(db, name).graph["nodes"] if n["id"] == "fresh_data")
        for name in (seed_tests.DQ_1, seed_tests.DQ_2)
    }
    assert fresh[seed_tests.DQ_1]["config"]["expected"] == "60"
    assert fresh[seed_tests.DQ_2]["config"]["expected"] == "1500"


def test_fix_workflows_split_the_causes(db: Session, admin: User) -> None:
    _seed(db)

    def classify_ports(name: str) -> set[str]:
        graph = _workflow(db, name).graph
        return {e["port"] for e in graph["edges"] if e["from"] == "classify"}

    assert classify_ports(seed_tests.FIX_1) == {"retryable"}
    assert classify_ports(seed_tests.FIX_2) == {"needs_fix"}
    assert classify_ports(seed_tests.FIX_3) == {"unknown"}


def test_rerun_is_idempotent_and_sets_the_etl_sla(db: Session, admin: User) -> None:
    seed_demo.seed(db, seed_demo.DemoConfig(mock_airflow=True))
    conn = db.scalar(select(AirflowConnection))
    db.add(MonitoredDag(connection_id=conn.id, dag_id=seed_tests.ETL_DAG, sla_minutes=60))
    db.commit()

    seed_tests.seed(db)
    versions = {n: _workflow(db, n).version for n in seed_tests.ALL_WORKFLOWS}
    seed_tests.seed(db)

    count = db.scalar(
        select(func.count()).select_from(Workflow).where(Workflow.name.in_(seed_tests.ALL_WORKFLOWS))
    )
    assert count == 8
    assert {n: _workflow(db, n).version for n in seed_tests.ALL_WORKFLOWS} == versions
    etl = db.scalar(select(MonitoredDag).where(MonitoredDag.dag_id == seed_tests.ETL_DAG))
    assert etl.sla_minutes == 1500
