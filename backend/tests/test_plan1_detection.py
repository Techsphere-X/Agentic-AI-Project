"""Plan 1 / Phases 2-3: rules, severity, scrubbing, schema, detection cycles, lease."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.core.crypto import encrypt_secret
from app.detection import evidence, rules, severity
from app.detection.types import (
    EvidenceKind,
    IncidentResolution,
    IncidentSeverity,
    IncidentStatus,
    IncidentType,
)
from app.models.airflow import (
    AirflowConnection,
    ConnectionKind,
    DeploymentEnvironment,
    MonitoredDag,
)
from app.models.audit_log import AuditLog
from app.models.incident import Incident
from app.orchestration.airflow import factory
from app.orchestration.airflow.base import AirflowDagRun, AuthType, ConnectionStatus
from app.services import detection_service
from tests.fake_airflow import FakeAirflow

T0 = datetime(2026, 9, 24, 12, 0, tzinfo=UTC)


def at(hour: int, minute: int = 0) -> datetime:
    return T0.replace(hour=hour, minute=minute)


def run(run_id: str, state: str, when: datetime, minutes: int = 5) -> AirflowDagRun:
    return AirflowDagRun(
        run_id=run_id,
        state=state,
        logical_date=when,
        start_date=when,
        end_date=when + timedelta(minutes=minutes) if state in ("success", "failed") else None,
    )


# ---------------------------------------------------------------------- rules (pure)


def test_failed_runs_after_watermark_only() -> None:
    runs = [run("a", "failed", at(9)), run("b", "success", at(10)), run("c", "failed", at(11))]
    found = rules.detect_failed_runs("d", runs, watermark=at(10))
    assert [f.run_id for f in found] == ["c"]


def test_first_cycle_does_not_backfill_recovered_failures() -> None:
    runs = [run("a", "failed", at(9)), run("b", "success", at(10)), run("c", "failed", at(11))]
    assert [f.run_id for f in rules.detect_failed_runs("d", runs, watermark=None)] == ["c"]
    recovered = [run("a", "failed", at(9)), run("b", "success", at(10))]
    assert rules.detect_failed_runs("d", recovered, watermark=None) == []


def test_running_runs_are_ignored_and_watermark_uses_terminal_runs() -> None:
    runs = [run("a", "success", at(10)), run("b", "running", at(11))]
    assert rules.detect_failed_runs("d", runs, None) == []
    assert rules.next_watermark(runs, None) == at(10)
    assert rules.next_watermark([], at(9)) == at(9)


@pytest.mark.parametrize("state", ["queued", "scheduled", "running", "deferred", "up_for_retry"])
def test_active_airflow_states_normalize_to_in_progress(state: str) -> None:
    active = run("active", state, at(11))
    assert active.normalized_state == "in_progress"
    assert rules.in_progress_runs([active]) == [active]


def test_sla_boundary() -> None:
    runs = [run("a", "success", at(10), minutes=0)]  # finished exactly 10:00
    assert rules.detect_sla_miss("d", runs, 120, now=at(12)) is None  # exactly at SLA: ok
    finding = rules.detect_sla_miss("d", runs, 119, now=at(12))
    assert finding is not None and finding.type == IncidentType.SLA_MISSED
    assert rules.detect_sla_miss("d", [], 60, now=at(12)) is not None  # never succeeded
    assert rules.detect_sla_miss("d", [], None, now=at(12)) is None  # no SLA configured


def test_find_recoveries() -> None:
    refs = [
        rules.OpenIncidentRef(uuid.uuid4(), IncidentType.DAG_RUN_FAILED, at(10)),
        rules.OpenIncidentRef(uuid.uuid4(), IncidentType.DAG_RUN_FAILED, at(11, 30)),
        rules.OpenIncidentRef(uuid.uuid4(), IncidentType.SLA_MISSED, at(11)),
    ]
    runs = [run("ok", "success", at(11), minutes=10)]
    recovered = rules.find_recoveries(runs, refs, sla_minutes=120, now=at(12))
    assert {r.incident_id for r in recovered} == {refs[0].id, refs[2].id}


def test_severity_rules() -> None:
    base = severity.score(
        IncidentType.DAG_RUN_FAILED, environment="DEV", tags=[], recent_failures=1
    )
    assert base.severity == IncidentSeverity.MEDIUM
    prod = severity.score(
        IncidentType.DAG_RUN_FAILED, environment="PROD", tags=["tier-1"], recent_failures=3
    )
    assert prod.severity == IncidentSeverity.CRITICAL
    assert len(prod.reasons) == 4
    sla = severity.score(IncidentType.SLA_MISSED, environment="DEV", tags=None, recent_failures=9)
    assert sla.severity == IncidentSeverity.LOW  # repeat failures don't apply to SLA


@pytest.mark.parametrize(
    ("raw", "secret"),
    [
        ("Connecting to postgres://etl:hunter2@db:5432/dw", "hunter2"),
        ("password=s3cr3t host=db", "s3cr3t"),
        ('{"api_key": "abcd1234"}', "abcd1234"),
        ("Authorization: Bearer eyJhbGciOi", "eyJhbGciOi"),
        ("export AWS_SECRET_ACCESS_KEY=AKIAXYZ", "AKIAXYZ"),
    ],
)
def test_log_scrubbing(raw: str, secret: str) -> None:
    cleaned = evidence.scrub(raw)
    assert secret not in cleaned
    assert "***" in cleaned


# ---------------------------------------------------------------------- schema


def _conn(db: Session, name: str = "live", **kwargs: object) -> AirflowConnection:
    values: dict = {
        "name": name,
        "environment": DeploymentEnvironment.DEV,
        "kind": ConnectionKind.LIVE,
        "base_url": "http://airflow.test:8080",
        "auth_type": AuthType.TOKEN,
        "encrypted_secret": encrypt_secret("static-token"),  # accepted by FakeAirflow
    }
    values.update(kwargs)
    conn = AirflowConnection(**values)
    db.add(conn)
    db.flush()
    return conn


def _dag(
    db: Session, conn: AirflowConnection, dag_id: str = "etl", **kwargs: object
) -> MonitoredDag:
    values: dict = {"is_monitored": True, "tags": []}
    values.update(kwargs)
    dag = MonitoredDag(connection_id=conn.id, dag_id=dag_id, **values)
    db.add(dag)
    db.commit()
    return dag


def _incident(conn: AirflowConnection, dag: MonitoredDag, status: IncidentStatus) -> Incident:
    return Incident(
        connection_id=conn.id,
        monitored_dag_id=dag.id,
        dag_id=dag.dag_id,
        type=IncidentType.DAG_RUN_FAILED,
        severity=IncidentSeverity.LOW,
        status=status,
        title="t",
        fingerprint="fp",
        occurred_at=T0,
    )


def test_one_open_incident_per_fingerprint(db: Session) -> None:
    conn = _conn(db)
    dag = _dag(db, conn)
    db.add(_incident(conn, dag, IncidentStatus.RESOLVED))
    db.add(_incident(conn, dag, IncidentStatus.OPEN))
    db.commit()  # many resolved + one open is fine
    db.add(_incident(conn, dag, IncidentStatus.ACKNOWLEDGED))
    with pytest.raises(IntegrityError):
        db.commit()


# ---------------------------------------------------------------------- detection cycles


@pytest.fixture
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeAirflow:
    server = FakeAirflow(major=3)
    monkeypatch.setattr(factory, "live_transport", server.transport())
    return server


FAIL_LOG = "Connecting to postgres://etl:hunter2@db/dw\nTraceback...\nValueError: bad row"


def iso(d: datetime) -> str:
    return d.isoformat()


def incidents(db: Session) -> list[Incident]:
    db.expire_all()
    return list(db.scalars(select(Incident).order_by(Incident.first_seen_at)).all())


def test_cycle_opens_incident_with_scrubbed_evidence(db: Session, fake: FakeAirflow) -> None:
    conn = _conn(db)
    _dag(db, conn)
    fake.add_run("etl", "r1", "success", iso(at(10)), iso(at(10, 5)))
    fake.add_run(
        "etl", "r2", "failed", iso(at(11)), iso(at(11, 2)), failed_task="load", log=FAIL_LOG
    )

    summary = detection_service.run_cycle(db, now=T0)
    assert (summary.opened, summary.dags, summary.errors) == (1, 1, [])

    [incident] = incidents(db)
    assert incident.type == IncidentType.DAG_RUN_FAILED
    assert incident.status == IncidentStatus.OPEN
    assert incident.severity == IncidentSeverity.MEDIUM
    assert incident.run_id == "r2"
    assert "load (try 2)" in incident.summary
    kinds = {e.kind for e in incident.evidence}
    assert kinds == {EvidenceKind.RUN_METADATA, EvidenceKind.TASK_INSTANCE, EvidenceKind.TASK_LOG}
    log = next(e for e in incident.evidence if e.kind == EvidenceKind.TASK_LOG)
    assert "ValueError: bad row" in log.content and "hunter2" not in log.content
    assert [e.event for e in incident.events] == ["opened", "evidence_added", "diagnosed"]
    actions = db.scalars(select(AuditLog.action)).all()
    assert "incident.opened" in actions and "detection.cycle" in actions


def test_cycles_are_idempotent_recur_and_auto_resolve(db: Session, fake: FakeAirflow) -> None:
    conn = _conn(db)
    dag = _dag(db, conn)
    fake.add_run("etl", "r1", "failed", iso(at(10)), failed_task="load", log="x")
    detection_service.run_cycle(db, now=T0)

    again = detection_service.run_cycle(db, now=T0 + timedelta(minutes=2))
    assert (again.opened, again.recurred) == (0, 0)
    assert len(incidents(db)) == 1

    fake.add_run("etl", "r2", "failed", iso(at(12, 5)), failed_task="load", log="y")
    recur = detection_service.run_cycle(db, now=at(12, 10))
    assert recur.recurred == 1
    [incident] = incidents(db)
    assert (incident.occurrence_count, incident.last_run_id) == (2, "r2")

    fake.add_run("etl", "r3", "success", iso(at(12, 15)), iso(at(12, 20)))
    resolved = detection_service.run_cycle(db, now=at(12, 25))
    assert resolved.auto_resolved == 1
    [incident] = incidents(db)
    assert incident.status == IncidentStatus.RESOLVED
    assert incident.resolution == IncidentResolution.AUTO_RECOVERED
    assert incident.resolved_by is None
    assert incident.events[-1].event == "auto_resolved"
    assert incident.events[-1].actor_name == "System"
    db.refresh(dag)
    assert dag.detection_watermark == at(12, 15)

    # A new failure after resolution opens a NEW incident (history preserved).
    fake.add_run("etl", "r4", "failed", iso(at(12, 30)), failed_task="load", log="z")
    detection_service.run_cycle(db, now=at(12, 40))
    assert [i.status for i in incidents(db)] == [IncidentStatus.RESOLVED, IncidentStatus.OPEN]


def test_sla_miss_opens_once_and_resolves_after_success(db: Session, fake: FakeAirflow) -> None:
    conn = _conn(db)
    _dag(db, conn, sla_minutes=60)
    fake.add_run("etl", "r1", "success", iso(at(10)), iso(at(10, 5)))

    first = detection_service.run_cycle(db, now=T0)
    assert first.opened == 1
    [incident] = incidents(db)
    assert incident.type == IncidentType.SLA_MISSED
    assert incident.severity == IncidentSeverity.LOW
    assert "SLA is 60 min" in incident.summary

    again = detection_service.run_cycle(db, now=T0 + timedelta(minutes=5))
    assert (again.opened, again.recurred) == (0, 0)
    assert incidents(db)[0].occurrence_count == 1

    fake.add_run("etl", "r2", "success", iso(at(12, 6)), iso(at(12, 9)))
    done = detection_service.run_cycle(db, now=at(12, 10))
    assert done.auto_resolved == 1


def test_sla_not_breached_within_window(db: Session, fake: FakeAirflow) -> None:
    conn = _conn(db)
    _dag(db, conn, sla_minutes=1)
    fake.add_run("etl", "r1", "success", iso(at(11, 24)), iso(at(11, 25)))
    now = T0.replace(hour=11, minute=25, second=59)
    assert detection_service.run_cycle(db, now=now).opened == 0


def test_one_minute_sla_breach_opens_incident(db: Session, fake: FakeAirflow) -> None:
    conn = _conn(db)
    _dag(db, conn, sla_minutes=1)
    fake.add_run("etl", "r1", "success", iso(at(11, 23)), iso(at(11, 24)))

    summary = detection_service.run_cycle(db, now=T0.replace(hour=11, minute=25, second=1))

    assert summary.opened == 1
    [incident] = incidents(db)
    assert incident.type == IncidentType.SLA_MISSED
    assert "SLA is 1 min" in incident.summary


def test_unreachable_connection_does_not_break_others(db: Session, fake: FakeAirflow) -> None:
    broken = _conn(
        db,
        "broken",
        kind=ConnectionKind.MOCK,
        base_url="http://mock?simulate=UNREACHABLE",
    )
    _dag(db, broken, "orders_pipeline")
    healthy = _conn(db, "healthy")
    _dag(db, healthy)
    fake.add_run("etl", "r1", "failed", iso(at(11)), failed_task="load", log="x")

    summary = detection_service.run_cycle(db, now=T0)
    assert summary.opened == 1
    assert [e["connection"] for e in summary.errors] == ["broken"]
    db.refresh(broken)
    assert broken.last_health_status == ConnectionStatus.UNREACHABLE


def test_severity_escalates_for_prod_tier1(db: Session, fake: FakeAirflow) -> None:
    conn = _conn(db, environment=DeploymentEnvironment.PROD)
    _dag(db, conn, tags=["tier-1"])
    fake.add_run("etl", "r1", "failed", iso(at(11)), failed_task="load", log="x")
    detection_service.run_cycle(db, now=T0)
    assert incidents(db)[0].severity == IncidentSeverity.CRITICAL


def test_unmonitored_and_inactive_are_skipped(db: Session, fake: FakeAirflow) -> None:
    conn = _conn(db)
    dag = MonitoredDag(connection_id=conn.id, dag_id="etl", is_monitored=False, tags=[])
    db.add(dag)
    db.commit()
    fake.add_run("etl", "r1", "failed", iso(at(11)), failed_task="load", log="x")
    assert detection_service.run_cycle(db, now=T0).dags == 0


# ---------------------------------------------------------------------- lease


def test_lease_is_exclusive_until_expiry(db: Session) -> None:
    acquire = detection_service.try_acquire_lease
    assert acquire(db, "worker-a", 60, T0) is True
    assert acquire(db, "worker-b", 60, T0 + timedelta(seconds=30)) is False
    assert acquire(db, "worker-a", 60, T0 + timedelta(seconds=30)) is True  # renew
    assert acquire(db, "worker-b", 60, T0 + timedelta(seconds=91)) is True  # expired
    assert detection_service.get_lease(db).holder == "worker-b"
