"""Deterministic detection rules over Airflow run history.

Pure functions: no database, HTTP, or framework imports, so they are trivially testable.
"""

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from app.detection.types import IncidentType
from app.orchestration.airflow.base import AirflowDagRun

FAILED = "failed"
SUCCESS = "success"


@dataclass(frozen=True)
class Finding:
    type: IncidentType
    dag_id: str
    occurred_at: datetime
    run: AirflowDagRun | None = None  # failing run (DAG_RUN_FAILED) or last success (SLA)
    sla_minutes: int | None = None

    @property
    def run_id(self) -> str | None:
        return self.run.run_id if self.run and self.type == IncidentType.DAG_RUN_FAILED else None


@dataclass(frozen=True)
class OpenIncidentRef:
    id: uuid.UUID
    type: IncidentType
    occurred_at: datetime


@dataclass(frozen=True)
class Recovery:
    incident_id: uuid.UUID
    run: AirflowDagRun | None  # the successful run that proves recovery


def _terminal_sorted(runs: list[AirflowDagRun]) -> list[AirflowDagRun]:
    """Terminal runs with a usable date, oldest first."""
    terminal = [r for r in runs if r.is_terminal and r.sort_date is not None]
    return sorted(terminal, key=lambda r: r.sort_date)  # type: ignore[arg-type,return-value]


def latest_success(runs: list[AirflowDagRun]) -> AirflowDagRun | None:
    successes = [r for r in _terminal_sorted(runs) if r.state == SUCCESS]
    return successes[-1] if successes else None


def detect_failed_runs(
    dag_id: str, runs: list[AirflowDagRun], watermark: datetime | None
) -> list[Finding]:
    """Failed runs newer than the watermark, oldest first.

    On the first cycle for a DAG (no watermark) history is not backfilled: only failures that
    have not already been followed by a successful run are reported.
    """
    terminal = _terminal_sorted(runs)
    if watermark is not None:
        candidates = [r for r in terminal if r.sort_date > watermark]  # type: ignore[operator]
    else:
        last_ok = latest_success(runs)
        candidates = [
            r
            for r in terminal
            if last_ok is None or r.sort_date > last_ok.sort_date  # type: ignore[operator]
        ]
    return [
        Finding(type=IncidentType.DAG_RUN_FAILED, dag_id=dag_id, occurred_at=r.sort_date, run=r)  # type: ignore[arg-type]
        for r in candidates
        if r.state == FAILED
    ]


def finished_runs(runs: list[AirflowDagRun], watermark: datetime | None) -> list[AirflowDagRun]:
    """Runs that ended (succeeded or failed) after the watermark, oldest first.

    On the first cycle for a DAG (no watermark) only the newest finished run is returned, so
    history is not replayed into workflows.
    """
    terminal = [r for r in _terminal_sorted(runs) if r.state in (SUCCESS, FAILED)]
    if watermark is None:
        return terminal[-1:]
    return [r for r in terminal if r.sort_date > watermark]  # type: ignore[operator]


def _success_time(run: AirflowDagRun) -> datetime:
    return run.end_date or run.sort_date  # type: ignore[return-value]


def is_sla_breached(runs: list[AirflowDagRun], sla_minutes: int, now: datetime) -> bool:
    last_ok = latest_success(runs)
    return last_ok is None or now - _success_time(last_ok) > timedelta(minutes=sla_minutes)


def detect_sla_miss(
    dag_id: str, runs: list[AirflowDagRun], sla_minutes: int | None, now: datetime
) -> Finding | None:
    """Freshness SLA: breached when the last success finished more than sla_minutes ago."""
    if not sla_minutes or not is_sla_breached(runs, sla_minutes, now):
        return None
    return Finding(
        type=IncidentType.SLA_MISSED,
        dag_id=dag_id,
        occurred_at=now,
        run=latest_success(runs),
        sla_minutes=sla_minutes,
    )


def find_recoveries(
    runs: list[AirflowDagRun],
    open_incidents: list[OpenIncidentRef],
    *,
    sla_minutes: int | None,
    now: datetime,
) -> list[Recovery]:
    """Open incidents that a later successful run has recovered."""
    last_ok = latest_success(runs)
    if last_ok is None:
        return []
    recoveries = []
    for incident in open_incidents:
        if incident.type == IncidentType.DAG_RUN_FAILED:
            if last_ok.sort_date > incident.occurred_at:  # type: ignore[operator]
                recoveries.append(Recovery(incident.id, last_ok))
        elif incident.type == IncidentType.SLA_MISSED:
            # SLA removed, or satisfied again.
            if not sla_minutes or not is_sla_breached(runs, sla_minutes, now):
                recoveries.append(Recovery(incident.id, last_ok))
    return recoveries


def count_recent_failures(runs: list[AirflowDagRun], now: datetime, hours: int = 24) -> int:
    cutoff = now - timedelta(hours=hours)
    return sum(
        1 for r in runs if r.state == FAILED and r.sort_date is not None and r.sort_date >= cutoff
    )


def next_watermark(runs: list[AirflowDagRun], current: datetime | None) -> datetime | None:
    terminal = _terminal_sorted(runs)
    if not terminal:
        return current
    newest = terminal[-1].sort_date
    return newest if current is None or newest > current else current  # type: ignore[operator]
