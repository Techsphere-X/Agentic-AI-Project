"""Deterministic, explainable severity scoring."""

from dataclasses import dataclass

from app.detection.types import IncidentSeverity, IncidentType

_BASE = {
    IncidentType.DAG_RUN_FAILED: IncidentSeverity.MEDIUM,
    IncidentType.SLA_MISSED: IncidentSeverity.LOW,
    IncidentType.DATA_CHECK_FAILED: IncidentSeverity.MEDIUM,
}
CRITICAL_TAG = "tier-1"
REPEAT_FAILURE_THRESHOLD = 3


@dataclass(frozen=True)
class SeverityScore:
    severity: IncidentSeverity
    reasons: list[str]


def score(
    incident_type: IncidentType,
    *,
    environment: str,
    tags: list[str] | None,
    recent_failures: int,
) -> SeverityScore:
    base = _BASE[incident_type]
    level = base.rank
    reasons = [f"base {base.value.lower()} for {incident_type.value}"]
    if environment == "PROD":
        level += 1
        reasons.append("production connection")
    if CRITICAL_TAG in (tags or []):
        level += 1
        reasons.append(f"DAG tagged {CRITICAL_TAG}")
    if incident_type == IncidentType.DAG_RUN_FAILED and recent_failures >= REPEAT_FAILURE_THRESHOLD:
        level += 1
        reasons.append(f"{recent_failures} failed runs in the last 24h")
    levels = list(IncidentSeverity)
    return SeverityScore(severity=levels[min(level, len(levels) - 1)], reasons=reasons)


def max_severity(a: IncidentSeverity, b: IncidentSeverity) -> IncidentSeverity:
    return a if a.rank >= b.rank else b
