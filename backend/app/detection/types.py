"""Detection vocabulary shared by rules, models and API (no framework imports)."""

from enum import StrEnum


class IncidentType(StrEnum):
    DAG_RUN_FAILED = "DAG_RUN_FAILED"
    SLA_MISSED = "SLA_MISSED"
    DATA_CHECK_FAILED = "DATA_CHECK_FAILED"  # a workflow found a finished run's data invalid


class IncidentSeverity(StrEnum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

    @property
    def rank(self) -> int:
        return list(IncidentSeverity).index(self)


class IncidentStatus(StrEnum):
    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"


class IncidentResolution(StrEnum):
    MANUAL = "MANUAL"
    AUTO_RECOVERED = "AUTO_RECOVERED"
    AUTO_REMEDIATED = "AUTO_REMEDIATED"  # fixed by an automation workflow (Plan 2)


class EvidenceKind(StrEnum):
    RUN_METADATA = "RUN_METADATA"
    TASK_INSTANCE = "TASK_INSTANCE"
    TASK_LOG = "TASK_LOG"
