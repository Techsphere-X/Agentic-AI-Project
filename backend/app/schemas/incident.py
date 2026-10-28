import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from app.detection.types import (
    EvidenceKind,
    IncidentResolution,
    IncidentSeverity,
    IncidentStatus,
    IncidentType,
)


class IncidentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    connection_id: uuid.UUID
    monitored_dag_id: uuid.UUID
    dag_id: str
    run_id: str | None
    last_run_id: str | None
    type: IncidentType
    severity: IncidentSeverity
    status: IncidentStatus
    title: str
    summary: str | None
    occurrence_count: int
    occurred_at: datetime
    first_seen_at: datetime
    last_seen_at: datetime
    acknowledged_at: datetime | None
    resolved_at: datetime | None
    resolution: IncidentResolution | None
    resolution_note: str | None


class IncidentOccurrenceRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    incident_id: uuid.UUID
    connection_id: uuid.UUID
    monitored_dag_id: uuid.UUID
    dag_id: str
    type: IncidentType
    title: str
    severity: IncidentSeverity
    status: IncidentStatus
    run_id: str | None
    first_seen_at: datetime
    occurred_at: datetime
    summary: str | None
    details: dict[str, Any]


class EvidenceRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    kind: EvidenceKind
    source: str
    content: str | None
    data: dict[str, Any]
    truncated: bool
    collected_at: datetime


class IncidentEventRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    event: str
    actor_user_id: uuid.UUID | None
    actor_name: str
    details: dict[str, Any]
    created_at: datetime


class ScoredCategory(BaseModel):
    category: str
    score: float


class DiagnosisSuggestion(BaseModel):
    category: str
    label: str | None = None
    confidence: float


class DiagnosisRead(BaseModel):
    category: str
    label: str
    retryable: bool
    confidence: float
    rule: str | None
    matched_line: str | None
    source: Literal["regex", "model", "operator"] = "regex"
    model_version: str | None = None
    top_predictions: list[ScoredCategory] = []
    suggestion: DiagnosisSuggestion | None = None  # low-confidence model answer; display only
    ml_status: Literal["used", "skipped", "unavailable", "disabled"] | None = None
    diagnosed_at: datetime | None = None
    note: str | None = None  # operator corrections
    corrected_by: str | None = None
    previous: dict[str, Any] | None = None


class DiagnosisOverride(BaseModel):
    category: str = Field(min_length=1, max_length=32)
    note: str | None = Field(default=None, max_length=2000)


class LLMAnalysis(BaseModel):
    category: str = "UNKNOWN"
    label: str = "Unknown"
    evidence_line: str | None = None  # verbatim log line the diagnosis rests on
    summary: str
    root_cause: str
    failed_location: str | None = None
    suggested_fix: str
    retry_safe: bool = False
    confidence: float = 0.85
    model: str = "qwen3:4b"
    latency_ms: float | None = None


class ClassifyLogRequest(BaseModel):
    log: str = Field(min_length=1)
    use_llm: bool = True


class ModelOutput(BaseModel):
    category: str
    confidence: float
    top_predictions: list[ScoredCategory]
    model_version: str | None
    signal_line: str | None
    device: str | None = None
    inference_ms: float | None = None


class ClassifyLogResponse(BaseModel):
    regex: DiagnosisRead  # regex alone
    model: ModelOutput | None  # raw model answer (None if disabled/unavailable)
    final: DiagnosisRead  # what the hybrid rule would store
    ml_status: str
    threshold: float
    window: str | None  # the preprocessed text the model actually saw
    llm_analysis: LLMAnalysis | None = None


class IncidentDetail(IncidentRead):
    evidence: list[EvidenceRead]
    events: list[IncidentEventRead]
    occurrences: list[IncidentOccurrenceRead] = []
    diagnosis: DiagnosisRead | None = None


class OpenCount(BaseModel):
    connection_id: uuid.UUID
    dag_id: str
    type: IncidentType
    count: int


class IncidentSummary(BaseModel):
    open_total: int
    by_severity: dict[str, int]


class ResolveRequest(BaseModel):
    note: str = Field(min_length=1, max_length=2000)


class CycleSummaryRead(BaseModel):
    trigger: str
    started_at: datetime
    finished_at: datetime | None
    duration_ms: int
    connections: int
    dags: int
    opened: int
    recurred: int
    auto_resolved: int
    automation_queued: int = 0
    errors: list[dict[str, str]]
    automation: dict[str, Any] | None = None


class LeaseRead(BaseModel):
    holder: str
    expires_at: datetime


class DetectionStatusRead(BaseModel):
    enabled: bool
    loop_running: bool
    interval_seconds: int
    this_worker: str
    lease: LeaseRead | None
    last_cycle: CycleSummaryRead | None
    last_error: str | None
