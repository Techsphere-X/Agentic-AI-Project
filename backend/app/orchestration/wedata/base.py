"""Tencent Cloud WeData adapter contract: enums, DTOs, and protocol.

Kept free of FastAPI/SQLAlchemy imports so orchestration logic stays framework-independent.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol


class WeDataConnectionStatus(StrEnum):
    UNKNOWN = "UNKNOWN"
    HEALTHY = "HEALTHY"
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    UNREACHABLE = "UNREACHABLE"
    TLS_ERROR = "TLS_ERROR"
    INVALID_ENDPOINT = "INVALID_ENDPOINT"
    WEDATA_ERROR = "WEDATA_ERROR"


class WeDataAuthType(StrEnum):
    TC3_HMAC_SHA256 = "TC3_HMAC_SHA256"


@dataclass(frozen=True)
class WeDataAdapterConfig:
    project_id: str
    region: str = "ap-guangzhou"
    secret_id: str | None = None
    secret_key: str | None = None
    endpoint: str = "wedata.tencentcloudapi.com"
    connect_timeout: float = 5.0
    read_timeout: float = 10.0


@dataclass(frozen=True)
class WeDataConnectionTestResult:
    status: WeDataConnectionStatus
    message: str
    latency_ms: int | None = None
    project_id: str | None = None
    project_name: str | None = None
    api_version: str = "2021-08-20"

    @property
    def ok(self) -> bool:
        return self.status == WeDataConnectionStatus.HEALTHY


@dataclass(frozen=True)
class WeDataTaskSummary:
    task_id: str
    task_name: str
    workflow_id: str | None = None
    workflow_name: str | None = None
    task_type: str | None = "SparkSQL"
    schedule_cron: str | None = None
    is_paused: bool = False
    owner: str | None = None
    tags: list[str] = field(default_factory=list)


TERMINAL_RUN_STATES = frozenset({"success", "failed", "killed"})
IN_PROGRESS_RUN_STATES = frozenset({"running", "waiting", "queued", "pending"})


def normalize_wedata_state(state: str) -> str:
    """Map WeData raw instance states to unified states (success, failed, in_progress, killed)."""
    s = state.lower()
    if s in {"success", "succeed", "finished", "done"}:
        return "success"
    if s in {"failed", "fail", "error", "abnormal"}:
        return "failed"
    if s in {"killed", "stopped", "terminated"}:
        return "killed"
    if s in {"running", "wait", "waiting", "queued", "pending", "dispatching"}:
        return "in_progress"
    return s


@dataclass(frozen=True)
class WeDataInstanceRun:
    run_id: str
    task_id: str
    state: str
    cur_run_date: str | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None

    @property
    def is_terminal(self) -> bool:
        return self.normalized_state in TERMINAL_RUN_STATES

    @property
    def normalized_state(self) -> str:
        return normalize_wedata_state(self.state)

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "task_id": self.task_id,
            "state": self.normalized_state,
            "raw_state": self.state,
            "cur_run_date": self.cur_run_date,
            "start_date": self.start_date.isoformat() if self.start_date else None,
            "end_date": self.end_date.isoformat() if self.end_date else None,
        }


@dataclass(frozen=True)
class WeDataTaskLog:
    content: str
    truncated: bool = False
    available: bool = True


def tail_text(text: str, max_bytes: int) -> tuple[str, bool]:
    """Keep the last max_bytes bytes of text; returns (text, truncated)."""
    data = text.encode("utf-8", errors="replace")
    if len(data) <= max_bytes:
        return text, False
    return data[-max_bytes:].decode("utf-8", errors="ignore"), True


class WeDataAdapterError(Exception):
    """Raised by WeData adapter operations when communication or API fails."""

    def __init__(self, result: WeDataConnectionTestResult) -> None:
        super().__init__(result.message)
        self.result = result


class WeDataAdapter(Protocol):
    async def probe(self) -> WeDataConnectionTestResult: ...

    async def list_tasks(self) -> list[WeDataTaskSummary]: ...

    async def list_task_instances(
        self, task_id: str, *, limit: int = 100
    ) -> list[WeDataInstanceRun]: ...

    async def get_task_log(
        self, task_id: str, run_id: str, *, max_bytes: int = 65536
    ) -> WeDataTaskLog: ...

    async def rerun_instance(self, task_id: str, run_id: str) -> bool: ...

    async def kill_instance(self, task_id: str, run_id: str) -> bool: ...

    async def set_task_paused(self, task_id: str, paused: bool) -> None: ...
