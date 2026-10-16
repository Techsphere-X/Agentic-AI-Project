"""Airflow adapter contract: enums, DTOs and the protocol every adapter implements.

Kept free of FastAPI/SQLAlchemy imports so orchestration logic stays framework-independent.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any, Protocol
from urllib.parse import urlsplit, urlunsplit


class ConnectionStatus(StrEnum):
    UNKNOWN = "UNKNOWN"
    HEALTHY = "HEALTHY"
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    UNREACHABLE = "UNREACHABLE"
    TLS_ERROR = "TLS_ERROR"
    INVALID_ENDPOINT = "INVALID_ENDPOINT"
    AIRFLOW_ERROR = "AIRFLOW_ERROR"


class AuthType(StrEnum):
    BASIC = "BASIC"
    TOKEN = "TOKEN"
    NONE = "NONE"


class ApiVersion(StrEnum):
    V1 = "v1"  # Airflow 2.x
    V2 = "v2"  # Airflow 3.x


@dataclass(frozen=True)
class AdapterConfig:
    base_url: str
    auth_type: AuthType = AuthType.NONE
    username: str | None = None
    secret: str | None = None
    api_version: ApiVersion | None = None  # detected on first probe, then reused
    connect_timeout: float = 5.0
    read_timeout: float = 10.0


@dataclass(frozen=True)
class ConnectionTestResult:
    status: ConnectionStatus
    message: str
    latency_ms: int | None = None
    api_version: ApiVersion | None = None
    airflow_version: str | None = None

    @property
    def ok(self) -> bool:
        return self.status == ConnectionStatus.HEALTHY


@dataclass(frozen=True)
class AirflowDagSummary:
    dag_id: str
    description: str | None = None
    schedule_summary: str | None = None
    is_paused: bool | None = None
    tags: list[str] = field(default_factory=list)


TERMINAL_RUN_STATES = frozenset({"success", "failed"})
IN_PROGRESS_RUN_STATES = frozenset(
    {"queued", "scheduled", "running", "deferred", "up_for_retry", "restarting"}
)


def normalize_run_state(state: str) -> str:
    """Map Airflow's active states to the workflow-facing in_progress state."""
    if state in IN_PROGRESS_RUN_STATES:
        return "in_progress"
    return state


@dataclass(frozen=True)
class AirflowDagRun:
    run_id: str
    state: str
    logical_date: datetime | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None
    run_type: str | None = None

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_RUN_STATES

    @property
    def normalized_state(self) -> str:
        return normalize_run_state(self.state)

    @property
    def sort_date(self) -> datetime | None:
        """Date used to order runs (Airflow 3 manual runs may have no logical_date)."""
        return self.logical_date or self.start_date

    def as_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "state": self.state,
            "run_type": self.run_type,
            "logical_date": _iso(self.logical_date),
            "start_date": _iso(self.start_date),
            "end_date": _iso(self.end_date),
        }


@dataclass(frozen=True)
class AirflowTaskInstance:
    task_id: str
    state: str | None
    try_number: int = 1
    start_date: datetime | None = None
    end_date: datetime | None = None
    operator: str | None = None

    def as_dict(self) -> dict[str, object]:
        duration = (
            (self.end_date - self.start_date).total_seconds()
            if self.start_date and self.end_date
            else None
        )
        return {
            "task_id": self.task_id,
            "state": self.state,
            "try_number": self.try_number,
            "operator": self.operator,
            "start_date": _iso(self.start_date),
            "end_date": _iso(self.end_date),
            "duration_seconds": duration,
        }


@dataclass(frozen=True)
class TaskLog:
    content: str
    truncated: bool = False
    available: bool = True


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


class AirflowAdapterError(Exception):
    """Raised by adapter operations (other than probe) when Airflow cannot be used."""

    def __init__(self, result: ConnectionTestResult) -> None:
        super().__init__(result.message)
        self.result = result


class AirflowAdapter(Protocol):
    async def probe(self) -> ConnectionTestResult: ...

    async def list_dags(self) -> list[AirflowDagSummary]: ...

    async def list_dag_runs(
        self, dag_id: str, *, since: datetime | None = None, limit: int = 100
    ) -> list[AirflowDagRun]:
        """Newest first."""
        ...

    async def list_task_instances(self, dag_id: str, run_id: str) -> list[AirflowTaskInstance]: ...

    async def get_task_log(
        self, dag_id: str, run_id: str, task_id: str, try_number: int, *, max_bytes: int
    ) -> TaskLog:
        """Tail of the task log, at most max_bytes."""
        ...

    # ---- write path (Plan 2: remediation actions)

    async def get_dag(self, dag_id: str) -> AirflowDagSummary | None:
        """None if the DAG does not exist."""
        ...

    async def get_dag_run(self, dag_id: str, run_id: str) -> AirflowDagRun | None:
        """None if the run does not exist."""
        ...

    async def clear_task_instances(
        self, dag_id: str, run_id: str, *, only_failed: bool = True, include_downstream: bool = True
    ) -> list[str]:
        """Clear (retry) task instances of one run. Returns the cleared task ids."""
        ...

    async def trigger_dag_run(
        self, dag_id: str, *, note: str | None = None, conf: dict[str, Any] | None = None
    ) -> AirflowDagRun: ...

    async def set_dag_paused(self, dag_id: str, paused: bool) -> None: ...


def tail_text(text: str, max_bytes: int) -> tuple[str, bool]:
    """Keep the last max_bytes bytes of text; returns (text, truncated)."""
    data = text.encode("utf-8", errors="replace")
    if len(data) <= max_bytes:
        return text, False
    return data[-max_bytes:].decode("utf-8", errors="ignore"), True


def describe_status(
    status: ConnectionStatus,
    base_url: str,
    *,
    http_status: int | None = None,
    airflow_version: str | None = None,
    latency_ms: int | None = None,
) -> str:
    """User-facing, actionable message for each status (see plan0.md §3.3)."""
    match status:
        case ConnectionStatus.HEALTHY:
            return f"Connection verified (Airflow {airflow_version or 'unknown'}, {latency_ms} ms)."
        case ConnectionStatus.UNAUTHORIZED:
            return "Authentication failed. Check username and password/token."
        case ConnectionStatus.FORBIDDEN:
            return (
                "Credentials are valid but lack permission to read DAGs. "
                "Grant the user a Viewer role or higher in Airflow."
            )
        case ConnectionStatus.UNREACHABLE:
            return (
                f"Cannot reach {base_url}. Ensure the Airflow API server is running and "
                "reachable from the backend."
            )
        case ConnectionStatus.TLS_ERROR:
            return f"TLS verification failed for {base_url}. Check the certificate."
        case ConnectionStatus.INVALID_ENDPOINT:
            return (
                "No Airflow REST API found at this URL. Use the webserver root, "
                "e.g. http://host:8080."
            )
        case ConnectionStatus.AIRFLOW_ERROR:
            code = f" ({http_status})" if http_status else ""
            return f"Airflow returned an error{code}. Check the Airflow API server logs."
        case _:
            return "Connection has not been tested yet."


def normalize_base_url(url: str, *, allow_query: bool = False) -> str:
    """Validate and normalize to the host root: scheme://host[:port][/path], no /api/vN suffix."""
    parts = urlsplit(url.strip())
    if parts.scheme not in ("http", "https"):
        raise ValueError("base_url must start with http:// or https://")
    if not parts.hostname:
        raise ValueError("base_url must include a host")
    if parts.username or parts.password:
        raise ValueError("Put credentials in the auth fields, not in base_url")
    if parts.fragment:
        raise ValueError("base_url must not contain a fragment")
    if parts.query and not allow_query:
        raise ValueError("base_url must not contain a query string")
    path = parts.path.rstrip("/")
    for suffix in ("/api/v1", "/api/v2"):
        if path.endswith(suffix):
            path = path[: -len(suffix)]
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, ""))
