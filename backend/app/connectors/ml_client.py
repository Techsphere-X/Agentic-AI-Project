"""Client for the ML classification service (ml_service), with a circuit breaker.

Sync on purpose: API routes and the detection loop are sync. Every failure (timeout, connection
error, non-200, bad payload) raises `MLUnavailable`; after `ML_BREAKER_FAILURES` consecutive
failures the breaker opens for `ML_BREAKER_COOLDOWN_SECONDS` and calls fail fast without touching
the network. Log text is redacted again before it leaves the process.
"""

import logging
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any

import httpx

from app.core.config import Settings, get_settings
from app.detection.evidence import REDACTED, scrub
from app.diagnosis.hybrid import MLUnavailable

logger = logging.getLogger(__name__)

# Stay under the service's 256KB body cap (JSON escaping included).
MAX_PAYLOAD_LOG_BYTES = 200 * 1024
_BEARER = re.compile(r"\b(Bearer|Basic)\s+[A-Za-z0-9._~+/\-]+=*", re.I)


@dataclass(frozen=True)
class ModelResult:
    category: str
    confidence: float
    top_predictions: list[dict[str, Any]]
    model_version: str | None
    signal_line: str | None
    window: str = ""
    device: str | None = None
    inference_ms: float | None = None


def redact(text: str) -> str:
    """Secrets out: password=/token=/api keys, Authorization/Bearer values, URL credentials."""
    return _BEARER.sub(rf"\1 {REDACTED}", scrub(text))


def _fit(logs: list[str], budget: int = MAX_PAYLOAD_LOG_BYTES) -> list[str]:
    """Trim each log to an equal share of the byte budget, keeping its tail (where errors are)."""
    share = budget // max(1, len(logs))
    out = []
    for log in logs:
        data = log.encode("utf-8")
        out.append(data[-share:].decode("utf-8", errors="ignore") if len(data) > share else log)
    return out


@dataclass
class CircuitBreaker:
    failures_to_open: int
    cooldown_seconds: float
    clock: Callable[[], float] = time.monotonic
    name: str = "ML service"
    failures: int = 0
    open_until: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @property
    def is_open(self) -> bool:
        return self.clock() < self.open_until

    def success(self) -> None:
        with self._lock:
            self.failures = 0
            self.open_until = 0.0

    def failure(self, reason: str) -> None:
        with self._lock:
            self.failures += 1
            if self.failures >= self.failures_to_open and not self.is_open:
                self.open_until = self.clock() + self.cooldown_seconds
                logger.warning(
                    "%s circuit opened for %ss after %s consecutive failures (last: %s)",
                    self.name,
                    self.cooldown_seconds,
                    self.failures,
                    reason,
                )


class MLClient:
    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.enabled = settings.ML_SERVICE_ENABLED
        self.breaker = CircuitBreaker(
            settings.ML_BREAKER_FAILURES, settings.ML_BREAKER_COOLDOWN_SECONDS, clock
        )
        self._http = httpx.Client(
            base_url=settings.ML_SERVICE_URL.rstrip("/"),
            timeout=settings.ML_SERVICE_TIMEOUT_SECONDS,
            headers={"X-ML-Token": settings.ML_SERVICE_TOKEN},
            transport=transport,
        )

    def classify(self, logs: list[str], top_k: int = 3) -> ModelResult:
        if self.breaker.is_open:
            raise MLUnavailable("circuit open")
        payload = {"logs": _fit([redact(log) for log in logs if log]), "top_k": top_k}
        try:
            response = self._http.post("/v1/classify", json=payload)
            if response.status_code != 200:
                raise MLUnavailable(f"HTTP {response.status_code}")
            body = response.json()
            result = ModelResult(
                category=str(body["category"]),
                confidence=float(body["confidence"]),
                top_predictions=list(body.get("top_predictions") or []),
                model_version=body.get("model_version"),
                signal_line=body.get("signal_line"),
                window=body.get("window") or "",
                device=body.get("device"),
                inference_ms=body.get("inference_ms"),
            )
        except MLUnavailable as exc:
            self.breaker.failure(str(exc))
            raise
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            self.breaker.failure(type(exc).__name__)
            raise MLUnavailable(type(exc).__name__) from exc
        self.breaker.success()
        return result

    def close(self) -> None:
        self._http.close()


@lru_cache
def get_ml_client() -> MLClient:
    """Process-wide client so the breaker state is shared by routes and the detection loop."""
    return MLClient(get_settings())
