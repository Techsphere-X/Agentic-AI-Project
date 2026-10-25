"""Diagnostic LLM connector (Ollama / Qwen 3:4B) for deep generative log analysis.

Analyzes raw or scrubbed failure logs to generate:
- Human-readable summary
- Root cause explanation
- Failed component / file location
- Step-by-step fix recommendation
- Retry safety judgment

Scrubbing: Credentials and tokens are redacted before sending to Ollama.
Resilience: Circuit breaker protects against hangs or timeouts if Ollama is busy or stopped.
"""

import json
import logging
import time
from collections.abc import Callable
from functools import lru_cache
from typing import Any

import httpx

from app.connectors.ml_client import CircuitBreaker, redact
from app.core.config import Settings, get_settings

logger = logging.getLogger(__name__)

MAX_PROMPT_LOG_LINES = 100
MAX_PROMPT_LOG_CHARS = 4500
KEEP_ALIVE = "15m"

SYSTEM_PROMPT = """\
You are an expert Data Platform SRE and Software Engineer analyzing task error logs.
Analyze the provided log error carefully. Identify the root cause, exact location
(file/line/query), suggested remediation, and whether an automatic retry is safe."""

DIAGNOSTIC_SCHEMA = {
    "type": "object",
    "properties": {
        "summary": {"type": "string"},
        "root_cause": {"type": "string"},
        "failed_location": {"type": ["string", "null"]},
        "suggested_fix": {"type": "string"},
        "retry_safe": {"type": "boolean"},
        "confidence": {"type": "number"},
    },
    "required": ["summary", "root_cause", "suggested_fix", "retry_safe", "confidence"],
}


def _prepare_log_context(log_text: str) -> str:
    """Redact secrets and extract the high-signal tail."""
    if not log_text or not log_text.strip():
        return "(empty log)"
    cleaned = redact(log_text).strip()
    lines = cleaned.splitlines()[-MAX_PROMPT_LOG_LINES:]
    return "\n".join(lines)[-MAX_PROMPT_LOG_CHARS:]


class DiagnosticLLM:
    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.enabled = settings.DIAGNOSTIC_LLM_ENABLED
        self.model = settings.DIAGNOSTIC_LLM_MODEL
        self.breaker = CircuitBreaker(
            settings.DIAGNOSTIC_LLM_BREAKER_FAILURES,
            settings.DIAGNOSTIC_LLM_BREAKER_COOLDOWN_SECONDS,
            clock,
            name="Diagnostic LLM",
        )
        self._http = httpx.Client(
            base_url=settings.DIAGNOSTIC_LLM_URL.rstrip("/"),
            timeout=settings.DIAGNOSTIC_LLM_TIMEOUT_SECONDS,
            transport=transport,
        )

    def analyze(self, log_text: str) -> dict[str, Any] | None:
        """Run deep generative analysis on the log. Returns dict on success, None on failure."""
        if not self.enabled:
            return None
        if self.breaker.is_open:
            logger.info("Diagnostic LLM circuit breaker open; skipping LLM analysis")
            return None

        context = _prepare_log_context(log_text)
        prompt = (
            f"Here is the error log to analyze:\n\n"
            f"```\n{context}\n```\n\n"
            f"Analyze the error, locate the failure, and propose remediation."
        )

        payload = {
            "model": self.model,
            "stream": False,
            "think": False,
            "keep_alive": KEEP_ALIVE,
            "format": DIAGNOSTIC_SCHEMA,
            "options": {"temperature": 0.1},
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        }

        start = time.perf_counter()
        try:
            response = self._http.post("/api/chat", json=payload)
            if response.status_code != 200:
                self.breaker.failure(f"HTTP {response.status_code}")
                logger.warning(
                    "Diagnostic LLM failed: HTTP %s: %s", response.status_code, response.text
                )
                return None

            data = response.json()
            raw_content = data.get("message", {}).get("content", "").strip()
            # Parse JSON response
            parsed = json.loads(raw_content)
            self.breaker.success()
            latency_ms = round((time.perf_counter() - start) * 1000, 1)

            return {
                "summary": str(parsed.get("summary") or "Failure detected in task execution."),
                "root_cause": str(
                    parsed.get("root_cause") or "Root cause could not be determined."
                ),
                "failed_location": str(parsed["failed_location"])
                if parsed.get("failed_location")
                else None,
                "suggested_fix": str(
                    parsed.get("suggested_fix") or "Inspect task logs and dependencies."
                ),
                "retry_safe": bool(parsed.get("retry_safe", False)),
                "confidence": max(0.0, min(1.0, float(parsed.get("confidence", 0.85)))),
                "model": self.model,
                "latency_ms": latency_ms,
            }
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            self.breaker.failure(type(exc).__name__)
            logger.warning("Diagnostic LLM unavailable: %s", exc)
            return None
        except Exception as exc:
            self.breaker.failure(f"parse/runtime: {type(exc).__name__}")
            logger.warning("Diagnostic LLM error during analysis: %s", exc)
            return None


@lru_cache(maxsize=1)
def get_diagnostic_llm() -> DiagnosticLLM:
    return DiagnosticLLM(get_settings())
