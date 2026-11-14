"""Diagnostic LLM connector (Ollama / Qwen 3:4B), the Log Analyzer's only AI model.

Analyzes a failure log and returns:
- Failure category (same taxonomy as the regex classifier) and the verbatim evidence line
- Human-readable summary and root cause explanation
- Failed component / file location (only when the log names one)
- Step-by-step fix recommendation and retry safety judgment

Context: secrets are redacted, Airflow log prefixes stripped, and the window keeps the first
error/traceback plus the tail, so the originating exception survives long logs.
Grounding: the answer is checked against the log; an evidence line or location the log does
not contain is dropped and confidence is capped.
Resilience: Circuit breaker protects against hangs or timeouts if Ollama is busy or stopped.
"""

import json
import logging
import re
import time
from collections.abc import Callable
from functools import lru_cache
from typing import Any

import httpx

from app.connectors.ml_client import CircuitBreaker, redact
from app.core.config import Settings, get_settings
from app.diagnosis.log_classifier import CATEGORY_LABELS, RETRYABLE, Diagnosis, FailureCategory

logger = logging.getLogger(__name__)

# ~6.5k chars of log is ~2.2k tokens; with the system prompt and the answer that fits a
# 4096-token context, which keeps qwen3:4b plus its KV cache inside a 4 GB GPU.
MAX_PROMPT_LOG_LINES = 120
MAX_PROMPT_LOG_CHARS = 6500
MAX_LINE_CHARS = 400
LINES_BEFORE_ERROR = 15
LINES_AFTER_ERROR = 40
TAIL_LINES = 40
UNGROUNDED_MAX_CONFIDENCE = 0.5
KEEP_ALIVE = "15m"

# Airflow prefixes: "[2026-10-01, 12:00:00 UTC] {taskinstance.py:2905} " or "[...Z] "
_AIRFLOW_PREFIX = re.compile(r"^\[\d{4}-\d{2}-\d{2}[^\]]*\]\s*(?:\{[^}]*\}\s*)?")
_FIRST_ERROR = re.compile(
    r"Traceback \(most recent call last\)|\bERROR\b|\bFATAL\b|\w+(?:Error|Exception)\b:"
)
_TASK_IDS = re.compile(r"\b(dag_id|task_id|run_id)=([\w.:+-]+)")
_LOCATION_FILE = re.compile(r"[\w.-]+\.\w+")

_CATEGORY_HINTS: dict[FailureCategory, str] = {
    FailureCategory.AUTH: "credentials rejected, permission denied, expired token, HTTP 401/403",
    FailureCategory.DATA_INTEGRITY: (
        "constraint or duplicate-key violation, NULL in NOT NULL column, a value that cannot "
        "be parsed or cast"
    ),
    FailureCategory.SCHEMA: "table/column missing or renamed, column type changed upstream",
    FailureCategory.CODE_BUG: (
        "a bug in task code: KeyError, TypeError, AttributeError, NameError, ImportError, "
        "SyntaxError, bad SQL written by the author"
    ),
    FailureCategory.RESOURCE: (
        "out of memory, OOMKilled / exit code 137 / return code -9, disk full, "
        "connection slots or pool exhausted"
    ),
    FailureCategory.TIMEOUT: "task execution timeout, statement/query timeout, deadline exceeded",
    FailureCategory.TRANSIENT_NETWORK: (
        "connection reset/refused, DNS failure, SSL handshake error, HTTP 502/503/504"
    ),
    FailureCategory.UPSTREAM_MISSING: (
        "input file, partition, S3 key or upstream table not there yet; sensor gave up waiting"
    ),
    FailureCategory.UNKNOWN: "the log does not show enough to tell",
}


def _category_guide() -> str:
    lines = []
    for category in FailureCategory:
        retry = "retryable" if category in RETRYABLE else "needs a fix first"
        lines.append(
            f"- {category.value} ({CATEGORY_LABELS[category]}; {retry}): "
            f"{_CATEGORY_HINTS[category]}"
        )
    return "\n".join(lines)


_EXAMPLE_LOG = """\
INFO - Running query on warehouse
ERROR - Task failed with exception
Traceback (most recent call last):
  File "/opt/airflow/dags/orders_etl.py", line 88, in transform
    total = row["order_total"]
KeyError: 'order_total'"""

_EXAMPLE_ANSWER = {
    "evidence_line": "KeyError: 'order_total'",
    "category": "CODE_BUG",
    "root_cause": (
        "transform() in orders_etl.py reads the key 'order_total', which the source rows do "
        "not contain (likely renamed or never selected)."
    ),
    "failed_location": "/opt/airflow/dags/orders_etl.py:88",
    "summary": "The transform step crashed on a missing 'order_total' field.",
    "suggested_fix": (
        "1. Check the query/columns feeding transform() for the real field name.\n"
        "2. Update orders_etl.py line 88 to use it, or use row.get() with a default.\n"
        "3. Re-run the task after deploying the fix."
    ),
    "retry_safe": False,
    "confidence": 0.9,
}

SYSTEM_PROMPT = f"""\
You are a senior data-platform SRE diagnosing failed Airflow / ETL task logs.
Return one JSON object that matches the schema. Base every field ONLY on the log.

Categories (pick exactly one):
{_category_guide()}

Rules:
1. Find the ORIGINATING error. Skip wrapper lines such as "Task failed with exception" and
   follow-up errors caused by the first one; the root is usually the last line of the first
   traceback.
2. evidence_line: copy that one error line exactly as it appears in the log. Do not rephrase.
3. failed_location: the user-code file:line, query or table named in the log. Prefer DAG/task
   code over library frames. Use null if the log does not name one. Never invent paths.
4. root_cause: explain WHY it failed in 1-2 sentences, naming the concrete object involved.
5. suggested_fix: 2-4 numbered, concrete steps an engineer can act on.
6. retry_safe: true only if simply re-running can succeed (network, timeout, resources,
   missing upstream). Code, schema, data and auth problems need a fix first, so false.
7. confidence: 0.85-1.0 when the error line states the cause clearly, 0.5-0.85 when you must
   infer, below 0.5 when the log is truncated or ambiguous.
8. A pattern-rule hint may be given. Agree with it only if the log supports it.

Example log:
```
{_EXAMPLE_LOG}
```
Example answer:
{json.dumps(_EXAMPLE_ANSWER)}"""

# Property order matters: the model writes the evidence first and reasons from it.
DIAGNOSTIC_SCHEMA = {
    "type": "object",
    "properties": {
        "evidence_line": {"type": ["string", "null"]},
        "category": {"type": "string", "enum": [c.value for c in FailureCategory]},
        "root_cause": {"type": "string"},
        "failed_location": {"type": ["string", "null"]},
        "summary": {"type": "string"},
        "suggested_fix": {"type": "string"},
        "retry_safe": {"type": "boolean"},
        "confidence": {"type": "number"},
    },
    "required": [
        "evidence_line",
        "category",
        "root_cause",
        "summary",
        "suggested_fix",
        "retry_safe",
        "confidence",
    ],
}


def _clip(line: str) -> str:
    return line if len(line) <= MAX_LINE_CHARS else line[:MAX_LINE_CHARS] + " …"


def _prepare_log_context(log_text: str) -> str:
    """Redact secrets, strip log prefixes, keep the first error plus the tail."""
    if not log_text or not log_text.strip():
        return "(empty log)"
    lines = [_clip(_AIRFLOW_PREFIX.sub("", line)) for line in redact(log_text).strip().splitlines()]
    if len(lines) > MAX_PROMPT_LOG_LINES:
        first = next((i for i, line in enumerate(lines) if _FIRST_ERROR.search(line)), None)
        tail_start = len(lines) - TAIL_LINES
        if first is None:
            lines = lines[-MAX_PROMPT_LOG_LINES:]
        elif first + LINES_AFTER_ERROR >= tail_start:
            lines = lines[max(0, first - LINES_BEFORE_ERROR) :][-MAX_PROMPT_LOG_LINES:]
        else:
            head_end = first + LINES_AFTER_ERROR
            head = lines[max(0, first - LINES_BEFORE_ERROR) : head_end]
            lines = [*head, f"... [{tail_start - head_end} lines omitted] ...", *lines[tail_start:]]
    text = "\n".join(lines)
    if len(text) <= MAX_PROMPT_LOG_CHARS:
        return text
    # Too long even after line selection: keep the start (first error) and the end.
    marker = "\n... [truncated] ...\n"
    head_chars = int(MAX_PROMPT_LOG_CHARS * 0.45)
    tail_chars = MAX_PROMPT_LOG_CHARS - head_chars - len(marker)
    return text[:head_chars] + marker + text[-tail_chars:]


def _build_prompt(context: str, hint: Diagnosis | None) -> str:
    parts = []
    ids = dict(_TASK_IDS.findall(context))
    if ids:
        parts.append("Task: " + ", ".join(f"{k}={v}" for k, v in ids.items()))
    if hint is not None and hint.category is not FailureCategory.UNKNOWN:
        parts.append(
            f"Pattern-rule hint: {hint.category.value} (rule {hint.rule})"
            + (f' on line "{hint.matched_line}"' if hint.matched_line else "")
        )
    parts.append(f"Log:\n```\n{context}\n```")
    parts.append("Diagnose the originating failure and answer with the JSON object.")
    return "\n\n".join(parts)


def _norm(text: str) -> str:
    return " ".join(text.split()).lower()


def _grounded_evidence(evidence: Any, context: str) -> str | None:
    if not evidence or not str(evidence).strip():
        return None
    line = str(evidence).strip()
    return line[:300] if _norm(line) in _norm(context) else None


def _grounded_location(location: Any, context: str) -> str | None:
    """Keep the location only if the file (or name) it points at appears in the log."""
    if not location or not str(location).strip():
        return None
    value = str(location).strip()
    files = _LOCATION_FILE.findall(value.replace("\\", "/").split("/")[-1])
    needle = files[0] if files else value.split(":")[0]
    return value if needle.lower() in context.lower() else None


def _category(value: Any) -> FailureCategory:
    try:
        return FailureCategory(str(value).strip().upper())
    except ValueError:
        return FailureCategory.UNKNOWN


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
        self.options = {
            "temperature": 0.1,
            "top_p": 0.8,
            "top_k": 20,
            "repeat_penalty": 1.05,
            "seed": 42,
            "num_ctx": settings.DIAGNOSTIC_LLM_NUM_CTX,
            "num_predict": settings.DIAGNOSTIC_LLM_MAX_TOKENS,
        }
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

    def analyze(self, log_text: str, hint: Diagnosis | None = None) -> dict[str, Any] | None:
        """Run deep generative analysis on the log. Returns dict on success, None on failure."""
        if not self.enabled:
            return None
        if self.breaker.is_open:
            logger.info("Diagnostic LLM circuit breaker open; skipping LLM analysis")
            return None

        context = _prepare_log_context(log_text)
        payload = {
            "model": self.model,
            "stream": False,
            "think": False,
            "keep_alive": KEEP_ALIVE,
            "format": DIAGNOSTIC_SCHEMA,
            "options": self.options,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": _build_prompt(context, hint)},
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
            parsed = json.loads(raw_content)
            self.breaker.success()
            latency_ms = round((time.perf_counter() - start) * 1000, 1)

            category = _category(parsed.get("category"))
            evidence = _grounded_evidence(parsed.get("evidence_line"), context)
            confidence = max(0.0, min(1.0, float(parsed.get("confidence", 0.5))))
            if evidence is None:
                confidence = min(confidence, UNGROUNDED_MAX_CONFIDENCE)

            return {
                "category": category.value,
                "label": CATEGORY_LABELS[category],
                "evidence_line": evidence,
                "summary": str(parsed.get("summary") or "Failure detected in task execution."),
                "root_cause": str(
                    parsed.get("root_cause") or "Root cause could not be determined."
                ),
                "failed_location": _grounded_location(parsed.get("failed_location"), context),
                "suggested_fix": str(
                    parsed.get("suggested_fix") or "Inspect task logs and dependencies."
                ),
                "retry_safe": bool(parsed.get("retry_safe", False)) and category in RETRYABLE,
                "confidence": confidence,
                "model": self.model,
                "latency_ms": latency_ms,
                "window": context,
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
