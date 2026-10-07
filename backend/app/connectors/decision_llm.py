"""Client for the local LLM (Ollama) that may pick the fix in a "Choose the fix" block.

Sync on purpose, like `ml_client`: the automation engine is sync. The model only ever chooses
among the fixes the rules allow: Ollama constrains the answer to a JSON schema whose `fix` is an
enum of those fixes, and `remediation.advise` checks it again. Every failure (timeout, connection
error, non-200, bad JSON) raises `AdvisorUnavailable`; consecutive failures open a circuit
breaker so a stopped Ollama costs nothing. Log text is redacted before it is sent.
"""

import json
import time
from collections.abc import Callable
from functools import lru_cache
from typing import Any

import httpx

from app.automation.remediation import FIX_LABELS, Advice, AdvisorUnavailable, Choice, Facts
from app.connectors.ml_client import CircuitBreaker, redact
from app.core.config import Settings, get_settings

MAX_LOG_LINES = 150
MAX_LOG_CHARS = 6000
KEEP_ALIVE = "30m"  # keep the model loaded between decisions; loading it takes ~30s

SYSTEM_PROMPT = """\
You are an SRE deciding how to handle a failed Apache Airflow pipeline run.
Pick exactly one fix from the allowed options. The options were already checked against safety
rules, so do not argue for anything else. Prefer the least disruptive fix that is likely to work;
hand it to a person when the evidence says retrying cannot help.
Weigh the history: fix_attempts are automatic retries or reruns already made on this incident,
and failed_verifications counts those that did not make the run succeed. Repeating a fix that
already failed rarely helps.
Answer with JSON only: {"fix": <one allowed option>, "reason": <one or two plain sentences that
cite the evidence>, "confidence": <0.0-1.0>}."""


def log_tail(logs: list[str]) -> str:
    """The redacted end of the newest log, where the error usually is."""
    if not logs:
        return ""
    lines = redact(logs[0]).splitlines()[-MAX_LOG_LINES:]
    return "\n".join(lines)[-MAX_LOG_CHARS:]


def build_prompt(facts: Facts, choice: Choice, diagnosis: dict[str, Any], logs: list[str]) -> str:
    """The evidence without the rules' own pick, so the model gives an independent opinion."""
    options = "\n".join(f"- {fix}: {FIX_LABELS[fix]}" for fix in choice.allowed)
    evidence = {
        "facts": facts.as_dict(),
        "diagnosis": {
            "label": diagnosis.get("label"),
            "category": diagnosis.get("category"),
            "source": diagnosis.get("source"),
            "confidence": diagnosis.get("confidence"),
            "matched_line": redact(str(diagnosis.get("matched_line") or "")) or None,
        },
    }
    tail = log_tail(logs)
    return (
        f"Allowed options:\n{options}\n\n"
        f"Evidence:\n{json.dumps(evidence, indent=1, default=str)}\n\n"
        f"End of the task log:\n{tail or '(no log available)'}"
    )


def answer_schema(allowed: tuple[str, ...]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            "fix": {"type": "string", "enum": list(allowed)},
            "reason": {"type": "string"},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        },
        "required": ["fix", "reason", "confidence"],
    }


class DecisionLLM:
    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.enabled = settings.DECISION_LLM_ENABLED
        self.model = settings.DECISION_LLM_MODEL
        self.breaker = CircuitBreaker(
            settings.DECISION_LLM_BREAKER_FAILURES,
            settings.DECISION_LLM_BREAKER_COOLDOWN_SECONDS,
            clock,
            name="Decision LLM",
        )
        self._http = httpx.Client(
            base_url=settings.DECISION_LLM_URL.rstrip("/"),
            timeout=settings.DECISION_LLM_TIMEOUT_SECONDS,
            transport=transport,
        )

    def advise(
        self, facts: Facts, choice: Choice, diagnosis: dict[str, Any], logs: list[str]
    ) -> Advice:
        if self.breaker.is_open:
            raise AdvisorUnavailable("circuit open")
        payload = {
            "model": self.model,
            "stream": False,
            "think": False,
            "keep_alive": KEEP_ALIVE,
            "format": answer_schema(choice.allowed),
            "options": {"temperature": 0},
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": build_prompt(facts, choice, diagnosis, logs)},
            ],
        }
        try:
            response = self._http.post("/api/chat", json=payload)
            if response.status_code != 200:
                raise AdvisorUnavailable(f"HTTP {response.status_code}")
            answer = json.loads(response.json()["message"]["content"])
            advice = Advice(
                fix=str(answer["fix"]),
                reason=str(answer["reason"]).strip()[:500],
                confidence=min(1.0, max(0.0, float(answer["confidence"]))),
                model=self.model,
            )
        except AdvisorUnavailable as exc:
            self.breaker.failure(str(exc))
            raise
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            self.breaker.failure(type(exc).__name__)
            raise AdvisorUnavailable(type(exc).__name__) from exc
        self.breaker.success()
        return advice

    def close(self) -> None:
        self._http.close()


@lru_cache
def get_decision_llm() -> DecisionLLM:
    """Process-wide client so the breaker state is shared by every workflow run."""
    return DecisionLLM(get_settings())
