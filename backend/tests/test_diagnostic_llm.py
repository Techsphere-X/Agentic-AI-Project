"""Tests for DiagnosticLLM (Qwen 3:4B via Ollama) connector and API integration."""

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from app.api.v1 import diagnosis as diagnosis_api
from app.connectors.diagnostic_llm import (
    MAX_PROMPT_LOG_CHARS,
    MAX_PROMPT_LOG_LINES,
    DiagnosticLLM,
    _prepare_log_context,
)
from app.core.config import get_settings


def fake_clock(start: float = 0.0) -> tuple[Callable[[], float], Callable[[float], None]]:
    current = [start]

    def now() -> float:
        return current[0]

    def advance(seconds: float) -> None:
        current[0] += seconds

    return now, advance


def make_diagnostic_llm(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    enabled: bool = True,
    clock: Callable[[], float] | None = None,
    breaker_failures: int = 2,
    cooldown: int = 60,
) -> DiagnosticLLM:
    settings = get_settings().model_copy(
        update={
            "DIAGNOSTIC_LLM_ENABLED": enabled,
            "DIAGNOSTIC_LLM_BREAKER_FAILURES": breaker_failures,
            "DIAGNOSTIC_LLM_BREAKER_COOLDOWN_SECONDS": cooldown,
            "DIAGNOSTIC_LLM_TIMEOUT_SECONDS": 5.0,
        }
    )
    return DiagnosticLLM(
        settings,
        transport=httpx.MockTransport(handler),
        clock=clock or (lambda: 100.0),
    )


# ------------------------------------------------------------------ Log context preparation


def test_prepare_log_context_empty() -> None:
    assert _prepare_log_context("") == "(empty log)"
    assert _prepare_log_context("   ") == "(empty log)"


def test_prepare_log_context_redacts_secrets() -> None:
    raw = "Connecting with password=super_secret_token_123 to database"
    scrubbed = _prepare_log_context(raw)
    assert "super_secret_token_123" not in scrubbed
    assert "***" in scrubbed


def test_prepare_log_context_slices_tail() -> None:
    lines = [f"line {i}" for i in range(200)]
    raw = "\n".join(lines)
    scrubbed = _prepare_log_context(raw)
    scrubbed_lines = scrubbed.splitlines()
    assert len(scrubbed_lines) <= MAX_PROMPT_LOG_LINES
    assert "line 199" in scrubbed
    assert "line 0" not in scrubbed


def test_prepare_log_context_caps_chars() -> None:
    raw = "A" * (MAX_PROMPT_LOG_CHARS + 500)
    scrubbed = _prepare_log_context(raw)
    assert len(scrubbed) <= MAX_PROMPT_LOG_CHARS


# ------------------------------------------------------------------ DiagnosticLLM unit tests


def test_diagnostic_llm_disabled_returns_none() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        pytest.fail("Should not make HTTP request when disabled")

    llm = make_diagnostic_llm(handler, enabled=False)
    assert llm.analyze("Some error log") is None


def test_diagnostic_llm_success_parses_json() -> None:
    sample_response = {
        "message": {
            "content": json.dumps(
                {
                    "summary": "Database connection pool exhausted.",
                    "root_cause": "Max connections reached on postgres instance.",
                    "failed_location": "backend/app/db/session.py:42",
                    "suggested_fix": "Increase DB_POOL_SIZE or release leaked connections.",
                    "retry_safe": True,
                    "confidence": 0.95,
                }
            )
        }
    }

    sent_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent_requests.append(request)
        return httpx.Response(200, json=sample_response)

    llm = make_diagnostic_llm(handler)
    result = llm.analyze("psycopg2.OperationalError: FATAL: remaining connection slots reserved")

    assert result is not None
    assert result["summary"] == "Database connection pool exhausted."
    assert result["root_cause"] == "Max connections reached on postgres instance."
    assert result["failed_location"] == "backend/app/db/session.py:42"
    assert result["suggested_fix"] == "Increase DB_POOL_SIZE or release leaked connections."
    assert result["retry_safe"] is True
    assert result["confidence"] == 0.95
    assert result["model"] == "qwen3:4b"
    assert isinstance(result["latency_ms"], float)

    # Verify request payload
    assert len(sent_requests) == 1
    body = json.loads(sent_requests[0].content)
    assert body["model"] == "qwen3:4b"
    assert body["think"] is False
    assert body["stream"] is False
    assert "remaining connection slots" in body["messages"][1]["content"]


def test_diagnostic_llm_clamps_confidence() -> None:
    sample_response = {
        "message": {
            "content": json.dumps(
                {
                    "summary": "Disk full",
                    "root_cause": "No space left on device",
                    "suggested_fix": "Clean tmp files",
                    "retry_safe": False,
                    "confidence": 1.5,
                }
            )
        }
    }

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=sample_response)

    llm = make_diagnostic_llm(handler)
    result = llm.analyze("OSError: [Errno 28] No space left on device")
    assert result is not None
    assert result["confidence"] == 1.0


def test_diagnostic_llm_handles_http_errors() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="Internal Server Error")

    llm = make_diagnostic_llm(handler)
    assert llm.analyze("Some log") is None
    assert llm.breaker.failures == 1


def test_diagnostic_llm_handles_malformed_json() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"message": {"content": "Not valid JSON at all!"}})

    llm = make_diagnostic_llm(handler)
    assert llm.analyze("Some log") is None
    assert llm.breaker.failures == 1


def test_diagnostic_llm_circuit_breaker() -> None:
    clock, advance = fake_clock(100.0)
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, text="Service Unavailable")

    llm = make_diagnostic_llm(handler, clock=clock, breaker_failures=2, cooldown=30)

    # 1st failure
    assert llm.analyze("log 1") is None
    assert calls == 1
    assert not llm.breaker.is_open

    # 2nd failure -> trips breaker
    assert llm.analyze("log 2") is None
    assert calls == 2
    assert llm.breaker.is_open

    # 3rd call while open -> short-circuits without making HTTP call
    assert llm.analyze("log 3") is None
    assert calls == 2

    # Advance past cooldown
    advance(35.0)
    assert not llm.breaker.is_open

    # Next call makes HTTP request again
    assert llm.analyze("log 4") is None
    assert calls == 3


# ------------------------------------------------------------------ API Integration tests


def test_api_classify_with_llm(
    client: TestClient,
    viewer_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(get_settings(), "DIAGNOSTIC_LLM_ENABLED", True)

    class FakeDiagLLM:
        def analyze(self, log_text: str) -> dict[str, Any]:
            return {
                "summary": "Memory limit exceeded during sorting.",
                "root_cause": "Worker killed by OOM killer.",
                "failed_location": "dags/etl_orders.py:120",
                "suggested_fix": "Allocate more memory in Kubernetes pod spec.",
                "retry_safe": False,
                "confidence": 0.98,
                "model": "qwen3:4b",
                "latency_ms": 142.5,
            }

    monkeypatch.setattr(diagnosis_api, "get_diagnostic_llm", lambda: FakeDiagLLM())

    response = client.post(
        "/api/v1/diagnosis/classify",
        json={"log": "Task exited with return code -9 (OOMKilled)", "use_llm": True},
        headers=viewer_headers,
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["llm_analysis"] is not None
    llm = data["llm_analysis"]
    assert llm["summary"] == "Memory limit exceeded during sorting."
    assert llm["root_cause"] == "Worker killed by OOM killer."
    assert llm["failed_location"] == "dags/etl_orders.py:120"
    assert llm["suggested_fix"] == "Allocate more memory in Kubernetes pod spec."
    assert llm["retry_safe"] is False
    assert llm["model"] == "qwen3:4b"


def test_api_classify_skip_llm_when_use_llm_false(
    client: TestClient,
    viewer_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(get_settings(), "DIAGNOSTIC_LLM_ENABLED", True)

    def fail_if_called():
        pytest.fail("Should not call LLM when use_llm is False")

    monkeypatch.setattr(diagnosis_api, "get_diagnostic_llm", fail_if_called)

    response = client.post(
        "/api/v1/diagnosis/classify",
        json={"log": "SyntaxError: invalid syntax", "use_llm": False},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["llm_analysis"] is None


def test_api_classify_graceful_when_llm_throws(
    client: TestClient,
    viewer_headers: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(get_settings(), "DIAGNOSTIC_LLM_ENABLED", True)

    class CrashingLLM:
        def analyze(self, _log: str) -> dict[str, Any]:
            raise RuntimeError("Ollama connection failed unexpectedly")

    monkeypatch.setattr(diagnosis_api, "get_diagnostic_llm", lambda: CrashingLLM())

    response = client.post(
        "/api/v1/diagnosis/classify",
        json={"log": "ValueError: invalid literal for int()", "use_llm": True},
        headers=viewer_headers,
    )
    assert response.status_code == 200
    data = response.json()
    # API succeeded and safely defaulted llm_analysis to None
    assert data["llm_analysis"] is None
    assert data["regex"]["category"] == "DATA_INTEGRITY"
