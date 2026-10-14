"""LLM clients for the workflow assistant (chat in the canvas editor).

Sync on purpose, like `decision_llm`: FastAPI runs sync endpoints in a thread pool. Each client
returns one JSON object that matches the given schema; `app.automation.assistant` validates what
the object means. Every failure raises `AssistantUnavailable`, and consecutive failures open a
circuit breaker so a stopped Ollama or a bad API key fails fast.

Providers: "ollama" (local, the default) and "anthropic" (Claude API). Only the Anthropic one
sends data off the machine: the user's messages and the current graph.
"""

import json
import time
from collections.abc import Callable
from functools import lru_cache
from typing import Any

import httpx

from app.automation.assistant import AssistantLLM
from app.connectors.ml_client import CircuitBreaker
from app.core.config import Settings, get_settings

KEEP_ALIVE = "30m"
# The system prompt is ~3.4k tokens and an answer up to ~1k; Ollama's default context (often
# 4096) would cut that. Bigger costs VRAM: on a 4 GB GPU more of the model then runs on the CPU.
OLLAMA_NUM_CTX = 8192
# Claude models that accept the server-side refusal fallback (`fallbacks: "default"`).
FALLBACK_MODELS = ("claude-fable-5-1", "claude-opus-5-5", "claude-opus-5", "claude-sonnet-5-5")


class AssistantUnavailable(Exception):
    """The model could not answer (disabled, unreachable, refused, or returned bad JSON)."""


def _breaker(settings: Settings, clock: Callable[[], float]) -> CircuitBreaker:
    return CircuitBreaker(
        settings.ASSISTANT_BREAKER_FAILURES,
        settings.ASSISTANT_BREAKER_COOLDOWN_SECONDS,
        clock,
        name="Workflow assistant",
    )


def _parse_object(text: str) -> dict[str, Any]:
    answer = json.loads(text)
    if not isinstance(answer, dict):
        raise ValueError("answer is not a JSON object")
    return answer


class OllamaAssistant:
    provider = "ollama"

    def __init__(
        self,
        settings: Settings,
        *,
        transport: httpx.BaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.model = settings.ASSISTANT_OLLAMA_MODEL
        self.breaker = _breaker(settings, clock)
        self._http = httpx.Client(
            base_url=settings.ASSISTANT_OLLAMA_URL.rstrip("/"),
            timeout=settings.ASSISTANT_TIMEOUT_SECONDS,
            transport=transport,
        )

    def complete_json(
        self, system: str, messages: list[dict[str, str]], schema: dict[str, Any]
    ) -> dict[str, Any]:
        if self.breaker.is_open:
            raise AssistantUnavailable("The local model is not responding; try again shortly")
        payload = {
            "model": self.model,
            "stream": False,
            "think": False,
            "keep_alive": KEEP_ALIVE,
            "format": schema,
            "options": {"temperature": 0.2, "num_ctx": OLLAMA_NUM_CTX},
            "messages": [{"role": "system", "content": system}, *messages],
        }
        try:
            response = self._http.post("/api/chat", json=payload)
            if response.status_code != 200:
                raise AssistantUnavailable(f"Ollama answered HTTP {response.status_code}")
            answer = _parse_object(response.json()["message"]["content"])
        except AssistantUnavailable as exc:
            self.breaker.failure(str(exc))
            raise
        except httpx.TimeoutException as exc:
            self.breaker.failure("timeout")
            raise AssistantUnavailable(
                f"The local model did not answer within {self._http.timeout.read:.0f}s "
                "(ASSISTANT_TIMEOUT_SECONDS); small GPUs need a few minutes per answer"
            ) from exc
        except httpx.HTTPError as exc:
            self.breaker.failure(type(exc).__name__)
            raise AssistantUnavailable(
                f"Could not reach Ollama at {self._http.base_url} ({type(exc).__name__})"
            ) from exc
        except (ValueError, KeyError, TypeError) as exc:
            self.breaker.failure(type(exc).__name__)
            raise AssistantUnavailable("The local model returned malformed JSON") from exc
        self.breaker.success()
        return answer

    def close(self) -> None:
        self._http.close()


class AnthropicAssistant:
    provider = "anthropic"

    def __init__(
        self,
        settings: Settings,
        *,
        client: Any = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        import anthropic  # optional dependency: only needed when this provider is selected

        self._anthropic = anthropic
        self.model = settings.ASSISTANT_ANTHROPIC_MODEL
        self.breaker = _breaker(settings, clock)
        self._client = client or anthropic.Anthropic(
            api_key=settings.ANTHROPIC_API_KEY or None,
            timeout=settings.ASSISTANT_TIMEOUT_SECONDS,
        )

    def complete_json(
        self, system: str, messages: list[dict[str, str]], schema: dict[str, Any]
    ) -> dict[str, Any]:
        anthropic = self._anthropic
        if self.breaker.is_open:
            raise AssistantUnavailable("The Claude API is not responding; try again shortly")
        extra: dict[str, Any] = {}
        if self.model in FALLBACK_MODELS:
            extra = {"betas": ["server-side-fallback-2026-07-01"], "fallbacks": "default"}
        try:
            response = self._client.beta.messages.create(
                model=self.model,
                max_tokens=16000,
                system=system,
                messages=messages,
                output_config={
                    "effort": "medium",
                    "format": {"type": "json_schema", "schema": schema},
                },
                **extra,
            )
            if response.stop_reason == "refusal":
                raise AssistantUnavailable("Claude declined this request")
            if response.stop_reason == "max_tokens":
                raise AssistantUnavailable("Claude's answer was cut off; ask for a smaller change")
            text = next(b.text for b in response.content if b.type == "text")
            answer = _parse_object(text)
        except AssistantUnavailable as exc:
            self.breaker.failure(str(exc))
            raise
        except anthropic.AuthenticationError as exc:
            self.breaker.failure("authentication")
            raise AssistantUnavailable("Claude API key is missing or invalid") from exc
        except anthropic.RateLimitError as exc:
            self.breaker.failure("rate limited")
            raise AssistantUnavailable("Claude API rate limit reached; try again shortly") from exc
        except anthropic.APIStatusError as exc:
            self.breaker.failure(f"HTTP {exc.status_code}")
            raise AssistantUnavailable(f"Claude API error (HTTP {exc.status_code})") from exc
        except anthropic.APIConnectionError as exc:
            self.breaker.failure(type(exc).__name__)
            raise AssistantUnavailable("Could not reach the Claude API") from exc
        except anthropic.AnthropicError as exc:  # e.g. no credentials configured
            self.breaker.failure(type(exc).__name__)
            raise AssistantUnavailable(f"Claude API is not configured ({exc})") from exc
        except (StopIteration, ValueError, TypeError) as exc:
            self.breaker.failure(type(exc).__name__)
            raise AssistantUnavailable("Claude returned malformed JSON") from exc
        self.breaker.success()
        return answer


def model_name(settings: Settings) -> str:
    if settings.ASSISTANT_PROVIDER == "anthropic":
        return settings.ASSISTANT_ANTHROPIC_MODEL
    return settings.ASSISTANT_OLLAMA_MODEL


@lru_cache
def get_assistant_llm() -> AssistantLLM:
    """Process-wide client so the breaker state is shared by every request."""
    settings = get_settings()
    if settings.ASSISTANT_PROVIDER == "anthropic":
        return AnthropicAssistant(settings)
    return OllamaAssistant(settings)
