import json
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from app.api.v1 import automation as automation_api
from app.automation import assistant
from app.automation.graph import NODE_TYPES
from app.connectors.assistant_llm import (
    AnthropicAssistant,
    AssistantUnavailable,
    OllamaAssistant,
)
from app.core.config import get_settings

API = "/api/v1/automation"


def node(node_id: str, node_type: str, config: dict[str, Any] | None = None, **extra: Any):
    return {"id": node_id, "type": node_type, "config": config or {}, **extra}


def answer_graph(nodes: list[dict[str, Any]], edges: list[tuple[str, str, str]]):
    """A graph as the model sends it: configs as JSON strings."""
    return {
        "nodes": [
            {
                "id": n["id"],
                "type": n["type"],
                "name": n.get("name", ""),
                "config": json.dumps(n["config"]),
            }
            for n in nodes
        ],
        "edges": [{"from": s, "port": p, "to": d} for s, p, d in edges],
    }


VALID = answer_graph(
    [
        node("t", "trigger.incident"),
        node("n", "notify", {"level": "WARNING"}, name="Tell the team"),
    ],
    [("t", "next", "n")],
)
# `notify` has no "approved" port.
INVALID = answer_graph(
    [node("t", "trigger.incident"), node("n", "notify")], [("t", "approved", "n")]
)


class FakeLLM:
    provider = "fake"
    model = "fake-1"

    def __init__(self, *answers: dict[str, Any]) -> None:
        self.answers = list(answers)
        self.calls: list[dict[str, Any]] = []

    def complete_json(self, system, messages, schema):
        self.calls.append({"system": system, "messages": [dict(m) for m in messages]})
        return self.answers.pop(0)


def proposal(graph: dict[str, Any], reply: str = "Done") -> dict[str, Any]:
    return {"reply": reply, "change_graph": True, "graph": graph}


def ask(llm, text="Build it", **kwargs):
    kwargs.setdefault("mode", "chat")
    kwargs.setdefault("current_graph", None)
    return assistant.propose(llm, messages=[{"role": "user", "content": text}], **kwargs)


# --------------------------------------------------------------------- prompt / schema


def test_catalog_prompt_lists_every_block_type_and_its_ports() -> None:
    prompt = assistant.system_prompt()
    for node_type in NODE_TYPES.values():
        assert f"* {node_type.type} - " in prompt
    assert "ports: retry (retry failed tasks)" in prompt  # port labels
    assert "[needs incident]" in prompt
    assert "x-hidden" not in prompt and "channel: one of" not in prompt  # hidden fields skipped


def test_answer_schema_is_closed_for_structured_outputs() -> None:
    def check(schema: dict[str, Any]) -> None:
        if schema.get("type") == "object":
            assert schema["additionalProperties"] is False
            assert set(schema["required"]) == set(schema["properties"])
            for prop in schema["properties"].values():
                check(prop)
        if schema.get("type") == "array":
            check(schema["items"])

    schema = assistant.answer_schema()
    check(schema)
    node_schema = schema["properties"]["graph"]["properties"]["nodes"]["items"]
    assert node_schema["properties"]["type"]["enum"] == list(NODE_TYPES)


# ------------------------------------------------------------------------- proposing


def test_valid_proposal_is_normalized_and_diffed() -> None:
    llm = FakeLLM(proposal(VALID, "Added a notification"))
    result = ask(llm)

    assert result.valid and result.problems == [] and result.attempts == 1
    assert result.reply == "Added a notification"
    notify = next(n for n in result.proposal.graph["nodes"] if n["id"] == "n")
    assert notify["config"]["level"] == "WARNING"
    assert notify["config"]["title"] == "{{workflow.name}}"  # defaults applied
    assert notify["name"] == "Tell the team"
    kinds = {(c["kind"], c["node"]) for c in result.proposal.changes["nodes"]}
    assert kinds == {("added", "t"), ("added", "n")}
    assert result.proposal.changes["edges_added"] == 1
    # The editor state is sent with the latest request.
    assert "Current graph in the editor is empty" in llm.calls[0]["messages"][-1]["content"]


def test_answer_without_graph_is_just_a_reply() -> None:
    llm = FakeLLM({"reply": "Use a filter block.", "change_graph": False, "graph": VALID})
    result = ask(llm, "Which block filters by DAG?")
    assert result.proposal is None and result.reply == "Use a filter block."


def test_invalid_proposal_triggers_a_repair_turn_with_the_problems() -> None:
    llm = FakeLLM(proposal(INVALID), proposal(VALID, "Fixed"))
    result = ask(llm)

    assert result.valid and result.attempts == 2 and result.reply == "Fixed"
    repair = llm.calls[1]["messages"]
    assert repair[-2]["role"] == "assistant"
    assert "has no output port 'approved'" in repair[-1]["content"]


def test_gives_up_after_max_repairs_and_returns_the_invalid_graph() -> None:
    llm = FakeLLM(proposal(INVALID), proposal(INVALID))
    result = ask(llm, max_repairs=1)

    assert not result.valid and result.attempts == 2 and len(llm.calls) == 2
    assert any("approved" in p["message"] for p in result.problems)
    assert result.proposal is not None  # shown in the editor, but cannot be applied


def test_bad_config_json_is_reported_per_block() -> None:
    graph = answer_graph([node("t", "trigger.manual")], [])
    graph["nodes"][0]["config"] = "{not json"
    result = ask(FakeLLM(proposal(graph)), max_repairs=0)
    assert not result.valid
    assert result.problems == [{"message": "config is not valid JSON", "node": "t"}]


def test_kept_blocks_keep_their_position_and_changes_are_detected() -> None:
    current = {
        "nodes": [
            node("t", "trigger.incident", position={"x": 10, "y": 20}),
            node("n", "notify", {"level": "INFO"}, position={"x": 10, "y": 200}),
            node("old", "notify", position={"x": 300, "y": 200}),
        ],
        "edges": [
            {"from": "t", "port": "next", "to": "n"},
            {"from": "t", "port": "next", "to": "old"},
        ],
    }
    result = ask(FakeLLM(proposal(VALID)), current_graph=current)

    positions = {n["id"]: n.get("position") for n in result.proposal.graph["nodes"]}
    assert positions == {"t": {"x": 10.0, "y": 20.0}, "n": {"x": 10.0, "y": 200.0}}
    kinds = {(c["kind"], c["node"]) for c in result.proposal.changes["nodes"]}
    assert kinds == {("changed", "n"), ("removed", "old")}
    assert result.proposal.changes["edges_removed"] == 1


def test_fix_mode_sends_the_problems_and_a_default_request() -> None:
    llm = FakeLLM(proposal(VALID))
    assistant.propose(
        llm,
        mode="fix",
        messages=[],
        current_graph={"nodes": [node("t", "trigger.incident")], "edges": []},
        problems=[{"message": "Node 'x' is not connected to the trigger", "node": "x"}],
    )
    last = llm.calls[0]["messages"][-1]["content"]
    assert "is not connected to the trigger (at x)" in last
    assert last.endswith(assistant.FIX_REQUEST)


def test_conversation_must_end_with_the_user() -> None:
    with pytest.raises(ValueError):
        assistant.propose(
            FakeLLM(),
            mode="chat",
            messages=[{"role": "assistant", "content": "hi"}],
            current_graph=None,
        )


# ------------------------------------------------------------------------- providers


def test_ollama_payload_and_failure_modes() -> None:
    seen: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(json.loads(request.content))
        return httpx.Response(200, json={"message": {"content": json.dumps({"reply": "ok"})}})

    llm = OllamaAssistant(get_settings(), transport=httpx.MockTransport(handler))
    schema = assistant.answer_schema()
    assert llm.complete_json("SYS", [{"role": "user", "content": "hi"}], schema) == {"reply": "ok"}
    payload = seen[0]
    assert payload["format"] == schema and payload["stream"] is False
    assert payload["messages"][0] == {"role": "system", "content": "SYS"}
    assert payload["options"]["num_ctx"] >= 8192

    broken = OllamaAssistant(
        get_settings(),
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"message": {}})),
    )
    with pytest.raises(AssistantUnavailable, match="malformed"):
        broken.complete_json("S", [], schema)


def test_anthropic_uses_structured_output_and_maps_refusals() -> None:
    calls: list[dict[str, Any]] = []

    def response(stop_reason: str, text: str):
        def create(**kwargs):
            calls.append(kwargs)
            return SimpleNamespace(
                stop_reason=stop_reason, content=[SimpleNamespace(type="text", text=text)]
            )

        return SimpleNamespace(beta=SimpleNamespace(messages=SimpleNamespace(create=create)))

    schema = assistant.answer_schema()
    ok = AnthropicAssistant(get_settings(), client=response("end_turn", '{"reply": "hi"}'))
    assert ok.complete_json("SYS", [{"role": "user", "content": "x"}], schema) == {"reply": "hi"}
    sent = calls[0]
    assert sent["system"] == "SYS"
    assert sent["output_config"]["format"] == {"type": "json_schema", "schema": schema}
    assert sent["fallbacks"] == "default"

    refused = AnthropicAssistant(get_settings(), client=response("refusal", ""))
    with pytest.raises(AssistantUnavailable, match="declined"):
        refused.complete_json("SYS", [{"role": "user", "content": "x"}], schema)


# ------------------------------------------------------------------------------- API


def test_api_is_off_by_default(
    client: TestClient, viewer_headers, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "ASSISTANT_ENABLED", False)
    status = client.get(f"{API}/assistant/status", headers=viewer_headers).json()
    assert status["enabled"] is False and status["provider"] == "ollama"
    body = {"messages": [{"role": "user", "content": "hi"}]}
    response = client.post(f"{API}/assistant", json=body, headers=viewer_headers)
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "assistant_unavailable"


def test_api_returns_a_proposal(
    client: TestClient, viewer_headers, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "ASSISTANT_ENABLED", True)
    llm = FakeLLM(proposal(VALID, "Here you go"))
    monkeypatch.setattr(automation_api, "get_assistant_llm", lambda: llm)

    body = {"mode": "chat", "messages": [{"role": "user", "content": "notify on incidents"}]}
    response = client.post(f"{API}/assistant", json=body, headers=viewer_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["valid"] and data["reply"] == "Here you go"
    assert {n["id"] for n in data["proposal"]["graph"]["nodes"]} == {"t", "n"}


def test_api_maps_provider_failures_to_503(
    client: TestClient, viewer_headers, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(get_settings(), "ASSISTANT_ENABLED", True)

    class Down(FakeLLM):
        def complete_json(self, system, messages, schema):
            raise AssistantUnavailable("Could not reach Ollama")

    monkeypatch.setattr(automation_api, "get_assistant_llm", lambda: Down())
    body = {"messages": [{"role": "user", "content": "hi"}]}
    response = client.post(f"{API}/assistant", json=body, headers=viewer_headers)
    assert response.status_code == 503
    assert response.json()["error"]["message"] == "Could not reach Ollama"
