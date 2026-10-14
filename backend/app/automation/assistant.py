"""Workflow assistant: turns a chat about a workflow into a validated graph proposal.

The model sees the node catalog, the graph-format rules and the current graph, and answers with
`{reply, change_graph, graph}`. A proposed graph goes through `validate_graph`; when it fails,
the problems are sent back for another try (at most `max_repairs` extra turns). The result is
only a proposal: the editor shows the changes and the user decides whether to apply it.

Block configs travel as JSON strings: structured-output schemas need closed objects, and every
block type has its own config shape, which `validate_graph` checks anyway.

Pure: no database or framework imports; the LLM client is passed in.
"""

import json
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from app.automation.graph import (
    INCIDENT_TRIGGER_TYPES,
    MAX_NODES,
    NODE_TYPES,
    GraphError,
    to_raw,
    validate_graph,
)

Mode = Literal["chat", "fix"]


class AssistantLLM(Protocol):
    """A model that answers with one JSON object matching `schema` (see connectors)."""

    provider: str
    model: str

    def complete_json(
        self, system: str, messages: list[dict[str, str]], schema: dict[str, Any]
    ) -> dict[str, Any]: ...


FIX_REQUEST = "Explain these problems in plain words and fix them."

GRAPH_RULES = f"""\
A workflow graph is JSON: {{"nodes": [{{id, type, name, config}}], "edges": [{{from, port, to}}]}}.
- Exactly one trigger node (a type starting with "trigger."). Nothing may link into a trigger.
- A run starts at the trigger and follows, for each block, the edge leaving the output port the
  block finished on. A port with no edge ends the run there; not every port needs an edge.
- `port` must be one of the source block's ports listed below.
- No loops, and every block must be reachable from the trigger.
- Blocks marked [needs incident] only work under an incident trigger
  ({", ".join(sorted(INCIDENT_TRIGGER_TYPES))}). To react to a failed DAG run and FIX it
  (analyze, approve, retry, rerun, pause, update the incident), use "trigger.incident" with
  incident_types ["DAG_RUN_FAILED"]: detection opens an incident for every failed run.
- Blocks marked [needs DAG run] only work under the "trigger.dag_run" trigger.
- Node ids: short, letters, digits, "_" or "-" (for example "start", "logs", "notify"). Keep the
  ids of blocks you keep, so the user's layout survives. At most {MAX_NODES} blocks.
- `config` is a JSON object written as a string, for example "{{\\"level\\": \\"WARNING\\"}}".
  Use "{{}}" for the defaults and only set fields that differ from them. Only use the fields
  listed for that block type.
- Text fields may use placeholders such as {{{{workflow.name}}}}, {{{{incident.title}}}} or
  {{{{dag_run.dag_id}}}}."""

SYSTEM_TEMPLATE = """\
You are the workflow assistant in the editor of a data-pipeline automation platform. People
describe what should happen (for example when an Airflow DAG run fails) and you build or change
the workflow graph for them. You only propose; the user reviews and applies your change.

{rules}

Block types (type - label: description; ports; config fields):
{catalog}

How to answer, as JSON:
- "reply": at most 3 short sentences: what you built or changed (or why you could not), and
  which settings the user still has to fill in (DAG ids, channels, connections, SQL).
- "change_graph": true when you propose a graph, false when you only answer a question.
- "graph": the COMPLETE new graph when change_graph is true (every block and edge, not only
  the changed ones); {{"nodes": [], "edges": []}} otherwise.
- Never invent block types. If something is impossible with these blocks, say so in "reply".
"""


# ------------------------------------------------------------------ catalog prompt


def _resolve(schema: dict[str, Any], defs: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Follow a $ref and unwrap `anyOf [X, null]`; returns (schema, nullable)."""
    nullable = False
    if "anyOf" in schema:
        options = [o for o in schema["anyOf"] if o.get("type") != "null"]
        nullable = len(options) < len(schema["anyOf"])
        if len(options) == 1:
            schema = {**options[0], **{k: v for k, v in schema.items() if k != "anyOf"}}
    if "$ref" in schema:
        target = defs.get(schema["$ref"].rsplit("/", 1)[-1], {})
        schema = {**target, **{k: v for k, v in schema.items() if k != "$ref"}}
    return schema, nullable


def _type_text(schema: dict[str, Any], defs: dict[str, Any]) -> str:
    schema, nullable = _resolve(schema, defs)
    if "enum" in schema:
        text = "one of " + "|".join(json.dumps(v) for v in schema["enum"])
    elif schema.get("type") == "array":
        text = f"list of {_type_text(schema.get('items', {}), defs)}"
    elif "type" in schema:
        text = str(schema["type"])
        bounds = [
            f"{label} {schema[key]}"
            for key, label in (("minimum", ">="), ("maximum", "<="), ("maxLength", "max len"))
            if key in schema
        ]
        if bounds:
            text += f" ({', '.join(bounds)})"
    else:
        text = "value"
    return f"{text} or null" if nullable else text


def _fields_text(config_schema: dict[str, Any]) -> list[str]:
    defs = config_schema.get("$defs", {})
    required = set(config_schema.get("required", []))
    lines = []
    for name, prop in config_schema.get("properties", {}).items():
        if prop.get("x-hidden"):
            continue
        text = f"{name}: {_type_text(prop, defs)}"
        if name in required:
            text += " (required)"
        elif "default" in prop and prop["default"] not in (None, "", []):
            text += f" = {json.dumps(prop['default'])}"
        hint = prop.get("x-hint") or prop.get("x-label")
        if hint:
            text += f" - {hint}"
        lines.append(text)
    return lines


def catalog_prompt() -> str:
    blocks = []
    for nt in NODE_TYPES.values():
        flags = [
            label
            for flag, label in (
                (nt.needs_incident, "[needs incident]"),
                (nt.needs_dag_run, "[needs DAG run]"),
                (nt.is_action, "[action: changes Airflow, policy-checked]"),
            )
            if flag
        ]
        ports = ", ".join(
            f"{p} ({nt.port_labels[p]})" if p in nt.port_labels else p for p in nt.ports
        )
        head = f"* {nt.type} - {nt.label}: {nt.description}"
        if flags:
            head += " " + " ".join(flags)
        lines = [head, f"  ports: {ports}"]
        fields = _fields_text(nt.config_model.model_json_schema())
        lines.append("  config: " + ("; ".join(fields) if fields else "none"))
        blocks.append("\n".join(lines))
    return "\n".join(blocks)


def system_prompt() -> str:
    return SYSTEM_TEMPLATE.format(rules=GRAPH_RULES, catalog=catalog_prompt())


def answer_schema() -> dict[str, Any]:
    def closed(properties: dict[str, Any]) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": properties,
            "required": list(properties),
            "additionalProperties": False,
        }

    node = closed(
        {
            "id": {"type": "string"},
            "type": {"type": "string", "enum": list(NODE_TYPES)},
            "name": {"type": "string"},
            "config": {"type": "string"},
        }
    )
    edge = closed(
        {"from": {"type": "string"}, "port": {"type": "string"}, "to": {"type": "string"}}
    )
    graph = closed(
        {"nodes": {"type": "array", "items": node}, "edges": {"type": "array", "items": edge}}
    )
    return closed(
        {"reply": {"type": "string"}, "change_graph": {"type": "boolean"}, "graph": graph}
    )


# ------------------------------------------------------------------------- diffing


def _label(node: dict[str, Any]) -> str:
    node_type = NODE_TYPES.get(str(node.get("type")))
    return str(node.get("name") or (node_type.label if node_type else node.get("type")))


def _normalized(graph: dict[str, Any] | None) -> dict[str, Any]:
    """A graph with defaults applied when it validates, else as given."""
    if not graph or not graph.get("nodes"):
        return {"nodes": [], "edges": []}
    try:
        return to_raw(validate_graph(graph))
    except GraphError:
        return {"nodes": list(graph.get("nodes") or []), "edges": list(graph.get("edges") or [])}


def diff_graphs(old: dict[str, Any] | None, new: dict[str, Any]) -> dict[str, Any]:
    before, after = _normalized(old), _normalized(new)
    old_nodes = {str(n.get("id")): n for n in before["nodes"] if isinstance(n, dict)}
    new_nodes = {str(n.get("id")): n for n in after["nodes"] if isinstance(n, dict)}
    changes: list[dict[str, str]] = []
    for node_id, node in new_nodes.items():
        prev = old_nodes.get(node_id)
        if prev is None:
            changes.append({"kind": "added", "node": node_id, "label": _label(node)})
        elif any(prev.get(k) != node.get(k) for k in ("type", "name", "config")):
            changes.append({"kind": "changed", "node": node_id, "label": _label(node)})
    for node_id, node in old_nodes.items():
        if node_id not in new_nodes:
            changes.append({"kind": "removed", "node": node_id, "label": _label(node)})

    def edge_set(graph: dict[str, Any]) -> set[tuple[str, str, str]]:
        return {
            (str(e.get("from")), str(e.get("port")), str(e.get("to")))
            for e in graph["edges"]
            if isinstance(e, dict)
        }

    old_edges, new_edges = edge_set(before), edge_set(after)
    return {
        "nodes": changes,
        "edges_added": len(new_edges - old_edges),
        "edges_removed": len(old_edges - new_edges),
    }


# ------------------------------------------------------------------------ proposing


@dataclass
class Proposal:
    graph: dict[str, Any]
    changes: dict[str, Any]


@dataclass
class AssistantResult:
    reply: str
    proposal: Proposal | None = None
    valid: bool = True
    problems: list[dict[str, str]] = field(default_factory=list)
    attempts: int = 1


def _strip_positions(graph: dict[str, Any] | None) -> dict[str, Any]:
    if not graph:
        return {"nodes": [], "edges": []}
    nodes = [
        {k: v for k, v in n.items() if k != "position"}
        for n in graph.get("nodes") or []
        if isinstance(n, dict)
    ]
    return {"nodes": nodes, "edges": list(graph.get("edges") or [])}


def _context_message(current: dict[str, Any] | None, problems: list[dict[str, str]]) -> str:
    graph = _strip_positions(current)
    text = "Current graph in the editor"
    text += (
        f":\n{json.dumps(graph, separators=(',', ':'))}"
        if graph["nodes"]
        else " is empty (no blocks yet)."
    )
    if problems:
        text += "\n\nThe editor reports these problems:\n" + _problems_text(problems)
    return text


def _problems_text(problems: list[dict[str, str]]) -> str:
    lines = []
    for p in problems:
        where = p.get("node") or p.get("edge")
        lines.append(f"- {p.get('message')}" + (f" (at {where})" if where else ""))
    return "\n".join(lines)


def _decode(
    answer_graph: dict[str, Any], current: dict[str, Any] | None
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """The model's graph in editor form: configs parsed, kept blocks keep their position."""
    positions = {
        str(n.get("id")): n["position"]
        for n in (current or {}).get("nodes") or []
        if isinstance(n, dict) and n.get("position")
    }
    problems: list[dict[str, str]] = []
    nodes = []
    for item in answer_graph.get("nodes") or []:
        if not isinstance(item, dict):
            continue
        node_id = str(item.get("id", ""))
        raw_config = item.get("config")
        config: Any = raw_config if isinstance(raw_config, dict) else {}
        if isinstance(raw_config, str) and raw_config.strip():
            try:
                config = json.loads(raw_config)
            except ValueError:
                config = {}
                problems.append({"message": "config is not valid JSON", "node": node_id})
            if not isinstance(config, dict):
                config = {}
                problems.append({"message": "config must be a JSON object", "node": node_id})
        node: dict[str, Any] = {"id": node_id, "type": item.get("type"), "config": config}
        if item.get("name"):
            node["name"] = str(item["name"])
        if node_id in positions:
            node["position"] = positions[node_id]
        nodes.append(node)
    edges = [
        {"from": str(e.get("from")), "port": str(e.get("port")), "to": str(e.get("to"))}
        for e in answer_graph.get("edges") or []
        if isinstance(e, dict)
    ]
    return {"nodes": nodes, "edges": edges}, problems


def _check(
    graph: dict[str, Any], decode_problems: list[dict[str, str]]
) -> tuple[dict[str, Any] | None, list[dict[str, str]]]:
    """Validate; returns (normalized graph or None, problems)."""
    try:
        validated = validate_graph(graph)
    except GraphError as exc:
        return None, decode_problems + exc.problems
    if decode_problems:
        return None, decode_problems
    return to_raw(validated), []


def propose(
    llm: AssistantLLM,
    *,
    mode: Mode,
    messages: list[dict[str, str]],
    current_graph: dict[str, Any] | None,
    problems: list[dict[str, str]] | None = None,
    max_repairs: int = 2,
) -> AssistantResult:
    """Ask the model, validate what it proposes and let it repair invalid graphs."""
    problems = problems if mode == "fix" else []
    history = [m for m in messages if m.get("role") in ("user", "assistant") and m.get("content")]
    if mode == "fix" and (not history or history[-1]["role"] != "user"):
        history.append({"role": "user", "content": FIX_REQUEST})
    if not history or history[-1]["role"] != "user":
        raise ValueError("The conversation must end with a user message")

    # The editor state goes right before the latest request, so earlier turns stay a stable
    # prefix and the model always reads the graph as it is now.
    turns = [
        *history[:-1],
        {
            "role": "user",
            "content": f"{_context_message(current_graph, problems or [])}\n\n"
            f"Request: {history[-1]['content']}",
        },
    ]
    system, schema = system_prompt(), answer_schema()

    attempts = 0
    while True:
        attempts += 1
        answer = llm.complete_json(system, turns, schema)
        reply = str(answer.get("reply") or "").strip()[:4000]
        graph_answer = answer.get("graph") if isinstance(answer.get("graph"), dict) else {}
        if not answer.get("change_graph") or not graph_answer.get("nodes"):
            return AssistantResult(reply=reply or "(no answer)", attempts=attempts)

        proposed, decode_problems = _decode(graph_answer, current_graph)
        normalized, graph_problems = _check(proposed, decode_problems)
        if normalized is not None:
            return AssistantResult(
                reply=reply,
                proposal=Proposal(normalized, diff_graphs(current_graph, normalized)),
                attempts=attempts,
            )
        if attempts > max_repairs:
            return AssistantResult(
                reply=reply,
                proposal=Proposal(proposed, diff_graphs(current_graph, proposed)),
                valid=False,
                problems=graph_problems,
                attempts=attempts,
            )
        turns += [
            {"role": "assistant", "content": json.dumps(answer)},
            {
                "role": "user",
                "content": "That graph fails validation:\n"
                f"{_problems_text(graph_problems)}\n"
                "Return the corrected COMPLETE graph, and keep the reply about the user's request.",
            },
        ]
