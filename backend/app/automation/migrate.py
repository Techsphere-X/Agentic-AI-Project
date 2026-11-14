"""Convert saved graphs from the replaced DAG start blocks to "When this DAG runs".

"When a DAG run finishes" (`trigger.dag_run`, any number of DAGs) and "Watch selected DAGs"
(`trigger.dag_watch`, one output per DAG, started by incidents) became one block that watches
one DAG: `trigger.dag` with outputs in_progress / success / failed.

Pure: no database access. The data migration and the tests share it.
"""

import copy
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

DAG_PORTS = ("in_progress", "success", "failed")

# Placed after a converted "Watch selected DAGs" trigger: that block started from incidents, so
# the steps after it may update the incident.
_OPEN_INCIDENT_ID = "open_incident"


@dataclass
class Migrated:
    """The result for one saved workflow.

    `graphs`: (DAG id, graph) per workflow to keep; one entry keeps the workflow itself,
    several split it into one workflow per DAG. `disable`: why the workflow must be turned
    off and edited by hand (its graph is then left as it was).
    """

    graphs: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    disable: str | None = None


def needs_migration(graph: dict[str, Any]) -> bool:
    return _legacy_trigger(graph) is not None


def migrate_graph(
    graph: dict[str, Any], connection_for: Callable[[str], str | None]
) -> Migrated | None:
    """None when the graph has no replaced start block. `connection_for(dag_id)` names the
    Airflow connection a DAG belongs to (None when it is not in any synced DAG list)."""
    trigger = _legacy_trigger(graph)
    if trigger is None:
        return None
    config = trigger.get("config") or {}
    if trigger["type"] == "trigger.dag_run":
        dag_ids = [d for d in config.get("dag_ids") or [] if d]
        if not dag_ids:
            return Migrated(
                disable="Choose a DAG: 'When a DAG run finishes' for every DAG was replaced by "
                "'When this DAG runs', which watches one DAG"
            )
        states = [s for s in config.get("states") or ["success", "failed"] if s in DAG_PORTS]
        targets = [(d, None) for d in dag_ids]
    else:  # trigger.dag_watch
        states = ["failed"]
        targets = [
            (d["dag_id"], d.get("connection_id"))
            for d in config.get("dags") or []
            if d.get("dag_id")
        ]
        if not targets:
            return Migrated(disable="Choose a DAG for 'When this DAG runs'")

    result = Migrated()
    for dag_id, connection_id in targets:
        connection_id = connection_id or connection_for(dag_id)
        if not connection_id:
            return Migrated(disable=f"DAG '{dag_id}' is not in any synced DAG list")
        port = next(
            (d.get("key") for d in config.get("dags") or [] if d.get("dag_id") == dag_id), None
        )
        result.graphs.append(
            (dag_id, _convert(graph, trigger, dag_id, connection_id, states, watch_port=port))
        )
    return result


def _legacy_trigger(graph: dict[str, Any]) -> dict[str, Any] | None:
    return next(
        (
            n
            for n in graph.get("nodes") or []
            if n.get("type") in ("trigger.dag_run", "trigger.dag_watch")
        ),
        None,
    )


def _convert(
    graph: dict[str, Any],
    trigger: dict[str, Any],
    dag_id: str,
    connection_id: str,
    states: list[str],
    *,
    watch_port: str | None,
) -> dict[str, Any]:
    out = copy.deepcopy(graph)
    trigger_id = trigger["id"]
    for node in out["nodes"]:
        if node["id"] == trigger_id:
            node["type"] = "trigger.dag"
            node["config"] = {
                "connection_id": connection_id,
                "dag_id": dag_id,
                "states": list(states),
            }
    edges = []
    for edge in out.get("edges") or []:
        if edge["from"] != trigger_id:
            edges.append(edge)
        elif watch_port is not None:
            if edge["port"] == watch_port:  # this DAG's branch; the other DAGs' are dropped
                edges.append({**edge, "port": "failed"})
        elif edge["port"] == "next":  # "any state" → every state the block starts on
            edges.extend({**edge, "port": port} for port in states)
        elif edge["port"] in DAG_PORTS:
            edges.append(edge)
    out["edges"] = _dedupe(edges)
    if watch_port is not None:
        _open_incident_first(out, trigger_id)
    _prune_unreachable(out, trigger_id)
    return out


def _open_incident_first(graph: dict[str, Any], trigger_id: str) -> None:
    """The old block started from an incident: open one first so later steps keep it."""
    first = [e for e in graph["edges"] if e["from"] == trigger_id]
    if not first:
        return
    taken = {n["id"] for n in graph["nodes"]}
    node_id = _OPEN_INCIDENT_ID
    n = 2
    while node_id in taken:
        node_id, n = f"{_OPEN_INCIDENT_ID}_{n}", n + 1
    position = next((n.get("position") for n in graph["nodes"] if n["id"] == trigger_id), None)
    graph["nodes"].append(
        {
            "id": node_id,
            "type": "incident.open",
            "config": {},
            **({"position": {"x": position["x"] + 160, "y": position["y"]}} if position else {}),
        }
    )
    graph["edges"] = [e for e in graph["edges"] if e["from"] != trigger_id]
    graph["edges"].append({"from": trigger_id, "port": "failed", "to": node_id})
    for edge in first:
        for port in ("opened", "already_open"):
            graph["edges"].append({"from": node_id, "port": port, "to": edge["to"]})
    graph["edges"] = _dedupe(graph["edges"])


def _prune_unreachable(graph: dict[str, Any], trigger_id: str) -> None:
    reach, stack = set(), [trigger_id]
    while stack:
        current = stack.pop()
        if current in reach:
            continue
        reach.add(current)
        stack.extend(e["to"] for e in graph["edges"] if e["from"] == current)
    graph["nodes"] = [n for n in graph["nodes"] if n["id"] in reach]
    graph["edges"] = [e for e in graph["edges"] if e["from"] in reach and e["to"] in reach]


def _dedupe(edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen, out = set(), []
    for edge in edges:
        key = (edge["from"], edge["port"], edge["to"])
        if key not in seen:
            seen.add(key)
            out.append(edge)
    return out
