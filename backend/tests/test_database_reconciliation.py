"""Database reconciliation workflow block tests."""

import pytest

from app.automation.graph import Node, catalog, validate_graph
from app.connectors.database import SqlResult
from app.services import automation_nodes


def sql_result(columns: list[str], rows: list[list[object]], *, truncated: bool = False) -> SqlResult:
    return SqlResult(
        columns=columns,
        rows=rows,
        row_count=len(rows) + int(truncated),
        returns_rows=True,
        truncated=truncated,
    )


class StubContext:
    def __init__(self, config: dict[str, object]):
        self.config = config
        self.context: dict[str, object] = {}
        self.calls: list[tuple[str, dict[str, object]]] = []

    def database_connection(self, connection_id: str) -> str:
        return connection_id

    def set_context(self, key: str, value: object) -> None:
        self.context[key] = value

    def get_context(self, key: str, default: object = None) -> object:
        return self.context.get(key, default)

    def record_result(self, node_id: str, result: dict[str, object]) -> None:
        self.context.setdefault("results", {})
        self.context["results"][node_id] = result  # type: ignore[index]


def reconcile_node(**overrides: object) -> Node:
    config: dict[str, object] = {
        "source_connection_id": "source",
        "source_sql": "SELECT id, amount FROM source_table",
        "destination_connection_id": "destination",
        "destination_sql": "SELECT id, amount FROM destination_table",
        "comparison_mode": "rows",
        "key_columns": [],
        "timeout_seconds": 30,
        "max_rows": 100,
        **overrides,
    }
    return Node("reconcile", "database.reconcile", None, config)


def run_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
    source: SqlResult,
    destination: SqlResult,
    **overrides: object,
):
    ctx = StubContext(overrides)
    results = iter([source, destination])
    calls: list[dict[str, object]] = []

    def query(connection: str, sql: str, **kwargs: object) -> SqlResult:
        calls.append({"connection": connection, "sql": sql, **kwargs})
        return next(results)

    monkeypatch.setattr(automation_nodes, "_query", query)
    result = automation_nodes._reconcile(ctx, reconcile_node(**overrides))
    return result, ctx, calls


def test_catalog_and_graph_validation_for_reconciliation() -> None:
    entry = next(item for item in catalog() if item["type"] == "database.reconcile")
    assert entry["label"] == "Reconcile databases"
    assert entry["ports"] == ["matched", "mismatched", "failed"]
    graph = {
        "nodes": [
            {"id": "trigger", "type": "trigger.manual", "config": {}},
            {"id": "reconcile", "type": "database.reconcile", "config": {
                "source_connection_id": "source",
                "source_sql": "SELECT 1",
                "destination_connection_id": "destination",
                "destination_sql": "SELECT 1",
            }},
        ],
        "edges": [{"from": "trigger", "port": "next", "to": "reconcile"}],
    }
    validated = validate_graph(graph)
    assert validated.nodes["reconcile"].config["max_rows"] == 1000


def test_matching_row_counts_are_routed_to_matched(monkeypatch: pytest.MonkeyPatch) -> None:
    result, _, calls = run_reconciliation(
        monkeypatch,
        sql_result(["id"], [[1], [2]]),
        sql_result(["id"], [[9], [8]]),
        comparison_mode="row_count",
    )
    assert result.port == "matched"
    assert result.output["source_row_count"] == result.output["destination_row_count"] == 2
    assert all(call["read_only"] is True for call in calls)


def test_mismatched_row_counts_are_routed_to_mismatched(monkeypatch: pytest.MonkeyPatch) -> None:
    result, _, _ = run_reconciliation(
        monkeypatch,
        sql_result(["id"], [[1], [2]]),
        sql_result(["id"], [[1]]),
        comparison_mode="row_count",
    )
    assert result.port == "mismatched"
    assert result.output["difference_count"] == 1


def test_matching_rows_are_order_independent(monkeypatch: pytest.MonkeyPatch) -> None:
    result, _, _ = run_reconciliation(
        monkeypatch,
        sql_result(["id", "amount"], [[1, 10], [2, 20]]),
        sql_result(["amount", "id"], [[20, 2], [10, 1]]),
    )
    assert result.port == "matched"


def test_key_based_comparison_handles_changed_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    result, _, _ = run_reconciliation(
        monkeypatch,
        sql_result(["id", "amount"], [[1, 10], [2, 20]]),
        sql_result(["id", "amount"], [[2, 20], [1, 99]]),
        key_columns=["id"],
    )
    assert result.port == "mismatched"
    assert result.output["difference_count"] == 1
    assert "amount" not in str(result.output["differences"])


def test_difference_list_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    result, _, _ = run_reconciliation(
        monkeypatch,
        sql_result(["id"], [[i] for i in range(40)]),
        sql_result(["id"], []),
    )
    assert result.port == "mismatched"
    assert len(result.output["differences"]) == automation_nodes.RECONCILIATION_MAX_DIFFERENCES


def test_query_failure_uses_failed_port_without_raw_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = StubContext({})

    def query(*args: object, **kwargs: object) -> SqlResult:
        raise automation_nodes.ActionFailed("password=secret-value")

    monkeypatch.setattr(automation_nodes, "_query", query)
    result = automation_nodes._reconcile(ctx, reconcile_node())
    assert result.port == "failed"
    assert "secret-value" not in str(result.output)


def test_invalid_query_result_uses_failed_port(monkeypatch: pytest.MonkeyPatch) -> None:
    result, _, _ = run_reconciliation(
        monkeypatch,
        SqlResult([], [], 0, False, False),
        sql_result(["id"], [[1]]),
    )
    assert result.port == "failed"