"""Static and configuration tests for the container-only reconciliation DAG."""

import ast
import os
from pathlib import Path
from urllib.parse import urlparse

import pytest

DAG_FILE = Path(__file__).parents[2] / "deploy" / "airflow" / "dags" / "agentic_test_dags.py"


def _config_function():
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    selected = [
        node
        for node in tree.body
        if isinstance(node, (ast.Assign, ast.FunctionDef))
        and (isinstance(node, ast.Assign) or node.name == "_reconciliation_config")
    ]
    namespace = {"os": os, "urlparse": urlparse}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(DAG_FILE), "exec"), namespace)
    return namespace["_reconciliation_config"]


def test_dag_definition_is_manual_and_tagged() -> None:
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    dag_function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "agentic_reconciliation_pipeline"
    )
    dag_call = next(
        decorator for decorator in dag_function.decorator_list if isinstance(decorator, ast.Call)
    )
    values = {
        keyword.arg: ast.literal_eval(keyword.value)
        for keyword in dag_call.keywords
        if keyword.arg in {"dag_id", "schedule", "catchup", "tags"}
    }
    assert values == {
        "dag_id": "agentic_reconciliation_pipeline",
        "schedule": None,
        "catchup": False,
        "tags": ["agentic-test", "reconciliation-test"],
    }
    task_names = {node.name for node in dag_function.body if isinstance(node, ast.FunctionDef)}
    assert task_names == {"validate_test_configuration", "prepare_test_data", "validate_fixture"}


def test_configuration_requires_explicit_opt_in_and_dedicated_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config_function()
    database_url = "postgresql://user:pass@db/agentic_reconciliation_test"
    monkeypatch.setenv("AGENTIC_RECONCILIATION_TEST_DATABASE_URL", database_url)
    monkeypatch.setenv("AGENTIC_RECONCILIATION_TEST_DATA_ENABLED", "false")
    with pytest.raises(RuntimeError, match="AGENTIC_RECONCILIATION_TEST_DATA_ENABLED"):
        config()

    monkeypatch.setenv("AGENTIC_RECONCILIATION_TEST_DATA_ENABLED", "true")
    monkeypatch.setenv("AGENTIC_RECONCILIATION_SCENARIO", "mismatch")
    assert config()[1] == "mismatch"

    monkeypatch.setenv(
        "AGENTIC_RECONCILIATION_TEST_DATABASE_URL",
        "postgresql://user:pass@db/agentic_ai",
    )
    with pytest.raises(RuntimeError, match="agentic_reconciliation_test"):
        config()


def test_configuration_rejects_unknown_scenario(monkeypatch: pytest.MonkeyPatch) -> None:
    config = _config_function()
    monkeypatch.setenv("AGENTIC_RECONCILIATION_TEST_DATA_ENABLED", "true")
    monkeypatch.setenv(
        "AGENTIC_RECONCILIATION_TEST_DATABASE_URL",
        "postgresql://user:pass@db/agentic_reconciliation_test",
    )
    monkeypatch.setenv("AGENTIC_RECONCILIATION_SCENARIO", "unexpected")
    with pytest.raises(RuntimeError, match="match or mismatch"):
        config()