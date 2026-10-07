"""Plan 2 / Phase 1: log classifier and the Airflow adapter write path (live v1/v2 + mock)."""

from datetime import UTC, datetime, timedelta

import pytest

from app.diagnosis.log_classifier import FailureCategory, classify, classify_logs
from app.orchestration.airflow import mock as mock_module
from app.orchestration.airflow.base import (
    AdapterConfig,
    AirflowAdapterError,
    ApiVersion,
    AuthType,
    ConnectionStatus,
)
from app.orchestration.airflow.client import LiveAirflowAdapter
from app.orchestration.airflow.mock import MockAirflowAdapter
from tests.fake_airflow import FakeAirflow

# ---------------------------------------------------------------------- classifier

C = FailureCategory


@pytest.mark.parametrize(
    ("log", "category"),
    [
        (
            "psycopg.errors.UniqueViolation: duplicate key value violates unique constraint",
            C.DATA_INTEGRITY,
        ),
        ("sqlalchemy.exc.IntegrityError: NOT NULL constraint failed", C.DATA_INTEGRITY),
        ('psycopg.errors.UndefinedColumn: column "amount" does not exist', C.SCHEMA),
        ("sqlite3.OperationalError: no such table: staging_orders", C.SCHEMA),
        ("NameError: name 'df' is not defined", C.CODE_BUG),
        ("ModuleNotFoundError: No module named 'pandas'", C.CODE_BUG),
        ("psycopg.OperationalError: password authentication failed for user etl", C.AUTH),
        ("botocore.exceptions.ClientError: Access Denied", C.AUTH),
        ("Task exited with return code -9 (OOMKilled)", C.RESOURCE),
        ("OSError: [Errno 28] No space left on device", C.RESOURCE),
        ("airflow.exceptions.AirflowTaskTimeout: Timeout, PID: 1234", C.TIMEOUT),
        ("requests.exceptions.ReadTimeout: read timed out", C.TIMEOUT),
        ("requests.exceptions.ConnectionError: Connection refused", C.TRANSIENT_NETWORK),
        ("HTTPError: 503 Server Error: Service Unavailable", C.TRANSIENT_NETWORK),
        ("socket.gaierror: Temporary failure in name resolution", C.TRANSIENT_NETWORK),
        (
            "FileNotFoundError: [Errno 2] No such file or directory: '/data/in.csv'",
            C.UPSTREAM_MISSING,
        ),
        ("botocore.errorfactory.NoSuchKey: The specified key does not exist", C.UPSTREAM_MISSING),
        ("Task failed for reasons nobody understands", C.UNKNOWN),
    ],
)
def test_classifier_categories(log: str, category: FailureCategory) -> None:
    diagnosis = classify(f"[2026-09-24] INFO - starting\n{log}\n[2026-09-24] INFO - done")
    assert diagnosis.category == category
    if category != C.UNKNOWN:
        assert diagnosis.matched_line == log  # the whole offending line, not the context


def test_specific_causes_win_over_generic_code_errors() -> None:
    log = "Traceback...\nKeyError: 'x'\npsycopg.errors.UniqueViolation: duplicate key"
    assert classify(log).category == C.DATA_INTEGRITY
    assert classify(log).retryable is False


def test_noise_lines_and_timestamps_do_not_decide_the_category() -> None:
    log = (
        "[2026-09-30T00:13:37.503+0000] {taskinstance.py:1225} INFO - Marking task as FAILED.\n"
        "[2026-09-30T00:13:37.100+0000] {http.py:88} WARNING - Connection reset, retrying (1/3)\n"
        "Traceback (most recent call last):\n"
        '  File "/site-packages/requests/adapters.py", line 903, in send\n'
        "    raise ConnectionError(e, request=request)\n"
        "    ^^^^^^^^^^\n"
        "\x1b[31m[2026-09-30T00:13:38.000+0000] {logging_mixin.py:190} INFO - heartbeat ok\x1b[0m\n"
        'psycopg2.errors.InvalidTextRepresentation: invalid input syntax for type integer: "12a"'
    )
    diagnosis = classify(log)
    assert diagnosis.category == C.DATA_INTEGRITY
    assert diagnosis.retryable is False


def test_killed_task_reported_at_info_level_is_a_resource_failure() -> None:
    log = "[2026-09-30] {local_task_job_runner.py:266} INFO - Task exited with return code -9"
    assert classify(log).category == C.RESOURCE


@pytest.mark.parametrize(
    ("log", "category"),
    [
        ("google.api_core.exceptions.Forbidden: 403 Quota exceeded for bytes scanned", C.RESOURCE),
        ("msal: AADSTS7000222: The provided client secret keys are expired.", C.AUTH),
        ("pyspark AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] `amt`", C.SCHEMA),
        ("ValueError: time data '2026-13-01' does not match format '%Y-%m-%d'", C.DATA_INTEGRITY),
        ("snowflake: Statement reached its statement or warehouse timeout of 3600", C.TIMEOUT),
        (
            "httpx.RemoteProtocolError: Server disconnected without sending a response.",
            C.TRANSIENT_NETWORK,
        ),
        (
            "AirflowException: The external task load_raw in DAG ingest_orders failed.",
            C.UPSTREAM_MISSING,
        ),
        ("RecursionError: maximum recursion depth exceeded", C.CODE_BUG),
    ],
)
def test_common_vendor_errors(log: str, category: FailureCategory) -> None:
    assert classify(f"[2026-09-30] ERROR - {log}").category == category


def test_retryable_flags_and_empty_logs() -> None:
    assert classify("ConnectionResetError: Connection reset by peer").retryable is True
    assert classify("SyntaxError: invalid syntax").retryable is False
    empty = classify("")
    assert (empty.category, empty.rule, empty.retryable) == (C.UNKNOWN, "no_log", True)
    assert classify_logs([None, "", "nothing useful"]).rule == "no_rule_matched"
    assert classify_logs(["nothing", "TimeoutError: boom"]).category == C.TIMEOUT


def test_mock_failure_logs_classify_as_expected() -> None:
    orders = mock_module._FAILURE_LOG.format(start="s", end="e", run_id="r", logical="l")
    partner = mock_module._TRANSIENT_LOG.format(start="s", end="e", run_id="r", logical="l")
    assert classify(orders).category == C.DATA_INTEGRITY
    assert classify(partner).category == C.TRANSIENT_NETWORK


# ---------------------------------------------------------------------- live adapter writes


def live(fake: FakeAirflow) -> LiveAirflowAdapter:
    return LiveAirflowAdapter(
        AdapterConfig(
            base_url="http://airflow.test:8080",
            auth_type=AuthType.TOKEN,
            secret="static-token",
            api_version=ApiVersion.V2 if fake.major == 3 else ApiVersion.V1,
        ),
        transport=fake.transport(),
    )


def seeded(major: int) -> FakeAirflow:
    fake = FakeAirflow(major=major)
    fake.add_run(
        "etl_a", "r1", "failed", "2026-09-24T10:00:00+00:00", failed_task="load", log="boom"
    )
    return fake


@pytest.mark.parametrize("major", [2, 3])
async def test_clear_task_instances(major: int) -> None:
    fake = seeded(major)
    cleared = await live(fake).clear_task_instances("etl_a", "r1")
    assert cleared == ["load"]
    method, path, body, _ = fake.writes[-1]
    assert (method, path) == ("POST", "etl_a/clearTaskInstances")
    assert body == {
        "dry_run": False,
        "dag_run_id": "r1",
        "only_failed": True,
        "include_downstream": True,
        "reset_dag_runs": True,
    }


@pytest.mark.parametrize(("major", "has_logical"), [(2, False), (3, True)])
async def test_trigger_dag_run(major: int, has_logical: bool) -> None:
    fake = seeded(major)
    run = await live(fake).trigger_dag_run("etl_a", note="by automation")
    assert run.state == "queued" and run.run_id.startswith("manual__")
    _, path, body, _ = fake.writes[-1]
    assert path == "etl_a/dagRuns"
    assert ("logical_date" in body) is has_logical
    assert body["note"] == "by automation"


@pytest.mark.parametrize("major", [2, 3])
async def test_pause_get_dag_and_get_run(major: int) -> None:
    fake = seeded(major)
    adapter = live(fake)
    await adapter.set_dag_paused("etl_a", True)
    method, path, body, params = fake.writes[-1]
    assert (method, path, body, params) == (
        "PATCH",
        "etl_a",
        {"is_paused": True},
        {"update_mask": "is_paused"},
    )
    dag = await adapter.get_dag("etl_a")
    assert dag is not None and dag.is_paused is True
    assert await adapter.get_dag("nope") is None
    run = await adapter.get_dag_run("etl_a", "r1")
    assert run is not None and run.state == "failed"
    assert await adapter.get_dag_run("etl_a", "missing") is None


async def test_rejected_write_surfaces_airflow_message() -> None:
    fake = seeded(3)
    fake.reject_writes = 409
    with pytest.raises(AirflowAdapterError) as exc:
        await live(fake).trigger_dag_run("etl_a")
    assert exc.value.result.status == ConnectionStatus.AIRFLOW_ERROR
    assert "409" in exc.value.result.message and "Rejected by fake" in exc.value.result.message


# ---------------------------------------------------------------------- mock writes


@pytest.fixture
def partner_clock(monkeypatch: pytest.MonkeyPatch) -> list[datetime]:
    """30s into a partner_api_sync bucket whose previous run failed (list: mutable time)."""
    current = int(datetime.now(UTC).timestamp()) // 600
    bucket = current - ((current - 1) % 4)  # bucket % 4 == 1 -> bucket-1 failed
    now = [datetime.fromtimestamp(bucket * 600 + 30, UTC)]
    monkeypatch.setattr(mock_module, "clock", lambda: now[0])
    return now


def mock_adapter() -> MockAirflowAdapter:
    return MockAirflowAdapter(AdapterConfig(base_url="http://mock-airflow"), simulate_latency=False)


async def test_mock_transient_failure_recovers_after_clear(partner_clock: list[datetime]) -> None:
    adapter = mock_adapter()
    runs = await adapter.list_dag_runs(
        "partner_api_sync", since=partner_clock[0] - timedelta(minutes=30)
    )
    failed = next(r for r in runs if r.state == "failed")
    log = await adapter.get_task_log(
        "partner_api_sync", failed.run_id, "fetch_partner_orders", 2, max_bytes=65536
    )
    assert "503" in log.content

    assert await adapter.clear_task_instances("partner_api_sync", failed.run_id) == [
        "fetch_partner_orders",
        "load_partner_orders",
    ]
    assert (await adapter.get_dag_run("partner_api_sync", failed.run_id)).state == "running"
    partner_clock[0] += timedelta(seconds=mock_module.RERUN_SECONDS + 1)
    assert (await adapter.get_dag_run("partner_api_sync", failed.run_id)).state == "success"
    # A different connection (base URL) does not see the write.
    other = MockAirflowAdapter(AdapterConfig(base_url="http://other-mock"), simulate_latency=False)
    assert (await other.get_dag_run("partner_api_sync", failed.run_id)).state == "failed"


async def test_mock_data_error_fails_again_after_clear(monkeypatch: pytest.MonkeyPatch) -> None:
    current = int(datetime.now(UTC).timestamp()) // 300
    bucket = current - ((current - 1) % 3)
    now = [datetime.fromtimestamp(bucket * 300 + 30, UTC)]
    monkeypatch.setattr(mock_module, "clock", lambda: now[0])
    adapter = mock_adapter()
    runs = await adapter.list_dag_runs("orders_pipeline", since=now[0] - timedelta(minutes=20))
    failed = next(r for r in runs if r.state == "failed")
    await adapter.clear_task_instances("orders_pipeline", failed.run_id)
    now[0] += timedelta(seconds=61)
    assert (await adapter.get_dag_run("orders_pipeline", failed.run_id)).state == "failed"


async def test_mock_trigger_and_pause(partner_clock: list[datetime]) -> None:
    adapter = mock_adapter()
    run = await adapter.trigger_dag_run("legacy_inventory_sync")
    assert (await adapter.get_dag_run("legacy_inventory_sync", run.run_id)).state == "running"
    partner_clock[0] += timedelta(seconds=61)
    assert (await adapter.get_dag_run("legacy_inventory_sync", run.run_id)).state == "success"

    assert (await adapter.get_dag("orders_pipeline")).is_paused is False
    await adapter.set_dag_paused("orders_pipeline", True)
    assert (await adapter.get_dag("orders_pipeline")).is_paused is True
    dags = {d.dag_id: d for d in await adapter.list_dags()}
    assert dags["orders_pipeline"].is_paused is True

    with pytest.raises(AirflowAdapterError):
        await adapter.clear_task_instances("orders_pipeline", "no-such-run")
