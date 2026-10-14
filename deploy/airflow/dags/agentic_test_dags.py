"""Deterministic DAGs for the live end-to-end test (backend/tests/live/test_live_airflow.py).

- agentic_flaky_once: the first attempt of every run fails with a transient network error;
  clearing the task (a retry) succeeds. The `retry-transient` workflow should fix it.
- agentic_bad_data: always fails with a unique-constraint violation. Automation must NOT retry it.
- agentic_sla_test: manual-only run that sleeps for a configurable duration to exercise freshness
    SLA monitoring without touching production data.

Works on Airflow 2.10 and 3.x. All DAGs are manual-only (schedule=None).
"""

import os
import time
from datetime import datetime
from urllib.parse import urlparse

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task

DEFAULTS = {"retries": 0, "owner": "agentic-tests"}


@dag(
    dag_id="agentic_flaky_once",
    schedule=None,
    start_date=datetime(2026, 10, 13),
    catchup=False,
    default_args=DEFAULTS,
    tags=["agentic-test"],
    description="Fails on the first attempt of each run (network error); a retry fixes it.",
)
def agentic_flaky_once():
    @task
    def fetch_partner_orders(**context):
        if context["ti"].try_number <= 1:
            raise ConnectionError(
                "503 Service Unavailable: partner API did not respond "
                "(simulated transient failure on the first attempt)"
            )
        return "fetched"

    fetch_partner_orders()


@dag(
    dag_id="agentic_bad_data",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULTS,
    tags=["agentic-test"],
    description="Always fails with a duplicate-key error; retrying would not help.",
)
def agentic_bad_data():
    @task
    def load_orders():
        raise RuntimeError(
            'psycopg.errors.UniqueViolation: duplicate key value violates unique constraint '
            '"fact_orders_pkey" DETAIL: Key (order_id)=(A-10492) already exists.'
        )

    load_orders()


agentic_flaky_once()
agentic_bad_data()


@dag(
    dag_id="agentic_sla_test",
    schedule=None,
    start_date=datetime(2026, 10, 13),
    catchup=False,
    default_args=DEFAULTS,
    tags=["agentic-test", "sla-test"],
    description="Manual-only approximately two-minute run for Agentic SLA monitoring tests.",
)
def agentic_sla_test():
    @task
    def hold_for_test_duration():
        duration_seconds = int(os.getenv("AGENTIC_SLA_TEST_DURATION_SECONDS", "360"))
        time.sleep(max(1, duration_seconds))

    hold_for_test_duration()


agentic_sla_test()


RECONCILIATION_DATABASE = "agentic_reconciliation_test"
SOURCE_TABLE = "agentic_reconciliation_source"
DESTINATION_TABLE = "agentic_reconciliation_destination"


def _reconciliation_config() -> tuple[str, str]:
    enabled = os.getenv("AGENTIC_RECONCILIATION_TEST_DATA_ENABLED", "false").lower()
    if enabled != "true":
        raise RuntimeError(
            "Set AGENTIC_RECONCILIATION_TEST_DATA_ENABLED=true to enable isolated test writes"
        )
    database_url = os.getenv("AGENTIC_RECONCILIATION_TEST_DATABASE_URL", "")
    parsed = urlparse(database_url)
    if not database_url or parsed.path.removeprefix("/") != RECONCILIATION_DATABASE:
        raise RuntimeError(
            f"AGENTIC_RECONCILIATION_TEST_DATABASE_URL must target {RECONCILIATION_DATABASE}"
        )
    scenario = os.getenv("AGENTIC_RECONCILIATION_SCENARIO", "match").lower()
    if scenario not in {"match", "mismatch"}:
        raise RuntimeError("AGENTIC_RECONCILIATION_SCENARIO must be match or mismatch")
    return database_url, scenario


@dag(
    dag_id="agentic_reconciliation_pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULTS,
    tags=["agentic-test", "reconciliation-test"],
    description="Manual-only isolated fixture loader for database reconciliation workflow tests.",
)
def agentic_reconciliation_pipeline():
    @task
    def validate_test_configuration():
        _, scenario = _reconciliation_config()
        return {"scenario": scenario, "database": RECONCILIATION_DATABASE}

    @task
    def prepare_test_data(config):
        database_url, scenario = _reconciliation_config()
        try:
            import psycopg2
        except ImportError as exc:
            raise RuntimeError("The Airflow image must provide psycopg2 for this test DAG") from exc

        source_rows = [(1, "alpha", 10), (2, "beta", 20)]
        destination_rows = source_rows if scenario == "match" else [(1, "alpha", 10), (2, "beta", 99)]
        with psycopg2.connect(database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"CREATE TABLE IF NOT EXISTS {SOURCE_TABLE} "
                    "(id INTEGER PRIMARY KEY, product TEXT NOT NULL, quantity INTEGER NOT NULL)"
                )
                cursor.execute(
                    f"CREATE TABLE IF NOT EXISTS {DESTINATION_TABLE} "
                    "(id INTEGER PRIMARY KEY, product TEXT NOT NULL, quantity INTEGER NOT NULL)"
                )
                cursor.execute(f"DELETE FROM {SOURCE_TABLE}")
                cursor.execute(f"DELETE FROM {DESTINATION_TABLE}")
                cursor.executemany(
                    f"INSERT INTO {SOURCE_TABLE} (id, product, quantity) VALUES (%s, %s, %s)",
                    source_rows,
                )
                cursor.executemany(
                    f"INSERT INTO {DESTINATION_TABLE} (id, product, quantity) VALUES (%s, %s, %s)",
                    destination_rows,
                )
        return config

    @task
    def validate_fixture(config):
        database_url, scenario = _reconciliation_config()
        import psycopg2

        with psycopg2.connect(database_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {SOURCE_TABLE}")
                source_count = cursor.fetchone()[0]
                cursor.execute(f"SELECT COUNT(*) FROM {DESTINATION_TABLE}")
                destination_count = cursor.fetchone()[0]
        if source_count != destination_count:
            raise RuntimeError("Reconciliation fixture counts were not prepared")
        if scenario == "mismatch" and source_count < 2:
            raise RuntimeError("Mismatch fixture is incomplete")
        return {"scenario": config["scenario"], "source_count": source_count}

    validate_fixture(prepare_test_data(validate_test_configuration()))


agentic_reconciliation_pipeline()
