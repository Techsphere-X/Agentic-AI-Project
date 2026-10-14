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
