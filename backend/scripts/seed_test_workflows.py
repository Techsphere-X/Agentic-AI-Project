"""Create (or update) the workflow test set: SLA, data quality and retry workflows.

    python scripts/seed_test_workflows.py

Needs the sandbox demo first (`scripts/seed_demo.py`): it uses the "Sandbox environment" Airflow
connection, the two "Local ETL" databases and, if present, the "Team Slack" channel. Idempotent:
workflows are matched by name. It also turns the four earlier demo workflows off (so nothing runs
twice) and sets the e-commerce ETL's freshness SLA to 1500 minutes (it runs daily).

Workflows (all enabled):
    SLA 1 · Alert when a run takes longer than usual
    SLA 2 · Alert while a run is still going past its usual time
    SLA 3 · Re-run the ETL when its data is late
    Data quality 1 · Check the warehouse after every ETL run
    Data quality 2 · Check the warehouse now (manual)
    Fix 1 · Auto-retry temporary failures
    Fix 2 · Ask before retrying failures that need a code or data fix
    Fix 3 · Flag failures with an unknown cause
"""

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))
import seed_demo  # noqa: E402  (also puts the backend on sys.path)

if __name__ == "__main__":
    os.chdir(seed_demo.BACKEND)

from sqlalchemy import select  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from app.models.airflow import AirflowConnection, MonitoredDag  # noqa: E402
from app.models.automation import Workflow  # noqa: E402
from app.models.database_connection import DatabaseConnection  # noqa: E402
from app.models.notification_channel import NotificationChannel  # noqa: E402
from app.models.user import User, UserRole  # noqa: E402
from app.services import airflow_service, automation_service  # noqa: E402

_n, _e = seed_demo._n, seed_demo._e

ETL_DAG = seed_demo.ETL_DAG
LAB_DAG = "sandbox_failure_lab"
ETL_SLA_MINUTES = 1500  # the ETL runs daily at 06:00; alert if a day passes without fresh data
OLD_DEMO_WORKFLOWS = (
    "Retry network failures",
    "Fix with approval",
    "Validate every e-commerce run",
    "Compare source and warehouse orders",
)

SLA_1 = "SLA 1 · Alert when a run takes longer than usual"
SLA_2 = "SLA 2 · Alert while a run is still going past its usual time"
SLA_3 = "SLA 3 · Re-run the ETL when its data is late"
DQ_1 = "Data quality 1 · Check the warehouse after every ETL run"
DQ_2 = "Data quality 2 · Check the warehouse now (manual)"
FIX_1 = "Fix 1 · Auto-retry temporary failures"
FIX_2 = "Fix 2 · Ask before retrying failures that need a code or data fix"
FIX_3 = "Fix 3 · Flag failures with an unknown cause"
ALL_WORKFLOWS = (SLA_1, SLA_2, SLA_3, DQ_1, DQ_2, FIX_1, FIX_2, FIX_3)

_SLA_DETAILS = (
    "• DAG: {{sla.dag_id}}\n"
    "• Run: {{sla.run_id}}\n"
    "• Took: {{sla.actual_minutes}} min\n"
    "• Usual (median): {{sla.estimated_baseline_minutes}} min, "
    "from {{sla.sample_count}} past runs\n"
    "• Limit (+{{sla.margin_pct}}%): {{sla.threshold_minutes}} min\n"
    "{{links.run}}"
)


@dataclass(frozen=True)
class Targets:
    warehouse: str  # database connection ids
    source: str
    send_via: str  # Team Slack channel id, or "" for in-app only


# ---------------------------------------------------------------------- SLA


def sla_after_run_graph(t: Targets) -> dict[str, Any]:
    return {
        "nodes": [
            _n("trigger", "trigger.dag_run", states=["success"], dag_ids=[ETL_DAG, LAB_DAG]),
            _n(
                "sla",
                "check.sla",
                "Slower than usual?",
                metric="duration",
                calculation_method="median",
                sample_size=10,
                margin_pct=100,
            ),
            _n(
                "alert",
                "notify",
                "Alert: run was slow",
                send_via=t.send_via,
                level="WARNING",
                title="SLA missed: {{sla.dag_id}} took {{sla.actual_minutes}} min",
                message="The run finished, but took longer than usual.\n" + _SLA_DETAILS,
            ),
        ],
        "edges": [_e("trigger", "next", "sla"), _e("sla", "exceeded", "alert")],
    }


def sla_in_flight_graph(t: Targets) -> dict[str, Any]:
    return {
        "nodes": [
            _n("trigger", "trigger.schedule", every_minutes=5),
            _n(
                "sla",
                "check.sla",
                "Running longer than usual?",
                dag_id=LAB_DAG,
                metric="duration",
                calculation_method="median",
                sample_size=10,
                margin_pct=100,
                only_in_flight=True,
            ),
            _n(
                "alert",
                "notify",
                "Alert: run is late",
                send_via=t.send_via,
                level="WARNING",
                title="SLA at risk: {{sla.dag_id}} still running after {{sla.actual_minutes}} min",
                message="The run has not finished yet and is already over its usual time.\n"
                + _SLA_DETAILS,
            ),
        ],
        "edges": [_e("trigger", "next", "sla"), _e("sla", "exceeded", "alert")],
    }


def sla_rerun_graph(t: Targets) -> dict[str, Any]:
    return {
        "nodes": [
            _n("trigger", "trigger.incident", events=["opened"], incident_types=["SLA_MISSED"]),
            _n("is_etl", "condition.filter", "Is it the e-commerce ETL?", dag_ids=[ETL_DAG]),
            _n("rerun", "action.trigger_dag_run", "Re-run the ETL"),
            _n("verify", "verify.run_success", "Wait for the run to succeed", timeout_minutes=15),
            _n(
                "resolve",
                "incident.update",
                operation="resolve",
                note="The ETL was re-run and the data is fresh again.",
            ),
            _n(
                "tell_fixed",
                "notify",
                "Tell the team it is fresh",
                send_via=t.send_via,
                level="INFO",
                title="Data is fresh again: {{incident.dag_id}}",
                message="The data was late, so the ETL was re-run. The new run succeeded.\n"
                "{{links.run}}",
            ),
            _n(
                "escalate",
                "incident.update",
                operation="escalate",
                note="Re-running the ETL did not fix the late data: {{last_error}}",
            ),
            _n(
                "tell_failed",
                "notify",
                "Tell the team the re-run failed",
                send_via=t.send_via,
                level="CRITICAL",
                title="Data still late: {{incident.dag_id}}",
                message="The ETL was re-run but did not succeed: {{last_error}}\n{{links.run}}",
            ),
        ],
        "edges": [
            _e("trigger", "next", "is_etl"),
            _e("is_etl", "true", "rerun"),
            _e("rerun", "success", "verify"),
            _e("rerun", "failed", "escalate"),
            _e("verify", "success", "resolve"),
            _e("verify", "failed", "escalate"),
            _e("resolve", "next", "tell_fixed"),
            _e("escalate", "next", "tell_failed"),
        ],
    }


# ---------------------------------------------------------------------- data quality


@dataclass(frozen=True)
class Check:
    id: str
    name: str
    sql: str
    operator: str = "="
    expected: str = "0"


@dataclass(frozen=True)
class Reconcile:
    id: str
    name: str
    source_sql: str
    destination_sql: str


FRESHNESS_SQL = (
    "SELECT round(extract(epoch FROM now() - max(loaded_at)) / 60) FROM warehouse.fact_orders"
)


def quality_checks(fresh_within_minutes: int) -> list[Check | Reconcile]:
    """The 20 checks, in order. Each passes on correct warehouse data."""
    return [
        # Volume
        Check(
            "orders_loaded",
            "Orders were loaded",
            "SELECT count(*) FROM warehouse.fact_orders",
            ">",
        ),
        Check(
            "customers_loaded",
            "Customers were loaded",
            "SELECT count(*) FROM warehouse.dim_customers",
            ">",
        ),
        # Completeness
        Reconcile(
            "all_source_orders",
            "Warehouse has every source order",
            "SELECT count(*) AS order_count FROM source.orders",
            "SELECT count(*) AS order_count FROM warehouse.fact_orders",
        ),
        Reconcile(
            "all_source_customers",
            "Warehouse has every source customer",
            "SELECT count(*) AS customer_count FROM source.customers",
            "SELECT count(*) AS customer_count FROM warehouse.dim_customers",
        ),
        Check(
            "etl_log_matches",
            "ETL log matches what was loaded",
            "SELECT count(*) FROM ops.etl_runs "
            "WHERE recorded_at = (SELECT max(recorded_at) FROM ops.etl_runs) "
            "AND status = 'success' AND published_orders = source_orders "
            "AND published_customers = source_customers",
            "=",
            "1",
        ),
        # Uniqueness
        Check(
            "no_duplicate_orders",
            "No duplicate order IDs",
            "SELECT count(*) - count(DISTINCT order_id) FROM warehouse.fact_orders",
        ),
        Check(
            "no_duplicate_customers",
            "No duplicate customer IDs",
            "SELECT count(*) - count(DISTINCT customer_id) FROM warehouse.dim_customers",
        ),
        # Missing values
        Check(
            "no_blank_order_fields",
            "No blank order fields",
            "SELECT count(*) FROM warehouse.fact_orders "
            "WHERE order_id IS NULL OR trim(order_id) = '' OR customer_id IS NULL "
            "OR trim(customer_id) = '' OR status IS NULL OR trim(status) = '' "
            "OR order_timestamp IS NULL",
        ),
        Check(
            "no_blank_customer_fields",
            "No blank customer fields",
            "SELECT count(*) FROM warehouse.dim_customers "
            "WHERE email IS NULL OR trim(email) = '' OR country IS NULL OR trim(country) = ''",
        ),
        # Validity
        Check(
            "valid_quantity_price",
            "Quantity and price are valid",
            "SELECT count(*) FROM warehouse.fact_orders WHERE quantity <= 0 OR unit_price < 0",
        ),
        Check(
            "totals_add_up",
            "Order totals add up",
            "SELECT count(*) FROM warehouse.fact_orders "
            "WHERE abs(order_total - quantity * unit_price) > 0.01",
        ),
        Check(
            "known_status",
            "Order status is known",
            "SELECT count(*) FROM warehouse.fact_orders "
            "WHERE status NOT IN ('placed', 'paid', 'cancelled')",
        ),
        Check(
            "valid_emails",
            "Customer emails look valid",
            "SELECT count(*) FROM warehouse.dim_customers "
            "WHERE email !~ '^[^@[:space:]]+@[^@[:space:]]+[.][^@[:space:]]+$'",
        ),
        Check(
            "no_future_orders",
            "No orders dated in the future",
            "SELECT count(*) FROM warehouse.fact_orders "
            "WHERE order_timestamp > now() + interval '5 minutes'",
        ),
        # Referential integrity
        Check(
            "orders_have_customers",
            "Every order has a known customer",
            "SELECT count(*) FROM warehouse.fact_orders f "
            "LEFT JOIN warehouse.dim_customers c ON c.customer_id = f.customer_id "
            "WHERE c.customer_id IS NULL",
        ),
        # Freshness
        Check(
            "fresh_data",
            "Data was loaded by this run"
            if fresh_within_minutes <= 60
            else "Data was loaded in the last 25 hours",
            FRESHNESS_SQL,
            "<=",
            str(fresh_within_minutes),
        ),
        # Schema drift
        Check(
            "order_columns",
            "Order table columns unchanged",
            "SELECT string_agg(column_name, ',' ORDER BY column_name) "
            "FROM information_schema.columns "
            "WHERE table_schema = 'warehouse' AND table_name = 'fact_orders'",
            "=",
            "customer_id,loaded_at,order_id,order_timestamp,order_total,quantity,status,unit_price",
        ),
        Check(
            "customer_columns",
            "Customer table columns unchanged",
            "SELECT string_agg(column_name, ',' ORDER BY column_name) "
            "FROM information_schema.columns "
            "WHERE table_schema = 'warehouse' AND table_name = 'dim_customers'",
            "=",
            "country,created_at,customer_id,email,loaded_at",
        ),
        # Distribution
        Check(
            "revenue_above_zero",
            "Revenue is above zero",
            "SELECT coalesce(sum(order_total), 0) FROM warehouse.fact_orders "
            "WHERE status <> 'cancelled'",
            ">",
        ),
        Check(
            "cancelled_below_half",
            "Cancelled orders below 50%",
            "SELECT round(100.0 * count(*) FILTER (WHERE status = 'cancelled') "
            "/ nullif(count(*), 0), 1) FROM warehouse.fact_orders",
            "<",
            "50",
        ),
    ]


def _quality_chain(
    t: Targets, checks: list[Check | Reconcile], on_pass: str, on_fail: str
) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
    """Checks one after another; the first failure goes to `on_fail`, all passing to `on_pass`."""
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, str]] = [_e("trigger", "next", checks[0].id)]
    for i, check in enumerate(checks):
        nxt = checks[i + 1].id if i + 1 < len(checks) else on_pass
        if isinstance(check, Reconcile):
            nodes.append(
                _n(
                    check.id,
                    "database.reconcile",
                    check.name,
                    source_connection_id=t.source,
                    source_sql=check.source_sql,
                    destination_connection_id=t.warehouse,
                    destination_sql=check.destination_sql,
                    comparison_mode="rows",
                )
            )
            edges += [
                _e(check.id, "matched", nxt),
                _e(check.id, "mismatched", on_fail),
                _e(check.id, "failed", on_fail),
            ]
        else:
            nodes.append(
                _n(
                    check.id,
                    "database.check",
                    check.name,
                    connection_id=t.warehouse,
                    sql=check.sql,
                    operator=check.operator,
                    expected=check.expected,
                )
            )
            edges += [_e(check.id, "pass", nxt), _e(check.id, "fail", on_fail)]
    return nodes, edges


def quality_after_run_graph(t: Targets) -> dict[str, Any]:
    checks = quality_checks(fresh_within_minutes=60)
    nodes, edges = _quality_chain(t, checks, "record_passed", "record_failed")
    return {
        "nodes": [
            _n("trigger", "trigger.dag_run", states=["success"], dag_ids=[ETL_DAG]),
            *nodes,
            _n(
                "record_passed",
                "check.record",
                "Mark the run as passed",
                result="passed",
                note=f"All {len(checks)} data quality checks passed",
            ),
            _n(
                "record_failed",
                "check.record",
                "Mark the run as failed",
                result="failed",
                note="{{last_error}}",
                open_incident=True,
            ),
            _n(
                "alert",
                "notify",
                "Alert: data quality failed",
                send_via=t.send_via,
                level="WARNING",
                title="Data quality failed: {{dag_run.dag_id}}",
                message="{{last_error}}\nRun {{dag_run.run_id}} succeeded in Airflow but its data "
                "did not pass. A 'Data check failed' incident is open.\n{{links.run}}",
            ),
        ],
        "edges": [*edges, _e("record_failed", "next", "alert")],
    }


def quality_manual_graph(t: Targets) -> dict[str, Any]:
    checks = quality_checks(fresh_within_minutes=ETL_SLA_MINUTES)
    nodes, edges = _quality_chain(t, checks, "all_passed", "alert")
    return {
        "nodes": [
            _n("trigger", "trigger.manual"),
            *nodes,
            _n(
                "all_passed",
                "notify",
                "Report: all checks passed",
                send_via=t.send_via,
                level="INFO",
                title=f"Warehouse data quality: all {len(checks)} checks passed",
                message="Volume, completeness, uniqueness, missing values, validity, referential "
                "integrity, freshness, schema and distribution checks all passed.\n{{links.run}}",
            ),
            _n(
                "alert",
                "notify",
                "Alert: a check failed",
                send_via=t.send_via,
                level="WARNING",
                title="Warehouse data quality check failed",
                message="{{last_error}}\n{{links.run}}",
            ),
        ],
        "edges": edges,
    }


# ---------------------------------------------------------------------- fixes

_FAILED_RUNS = {"events": ["opened", "recurred"], "incident_types": ["DAG_RUN_FAILED"]}


def fix_retry_graph(t: Targets) -> dict[str, Any]:
    return {
        "nodes": [
            _n("trigger", "trigger.incident", **_FAILED_RUNS),
            _n("classify", "diagnose.classify_log", "What caused the failure?"),
            _n(
                "not_flapping",
                "condition.filter",
                "Failed 3 times or fewer?",
                max_occurrences=3,
            ),
            _n(
                "retry",
                "action.clear_failed_tasks",
                "Retry the failed tasks",
                include_downstream=True,
            ),
            _n("verify", "verify.run_success", "Wait for the retry to succeed", timeout_minutes=15),
            _n("resolve", "incident.update", operation="resolve", note="Fixed by retrying."),
            _n(
                "tell_fixed",
                "notify",
                "Tell the team it is fixed",
                send_via=t.send_via,
                level="INFO",
                title="Fixed by retrying: {{incident.dag_id}}",
                message="{{diagnosis.label}}. The failed tasks were retried and the run "
                "succeeded.\n{{links.run}}",
            ),
            _n(
                "escalate_failed",
                "incident.update",
                operation="escalate",
                note="The retry did not fix it: {{last_error}}",
            ),
            _n(
                "tell_failed",
                "notify",
                "Tell the team the retry failed",
                send_via=t.send_via,
                level="CRITICAL",
                title="Retry failed: {{incident.dag_id}}",
                message="{{diagnosis.label}}. Retrying did not fix it: {{last_error}}\n"
                "{{links.run}}",
            ),
            _n(
                "escalate_flapping",
                "incident.update",
                operation="escalate",
                note="Failed {{incident.occurrence_count}} times; not retried again.",
            ),
            _n(
                "tell_flapping",
                "notify",
                "Tell the team it keeps failing",
                send_via=t.send_via,
                level="CRITICAL",
                title="Keeps failing, not retried: {{incident.dag_id}}",
                message="{{diagnosis.label}}. It failed {{incident.occurrence_count}} times, so it "
                "was not retried again. Someone needs to look.\n{{links.run}}",
            ),
        ],
        "edges": [
            _e("trigger", "next", "classify"),
            _e("classify", "retryable", "not_flapping"),
            _e("not_flapping", "true", "retry"),
            _e("not_flapping", "false", "escalate_flapping"),
            _e("retry", "success", "verify"),
            _e("retry", "failed", "escalate_failed"),
            _e("verify", "success", "resolve"),
            _e("verify", "failed", "escalate_failed"),
            _e("resolve", "next", "tell_fixed"),
            _e("escalate_failed", "next", "tell_failed"),
            _e("escalate_flapping", "next", "tell_flapping"),
        ],
    }


def fix_with_approval_graph(t: Targets) -> dict[str, Any]:
    return {
        "nodes": [
            _n("trigger", "trigger.incident", **_FAILED_RUNS),
            _n("classify", "diagnose.classify_log", "What caused the failure?"),
            _n("logs", "analyze.task_logs", "Read the error from the logs"),
            _n("ai_fix", "ai.generate_fix", "AI suggests a fix", min_confidence_pct=50),
            _n(
                "approval",
                "approval.request",
                "Approve a retry once the cause is fixed",
                required_environments=["DEV", "STAGING", "PROD"],
                timeout_minutes=120,
                send_via=t.send_via,
            ),
            _n(
                "retry",
                "action.clear_failed_tasks",
                "Retry the failed tasks",
                include_downstream=True,
            ),
            _n("verify", "verify.run_success", "Wait for the retry to succeed", timeout_minutes=15),
            _n(
                "resolve",
                "incident.update",
                operation="resolve",
                note="Fixed after approval and retry.",
            ),
            _n(
                "tell_fixed",
                "notify",
                "Tell the team it is fixed",
                send_via=t.send_via,
                level="INFO",
                title="Fixed after approval: {{incident.dag_id}}",
                message="{{diagnosis.label}}. The fix was approved, the tasks were retried and the "
                "run succeeded.\n{{links.run}}",
            ),
            _n(
                "escalate",
                "incident.update",
                operation="escalate",
                note="Not fixed: {{last_error}}",
            ),
            _n(
                "tell_not_fixed",
                "notify",
                "Tell the team it is not fixed",
                send_via=t.send_via,
                level="CRITICAL",
                title="Needs a fix: {{incident.dag_id}}",
                message="{{diagnosis.label}}: {{last_error}}\n"
                "AI: {{ai_solution.explanation}}\n"
                "Suggested fix: {{ai_solution.recommended_action}}\n{{links.run}}",
            ),
        ],
        "edges": [
            _e("trigger", "next", "classify"),
            _e("classify", "needs_fix", "logs"),
            _e("logs", "analyzed", "ai_fix"),
            _e("logs", "no_logs", "ai_fix"),
            _e("ai_fix", "solution_ready", "approval"),
            _e("ai_fix", "uncertain", "approval"),
            _e("approval", "approved", "retry"),
            _e("approval", "rejected", "escalate"),
            _e("retry", "success", "verify"),
            _e("retry", "failed", "escalate"),
            _e("verify", "success", "resolve"),
            _e("verify", "failed", "escalate"),
            _e("resolve", "next", "tell_fixed"),
            _e("escalate", "next", "tell_not_fixed"),
        ],
    }


def fix_unknown_graph(t: Targets) -> dict[str, Any]:
    return {
        "nodes": [
            _n("trigger", "trigger.incident", **_FAILED_RUNS),
            _n("classify", "diagnose.classify_log", "What caused the failure?"),
            _n("logs", "analyze.task_logs", "Read the error from the logs"),
            _n("ai_fix", "ai.generate_fix", "AI explains the error", min_confidence_pct=50),
            _n(
                "tell_unknown",
                "notify",
                "Ask someone to look",
                send_via=t.send_via,
                level="WARNING",
                title="Needs a look: {{incident.dag_id}}",
                message="The cause is not a known type, so nothing was retried.\n"
                "Error: {{analysis.headline}}\n"
                "AI: {{ai_solution.explanation}}\n"
                "Suggested fix: {{ai_solution.recommended_action}}\n{{links.run}}",
            ),
        ],
        "edges": [
            _e("trigger", "next", "classify"),
            _e("classify", "unknown", "logs"),
            _e("logs", "analyzed", "ai_fix"),
            _e("logs", "no_logs", "ai_fix"),
            _e("ai_fix", "solution_ready", "tell_unknown"),
            _e("ai_fix", "uncertain", "tell_unknown"),
        ],
    }


# ---------------------------------------------------------------------- entry points

WORKFLOWS = (
    (SLA_1, "After each ETL or failure-lab run: alert if it took over twice as long as usual.",
     sla_after_run_graph),
    (SLA_2, "Every 5 minutes: alert if a failure-lab run is still going after twice its usual "
     "time.", sla_in_flight_graph),
    (SLA_3, "When the ETL's data is late (freshness SLA missed): re-run it and check it worked.",
     sla_rerun_graph),
    (DQ_1, "After each successful ETL run: 20 data quality checks on the warehouse; a failure "
     "marks the run failed, opens an incident and alerts.", quality_after_run_graph),
    (DQ_2, "Press Run now: the same 20 checks, reported to the team. Changes nothing.",
     quality_manual_graph),
    (FIX_1, "Network, timeout, memory or missing-input failures: retry, verify and resolve. "
     "Stops retrying after 3 failures.", fix_retry_graph),
    (FIX_2, "Bad data, schema or login failures: AI suggests a fix and a person approves the "
     "retry.", fix_with_approval_graph),
    (FIX_3, "Failures with no known cause: AI explains the error and the team is asked to look.",
     fix_unknown_graph),
)


def _named(db: Session, model: type, name: str) -> Any:
    return db.scalar(select(model).where(model.name == name))


def seed(db: Session) -> seed_demo.SeedReport:
    report = seed_demo.SeedReport()
    actor = db.scalar(
        select(User).where(User.role == UserRole.ADMIN, User.is_active.is_(True)).limit(1)
    )
    if actor is None:
        raise SystemExit("No active admin user: start the backend once to create it")
    warehouse = _named(db, DatabaseConnection, seed_demo.WAREHOUSE_DB_NAME)
    source = _named(db, DatabaseConnection, seed_demo.SOURCE_DB_NAME)
    airflow = _named(db, AirflowConnection, seed_demo.AIRFLOW_NAME)
    if warehouse is None or source is None or airflow is None:
        raise SystemExit("Run scripts/seed_demo.py first (connections are missing)")
    slack = _named(db, NotificationChannel, seed_demo.SLACK_NAME)
    targets = Targets(str(warehouse.id), str(source.id), str(slack.id) if slack else "")

    for name in OLD_DEMO_WORKFLOWS:
        old = _named(db, Workflow, name)
        if old is not None and old.enabled:
            automation_service.update_workflow(db, old.id, {"enabled": False}, actor=actor)
            report.add(f"Workflow '{name}' turned off")

    etl = db.scalar(
        select(MonitoredDag).where(
            MonitoredDag.connection_id == airflow.id, MonitoredDag.dag_id == ETL_DAG
        )
    )
    if etl is not None:
        airflow_service.update_dag(
            db,
            etl.id,
            {"is_monitored": True, "sla_minutes": ETL_SLA_MINUTES},
            actor=actor,
            ip_address=None,
        )
        report.add(f"{ETL_DAG}: freshness SLA {ETL_SLA_MINUTES} min")
    else:
        report.add(f"{ETL_DAG} not synced yet: SLA 3 has no freshness monitor to react to")

    for name, description, build in WORKFLOWS:
        seed_demo._save_workflow(
            db, actor, report, name=name, description=description, graph=build(targets)
        )
    return report


def main() -> None:
    from app.db.session import SessionLocal

    with SessionLocal() as db:
        report = seed(db)
    print("\n".join(report.lines))


if __name__ == "__main__":
    main()
