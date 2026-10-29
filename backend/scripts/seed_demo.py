"""Create (or repair) the sandbox demo: connections, monitored DAGs and the four demo workflows.

    python scripts/seed_demo.py [--mock-airflow]

Runs against DATABASE_URL from `.env` and is idempotent: records are matched by name and updated,
so running it again re-applies the demo configuration without creating duplicates.

Settings (environment variables or backend/.env, all optional unless noted):
    DEMO_AIRFLOW_URL           http://localhost:8080
    DEMO_AIRFLOW_USERNAME      admin
    DEMO_AIRFLOW_PASSWORD      required when the Airflow connection does not exist yet
    DEMO_DB_HOST / DEMO_DB_PORT                   localhost / 5433
    DEMO_DB_USERNAME / DEMO_DB_PASSWORD           sandbox_admin / (none)
    DEMO_SOURCE_DB / DEMO_WAREHOUSE_DB            source_db / warehouse_db
    DEMO_SLACK_WEBHOOK_URL     without it the workflows notify in the app only

The Airflow DAGs themselves (sandbox_*) must already be deployed to that Airflow.
"""

import argparse
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))
if __name__ == "__main__":
    os.chdir(BACKEND)  # DATABASE_URL and .env are relative to the backend folder

from dotenv import dotenv_values  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from app.automation.graph import validate_graph  # noqa: E402
from app.core.exceptions import AppError  # noqa: E402
from app.models.airflow import (  # noqa: E402
    AirflowConnection,
    ConnectionKind,
    DeploymentEnvironment,
    MonitoredDag,
)
from app.models.automation import Workflow  # noqa: E402
from app.models.database_connection import DatabaseConnection, DatabaseEngine  # noqa: E402
from app.models.notification_channel import ChannelKind, NotificationChannel  # noqa: E402
from app.models.user import User, UserRole  # noqa: E402
from app.schemas.airflow import AirflowConnCreate, AirflowConnUpdate  # noqa: E402
from app.schemas.channel import ChannelCreate  # noqa: E402
from app.schemas.database import DbConnCreate  # noqa: E402
from app.services import (  # noqa: E402
    airflow_service,
    automation_service,
    database_connection_service,
    notification_channel_service,
)

AIRFLOW_NAME = "Sandbox environment"
SOURCE_DB_NAME = "Local ETL Source DB"
WAREHOUSE_DB_NAME = "Local ETL Warehouse DB"
SLACK_NAME = "Team Slack"
ETL_DAG = "sandbox_ecommerce_etl"

# dag_id -> monitor settings. Other synced DAGs are left as they are.
MONITORED_DAGS: dict[str, dict[str, Any]] = {
    "sandbox_retry_demo": {"detect_failures": True, "sla_minutes": None},
    "sandbox_failure_lab": {"detect_failures": True, "sla_minutes": None},
    ETL_DAG: {"detect_failures": True, "sla_minutes": 60},
    "sandbox_customer_metrics": {"detect_failures": True, "sla_minutes": None},
    "sandbox_daily_revenue": {"detect_failures": True, "sla_minutes": None},
    "sandbox_analytics_publish": {"detect_failures": True, "sla_minutes": None},
}


@dataclass
class DemoConfig:
    airflow_url: str = "http://localhost:8080"
    airflow_username: str = "admin"
    airflow_password: str | None = None
    mock_airflow: bool = False
    db_host: str = "localhost"
    db_port: int = 5433
    db_username: str = "sandbox_admin"
    db_password: str | None = None
    source_db: str = "source_db"
    warehouse_db: str = "warehouse_db"
    slack_webhook_url: str | None = None

    @classmethod
    def from_env(cls, *, mock_airflow: bool = False) -> "DemoConfig":
        """DEMO_* settings from the environment, falling back to backend/.env."""
        values = {**dotenv_values(BACKEND / ".env"), **os.environ}
        env = values.get
        return cls(
            airflow_url=env("DEMO_AIRFLOW_URL", cls.airflow_url),
            airflow_username=env("DEMO_AIRFLOW_USERNAME", cls.airflow_username),
            airflow_password=env("DEMO_AIRFLOW_PASSWORD") or None,
            mock_airflow=mock_airflow,
            db_host=env("DEMO_DB_HOST", cls.db_host),
            db_port=int(env("DEMO_DB_PORT", str(cls.db_port))),
            db_username=env("DEMO_DB_USERNAME", cls.db_username),
            db_password=env("DEMO_DB_PASSWORD") or None,
            source_db=env("DEMO_SOURCE_DB", cls.source_db),
            warehouse_db=env("DEMO_WAREHOUSE_DB", cls.warehouse_db),
            slack_webhook_url=env("DEMO_SLACK_WEBHOOK_URL") or None,
        )


@dataclass
class SeedReport:
    lines: list[str] = field(default_factory=list)

    def add(self, line: str) -> None:
        self.lines.append(line)


# ---------------------------------------------------------------------- connections


def _airflow(db: Session, actor: User, cfg: DemoConfig, report: SeedReport) -> AirflowConnection:
    conn = db.scalar(select(AirflowConnection).where(AirflowConnection.name == AIRFLOW_NAME))
    if conn is None:
        if cfg.mock_airflow:
            data = AirflowConnCreate(
                name=AIRFLOW_NAME,
                kind=ConnectionKind.MOCK,
                base_url="http://mock-airflow",
                auth_type="NONE",
            )
        else:
            if not cfg.airflow_password:
                raise SystemExit("Set DEMO_AIRFLOW_PASSWORD to create the Airflow connection")
            data = AirflowConnCreate(
                name=AIRFLOW_NAME,
                environment=DeploymentEnvironment.DEV,
                base_url=cfg.airflow_url,
                username=cfg.airflow_username,
                secret=cfg.airflow_password,
            )
        conn = airflow_service.create_connection(db, data, actor=actor, ip_address=None)
        report.add(f"Airflow connection '{AIRFLOW_NAME}' created")
    elif cfg.airflow_password and conn.kind == ConnectionKind.LIVE:
        data = AirflowConnUpdate(secret=cfg.airflow_password)
        airflow_service.update_connection(db, conn.id, data, actor=actor, ip_address=None)
        report.add(f"Airflow connection '{AIRFLOW_NAME}' password updated")
    else:
        report.add(f"Airflow connection '{AIRFLOW_NAME}' kept")
    return conn


def _database(
    db: Session, actor: User, cfg: DemoConfig, name: str, database: str, report: SeedReport
) -> DatabaseConnection:
    conn = db.scalar(select(DatabaseConnection).where(DatabaseConnection.name == name))
    if conn is not None:
        report.add(f"Database connection '{name}' kept")
        return conn
    conn = database_connection_service.create_connection(
        db,
        DbConnCreate(
            name=name,
            engine=DatabaseEngine.POSTGRESQL,
            host=cfg.db_host,
            port=cfg.db_port,
            database=database,
            username=cfg.db_username,
            secret=cfg.db_password,
        ),
        actor=actor,
        ip_address=None,
    )
    report.add(f"Database connection '{name}' created")
    return conn


def _slack(
    db: Session, actor: User, cfg: DemoConfig, report: SeedReport
) -> NotificationChannel | None:
    channel = db.scalar(select(NotificationChannel).where(NotificationChannel.name == SLACK_NAME))
    if channel is not None:
        report.add(f"Channel '{SLACK_NAME}' kept")
        return channel
    if not cfg.slack_webhook_url:
        report.add("No DEMO_SLACK_WEBHOOK_URL: workflows will notify in the app only")
        return None
    channel = notification_channel_service.create_channel(
        db,
        ChannelCreate(name=SLACK_NAME, kind=ChannelKind.SLACK, secret=cfg.slack_webhook_url),
        actor=actor,
        ip_address=None,
    )
    report.add(f"Channel '{SLACK_NAME}' created")
    return channel


def _monitor_dags(db: Session, actor: User, conn: AirflowConnection, report: SeedReport) -> None:
    try:
        airflow_service.sync_dags(db, conn.id, actor=actor, ip_address=None)
    except AppError as exc:
        report.add(f"DAG sync skipped ({exc.message}); configuring DAGs already known")
    dags = {
        d.dag_id: d
        for d in db.scalars(select(MonitoredDag).where(MonitoredDag.connection_id == conn.id))
    }
    for dag_id, settings in MONITORED_DAGS.items():
        dag = dags.get(dag_id)
        if dag is None:
            report.add(f"DAG {dag_id} not found in Airflow: not monitored")
            continue
        airflow_service.update_dag(
            db, dag.id, {"is_monitored": True, **settings}, actor=actor, ip_address=None
        )
    report.add(f"Monitoring {sum(1 for d in MONITORED_DAGS if d in dags)} DAG(s)")


# ---------------------------------------------------------------------- workflows


def _n(node_id: str, node_type: str, name: str | None = None, **config: Any) -> dict[str, Any]:
    node: dict[str, Any] = {"id": node_id, "type": node_type, "config": config}
    if name:
        node["name"] = name
    return node


def _e(source: str, port: str, target: str) -> dict[str, str]:
    return {"from": source, "port": port, "to": target}


def retry_network_graph(send_via: str) -> dict[str, Any]:
    """Retryable failures: clear, verify, resolve. Unknown causes and SLA misses: tell the team."""
    return {
        "nodes": [
            _n(
                "trigger",
                "trigger.incident",
                events=["opened"],
                incident_types=["DAG_RUN_FAILED", "SLA_MISSED"],
            ),
            _n("classify", "diagnose.classify_log"),
            _n("retry", "action.clear_failed_tasks", include_downstream=True),
            _n("verify", "verify.run_success", timeout_minutes=10),
            _n("resolve", "incident.update", operation="resolve", note="Fixed by retrying."),
            _n(
                "tell_fixed",
                "notify",
                "Tell the team it is fixed",
                send_via=send_via,
                level="INFO",
                title="Fixed: {{incident.dag_id}}",
                message="{{diagnosis.label}} on {{incident.dag_id}}. Retried and verified.\n"
                "{{links.run}}",
            ),
            _n(
                "tell_failed",
                "notify",
                "Tell the team the retry failed",
                send_via=send_via,
                level="CRITICAL",
                title="Retry failed: {{incident.dag_id}}",
                message="{{diagnosis.label}}: {{last_error}}\n{{links.run}}",
            ),
            _n(
                "tell_unknown",
                "notify",
                "Ask someone to look",
                send_via=send_via,
                level="WARNING",
                title="Needs a look: {{incident.title}}",
                message="No known cause was found, so nothing was retried. Open the incident to "
                "investigate.\n{{links.run}}",
            ),
        ],
        "edges": [
            _e("trigger", "next", "classify"),
            _e("classify", "retryable", "retry"),
            _e("classify", "unknown", "tell_unknown"),
            _e("retry", "success", "verify"),
            _e("retry", "failed", "tell_failed"),
            _e("verify", "success", "resolve"),
            _e("verify", "failed", "tell_failed"),
            _e("resolve", "next", "tell_fixed"),
        ],
    }


def fix_with_approval_graph(send_via: str) -> dict[str, Any]:
    """Failures that need a fix: AI explanation, approval to retry once fixed, verify, resolve."""
    return {
        "nodes": [
            _n("trigger", "trigger.incident", events=["opened"], incident_types=["DAG_RUN_FAILED"]),
            _n("classify", "diagnose.classify_log"),
            _n("logs", "analyze.task_logs", max_log_lines=100, redact_secrets=True),
            _n("ai_fix", "ai.generate_fix", min_confidence_pct=50),
            _n(
                "approval",
                "approval.request",
                "Approve a retry after the fix",
                required_environments=["DEV", "STAGING", "PROD"],
                timeout_minutes=120,
                send_via=send_via,
            ),
            _n("retry", "action.clear_failed_tasks", include_downstream=True),
            _n("verify", "verify.run_success", timeout_minutes=10),
            _n(
                "resolve",
                "incident.update",
                operation="resolve",
                note="Fixed after approval and retry.",
            ),
            _n(
                "escalate",
                "incident.update",
                operation="escalate",
                note="Not retried: {{last_error}}",
            ),
            _n(
                "tell_result",
                "notify",
                "Tell the team the result",
                send_via=send_via,
                level="INFO",
                title="{{incident.dag_id}}: {{diagnosis.label}}",
                message="AI: {{ai_solution.explanation}}\n"
                "Next: {{ai_solution.recommended_action}}\n{{links.run}}",
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
            _e("retry", "failed", "tell_result"),
            _e("verify", "success", "resolve"),
            _e("verify", "failed", "tell_result"),
            _e("resolve", "next", "tell_result"),
            _e("escalate", "next", "tell_result"),
        ],
    }


def _with_send_via(graph: dict[str, Any], send_via: str) -> dict[str, Any]:
    for node in graph["nodes"]:
        if node["type"] == "notify" and not node["config"].get("send_via"):
            node["config"]["send_via"] = send_via
    return graph


def _save_workflow(
    db: Session,
    actor: User,
    report: SeedReport,
    *,
    name: str,
    description: str,
    graph: dict[str, Any],
    template_key: str | None = None,
    template_parameters: dict[str, Any] | None = None,
) -> Workflow:
    validate_graph(graph)  # fail before touching the database
    workflow = db.scalar(select(Workflow).where(Workflow.name == name))
    if workflow is None:
        workflow = automation_service.create_workflow(
            db, actor=actor, name=name, description=description, graph=graph
        )
        verb = "created"
    else:
        verb = "updated"
    automation_service.update_workflow(
        db,
        workflow.id,
        {"description": description, "graph": graph, "enabled": True},
        actor=actor,
    )
    if template_key:
        workflow.template_key = template_key
        workflow.template_parameters = template_parameters
        db.commit()
    report.add(f"Workflow '{name}' {verb} (v{workflow.version}, enabled)")
    return workflow


def _workflows(
    db: Session,
    actor: User,
    report: SeedReport,
    *,
    source: DatabaseConnection,
    warehouse: DatabaseConnection,
    slack: NotificationChannel | None,
) -> None:
    send_via = str(slack.id) if slack else ""
    _save_workflow(
        db,
        actor,
        report,
        name="Retry network failures",
        description="Retryable failures are retried, verified and resolved; unknown causes are "
        "reported for a person to look at.",
        graph=retry_network_graph(send_via),
    )
    _save_workflow(
        db,
        actor,
        report,
        name="Fix with approval",
        description="Failures that need a fix get an AI explanation; once someone fixes the cause "
        "and approves, the tasks are retried, verified and the incident resolved.",
        graph=fix_with_approval_graph(send_via),
    )

    validate_params = {
        "dag_id": ETL_DAG,
        "database_connection_id": str(warehouse.id),
        "sql": "SELECT count(*) FROM warehouse.fact_orders",
        "operator": ">",
        "expected": "0",
        "send_via": send_via,
    }
    _, graph, params = automation_service.build_template("validate-every-run", validate_params)
    _save_workflow(
        db,
        actor,
        report,
        name="Validate every e-commerce run",
        description="Each successful e-commerce ETL run must leave orders in the warehouse.",
        graph=graph,
        template_key="validate-every-run",
        template_parameters=params,
    )

    # "rows" compares the values the queries return; "row_count" would only compare how many
    # rows came back (always 1 for a count(*)), so a mismatch could never be found.
    reconcile_params = {
        "source_connection_id": str(source.id),
        "source_sql": "SELECT count(*) AS order_count FROM source.orders",
        "destination_connection_id": str(warehouse.id),
        "destination_sql": "SELECT count(*) AS order_count FROM warehouse.fact_orders",
        "comparison_mode": "rows",
    }
    _, graph, params = automation_service.build_template("reconcile-databases", reconcile_params)
    _save_workflow(
        db,
        actor,
        report,
        name="Compare source and warehouse orders",
        description="Run on demand: the warehouse must hold as many orders as the source.",
        graph=_with_send_via(graph, send_via),
        template_key="reconcile-databases",
        template_parameters=params,
    )


# ---------------------------------------------------------------------- entry points


def seed(db: Session, cfg: DemoConfig) -> SeedReport:
    report = SeedReport()
    actor = db.scalar(
        select(User).where(User.role == UserRole.ADMIN, User.is_active.is_(True)).limit(1)
    )
    if actor is None:
        raise SystemExit("No active admin user: start the backend once to create it")
    automation_service.seed_templates(db, actor)
    airflow = _airflow(db, actor, cfg, report)
    _monitor_dags(db, actor, airflow, report)
    source = _database(db, actor, cfg, SOURCE_DB_NAME, cfg.source_db, report)
    warehouse = _database(db, actor, cfg, WAREHOUSE_DB_NAME, cfg.warehouse_db, report)
    slack = _slack(db, actor, cfg, report)
    _workflows(db, actor, report, source=source, warehouse=warehouse, slack=slack)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--mock-airflow", action="store_true", help="use the built-in mock Airflow"
    )
    args = parser.parse_args()

    from app.db.session import SessionLocal

    with SessionLocal() as db:
        report = seed(db, DemoConfig.from_env(mock_airflow=args.mock_airflow))
    print("\n".join(report.lines))


if __name__ == "__main__":
    main()
