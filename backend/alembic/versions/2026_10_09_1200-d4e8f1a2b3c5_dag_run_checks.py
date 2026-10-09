"""per-run validation status (dag_run_checks) and the DATA_CHECK_FAILED incident type

Also merges the two heads (incident diagnosis, workflow template metadata).

Revision ID: d4e8f1a2b3c5
Revises: 2a4b6c8d0e1f, c7e2a91f5b30
Create Date: 2026-10-09 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d4e8f1a2b3c5"
down_revision: str | Sequence[str] | None = ("2a4b6c8d0e1f", "c7e2a91f5b30")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

OLD_TYPES = "type IN ('DAG_RUN_FAILED', 'SLA_MISSED')"
NEW_TYPES = "type IN ('DAG_RUN_FAILED', 'SLA_MISSED', 'DATA_CHECK_FAILED')"


def _incident_types(condition: str) -> None:
    with op.batch_alter_table("incidents", schema=None) as batch_op:
        batch_op.drop_constraint(op.f("ck_incidents_incident_type"), type_="check")
        batch_op.create_check_constraint(op.f("ck_incidents_incident_type"), condition)


def upgrade() -> None:
    _incident_types(NEW_TYPES)
    op.create_table(
        "dag_run_checks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("connection_id", sa.Uuid(), nullable=False),
        sa.Column("monitored_dag_id", sa.Uuid(), nullable=True),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column("run_id", sa.String(length=250), nullable=False),
        sa.Column("run_state", sa.String(length=32), nullable=False),
        sa.Column("workflow_run_id", sa.Uuid(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "PENDING",
                "PASSED",
                "FAILED",
                "NOT_CHECKED",
                "ERROR",
                name="dag_run_check_status",
                native_enum=False,
                create_constraint=True,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["connection_id"],
            ["airflow_connections.id"],
            name=op.f("fk_dag_run_checks_connection_id_airflow_connections"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["monitored_dag_id"],
            ["monitored_dags.id"],
            name=op.f("fk_dag_run_checks_monitored_dag_id_monitored_dags"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["workflow_run_id"],
            ["workflow_runs.id"],
            name=op.f("fk_dag_run_checks_workflow_run_id_workflow_runs"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_dag_run_checks")),
        sa.UniqueConstraint("workflow_run_id", name=op.f("uq_dag_run_checks_workflow_run_id")),
    )
    op.create_index(
        "ix_dag_run_checks_dag_created",
        "dag_run_checks",
        ["connection_id", "dag_id", "created_at"],
        unique=False,
    )
    op.create_index(
        op.f("ix_dag_run_checks_monitored_dag_id"),
        "dag_run_checks",
        ["monitored_dag_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_dag_run_checks_monitored_dag_id"), table_name="dag_run_checks")
    op.drop_index("ix_dag_run_checks_dag_created", table_name="dag_run_checks")
    op.drop_table("dag_run_checks")
    op.execute("DELETE FROM incidents WHERE type = 'DATA_CHECK_FAILED'")
    _incident_types(OLD_TYPES)
