"""add wedata connections and monitored tasks

Revision ID: wedata_20261025
Revises: 202610161200
Create Date: 2026-10-25 20:00:00

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

import app.db.base

revision: str = "wedata_20261025"
down_revision: Union[str, Sequence[str], None] = "202610161200"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "wedata_connections",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column(
            "environment",
            sa.Enum("DEV", "STAGING", "PROD", name="wedata_deployment_environment", native_enum=False, create_constraint=True, length=32),
            nullable=False,
        ),
        sa.Column(
            "kind",
            sa.Enum("LIVE", "MOCK", name="wedata_connection_kind", native_enum=False, create_constraint=True, length=32),
            nullable=False,
        ),
        sa.Column("project_id", sa.String(length=100), nullable=False),
        sa.Column("region", sa.String(length=50), nullable=False, server_default="ap-guangzhou"),
        sa.Column("endpoint", sa.String(length=200), nullable=False, server_default="wedata.tencentcloudapi.com"),
        sa.Column("secret_id", sa.String(length=200), nullable=True),
        sa.Column("encrypted_secret_key", sa.Text(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("is_default", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "last_health_status",
            sa.Enum(
                "UNKNOWN", "HEALTHY", "UNAUTHORIZED", "FORBIDDEN", "UNREACHABLE", "TLS_ERROR", "INVALID_ENDPOINT", "WEDATA_ERROR",
                name="wedata_connection_status",
                native_enum=False,
                create_constraint=True,
                length=32,
            ),
            nullable=False,
            server_default="UNKNOWN",
        ),
        sa.Column("last_health_message", sa.Text(), nullable=True),
        sa.Column("last_latency_ms", sa.Integer(), nullable=True),
        sa.Column("last_checked_at", app.db.base.UTCDateTime(timezone=True), nullable=True),
        sa.Column("created_by", sa.Uuid(), nullable=True),
        sa.Column("created_at", app.db.base.UTCDateTime(timezone=True), nullable=False),
        sa.Column("updated_at", app.db.base.UTCDateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"], name=op.f("fk_wedata_connections_created_by_users"), ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_wedata_connections")),
        sa.UniqueConstraint("name", name=op.f("uq_wedata_connections_name")),
    )

    op.create_table(
        "wedata_monitored_tasks",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("connection_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.String(length=200), nullable=False),
        sa.Column("task_name", sa.String(length=200), nullable=False),
        sa.Column("workflow_id", sa.String(length=100), nullable=True),
        sa.Column("workflow_name", sa.String(length=200), nullable=True),
        sa.Column("task_type", sa.String(length=50), nullable=True),
        sa.Column("schedule_cron", sa.String(length=100), nullable=True),
        sa.Column("is_paused", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("is_monitored", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("is_present_in_wedata", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("last_synced_at", app.db.base.UTCDateTime(timezone=True), nullable=False),
        sa.Column("last_run_state", sa.String(length=50), nullable=True),
        sa.Column("last_run_id", sa.String(length=200), nullable=True),
        sa.Column("last_run_at", app.db.base.UTCDateTime(timezone=True), nullable=True),
        sa.Column("created_at", app.db.base.UTCDateTime(timezone=True), nullable=False),
        sa.Column("updated_at", app.db.base.UTCDateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["connection_id"], ["wedata_connections.id"], name=op.f("fk_wedata_monitored_tasks_connection_id"), ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_wedata_monitored_tasks")),
        sa.UniqueConstraint("connection_id", "task_id", name="uq_wedata_monitored_tasks_conn_task"),
    )
    op.create_index(op.f("ix_wedata_monitored_tasks_connection_id"), "wedata_monitored_tasks", ["connection_id"], unique=False)
    op.create_index(op.f("ix_wedata_monitored_tasks_is_monitored"), "wedata_monitored_tasks", ["is_monitored"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_wedata_monitored_tasks_is_monitored"), table_name="wedata_monitored_tasks")
    op.drop_index(op.f("ix_wedata_monitored_tasks_connection_id"), table_name="wedata_monitored_tasks")
    op.drop_table("wedata_monitored_tasks")
    op.drop_table("wedata_connections")
