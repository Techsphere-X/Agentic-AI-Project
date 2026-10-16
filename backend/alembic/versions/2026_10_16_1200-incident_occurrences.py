"""add incident occurrence history

Revision ID: 202610161200
Revises: d4e8f1a2b3c5
Create Date: 2026-10-16 12:00:00

"""
import uuid
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

import app.db.base


revision: str = "202610161200"
down_revision: Union[str, Sequence[str], None] = "d4e8f1a2b3c5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "incident_occurrences",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("incident_id", sa.Uuid(), nullable=False),
        sa.Column("connection_id", sa.Uuid(), nullable=False),
        sa.Column("monitored_dag_id", sa.Uuid(), nullable=False),
        sa.Column("dag_id", sa.String(length=250), nullable=False),
        sa.Column(
            "type",
            sa.Enum(
                "DAG_RUN_FAILED",
                "SLA_MISSED",
                name="incident_occurrence_type",
                native_enum=False,
                create_constraint=True,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("title", sa.String(length=300), nullable=False),
        sa.Column(
            "severity",
            sa.Enum(
                "LOW",
                "MEDIUM",
                "HIGH",
                "CRITICAL",
                name="incident_occurrence_severity",
                native_enum=False,
                create_constraint=True,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.Enum(
                "OPEN",
                "ACKNOWLEDGED",
                "RESOLVED",
                name="incident_occurrence_status",
                native_enum=False,
                create_constraint=True,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("run_id", sa.String(length=250), nullable=True),
        sa.Column("first_seen_at", app.db.base.UTCDateTime(timezone=True), nullable=False),
        sa.Column("occurred_at", app.db.base.UTCDateTime(timezone=True), nullable=False),
        sa.Column("summary", sa.Text(), nullable=True),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(
            ["incident_id"], ["incidents.id"], name=op.f("fk_incident_occurrences_incident_id_incidents"), ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["connection_id"], ["airflow_connections.id"], name=op.f("fk_incident_occurrences_connection_id_airflow_connections"), ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["monitored_dag_id"], ["monitored_dags.id"], name=op.f("fk_incident_occurrences_monitored_dag_id_monitored_dags"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_incident_occurrences")),
    )
    with op.batch_alter_table("incident_occurrences", schema=None) as batch_op:
        batch_op.create_index(
            batch_op.f("ix_incident_occurrences_incident_id"), ["incident_id"], unique=False
        )
        batch_op.create_index(
            batch_op.f("ix_incident_occurrences_connection_id"), ["connection_id"], unique=False
        )
        batch_op.create_index(
            batch_op.f("ix_incident_occurrences_monitored_dag_id"), ["monitored_dag_id"], unique=False
        )
        batch_op.create_index(
            "ix_incident_occurrences_incident_occurred", ["incident_id", "occurred_at"], unique=False
        )
        batch_op.create_index(
            batch_op.f("ix_incident_occurrences_occurred_at"), ["occurred_at"], unique=False
        )

    bind = op.get_bind()
    incidents = sa.table(
        "incidents",
        sa.column("id", sa.Uuid()),
        sa.column("connection_id", sa.Uuid()),
        sa.column("monitored_dag_id", sa.Uuid()),
        sa.column("dag_id", sa.String()),
        sa.column("type", sa.String()),
        sa.column("title", sa.String()),
        sa.column("severity", sa.String()),
        sa.column("status", sa.String()),
        sa.column("run_id", sa.String()),
        sa.column("summary", sa.Text()),
        sa.column("first_seen_at", app.db.base.UTCDateTime(timezone=True)),
        sa.column("occurred_at", app.db.base.UTCDateTime(timezone=True)),
    )
    occurrences = sa.table(
        "incident_occurrences",
        sa.column("id", sa.Uuid()),
        sa.column("incident_id", sa.Uuid()),
        sa.column("connection_id", sa.Uuid()),
        sa.column("monitored_dag_id", sa.Uuid()),
        sa.column("dag_id", sa.String()),
        sa.column("type", sa.String()),
        sa.column("title", sa.String()),
        sa.column("severity", sa.String()),
        sa.column("status", sa.String()),
        sa.column("run_id", sa.String()),
        sa.column("first_seen_at", app.db.base.UTCDateTime(timezone=True)),
        sa.column("occurred_at", app.db.base.UTCDateTime(timezone=True)),
        sa.column("summary", sa.Text()),
        sa.column("details", sa.JSON()),
    )
    rows = bind.execute(sa.select(incidents)).mappings()
    for row in rows:
        bind.execute(
            occurrences.insert().values(
                id=uuid.uuid4(),
                incident_id=row["id"],
                connection_id=row["connection_id"],
                monitored_dag_id=row["monitored_dag_id"],
                dag_id=row["dag_id"],
                type=row["type"],
                title=row["title"],
                severity=row["severity"],
                status=row["status"],
                run_id=row["run_id"],
                first_seen_at=row["first_seen_at"],
                occurred_at=row["occurred_at"],
                summary=row["summary"],
                details={"backfilled": True},
            )
        )


def downgrade() -> None:
    with op.batch_alter_table("incident_occurrences", schema=None) as batch_op:
        batch_op.drop_index("ix_incident_occurrences_occurred_at")
        batch_op.drop_index("ix_incident_occurrences_incident_occurred")
        batch_op.drop_index(batch_op.f("ix_incident_occurrences_monitored_dag_id"))
        batch_op.drop_index(batch_op.f("ix_incident_occurrences_connection_id"))
        batch_op.drop_index(batch_op.f("ix_incident_occurrences_incident_id"))
    op.drop_table("incident_occurrences")
