"""workflow version history

Revision ID: 202611061200
Revises: 202611051200
Create Date: 2026-11-06 12:00:00

Each saved change in a workflow's behaviour keeps its graph. Existing workflows get their
current graph as their first stored version.
"""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op

import app.db.base

revision: str = "202611061200"
down_revision: str | Sequence[str] | None = "202611051200"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    versions = op.create_table(
        "workflow_versions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("workflow_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("graph", sa.JSON(), nullable=False),
        sa.Column("created_by", sa.Uuid(), nullable=True),
        sa.Column("created_at", app.db.base.UTCDateTime(), nullable=False),
        sa.ForeignKeyConstraint(["workflow_id"], ["automation_workflows.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["created_by"], ["users.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("workflow_id", "version"),
    )
    op.create_index("ix_workflow_versions_workflow_id", "workflow_versions", ["workflow_id"])
    workflows = sa.table(
        "automation_workflows",
        sa.column("id", sa.Uuid()),
        sa.column("name", sa.String()),
        sa.column("graph", sa.JSON()),
        sa.column("version", sa.Integer()),
        sa.column("created_by", sa.Uuid()),
    )
    bind = op.get_bind()
    now = datetime.now(UTC)
    rows = bind.execute(sa.select(workflows)).mappings().all()
    if rows:
        op.bulk_insert(
            versions,
            [
                {
                    "id": uuid.uuid4(),
                    "workflow_id": row["id"],
                    "version": row["version"],
                    "name": row["name"],
                    "graph": row["graph"],
                    "created_by": row["created_by"],
                    "created_at": now,
                }
                for row in rows
            ],
        )


def downgrade() -> None:
    op.drop_index("ix_workflow_versions_workflow_id", table_name="workflow_versions")
    op.drop_table("workflow_versions")
