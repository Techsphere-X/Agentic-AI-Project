"""workflow webhook token ('When a webhook is called')

Revision ID: 202611051200
Revises: 202611041200
Create Date: 2026-11-05 12:00:00

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "202611051200"
down_revision: str | Sequence[str] | None = "202611041200"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("automation_workflows") as batch:
        batch.add_column(sa.Column("webhook_token", sa.String(length=64), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("automation_workflows") as batch:
        batch.drop_column("webhook_token")
