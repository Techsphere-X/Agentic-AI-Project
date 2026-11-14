"""'When this DAG runs' replaces 'When a DAG run finishes' and 'Watch selected DAGs'

Saved workflows are converted (see app/automation/migrate.py):
- one DAG: the start block is swapped in place;
- several DAGs: one workflow per DAG ("<name> · <dag>"); the original is deleted when it has no
  runs, otherwise turned off and renamed "(replaced) <name>";
- no DAG ("every DAG"): the workflow is turned off with a note in its description.

Revision ID: 202611041200
Revises: 202610161200
Create Date: 2026-11-04 12:00:00

"""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime

import sqlalchemy as sa
from alembic import op

from app.automation.migrate import migrate_graph

revision: str = "202611041200"
down_revision: str | Sequence[str] | None = "202610161200"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

workflows = sa.table(
    "automation_workflows",
    sa.column("id", sa.Uuid()),
    sa.column("key", sa.String()),
    sa.column("template_key", sa.String()),
    sa.column("template_version", sa.Integer()),
    sa.column("template_parameters", sa.JSON()),
    sa.column("name", sa.String()),
    sa.column("description", sa.Text()),
    sa.column("enabled", sa.Boolean()),
    sa.column("mode", sa.String()),
    sa.column("graph", sa.JSON()),
    sa.column("version", sa.Integer()),
    sa.column("created_by", sa.Uuid()),
    sa.column("created_at", sa.DateTime()),
    sa.column("updated_at", sa.DateTime()),
)
runs = sa.table("workflow_runs", sa.column("id", sa.Uuid()), sa.column("workflow_id", sa.Uuid()))
dags = sa.table(
    "monitored_dags",
    sa.column("connection_id", sa.Uuid()),
    sa.column("dag_id", sa.String()),
)
connections = sa.table(
    "airflow_connections", sa.column("id", sa.Uuid()), sa.column("is_default", sa.Boolean())
)


def upgrade() -> None:
    bind = op.get_bind()
    now = datetime.now(UTC)
    default = bind.execute(
        sa.select(connections.c.id).where(connections.c.is_default.is_(True))
    ).scalar()

    def connection_for(dag_id: str) -> str | None:
        found = [
            row[0]
            for row in bind.execute(
                sa.select(dags.c.connection_id).where(dags.c.dag_id == dag_id)
            ).all()
        ]
        if not found:
            return None
        return str(default if default in found else found[0])

    for row in bind.execute(sa.select(workflows)).mappings().all():
        result = migrate_graph(row["graph"] or {}, connection_for)
        if result is None:
            continue
        if result.disable:
            note = f"Turned off: {result.disable}."
            description = "\n\n".join(filter(None, [row["description"], note]))
            bind.execute(
                workflows.update()
                .where(workflows.c.id == row["id"])
                .values(enabled=False, description=description, updated_at=now)
            )
            continue
        if len(result.graphs) == 1:
            bind.execute(
                workflows.update()
                .where(workflows.c.id == row["id"])
                .values(graph=result.graphs[0][1], version=row["version"] + 1, updated_at=now)
            )
            continue
        for dag_id, graph in result.graphs:
            bind.execute(
                workflows.insert().values(
                    id=uuid.uuid4(),
                    key=None,
                    template_key=row["template_key"],
                    template_version=row["template_version"],
                    template_parameters=row["template_parameters"],
                    name=f"{row['name']} · {dag_id}"[:200],
                    description=row["description"],
                    enabled=row["enabled"],
                    mode=row["mode"],
                    graph=graph,
                    version=1,
                    created_by=row["created_by"],
                    created_at=now,
                    updated_at=now,
                )
            )
        has_runs = bind.execute(
            sa.select(runs.c.id).where(runs.c.workflow_id == row["id"]).limit(1)
        ).first()
        if has_runs:
            bind.execute(
                workflows.update()
                .where(workflows.c.id == row["id"])
                .values(enabled=False, name=f"(replaced) {row['name']}"[:200], updated_at=now)
            )
        else:
            bind.execute(workflows.delete().where(workflows.c.id == row["id"]))


def downgrade() -> None:
    # Converted graphs are not turned back into the old start blocks.
    pass
