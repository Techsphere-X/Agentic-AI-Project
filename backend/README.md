# Backend

FastAPI control plane for Agentic Data Automation (see `../plan0.md`).

## Setup

```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-dev.txt
copy .env.example .env      # then set JWT_SECRET_KEY, ENCRYPTION_KEY, ADMIN_PASSWORD
alembic upgrade head
```

The first start creates an admin from `ADMIN_EMAIL` / `ADMIN_PASSWORD` if no users exist.

## Run

```powershell
uvicorn app.main:app --reload
```

- Liveness: `GET http://127.0.0.1:8000/api/v1/health/live`
- API docs (development only): `http://127.0.0.1:8000/docs`

## Test & lint

```powershell
pytest                       # SQLite; PostgreSQL migration test runs when TEST_POSTGRES_URL is set
ruff check app tests
```

## Layout

| Path | Purpose |
|---|---|
| `app/api/v1` | HTTP routes (thin; delegate to services) |
| `app/core` | Settings, security (JWT/Argon2), secret encryption, DI, errors, rate limiting |
| `app/db`, `app/models` | SQLAlchemy engine/session, ORM models |
| `app/schemas` | Pydantic request/response models |
| `app/services` | Business logic + audit logging |
| `app/orchestration/airflow` | Airflow 2.x/3.x REST adapter, mock adapter, factory |
| `app/detection` | Detection rules, severity, evidence scrubbing (pure) + lease-guarded scheduler |
| `app/diagnosis` | Log classifier: failure category from task logs (pure) |
| `app/automation` | Workflow graph catalog + validation, policy, templates (pure) |
| `alembic/` | Migrations (`render_as_batch=True` for SQLite) |

## Detection

With `DETECTION_ENABLED=true` (default), the server polls monitored DAGs every
`DETECTION_INTERVAL_SECONDS` and opens incidents for failed runs and missed freshness SLAs
(set `SLA (min)` per DAG in the UI). Operators can also trigger a cycle with
`POST /api/v1/detection/run` or the "Run detection now" button. Only one worker polls at a time
(DB lease in `detection_leases`).

After pulling Plan 1, run `alembic upgrade head` to create the incident tables.

## Connection monitor

With `AIRFLOW_MONITOR_ENABLED=true` (default), the server lists DAGs from every active
connection every `AIRFLOW_MONITOR_INTERVAL_SECONDS` (30s). Success refreshes the DAG list and
marks the connection `HEALTHY`; failure stores the reason (`UNREACHABLE`, `UNAUTHORIZED`, ...).
Each status change is written to the audit log as `airflow_conn.status_changed`.

The API reports `is_live` on every connection: `HEALTHY` and checked within two monitor
intervals. The UI only shows a connection's DAGs while it is live; otherwise it shows
"Connection lost" / "Airflow not connected". Operators can force a check with
`POST /api/v1/airflow/connections/{id}/refresh` ("Retry now"). Only one worker polls
(`connections` lease in `detection_leases`).

## Automation (Plan 2)

Incidents trigger **workflows**: graphs of typed steps (trigger → filter → diagnose → approval →
Airflow action → verify → update incident / notify). Six built-in workflows are added, disabled,
on first start. Turn them on under *Automation → Workflows* (Admin), ideally in **Dry run** first.

- Runs are queued by detection and advanced right after each detection cycle (and after an
  approval decision, or `POST /api/v1/automation/tick`).
- Safety: any Airflow change on a `PROD` connection needs a human approval; at most
  `AUTOMATION_MAX_ACTIONS_PER_DAG_PER_DAY` automated actions per DAG; `AUTOMATION_FORCE_DRY_RUN=true`
  turns every workflow into a simulation. Everything is audited.
- Mock connections simulate retries: `partner_api_sync` fails with a transient HTTP 503 (a retry
  fixes it), `orders_pipeline` fails with bad data (a retry fails again).

After pulling Plan 2, run `alembic upgrade head` to create the automation tables.

## Local PostgreSQL / Airflow

```powershell
docker compose up -d postgres airflow3     # from the repo root
$env:DATABASE_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/agentic_ai"
alembic upgrade head
```

Register Airflow in the UI with base URL `http://localhost:8080`, user `admin` / `admin`.

## Sandbox demo

`scripts/seed_demo.py` sets up the demo: the "Sandbox environment" Airflow connection, the
source and warehouse PostgreSQL connections, the Team Slack channel, the monitored `sandbox_*`
DAGs, and four enabled workflows (retry network failures, fix with approval, validate every
e-commerce run, compare source and warehouse orders). It matches records by name, so running it
again repairs the demo instead of duplicating it. The `sandbox_*` DAGs must already be deployed
to that Airflow.

```powershell
$env:DEMO_AIRFLOW_PASSWORD = "admin"       # needed only when the connection does not exist yet
python scripts/seed_demo.py                # --mock-airflow uses the built-in mock instead
```

The `DEMO_*` settings are listed at the end of `.env.example`.

### Workflow test set

`scripts/seed_test_workflows.py` (run after `seed_demo.py`) adds eight workflows, turns the four
demo workflows above off, and sets the ETL's freshness SLA to 1500 minutes:

| Workflow | Starts when | Does |
|---|---|---|
| SLA 1 · Alert when a run takes longer than usual | ETL or failure-lab run succeeds | Alerts if it took over twice as long as usual (median of its last 10 runs) |
| SLA 2 · Alert while a run is still going past its usual time | Every 5 minutes | Alerts if a failure-lab run is still running past that limit |
| SLA 3 · Re-run the ETL when its data is late | ETL freshness SLA missed | Re-runs the ETL, verifies, resolves (escalates if the re-run fails) |
| Data quality 1 · Check the warehouse after every ETL run | ETL run succeeds | 20 checks; marks the run passed, or failed with an incident and alert |
| Data quality 2 · Check the warehouse now (manual) | Run now | The same 20 checks, reported to the team; changes nothing |
| Fix 1 · Auto-retry temporary failures | Network, timeout, memory, missing-input failure | Retries, verifies, resolves; stops after 3 failures |
| Fix 2 · Ask before retrying failures that need a code or data fix | Bad data, schema, login failure | AI suggests a fix; a person approves the retry |
| Fix 3 · Flag failures with an unknown cause | Failure with no known cause | AI explains the error; asks the team to look |

The 20 data quality checks cover volume, completeness (against the source database and the ETL
log), uniqueness, missing values, validity, referential integrity, freshness, schema drift and
distribution. The chain stops at the first failed check and the alert names it.

To try them: trigger `sandbox_failure_lab` with run config `{"failure": "<mode>"}` (`network`,
`timeout`, `oom`, `missing_input` for Fix 1; `bad_data`, `schema`, `auth` for Fix 2; `slow` with
`"seconds": 90` for SLA 1, or `600` for SLA 2), trigger `sandbox_retry_demo` for Fix 3, and run the
ETL for Data quality 1. To see a data quality failure, change a warehouse row, e.g.
`UPDATE warehouse.fact_orders SET quantity = -1 WHERE order_id = (SELECT min(order_id) FROM warehouse.fact_orders);`,
then press Run now on Data quality 2. The next ETL run reloads the data. Raise
`AUTOMATION_MAX_ACTIONS_PER_DAG_PER_DAY` (default 3) when testing many retries in one day.
