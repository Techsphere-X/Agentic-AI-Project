# Plan 5: A visual workflow builder for data engineers

## 1. Product, in one line

**Data engineers draw automations on a canvas: a trigger starts the workflow, blocks do the work (Airflow, SQL, checks, messages), and each block's outputs decide what happens next.** It is like n8n or Zapier, but for data pipelines, with approvals, dry runs and an audit trail built in.

| It is | It is not |
|---|---|
| A builder: you design the flow, the engine runs it | An AI agent that decides on its own |
| For data engineers (knows DAGs, SQL, connections) | A tool for non-technical users |
| Airflow and databases first; other systems through blocks | A replacement for Airflow (it *drives* Airflow) |
| Safe by default: risky blocks need approval on PROD; dry runs; every action audited | A place to run arbitrary code |

**Incidents become one kind of trigger, not the centre of the product.** Failure detection, diagnosis and the log analyzer stay as features that feed workflows.

## 2. Where we are

Plans 0–4 built an **incident auto-fixer** first and then grew a canvas. The canvas is now a general builder, but the product still reads as incident-first:

- **Navigation and defaults are incident-first.** A new workflow starts with an incident trigger. "Filter incidents" is the only general condition block. The sidebar mixes Incidents, Run checks and the Log analyzer in with the builder.
- **Builder basics are missing:**
  - an If/Switch block on any value
  - cron schedules (only "every N minutes" today)
  - a webhook trigger and an HTTP block
  - per-block retry, timeout and error output
  - "test this block"
  - version history
  - import/export
  - sub-workflows
  - for-each
- **Known engine bugs (from the review):**
  - A "When a DAG run finishes" trigger first seen *in progress* never takes its success/failed branch.
  - The "incident ignored" trigger fires only once per incident.
  - The plan docs are out of date.

**What already fits and stays:**
- the canvas and palette
- the typed block catalog (`backend/app/automation/graph.py`)
- the six triggers, plus "Watch selected DAGs"
- the Airflow, SQL and compare-databases blocks
- notification channels
- approvals, the policy layer and dry runs
- run replay
- `{{placeholders}}`
- templates and the assistant
- per-config outputs (`port_rule` / `portsOf`, built for "Watch selected DAGs") — reused below for Switch

## 3. Phases

Each phase ships on its own and ends with backend, canvas and Playwright tests.

### Phase A — Reframe (small, first)
- **Sidebar:**
  - **Build:** Workflows, Templates
  - **Runs:** Runs, Approvals, Notifications
  - **Pipelines:** Pipelines canvas, Incidents, Run checks, Log analyzer
  - **Connections:** Airflow, Databases, Channels
- The landing page after login becomes Workflows.
- **New workflow:** a "How should it start?" picker (manual, schedule, webhook, DAG run finishes, incident, watch DAGs) instead of defaulting to incidents.
- **Fix the review bugs:**
  - DAG-run trigger: dedup per run *and* state
  - stale trigger: dedup per occurrence
  - stale trigger: validate graphs once per tick
- Bring `project.md` and plans 2–3 up to date with the trigger list.

### Phase B — Logic blocks and error handling
- **If**: compare any value (`{{results.q.value}}`, `{{trigger.body.x}}`) with an operator (=, ≠, >, <, contains, is empty). Outputs: *true* / *false*.
- **Switch**: one output per case you add, plus *otherwise*. It uses dynamic ports (`port_rule`).
- **Wait for all**: a join; continues once every incoming branch has arrived. This needs the engine to track arrivals per run.
- **Set value**: stores a named value for later blocks (`{{vars.name}}`).
- **Per-block settings on every action block:**
  - retries (count and delay)
  - timeout
  - "on error" behaviour: *fail the run* or *take the error output*. The *error* output appears on the block, with `{{last_error}}`.
- "Filter incidents" stays for incident workflows. The palette shows If/Switch first.

### Phase C — Triggers
- **Schedule**: a cron expression and a timezone (keep "every N minutes" as a simple mode). It shows the next 3 run times in the form.
- **Webhook**: a secret URL per workflow (`POST /hooks/{workflow_id}/{token}`, token rotatable). The JSON body becomes `{{trigger.body.*}}`. Each request is rate-limited and audited.
- The incident, watch-DAGs, DAG-run and manual triggers are unchanged.

### Phase D — Builder workflow (the developer experience)
- **Test a block**: run one block in dry-run mode with the values from the last run, or with sample input you paste. The result shows in the settings popover.
- **Run with input**: "Run now" with a JSON payload, so manual and webhook flows can be tested.
- **Versions**: every save that changes behaviour stores a version. You can list versions, see a diff of the blocks and links, and restore one. Runs keep linking to the version they ran.
- **Import / export**: workflow JSON, so workflows can live in git. Import is validated like a save.
- **Runs page**:
  - filter by workflow, trigger and status
  - "Run again with the same input"
  - delete (done)

### Phase E — More blocks
- **HTTP request**:
  - method, URL, headers, JSON body (templated)
  - the response becomes available as `{{results.<id>.status}}` and `.body`
  - outputs: *success* (2xx) / *failed*
  - counts as an action (policy-checked)
- **For each row**: runs a sub-flow for each row of a SQL result, capped (e.g. 100 rows), one at a time.
- **Call workflow**: starts another workflow (manual/webhook kind) with input and waits for it, so steps can be reused.
- **Later:**
  - dbt run/test
  - S3/GCS "file arrived" trigger
  - Spark/Databricks job

### Out of scope
- Arbitrary Python or shell execution.
- Loops in the graph itself (the graph stays acyclic; repetition lives inside "For each").
- Multi-tenant workspaces.
- A public template marketplace.

## 4. Key decisions

| Topic | Decision | Why |
|---|---|---|
| Engine | Keep the graph acyclic and run it step by step from the DB (as today) | Runs survive restarts and replay on the canvas |
| Dynamic outputs | Reuse `port_rule` / `portsOf` from "Watch selected DAGs" | Already works end to end, with tests |
| Safety | Every block that changes something stays policy-checked; HTTP and webhook count too | The safety layer is what sets this apart from n8n |
| Errors | An error output is opt-in per block; the default is still "fail the run" | Existing workflows behave the same |
| Values between blocks | Keep `{{...}}` placeholders; add `trigger.body` and `vars` | One way of passing data, already known to users |

## 5. Verification

- **Backend (pytest):**
  - every new block validates, runs and branches correctly
  - webhook auth and rate limits
  - cron next-run times across timezones
  - version restore
  - error outputs and retries
- **Canvas (`npm run test:canvas`):**
  - Switch outputs
  - error output toggling
  - import/export round trip
- **Browser (Playwright):**
  - build *webhook → If → Run SQL / Notify*
  - call the webhook
  - see the run take the right branch
  - restore an older version
- **Live:** a cron workflow that runs an Airflow DAG and an HTTP call against the docker-compose stack.
