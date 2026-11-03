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
- the manual, schedule and incident triggers (Phase 0 replaces the DAG triggers)
- the Airflow, SQL and compare-databases blocks
- notification channels
- approvals, the policy layer and dry runs
- run replay
- `{{placeholders}}`
- templates and the assistant
- per-config outputs (`port_rule` / `portsOf`, built for "Watch selected DAGs") — kept and reused for Switch

## 3. Phases

Each phase ships on its own and ends with backend, canvas and Playwright tests.

### Phase 0 — Restructure "Start when…" around one DAG (first priority)

**Problem.** The logic for a single DAG is spread over three places:
- Pipelines settings: monitored, failure monitor, SLA
- global detection: one shared, deduplicated incident per failure
- several overlapping triggers: any-DAG incident, watch selected DAGs, DAG run finishes

So an engineer cannot say, in one workflow: *"for `orders_dag`: when it succeeds, run my checks; when it fails, tell Priya and try my fix."*

**Decisions (agreed):**

| Topic | Decision |
|---|---|
| New block | **"When this DAG runs"**: pick **one** DAG. Outputs: **in progress / succeeded / failed** |
| Events | Each event starts its **own** workflow run. One Airflow run can fire *in progress* when it starts, then *succeeded* or *failed* when it ends |
| Incidents | **The workflow decides.** No automatic *failure* incidents for a DAG whose enabled "When this DAG runs" workflow starts on **failed** (it owns the failures). SLA/freshness incidents still open, because the block has no such event. A new **Open incident** block opens one where you put it |
| Other DAGs | Catch-all as today. Monitored DAGs with no DAG workflow still get automatic incidents. "Any DAG has an incident" and "Incident ignored" keep working |
| Fix blocks | Fetch logs, Classify, Suggest a fix (AI), Choose a fix, Check DAG state, Retry failed tasks, Re-run, Pause all work on **the trigger's DAG run** directly; no incident needed. They still work under incident triggers |
| After a fix | **The same workflow run waits** for the fixed Airflow run (Verify) and continues on its result. The fixed run does **not** start the workflow again, so there are no retry loops |
| Same DAG | Several workflows may start from one DAG; all of them run. The editor shows "also used by: …" |
| Old blocks | "Watch selected DAGs" and "When a DAG run finishes" are **removed and migrated**. Kept: Any DAG has an incident, Incident ignored, Run manually, Schedule |

**The new "Start when…" palette:**
```
Start when…
  When this DAG runs        ← main block; outputs: in progress · succeeded · failed
    DAGs: orders_pipeline, billing_daily, …   (drag a DAG = this block, already set to it)
  Incidents (catch-all)
    When any DAG has an incident
    When an incident is ignored
  Orchestration
    When run manually
    On a schedule
```

**Example workflow:**
```
[When this DAG runs · orders_pipeline]
   succeeded ─▶ [Check data (SQL): rows > 0] ─ pass ─▶ [Record passed]
                                              └ fail ─▶ [Notify data-team] ─▶ [Open incident]
   failed    ─▶ [Classify the failure] ─ retry may help ─▶ [Retry failed tasks] ─▶ [Verify]
                                                              ├ succeeded ─▶ [Notify: fixed]
                                                              └ failed    ─▶ [Notify priya@…] ─▶ [Open incident]
   in progress ─▶ [Check SLA] ─ over SLA ─▶ [Notify: running late]
```

**Backend:**
- **`graph.py`:**
  - New `trigger.dag` ("When this DAG runs"). Config: `connection_id` and `dag_id` (required, the `dag` widget), plus `states` (which outputs fire; default all three). Ports: `in_progress`, `success`, `failed`.
  - Remove `trigger.dag_watch` and `trigger.dag_run` from the palette catalog.
  - Split the block flags:
    - `needs_incident`: only *Filter incidents* and *Update the incident*.
    - New `needs_target`: blocks that work on "a DAG run". Valid under any incident trigger or `trigger.dag`.
    - `needs_dag_run`: Record the run result, valid under `trigger.dag` only.
  - New block `incident.open` ("Open incident"). Settings: type, severity, title/note (templated). It opens or updates an incident for the trigger's DAG run and attaches evidence. Reuse `detection_service._collect_failure_evidence`, `_fingerprint` and `incident_service`. Outputs: *opened* (or *already open*). Later blocks see `{{incident.*}}`.
- **`automation_nodes.py`:**
  - Add `NodeContext.target`: connection, dag_id, run_id and the incident or None. It comes from the incident, or from `context["dag_run"]`.
  - Replace the `the_incident` DAG lookups in logs, classify, AI fix, choose fix, clear, rerun, pause and DAG state with `ctx.target`.
  - The diagnosis is stored on the incident when there is one, otherwise in `run.context["diagnosis"]`.
  - "Choose a fix" counts earlier attempts from previous workflow runs on the same DAG when there is no incident.
- **`automation_service.py`:**
  - `enqueue_for_dag_event(conn, dag, run, state)`. Dedup is `dagrun:{conn}:{dag}:{run_id}:{state}`, so each event fires once and the fixed run's states don't repeat.
  - Fix suppression: when a run clears tasks or re-runs, it records the Airflow run as owned by that workflow run (extend `AirflowTriggerReservation`). Events for an owned run don't start that workflow again; the owning run's Verify reads the result.
  - On enable, switch the DAG to monitored (reuse `_monitor_watched_dags`).
- **`detection_service._process_dag`:**
  - Fire `in_progress` for running runs and `success`/`failed` for finished runs to the DAG workflows.
  - Skip automatic failure/SLA incidents for DAGs that an enabled `trigger.dag` workflow handles.
- **Migration** (Alembic data migration plus a template update):
  - `trigger.dag_run` with one DAG becomes `trigger.dag`; ports map 1:1, and `next` maps to all three outputs.
  - `trigger.dag_watch` and multi-DAG `dag_run` become one workflow per DAG (named "‹name› · ‹dag›"), each with the same follow-up blocks. A watch DAG's output becomes the *failed* output.
  - `dag_run` with no DAG (meaning "all DAGs") is turned off and flagged *"choose a DAG"*.
  - Templates in `templates.py` switch to `trigger.dag`.

**Frontend:**
- Palette: the structure above. The DAG list under "When this DAG runs" drops a ready-set block (reuse `PipelineSources`), and clicking a DAG on the canvas replaces the trigger.
- Block: the DAG name in big type, and three coloured outputs.
- Settings: connection, DAG, outputs to fire, and "Also used by: workflow A, B".
- Remove the dag_watch drop-onto-block code: `addWatchedDag`, `DagListInput` and the drop highlight. `portsOf` stays for Switch.
- Pipelines canvas: a DAG with a DAG workflow shows "handled by ‹workflow›" instead of the automatic failure monitor.

**Verification:**
- Backend tests:
  - each event starts one run on the right output
  - in progress then succeeded gives two runs
  - retry → verify continues in the same run, with no new run for the fixed Airflow run
  - Open incident, then Update the incident
  - no automatic incident for handled DAGs; still one for other DAGs
  - every migration case, plus templates validating
- Playwright: drag `orders_pipeline` → wire succeeded/failed → save and turn on → run detection on the mock → see the run take the right output.

### Phase A — Reframe (small, first)
- **Sidebar:**
  - **Build:** Workflows, Templates
  - **Runs:** Runs, Approvals, Notifications
  - **Pipelines:** Pipelines canvas, Incidents, Run checks, Log analyzer
  - **Connections:** Airflow, Databases, Channels
- The landing page after login becomes Workflows.
- **New workflow:** a "How should it start?" picker (When this DAG runs, manual, schedule, webhook, any-DAG incident) instead of defaulting to incidents.
- **Fix the remaining review bugs** (the DAG-run state bug is fixed by Phase 0):
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

## 6. Status (implemented)

All phases are built and tested:
- Backend: 541 tests pass.
- Canvas: 24 tests pass.
- Browser (Playwright): 14 tests pass.

| Phase | Done |
|---|---|
| 0 | `trigger.dag` "When this DAG runs" (in progress / succeeded / failed); `incident.open`; fix blocks work on the DAG run without an incident; fixes don't restart their own workflow; migration `202611041200` converts saved DAG triggers |
| A | Builder-first sidebar; Workflows is the home page (the dashboard moved to `/dashboard`); "How should this workflow start?" picker; the stale trigger fires again after a recurrence |
| B | `logic.if`, `logic.switch` (an output per case), `logic.join` (Wait for all), `logic.set` (`{{vars.*}}`); per-block run settings: retries, delay, and an `error` output |
| C | Cron schedules with a time zone (`app/automation/cron.py`, no new dependency) and a live preview; `trigger.webhook`, with its secret token in its own column (admins only) and rate limiting |
| D | Test this block (a dry run that is rolled back); Run now with input; version history with compare and restore (`workflow_versions`); export/import; run filters; Run again |
| E | `http.request` (policy-checked; GET needs no approval); `flow.call_workflow`; `flow.for_each` (≤ 100 rows, one at a time) |

Differences from the plan:
- **Incidents:** automatic incidents stop only for DAGs whose DAG workflow starts on *failed*; SLA incidents are unchanged. See Phase 0.
- **Templates:** there is no separate Templates page; templates stay in the editor.
- **Export/import:** done in the browser. Export downloads the workflow's JSON; import reuses `POST /automation/workflows`.
- **HTTP block:** uses the same host allowlist as notification webhooks (`AUTOMATION_WEBHOOK_ALLOWED_HOSTS`).
