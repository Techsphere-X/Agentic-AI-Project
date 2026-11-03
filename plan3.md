# Plan 3: Visual Canvas (n8n + draw.io style)

## 1. Overview & Objective

Plan 2 made incidents trigger automation workflows, stored as graphs of typed nodes. They can only be viewed as a read-only list, and monitoring is still configured in settings tables. Plan 3 puts both on a **drag-and-drop canvas**:

1. **Workflow editor**: build and edit automation workflows visually, n8n style. Drag blocks from a palette, wire output ports to the next block, and configure each block in a side panel. The server validates on save and problems are highlighted on the blocks.
2. **Pipeline canvas**: see and configure monitoring as connected blocks, draw.io style: **Airflow connection → DAG → monitor blocks → automations**. Attach a *Failure monitor* or *Freshness SLA* block to a DAG by dropping it on the canvas and connecting it. The canvas also shows which automation workflows each monitor feeds.
3. **Run replay**: open any automation run on the canvas to see the path it took. Executed steps are colored by result and the branches it followed are highlighted.

Target outcome: **an Admin can build a new workflow from scratch on the canvas, validate it, save it, and turn it on; an Operator can attach monitors to pipelines by drawing blocks; everyone can replay a run visually. The canvas reads and writes the same records as the APIs from Plans 0–2.**

### Out of Scope (deferred)
- New monitor types (data-quality checks such as row count, null rate and schema drift) and non-Airflow sources: **Plan 4**. The pipeline canvas is designed so they become new block types.
- Real-time collaboration, comments on the canvas, undo history beyond the current session.
- Slack/email blocks, stuck-run detector, maintenance windows.

### Key Decisions
| Decision | Choice | Rationale |
|---|---|---|
| Canvas library | React Flow (`@xyflow/react` 12, MIT) | The standard base for n8n-like React editors: pan/zoom, typed handles, minimap, selection, keyboard delete. |
| Block catalog | Driven by `GET /automation/node-types` (ports + JSON schema) | New backend node types appear in the palette with a working settings form, no UI change needed. |
| Settings forms | Generated from each node's Pydantic JSON schema | One generic form for every block; the server stays the source of truth for validation. |
| Positions | Optional `position {x, y}` on graph nodes, preserved by validation | Layouts survive saves; graphs without positions (templates) are auto-laid out. |
| Validation | Client checks while editing (port already used, edge into trigger, self-loop); server `POST /automation/workflows/validate` + save are authoritative | Instant feedback plus one source of truth. |
| Pipeline canvas model | A **view over existing records**: connections, DAGs (`is_monitored`), `sla_minutes`, plus a new `detect_failures` flag | No new "block" tables; the settings pages and the canvas edit the same fields. |
| Failure vs SLA monitors | New `monitored_dags.detect_failures` (default true); a DAG is polled when it has any monitor | Lets a DAG have only an SLA monitor, which the canvas needs to express. |
| Permissions | Workflow editor: Admin edits, others read-only. Pipeline canvas: Operator+ edits (same as the DAG API) | Unchanged from Plans 0–2. |
| Delete workflow | Allowed only when it has no runs (409 otherwise; disable it instead) | Run history is audit evidence. |

---

## 2. Architecture & Directory Layout (new/changed only)

```text
backend/app/
├── automation/graph.py          # + node `position`, kept by to_raw()
├── models/airflow.py            # + MonitoredDag.detect_failures
├── services/
│   ├── automation_service.py    # + delete_workflow, validate-only
│   ├── detection_service.py     # failure rule only when detect_failures
│   └── incident_service.py      # + open_counts()
└── api/v1/
    ├── automation.py            # + POST /workflows/validate, DELETE /workflows/{id}
    ├── incidents.py             # + GET /incidents/open-counts
    └── airflow.py               # DAG PATCH accepts detect_failures

frontend/src/
├── components/flow/
│   ├── FlowCanvas.css           # canvas + block styles (light/dark)
│   ├── BlockNode.jsx            # workflow block: icon, title, summary, input + labelled output ports
│   ├── SchemaForm.jsx           # settings form from JSON schema
│   ├── graph.js                 # graph <-> React Flow nodes/edges, client checks
│   └── layout.js                # layered auto-layout
├── pages/automation/
│   ├── WorkflowEditor.jsx       # NEW: palette | canvas | settings panel; validate/save
│   ├── Workflows.jsx            # + New workflow, Edit on canvas, Delete
│   └── RunDetail.jsx            # + canvas replay tab
├── pages/pipelines/
│   ├── PipelineCanvas.jsx       # NEW: connection → DAG → monitors → automations
│   └── pipelineNodes.jsx        # NEW: node components for the pipeline canvas
└── layouts/AppLayout.jsx        # + "Pipelines" nav entry
```

---

## 3. Core Components Breakdown

### 3.1. Workflow Graph Additions
- A node may carry `position: {x: number, y: number}` (finite, within ±100000). Validation keeps it, and `to_raw()` writes it back. Graphs without positions stay valid.
- `POST /automation/workflows/validate` with body `{graph}` returns `{valid, problems[], graph}` (the graph with defaults applied). It never saves.
- `DELETE /automation/workflows/{id}` (Admin) returns `409 workflow_has_runs` if any run references the workflow. Otherwise it deletes and audits `workflow.delete`.

### 3.2. Workflow Editor (`/automation/workflows/:id` and `/automation/workflows/new`)

```text
┌────────────┬──────────────────────────────────────────┬──────────────────┐
│ Palette    │  Canvas (pan / zoom / minimap)           │ Settings         │
│ ⚡ Triggers │   [⚡ When incident…] ──next──▶ [🔎 Why] │ Name             │
│ ⋔ Logic    │                         ──▶ [⋔ Only if] ─┤ Config form      │
│ ✋ Approval │                    true ▶ … false ▶ …    │ (from schema)    │
│ ⚙ Actions  │                                          │ Delete block     │
│ ✓ Verify   │                                          │                  │
│ ✉ Output   │                                          │ Problems (n)     │
└────────────┴──────────────────────────────────────────┴──────────────────┘
 Toolbar: name · description · Auto-layout · Validate · Save · enabled/mode
```

- **Palette**: grouped by stage from the node-type catalog. Drag onto the canvas or click to add. A workflow has one trigger: picking another trigger replaces it in place (same id and links). *(Since Plan 5: "Start when…" lists "When this DAG runs" with the real DAGs under it, then "Incidents (catch-all)" and "Orchestration"; a new workflow first asks "How should this workflow start?".)*
- **Block**: category colour and icon, title (custom name or type label), config summary, one input handle (not on triggers), and one labelled output handle per port (`true`/`false`, `approved`/`rejected`, `success`/`failed`, …).
- **Connecting**: drag from an output port to a block.
  - A port can have only one outgoing edge; a new connection replaces the old one.
  - No edges into triggers and no self-loops.
  - Cycles are rejected client-side, and by the server again on save.
- **Editing**:
  - Select a block to edit its name and config in the side panel.
  - `Delete`/`Backspace` removes the selected blocks or edges.
  - The block ID is generated (`<type>_<n>`) and shown read-only.
- **Validate / Save**: server problems are listed in the panel. The blocks they mention get a red outline; click a problem to select its block.
  - Save on a new workflow creates it disabled.
  - Save on an existing workflow PATCHes the graph, which bumps the version.
  - Unsaved changes show a badge, and the browser warns before leaving.
- **Auto-layout**: a layered left-to-right layout (depth from the trigger; branches spread vertically). Used automatically when nodes have no positions.
- **Read-only** for non-Admins: no palette, no dragging or connecting, and the form is disabled.

### 3.3. Settings Form from JSON Schema (`SchemaForm.jsx`)

| Schema shape | Control |
|---|---|
| `boolean` | toggle |
| `integer` (+ `minimum`/`maximum`) | number input |
| `string` (+ `maxLength`) | text input; `message`/`note` → textarea |
| `enum` / `$ref` to an enum / `anyOf [enum, null]` | select (with "— none —" when nullable) |
| `array` of enum (or `$ref` enum) | checkbox group |
| `array` of string | comma-separated text |
| `anyOf [integer, null]` | number input, empty = null |

Field labels come from the property name (`max_occurrences` → "Max occurrences"). Defaults come from the schema. Values are stored as the graph's `config`.

### 3.4. Pipeline Canvas (`/pipelines`)

```text
[Airflow: prod-airflow ●]──▶[DAG orders_pipeline]──▶[⚠ Failure monitor  2 open]──feeds──▶[Auto-retry transient]
                         └─▶[DAG daily_etl     ]──▶[⏱ SLA 60 min       0 open]──feeds──▶[Re-run on SLA miss]
                         └─▶[DAG legacy (not monitored, faded)]
```

- **Columns** are laid out automatically: connections → DAGs → monitors → enabled automation workflows. A toggle shows or hides unmonitored DAGs.
- **Monitor blocks**: *Failure monitor* (`detect_failures`) and *Freshness SLA* (`sla_minutes`). Each shows its open-incident count, and clicking it opens the incidents filtered to that DAG.
- **Draw to configure** (Operator+):
  1. Drag a monitor from the palette onto the canvas. It appears as an unattached draft.
  2. Connect a DAG to it. This saves: `PATCH /airflow/dags/{id}` with `is_monitored: true` plus `detect_failures: true` or `sla_minutes` (default 60).
  3. Select an SLA block to change its minutes in the side panel.
  4. Delete a monitor block to detach it. The DAG stops being monitored when it has no monitors left.
- **Automations column**: enabled workflows with a `trigger.incident` whose incident types match a monitor's type (failure → `DAG_RUN_FAILED`, SLA → `SLA_MISSED`). If the workflow's first filter restricts `dag_ids`, only those DAGs link to it. Edges are read-only "feeds" links; clicking a workflow opens the editor.
- **Stale-incident workflows** (`trigger.incident_stale`) feed every monitor, shown as dashed links.

### 3.5. Run Replay (Run detail → "Canvas" tab)
- The run's own graph snapshot, read-only.
- Executed steps are coloured by status and port (success / warning for `false`/`rejected`/`failed` / danger for errors); the current waiting step pulses.
- Edges the run followed are animated and highlighted; untouched blocks are faded.

### 3.6. Backend Additions

| Method | Endpoint | Description | Access |
|---|---|---|---|
| `POST` | `/api/v1/automation/workflows/validate` | `{graph}` → `{valid, problems, graph}` | Viewer+ |
| `DELETE` | `/api/v1/automation/workflows/{id}` | 204; 409 `workflow_has_runs` | Admin |
| `GET` | `/api/v1/incidents/open-counts` | `[{connection_id, dag_id, type, count}]` for open incidents | Viewer+ |
| `PATCH` | `/api/v1/airflow/dags/{id}` | also accepts `detect_failures` | Operator+ |

Schema: `monitored_dags.detect_failures BOOLEAN NOT NULL DEFAULT true` (migration, server default, so existing monitored DAGs keep detecting failures). Detection: the failure rule runs only when `detect_failures`; SLA and recovery are unchanged. Audit: `monitored_dag.failure_monitor` on change, `workflow.delete`.

---

## 4. Implementation Steps & Milestones

### Phase 1: Backend
- [x] `position` in the graph; validate endpoint; delete workflow; `detect_failures` + migration + detection; open-counts endpoint.
- **Tests**: positions round-trip and bad positions are rejected; validate returns problems without saving; delete → 204, 409 with runs, 403 for non-Admin; `detect_failures=false` skips failure incidents but keeps SLA; migration round trip; open-counts groups correctly; PATCH audits.

### Phase 2: Canvas Foundation
- [x] `@xyflow/react`; `graph.js` (convert, client checks), `layout.js`, `BlockNode.jsx`, `SchemaForm.jsx`, canvas CSS (light/dark).

### Phase 3: Workflow Editor
- [x] `WorkflowEditor.jsx` (palette, drag/drop, connect rules, settings panel, validate/save, problems, auto-layout, read-only mode); Workflows list: New / Edit / Delete.

### Phase 4: Pipeline Canvas & Run Replay
- [x] `PipelineCanvas.jsx` (columns, draw-to-attach monitors, SLA editing, incident counts, feeds links); nav entry.
- [x] Run detail canvas tab.

### Phase 5: Verification
- [x] `npm run lint`, `npm run build`, `npm run test:canvas`; backend suite.
- [ ] A browser check of the editor, the pipeline canvas and a run replay (not yet done, see §6).

---

## 5. Definition of Done for Plan 3
1. **Tests**: the new backend tests pass; the Plan 0–2 suites still pass; `ruff` and `oxlint` are clean; the frontend builds.
2. **Workflow editor**: an Admin builds *trigger → classify → filter(network glitch) → approval → retry → verify → resolve* from an empty canvas, saves it, reopens it with the same layout, turns it on, and it runs like a template. Invalid wiring is blocked or highlighted. Viewers see it read-only.
3. **Pipeline canvas**: an Operator attaches a Failure monitor and an SLA monitor to DAGs by drawing. Detection then behaves accordingly, and the Monitored DAGs settings page shows the same state. Monitors show open-incident counts and link to the workflows they feed.
4. **Run replay**: a completed run and a waiting run both show their path on the canvas.

**Hand-off to Plan 4**: data-quality monitors (row count, null rate, freshness column, schema drift) become new monitor blocks on the pipeline canvas, with a database-source block alongside the Airflow connection.

---

## 6. Implementation Notes (as built)

- **Block component location:** `BlockNode.jsx` lives in `pages/automation/` (not `components/flow/`) because it uses the automation labels. The generic pieces (`graph.js`, `layout.js`, `SchemaForm.jsx`, `FlowCanvas.css`) are in `components/flow/`.
- **Layout-only saves:** moving blocks updates the stored graph but does **not** bump the workflow version. Only behaviour changes (blocks, settings, wiring) do.
- **Pipeline canvas state:** it rebuilds from the API after every change and keeps positions you dragged, for the current visit only (positions are not saved). Deleting both monitors of a DAG in one gesture sends a single "stop monitoring" update.
- **Duplicate:** `/automation/workflows/new?from=<id>` opens a copy of a workflow in the editor; it is created on save.
- **Lazy loading:** React Flow is code-split, so the workflow editor, pipeline canvas and run replay download it on first use. The main bundle stays about the same size.
- **Canvas logic tests:** `npm run test:canvas` bundles `components/flow/canvas.test.js` with Vite and runs it with `node:test`. It covers graph conversion, connection rules, layout, monitor attach/detach bodies, workflow "feeds" and the pipeline diagram. No extra test dependency.
- **Not yet done:** the Definition of Done asks for a browser check of the editor, the pipeline canvas and a run replay. No browser was available in the implementing session, so the UI was verified by lint, build, logic tests and the backend API tests only.
