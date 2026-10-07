# Graph Report - .  (2026-09-28)

## Corpus Check
- 172 files · ~92,806 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1858 nodes · 5408 edges · 79 communities (76 shown, 3 thin omitted)
- Extraction: 84% EXTRACTED · 16% INFERRED · 0% AMBIGUOUS · INFERRED: 875 edges (avg confidence: 0.62)
- Token cost: 44,157 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Workflow Graph Validation|Workflow Graph Validation]]
- [[_COMMUNITY_Fake Airflow Test Server|Fake Airflow Test Server]]
- [[_COMMUNITY_Errors & Airflow Service|Errors & Airflow Service]]
- [[_COMMUNITY_Automation API Routes|Automation API Routes]]
- [[_COMMUNITY_Database Connections API|Database Connections API]]
- [[_COMMUNITY_Notification Channels API|Notification Channels API]]
- [[_COMMUNITY_Automation Nodes & Templating|Automation Nodes & Templating]]
- [[_COMMUNITY_Project Plans & Infra|Project Plans & Infra]]
- [[_COMMUNITY_Engine & Loop Tests|Engine & Loop Tests]]
- [[_COMMUNITY_Detection & Incidents|Detection & Incidents]]
- [[_COMMUNITY_Auth & Rate Limiting|Auth & Rate Limiting]]
- [[_COMMUNITY_Log Classifier Diagnosis|Log Classifier Diagnosis]]
- [[_COMMUNITY_Workflow Graph Catalog|Workflow Graph Catalog]]
- [[_COMMUNITY_Airflow HTTP Client|Airflow HTTP Client]]
- [[_COMMUNITY_Frontend Canvas Graph Logic|Frontend Canvas Graph Logic]]
- [[_COMMUNITY_Airflow Connections API|Airflow Connections API]]
- [[_COMMUNITY_Orchestration Tests|Orchestration Tests]]
- [[_COMMUNITY_Frontend App Routing|Frontend App Routing]]
- [[_COMMUNITY_Notification Channel Tests|Notification Channel Tests]]
- [[_COMMUNITY_Frontend UI Kit|Frontend UI Kit]]
- [[_COMMUNITY_App Layout & Auth Context|App Layout & Auth Context]]
- [[_COMMUNITY_Auth Tests & Users|Auth Tests & Users]]
- [[_COMMUNITY_Settings & Config|Settings & Config]]
- [[_COMMUNITY_Forms & Login UI|Forms & Login UI]]
- [[_COMMUNITY_Database Connection Tests|Database Connection Tests]]
- [[_COMMUNITY_Frontend Dependencies|Frontend Dependencies]]
- [[_COMMUNITY_Pipeline Canvas UI|Pipeline Canvas UI]]
- [[_COMMUNITY_Incident UI|Incident UI]]
- [[_COMMUNITY_Plan 1 API Tests|Plan 1 API Tests]]
- [[_COMMUNITY_Run Canvas & Block UI|Run Canvas & Block UI]]
- [[_COMMUNITY_Detection Rules|Detection Rules]]
- [[_COMMUNITY_Connection Monitor Tests|Connection Monitor Tests]]
- [[_COMMUNITY_Airflow Adapter Base|Airflow Adapter Base]]
- [[_COMMUNITY_Automation Safety Policy|Automation Safety Policy]]
- [[_COMMUNITY_Mock Airflow Adapter|Mock Airflow Adapter]]
- [[_COMMUNITY_Test Fixtures|Test Fixtures]]
- [[_COMMUNITY_Detection Scheduler Lease|Detection Scheduler Lease]]
- [[_COMMUNITY_Mock Write Simulation|Mock Write Simulation]]
- [[_COMMUNITY_Plan 2 API Tests|Plan 2 API Tests]]
- [[_COMMUNITY_Schema Form UI|Schema Form UI]]
- [[_COMMUNITY_Evidence & Secret Scrubbing|Evidence & Secret Scrubbing]]
- [[_COMMUNITY_Connection Monitor Service|Connection Monitor Service]]
- [[_COMMUNITY_Airflow Adapter Interface|Airflow Adapter Interface]]
- [[_COMMUNITY_DB Session & Migrations Env|DB Session & Migrations Env]]
- [[_COMMUNITY_App Factory|App Factory]]
- [[_COMMUNITY_Built-in Workflow Recipes|Built-in Workflow Recipes]]
- [[_COMMUNITY_Airflow DTOs|Airflow DTOs]]
- [[_COMMUNITY_Audit Service|Audit Service]]
- [[_COMMUNITY_Admin Bootstrap|Admin Bootstrap]]
- [[_COMMUNITY_Lint Config|Lint Config]]
- [[_COMMUNITY_Live Test DAGs|Live Test DAGs]]
- [[_COMMUNITY_E2E Test Server|E2E Test Server]]
- [[_COMMUNITY_Favicon|Favicon]]

## God Nodes (most connected - your core abstractions)
1. `User` - 124 edges
2. `NodeContext` - 61 edges
3. `AirflowConnection` - 47 edges
4. `Clock` - 47 edges
5. `get_settings()` - 45 edges
6. `Incident` - 41 edges
7. `FakeAirflow` - 40 edges
8. `NodeResult` - 37 edges
9. `WorkflowRun` - 36 edges
10. `RunStatus` - 35 edges

## Surprising Connections (you probably didn't know these)
- `Test DAGs (agentic_flaky_once, agentic_bad_data)` --semantically_similar_to--> `Mock DAG partner_api_sync (transient 503)`  [INFERRED] [semantically similar]
  plan4.md → plan2.md
- `Frontend index.html (Agentic Data Automation)` --conceptually_related_to--> `Agentic Data Automation (project.md)`  [INFERRED]
  frontend/index.html → project.md
- `PyMySQL` --conceptually_related_to--> `Data-Quality Monitoring`  [AMBIGUOUS]
  backend/requirements.txt → project.md
- `Automation Policy (check_action)` --implements--> `Safety Requirements (policy checks, approvals, audit, dry-run)`  [INFERRED]
  plan2.md → project.md
- `test_recipients_must_be_email_addresses()` --calls--> `validate_graph()`  [INFERRED]
  backend/tests/test_notification_channels.py → backend/app/automation/graph.py

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Detect-Diagnose-Approve-Act-Verify Incident Lifecycle** — plan1_detection_cycle, plan2_log_classifier, plan2_approvals, plan2_airflow_write_path, plan2_automation_engine, plan1_incident_model [INFERRED 0.85]
- **Automation Safety Controls** — plan2_policy_layer, plan2_approvals, plan2_dry_run, plan2_rate_limit, plan0_audit_log [EXTRACTED 1.00]
- **Lease-guarded Background Loops** — plan1_detection_lease, plan4_automation_loop, backend_readme_connection_monitor [INFERRED 0.85]

## Communities (79 total, 3 thin omitted)

### Community 0 - "Workflow Graph Validation"
Cohesion: 0.06
Nodes (92): Validate a raw graph and return it with defaults applied. Raises GraphError., Serialize a validated graph (defaults applied) back to JSON form., to_raw(), validate_graph(), TriggerEvent, get_settings(), get_current_user(), DbSession (+84 more)

### Community 1 - "Fake Airflow Test Server"
Cohesion: 0.06
Nodes (82): FakeAirflow, Exception, MockTransport, Request, Response, raising(), Scriptable fake Airflow REST API for httpx.MockTransport., static() (+74 more)

### Community 2 - "Errors & Airflow Service"
Cohesion: 0.06
Nodes (75): AppError, BadRequestError, _error_body(), PermissionDeniedError, Any, FastAPI, RateLimitedError, An external system (e.g. Airflow) failed. (+67 more)

### Community 3 - "Automation API Routes"
Cohesion: 0.09
Nodes (77): approve(), cancel_run(), create_workflow(), _decide(), delete_workflow(), get_run(), get_workflow(), list_approvals() (+69 more)

### Community 4 - "Database Connections API"
Cohesion: 0.06
Nodes (78): create_connection(), delete_connection(), get_connection(), list_connections(), AdminUser, ClientIp, DbSession, OperatorUser (+70 more)

### Community 5 - "Notification Channels API"
Cohesion: 0.06
Nodes (69): create_channel(), delete_channel(), get_channel(), list_channels(), AdminUser, ClientIp, DbSession, OperatorUser (+61 more)

### Community 6 - "Automation Nodes & Templating"
Cohesion: 0.09
Nodes (60): Node, _lookup(), Any, Tiny, safe `{{dotted.key}}` templating for notification titles and messages.  On, Replace `{{a.b}}` with values["a"]["b"]; unknown keys render as an empty string., render(), _action_environment(), ActionFailed (+52 more)

### Community 7 - "Project Plans & Infra"
Cohesion: 0.05
Nodes (74): Backend README, Airflow Connection Monitor (is_live), Backend Runtime Requirements, Alembic, cryptography, Backend Dev Requirements (pytest, ruff), FastAPI, httpx (+66 more)

### Community 8 - "Engine & Loop Tests"
Cohesion: 0.11
Nodes (65): audit_actions(), Clock, detect(), enable(), events(), make_env(), make_incident(), only() (+57 more)

### Community 9 - "Detection & Incidents"
Cohesion: 0.07
Nodes (60): detection_status(), CycleSummary, DbSession, OperatorUser, Request, ViewerUser, run_detection(), _scheduler() (+52 more)

### Community 10 - "Auth & Rate Limiting"
Cohesion: 0.06
Nodes (51): _cookie_path(), login(), logout(), me(), ClientIp, DbSession, Depends, Response (+43 more)

### Community 11 - "Log Classifier Diagnosis"
Cohesion: 0.07
Nodes (49): classify(), classify_logs(), Diagnosis, _line_at(), Deterministic failure classification from task log text.  Pure: no database, HTT, Classify a failure from its log text (first matching rule wins)., Classify the first log (newest first) that yields a known category., _Rule (+41 more)

### Community 12 - "Workflow Graph Catalog"
Cohesion: 0.08
Nodes (40): _AirflowTarget, ApprovalConfig, catalog(), _check_acyclic_and_reachable(), CheckDataConfig, ClearTasksConfig, _Config, _DatabaseTarget (+32 more)

### Community 13 - "Airflow HTTP Client"
Cohesion: 0.16
Nodes (24): AsyncClient, ApiVersion, _elapsed_ms(), _flatten_log(), _format_date(), _is_tls_error(), _json_or_none(), LiveAirflowAdapter (+16 more)

### Community 14 - "Frontend Canvas Graph Logic"
Cohesion: 0.09
Nodes (31): retryGraph, connect(), connectionProblem(), defaultConfig(), edgeId(), fromFlow(), isTrigger(), newEdge() (+23 more)

### Community 15 - "Airflow Connections API"
Cohesion: 0.14
Nodes (36): create_connection(), delete_connection(), get_connection(), list_connections(), list_dags(), AdminUser, ClientIp, DbSession (+28 more)

### Community 16 - "Orchestration Tests"
Cohesion: 0.19
Nodes (34): airflow(), chain(), Clock, count(), database(), datetime, MonkeyPatch, Path (+26 more)

### Community 17 - "Frontend App Routing"
Cohesion: 0.10
Nodes (26): App(), WorkflowEditor, RedirectIfAuthenticated(), RequireAuth(), RequireRole(), AuthProvider(), useAuth(), ToastContext (+18 more)

### Community 18 - "Notification Channel Tests"
Cohesion: 0.13
Nodes (27): _channel(), create(), FakeSMTP, Http, MonkeyPatch, Request, Response, Session (+19 more)

### Community 19 - "Frontend UI Kit"
Cohesion: 0.14
Nodes (26): Badge(), Button(), Card(), EmptyState(), STATUS_LABELS, STATUS_TONES, Toggle(), formatDateTime() (+18 more)

### Community 20 - "App Layout & Auth Context"
Cohesion: 0.11
Nodes (22): NavIcon(), PATHS, AuthContext, AppLayout(), NAV, readCollapsed(), Dashboard(), SEVERITIES (+14 more)

### Community 21 - "Auth Tests & Users"
Cohesion: 0.18
Nodes (29): User, actions(), MonkeyPatch, Session, TestClient, Phase 2: authentication, tokens, RBAC, user management, bootstrap., test_access_token_cannot_be_used_as_refresh_token(), test_admin_can_be_demoted_when_another_admin_exists() (+21 more)

### Community 22 - "Settings & Config"
Cohesion: 0.12
Nodes (22): Settings, _alembic_config(), _conn(), Path, Session, Phase 1: configuration safety, migrations and database constraints., _round_trip(), test_audit_details_redact_secrets() (+14 more)

### Community 23 - "Forms & Login UI"
Cohesion: 0.13
Nodes (18): Alert(), Field(), Modal(), Login(), ConnectionForm(), EMPTY, TestResult(), FILTERS (+10 more)

### Community 24 - "Database Connection Tests"
Cohesion: 0.20
Nodes (23): _conn(), conn_id(), FakeServer, MonkeyPatch, Session, TestClient, UUID, Database connections in the catalog: CRUD, RBAC, probing, monitor cycle and heal (+15 more)

### Community 25 - "Frontend Dependencies"
Cohesion: 0.08
Nodes (23): dependencies, react, react-dom, react-router, @xyflow/react, devDependencies, oxlint, @playwright/test (+15 more)

### Community 26 - "Pipeline Canvas UI"
Cohesion: 0.12
Nodes (18): StatusPill(), Canvas(), EDGE_OPTIONS, MONITORS, NODE_TYPES, Panel(), attachBody(), buildPipeline() (+10 more)

### Community 27 - "Incident UI"
Cohesion: 0.12
Nodes (15): EVENT_LABELS, IncidentDetail(), RunDetails(), TABS, Timeline(), IncidentList(), STATUS_FILTERS, describeCycle() (+7 more)

### Community 28 - "Plan 1 API Tests"
Cohesion: 0.18
Nodes (19): by_dag(), clock(), MockClock, datetime, MonkeyPatch, Session, TestClient, Plan 1 / Phase 4: incident + detection API, state machine, RBAC, and the E2E moc (+11 more)

### Community 29 - "Run Canvas & Block UI"
Cohesion: 0.13
Nodes (18): BY_HAND_TRIGGERS, CATEGORY_LABELS, describeConfig(), NODE_ICONS, nodeIcon(), shortSql(), startsByHand(), TRIGGER_EVENTS (+10 more)

### Community 30 - "Detection Rules"
Cohesion: 0.22
Nodes (19): count_recent_failures(), detect_failed_runs(), detect_sla_miss(), find_recoveries(), Finding, is_sla_breached(), latest_success(), next_watermark() (+11 more)

### Community 31 - "Connection Monitor Tests"
Cohesion: 0.21
Nodes (18): airflow(), _conn(), conn_id(), MonkeyPatch, Request, Response, Session, TestClient (+10 more)

### Community 32 - "Airflow Adapter Base"
Cohesion: 0.13
Nodes (12): AirflowAdapterError, ConnectionTestResult, describe_status(), Airflow adapter contract: enums, DTOs and the protocol every adapter implements., Raised by adapter operations (other than probe) when Airflow cannot be used., Tail of the task log, at most max_bytes., Keep the last max_bytes bytes of text; returns (text, truncated)., User-facing, actionable message for each status (see plan0.md §3.3). (+4 more)

### Community 33 - "Automation Safety Policy"
Cohesion: 0.17
Nodes (15): check_action(), PolicyDecision, Safety policy for automation actions. Pure and deterministic.  The policy is enf, Decide whether `action` may run against a DAG in `environment`., check(), mutate(), _problems(), Any (+7 more)

### Community 34 - "Mock Airflow Adapter"
Cohesion: 0.29
Nodes (4): AirflowDagSummary, MockAirflowAdapter, Any, Clock

### Community 35 - "Test Fixtures"
Cohesion: 0.26
Nodes (16): admin(), admin_headers(), client(), db(), login(), make_user(), operator(), operator_headers() (+8 more)

### Community 36 - "Detection Scheduler Lease"
Cohesion: 0.17
Nodes (9): DetectionBusyError, DetectionScheduler, datetime, Session, sessionmaker, UUID, In-process polling loops guarded by a DB lease (one active holder per loop acros, Owns the polling loop and serializes cycles within this process.      `run_cyc (+1 more)

### Community 37 - "Mock Write Simulation"
Cohesion: 0.18
Nodes (12): _apply_writes(), _parse_simulated_status(), datetime, Mock Airflow adapter for offline development and UI testing.  Append `?simulate=, Remediation calls made against one mock connection., Forget all simulated write calls (tests)., Newest first, logical_date >= since., Overlay cleared and triggered runs on the clock-derived history. Newest first. (+4 more)

### Community 38 - "Plan 2 API Tests"
Cohesion: 0.23
Nodes (14): by_key(), partner_clock(), prod_partner(), MonkeyPatch, Session, TestClient, Plan 2 / Phase 4: automation API, RBAC, and the approval flow through HTTP., The built-in templates, seeded as on first start (disabled). (+6 more)

### Community 39 - "Schema Form UI"
Cohesion: 0.19
Nodes (13): ChannelSelect(), ConnectionSelect(), DagInput(), FieldControl(), humanize(), LONG_TEXT, normalize(), resolve() (+5 more)

### Community 40 - "Evidence & Secret Scrubbing"
Cohesion: 0.23
Nodes (12): failure_summary(), Any, Evidence records built from Airflow DTOs, with secret scrubbing for log text., run_metadata(), run_source(), scrub(), task_instances(), task_log() (+4 more)

### Community 41 - "Connection Monitor Service"
Cohesion: 0.30
Nodes (13): _adapters(), CycleSummary, _Fetch, _fetch_all(), datetime, Session, Connection monitor: keeps each active connection's health and DAG list current., Build adapters; connections whose stored config is unusable get a failure right (+5 more)

### Community 42 - "Airflow Adapter Interface"
Cohesion: 0.15
Nodes (6): AirflowAdapter, Any, None if the DAG does not exist., None if the run does not exist., Clear (retry) task instances of one run. Returns the cleared task ids., Protocol

### Community 43 - "DB Session & Migrations Env"
Cohesion: 0.27
Nodes (9): _configure_kwargs(), _database_url(), run_migrations_offline(), run_migrations_online(), create_db_engine(), create_session_factory(), Engine, Session (+1 more)

### Community 44 - "App Factory"
Cohesion: 0.31
Nodes (9): create_app(), CycleSummary, FastAPI, Session, sessionmaker, One detection cycle. A manual run ("Run detection now") also advances automation, `detection=None` follows DETECTION_ENABLED (and AUTOMATION_ENABLED for the autom, run_automation_tick() (+1 more)

### Community 45 - "Built-in Workflow Recipes"
Cohesion: 0.36
Nodes (7): _e(), _n(), Any, Built-in workflows ("recipes"), seeded disabled on first start., failed run -> classify -> filter -> approval -> clear -> verify -> resolve / esc, _retry_graph(), Template

### Community 46 - "Airflow DTOs"
Cohesion: 0.29
Nodes (3): _iso(), datetime, Date used to order runs (Airflow 3 manual runs may have no logical_date).

### Community 47 - "Audit Service"
Cohesion: 0.38
Nodes (6): Any, Session, Recursively mask values under sensitive-looking keys (flags like booleans are ke, Add an audit entry to the session. The caller's commit persists it with the chan, record(), redact()

### Community 48 - "Admin Bootstrap"
Cohesion: 0.47
Nodes (5): bootstrap_admin(), Session, sessionmaker, First-run bootstrap: create the initial admin when the users table is empty, and, run_bootstrap()

### Community 49 - "Lint Config"
Cohesion: 0.33
Nodes (5): plugins, rules, react/only-export-components, react/rules-of-hooks, $schema

## Ambiguous Edges - Review These
- `Data-Quality Monitoring` → `PyMySQL`  [AMBIGUOUS]
  backend/requirements.txt · relation: conceptually_related_to

## Knowledge Gaps
- **94 isolated node(s):** `Template`, `$schema`, `plugins`, `react/rules-of-hooks`, `react/only-export-components` (+89 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `Data-Quality Monitoring` and `PyMySQL`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **Why does `User` connect `Auth Tests & Users` to `Workflow Graph Validation`, `Errors & Airflow Service`, `Automation API Routes`, `Detection Scheduler Lease`, `Notification Channels API`, `Database Connections API`, `Test Fixtures`, `Engine & Loop Tests`, `Detection & Incidents`, `Auth & Rate Limiting`, `Connection Monitor Service`, `App Factory`, `Audit Service`, `Admin Bootstrap`, `Orchestration Tests`, `Notification Channel Tests`, `Settings & Config`?**
  _High betweenness centrality (0.240) - this node is a cross-community bridge._
- **Why does `get_settings()` connect `Workflow Graph Validation` to `Fake Airflow Test Server`, `Errors & Airflow Service`, `Automation API Routes`, `Database Connections API`, `Notification Channels API`, `Test Fixtures`, `Engine & Loop Tests`, `Detection & Incidents`, `Auth & Rate Limiting`, `DB Session & Migrations Env`, `App Factory`, `Admin Bootstrap`, `Auth Tests & Users`, `Settings & Config`, `Database Connection Tests`, `Connection Monitor Tests`?**
  _High betweenness centrality (0.109) - this node is a cross-community bridge._
- **Why does `Clock` connect `Engine & Loop Tests` to `Workflow Graph Validation`, `Errors & Airflow Service`, `Automation API Routes`, `Mock Airflow Adapter`, `Notification Channels API`, `Detection & Incidents`, `Airflow Connections API`, `Auth Tests & Users`?**
  _High betweenness centrality (0.053) - this node is a cross-community bridge._
- **Are the 30 inferred relationships involving `User` (e.g. with `Pagination` and `DetectionBusyError`) actually correct?**
  _`User` has 30 INFERRED edges - model-reasoned connections that need verification._
- **Are the 19 inferred relationships involving `NodeContext` (e.g. with `automation_nodes.py` and `Graph`) actually correct?**
  _`NodeContext` has 19 INFERRED edges - model-reasoned connections that need verification._
- **Are the 16 inferred relationships involving `AirflowConnection` (e.g. with `Base` and `TimestampMixin`) actually correct?**
  _`AirflowConnection` has 16 INFERRED edges - model-reasoned connections that need verification._