# Presentation Content: Automating Managed Services Using Agentic AI

> **Note for the slide generator (ChatGPT / Claude):**
> Each `## Slide` below is one slide. Use the **bullet points** as slide text and the
> **Speaker notes** as what the presenter says. Keep slides clean: short bullets, one diagram or
> table at most per slide. Suggested theme: dark blue / teal "operations dashboard" look.

---

## Slide 1: Title Slide

# Automating Managed Services Using Agentic AI

**An AI-powered operations platform that monitors data pipelines, detects failures, diagnoses the root cause, and fixes them safely.**

**Presented by:**
- Ayush Shahi
- Prabesh Paudel

*Speaker notes:* Good morning everyone. We are Ayush Shahi and Prabesh Paudel, and our project is "Automating Managed Services Using Agentic AI": a platform that does the routine monitoring and recovery work a data operations team normally does by hand.

---

## Slide 2: Introduction: The Three Key Ideas

### What is an AI Agent?
- A software system that **observes** its environment, **reasons** about what it sees, **decides** on an action, and **acts**, then checks the result.
- Unlike a chatbot, an agent does not just answer questions. It carries out a loop: *Observe → Think → Act → Verify*.

### What is Automation?
- Replacing repetitive manual steps with software that runs them reliably, 24/7.
- Examples: retrying a failed job, sending an alert, pausing a broken pipeline.

### What is a Managed Service in Data Monitoring?
- Companies run hundreds of **data pipelines** (e.g., on Apache Airflow) that move and transform data every day.
- A **managed service team** watches these pipelines, responds to failures, checks data quality, and keeps service-level agreements (SLAs).
- Today, most of this is done by **humans reading logs at 3 AM**.

*Speaker notes:* Our project combines these three ideas. We take the managed-service work (watching pipelines and responding to incidents), automate the repetitive parts, and use AI agents for the parts that need judgment, such as reading an error log and deciding what went wrong.

---

## Slide 3: The Problem

- Data pipelines fail often: network outages, timeouts, out-of-memory errors, bad data, schema changes, expired credentials.
- An engineer has to:
  1. Notice the failure
  2. Open the logs and find the real error
  3. Decide what kind of problem it is
  4. Decide whether a retry is safe
  5. Fix it, check that it worked, and inform the team
- This is **slow, repetitive, error-prone**, and it does not scale.
- Blindly retrying everything is dangerous: retrying a job with **bad data** or a **code bug** wastes resources and can corrupt data.

*Speaker notes:* The key insight is that most failures fall into a small number of categories, and each category has a known correct response. A network glitch should be retried. A duplicate-key error should never be retried; a person must look at it. That is what we automate.

---

## Slide 4: Objectives

1. **Continuous monitoring** of Airflow pipelines and database connections.
2. **Automatic detection** of task failures and SLA (deadline) breaches, turned into tracked incidents with evidence.
3. **Intelligent diagnosis** of the root cause using a hybrid of rule-based (regex) classification and a fine-tuned AI model (DistilBERT).
4. **Safe, automatic remediation**: retry, rerun, wait, pause, or escalate, chosen by explainable rules with optional AI advice.
5. **Human-in-the-loop safety**: approvals for production, daily action limits, dry-run mode, and a full audit trail.
6. **Verification**: after every fix, confirm the pipeline actually recovered.
7. **Alerting** through Email, Slack, Microsoft Teams, and webhooks.
8. **Visual, no-code workflow builder** so operators can design their own automation, with an AI assistant to help.
9. **Privacy and low cost**: AI models run **locally** (Ollama, local GPU) so logs never leave the company.

*Speaker notes:* Our guiding principle: this is not a generic chatbot or a CRUD app. It is a full incident lifecycle: detect, diagnose, decide, approve, act, verify, and record.

---

## Slide 5: System Architecture

```
┌──────────────────────┐      ┌────────────────────────────────┐      ┌─────────────────────┐
│  React Frontend      │◄────►│  FastAPI Backend (Control Plane)│◄────►│  Apache Airflow 2/3 │
│  Dashboard, Canvas,  │      │  Detection · Diagnosis ·        │      │  (pipelines / DAGs) │
│  Incidents, Workflows│      │  Automation Engine · Policy ·   │      └─────────────────────┘
└──────────────────────┘      │  Audit · Notifications          │      ┌─────────────────────┐
                              └───────┬───────────────┬────────┘◄────►│ PostgreSQL / MySQL  │
                                      │               │               │ (monitored DBs)     │
                        ┌─────────────▼───┐   ┌───────▼─────────┐     └─────────────────────┘
                        │ ML Service (GPU)│   │ Local LLM       │     ┌─────────────────────┐
                        │ DistilBERT log  │   │ (Ollama)        │     │ Email · Slack ·     │
                        │ classifier      │   │ fix advisor &   │     │ Teams · Webhook     │
                        └─────────────────┘   │ workflow helper │     └─────────────────────┘
                                              └─────────────────┘
```

**Tech stack:** Python · FastAPI · SQLAlchemy + Alembic · PostgreSQL · React 19 + Vite · React Flow · Apache Airflow · PyTorch / DistilBERT · Ollama · Docker Compose · Playwright · Pytest

*Speaker notes:* The backend is the control plane. It talks to Airflow, to the databases, to the AI services, and to the alert channels. Business logic is kept independent of the frameworks, so each part can be tested on its own.

---

## Slide 6: Core Workflow: The Agent Loop

```
Monitor pipelines
   → Detect failure / SLA breach
   → Collect evidence (runs, task details, logs)
   → Diagnose root cause (Regex + AI)
   → Choose the fix (rules + optional LLM advice)
   → Safety policy check / human approval
   → Execute action on Airflow
   → Verify recovery
   → Update incident, notify team, write audit log
```

*Speaker notes:* This is the agent loop from our introduction, applied to data operations. Every step leaves evidence behind, so every decision can be explained afterwards.

---

## Slide 7: Feature: Airflow Connection & Pipeline Monitoring

- Connect any **Apache Airflow 2.x or 3.x** instance; the version is detected automatically.
- **Connection catalog:** add, test, and manage connections from the UI, with a health status for each.
- **Connection monitor:** a background service keeps each connection's health and DAG list current, checking all connections concurrently so one slow server does not delay the others.
- **Monitored DAG registry:** choose which pipelines to watch and set freshness SLAs.
- **Pipeline Canvas:** a visual map of Airflow connection → DAG → Failure / SLA monitor → the automations they feed.
- A **mock Airflow adapter** lets the full system run as a demo without a real Airflow.

*Speaker notes:* We tested this against real Airflow 2.10 and 3.1 running in Docker, using test DAGs that fail on purpose: one with a network error that a retry fixes, and one with bad data that must not be retried.

---

## Slide 8: Feature: Database Connections

- Connect **PostgreSQL** and **MySQL** databases from the connection catalog.
- Test a connection before saving it; SSL modes are supported.
- **Scheduled health checks** run in the same cycle as the Airflow monitor.
- Credentials are **encrypted at rest**, and private or unsafe hosts are blocked.
- **Data reconciliation workflows:** compare two databases by row count or row by row and report the differences.
- **Airflow-to-database reconciliation:** after a DAG run, check that the data actually arrived.

*Speaker notes:* A pipeline can report "success" and still deliver wrong or missing data. Database connections let us check the data itself, not just the job status.

---

## Slide 9: Feature: Detection & Incident Management

- **Failure detection:** a failed DAG run becomes an incident automatically.
- **SLA detection:** a pipeline that has not delivered fresh data on time raises an SLA-breach incident.
- **Evidence collection:** DAG runs, task instances, and log tails, with **secrets scrubbed** (passwords and tokens removed).
- **Deduplication:** repeated failures of the same pipeline update one incident instead of creating spam.
- **Auto-resolve:** when the pipeline recovers, the incident closes itself.
- **Severity scoring** and a complete event timeline for every incident.
- A lease-guarded background scheduler, so multiple server instances never process the same work twice.

*Speaker notes:* Every incident page shows what happened, the evidence, the diagnosis, every action taken, and the final outcome.

---

## Slide 10: Feature: AI-Powered Hybrid Diagnosis

**Two-stage diagnosis of failure logs into 9 categories:**
`AUTH · DATA_INTEGRITY · SCHEMA · CODE_BUG · RESOURCE · TIMEOUT · TRANSIENT_NETWORK · UPSTREAM_MISSING · UNKNOWN`

1. **Stage 1: Regex rules.** Fast, explainable, high confidence for known patterns.
2. **Stage 2: Fine-tuned DistilBERT model** on a local GPU service, used **only when regex is unsure** (UNKNOWN / generic CODE_BUG).
3. **Hybrid decision rule:** the AI result is used only when its confidence is **≥ 85%**; otherwise it is shown as a *suggestion*.
4. **Safety:** circuit breaker plus secret redaction. If the ML service is down, the system falls back to regex.
5. **Operator feedback loop:** engineers can correct a diagnosis, and corrections become new training data.

**Results (evaluation set of 155 hard logs):**

| Method | Accuracy |
|---|---|
| Regex only | 37.4% |
| **Hybrid (Regex + AI)** | **76.8%** |
| AI override precision | **93.1%** |

- Trained on 6,300 logs in about 5.5 minutes on an RTX 3050 laptop GPU.

*Speaker notes:* The evaluation set was built on purpose from logs that regex handles badly. The hybrid design means the AI improves the hard cases but never overrides a confident, specific rule.

---

## Slide 11: Feature: Automation Workflows (The Agent's Brain)

- Workflows are **graphs of typed blocks**:
  - **Triggers:** incident, DAG run finished, schedule, manual, stale incident
  - **Logic:** filter / condition, wait
  - **Diagnosis:** classify log
  - **Decision:** "Choose the fix"
  - **Approval:** human sign-off
  - **Actions:** retry task, rerun DAG, pause DAG
  - **Checks:** verify DAG state, check SLA, reconcile databases
  - **Outputs:** update incident, send notification
- **Built-in workflow templates:**
  - `retry-transient`: retry network and timeout failures automatically
  - `retry-with-approval`: retry resource and upstream issues after a person approves
  - `no-retry-data-code`: never retry bad data or code bugs; escalate them
  - `sla-rerun`, `flapping-breaker` (stop retry loops), `stale-escalation`
  - `choose-fix`, `validate-every-run`, `reconcile-databases`, `adaptive-sla-alert`, `in-flight-sla-monitor`

*Speaker notes:* Operators can use the templates as they are or build their own. The engine runs the workflows on their own fast loop, every 30 seconds by default.

---

## Slide 12: Feature: "Choose the Fix": Rules + Local LLM

- A **deterministic decision table** looks at the diagnosis, the fix history, the daily action budget, and the DAG state.
- Possible fixes: **retry · rerun · wait · pause · escalate · ignore**
- The rules first decide which fixes are **safe at all**.
- An optional **local LLM (Ollama)** then picks the best fix **only from that safe list**, with a short reason and a confidence score.
- If the LLM is unavailable or slow, the rules decide alone; a circuit breaker keeps a stopped LLM from adding cost.

*Speaker notes:* This is the agentic part with guard rails. The AI adds judgment, but the rules limit what it is allowed to do, so it cannot choose an unsafe action.

---

## Slide 13: Feature: Safety, Approvals & Audit

- **Safety policy** is checked on every action, however the workflow was built:
  - **PROD** pipelines always need human approval
  - **Daily action limit** per pipeline
  - **Dry-run mode:** record what would happen without changing anything
- **Approvals page:** operators approve or reject pending actions.
- **Audit log:** every action stores the request, evidence, policy decision, approver, result, and verification.
- **Authentication & role-based access control (RBAC)**, rate limiting, encrypted secrets.

*Speaker notes:* Trust is the main barrier to automation. A single wrong action in production destroys trust, so safety is part of the engine itself, not an optional extra.

---

## Slide 14: Feature: Alert & Notification Channels

- Supported channels: **Email (SMTP) · Slack · Microsoft Teams · Generic Webhooks**
- Configure and **test** each channel from the UI; its status (OK / Failed) is tracked.
- Secrets such as webhook URLs and SMTP passwords are encrypted.
- Workflows send alerts with a direct link back to the incident.
- In-app **Notifications** page.

*Speaker notes:* When the system escalates to a human, the right team hears about it right away, on the tool they already use.

---

## Slide 15: Feature: Visual Workflow Editor & AI Assistant

- **Drag-and-drop canvas** (React Flow) for building workflows with no code.
- The block palette and settings forms are **generated from the node catalog**, so new block types appear automatically.
- **Server-side validation** errors are shown directly on the blocks.
- **Run replay:** watch a past run step by step on the canvas.
- **AI Workflow Assistant:** describe what you want in plain English, e.g. *"retry network failures, but ask me first for production"*:
  - The assistant proposes a graph, the system validates it, and the assistant repairs it automatically if it is invalid.
  - The user reviews the proposed changes and decides whether to apply them.
  - Runs on a local Ollama model, or optionally on Anthropic Claude.

*Speaker notes:* This turns automation design into a conversation. The AI never changes a workflow directly; it only proposes, and the human approves.

---

## Slide 16: Feature: Dashboard, Incidents UI & Log Analyzer

- **Dashboard:** open incidents, automation loop status, connection health.
- **Incidents list and detail:** timeline, evidence, diagnosis card with confidence bars, actions.
- **Runs and run checks:** history of every workflow run and DAG-run validation.
- **Log Analyzer:** paste any log and get an instant diagnosis.
- **User management** with roles.

---

## Slide 17: Testing & Quality

- Large **Pytest** suite: unit, API, and integration tests (backend runs on SQLite or **PostgreSQL**).
- **Live end-to-end test** against real Airflow 3: a real failure is detected, diagnosed, retried, verified, and auto-resolved.
- **Playwright browser tests** for login, pipeline canvas, workflow editor, and run replay.
- **Fake Airflow server** and **mock adapter** for fast, offline tests.
- ML model gated by an evaluation set: a model version that fails the gates is not deployed (NO-GO).

*Speaker notes:* We proved the core loop on the real stack, not only on mocks: a real Airflow task failed, and our system fixed it without human help.

---

## Slide 18: Demo Scenario (Optional Live Demo)

1. Airflow DAG `agentic_flaky_once` fails with *503 Service Unavailable*.
2. The system detects it and opens an incident with logs as evidence.
3. Diagnosis: **TRANSIENT_NETWORK** (retryable).
4. The `retry-transient` workflow clears the task on Airflow.
5. The rerun succeeds, the verify step confirms it, and the incident is marked **AUTO_REMEDIATED**.
6. Meanwhile, `agentic_bad_data` fails with a duplicate-key error, diagnosed as **DATA_INTEGRITY**. It is **not retried**; the team is notified instead.

---

## Slide 19: Future Work

- **Data-quality monitors:** row count, null rate, freshness column, schema drift.
- Historical incident search (RAG) to suggest fixes from past incidents.
- More execution adapters (SQL, Kubernetes).
- Insights and trend reports on recurring failures.
- Continuous retraining of the diagnosis model from operator corrections.

---

## Slide 20: Conclusion

- We built an **end-to-end agentic platform** for managed data services: **monitor → detect → diagnose → decide → approve → act → verify → report**.
- **Hybrid AI** (rules + DistilBERT) raised diagnosis accuracy on hard logs from **37% to 77%**, with **93% precision** when the AI overrides.
- **Safe by design:** approvals, policies, dry runs, audit logs, and AI limited to safe options.
- **Practical and private:** runs on local models and a laptop GPU; logs never leave the organization.
- **Proven on real Airflow:** failures are fixed automatically without human intervention.
- **Impact:** less manual effort, faster recovery, fewer late-night alerts, and engineers free to focus on real problems.

### Thank You!
**Questions?**

*Ayush Shahi & Prabesh Paudel*
