import { useCallback, useEffect, useState } from 'react'
import { Link, useOutletContext } from 'react-router'
import { Badge, Button, Card, StatusPill } from '../../components/ui'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { formatRelative } from '../../format'
import ConnectorMetricsSection from './ConnectorMetricsSection'
import {
  airflowApi,
  automationApi,
  channelsApi,
  databaseApi,
  detectionApi,
  healthApi,
  incidentsApi,
} from '../../services/endpoints'

const SEVERITIES = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']
const SEVERITY_PILL = {
  CRITICAL: 'error',
  HIGH: 'error',
  MEDIUM: 'degraded',
  LOW: 'UNKNOWN',
}
const SEVERITY_BADGE_TONE = {
  CRITICAL: 'danger',
  HIGH: 'danger',
  MEDIUM: 'warning',
  LOW: 'neutral',
}
const RUN_STATUS_TONE = {
  COMPLETED: 'success',
  RUNNING: 'info',
  WAITING_APPROVAL: 'warning',
  FAILED: 'danger',
  CANCELED: 'neutral',
}

function Step({ done, children }) {
  return (
    <li className={`step ${done ? 'step-done' : ''}`}>
      <span className="step-mark" aria-hidden="true">
        {done ? '✓' : ''}
      </span>
      <span>{children}</span>
    </li>
  )
}

export default function Dashboard() {
  const { health, incidentSummary, automationSummary } = useOutletContext()
  const { user } = useAuth()
  const toast = useToast()

  const [connections, setConnections] = useState(null)
  const [databases, setDatabases] = useState(null)
  const [channels, setChannels] = useState(null)
  const [dagStats, setDagStats] = useState(null)
  const [detection, setDetection] = useState(null)
  const [recentIncidents, setRecentIncidents] = useState(null)
  const [recentRuns, setRecentRuns] = useState(null)
  const [connectorMetrics, setConnectorMetrics] = useState(null)
  const [runningDetection, setRunningDetection] = useState(false)
  const [error, setError] = useState(null)

  const loadDashboardData = useCallback(async () => {
    try {
      detectionApi
        .status()
        .then((d) => setDetection(d))
        .catch(() => {})

      healthApi
        .connectorMetrics()
        .then((m) => setConnectorMetrics(m))
        .catch(() => {})

      databaseApi
        .listConnections()
        .then(({ items }) => setDatabases(items))
        .catch(() => setDatabases([]))

      channelsApi
        .listConnections()
        .then(({ items }) => setChannels(items))
        .catch(() => setChannels([]))

      incidentsApi
        .list({ limit: 5 })
        .then((res) => setRecentIncidents(res.items ?? []))
        .catch(() => setRecentIncidents([]))

      automationApi
        .listRuns({ limit: 5 })
        .then((res) => setRecentRuns(res.items ?? []))
        .catch(() => setRecentRuns([]))

      const { items } = await airflowApi.listConnections()
      setConnections(items)

      const liveConns = items.filter((c) => c.is_live)
      if (liveConns.length > 0) {
        const stats = await Promise.all(
          liveConns.map(async (c) => {
            const [all, monitored] = await Promise.all([
              airflowApi.listDags(c.id, { limit: 1 }),
              airflowApi.listDags(c.id, { limit: 1, monitored: true }),
            ])
            return { synced: all.total, monitored: monitored.total }
          }),
        )
        setDagStats(
          stats.reduce(
            (acc, s) => ({ synced: acc.synced + s.synced, monitored: acc.monitored + s.monitored }),
            { synced: 0, monitored: 0, live: stats.length },
          ),
        )
      } else {
        setDagStats({ synced: 0, monitored: 0, live: 0 })
      }
    } catch (err) {
      setError(err.message)
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    async function load() {
      if (!cancelled) {
        await loadDashboardData()
      }
    }
    load()
    const metricsTimer = setInterval(async () => {
      try {
        const metrics = await healthApi.connectorMetrics()
        if (!cancelled) setConnectorMetrics(metrics)
      } catch (err) {
        if (!cancelled) setError(err.message)
      }
    }, 15000)
    return () => {
      cancelled = true
      clearInterval(metricsTimer)
    }
  }, [loadDashboardData])

  async function handleRunDetection() {
    setRunningDetection(true)
    try {
      const result = await detectionApi.run()
      toast.success(
        `Scan complete: inspected ${result.dags_checked ?? 0} DAG(s), found ${result.incidents_found ?? 0} issue(s)`,
      )
      loadDashboardData()
    } catch (err) {
      toast.error(`Detection scan failed: ${err.message}`)
    } finally {
      setRunningDetection(false)
    }
  }

  const tested = connections?.some((c) => c.last_health_status === 'HEALTHY')
  const totalOpenIncidents = incidentSummary?.open_total ?? 0
  const totalBySev = incidentSummary?.by_severity ?? {}

  // Severity proportions for the meter
  const crit = totalBySev.CRITICAL ?? 0
  const high = totalBySev.HIGH ?? 0
  const med = totalBySev.MEDIUM ?? 0
  const low = totalBySev.LOW ?? 0
  const sevSum = crit + high + med + low || 1

  // Setup completion progress
  const setupSteps = [
    Boolean(connections?.length),
    Boolean(tested),
    Boolean(dagStats?.synced > 0),
    Boolean(dagStats?.monitored > 0),
    Boolean(detection?.last_cycle),
    Boolean(automationSummary?.enabled_workflows > 0),
  ]
  const completedSteps = setupSteps.filter(Boolean).length
  const setupPercent = Math.round((completedSteps / setupSteps.length) * 100)

  return (
    <div className="page">
      {/* ----------------- Hero / Welcome Header ----------------- */}
      <div className="dashboard-hero">
        <div className="dashboard-hero-title">
          <h1>
            Welcome back, {user?.full_name ? user.full_name.split(' ')[0] : 'Operator'}
          </h1>
          <div className="dashboard-hero-subtitle">
            <span>Autonomous Pipeline Operations & Self-Healing Engine</span>
          </div>
        </div>

        <div className="dashboard-actions">
          <Button
            variant="secondary"
            size="sm"
            loading={runningDetection}
            onClick={handleRunDetection}
            title="Scan Airflow task logs and metadata for new failures"
          >
            ▶ Run Detection Now
          </Button>
        </div>
      </div>

      {error && <p className="field-error">{error}</p>}

      {/* ----------------- Pending Approvals Alert ----------------- */}
      {automationSummary?.pending_approvals > 0 && (
        <div className="banner-alert">
          <div className="banner-alert-content">
              <span>
              <strong>{automationSummary.pending_approvals} action(s)</strong> waiting for human approval
              before executing on production pipelines.
            </span>
          </div>
          <Link to="/automation/approvals" className="btn btn-sm btn-primary">
            Review Approvals
          </Link>
        </div>
      )}

      {/* ----------------- KPI Cards Grid ----------------- */}
      <div className="stat-grid">
        {/* Card 1: Platform Database */}
        <div className="kpi-card">
          <div className="kpi-header">
            <span className="kpi-title">Platform Database</span>
            <StatusPill
              status={health ? health.database.status : 'UNKNOWN'}
              label={health?.database.status === 'ok' ? 'Online' : 'Checking…'}
            />
          </div>
          <div className="stat-value">
            {health?.database.status === 'ok' ? 'Healthy' : 'Degraded'}
          </div>
          <p className="muted small" style={{ margin: 0 }}>
            {health?.database.latency_ms != null
              ? `${health.database.latency_ms} ms query latency`
              : 'Verifying database…'}
          </p>
        </div>

        {/* Card 2: Pipelines & Connections */}
        <div className="kpi-card">
          <div className="kpi-header">
            <span className="kpi-title">Pipeline Ecosystem</span>
            <Link to="/connections/airflow" className="small">
              Manage
            </Link>
          </div>
          <div className="stat-value">
            {connections && databases ? connections.length + databases.length : '—'}
          </div>
          <p className="muted small" style={{ margin: 0 }}>
            {connections?.length ?? 0} Airflow · {databases?.length ?? 0} DB
            {databases?.length === 1 ? '' : 's'} · {channels?.length ?? 0} Channel
            {channels?.length === 1 ? '' : 's'}
          </p>
          <p className="small muted" style={{ marginTop: '0.2rem', marginBottom: 0 }}>
            {dagStats?.live ? `${dagStats.monitored} of ${dagStats.synced} DAGs monitored` : 'No live Airflow DAGs'}
          </p>
        </div>

        {/* Card 3: Open Incidents */}
        <div className="kpi-card">
          <div className="kpi-header">
            <span className="kpi-title">Open Incidents</span>
            <Link to="/incidents" className="small">
              View all
            </Link>
          </div>
          <div className="stat-value">{totalOpenIncidents}</div>
          {totalOpenIncidents > 0 ? (
            <>
              <div className="severity-meter">
                {crit > 0 && (
                  <div
                    className="severity-meter-segment meter-critical"
                    style={{ width: `${(crit / sevSum) * 100}%` }}
                    title={`${crit} critical`}
                  />
                )}
                {high > 0 && (
                  <div
                    className="severity-meter-segment meter-high"
                    style={{ width: `${(high / sevSum) * 100}%` }}
                    title={`${high} high`}
                  />
                )}
                {med > 0 && (
                  <div
                    className="severity-meter-segment meter-medium"
                    style={{ width: `${(med / sevSum) * 100}%` }}
                    title={`${med} medium`}
                  />
                )}
                {low > 0 && (
                  <div
                    className="severity-meter-segment meter-low"
                    style={{ width: `${(low / sevSum) * 100}%` }}
                    title={`${low} low`}
                  />
                )}
              </div>
              <div className="pill-row" style={{ marginTop: '0.25rem' }}>
                {SEVERITIES.filter((s) => totalBySev[s]).map((s) => (
                  <StatusPill
                    key={s}
                    status={SEVERITY_PILL[s]}
                    label={`${totalBySev[s]} ${s.toLowerCase()}`}
                  />
                ))}
              </div>
            </>
          ) : (
            <p className="muted small" style={{ margin: 0 }}>
              All pipeline runs healthy
            </p>
          )}
        </div>

        {/* Card 4: Autonomous Automation */}
        <div className="kpi-card">
          <div className="kpi-header">
            <span className="kpi-title">Automation Engine</span>
            <Link to="/automation/workflows" className="small">
              Workflows
            </Link>
          </div>
          <div className="stat-value">{automationSummary?.active_runs ?? 0}</div>
          <p className="muted small" style={{ margin: 0 }}>
            Active execution(s) right now
          </p>
          <p className="small muted" style={{ marginTop: '0.2rem', marginBottom: 0 }}>
            {automationSummary
              ? `${automationSummary.enabled_workflows} active workflow(s) • ${automationSummary.pending_approvals} approvals`
              : 'Checking…'}
          </p>
        </div>

        {/* Card 5: Detection Engine */}
        <div className="kpi-card">
          <div className="kpi-header">
            <span className="kpi-title">Failure Detection</span>
            <Link to="/incidents" className="small">
              Scan history
            </Link>
          </div>
          <div style={{ marginTop: '0.2rem', marginBottom: '0.4rem' }}>
            <StatusPill
              status={detection?.enabled ? (detection.last_error ? 'degraded' : 'ok') : 'UNKNOWN'}
              label={detection?.enabled ? `Scanning (${Math.round(detection.interval_seconds / 60)}m)` : 'Disabled'}
              title={detection?.last_error ?? ''}
            />
          </div>
          <p className="muted small" style={{ margin: 0 }}>
            {detection?.last_cycle
              ? `Inspected ${detection.last_cycle.dags} DAG(s) ${formatRelative(detection.last_cycle.finished_at)}`
              : 'No detection cycle run yet'}
          </p>
        </div>
      </div>

      {/* ----------------- Connector Performance & Run KPIs (Graph + Leaderboard) ----------------- */}
      <ConnectorMetricsSection
        data={connectorMetrics}
        loading={connectorMetrics === null}
      />

      {/* ----------------- Dual Feeds: Incidents & Workflow Runs ----------------- */}
      <div className="activity-split">
        {/* Left: Recent Incidents */}
        <Card
          title="Recent Pipeline Incidents"
          actions={
            <Link to="/incidents" className="small">
              All incidents →
            </Link>
          }
        >
          {recentIncidents === null ? (
            <p className="muted small">Loading incidents…</p>
          ) : recentIncidents.length === 0 ? (
            <p className="muted small">No incidents recorded. Pipelines are running cleanly.</p>
          ) : (
            <ul className="activity-list">
              {recentIncidents.map((inc) => (
                <li key={inc.id}>
                  <Link to={`/incidents/${inc.id}`} className="activity-card">
                    <div className="activity-meta">
                      <span className="activity-title">{inc.title}</span>
                      <span className="activity-sub">
                        <span className="mono" style={{ fontWeight: 600 }}>{inc.dag_id}</span>
                        <span>•</span>
                        <span>{formatRelative(inc.occurred_at ?? inc.first_seen_at)}</span>
                        {inc.occurrence_count > 1 && (
                          <>
                            <span>•</span>
                            <span className="badge badge-warning small">{inc.occurrence_count}x</span>
                          </>
                        )}
                      </span>
                    </div>
                    <Badge tone={SEVERITY_BADGE_TONE[inc.severity] ?? 'neutral'}>
                      {inc.severity}
                    </Badge>
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </Card>

        {/* Right: Recent Workflow Runs */}
        <Card
          title="Recent Autonomous Executions"
          actions={
            <Link to="/automation/runs" className="small">
              All executions →
            </Link>
          }
        >
          {recentRuns === null ? (
            <p className="muted small">Loading runs…</p>
          ) : recentRuns.length === 0 ? (
            <p className="muted small">No workflow executions yet.</p>
          ) : (
            <ul className="activity-list">
              {recentRuns.map((run) => (
                <li key={run.id}>
                  <Link to={`/automation/runs/${run.id}`} className="activity-card">
                    <div className="activity-meta">
                      <span className="activity-title">{run.workflow_name}</span>
                      <span className="activity-sub">
                        <span>{run.trigger_event ?? 'Manual trigger'}</span>
                        <span>•</span>
                        <span>{formatRelative(run.started_at ?? run.created_at)}</span>
                        {run.dry_run && <span className="badge badge-neutral small">Dry run</span>}
                      </span>
                    </div>
                    <Badge tone={RUN_STATUS_TONE[run.status] ?? 'neutral'}>
                      {run.status}
                    </Badge>
                  </Link>
                </li>
              ))}
            </ul>
          )}
        </Card>
      </div>

      {/* ----------------- Guided Platform Maturity / Setup ----------------- */}
      <Card title="Platform Setup & Readiness" style={{ marginTop: '1.25rem' }}>
        <div className="setup-progress-wrapper">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span className="small muted">
              {completedSteps} of {setupSteps.length} milestones complete
            </span>
            <strong className="small" style={{ color: 'var(--accent)' }}>{setupPercent}%</strong>
          </div>
          <div className="setup-progress-track">
            <div className="setup-progress-fill" style={{ width: `${setupPercent}%` }} />
          </div>
        </div>

        <ol className="steps">
          <Step done={Boolean(connections?.length)}>
            <Link to="/connections/airflow">Add an Airflow connection</Link>
          </Step>
          <Step done={tested}>Test connection health until it reports Healthy</Step>
          <Step done={Boolean(dagStats?.synced > 0)}>
            <Link to="/connections/airflow">Sync DAGs</Link> from Airflow
          </Step>
          <Step done={Boolean(dagStats?.monitored > 0)}>
            Open connection and enable monitoring on critical DAGs
          </Step>
          <Step done={Boolean(detection?.last_cycle)}>
            Detection cycle executed (automated or via &quot;Run Detection Now&quot;)
          </Step>
          <Step done={Boolean(automationSummary?.enabled_workflows > 0)}>
            <Link to="/automation/workflows">Activate an automated remediation workflow</Link>
          </Step>
        </ol>
      </Card>
    </div>
  )
}
