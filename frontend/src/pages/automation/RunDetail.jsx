import { lazy, Suspense, useCallback, useEffect, useState } from 'react'
import { Link, useOutletContext, useParams } from 'react-router'
import { Alert, Badge, Button, Card, EmptyState } from '../../components/ui'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { formatDateTime, formatRelative } from '../../format'
import { automationApi } from '../../services/endpoints'
import { BAD_PORTS } from '../../components/flow/graph'
import { ApprovalCard } from './Approvals'
import { NODE_LABELS, describeConfig, duration, nodeIcon, triggerText } from './automationText'
import { ApprovalStatusBadge, RunStatusBadge } from './automationUi'

// The canvas pulls in React Flow; load it only when a run is opened.
const RunCanvas = lazy(() => import('./RunCanvas'))

const STEP_TONES = { COMPLETED: 'success', WAITING: 'warning', FAILED: 'danger', RUNNING: 'info', SKIPPED: 'neutral' }

function StepItem({ step, node }) {
  const [open, setOpen] = useState(false)
  const hasOutput = step.output && Object.keys(step.output).length > 0
  const tone = step.status === 'COMPLETED' && BAD_PORTS.has(step.port) ? 'warning' : STEP_TONES[step.status]
  return (
    <li className={`flow-step flow-${tone}`}>
      <span className="flow-icon" aria-hidden="true">
        {nodeIcon(step.node_type)}
      </span>
      <div className="flow-body">
        <div className="cell-title">
          <strong>{node?.name ?? NODE_LABELS[step.node_type] ?? step.node_type}</strong>
          {step.port ? <Badge tone={tone}>{step.port}</Badge> : <Badge tone={tone}>{step.status.toLowerCase()}</Badge>}
          <span className="muted small mono">{step.node_id}</span>
        </div>
        {node && <div className="muted small">{describeConfig(node.type, node.config)}</div>}
        {step.message && <div className="small">{step.message}</div>}
        {hasOutput && (
          <button type="button" className="link-btn small" onClick={() => setOpen(!open)}>
            {open ? 'Hide details' : 'Details'}
          </button>
        )}
        {open && <pre className="json-view">{JSON.stringify(step.output, null, 2)}</pre>}
      </div>
    </li>
  )
}

export default function RunDetail() {
  const { id } = useParams()
  const { hasRole } = useAuth()
  const { refreshHealth } = useOutletContext()
  const toast = useToast()
  const [run, setRun] = useState(null)
  const [error, setError] = useState(null)
  const [cancelling, setCancelling] = useState(false)
  const canOperate = hasRole('ADMIN', 'OPERATOR')

  const load = useCallback(async () => {
    try {
      setRun(await automationApi.getRun(id))
      setError(null)
    } catch (err) {
      setError(err.message)
    }
  }, [id])

  useEffect(() => {
    // eslint-disable-next-line react/set-state-in-effect -- fetch on mount/id change; state is set after await
    load()
  }, [load])

  async function cancel() {
    setCancelling(true)
    try {
      setRun(await automationApi.cancelRun(id))
      toast.success('Run cancelled')
      refreshHealth()
    } catch (err) {
      toast.error(err.message)
    } finally {
      setCancelling(false)
    }
  }

  if (error) {
    return (
      <div className="page">
        <Link to="/automation/runs">← Automation runs</Link>
        <Alert tone="danger">{error}</Alert>
      </div>
    )
  }
  if (!run) return <p className="muted">Loading…</p>

  const nodes = Object.fromEntries((run.graph?.nodes ?? []).map((n) => [n.id, n]))
  const active = ['PENDING', 'RUNNING', 'WAITING'].includes(run.status)
  const pending = run.approvals.filter((a) => a.status === 'PENDING')
  const diagnosis = run.context?.diagnosis

  return (
    <div className="page">
      <Link to="/automation/runs" className="small">
        ← Automation runs
      </Link>
      <div className="page-header">
        <div>
          <RunStatusBadge run={run} />
          <h1>{run.workflow_name}</h1>
          {run.incident && (
            <p className="muted">
              For <Link to={`/incidents/${run.incident_id}`}>{run.incident.title}</Link>
            </p>
          )}
        </div>
        {canOperate && active && (
          <Button variant="danger" onClick={cancel} loading={cancelling}>
            Cancel run
          </Button>
        )}
      </div>

      {run.error && <Alert tone="danger">{run.error}</Alert>}
      {pending.map((a) => (
        <ApprovalCard
          key={a.id}
          approval={a}
          canDecide={canOperate}
          onDecided={() => {
            load()
            refreshHealth()
          }}
        />
      ))}

      <Card>
        <dl className="kv kv-grid">
          <div className="kv-row">
            <dt>Triggered by</dt>
            <dd>{triggerText(run.trigger_event)}</dd>
          </div>
          <div className="kv-row">
            <dt>Started</dt>
            <dd>{formatDateTime(run.started_at ?? run.created_at)}</dd>
          </div>
          <div className="kv-row">
            <dt>Took</dt>
            <dd>{duration(run)}</dd>
          </div>
          {run.wake_at && (
            <div className="kv-row">
              <dt>Checks again</dt>
              <dd>{formatDateTime(run.wake_at)}</dd>
            </div>
          )}
          {diagnosis && (
            <div className="kv-row">
              <dt>Diagnosis</dt>
              <dd>{diagnosis.label}</dd>
            </div>
          )}
          {run.context?.sla && (
            <div className="kv-row">
              <dt>SLA Check</dt>
              <dd>
                <Badge tone={run.context.sla.exceeded ? 'warning' : 'success'}>
                  {run.context.sla.exceeded ? 'SLA Exceeded' : 'Within SLA'}
                </Badge>{' '}
                <span className="small muted">
                  {run.context.sla.actual_minutes} min vs {run.context.sla.threshold_minutes} min threshold ({run.context.sla.metric})
                </span>
              </dd>
            </div>
          )}
          <div className="kv-row">
            <dt>Workflow version</dt>
            <dd>
              v{run.workflow_version}
              {run.dry_run && ' · dry run (no changes made)'}
            </dd>
          </div>
        </dl>
      </Card>

      <Card title="Path taken">
        <Suspense fallback={<p className="muted">Loading…</p>}>
          <RunCanvas run={run} />
        </Suspense>
      </Card>

      <div className="detail-grid">
        <Card title="Steps">
          {run.steps.length === 0 ? (
            <EmptyState title="Not started yet">The run starts on the next automation check.</EmptyState>
          ) : (
            <ol className="flow">
              {run.steps.map((s) => (
                <StepItem key={s.id} step={s} node={nodes[s.node_id]} />
              ))}
            </ol>
          )}
        </Card>
        <Card title="Approvals">
          {run.approvals.length === 0 ? (
            <p className="muted small">No approval was needed.</p>
          ) : (
            <ul className="plain-list">
              {run.approvals.map((a) => (
                <li key={a.id}>
                  <ApprovalStatusBadge status={a.status} /> <span className="small">{a.title}</span>
                  <div className="muted small">
                    {a.decided_by_name ? `${a.decided_by_name} · ${formatRelative(a.decided_at)}` : `expires ${formatDateTime(a.expires_at)}`}
                    {a.comment && ` · “${a.comment}”`}
                  </div>
                </li>
              ))}
            </ul>
          )}
        </Card>
      </div>
    </div>
  )
}
