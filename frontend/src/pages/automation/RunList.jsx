import { useCallback, useEffect, useState } from 'react'
import { Link, useSearchParams } from 'react-router'
import { Button, Card, EmptyState } from '../../components/ui'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { formatRelative } from '../../format'
import { automationApi } from '../../services/endpoints'
import { duration, triggerText } from './automationText'
import { RunStatusBadge } from './automationUi'

const PAGE_SIZE = 50
const STATUS_FILTERS = {
  all: [],
  active: ['PENDING', 'RUNNING', 'WAITING'],
  COMPLETED: ['COMPLETED'],
  FAILED: ['FAILED'],
  CANCELLED: ['CANCELLED'],
}

export function RunTable({ runs, showIncident = true }) {
  return (
    <div className="table-wrap">
      <table className="table">
        <thead>
          <tr>
            <th>Status</th>
            <th>Workflow</th>
            {showIncident && <th>Incident</th>}
            <th>Trigger</th>
            <th>Started</th>
            <th>Took</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((r) => (
            <tr key={r.id}>
              <td>
                <RunStatusBadge run={r} />
              </td>
              <td>
                <Link to={`/automation/runs/${r.id}`} className="cell-title">
                  {r.workflow_name}
                </Link>
                {r.context?.diagnosis && <div className="muted small">{r.context.diagnosis.label}</div>}
              </td>
              {showIncident && (
                <td className="small">
                  {r.incident ? (
                    <Link to={`/incidents/${r.incident_id}`}>{r.incident.title}</Link>
                  ) : (
                    '—'
                  )}
                </td>
              )}
              <td className="small">{triggerText(r.trigger_event)}</td>
              <td className="small">{formatRelative(r.started_at ?? r.created_at)}</td>
              <td className="small">{duration(r)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export default function RunList() {
  const { hasRole } = useAuth()
  const toast = useToast()
  const [status, setStatus] = useState('all')
  const [offset, setOffset] = useState(0)
  const [page, setPage] = useState(null)
  const [ticking, setTicking] = useState(false)
  const [error, setError] = useState(null)
  const [params, setParams] = useSearchParams()
  const workflowId = params.get('workflow_id')

  const load = useCallback(async () => {
    setError(null)
    try {
      setPage(
        await automationApi.listRuns({
          status: STATUS_FILTERS[status],
          workflow_id: workflowId ?? undefined,
          limit: PAGE_SIZE,
          offset,
        }),
      )
    } catch (err) {
      const message = err.message || 'Unable to load automation runs'
      setError(message)
      toast.error(message)
    }
  }, [status, workflowId, offset, toast])

  useEffect(() => {
    // eslint-disable-next-line react/set-state-in-effect -- fetch on mount/filter change; state is set after await
    load()
  }, [load])

  async function onTick() {
    setTicking(true)
    try {
      const summary = await automationApi.tick()
      toast.success(
        `Advanced ${summary.advanced} run(s): ${summary.completed} completed, ${summary.waiting} waiting, ${summary.failed} failed`,
      )
      await load()
    } catch (err) {
      toast.error(err.message)
    } finally {
      setTicking(false)
    }
  }

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Automation runs</h1>
          <p className="muted">Every time a workflow ran, step by step.</p>
        </div>
        {hasRole('ADMIN', 'OPERATOR') && (
          <Button onClick={onTick} loading={ticking} title="Advance waiting runs now instead of on the next cycle">
            Check runs now
          </Button>
        )}
      </div>
      <Card>
        <div className="toolbar">
          <select
            value={status}
            aria-label="Status"
            onChange={(e) => {
              setOffset(0)
              setStatus(e.target.value)
            }}
          >
            <option value="all">All statuses</option>
            <option value="active">Active (pending / waiting)</option>
            <option value="COMPLETED">Completed</option>
            <option value="FAILED">Failed</option>
            <option value="CANCELLED">Cancelled</option>
          </select>
          {workflowId && (
            <span className="small">
              One workflow only ·{' '}
              <button type="button" className="link-btn" onClick={() => setParams({})}>
                show all
              </button>
            </span>
          )}
        </div>
        {error && !page ? (
          <div role="alert">
            <p className="muted">Unable to load automation runs: {error}</p>
            <Button size="sm" onClick={load}>
              Retry
            </Button>
          </div>
        ) : !page ? (
          <p className="muted">Loading…</p>
        ) : page.total === 0 ? (
          <EmptyState title="No runs yet">Runs appear when you press Run now, on a schedule, or when an incident starts a workflow.</EmptyState>
        ) : (
          <>
            <RunTable runs={page.items} />
            <div className="pager">
              <span className="muted small">
                {offset + 1}–{Math.min(offset + PAGE_SIZE, page.total)} of {page.total}
              </span>
              <Button size="sm" disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}>
                Previous
              </Button>
              <Button size="sm" disabled={offset + PAGE_SIZE >= page.total} onClick={() => setOffset(offset + PAGE_SIZE)}>
                Next
              </Button>
            </div>
          </>
        )}
      </Card>
    </div>
  )
}
