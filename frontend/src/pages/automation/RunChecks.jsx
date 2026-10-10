import { useCallback, useEffect, useState } from 'react'
import { Link, useSearchParams } from 'react-router'
import { Badge, Button, Card, EmptyState } from '../../components/ui'
import { useToast } from '../../context/ToastContext'
import { formatRelative } from '../../format'
import { automationApi } from '../../services/endpoints'
import { CHECK_STATUS, checkStatus } from './automationText'

const PAGE_SIZE = 50

/** Every finished DAG run a "When a DAG run finishes" workflow checked, and how it came out. */
export default function RunChecks() {
  const toast = useToast()
  const [params, setParams] = useSearchParams()
  const dagId = params.get('dag_id')
  const [status, setStatus] = useState('')
  const [offset, setOffset] = useState(0)
  const [page, setPage] = useState(null)
  const [error, setError] = useState(null)

  const load = useCallback(async () => {
    setError(null)
    try {
      setPage(
        await automationApi.listRunChecks({
          status: status ? [status] : [],
          dag_id: dagId ?? undefined,
          limit: PAGE_SIZE,
          offset,
        }),
      )
    } catch (err) {
      const message = err.message || 'Unable to load run checks'
      setError(message)
      toast.error(message)
    }
  }, [status, dagId, offset, toast])

  useEffect(() => {
    // eslint-disable-next-line react/set-state-in-effect -- fetch on mount/filter change; state is set after await
    load()
  }, [load])

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Run checks</h1>
          <p className="muted">
            Each finished DAG run that a &ldquo;When a DAG run finishes&rdquo; workflow checked, and whether it passed.
          </p>
        </div>
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
            <option value="">All statuses</option>
            {Object.entries(CHECK_STATUS).map(([value, { label }]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
          {dagId && (
            <span className="small">
              DAG <span className="mono">{dagId}</span> only ·{' '}
              <button type="button" className="link-btn" onClick={() => setParams({})}>
                show all
              </button>
            </span>
          )}
        </div>
        {error && !page ? (
          <div role="alert">
            <p className="muted">Unable to load run checks: {error}</p>
            <Button size="sm" onClick={load}>
              Retry
            </Button>
          </div>
        ) : !page ? (
          <p className="muted">Loading…</p>
        ) : page.total === 0 ? (
          <EmptyState title="No run checks yet">
            Create a workflow that starts with &ldquo;When a DAG run finishes&rdquo; (or use the &ldquo;Validate every
            run&rdquo; template) and turn it on. Each finished run then shows up here.
          </EmptyState>
        ) : (
          <>
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr>
                    <th>Check</th>
                    <th>DAG</th>
                    <th>Run</th>
                    <th>Airflow</th>
                    <th>Details</th>
                    <th>Workflow</th>
                    <th>Finished</th>
                  </tr>
                </thead>
                <tbody>
                  {page.items.map((c) => {
                    const s = checkStatus(c.status)
                    return (
                      <tr key={c.id}>
                        <td>
                          <Badge tone={s.tone}>{s.label}</Badge>
                        </td>
                        <td className="mono small">{c.dag_id}</td>
                        <td className="mono small">{c.run_id}</td>
                        <td className="small">
                          <Badge tone={c.run_state === 'success' ? 'success' : 'danger'}>{c.run_state}</Badge>
                        </td>
                        <td className="small">{c.message ?? '—'}</td>
                        <td className="small">
                          <Link to={`/automation/runs/${c.workflow_run_id}`}>{c.workflow_name}</Link>
                        </td>
                        <td className="small">{formatRelative(c.checked_at ?? c.created_at)}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
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
