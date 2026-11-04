import { useCallback, useEffect, useState } from 'react'
import { Link, useSearchParams } from 'react-router'
import { ActionMenu, ActionMenuItem, Badge, Button, Card, ConfirmDialog, EmptyState } from '../../components/ui'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { formatRelative } from '../../format'
import { automationApi } from '../../services/endpoints'
import { CHECK_STATUS, checkStatus } from './automationText'

const PAGE_SIZE = 50

/** Every finished DAG run a "When a DAG run finishes" workflow checked, and how it came out. */
export default function RunChecks() {
  const { hasRole } = useAuth()
  const toast = useToast()
  const [params, setParams] = useSearchParams()
  const dagId = params.get('dag_id')
  const [status, setStatus] = useState('')
  const [offset, setOffset] = useState(0)
  const [page, setPage] = useState(null)
  const [error, setError] = useState(null)
  const canDelete = hasRole('ADMIN')
  const [deleteTarget, setDeleteTarget] = useState(null)
  const [deleting, setDeleting] = useState(false)
  const [selected, setSelected] = useState(() => new Set())

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

  async function confirmDelete() {
    setDeleting(true)
    try {
      const ids = deleteTarget.ids ?? [deleteTarget.id]
      await Promise.all(ids.map((id) => automationApi.deleteRunCheck(id)))
      toast.success(`${ids.length} run check${ids.length === 1 ? '' : 's'} deleted`)
      setDeleteTarget(null)
      setSelected(new Set())
      await load()
    } catch (err) {
      toast.error(err.message)
    } finally {
      setDeleting(false)
    }
  }

  const toggleSelected = (id) => {
    setSelected((current) => {
      const next = new Set(current)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

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
          {canDelete && selected.size > 0 && (
            <Button size="sm" variant="danger" onClick={() => setDeleteTarget({ ids: [...selected], count: selected.size })}>
              Delete selected ({selected.size})
            </Button>
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
                    {canDelete && <th aria-label="Select" />}
                    <th>Check</th>
                    <th>DAG</th>
                    <th>Run</th>
                    <th>Airflow</th>
                    <th>Details</th>
                    <th>Workflow</th>
                    <th>Finished</th>
                    {canDelete && <th aria-label="Actions" />}
                  </tr>
                </thead>
                <tbody>
                  {page.items.map((c) => {
                    const s = checkStatus(c.status)
                    return (
                      <tr key={c.id}>
                        {canDelete && (
                          <td>
                            <input type="checkbox" checked={selected.has(c.id)} onChange={() => toggleSelected(c.id)} aria-label={`Select ${c.dag_id} run check for deletion`} />
                          </td>
                        )}
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
                        {canDelete && (
                          <td>
                            <ActionMenu label={`Actions for ${c.dag_id} run check`}>
                              {(close) => (
                                <>
                                <ActionMenuItem
                                  onClick={() => {
                                    close()
                                    toggleSelected(c.id)
                                  }}
                                >
                                  {selected.has(c.id) ? 'Remove from deletion' : 'Select for deletion'}
                                </ActionMenuItem>
                                <ActionMenuItem
                                  tone="danger"
                                  onClick={() => {
                                    close()
                                    setDeleteTarget(c)
                                  }}
                                >
                                  Delete
                                </ActionMenuItem>
                                </>
                              )}
                            </ActionMenu>
                          </td>
                        )}
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
      <ConfirmDialog
        open={Boolean(deleteTarget)}
        title="Delete run check?"
        message={deleteTarget?.ids ? `Delete ${deleteTarget.count} selected run checks? This cannot be undone.` : deleteTarget ? `Delete the run check for ${deleteTarget.dag_id}? This cannot be undone.` : ''}
        onCancel={() => setDeleteTarget(null)}
        onConfirm={confirmDelete}
        loading={deleting}
      />
    </div>
  )
}
