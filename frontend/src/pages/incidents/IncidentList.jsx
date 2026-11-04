import { useCallback, useEffect, useState } from 'react'
import { Link, useOutletContext, useSearchParams } from 'react-router'
import { ActionMenu, ActionMenuItem, Button, Card, ConfirmDialog, EmptyState } from '../../components/ui'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { formatRelative } from '../../format'
import { airflowApi, detectionApi, incidentsApi } from '../../services/endpoints'
import { describeCycle, typeLabel } from './incidentText'
import { IncidentStatusBadge, SeverityBadge } from './incidentUi'

const PAGE_SIZE = 50
const STATUS_FILTERS = {
  active: ['OPEN', 'ACKNOWLEDGED'],
  OPEN: ['OPEN'],
  ACKNOWLEDGED: ['ACKNOWLEDGED'],
  RESOLVED: ['RESOLVED'],
  all: [],
}

export default function IncidentList() {
  const { hasRole } = useAuth()
  const { refreshHealth } = useOutletContext()
  const toast = useToast()
  const [page, setPage] = useState(null)
  const [connections, setConnections] = useState([])
  const [searchParams, setSearchParams] = useSearchParams()
  const [filters, setFilters] = useState(() => ({
    status: 'active',
    severity: '',
    type: '',
    connection_id: '',
    search: searchParams.get('search') ?? '',
  }))
  const [occurrencesOrder, setOccurrencesOrder] = useState(searchParams.get('order') ?? '')
  const [offset, setOffset] = useState(0)
  const [running, setRunning] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState(null)
  const [deleting, setDeleting] = useState(false)
  const [selected, setSelected] = useState(() => new Set())

  const canOperate = hasRole('ADMIN', 'OPERATOR')
  const connectionName = (id) => connections.find((c) => c.id === id)?.name ?? '—'

  useEffect(() => {
    airflowApi
      .listConnections()
      .then(({ items }) => setConnections(items))
      .catch(() => {})
  }, [])

  const load = useCallback(async () => {
    try {
      setPage(
        await incidentsApi.list({
          status: STATUS_FILTERS[filters.status],
          severity: filters.severity,
          type: filters.type,
          connection_id: filters.connection_id,
          search: filters.search.trim(),
          sort: occurrencesOrder ? 'occurrences' : undefined,
          order: occurrencesOrder || undefined,
          limit: PAGE_SIZE,
          offset,
        }),
      )
    } catch (err) {
      toast.error(err.message)
    }
  }, [filters, occurrencesOrder, offset, toast])

  useEffect(() => {
    const timer = setTimeout(load, 200) // debounce search typing
    return () => clearTimeout(timer)
  }, [load])

  const setFilter = (key) => (e) => {
    setOffset(0)
    const value = e.target.value
    setFilters((f) => {
      const next = { ...f, [key]: value }
      const params = new URLSearchParams()
      Object.entries(next).forEach(([name, item]) => {
        if (item && !(name === 'status' && item === 'active')) params.set(name, item)
      })
      setSearchParams(params, { replace: true })
      return next
    })
  }

  const toggleSelected = (id) => {
    setSelected((current) => {
      const next = new Set(current)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function toggleOccurrencesSort() {
    setOffset(0)
    const next = occurrencesOrder === 'asc' ? 'desc' : 'asc'
    setOccurrencesOrder(next)
    const params = new URLSearchParams(searchParams)
    params.set('sort', 'occurrences')
    params.set('order', next)
    setSearchParams(params, { replace: true })
  }

  const displayedItems = page
    ? [...page.items].sort((a, b) => {
        if (!occurrencesOrder) return 0
        const difference = a.occurrence_count - b.occurrence_count
        return occurrencesOrder === 'asc' ? difference : -difference
      })
    : []

  async function onRunDetection() {
    setRunning(true)
    try {
      const summary = await detectionApi.run()
      ;(summary.errors.length ? toast.error : toast.success)(describeCycle(summary))
      await Promise.all([load(), refreshHealth()])
    } catch (err) {
      toast.error(err.message)
    } finally {
      setRunning(false)
    }
  }

  async function confirmDelete() {
    setDeleting(true)
    try {
      const ids = deleteTarget.ids ?? [deleteTarget.id]
      await Promise.all(ids.map((id) => incidentsApi.delete(id)))
      toast.success(`${ids.length} incident${ids.length === 1 ? '' : 's'} deleted`)
      setSelected(new Set())
      setDeleteTarget(null)
      await Promise.all([load(), refreshHealth()])
    } catch (err) {
      toast.error(err.message)
    } finally {
      setDeleting(false)
    }
  }

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Incidents</h1>
          <p className="muted">Failed runs and SLA breaches detected on monitored DAGs.</p>
        </div>
        {canOperate && (
          <Button variant="primary" onClick={onRunDetection} loading={running}>
            Run detection now
          </Button>
        )}
      </div>

      <Card>
        <div className="toolbar">
          <select value={filters.status} onChange={setFilter('status')} aria-label="Status">
            <option value="active">Open &amp; acknowledged</option>
            <option value="OPEN">Open</option>
            <option value="ACKNOWLEDGED">Acknowledged</option>
            <option value="RESOLVED">Resolved</option>
            <option value="all">All statuses</option>
          </select>
          <select value={filters.severity} onChange={setFilter('severity')} aria-label="Severity">
            <option value="">All severities</option>
            <option value="CRITICAL">Critical</option>
            <option value="HIGH">High</option>
            <option value="MEDIUM">Medium</option>
            <option value="LOW">Low</option>
          </select>
          <select value={filters.type} onChange={setFilter('type')} aria-label="Type">
            <option value="">All types</option>
            <option value="DAG_RUN_FAILED">Run failed</option>
            <option value="SLA_MISSED">SLA missed</option>
            <option value="DATA_CHECK_FAILED">Data check failed</option>
          </select>
          {connections.length > 1 && (
            <select value={filters.connection_id} onChange={setFilter('connection_id')} aria-label="Connection">
              <option value="">All connections</option>
              {connections.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.name}
                </option>
              ))}
            </select>
          )}
          <span className="spacer" />
          <input
            type="search"
            placeholder="Search title or DAG"
            value={filters.search}
            onChange={setFilter('search')}
            aria-label="Search incidents"
          />
          {hasRole('ADMIN') && selected.size > 0 && (
            <Button size="sm" variant="danger" onClick={() => setDeleteTarget({ ids: [...selected], count: selected.size })}>
              Delete selected ({selected.size})
            </Button>
          )}
        </div>

        {!page ? (
          <p className="muted">Loading…</p>
        ) : page.total === 0 ? (
          <EmptyState title={filters.status === 'active' ? 'No open incidents' : 'No incidents match the filters'}>
            {filters.status === 'active' && 'Monitored DAGs are healthy. Detection runs automatically.'}
          </EmptyState>
        ) : (
          <>
            <div className="table-wrap">
              <table className="table">
                <thead>
                  <tr>
                    {hasRole('ADMIN') && <th aria-label="Select" />}
                    <th>Severity</th>
                    <th>Incident</th>
                    <th>Type</th>
                    <th>Status</th>
                    <th aria-sort={occurrencesOrder ? (occurrencesOrder === 'asc' ? 'ascending' : 'descending') : 'none'}>
                      <button
                        type="button"
                        className="table-sort-button"
                        onClick={toggleOccurrencesSort}
                        aria-label={`Sort by occurrences ${occurrencesOrder === 'asc' ? 'descending' : 'ascending'}`}
                      >
                        Occurrences {occurrencesOrder === 'asc' ? '↑' : occurrencesOrder === 'desc' ? '↓' : '↕'}
                      </button>
                    </th>
                    <th>Last seen</th>
                    {hasRole('ADMIN') && <th aria-label="Actions" />}
                  </tr>
                </thead>
                <tbody>
                  {displayedItems.map((i) => (
                    <tr key={i.id} className={i.status === 'RESOLVED' ? 'row-muted' : ''}>
                      {hasRole('ADMIN') && (
                        <td>
                          <input type="checkbox" checked={selected.has(i.id)} onChange={() => toggleSelected(i.id)} aria-label={`Select ${i.title} for deletion`} />
                        </td>
                      )}
                      <td>
                        <SeverityBadge severity={i.severity} />
                      </td>
                      <td>
                        <Link to={`/incidents/${i.id}`} className="cell-title">
                          {i.title}
                        </Link>
                        <div className="muted small">
                          {connectionName(i.connection_id)} · <span className="mono">{i.dag_id}</span>
                        </div>
                      </td>
                      <td className="small">{typeLabel(i.type)}</td>
                      <td>
                        <IncidentStatusBadge status={i.status} resolution={i.resolution} />
                      </td>
                      <td className="small">{i.occurrence_count}×</td>
                      <td className="small">{formatRelative(i.last_seen_at)}</td>
                      {hasRole('ADMIN') && (
                        <td>
                          <ActionMenu label={`Actions for ${i.title}`}>
                            {(close) => (
                              <>
                              <ActionMenuItem
                                onClick={() => {
                                  close()
                                  toggleSelected(i.id)
                                }}
                              >
                                {selected.has(i.id) ? 'Remove from deletion' : 'Select for deletion'}
                              </ActionMenuItem>
                              <ActionMenuItem
                                tone="danger"
                                onClick={() => {
                                  close()
                                  setDeleteTarget(i)
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
                  ))}
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
        title="Delete incident?"
        message={deleteTarget?.ids ? `Delete ${deleteTarget.count} selected incidents? This cannot be undone.` : deleteTarget ? `Delete "${deleteTarget.title}"? This cannot be undone.` : ''}
        onCancel={() => setDeleteTarget(null)}
        onConfirm={confirmDelete}
        loading={deleting}
      />
    </div>
  )
}
