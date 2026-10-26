import { useCallback, useEffect, useState } from 'react'
import { Badge, Button, EmptyState, Toggle } from '../../components/ui'
import { formatRelative } from '../../format'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { wedataApi } from '../../services/endpoints'

export default function WeDataTaskPanel({ connection, onConnectionChanged }) {
  const { hasRole } = useAuth()
  const toast = useToast()
  const [tasks, setTasks] = useState([])
  const [search, setSearch] = useState('')
  const [monitoredOnly, setMonitoredOnly] = useState(false)
  const [syncing, setSyncing] = useState(false)
  const [pending, setPending] = useState(() => new Set())
  const [loading, setLoading] = useState(true)

  const canOperate = hasRole('ADMIN', 'OPERATOR')

  const loadTasks = useCallback(async () => {
    try {
      setLoading(true)
      const data = await wedataApi.listTasks(connection.id, {
        monitored_only: monitoredOnly ? true : undefined,
      })
      setTasks(data)
    } catch (err) {
      toast.error(err.message)
    } finally {
      setLoading(false)
    }
  }, [connection.id, monitoredOnly, toast])

  useEffect(() => {
    loadTasks()
  }, [loadTasks])

  async function onSync() {
    setSyncing(true)
    try {
      const outcome = await wedataApi.syncTasks(connection.id)
      toast.success(
        `Synced WeData tasks: ${outcome.total} total (${outcome.created} added, ${outcome.updated} updated${
          outcome.missing ? `, ${outcome.missing} missing` : ''
        })`,
      )
      await loadTasks()
      onConnectionChanged?.()
    } catch (err) {
      toast.error(`Sync failed: ${err.message}`)
    } finally {
      setSyncing(false)
    }
  }

  async function onToggle(task) {
    const next = !task.is_monitored
    setPending((prev) => new Set(prev).add(task.task_id))
    try {
      await wedataApi.setMonitored(connection.id, task.task_id, next)
      setTasks((prev) =>
        prev.map((t) => (t.task_id === task.task_id ? { ...t, is_monitored: next } : t)),
      )
      toast.success(`Task “${task.task_name}” ${next ? 'is now monitored' : 'monitoring stopped'}`)
    } catch (err) {
      toast.error(err.message)
    } finally {
      setPending((prev) => {
        const copy = new Set(prev)
        copy.delete(task.task_id)
        return copy
      })
    }
  }

  const filtered = tasks.filter((t) => {
    if (monitoredOnly && !t.is_monitored) return false
    if (!search) return true
    const term = search.toLowerCase()
    return (
      t.task_name.toLowerCase().includes(term) ||
      t.task_id.toLowerCase().includes(term) ||
      (t.workflow_name && t.workflow_name.toLowerCase().includes(term))
    )
  })

  return (
    <div className="panel-container flow">
      <div className="flex justify-between items-center wrap gap-3">
        <div className="flex items-center gap-3">
          <input
            type="search"
            placeholder="Filter tasks or workflows…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="input-sm"
          />
          <label className="checkbox-label small">
            <input
              type="checkbox"
              checked={monitoredOnly}
              onChange={(e) => setMonitoredOnly(e.target.checked)}
            />
            Monitored only
          </label>
        </div>

        {canOperate && (
          <Button size="sm" variant="secondary" onClick={onSync} loading={syncing}>
            Sync tasks from WeData
          </Button>
        )}
      </div>

      {loading && !tasks.length ? (
        <p className="muted small">Loading tasks…</p>
      ) : !filtered.length ? (
        <EmptyState
          title={tasks.length ? 'No tasks match filter' : 'No tasks synced yet'}
          description={
            tasks.length
              ? 'Try adjusting your search criteria.'
              : 'Click “Sync tasks from WeData” to discover tasks and workflows in this workspace.'
          }
          action={
            canOperate && !tasks.length ? (
              <Button size="sm" onClick={onSync} loading={syncing}>
                Sync tasks now
              </Button>
            ) : null
          }
        />
      ) : (
        <div className="table-responsive">
          <table className="table table-compact">
            <thead>
              <tr>
                <th style={{ width: 80 }}>Monitor</th>
                <th>Task & Workflow</th>
                <th>Type</th>
                <th>Schedule</th>
                <th>State</th>
                <th>Last synced</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((task) => (
                <tr key={task.task_id}>
                  <td>
                    <Toggle
                      checked={task.is_monitored}
                      disabled={!canOperate || pending.has(task.task_id)}
                      onChange={() => onToggle(task)}
                      aria-label={`Toggle monitoring for ${task.task_name}`}
                    />
                  </td>
                  <td>
                    <div className="cell-title">
                      <strong>{task.task_name}</strong>
                      {!task.is_present_in_wedata && <Badge tone="warning">missing</Badge>}
                    </div>
                    <div className="muted small">
                      {task.workflow_name || 'Individual task'} · ID: <span className="mono">{task.task_id}</span>
                    </div>
                  </td>
                  <td>
                    <Badge tone="info">{task.task_type || 'SparkSQL'}</Badge>
                  </td>
                  <td className="mono small">{task.schedule_cron || '—'}</td>
                  <td>
                    {task.is_paused ? (
                      <Badge tone="neutral">Paused</Badge>
                    ) : (
                      <Badge tone="success">Active</Badge>
                    )}
                  </td>
                  <td className="small muted">{formatRelative(task.last_synced_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
