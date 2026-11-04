import { useCallback, useEffect, useState } from 'react'
import { Link, useOutletContext } from 'react-router'
import { ActionMenu, ActionMenuItem, Button, Card, ConfirmDialog, EmptyState } from '../../components/ui'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { formatRelative } from '../../format'
import { automationApi } from '../../services/endpoints'
import { LevelBadge } from './automationUi'

const PAGE_SIZE = 50

export default function Notifications() {
  const { hasRole } = useAuth()
  const { refreshHealth } = useOutletContext()
  const toast = useToast()
  const [unreadOnly, setUnreadOnly] = useState(false)
  const [offset, setOffset] = useState(0)
  const [page, setPage] = useState(null)
  const [marking, setMarking] = useState(false)
  const [deleteTarget, setDeleteTarget] = useState(null)
  const [deleting, setDeleting] = useState(false)
  const [selected, setSelected] = useState(() => new Set())
  const canDelete = hasRole('ADMIN')

  const load = useCallback(async () => {
    try {
      setPage(await automationApi.listNotifications({ unread_only: unreadOnly || undefined, limit: PAGE_SIZE, offset }))
    } catch (err) {
      toast.error(err.message)
    }
  }, [unreadOnly, offset, toast])

  useEffect(() => {
    // eslint-disable-next-line react/set-state-in-effect -- fetch on mount/filter change; state is set after await
    load()
  }, [load])

  async function markAllRead() {
    setMarking(true)
    try {
      await automationApi.readAllNotifications()
      await Promise.all([load(), refreshHealth()])
    } catch (err) {
      toast.error(err.message)
    } finally {
      setMarking(false)
    }
  }

  async function deleteNotification(notification) {
    setDeleteTarget(notification)
  }

  const toggleSelected = (id) => {
    setSelected((current) => {
      const next = new Set(current)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  async function confirmDelete() {
    setDeleting(true)
    try {
      const ids = deleteTarget.ids ?? [deleteTarget.id]
      await Promise.all(ids.map((id) => automationApi.deleteNotification(id)))
      toast.success(`${ids.length} notification${ids.length === 1 ? '' : 's'} deleted`)
      setDeleteTarget(null)
      setSelected(new Set())
      await Promise.all([load(), refreshHealth()])
    } catch (err) {
      toast.error(err.message)
    } finally {
      setDeleting(false)
    }
  }

  const hasUnread = page?.items.some((n) => !n.read_at)

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Notifications</h1>
          <p className="muted">Messages sent by automation workflows: fixes made, approvals needed, and things that need a human.</p>
        </div>
        <Button onClick={markAllRead} loading={marking} disabled={!hasUnread}>
          Mark all read
        </Button>
      </div>
      <Card>
        <div className="toolbar">
          <label className="checkbox-inline">
            <input
              type="checkbox"
              checked={unreadOnly}
              onChange={(e) => {
                setOffset(0)
                setUnreadOnly(e.target.checked)
              }}
            />
            Unread only
          </label>
          {canDelete && selected.size > 0 && (
            <Button size="sm" variant="danger" onClick={() => setDeleteTarget({ ids: [...selected], count: selected.size })}>
              Delete selected ({selected.size})
            </Button>
          )}
        </div>
        {!page ? (
          <p className="muted">Loading…</p>
        ) : page.total === 0 ? (
          <EmptyState title="No notifications" />
        ) : (
          <>
            <ul className="notification-list">
              {page.items.map((n) => (
                <li key={n.id} className={n.read_at ? 'notification-read' : 'notification-unread'}>
                  <div className="cell-title notification-row-header">
                    {canDelete && (
                      <input
                        type="checkbox"
                        checked={selected.has(n.id)}
                        disabled={!n.read_at}
                        onChange={() => toggleSelected(n.id)}
                        aria-label={`Select ${n.title} for deletion`}
                        title={!n.read_at ? 'Mark as read before deleting' : undefined}
                      />
                    )}
                    <LevelBadge level={n.level} />
                    <strong>{n.title}</strong>
                    <span className="muted small">{formatRelative(n.created_at)}</span>
                    {canDelete && (
                      <ActionMenu label={`Actions for ${n.title}`}>
                        {(close) => (
                          <>
                          <ActionMenuItem
                            disabled={!n.read_at}
                            onClick={() => {
                              close()
                              toggleSelected(n.id)
                            }}
                          >
                            {selected.has(n.id) ? 'Remove from deletion' : 'Select for deletion'}
                          </ActionMenuItem>
                          <ActionMenuItem
                            tone="danger"
                            disabled={!n.read_at}
                            onClick={() => {
                              close()
                              deleteNotification(n)
                            }}
                          >
                            Delete
                          </ActionMenuItem>
                          </>
                        )}
                      </ActionMenu>
                    )}
                  </div>
                  {n.body && <p className="small">{n.body}</p>}
                  <div className="small">
                    {n.incident_id && <Link to={`/incidents/${n.incident_id}`}>Incident</Link>}
                    {n.incident_id && n.run_id && ' · '}
                    {n.run_id && <Link to={`/automation/runs/${n.run_id}`}>Automation run</Link>}
                  </div>
                </li>
              ))}
            </ul>
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
        title="Delete notification?"
        message={deleteTarget?.ids ? `Delete ${deleteTarget.count} selected notifications? This cannot be undone.` : deleteTarget ? `Delete "${deleteTarget.title}"? This cannot be undone.` : ''}
        onCancel={() => setDeleteTarget(null)}
        onConfirm={confirmDelete}
        loading={deleting}
      />
    </div>
  )
}
