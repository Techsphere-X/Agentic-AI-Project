import { useCallback, useEffect, useState } from 'react'
import { Link, useOutletContext } from 'react-router'
import { Button, Card, EmptyState } from '../../components/ui'
import { useToast } from '../../context/ToastContext'
import { formatRelative } from '../../format'
import { automationApi } from '../../services/endpoints'
import { LevelBadge } from './automationUi'

const PAGE_SIZE = 50

export default function Notifications() {
  const { refreshHealth } = useOutletContext()
  const toast = useToast()
  const [unreadOnly, setUnreadOnly] = useState(false)
  const [offset, setOffset] = useState(0)
  const [page, setPage] = useState(null)
  const [marking, setMarking] = useState(false)

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

  const hasUnread = page?.items.some((n) => !n.read_at)

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Notifications</h1>
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
                  <div className="cell-title">
                    <LevelBadge level={n.level} />
                    <strong>{n.title}</strong>
                    <span className="muted small">{formatRelative(n.created_at)}</span>
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
    </div>
  )
}
