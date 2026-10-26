import { Fragment, useCallback, useEffect, useState } from 'react'
import { useOutletContext, useSearchParams } from 'react-router'
import { Badge, Button, Card, EmptyState, Modal, StatusPill } from '../../components/ui'
import { channelState, connectionState, databaseState } from '../../connectionState'
import { formatRelative } from '../../format'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { airflowApi, channelsApi, databaseApi } from '../../services/endpoints'
import AirflowConnectionForm, { TestResult } from './AirflowConnectionForm'
import DagPanel from './DagPanel'
import DatabaseConnectionForm from './DatabaseConnectionForm'
import { CHANNEL_KINDS, ENGINES } from './engines'
import NotificationChannelForm from './NotificationChannelForm'

const POLL_MS = 15000

const TYPES = {
  airflow: { label: 'Airflow', plural: 'Airflow connections', api: airflowApi, state: connectionState, tone: 'info' },
  database: { label: 'Database', plural: 'Database connections', api: databaseApi, state: databaseState, tone: 'neutral' },
  channel: { label: 'Notification', plural: 'Notification channels', api: channelsApi, state: channelState, tone: 'warning' },
}

const PAGE_COPY = {
  airflow: {
    title: 'Airflow connections',
    description: 'Airflow instances connected to this platform. Open a connection to choose which DAGs are monitored.',
    addLabel: 'Add Airflow',
    emptyAdmin: 'Add an Airflow instance to start monitoring DAGs. Use the Mock Airflow type if none is running locally.',
  },
  database: {
    title: 'Database connections',
    description: 'Databases available to workflows for checks, queries, and automated actions.',
    addLabel: 'Add database',
    emptyAdmin: 'Add a database connection to make it available to workflows.',
  },
  channel: {
    title: 'Notification channels',
    description: 'Email, Slack, and Teams destinations used by workflows to notify people.',
    addLabel: 'Add channel',
    emptyAdmin: 'Add an email, Slack, or Teams channel to make it available to workflows.',
  },
}

function endpoint(type, c) {
  if (type === 'airflow') return c.base_url
  if (type === 'channel') return c.target
  return `${c.host}:${c.port}/${c.database}`
}

function version(type, c) {
  if (type === 'airflow') return c.airflow_version ? `Airflow ${c.airflow_version} (${c.api_version})` : '—'
  if (type === 'channel') return CHANNEL_KINDS[c.kind]?.label ?? c.kind
  return c.server_version ? `${ENGINES[c.engine]?.label ?? c.engine} ${c.server_version}` : ENGINES[c.engine]?.label ?? c.engine
}

function subtitle(type, c) {
  if (type === 'channel') {
    const to = c.settings?.default_recipients ?? []
    return c.kind === 'EMAIL' ? `from ${c.settings?.from_address}${to.length ? ` · to ${to.join(', ')}` : ''}` : 'posts to the linked channel'
  }
  const parts = [c.environment.toLowerCase()]
  if (type === 'airflow') parts.push(`${c.auth_type.toLowerCase()} auth`)
  else parts.push(c.username)
  return parts.join(' · ')
}

/**
 * A type-specific connection page. Airflow rows expand to their DAGs, where monitoring,
 * failure alerts and SLAs are configured.
 */
export default function Catalog({ type }) {
  const { hasRole, user } = useAuth()
  const { health, refreshHealth } = useOutletContext()
  const toast = useToast()
  const [params, setParams] = useSearchParams()
  const [lists, setLists] = useState({}) // type -> connections (missing = still loading)
  const [editing, setEditing] = useState(null) // { type, connection: null (new) | object }
  const [testingId, setTestingId] = useState(null)
  const [lastTest, setLastTest] = useState(null) // { connection, result }
  const [deleting, setDeleting] = useState(null) // { type, connection }

  const isAdmin = hasRole('ADMIN')
  const canOperate = hasRole('ADMIN', 'OPERATOR')
  const allowMock = health?.environment !== 'production'
  const openId = params.get('open')
  const page = PAGE_COPY[type]

  const load = useCallback(async () => {
    try {
      const result = await TYPES[type].api.listConnections()
      setLists((prev) => ({ ...prev, [type]: result.items }))
    } catch (err) {
      setLists((prev) => ({ ...prev, [type]: prev[type] ?? [] }))
      toast.error(`${TYPES[type].plural}: ${err.message}`)
    }
  }, [toast, type])

  useEffect(() => {
    // eslint-disable-next-line react/set-state-in-effect -- initial fetch; state is set after await
    load()
    // Keep statuses current so an expanded DAG list hides as soon as Airflow goes away.
    const timer = setInterval(() => document.visibilityState === 'visible' && load(), POLL_MS)
    return () => clearInterval(timer)
  }, [load])

  function setParam(key, value) {
    setParams(
      (p) => {
        const next = new URLSearchParams(p)
        if (value) next.set(key, value)
        else next.delete(key)
        return next
      },
      { replace: true },
    )
  }

  async function reloadAll() {
    await Promise.all([load(), refreshHealth()])
  }

  async function onTest(type, conn) {
    let to = []
    if (type === 'channel' && conn.kind === 'EMAIL') {
      const defaults = conn.settings?.default_recipients ?? []
      const answer = window.prompt('Send a test email to:', defaults.length ? defaults.join(', ') : (user?.email ?? ''))
      if (answer === null) return
      to = answer.split(/[,\s;]+/).filter(Boolean)
    }
    setTestingId(conn.id)
    try {
      const result = await TYPES[type].api.testSaved(conn.id, to)
      setLastTest({ connection: conn, result })
      await reloadAll()
    } catch (err) {
      toast.error(err.message)
    } finally {
      setTestingId(null)
    }
  }

  async function onDelete() {
    const { type, connection } = deleting
    try {
      await TYPES[type].api.deleteConnection(connection.id)
      toast.success(`Deleted “${connection.name}”`)
      setDeleting(null)
      if (openId === connection.id) setParam('open', null)
      await reloadAll()
    } catch (err) {
      toast.error(err.message)
    }
  }

  const loading = !lists[type]
  const rows = (lists[type] ?? []).map((c) => ({ type, c }))

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>{page.title}</h1>
          <p className="muted">{page.description}</p>
        </div>
        {isAdmin && (
          <div className="header-actions">
            <Button variant="primary" onClick={() => setEditing({ type, connection: null })}>
              {page.addLabel}
            </Button>
          </div>
        )}
      </div>

      <Card>
        {loading ? (
          <p className="muted">Loading…</p>
        ) : rows.length === 0 ? (
          <EmptyState title={`No ${TYPES[type].plural.toLowerCase()} yet`}>
            {isAdmin ? page.emptyAdmin : 'Ask an administrator to register a connection.'}
          </EmptyState>
        ) : (
          <div className="table-wrap">
            <table className="table catalog-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Type</th>
                  <th>Status</th>
                  <th>Endpoint</th>
                  <th>Version</th>
                  <th>Last checked</th>
                  <th aria-label="Actions" />
                </tr>
              </thead>
              <tbody>
                {rows.map(({ type, c }) => {
                  const state = TYPES[type].state(c)
                  const open = type === 'airflow' && openId === c.id
                  return (
                    <Fragment key={c.id}>
                      <tr className={`${c.is_active ? '' : 'row-muted'} ${open ? 'row-open' : ''}`}>
                        <td>
                          <div className="cell-title">
                            {c.name}
                            {c.is_default && <Badge tone="info">default</Badge>}
                            {c.kind === 'MOCK' && <Badge tone="neutral">mock</Badge>}
                            {!c.is_active && <Badge tone="neutral">inactive</Badge>}
                          </div>
                          <div className="muted small">{subtitle(type, c)}</div>
                        </td>
                        <td>
                          <Badge tone={TYPES[type].tone}>{TYPES[type].label}</Badge>
                        </td>
                        <td>
                          <StatusPill tone={state.tone} label={state.label} title={state.message ?? state.hint} />
                        </td>
                        <td className="mono small">{endpoint(type, c)}</td>
                        <td className="small">{version(type, c)}</td>
                        <td className="small">
                          {formatRelative(c.last_checked_at ?? c.last_used_at)}
                          {c.last_latency_ms != null && <div className="muted">{c.last_latency_ms} ms</div>}
                        </td>
                        <td className="actions">
                          {type === 'airflow' && (
                            <Button
                              size="sm"
                              variant={open ? 'primary' : 'secondary'}
                              aria-expanded={open}
                              onClick={() => setParam('open', open ? null : c.id)}
                            >
                              DAGs {open ? '▴' : '▾'}
                            </Button>
                          )}
                          {canOperate && (
                            <Button size="sm" onClick={() => onTest(type, c)} loading={testingId === c.id}>
                              Test
                            </Button>
                          )}
                          {isAdmin && (
                            <>
                              <Button size="sm" variant="ghost" onClick={() => setEditing({ type, connection: c })}>
                                Edit
                              </Button>
                              <Button size="sm" variant="ghost-danger" onClick={() => setDeleting({ type, connection: c })}>
                                Delete
                              </Button>
                            </>
                          )}
                        </td>
                      </tr>
                      {open && (
                        <tr className="row-detail">
                          <td colSpan={7}>
                            <DagPanel connection={c} onConnectionChanged={reloadAll} />
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      {lastTest && (
        <Card
          title={`Test result — ${lastTest.connection.name}`}
          actions={
            <Button size="sm" variant="ghost" onClick={() => setLastTest(null)}>
              Dismiss
            </Button>
          }
        >
          <TestResult result={lastTest.result} />
        </Card>
      )}

      {editing?.type === 'airflow' && (
        <AirflowConnectionForm
          connection={editing.connection}
          allowMock={allowMock}
          onClose={() => setEditing(null)}
          onSaved={async () => {
            setEditing(null)
            await reloadAll()
          }}
        />
      )}
      {editing?.type === 'channel' && (
        <NotificationChannelForm
          connection={editing.connection}
          onClose={() => setEditing(null)}
          onSaved={async () => {
            setEditing(null)
            await reloadAll()
          }}
        />
      )}
      {editing?.type === 'database' && (
        <DatabaseConnectionForm
          connection={editing.connection}
          onClose={() => setEditing(null)}
          onSaved={async () => {
            setEditing(null)
            await reloadAll()
          }}
        />
      )}

      {deleting && (
        <Modal
          title="Delete connection?"
          onClose={() => setDeleting(null)}
          footer={
            <>
              <Button onClick={() => setDeleting(null)}>Cancel</Button>
              <Button variant="danger" onClick={onDelete}>
                Delete
              </Button>
            </>
          }
        >
          <p>
            This removes <strong>{deleting.connection.name}</strong>
            {deleting.type === 'airflow' && ' and all of its synced and monitored DAGs'}. The action is
            recorded in the audit log.
          </p>
        </Modal>
      )}
    </div>
  )
}
