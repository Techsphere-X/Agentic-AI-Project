import { useCallback, useEffect, useState } from 'react'
import { Link, useOutletContext, useParams } from 'react-router'
import DiagnosisCard from '../../components/diagnosis/DiagnosisCard'
import { Alert, Button, Card, EmptyState, Field, Modal } from '../../components/ui'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { formatDateTime, formatRelative } from '../../format'
import { automationApi, incidentsApi } from '../../services/endpoints'
import { ApprovalCard } from '../automation/Approvals'
import { RunTable } from '../automation/RunList'
import { typeLabel } from './incidentText'
import { IncidentStatusBadge, SeverityBadge } from './incidentUi'

const EVENT_LABELS = {
  opened: 'Incident opened',
  recurred: 'Failure recurred',
  evidence_added: 'Evidence collected',
  acknowledged: 'Acknowledged',
  resolved: 'Resolved',
  auto_resolved: 'Auto-resolved (recovered)',
  reopened: 'Reopened',
  automation_started: 'Automation started',
  automation_finished: 'Automation finished',
  diagnosed: 'Diagnosed',
  diagnosis_corrected: 'Diagnosis corrected',
  approval_requested: 'Approval requested',
  approved: 'Approved',
  rejected: 'Rejected',
  approval_expired: 'Approval expired',
  action_executed: 'Fix applied',
  action_simulated: 'Fix simulated (dry run)',
  action_failed: 'Fix failed',
  action_denied: 'Fix blocked by policy',
  verified: 'Verified: run succeeded',
  verification_failed: 'Verification failed',
  auto_remediated: 'Resolved by automation',
  escalated: 'Severity raised',
  automation_note: 'Automation note',
  notified: 'Notification sent',
  notify_failed: 'Notification failed',
}

const TABS = [
  { key: 'TASK_LOG', label: 'Logs' },
  { key: 'TASK_INSTANCE', label: 'Tasks' },
  { key: 'RUN_METADATA', label: 'Runs' },
]

function ResolveModal({ onClose, onResolve }) {
  const [note, setNote] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState(null)

  async function submit(event) {
    event.preventDefault()
    if (!note.trim()) return setError('A resolution note is required.')
    setSaving(true)
    try {
      await onResolve(note.trim())
    } catch (err) {
      setError(err.message)
      setSaving(false)
    }
  }

  return (
    <Modal
      title="Resolve incident"
      onClose={onClose}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" type="submit" form="resolve-form" loading={saving}>
            Resolve
          </Button>
        </>
      }
    >
      <form id="resolve-form" className="form-grid" onSubmit={submit}>
        {error && <Alert tone="danger">{error}</Alert>}
        <Field label="Resolution note" hint="What was done or why no action is needed. Recorded in the audit log.">
          {(id) => <textarea id={id} rows={4} value={note} onChange={(e) => setNote(e.target.value)} maxLength={2000} autoFocus />}
        </Field>
      </form>
    </Modal>
  )
}

function LogViewer({ item }) {
  const toast = useToast()
  if (!item.content) {
    return <p className="muted">{item.data?.reason ?? 'Log not available.'}</p>
  }
  async function copy() {
    try {
      await navigator.clipboard.writeText(item.content)
      toast.success('Log copied')
    } catch {
      toast.error('Copy failed')
    }
  }
  return (
    <div className="log-block">
      <div className="log-header">
        <span className="mono small">{item.source}</span>
        <span className="spacer" />
        {item.truncated && <span className="muted small">Showing the end of the log (truncated)</span>}
        <Button size="sm" variant="ghost" onClick={copy}>
          Copy
        </Button>
      </div>
      <pre className="log-viewer">{item.content}</pre>
    </div>
  )
}

function TaskTable({ item }) {
  const rows = item.data?.task_instances ?? []
  if (!rows.length) return <p className="muted">{item.data?.reason ?? 'No task details.'}</p>
  return (
    <div className="table-wrap">
      <p className="mono small muted">{item.source}</p>
      <table className="table">
        <thead>
          <tr>
            <th>Task</th>
            <th>State</th>
            <th>Try</th>
            <th>Duration</th>
            <th>Operator</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((t) => (
            <tr key={t.task_id}>
              <td className="mono">{t.task_id}</td>
              <td>{t.state}</td>
              <td>{t.try_number}</td>
              <td className="small">{t.duration_seconds != null ? `${Math.round(t.duration_seconds)} s` : '—'}</td>
              <td className="small">{t.operator ?? '—'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function RunDetails({ item }) {
  const entries = Object.entries(item.data ?? {}).filter(([, v]) => v !== null && v !== '')
  return (
    <dl className="kv kv-block">
      {entries.map(([k, v]) => (
        <div key={k} className="kv-row">
          <dt>{k.replaceAll('_', ' ')}</dt>
          <dd className={k.endsWith('_date') || k === 'run_id' ? 'mono small' : ''}>
            {k.endsWith('_date') ? formatDateTime(v) : String(v)}
          </dd>
        </div>
      ))}
    </dl>
  )
}

function Evidence({ evidence }) {
  const available = TABS.filter((t) => evidence.some((e) => e.kind === t.key))
  const [tab, setTab] = useState(available[0]?.key)
  if (!available.length) return <EmptyState title="No evidence collected" />
  const items = evidence.filter((e) => e.kind === tab).slice().reverse() // newest first
  return (
    <>
      <div className="tabs" role="tablist">
        {available.map((t) => (
          <button
            key={t.key}
            type="button"
            role="tab"
            aria-selected={tab === t.key}
            className={`tab ${tab === t.key ? 'tab-active' : ''}`}
            onClick={() => setTab(t.key)}
          >
            {t.label} <span className="muted">({evidence.filter((e) => e.kind === t.key).length})</span>
          </button>
        ))}
      </div>
      <div className="evidence-list">
        {items.map((item) => (
          <div key={item.id} className="evidence-item">
            {item.kind === 'TASK_LOG' && <LogViewer item={item} />}
            {item.kind === 'TASK_INSTANCE' && <TaskTable item={item} />}
            {item.kind === 'RUN_METADATA' && <RunDetails item={item} />}
          </div>
        ))}
      </div>
    </>
  )
}

function AutomationEventDetails({ event: e }) {
  const d = e.details ?? {}
  const lines = []
  if (d.workflow && e.event.startsWith('automation_')) lines.push(d.workflow)
  if (e.event === 'diagnosed') {
    const source = d.source === 'model' ? `model${d.model_version ? ` ${d.model_version}` : ''}` : d.source
    lines.push([d.label ?? d.category, source && `via ${source}`, d.backfill && 'backfilled'].filter(Boolean).join(' '))
  }
  if (e.event === 'diagnosis_corrected') {
    lines.push(`${d.previous_category ?? '?'} → ${d.category}`)
  }
  if (d.action) lines.push(d.action)
  if (d.reasons) lines.push(d.reasons.join('; '))
  if (d.error) lines.push(d.error)
  if (e.event === 'escalated' && d.severity?.from) lines.push(`${d.severity.from} → ${d.severity.to}`)
  if (e.event === 'automation_finished' && d.status) lines.push(d.status.toLowerCase())
  if ((e.event === 'verified' || e.event === 'verification_failed') && d.run) {
    lines.push(`Run ${d.run}${d.state ? ` ${d.state}` : ''}`)
  }
  if (e.event === 'notified') lines.push(d.title ?? d.host ?? d.channel)
  if (!lines.length) return null
  return (
    <div className="muted small">
      {lines.join(' · ')}
      {d.run_id && e.event.startsWith('automation_') && (
        <>
          {' '}
          · <Link to={`/automation/runs/${d.run_id}`}>view run</Link>
        </>
      )}
    </div>
  )
}

function AutomationCard({ incidentId, reloadKey, onChanged }) {
  const { hasRole } = useAuth()
  const [runs, setRuns] = useState(null)
  const [approvals, setApprovals] = useState([])

  const load = useCallback(async () => {
    try {
      const [runPage, approvalPage] = await Promise.all([
        automationApi.listRuns({ incident_id: incidentId, limit: 20 }),
        automationApi.listApprovals({ incident_id: incidentId, status: ['PENDING'] }),
      ])
      setRuns(runPage.items)
      setApprovals(approvalPage.items)
    } catch {
      setRuns([])
    }
  }, [incidentId])

  useEffect(() => {
    // eslint-disable-next-line react/set-state-in-effect -- fetch on mount/change; state is set after await
    load()
  }, [load, reloadKey])

  if (runs === null) return null
  return (
    <Card title="Automation">
      {approvals.map((a) => (
        <ApprovalCard
          key={a.id}
          approval={a}
          canDecide={hasRole('ADMIN', 'OPERATOR')}
          onDecided={() => {
            load()
            onChanged()
          }}
        />
      ))}
      {runs.length === 0 ? (
        <p className="muted small">
          No workflow has run for this incident yet. See <Link to="/automation/workflows">Workflows</Link>.
        </p>
      ) : (
        <RunTable runs={runs} showIncident={false} />
      )}
    </Card>
  )
}

function Timeline({ events }) {
  return (
    <ol className="timeline">
      {events
        .slice()
        .reverse()
        .map((e) => (
          <li key={e.id} className={`timeline-item timeline-${e.event}`}>
            <div className="timeline-title">
              <strong>{EVENT_LABELS[e.event] ?? e.event}</strong>
              <span className="muted small"> · {e.actor_name} · {formatRelative(e.created_at)}</span>
            </div>
            {e.details?.note && <div className="small">“{e.details.note}”</div>}
            {e.details?.severity_reasons && (
              <div className="muted small">Severity: {e.details.severity_reasons.join('; ')}</div>
            )}
            {e.details?.recovered_by_run && (
              <div className="muted small mono">Recovered by {e.details.recovered_by_run}</div>
            )}
            {e.event === 'recurred' && e.details?.run_id && (
              <div className="muted small mono">Run {e.details.run_id}</div>
            )}
            <AutomationEventDetails event={e} />
          </li>
        ))}
    </ol>
  )
}

function OccurrenceTimeline({ occurrences }) {
  if (!occurrences?.length) {
    return <p className="muted small">No occurrence history has been recorded yet.</p>
  }
  return (
    <ol className="timeline">
      {occurrences.map((occurrence) => (
        <li key={occurrence.id} className="timeline-item">
          <div className="timeline-title">
            <strong>Occurrence detected</strong>
            <span className="muted small"> · {formatDateTime(occurrence.occurred_at)}</span>
          </div>
          <div className="muted small">
            {occurrence.dag_id} · {typeLabel(occurrence.type)} · {occurrence.severity} · {occurrence.status}
          </div>
          {occurrence.run_id && <div className="muted small mono">Run {occurrence.run_id}</div>}
          {occurrence.summary && <div className="small">{occurrence.summary}</div>}
        </li>
      ))}
    </ol>
  )
}

export default function IncidentDetail() {
  const { id } = useParams()
  const { hasRole } = useAuth()
  const { refreshHealth } = useOutletContext()
  const toast = useToast()
  const [incident, setIncident] = useState(null)
  const [error, setError] = useState(null)
  const [busy, setBusy] = useState(null)
  const [resolving, setResolving] = useState(false)
  const [reloadKey, setReloadKey] = useState(0)
  const canOperate = hasRole('ADMIN', 'OPERATOR')

  const load = useCallback(async () => {
    try {
      setIncident(await incidentsApi.get(id))
      setError(null)
    } catch (err) {
      setError(err.message)
    }
  }, [id])

  useEffect(() => {
    // eslint-disable-next-line react/set-state-in-effect -- fetch on mount/id change; state is set after await
    load()
  }, [load])

  async function act(action, fn, message) {
    setBusy(action)
    try {
      setIncident(await fn())
      setReloadKey((k) => k + 1)
      toast.success(message)
      refreshHealth()
    } catch (err) {
      toast.error(err.message)
    } finally {
      setBusy(null)
    }
  }

  if (error) {
    return (
      <div className="page">
        <Link to="/incidents">← Incidents</Link>
        <Alert tone="danger">{error}</Alert>
      </div>
    )
  }
  if (!incident) return <p className="muted">Loading…</p>

  const isOpen = incident.status !== 'RESOLVED'

  return (
    <div className="page">
      <Link to="/incidents" className="small">
        ← Incidents
      </Link>
      <div className="page-header">
        <div>
          <div className="cell-title">
            <SeverityBadge severity={incident.severity} />
            <IncidentStatusBadge status={incident.status} resolution={incident.resolution} />
            <span className="muted small">{typeLabel(incident.type)}</span>
          </div>
          <h1>{incident.title}</h1>
          {incident.summary && <p className="muted">{incident.summary}</p>}
        </div>
        {canOperate && (
          <div className="card-actions">
            {incident.status === 'OPEN' && (
              <Button
                loading={busy === 'ack'}
                onClick={() => act('ack', () => incidentsApi.acknowledge(incident.id), 'Incident acknowledged')}
              >
                Acknowledge
              </Button>
            )}
            {isOpen && (
              <Button variant="primary" onClick={() => setResolving(true)}>
                Resolve
              </Button>
            )}
            {!isOpen && (
              <Button
                loading={busy === 'reopen'}
                onClick={() => act('reopen', () => incidentsApi.reopen(incident.id), 'Incident reopened')}
              >
                Reopen
              </Button>
            )}
          </div>
        )}
      </div>

      <Card>
        <dl className="kv kv-grid">
          <div className="kv-row">
            <dt>DAG</dt>
            <dd className="mono">{incident.dag_id}</dd>
          </div>
          {incident.run_id && (
            <div className="kv-row">
              <dt>{incident.occurrence_count > 1 ? 'First failing run' : 'Run'}</dt>
              <dd className="mono small">{incident.run_id}</dd>
            </div>
          )}
          {incident.last_run_id && incident.last_run_id !== incident.run_id && (
            <div className="kv-row">
              <dt>Latest failing run</dt>
              <dd className="mono small">{incident.last_run_id}</dd>
            </div>
          )}
          <div className="kv-row">
            <dt>Occurrences</dt>
            <dd>{incident.occurrence_count}</dd>
          </div>
          <div className="kv-row">
            <dt>First seen</dt>
            <dd>{formatDateTime(incident.first_seen_at)}</dd>
          </div>
          <div className="kv-row">
            <dt>Last seen</dt>
            <dd>{formatDateTime(incident.last_seen_at)}</dd>
          </div>
          {incident.resolved_at && (
            <div className="kv-row">
              <dt>Resolved</dt>
              <dd>
                {formatDateTime(incident.resolved_at)}
                {incident.resolution_note && <div className="small">“{incident.resolution_note}”</div>}
              </dd>
            </div>
          )}
        </dl>
      </Card>

      {incident.diagnosis && (
        <DiagnosisCard
          diagnosis={incident.diagnosis}
          canCorrect={canOperate}
          onCorrect={async (category, note) => {
            setIncident(await incidentsApi.setDiagnosis(incident.id, category, note))
            toast.success('Diagnosis corrected')
          }}
        />
      )}
      <AutomationCard
        incidentId={incident.id}
        reloadKey={reloadKey}
        onChanged={() => {
          load()
          refreshHealth()
        }}
      />

      <div className="detail-grid">
        <Card title="Occurrence history">
          <OccurrenceTimeline occurrences={incident.occurrences} />
        </Card>
        <Card title="Evidence">
          <Evidence evidence={incident.evidence} />
        </Card>
        <Card title="Timeline">
          <Timeline events={incident.events} />
        </Card>
      </div>

      {resolving && (
        <ResolveModal
          onClose={() => setResolving(false)}
          onResolve={async (note) => {
            const updated = await incidentsApi.resolve(incident.id, note)
            setIncident(updated)
            setReloadKey((k) => k + 1)
            setResolving(false)
            toast.success('Incident resolved')
            refreshHealth()
          }}
        />
      )}
    </div>
  )
}
