import { useCallback, useEffect, useState } from 'react'
import { Link, useOutletContext } from 'react-router'
import { Badge, Button, Card, EmptyState } from '../../components/ui'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { formatDateTime, formatRelative } from '../../format'
import { automationApi } from '../../services/endpoints'
import { SeverityBadge } from '../incidents/incidentUi'
import { ApprovalStatusBadge, DecisionModal } from './automationUi'

function timeLeft(expiresAt) {
  const minutes = Math.round((new Date(expiresAt).getTime() - Date.now()) / 60000)
  if (minutes <= 0) return 'expiring'
  if (minutes < 60) return `${minutes} min left`
  return `${Math.round(minutes / 60)} h left`
}

export function ApprovalCard({ approval, canDecide, onDecided }) {
  const toast = useToast()
  const [deciding, setDeciding] = useState(null) // 'approve' | 'reject'
  const env = approval.proposed_action?.environment

  async function decide(comment) {
    const fn = deciding === 'approve' ? automationApi.approve : automationApi.reject
    await fn(approval.id, comment || null)
    toast.success(deciding === 'approve' ? 'Approved: the workflow continues' : 'Rejected: no action taken')
    setDeciding(null)
    onDecided()
  }

  return (
    <div className="approval-card">
      <div className="approval-main">
        <div className="cell-title">
          {env && <Badge tone={env === 'PROD' ? 'danger' : 'neutral'}>{env}</Badge>}
          {approval.incident && <SeverityBadge severity={approval.incident.severity} />}
          <span className="muted small">{approval.workflow_name}</span>
        </div>
        <strong>{approval.title}</strong>
        {approval.summary && <p className="muted small">{approval.summary}</p>}
        <div className="muted small">
          {approval.incident && (
            <>
              <Link to={`/incidents/${approval.incident_id}`}>{approval.incident.title}</Link> ·{' '}
            </>
          )}
          requested {formatRelative(approval.created_at)} · {timeLeft(approval.expires_at)}
        </div>
      </div>
      {canDecide && (
        <div className="card-actions">
          <Button onClick={() => setDeciding('reject')}>Reject</Button>
          <Button variant="primary" onClick={() => setDeciding('approve')}>
            Approve
          </Button>
        </div>
      )}
      {deciding && (
        <DecisionModal
          approval={approval}
          approve={deciding === 'approve'}
          onClose={() => setDeciding(null)}
          onDecide={decide}
        />
      )}
    </div>
  )
}

export default function Approvals() {
  const { hasRole } = useAuth()
  const { refreshHealth } = useOutletContext()
  const toast = useToast()
  const [tab, setTab] = useState('PENDING')
  const [page, setPage] = useState(null)
  const canDecide = hasRole('ADMIN', 'OPERATOR')

  const load = useCallback(async () => {
    try {
      const status = tab === 'PENDING' ? ['PENDING'] : ['APPROVED', 'REJECTED', 'EXPIRED']
      setPage(await automationApi.listApprovals({ status, limit: 100 }))
    } catch (err) {
      toast.error(err.message)
    }
  }, [tab, toast])

  useEffect(() => {
    // eslint-disable-next-line react/set-state-in-effect -- fetch on mount/tab change; state is set after await
    load()
  }, [load])

  function onDecided() {
    load()
    refreshHealth()
  }

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Approvals</h1>
        </div>
      </div>

      <Card>
        <div className="tabs" role="tablist">
          {[
            ['PENDING', 'Waiting for you'],
            ['DECIDED', 'History'],
          ].map(([key, label]) => (
            <button
              key={key}
              type="button"
              role="tab"
              aria-selected={tab === key}
              className={`tab ${tab === key ? 'tab-active' : ''}`}
              onClick={() => {
                setPage(null)
                setTab(key)
              }}
            >
              {label}
            </button>
          ))}
        </div>

        {!page ? (
          <p className="muted">Loading…</p>
        ) : page.total === 0 ? (
          <EmptyState title={tab === 'PENDING' ? 'Nothing waiting for approval' : 'No decisions yet'} />
        ) : tab === 'PENDING' ? (
          <div className="approval-list">
            {page.items.map((a) => (
              <ApprovalCard key={a.id} approval={a} canDecide={canDecide} onDecided={onDecided} />
            ))}
          </div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Decision</th>
                  <th>Action</th>
                  <th>By</th>
                  <th>When</th>
                </tr>
              </thead>
              <tbody>
                {page.items.map((a) => (
                  <tr key={a.id}>
                    <td>
                      <ApprovalStatusBadge status={a.status} />
                    </td>
                    <td>
                      <div className="cell-title">{a.title}</div>
                      <div className="muted small">
                        {a.workflow_name}
                        {a.comment && <> · “{a.comment}”</>}
                      </div>
                    </td>
                    <td className="small">{a.decided_by_name ?? (a.status === 'EXPIRED' ? 'Timed out' : '—')}</td>
                    <td className="small">{formatDateTime(a.decided_at)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  )
}
