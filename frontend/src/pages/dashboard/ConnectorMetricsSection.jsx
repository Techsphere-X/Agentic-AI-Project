import { useState } from 'react'
import { Link } from 'react-router'
import { Badge, StatusPill } from '../../components/ui'

const TYPE_LABELS = {
  airflow: 'Airflow',
  database: 'Database',
  channel: 'Notification',
  workflow_engine: 'Workflow Engine',
}

const TYPE_LINKS = {
  airflow: '/connections/airflow',
  database: '/connections/databases',
  channel: '/connections/channels',
  workflow_engine: '/automation/workflows',
}

function manageLink(connector) {
  const path = TYPE_LINKS[connector.type] ?? '/connections/airflow'
  return `${path}?open=${encodeURIComponent(connector.id)}`
}

export default function ConnectorMetricsSection({ data, loading }) {
  const [selectedConnectorId, setSelectedConnectorId] = useState(null)
  const connectors = data?.connectors ?? []
  const totalExecutions = data?.total_executions ?? 0
  const successfulExecutions = data?.successful_executions ?? 0
  const failedExecutions = data?.failed_executions ?? 0
  const successRate = data?.execution_success_rate ?? 0

  function toggleConnector(id) {
    setSelectedConnectorId((current) => (current === id ? null : id))
  }

  return (
    <section className="connector-kpi-section" aria-labelledby="execution-statistics-title">
      <div className="connector-kpi-header">
        <div>
          <div className="connector-kpi-title-row">
            <h2 id="execution-statistics-title">Workflow execution statistics</h2>
          </div>
          <p className="connector-kpi-subtitle">
            Counts and success rate from the workflow execution history.
          </p>
        </div>
      </div>

      <div className="connector-kpi-summary-grid">
        <div className="metric-pill-card">
          <div className="metric-pill-body">
            <span className="metric-pill-label">Total Executions</span>
            <span className="metric-pill-val">{totalExecutions.toLocaleString()}</span>
          </div>
        </div>
        <div className="metric-pill-card">
          <div className="metric-pill-body">
            <span className="metric-pill-label">Successful Runs</span>
            <span className="metric-pill-val text-success">{successfulExecutions.toLocaleString()}</span>
          </div>
        </div>
        <div className="metric-pill-card">
          <div className="metric-pill-body">
            <span className="metric-pill-label">Failed Runs</span>
            <span className={`metric-pill-val ${failedExecutions ? 'text-danger' : 'text-muted'}`}>
              {failedExecutions.toLocaleString()}
            </span>
          </div>
        </div>
        <div className="metric-pill-card">
          <div className="metric-pill-body">
            <span className="metric-pill-label">Fleet Success Rate</span>
            <span className="metric-pill-val text-accent">{successRate}%</span>
          </div>
        </div>
      </div>

      <div className="connector-leaderboard-card">
        <div className="leaderboard-header">
          <div>
            <h3>Connections and operational status</h3>
            <span className="leaderboard-subtitle">
              Select a connection to view its status and management options.
            </span>
          </div>
          <span className="leaderboard-counter">{connectors.length} connections</span>
        </div>

        {loading ? (
          <p className="muted">Loading connection status…</p>
        ) : connectors.length === 0 ? (
          <p className="muted">No active connections are configured.</p>
        ) : (
          <div className="leaderboard-table-wrapper">
            <table className="leaderboard-table">
              <thead>
                <tr>
                  <th>Connection</th>
                  <th>Type</th>
                  <th>Status</th>
                  <th style={{ textAlign: 'right' }}>Executions</th>
                  <th style={{ textAlign: 'center' }}>Management</th>
                </tr>
              </thead>
              <tbody>
                {connectors.map((connector) => {
                  const selected = selectedConnectorId === connector.id
                  return (
                    <tr
                      key={connector.id}
                      className={`leaderboard-row ${selected ? 'row-highlighted' : ''}`}
                      onClick={() => toggleConnector(connector.id)}
                      onKeyDown={(event) => {
                        if (event.key === 'Enter' || event.key === ' ') {
                          event.preventDefault()
                          toggleConnector(connector.id)
                        }
                      }}
                      tabIndex={0}
                      aria-expanded={selected}
                    >
                      <td>
                        <div className="cell-details">
                          <span className="cell-title">{connector.name}</span>
                          <span className="cell-sub">{connector.description || connector.kind || 'Connection'}</span>
                        </div>
                      </td>
                      <td>
                        <Badge tone="neutral">{TYPE_LABELS[connector.type] ?? connector.type}</Badge>
                      </td>
                      <td><StatusPill status={connector.status} label={connector.status} /></td>
                      <td style={{ textAlign: 'right' }}>{connector.total_runs.toLocaleString()}</td>
                      <td style={{ textAlign: 'center' }}>
                        <Link
                          to={manageLink(connector)}
                          className="btn btn-secondary btn-sm table-action-btn"
                          onClick={(event) => event.stopPropagation()}
                        >
                          Manage
                        </Link>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}

        {selectedConnectorId && (
          <div className="connector-selection-detail">
            {(() => {
              const connector = connectors.find((item) => item.id === selectedConnectorId)
              if (!connector) return null
              return (
                <>
                  <div>
                    <strong>{connector.name}</strong>
                    <p className="muted small">
                      {TYPE_LABELS[connector.type] ?? connector.type} · {connector.status}
                      {connector.latency_ms != null ? ` · ${connector.latency_ms} ms` : ''}
                    </p>
                  </div>
                  <Link to={manageLink(connector)} className="btn btn-primary btn-sm">
                    Open details
                  </Link>
                </>
              )
            })()}
          </div>
        )}
      </div>
    </section>
  )
}
