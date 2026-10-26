import { useMemo, useState } from 'react'
import { Link } from 'react-router'
import { Badge, StatusPill } from '../../components/ui'
import { formatRelative } from '../../format'

const TYPE_ICONS = {
  airflow: '🌀',
  database: '🗄️',
  channel: '🔔',
  workflow_engine: '⚡',
}

const TYPE_LABELS = {
  airflow: 'Airflow',
  database: 'Database',
  channel: 'Notification',
  workflow_engine: 'Workflow Engine',
}

const TYPE_LINKS = {
  airflow: '/connections/airflow',
  database: '/connections/database',
  channel: '/connections/channel',
  workflow_engine: '/automation/workflows',
}

export default function ConnectorMetricsSection({ data, loading, onRefresh }) {
  const [selectedType, setSelectedType] = useState('all')
  const [sortBy, setSortBy] = useState('failures') // 'failures' | 'volume' | 'rate' | 'name'
  const [hoveredConnector, setHoveredConnector] = useState(null)
  const [selectedConnectorId, setSelectedConnectorId] = useState(null)
  const [chartMode, setChartMode] = useState('stacked') // 'stacked' | 'rate'

  const connectors = data?.connectors ?? []

  // Filter by type
  const filtered = useMemo(() => {
    let list = connectors
    if (selectedType !== 'all') {
      list = list.filter((c) => c.type === selectedType)
    }

    // Sort
    return [...list].sort((a, b) => {
      if (sortBy === 'failures') {
        if (b.failed_runs !== a.failed_runs) return b.failed_runs - a.failed_runs
        return b.total_runs - a.total_runs
      }
      if (sortBy === 'volume') {
        return b.total_runs - a.total_runs
      }
      if (sortBy === 'rate') {
        return b.success_rate - a.success_rate
      }
      if (sortBy === 'name') {
        return a.name.localeCompare(b.name)
      }
      return 0
    })
  }, [connectors, selectedType, sortBy])

  // Count by type for filter tabs
  const typeCounts = useMemo(() => {
    const counts = { all: connectors.length, airflow: 0, database: 0, channel: 0 }
    connectors.forEach((c) => {
      if (counts[c.type] !== undefined) counts[c.type]++
    })
    return counts
  }, [connectors])

  // Maximum runs for chart scaling
  const maxRuns = useMemo(() => {
    const max = Math.max(...connectors.map((c) => c.total_runs), 1)
    return Math.ceil(max * 1.1)
  }, [connectors])

  const totalRuns = data?.total_runs ?? 0
  const totalSuccess = data?.total_success ?? 0
  const totalFailed = data?.total_failed ?? 0
  const overallRate = data?.overall_success_rate ?? 100.0

  return (
    <div className="connector-kpi-section">
      {/* ----------------- Section Header & Controls ----------------- */}
      <div className="connector-kpi-header">
        <div>
          <div className="connector-kpi-title-row">
            <span className="connector-kpi-badge">OBSERVABILITY</span>
            <h2>Connector Run Performance & Reliability Tracker</h2>
          </div>
          <p className="connector-kpi-subtitle">
            Real-time execution telemetry tracking total runs, successes, and failure rates across pipeline and database connectors.
          </p>
        </div>

        <div className="connector-kpi-actions">
          <div className="chart-mode-toggle" role="group" aria-label="Chart mode">
            <button
              type="button"
              className={`mode-btn ${chartMode === 'stacked' ? 'active' : ''}`}
              onClick={() => setChartMode('stacked')}
              title="Show run volume breakdown (success vs failure)"
            >
              📊 Run Volume
            </button>
            <button
              type="button"
              className={`mode-btn ${chartMode === 'rate' ? 'active' : ''}`}
              onClick={() => setChartMode('rate')}
              title="Show reliability percentage"
            >
              🎯 Success Rate %
            </button>
          </div>

          {onRefresh && (
            <button
              type="button"
              className="btn btn-secondary btn-sm refresh-btn"
              onClick={onRefresh}
              disabled={loading}
              title="Refresh connector metrics"
            >
              <span className={`refresh-icon ${loading ? 'spinning' : ''}`}>🔄</span>
              <span>Refresh</span>
            </button>
          )}
        </div>
      </div>

      {/* ----------------- Summary Stat Badges ----------------- */}
      <div className="connector-kpi-summary-grid">
        <div className="metric-pill-card">
          <div className="metric-pill-icon metric-icon-primary">⚡</div>
          <div className="metric-pill-body">
            <span className="metric-pill-label">Total Executions</span>
            <span className="metric-pill-val">{totalRuns.toLocaleString()}</span>
            <span className="metric-pill-sub">across {connectors.length} active connectors</span>
          </div>
        </div>

        <div className="metric-pill-card">
          <div className="metric-pill-icon metric-icon-success">✓</div>
          <div className="metric-pill-body">
            <span className="metric-pill-label">Successful Runs</span>
            <span className="metric-pill-val text-success">{totalSuccess.toLocaleString()}</span>
            <span className="metric-pill-sub">
              {totalRuns > 0 ? `${Math.round((totalSuccess / totalRuns) * 100)}% of executions` : '100%'}
            </span>
          </div>
        </div>

        <div className="metric-pill-card">
          <div className="metric-pill-icon metric-icon-danger">✕</div>
          <div className="metric-pill-body">
            <span className="metric-pill-label">Failed Runs</span>
            <span className={`metric-pill-val ${totalFailed > 0 ? 'text-danger' : 'text-muted'}`}>
              {totalFailed.toLocaleString()}
            </span>
            <span className="metric-pill-sub">
              {totalFailed > 0 ? `${Math.round((totalFailed / totalRuns) * 100)}% failure rate` : 'Zero errors recorded'}
            </span>
          </div>
        </div>

        <div className="metric-pill-card">
          <div className="metric-pill-icon metric-icon-accent">🎯</div>
          <div className="metric-pill-body">
            <span className="metric-pill-label">Fleet Success Rate</span>
            <span className="metric-pill-val text-accent">{overallRate}%</span>
            <span className="metric-pill-sub">
              {overallRate >= 95 ? '⭐ Highly reliable' : overallRate >= 80 ? '⚠️ Needs attention' : '🚨 Critical attention'}
            </span>
          </div>
        </div>
      </div>

      {/* ----------------- Filter Chips ----------------- */}
      <div className="connector-filter-bar">
        <div className="filter-chips">
          <button
            type="button"
            className={`filter-chip ${selectedType === 'all' ? 'active' : ''}`}
            onClick={() => setSelectedType('all')}
          >
            All Connectors <span className="chip-count">{typeCounts.all}</span>
          </button>
          <button
            type="button"
            className={`filter-chip ${selectedType === 'airflow' ? 'active' : ''}`}
            onClick={() => setSelectedType('airflow')}
          >
            🌀 Airflow <span className="chip-count">{typeCounts.airflow}</span>
          </button>
          <button
            type="button"
            className={`filter-chip ${selectedType === 'database' ? 'active' : ''}`}
            onClick={() => setSelectedType('database')}
          >
            🗄️ Databases <span className="chip-count">{typeCounts.database}</span>
          </button>
          <button
            type="button"
            className={`filter-chip ${selectedType === 'channel' ? 'active' : ''}`}
            onClick={() => setSelectedType('channel')}
          >
            🔔 Channels <span className="chip-count">{typeCounts.channel}</span>
          </button>
        </div>

        <div className="sort-controls">
          <span className="sort-label">Rank by:</span>
          <select
            className="sort-select"
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            aria-label="Sort connectors"
          >
            <option value="failures">🚨 Most Failures (Risk Priority)</option>
            <option value="volume">📈 Highest Volume (Total Runs)</option>
            <option value="rate">🎯 Highest Success Rate</option>
            <option value="name">🔤 Name (A - Z)</option>
          </select>
        </div>
      </div>

      {/* ----------------- Visual KPI Graph ----------------- */}
      <div className="connector-graph-card">
        <div className="graph-header">
          <div className="graph-title-group">
            <span className="graph-heading">
              {chartMode === 'stacked'
                ? 'Execution Volume & Success/Failure Distribution'
                : 'Reliability & Success Rate Comparison'}
            </span>
            <span className="graph-hint">Hover over any bar to inspect connector telemetry</span>
          </div>
          <div className="graph-legend">
            <span className="legend-item">
              <span className="legend-dot success-dot" /> Successful Runs
            </span>
            <span className="legend-item">
              <span className="legend-dot danger-dot" /> Failed Runs
            </span>
          </div>
        </div>

        {loading ? (
          <div className="graph-loading">
            <div className="graph-skeleton-bar" />
            <div className="graph-skeleton-bar" />
            <div className="graph-skeleton-bar" />
          </div>
        ) : filtered.length === 0 ? (
          <div className="graph-empty">No connectors matched the selected filter.</div>
        ) : (
          <div className="connector-bars-container">
            {filtered.map((conn) => {
              const isSelected = selectedConnectorId === conn.id
              const isHovered = hoveredConnector?.id === conn.id
              const successWidth = chartMode === 'stacked'
                ? (conn.success_runs / maxRuns) * 100
                : conn.success_rate
              const failedWidth = chartMode === 'stacked'
                ? (conn.failed_runs / maxRuns) * 100
                : 100 - conn.success_rate

              return (
                <div
                  key={conn.id}
                  className={`connector-bar-row ${isSelected ? 'row-selected' : ''} ${isHovered ? 'row-hovered' : ''}`}
                  onMouseEnter={() => setHoveredConnector(conn)}
                  onMouseLeave={() => setHoveredConnector(null)}
                  onClick={() => setSelectedConnectorId(isSelected ? null : conn.id)}
                  tabIndex={0}
                  role="button"
                  aria-pressed={isSelected}
                  onKeyDown={(e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                      setSelectedConnectorId(isSelected ? null : conn.id)
                    }
                  }}
                >
                  <div className="bar-label-col">
                    <span className="connector-icon" aria-hidden="true">{TYPE_ICONS[conn.type] ?? '🔌'}</span>
                    <span className="connector-name" title={conn.name}>{conn.name}</span>
                    <Badge tone={conn.environment === 'PROD' ? 'danger' : 'neutral'} title={`Environment: ${conn.environment}`}>
                      {conn.environment ?? 'DEV'}
                    </Badge>
                  </div>

                  <div className="bar-track-col">
                    <div className="bar-track">
                      {conn.total_runs === 0 ? (
                        <div className="bar-segment segment-empty" style={{ width: '100%' }}>
                          <span>No runs recorded yet</span>
                        </div>
                      ) : (
                        <>
                          {successWidth > 0 && (
                            <div
                              className="bar-segment segment-success"
                              style={{ width: `${Math.max(successWidth, 1)}%` }}
                              title={`${conn.success_runs} successful runs (${conn.success_rate}%)`}
                            />
                          )}
                          {failedWidth > 0 && (
                            <div
                              className="bar-segment segment-danger"
                              style={{ width: `${Math.max(failedWidth, 1)}%` }}
                              title={`${conn.failed_runs} failed runs`}
                            />
                          )}
                        </>
                      )}
                    </div>
                  </div>

                  <div className="bar-metrics-col">
                    <div className="metric-pill success-text">
                      <span className="dot dot-success" />
                      <strong>{conn.success_runs}</strong> ok
                    </div>
                    {conn.failed_runs > 0 ? (
                      <div className="metric-pill danger-text">
                        <span className="dot dot-danger" />
                        <strong>{conn.failed_runs}</strong> err
                      </div>
                    ) : (
                      <div className="metric-pill muted-text">0 err</div>
                    )}
                    <div className="rate-badge">
                      <strong>{conn.success_rate}%</strong>
                    </div>
                  </div>
                </div>
              )
            })}
          </div>
        )}

        {/* Hovered Tooltip Card */}
        {hoveredConnector && (
          <div className="connector-tooltip-card">
            <div className="tooltip-header">
              <span className="tooltip-icon">{TYPE_ICONS[hoveredConnector.type]}</span>
              <div>
                <strong>{hoveredConnector.name}</strong>
                <span className="tooltip-type">{TYPE_LABELS[hoveredConnector.type]} · {hoveredConnector.kind}</span>
              </div>
              <StatusPill status={hoveredConnector.status} label={hoveredConnector.status} />
            </div>
            <div className="tooltip-grid">
              <div>
                <span className="tooltip-stat-label">Total Runs</span>
                <span className="tooltip-stat-val">{hoveredConnector.total_runs}</span>
              </div>
              <div>
                <span className="tooltip-stat-label">Success Rate</span>
                <span className="tooltip-stat-val text-success">{hoveredConnector.success_rate}%</span>
              </div>
              <div>
                <span className="tooltip-stat-label">Failures</span>
                <span className={`tooltip-stat-val ${hoveredConnector.failed_runs > 0 ? 'text-danger' : ''}`}>
                  {hoveredConnector.failed_runs}
                </span>
              </div>
              <div>
                <span className="tooltip-stat-label">Latency</span>
                <span className="tooltip-stat-val">
                  {hoveredConnector.latency_ms != null ? `${hoveredConnector.latency_ms} ms` : '—'}
                </span>
              </div>
            </div>
          </div>
        )}
      </div>

      {/* ----------------- Ranking & Listing Leaderboard ----------------- */}
      <div className="connector-leaderboard-card">
        <div className="leaderboard-header">
          <div>
            <h3>Connector Reliability & Performance Leaderboard</h3>
            <span className="leaderboard-subtitle">
              Ranked breakdown of all connected infrastructure with individual health states and execution outcomes.
            </span>
          </div>
          <span className="leaderboard-counter">
            Showing {filtered.length} of {connectors.length} connectors
          </span>
        </div>

        <div className="leaderboard-table-wrapper">
          <table className="leaderboard-table">
            <thead>
              <tr>
                <th style={{ width: '60px' }}>Rank</th>
                <th>Connector</th>
                <th>Type & Environment</th>
                <th>Health Status</th>
                <th style={{ textAlign: 'right' }}>Total Runs</th>
                <th style={{ textAlign: 'right' }}>Success</th>
                <th style={{ textAlign: 'right' }}>Failed</th>
                <th style={{ width: '180px' }}>Success Ratio</th>
                <th style={{ textAlign: 'center' }}>Action</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((conn, index) => {
                const rank = index + 1
                const isSelected = selectedConnectorId === conn.id
                const targetLink = TYPE_LINKS[conn.type] ?? '/connections/catalog'

                return (
                  <tr
                    key={conn.id}
                    className={`leaderboard-row ${isSelected ? 'row-highlighted' : ''}`}
                    onClick={() => setSelectedConnectorId(isSelected ? null : conn.id)}
                  >
                    {/* Rank Badge */}
                    <td>
                      <span className={`rank-badge rank-${rank <= 3 ? rank : 'normal'}`}>
                        {rank === 1 ? '🥇 1' : rank === 2 ? '🥈 2' : rank === 3 ? '🥉 3' : `#${rank}`}
                      </span>
                    </td>

                    {/* Connector Info */}
                    <td>
                      <div className="table-connector-cell">
                        <span className="cell-icon" aria-hidden="true">{TYPE_ICONS[conn.type] ?? '🔌'}</span>
                        <div className="cell-details">
                          <Link to={targetLink} className="cell-title" onClick={(e) => e.stopPropagation()}>
                            {conn.name}
                          </Link>
                          <span className="cell-sub">{conn.description || `${conn.kind || conn.type}`}</span>
                        </div>
                      </div>
                    </td>

                    {/* Type & Env */}
                    <td>
                      <div className="cell-pill-group">
                        <Badge tone="neutral">{TYPE_LABELS[conn.type] ?? conn.type}</Badge>
                        <Badge tone={conn.environment === 'PROD' ? 'danger' : 'neutral'}>
                          {conn.environment ?? 'DEV'}
                        </Badge>
                      </div>
                    </td>

                    {/* Health Status */}
                    <td>
                      <div className="cell-health-group">
                        <StatusPill status={conn.status} label={conn.status} />
                        {conn.latency_ms != null && (
                          <span className="cell-latency">{conn.latency_ms} ms</span>
                        )}
                      </div>
                    </td>

                    {/* Total Runs */}
                    <td style={{ textAlign: 'right', fontWeight: 600 }}>
                      {conn.total_runs.toLocaleString()}
                    </td>

                    {/* Success */}
                    <td style={{ textAlign: 'right' }}>
                      <span className="badge badge-success small">{conn.success_runs.toLocaleString()}</span>
                    </td>

                    {/* Failed */}
                    <td style={{ textAlign: 'right' }}>
                      {conn.failed_runs > 0 ? (
                        <span className="badge badge-danger small">{conn.failed_runs.toLocaleString()}</span>
                      ) : (
                        <span className="text-muted small">0</span>
                      )}
                    </td>

                    {/* Success Ratio Bar & Rate */}
                    <td>
                      <div className="ratio-cell">
                        <div className="ratio-bar-track">
                          <div
                            className="ratio-bar-fill"
                            style={{
                              width: `${conn.success_rate}%`,
                              background: conn.success_rate >= 90
                                ? 'linear-gradient(90deg, #10b981, #059669)'
                                : conn.success_rate >= 75
                                ? 'linear-gradient(90deg, #f59e0b, #d97706)'
                                : 'linear-gradient(90deg, #ef4444, #dc2626)',
                            }}
                          />
                        </div>
                        <span
                          className="ratio-percent"
                          style={{
                            color: conn.success_rate >= 90 ? 'var(--success)' : conn.success_rate >= 75 ? 'var(--warning)' : 'var(--danger)',
                          }}
                        >
                          {conn.success_rate}%
                        </span>
                      </div>
                    </td>

                    {/* Action */}
                    <td style={{ textAlign: 'center' }}>
                      <Link
                        to={targetLink}
                        className="btn btn-secondary btn-sm table-action-btn"
                        onClick={(e) => e.stopPropagation()}
                        title={`Manage ${conn.name}`}
                      >
                        Inspect →
                      </Link>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
