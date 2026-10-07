import { useCallback, useEffect, useState } from 'react'
import { NavLink, Outlet } from 'react-router'
import NavIcon from '../components/NavIcon'
import { StatusPill } from '../components/ui'
import { useAuth } from '../context/AuthContext'
import { automationApi, healthApi, incidentsApi } from '../services/endpoints'

const HEALTH_POLL_MS = 30_000
const COLLAPSED_KEY = 'sidebar-collapsed'

// Sidebar sections. `badge` picks a count from the polled summaries; `admin` hides a section
// from non-admins.
const NAV = [
  { items: [{ to: '/', end: true, label: 'Dashboard', icon: 'dashboard' }] },
  {
    title: 'Connections',
    items: [
      { to: '/connections/airflow', label: 'Airflow', icon: 'airflow' },
      { to: '/connections/databases', label: 'Databases', icon: 'database' },
      { to: '/connections/channels', label: 'Channels', icon: 'channel' },
    ],
  },
  {
    title: 'Workflow orchestration',
    items: [
      { to: '/automation/workflows', label: 'Workflows', icon: 'workflows' },
      { to: '/automation/runs', label: 'Runs', icon: 'runs' },
      {
        to: '/automation/approvals',
        label: 'Approvals',
        icon: 'approvals',
        badge: ({ automation }) => automation?.pending_approvals,
        tone: 'warning',
        badgeTitle: 'Waiting for approval',
      },
      {
        to: '/incidents',
        label: 'Incidents',
        icon: 'incidents',
        badge: ({ incidents }) => incidents?.open_total,
        badgeTitle: 'Open incidents',
      },
      {
        to: '/automation/notifications',
        label: 'Notifications',
        icon: 'notifications',
        badge: ({ automation }) => automation?.unread_notifications,
        tone: 'neutral',
        badgeTitle: 'Unread',
      },
    ],
  },
  { title: 'Diagnosis', items: [{ to: '/diagnosis/analyzer', label: 'Log analyzer', icon: 'analyzer' }] },
  { title: 'Settings', admin: true, items: [{ to: '/settings/users', label: 'Users', icon: 'users' }] },
]

function readCollapsed() {
  try {
    return localStorage.getItem(COLLAPSED_KEY) === '1'
  } catch {
    return false
  }
}

function HealthIndicator({ health, error }) {
  if (error) return <StatusPill status="error" label="API unreachable" title={error} />
  if (!health) return <StatusPill status="UNKNOWN" label="Checking…" />
  const db = health.database.status
  const databases = health.databases ?? []
  const failing = [...health.airflow, ...databases].filter((a) => !['HEALTHY', 'UNKNOWN'].includes(a.status))
  const title = [
    `Platform database: ${db}`,
    ...health.airflow.map((a) => `Airflow “${a.name}”: ${a.status}`),
    ...databases.map((d) => `Database “${d.name}”: ${d.status}`),
  ].join('\n')
  const label =
    health.status === 'ok'
      ? 'All systems OK'
      : db !== 'ok'
        ? 'Platform database error'
        : failing.length
          ? `${failing.length} connection issue${failing.length === 1 ? '' : 's'}`
          : 'Connection status unknown'
  return <StatusPill status={health.status} label={label} title={title} />
}

export default function AppLayout() {
  const { user, logout, hasRole } = useAuth()
  const [health, setHealth] = useState(null)
  const [healthError, setHealthError] = useState(null)
  const [incidentSummary, setIncidentSummary] = useState(null)
  const [automationSummary, setAutomationSummary] = useState(null)
  const [collapsed, setCollapsed] = useState(readCollapsed)

  const toggleSidebar = useCallback(() => {
    setCollapsed((c) => {
      try {
        localStorage.setItem(COLLAPSED_KEY, c ? '0' : '1')
      } catch {
        // storage unavailable (private mode): the choice lasts for this visit only
      }
      return !c
    })
  }, [])

  // Ctrl/Cmd+B toggles the sidebar, as in most editors.
  useEffect(() => {
    const onKey = (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'b' && !e.target.closest?.('input, textarea, select, [contenteditable]')) {
        e.preventDefault()
        toggleSidebar()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [toggleSidebar])

  // Refreshes the header health pill and the sidebar badges together.
  const refreshHealth = useCallback(async () => {
    const [healthResult, summaryResult, automationResult] = await Promise.allSettled([
      healthApi.status(),
      incidentsApi.summary(),
      automationApi.summary(),
    ])
    if (healthResult.status === 'fulfilled') {
      setHealth(healthResult.value)
      setHealthError(null)
    } else {
      setHealthError(healthResult.reason.message)
    }
    if (summaryResult.status === 'fulfilled') setIncidentSummary(summaryResult.value)
    if (automationResult.status === 'fulfilled') setAutomationSummary(automationResult.value)
  }, [])

  useEffect(() => {
    // eslint-disable-next-line react/set-state-in-effect -- initial fetch; state is set after await
    refreshHealth()
    const timer = setInterval(refreshHealth, HEALTH_POLL_MS)
    return () => clearInterval(timer)
  }, [refreshHealth])

  const navClass = ({ isActive }) => `nav-link ${isActive ? 'active' : ''}`
  const counts = { automation: automationSummary, incidents: incidentSummary }

  return (
    <div className={`app-shell ${collapsed ? 'sidebar-collapsed' : ''}`}>
      <aside className="sidebar">
        <div className="brand" title="Agentic Ops">
          <span className="brand-mark" aria-hidden="true">◆</span>
          <span className="nav-label">Agentic Ops</span>
        </div>
        <nav aria-label="Main">
          {NAV.filter((section) => !section.admin || hasRole('ADMIN')).map((section) => (
            <div key={section.title ?? 'home'} className="nav-group">
              {section.title && <div className="nav-section">{section.title}</div>}
              {section.items.map((item) => {
                const count = item.badge?.(counts) ?? 0
                return (
                  <NavLink
                    key={item.to}
                    to={item.to}
                    end={item.end}
                    className={navClass}
                    title={collapsed ? (count > 0 ? `${item.label} (${count})` : item.label) : undefined}
                  >
                    <NavIcon name={item.icon} />
                    <span className="nav-label">{item.label}</span>
                    {count > 0 && (
                      <span className={`nav-badge ${item.tone ? `nav-badge-${item.tone}` : ''}`} title={item.badgeTitle}>
                        {count}
                      </span>
                    )}
                  </NavLink>
                )
              })}
            </div>
          ))}
        </nav>
        <button
          type="button"
          className="sidebar-toggle"
          onClick={toggleSidebar}
          aria-expanded={!collapsed}
          title={`${collapsed ? 'Expand' : 'Collapse'} sidebar (Ctrl+B)`}
        >
          <NavIcon name={collapsed ? 'expand' : 'collapse'} />
          <span className="nav-label">Collapse</span>
        </button>
      </aside>

      <div className="main">
        <header className="topbar">
          <HealthIndicator health={health} error={healthError} />
          <div className="user-menu">
            <div className="user-meta">
              <span className="user-name">{user.full_name}</span>
              <span className="user-role">{user.role.toLowerCase()}</span>
            </div>
            <button type="button" className="btn btn-ghost btn-sm" onClick={logout}>
              Sign out
            </button>
          </div>
        </header>
        <main className="content">
          <Outlet context={{ health, incidentSummary, automationSummary, refreshHealth }} />
        </main>
      </div>
    </div>
  )
}
