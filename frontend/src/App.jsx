import { lazy, Suspense } from 'react'
import { Navigate, Route, Routes, useSearchParams } from 'react-router'
import { RedirectIfAuthenticated, RequireAuth, RequireRole } from './components/RouteGuards'
import AppLayout from './layouts/AppLayout'
import AuthLayout from './layouts/AuthLayout'
import Login from './pages/auth/Login'
import Approvals from './pages/automation/Approvals'
import Notifications from './pages/automation/Notifications'
import RunDetail from './pages/automation/RunDetail'
import RunList from './pages/automation/RunList'
import Workflows from './pages/automation/Workflows'
import Catalog from './pages/connections/Catalog'
import Dashboard from './pages/dashboard/Dashboard'
import LogAnalyzer from './pages/diagnosis/LogAnalyzer'
import IncidentDetail from './pages/incidents/IncidentDetail'
import IncidentList from './pages/incidents/IncidentList'
import Users from './pages/settings/Users'

// The workflow editor pulls in React Flow; load it only when opened.
const WorkflowEditor = lazy(() => import('./pages/automation/WorkflowEditor'))

function Lazy({ children }) {
  return <Suspense fallback={<p className="muted">Loading…</p>}>{children}</Suspense>
}

// Keep old catalog and DAG links working after connections were split by type.
function ToConnections() {
  const [params] = useSearchParams()
  const id = params.get('connection') ?? params.get('open')
  const type = params.get('type')
  const path = type === 'database' ? '/connections/databases' : type === 'channel' ? '/connections/channels' : '/connections/airflow'
  return <Navigate to={id ? `${path}?open=${id}` : path} replace />
}

export default function App() {
  return (
    <Routes>
      <Route element={<RedirectIfAuthenticated />}>
        <Route element={<AuthLayout />}>
          <Route path="/login" element={<Login />} />
        </Route>
      </Route>

      <Route element={<RequireAuth />}>
        <Route element={<AppLayout />}>
          <Route index element={<Dashboard />} />
          <Route path="/incidents" element={<IncidentList />} />
          <Route path="/incidents/:id" element={<IncidentDetail />} />
          <Route path="/automation/approvals" element={<Approvals />} />
          <Route path="/automation/runs" element={<RunList />} />
          <Route path="/automation/runs/:id" element={<RunDetail />} />
          <Route path="/automation/workflows" element={<Workflows />} />
          <Route path="/automation/workflows/:id" element={<Lazy><WorkflowEditor /></Lazy>} />
          <Route path="/automation/notifications" element={<Notifications />} />
          <Route path="/diagnosis/analyzer" element={<LogAnalyzer />} />
          <Route path="/connections" element={<ToConnections />} />
          <Route path="/connections/airflow" element={<Catalog type="airflow" />} />
          <Route path="/connections/databases" element={<Catalog type="database" />} />
          <Route path="/connections/channels" element={<Catalog type="channel" />} />
          <Route path="/settings/connections" element={<ToConnections />} />
          <Route path="/settings/dags" element={<ToConnections />} />
          <Route path="/pipelines" element={<ToConnections />} />
          <Route element={<RequireRole roles={['ADMIN']} />}>
            <Route path="/settings/users" element={<Users />} />
          </Route>
        </Route>
      </Route>

      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  )
}
