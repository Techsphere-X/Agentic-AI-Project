import {
  Background,
  Controls,
  MarkerType,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useReactFlow,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import '../../components/flow/FlowCanvas.css'
import { useCallback, useEffect, useRef, useState } from 'react'
import { Link } from 'react-router'
import { Alert, Badge, Button, EmptyState, StatusPill, Toggle } from '../../components/ui'
import { connectionState } from '../../connectionState'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { formatRelative } from '../../format'
import { checkStatus } from '../automation/automationText'
import { airflowApi, automationApi, incidentsApi } from '../../services/endpoints'
import { ConnectionNode, DagNode, MonitorNode, WorkflowNode } from './pipelineNodes'
import { attachBody, buildPipeline, detachBody } from './pipelineModel'

const NODE_TYPES = { conn: ConnectionNode, dag: DagNode, monitor: MonitorNode, workflow: WorkflowNode }
const DRAG_TYPE = 'application/x-monitor-block'
const POLL_MS = 15000
const EDGE_OPTIONS = { markerEnd: { type: MarkerType.ArrowClosed, width: 16, height: 16 } }
const MONITORS = [
  { kind: 'failure', icon: '⚠', label: 'Failure monitor', hint: 'Incident when a run fails' },
  { kind: 'sla', icon: '⏱', label: 'Freshness SLA', hint: 'Incident when no run succeeds in time' },
]

function SlaEditor({ dag, onSave, disabled }) {
  const [minutes, setMinutes] = useState(dag.sla_minutes ?? 60)
  const valid = Number.isInteger(minutes) && minutes >= 5 && minutes <= 10080
  return (
    <div className="field">
      <label htmlFor="sla-minutes">Latest success must be within (minutes)</label>
      <div className="inline-field">
        <input
          id="sla-minutes"
          type="number"
          min={5}
          max={10080}
          value={minutes}
          disabled={disabled}
          onChange={(e) => setMinutes(Number(e.target.value))}
        />
        <Button size="sm" disabled={disabled || !valid || minutes === dag.sla_minutes} onClick={() => onSave(minutes)}>
          Save
        </Button>
      </div>
      <small className="field-hint">5–10080 minutes</small>
    </div>
  )
}

function Panel({
  node,
  canEdit,
  busy,
  onAttach,
  onDetach,
  onSetSla,
  onRemoveDraft,
  onRetry,
  showUnmonitored,
  setShowUnmonitored,
}) {
  if (!node) {
    return (
      <>
        <strong>Pipelines</strong>
        <p className="muted small">
          Each Airflow connection feeds its DAGs. Monitor blocks attached to a DAG open incidents, and incidents start
          the automation workflows they are wired to.
        </p>
        {canEdit && (
          <p className="muted small">
            To add monitoring, drag a monitor block onto the canvas, then drag from a DAG&apos;s right edge to it. Select
            a monitor and press Delete to remove it.
          </p>
        )}
        <div className="checkbox-inline">
          <Toggle checked={showUnmonitored} onChange={setShowUnmonitored} label="Show unmonitored DAGs" />
          <span className="small">Show unmonitored DAGs</span>
        </div>
      </>
    )
  }
  const { data } = node
  if (node.type === 'conn') {
    const { conn, state } = data
    return (
      <>
        <strong>{conn.name}</strong>
        <p className="muted small">
          {conn.environment} · {conn.kind === 'MOCK' ? 'mock' : conn.base_url}
        </p>
        <p>
          <StatusPill tone={state.tone} label={state.label} />
        </p>
        {state.hint && <p className="small">{state.hint}</p>}
        {state.message && <p className="muted small">{state.message}</p>}
        <p className="muted small">Last checked {formatRelative(conn.last_checked_at)}</p>
        {canEdit && !state.live && (
          <p>
            <Button size="sm" loading={busy} disabled={busy} onClick={() => onRetry(conn)}>
              Retry now
            </Button>
          </p>
        )}
        <Link className="small" to="/connections/airflow">
          Manage connections
        </Link>
      </>
    )
  }
  if (node.type === 'dag') {
    const { dag, lastCheck } = data
    const check = lastCheck && checkStatus(lastCheck.status)
    const missing = ['failure', 'sla'].filter((kind) =>
      kind === 'failure' ? !(dag.is_monitored && dag.detect_failures) : !(dag.is_monitored && dag.sla_minutes != null),
    )
    return (
      <>
        <strong className="mono">{dag.dag_id}</strong>
        {dag.description && <p className="muted small">{dag.description}</p>}
        <p className="small">
          {dag.schedule_summary && <span className="mono">{dag.schedule_summary}</span>}
          {dag.is_paused && ' · paused'}
        </p>
        {dag.tags?.length > 0 && (
          <div className="tag-list">
            {dag.tags.map((t) => (
              <Badge key={t}>{t}</Badge>
            ))}
          </div>
        )}
        {canEdit &&
          missing.map((kind) => (
            <Button key={kind} size="sm" loading={busy} onClick={() => onAttach(dag, kind)}>
              Add {kind === 'failure' ? 'failure monitor' : 'freshness SLA'}
            </Button>
          ))}
        {check && (
          <p className="small">
            Latest run <span className="mono">{lastCheck.run_id}</span>: <Badge tone={check.tone}>{check.label}</Badge>
            {lastCheck.message && <span className="muted"> · {lastCheck.message}</span>}
          </p>
        )}
        <Link className="small" to={`/incidents?search=${encodeURIComponent(dag.dag_id)}`}>
          Incidents for this DAG
        </Link>
        <Link className="small" to={`/automation/run-checks?dag_id=${encodeURIComponent(dag.dag_id)}`}>
          Run checks for this DAG
        </Link>
      </>
    )
  }
  if (node.type === 'monitor' && data.draft) {
    return (
      <>
        <strong>{data.kind === 'failure' ? 'Failure monitor' : 'Freshness SLA'} (not attached)</strong>
        <p className="muted small">Drag from a DAG&apos;s right edge to this block to attach it.</p>
        <Button size="sm" onClick={() => onRemoveDraft(node.id)}>
          Remove
        </Button>
      </>
    )
  }
  if (node.type === 'monitor') {
    const { dag, kind } = data
    return (
      <>
        <strong>{kind === 'failure' ? 'Failure monitor' : 'Freshness SLA'}</strong>
        <p className="small">
          on <span className="mono">{dag.dag_id}</span>
        </p>
        <p className="muted small">
          {kind === 'failure'
            ? 'Opens an incident when a run of this DAG fails, with the failed tasks and their logs.'
            : 'Opens an incident when the latest successful run finished longer ago than the SLA.'}
        </p>
        {kind === 'sla' && <SlaEditor key={dag.sla_minutes} dag={dag} onSave={(m) => onSetSla(dag, m)} disabled={!canEdit || busy} />}
        <Link className="small" to={`/incidents?search=${encodeURIComponent(dag.dag_id)}`}>
          {data.count} open incident{data.count === 1 ? '' : 's'}
        </Link>
        {canEdit && (
          <Button variant="danger" size="sm" loading={busy} onClick={() => onDetach(dag, kind)}>
            Remove monitor
          </Button>
        )}
      </>
    )
  }
  const { workflow } = data
  return (
    <>
      <strong>{workflow.name}</strong>
      {workflow.description && <p className="muted small">{workflow.description}</p>}
      <p className="muted small">
        {data.stale
          ? 'Runs for any incident that stays unacknowledged too long.'
          : data.dagRun
            ? 'Runs for every finished run of the DAGs wired to it, and records whether each run passed.'
            : 'Runs when a connected monitor opens or re-records an incident.'}
      </p>
      <Link className="small" to={`/automation/workflows/${workflow.id}`}>
        Open in the workflow editor
      </Link>
    </>
  )
}

function Canvas() {
  const { hasRole } = useAuth()
  const canEdit = hasRole('ADMIN', 'OPERATOR')
  const toast = useToast()
  const { screenToFlowPosition, fitView } = useReactFlow()
  const wrapper = useRef(null)
  const draftSeq = useRef(0)

  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [refreshError, setRefreshError] = useState(null)
  const [drafts, setDrafts] = useState([])
  const [showUnmonitored, setShowUnmonitored] = useState(true)
  const [selectedId, setSelectedId] = useState(null)
  const [busy, setBusy] = useState(false)
  const [nodes, setNodes, onNodesChange] = useNodesState([])
  const [edges, setEdges, onEdgesChange] = useEdgesState([])
  const fitted = useRef(false)

  const loading = useRef(false)
  const hasData = useRef(false)
  const load = useCallback(async () => {
    if (loading.current) return
    loading.current = true
    try {
      const [{ items: connections }, workflows, openCounts, latestChecks] = await Promise.all([
        airflowApi.listConnections(),
        automationApi.listWorkflows(),
        incidentsApi.openCounts(),
        automationApi.latestRunChecks(),
      ])
      const dagPages = await Promise.all(connections.map((c) => airflowApi.listDags(c.id, { limit: 200 })))
      const counts = {}
      openCounts.forEach((c) => (counts[`${c.connection_id}:${c.dag_id}:${c.type}`] = c.count))
      const checks = Object.fromEntries(latestChecks.map((c) => [`${c.connection_id}:${c.dag_id}`, c]))
      hasData.current = true
      setData({
        connections: connections.filter((c) => c.is_active),
        dagsByConnection: Object.fromEntries(connections.map((c, i) => [c.id, dagPages[i].items])),
        workflows: workflows.filter((w) => w.enabled),
        counts,
        checks,
      })
      setError(null)
      setRefreshError(null)
    } catch (err) {
      // Keep the last good diagram on a failed background refresh, but say so.
      if (hasData.current) setRefreshError(err.message)
      else setError(err.message)
    } finally {
      loading.current = false
    }
  }, [])

  useEffect(() => {
    // eslint-disable-next-line react/set-state-in-effect -- initial fetch; state is set after await
    load()
    // Poll so the canvas follows Airflow going down or coming back; skip while the tab is hidden.
    const refresh = () => document.visibilityState === 'visible' && load()
    const timer = setInterval(refresh, POLL_MS)
    document.addEventListener('visibilitychange', refresh)
    return () => {
      clearInterval(timer)
      document.removeEventListener('visibilitychange', refresh)
    }
  }, [load])

  async function retry(conn) {
    setBusy(true)
    try {
      const updated = await airflowApi.refreshConnection(conn.id)
      if (updated.is_live) toast.success(`${conn.name} is connected`)
      else toast.error(`${conn.name}: ${updated.last_health_message ?? 'still not reachable'}`)
      await load()
    } catch (err) {
      toast.error(err.message)
    } finally {
      setBusy(false)
    }
  }

  // Rebuild the diagram when data changes, keeping positions the user dragged to.
  useEffect(() => {
    if (!data) return
    const built = buildPipeline({ ...data, drafts, showUnmonitored, canEdit })
    setNodes((previous) => {
      const moved = Object.fromEntries(previous.map((n) => [n.id, n]))
      return built.nodes.map((n) =>
        moved[n.id] ? { ...n, position: moved[n.id].position, selected: moved[n.id].selected } : n,
      )
    })
    setEdges(built.edges)
    if (!fitted.current) {
      fitted.current = true
      requestAnimationFrame(() => fitView({ padding: 0.15, maxZoom: 1 }))
    }
  }, [data, drafts, showUnmonitored, canEdit, setNodes, setEdges, fitView])

  async function patchDag(dag, body, message) {
    setBusy(true)
    try {
      await airflowApi.updateDag(dag.id, body)
      toast.success(message)
      await load()
    } catch (err) {
      toast.error(err.message)
    } finally {
      setBusy(false)
    }
  }

  const attach = (dag, kind) =>
    patchDag(dag, attachBody(dag, kind), `${kind === 'failure' ? 'Failure monitor' : 'Freshness SLA'} added to ${dag.dag_id}`)
  const detach = (dag, kind) => {
    setSelectedId(null)
    return patchDag(dag, detachBody(dag, kind), `Monitor removed from ${dag.dag_id}`)
  }

  const isValidConnection = useCallback(
    (c) => c.source?.startsWith('dag:') && c.target?.startsWith('draft:'),
    [],
  )

  async function onConnect(connection) {
    if (!isValidConnection(connection)) return toast.error('Connect a DAG to an unattached monitor block')
    const draft = drafts.find((d) => d.id === connection.target)
    const dagNode = nodes.find((n) => n.id === connection.source)
    if (!draft || !dagNode) return
    const { dag } = dagNode.data
    const already = draft.kind === 'failure' ? dag.is_monitored && dag.detect_failures : dag.is_monitored && dag.sla_minutes != null
    if (already) return toast.error(`${dag.dag_id} already has this monitor`)
    setDrafts((list) => list.filter((d) => d.id !== draft.id))
    await attach(dag, draft.kind)
  }

  // One callback for a delete gesture: removing a monitor also removes its incoming edge, and
  // that must not detach it twice.
  function onDelete({ nodes: deletedNodes, edges: deletedEdges }) {
    const monitors = new Map()
    deletedNodes.forEach((node) => {
      if (node.data.draft) setDrafts((list) => list.filter((d) => d.id !== node.id))
      else if (node.type === 'monitor') monitors.set(node.id, node)
    })
    deletedEdges
      .filter((e) => e.target.startsWith('mon:'))
      .forEach((e) => {
        const monitor = nodes.find((n) => n.id === e.target)
        if (monitor) monitors.set(monitor.id, monitor)
      })
    const byDag = new Map()
    monitors.forEach((m) => byDag.set(m.data.dag.id, [...(byDag.get(m.data.dag.id) ?? []), m]))
    byDag.forEach((list) => {
      const { dag } = list[0].data
      if (list.length > 1) {
        setSelectedId(null)
        patchDag(dag, { is_monitored: false, sla_minutes: null }, `Monitoring removed from ${dag.dag_id}`)
      } else detach(dag, list[0].data.kind)
    })
  }

  function addDraft(kind, position) {
    let at = position
    if (!at) {
      const box = wrapper.current?.getBoundingClientRect()
      at = screenToFlowPosition(box ? { x: box.left + box.width / 2, y: box.top + 80 } : { x: 0, y: 0 })
    }
    draftSeq.current += 1
    setDrafts((list) => [...list, { id: `draft:${draftSeq.current}`, kind, position: at }])
  }

  if (error) return <Alert tone="danger">{error}</Alert>
  if (!data) return <p className="muted">Loading…</p>
  if (!data.connections.length) {
    return (
      <EmptyState title="No Airflow connections yet">
        <Link to="/connections/airflow">Add a connection</Link> and sync its DAGs to see your pipelines here.
      </EmptyState>
    )
  }

  const selected = nodes.find((n) => n.id === selectedId)
  const offline = data.connections
    .map((conn) => ({
      conn,
      state: connectionState(conn, {
        everConnected: (data.dagsByConnection[conn.id] ?? []).some((d) => d.last_synced_at),
      }),
    }))
    .filter(({ state }) => !state.live)

  return (
    <>
      {refreshError && (
        <Alert tone="danger">Could not refresh pipelines: {refreshError}. Showing the last loaded view.</Alert>
      )}
      {offline.length > 0 && (
        <Alert tone="danger">
          <div className="connection-banner">
            <span>
              {offline.map(({ conn, state }, i) => (
                <span key={conn.id}>
                  {i > 0 && ' · '}
                  <strong>{conn.name}</strong>: {state.label}
                </span>
              ))}
              {offline.length === 1 ? '. Its pipelines are' : '. Their pipelines are'} hidden until Airflow responds.
            </span>
            {canEdit && offline.length === 1 && (
              <Button size="sm" loading={busy} disabled={busy} onClick={() => retry(offline[0].conn)}>
                Retry now
              </Button>
            )}
          </div>
        </Alert>
      )}
      <div className={`editor-shell ${canEdit ? '' : 'editor-readonly'}`}>
        {canEdit && (
          <aside className="editor-palette" aria-label="Monitor blocks">
            <p className="muted small">Drag a monitor onto the canvas, then connect a DAG to it.</p>
            <div className="palette-group">
              <div className="palette-title">Monitors</div>
              {MONITORS.map((m) => (
                <button
                  key={m.kind}
                  type="button"
                  className={`palette-item pnode-monitor-${m.kind}`}
                  draggable
                  title={m.hint}
                  onDragStart={(e) => {
                    e.dataTransfer.setData(DRAG_TYPE, m.kind)
                    e.dataTransfer.effectAllowed = 'move'
                  }}
                  onClick={() => addDraft(m.kind)}
                >
                  <span className="block-icon" aria-hidden="true">
                    {m.icon}
                  </span>
                  <span>{m.label}</span>
                </button>
              ))}
            </div>
            <div className="palette-group">
              <div className="palette-title muted">Coming in Plan 4</div>
              {['Row count', 'Null rate', 'Schema drift'].map((label) => (
                <button key={label} type="button" className="palette-item" disabled title="Data-quality monitors (Plan 4)">
                  <span className="block-icon" aria-hidden="true">
                    ▦
                  </span>
                  <span>{label}</span>
                </button>
              ))}
            </div>
          </aside>
        )}

        <div
          className="editor-canvas"
          ref={wrapper}
          onDragOver={(e) => e.preventDefault()}
          onDrop={(e) => {
            e.preventDefault()
            const kind = e.dataTransfer.getData(DRAG_TYPE)
            if (kind && canEdit) addDraft(kind, screenToFlowPosition({ x: e.clientX - 100, y: e.clientY - 30 }))
          }}
        >
          <ReactFlow
            nodes={nodes}
            edges={edges}
            nodeTypes={NODE_TYPES}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onConnect={onConnect}
            isValidConnection={isValidConnection}
            onDelete={onDelete}
            onSelectionChange={({ nodes: ns }) => setSelectedId(ns.length === 1 ? ns[0].id : null)}
            nodesConnectable={canEdit}
            deleteKeyCode={canEdit ? ['Backspace', 'Delete'] : null}
            defaultEdgeOptions={EDGE_OPTIONS}
            colorMode="system"
            minZoom={0.15}
            proOptions={{ hideAttribution: true }}
          >
            <Background gap={20} />
            <Controls showInteractive={false} />
            <MiniMap pannable zoomable className="editor-minimap" />
          </ReactFlow>
        </div>

        <aside className="editor-panel" aria-label="Details">
          <Panel
            node={selected}
            canEdit={canEdit}
            busy={busy}
            onAttach={attach}
            onDetach={detach}
            onSetSla={(dag, minutes) => patchDag(dag, { sla_minutes: minutes }, `SLA for ${dag.dag_id} set to ${minutes} min`)}
            onRemoveDraft={(id) => {
              setDrafts((list) => list.filter((d) => d.id !== id))
              setSelectedId(null)
            }}
            onRetry={retry}
            showUnmonitored={showUnmonitored}
            setShowUnmonitored={setShowUnmonitored}
          />
        </aside>
      </div>
    </>
  )
}

export default function PipelineCanvas() {
  return (
    <div className="editor-page">
      <div className="editor-toolbar">
        <div>
          <h1 className="editor-title">Pipelines</h1>
        </div>
        <span className="muted small">Airflow connection → DAG → monitors → automations</span>
      </div>
      <ReactFlowProvider>
        <Canvas />
      </ReactFlowProvider>
    </div>
  )
}
