import { useCallback, useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router'
import { Badge, Button, Card, EmptyState, Toggle } from '../../components/ui'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { formatRelative } from '../../format'
import { automationApi } from '../../services/endpoints'
import { NODE_LABELS, describeConfig, nodeIcon, startsByHand, triggerType } from './automationText'

const PORT_LABELS = {
  true: 'if yes',
  false: 'if no',
  approved: 'if approved',
  rejected: 'if rejected',
  success: 'if it worked',
  failed: 'if it failed',
  ready: 'if idle',
  paused: 'if paused',
  busy: 'if a run is active',
  pass: 'if it passes',
  fail: 'if it fails',
  timeout: 'if it gave up',
}

/** Read-only branch view of a workflow graph, starting at the trigger. */
function GraphTree({ graph }) {
  const nodes = Object.fromEntries(graph.nodes.map((n) => [n.id, n]))
  const edgesFrom = {}
  graph.edges.forEach((e) => (edgesFrom[e.from] ??= []).push(e))
  const trigger = graph.nodes.find((n) => n.type.startsWith('trigger.'))
  const seen = new Set()

  function renderNode(id) {
    const node = nodes[id]
    const repeated = seen.has(id)
    seen.add(id)
    const outgoing = repeated ? [] : (edgesFrom[id] ?? [])
    const branching = outgoing.length > 1 || outgoing.some((e) => e.port !== 'next')
    return (
      <li key={`${id}-${repeated}`} className="tree-node">
        <div className="tree-label">
          <span className="flow-icon" aria-hidden="true">
            {nodeIcon(node.type)}
          </span>
          <span>
            <strong>{node.name ?? NODE_LABELS[node.type] ?? node.type}</strong>
            {repeated ? (
              <span className="muted small"> (same step as above)</span>
            ) : (
              <span className="muted small"> {describeConfig(node.type, node.config)}</span>
            )}
          </span>
        </div>
        {outgoing.length > 0 &&
          (branching ? (
            <ul className="tree-branches">
              {outgoing.map((e) => (
                <li key={e.port}>
                  <span className="tree-port">{PORT_LABELS[e.port] ?? e.port}</span>
                  <ul className="tree">{renderNode(e.to)}</ul>
                </li>
              ))}
            </ul>
          ) : (
            <ul className="tree">{renderNode(outgoing[0].to)}</ul>
          ))}
      </li>
    )
  }

  return trigger ? <ul className="tree tree-root">{renderNode(trigger.id)}</ul> : null
}

function WorkflowCard({ workflow, canEdit, canRun, onChange, onDelete }) {
  const [open, setOpen] = useState(false)
  const [saving, setSaving] = useState(false)
  const [running, setRunning] = useState(false)
  const toast = useToast()
  const navigate = useNavigate()
  const trigger = workflow.graph?.nodes?.find((n) => n.type === triggerType(workflow.graph))
  const startsWith = trigger && [NODE_LABELS[trigger.type] ?? trigger.type, describeConfig(trigger.type, trigger.config)]

  async function runNow() {
    setRunning(true)
    try {
      const run = await automationApi.runWorkflow(workflow.id)
      navigate(`/automation/runs/${run.id}`)
    } catch (err) {
      toast.error(err.message)
      setRunning(false)
    }
  }

  async function save(changes, message) {
    setSaving(true)
    try {
      onChange(await automationApi.updateWorkflow(workflow.id, changes))
      toast.success(message)
    } catch (err) {
      toast.error(err.message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <Card className="workflow-card">
      <div className="workflow-head">
        <Toggle
          checked={workflow.enabled}
          disabled={!canEdit || saving}
          label={`Enable ${workflow.name}`}
          onChange={(enabled) => save({ enabled }, `“${workflow.name}” is ${enabled ? 'on' : 'off'}`)}
        />
        <div className="workflow-title">
          <div className="cell-title">
            <strong>{workflow.name}</strong>
            {workflow.mode === 'DRY_RUN' && <Badge tone="info">dry run</Badge>}
            {!workflow.key && <Badge>custom</Badge>}
          </div>
          {startsWith && (
            <div className="small">
              <span className="muted">Starts:</span> {startsWith.filter(Boolean).join(' · ')}
            </div>
          )}
          {workflow.description && <p className="muted small">{workflow.description}</p>}
          <div className="muted small">
            v{workflow.version} · {workflow.run_count} run(s)
            {workflow.last_run_at && <> · last {formatRelative(workflow.last_run_at)}</>}
            {workflow.run_count > 0 && (
              <>
                {' '}
                · <Link to={`/automation/runs?workflow_id=${workflow.id}`}>view runs</Link>
              </>
            )}
          </div>
        </div>
        <div className="card-actions">
          {canRun && startsByHand(workflow.graph) && (
            <Button size="sm" variant="primary" loading={running} onClick={runNow} title="Start a run now">
              ▶ Run now
            </Button>
          )}
          {canEdit && (
            <select
              value={workflow.mode}
              disabled={saving}
              aria-label="Mode"
              onChange={(e) =>
                save(
                  { mode: e.target.value },
                  e.target.value === 'DRY_RUN'
                    ? 'Dry run: it will only show what it would do'
                    : 'Live: it will make changes',
                )
              }
            >
              <option value="LIVE">Live</option>
              <option value="DRY_RUN">Dry run</option>
            </select>
          )}
          <Link className="btn btn-secondary btn-sm" to={`/automation/workflows/${workflow.id}`}>
            {canEdit ? 'Edit on canvas' : 'Open canvas'}
          </Link>
          {canEdit && (
            <Link className="btn btn-ghost btn-sm" to={`/automation/workflows/new?from=${workflow.id}`}>
              Duplicate
            </Link>
          )}
          {canEdit && workflow.run_count === 0 && (
            <Button variant="ghost-danger" size="sm" disabled={saving} onClick={() => onDelete(workflow)}>
              Delete
            </Button>
          )}
          <button type="button" className="link-btn small" onClick={() => setOpen(!open)}>
            {open ? 'Hide steps' : 'Show steps'}
          </button>
        </div>
      </div>
      {open && <GraphTree graph={workflow.graph} />}
    </Card>
  )
}

export default function Workflows() {
  const { hasRole } = useAuth()
  const toast = useToast()
  const [workflows, setWorkflows] = useState(null)
  const canEdit = hasRole('ADMIN')
  const canRun = hasRole('ADMIN', 'OPERATOR')

  const load = useCallback(async () => {
    try {
      setWorkflows(await automationApi.listWorkflows())
    } catch (err) {
      toast.error(err.message)
    }
  }, [toast])

  useEffect(() => {
    // eslint-disable-next-line react/set-state-in-effect -- initial fetch; state is set after await
    load()
  }, [load])

  function onChange(updated) {
    // PATCH responses carry no run stats; keep the ones we have.
    setWorkflows((list) =>
      list.map((w) => (w.id === updated.id ? { ...updated, run_count: w.run_count, last_run_at: w.last_run_at } : w)),
    )
  }

  async function onDelete(workflow) {
    if (!window.confirm(`Delete “${workflow.name}”? This cannot be undone.`)) return
    try {
      await automationApi.deleteWorkflow(workflow.id)
      setWorkflows((list) => list.filter((w) => w.id !== workflow.id))
      toast.success('Workflow deleted')
    } catch (err) {
      toast.error(err.message)
    }
  }

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Workflows</h1>
          {!canEdit && <p className="muted">Only admins can change workflows.</p>}
        </div>
        {canEdit && (
          <Link className="btn btn-primary" to="/automation/workflows/new">
            New workflow
          </Link>
        )}
      </div>
      {!workflows ? (
        <p className="muted">Loading…</p>
      ) : workflows.length === 0 ? (
        <EmptyState title="No workflows" />
      ) : (
        <div className="workflow-list">
          {workflows.map((w) => (
            <WorkflowCard key={w.id} workflow={w} canEdit={canEdit} canRun={canRun} onChange={onChange} onDelete={onDelete} />
          ))}
        </div>
      )}
    </div>
  )
}
