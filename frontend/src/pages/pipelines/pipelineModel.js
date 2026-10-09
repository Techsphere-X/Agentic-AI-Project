// Builds the pipeline canvas (connection → DAG → monitors → automations) from API records.
// Pure functions: no React, no fetching.

import { connectionState } from '../../connectionState'

const X = { conn: 0, dag: 320, monitor: 660, workflow: 1010 }
const MONITOR_ROW = 110
const DAG_GAP = 30

export const TYPE_BY_MONITOR = { failure: 'DAG_RUN_FAILED', sla: 'SLA_MISSED' }

/** Which monitors a DAG currently has on the canvas. */
export function monitorsOf(dag) {
  if (!dag.is_monitored) return []
  const kinds = []
  if (dag.detect_failures) kinds.push('failure')
  if (dag.sla_minutes != null) kinds.push('sla')
  return kinds
}

/** PATCH body that attaches a monitor of `kind` to `dag`. */
export function attachBody(dag, kind, minutes = 60) {
  if (kind === 'failure') {
    // Attaching to an unmonitored DAG must not resurrect an old SLA setting.
    return { is_monitored: true, detect_failures: true, ...(dag.is_monitored ? {} : { sla_minutes: null }) }
  }
  return { is_monitored: true, sla_minutes: minutes, ...(dag.is_monitored ? {} : { detect_failures: false }) }
}

/** PATCH body that detaches a monitor; the DAG stops being polled when none are left. */
export function detachBody(dag, kind) {
  if (kind === 'failure') {
    return { detect_failures: false, ...(dag.sla_minutes == null ? { is_monitored: false } : {}) }
  }
  return { sla_minutes: null, ...(!dag.detect_failures ? { is_monitored: false } : {}) }
}

/**
 * What a workflow listens to: incident types, and any DAG / environment restrictions from the
 * filters it passes straight after the trigger. "When a DAG run finishes" workflows are fed by the
 * DAGs themselves (`dagRun`). Returns null for workflows that nothing on the canvas feeds.
 */
export function workflowFeeds(workflow) {
  const nodes = Object.fromEntries(workflow.graph.nodes.map((n) => [n.id, n]))
  const edges = {}
  workflow.graph.edges.forEach((e) => (edges[`${e.from}:${e.port}`] = e.to))
  const trigger = workflow.graph.nodes.find((n) => n.type.startsWith('trigger.'))
  if (!trigger) return null
  if (trigger.type === 'trigger.incident_stale') return { stale: true, types: null, dagIds: null, envs: null }
  if (trigger.type === 'trigger.dag_run') {
    const dagIds = trigger.config?.dag_ids?.length ? new Set(trigger.config.dag_ids) : null
    return { stale: false, dagRun: true, types: null, dagIds, envs: null }
  }

  const types = trigger.config?.incident_types?.length ? new Set(trigger.config.incident_types) : null
  let dagIds = null
  let envs = null
  let current = edges[`${trigger.id}:next`]
  const seen = new Set()
  while (current && !seen.has(current)) {
    seen.add(current)
    const node = nodes[current]
    if (node.type === 'condition.filter') {
      if (node.config?.dag_ids?.length) dagIds = new Set(node.config.dag_ids)
      if (node.config?.environments?.length) envs = new Set(node.config.environments)
      current = edges[`${node.id}:true`]
    } else if (node.type === 'diagnose.classify_log') {
      // 'any outcome' if wired, else the first connected outcome output.
      current = ['next', 'retryable', 'needs_fix', 'unknown'].map((p) => edges[`${node.id}:${p}`]).find(Boolean)
    } else {
      break
    }
  }
  return { stale: false, types, dagIds, envs }
}

function feedsMonitor(feeds, kind, dag, conn) {
  if (feeds.stale) return true
  if (feeds.types && !feeds.types.has(TYPE_BY_MONITOR[kind])) return false
  if (feeds.dagIds && !feeds.dagIds.has(dag.dag_id)) return false
  if (feeds.envs && !feeds.envs.has(conn.environment)) return false
  return true
}

/**
 * @param {object} input
 * @param {object[]} input.connections
 * @param {Record<string, object[]>} input.dagsByConnection
 * @param {object[]} input.workflows   enabled workflows
 * @param {Record<string, number>} input.counts  key `${connection_id}:${dag_id}:${type}`
 * @param {Record<string, object>} [input.checks]  newest run check, key `${connection_id}:${dag_id}`
 * @param {object[]} input.drafts      {id, kind, position}
 * @param {boolean} input.showUnmonitored
 * @param {boolean} input.canEdit
 */
export function buildPipeline({
  connections,
  dagsByConnection,
  workflows,
  counts,
  checks = {},
  drafts,
  showUnmonitored,
  canEdit,
}) {
  const nodes = []
  const edges = []
  const monitorNodes = []
  const dagNodes = []
  let y = 0

  connections.forEach((conn) => {
    const known = dagsByConnection[conn.id] ?? []
    const state = connectionState(conn, { everConnected: known.some((d) => d.last_synced_at) })
    // Only a live connection's DAGs are current; otherwise show the connection alone with its status.
    const dags = state.live ? known.filter((d) => d.is_present && (showUnmonitored || d.is_monitored)) : []
    const top = y
    dags.forEach((dag) => {
      const kinds = monitorsOf(dag)
      const rows = Math.max(1, kinds.length)
      const dagId = `dag:${dag.id}`
      const dagNode = {
        id: dagId,
        type: 'dag',
        position: { x: X.dag, y: y + ((rows - 1) * MONITOR_ROW) / 2 },
        deletable: false,
        data: { dag, conn, canEdit, lastCheck: checks[`${conn.id}:${dag.dag_id}`] ?? null },
      }
      nodes.push(dagNode)
      dagNodes.push(dagNode)
      edges.push({ id: `e:${conn.id}:${dag.id}`, source: `conn:${conn.id}`, target: dagId, deletable: false })
      kinds.forEach((kind, i) => {
        const id = `mon:${kind}:${dag.id}`
        const node = {
          id,
          type: 'monitor',
          position: { x: X.monitor, y: y + i * MONITOR_ROW },
          deletable: canEdit,
          data: {
            kind,
            dag,
            conn,
            minutes: dag.sla_minutes,
            count: counts[`${conn.id}:${dag.dag_id}:${TYPE_BY_MONITOR[kind]}`] ?? 0,
          },
        }
        nodes.push(node)
        monitorNodes.push(node)
        edges.push({ id: `e:${dagId}:${id}`, source: dagId, target: id, deletable: canEdit, className: 'edge-attach' })
      })
      y += rows * MONITOR_ROW + DAG_GAP
    })
    const bottom = Math.max(top, y - MONITOR_ROW - DAG_GAP)
    nodes.unshift({
      id: `conn:${conn.id}`,
      type: 'conn',
      position: { x: X.conn, y: (top + bottom) / 2 },
      deletable: false,
      data: { conn, state },
    })
    y += DAG_GAP * 2
  })

  workflows.forEach((workflow, i) => {
    const feeds = workflowFeeds(workflow)
    if (!feeds) return
    const id = `wf:${workflow.id}`
    nodes.push({
      id,
      type: 'workflow',
      position: { x: X.workflow, y: i * 120 },
      deletable: false,
      data: { workflow, stale: feeds.stale, dagRun: Boolean(feeds.dagRun) },
    })
    if (feeds.dagRun) {
      // Fed by every finished run of the (monitored) DAGs it names.
      dagNodes
        .filter((d) => d.data.dag.is_monitored && (!feeds.dagIds || feeds.dagIds.has(d.data.dag.dag_id)))
        .forEach((d) =>
          edges.push({
            id: `r:${d.id}:${workflow.id}`,
            source: d.id,
            target: id,
            deletable: false,
            className: 'edge-feed edge-feed-run',
          }),
        )
      return
    }
    monitorNodes.forEach((monitor) => {
      if (feedsMonitor(feeds, monitor.data.kind, monitor.data.dag, monitor.data.conn)) {
        edges.push({
          id: `f:${monitor.id}:${workflow.id}`,
          source: monitor.id,
          target: id,
          deletable: false,
          className: feeds.stale ? 'edge-feed edge-feed-stale' : 'edge-feed',
        })
      }
    })
  })

  drafts.forEach((draft) => {
    nodes.push({
      id: draft.id,
      type: 'monitor',
      position: draft.position,
      deletable: true,
      data: { kind: draft.kind, draft: true },
    })
  })

  return { nodes, edges }
}
