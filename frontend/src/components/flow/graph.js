// Convert between the backend workflow graph ({nodes, edges}) and React Flow nodes/edges,
// plus the client-side connection rules. The server remains the authority on validity.

import { layeredLayout } from './layout'

export const BLOCK = 'block'

export function isTrigger(type) {
  return type.startsWith('trigger.')
}

// Output ports taken when something went wrong or the answer was "no". The catalog's
// `port_tones` is the source of truth; this is the fallback for pages without a catalog.
export const BAD_PORTS = new Set([
  'failed',
  'fail',
  'rejected',
  'false',
  'timeout',
  'mismatched',
  'no_logs',
  'uncertain',
  'exceeded',
  'needs_fix',
  'escalate',
  'paused',
  'busy',
])

/** "good" | "bad" | "neutral" for an output port, from its block's catalog entry. */
export function portTone(def, port) {
  if (port === 'error') return 'bad'
  if (def?.dynamic_ports) return port === 'otherwise' ? 'neutral' : 'good'
  return def?.port_tones?.[port] ?? (BAD_PORTS.has(port) ? 'bad' : 'good')
}

// Blocks whose outputs follow their settings (`dynamic_ports` in the catalog):
// rule name -> config -> [{port, label}]. Keep in step with `port_rule` in the backend.
const DYNAMIC_PORTS = {
  // Switch: one output per case, plus 'otherwise'.
  cases: (config) => [
    ...(config.cases ?? []).filter((c) => c.key).map((c) => ({ port: c.key, label: c.label || c.equals })),
    { port: 'otherwise', label: 'otherwise' },
  ],
}

/**
 * Output ports of a block: fixed by the catalog, or (dynamic blocks) from its config; plus
 * 'error' when its run settings send failures there.
 */
export function portsOf(def, config, settings) {
  const rule = DYNAMIC_PORTS[def?.dynamic_ports]
  const ports = rule ? rule(config ?? {}).map((p) => p.port) : (def?.ports ?? [])
  return settings?.on_error === 'output' ? [...ports, 'error'] : ports
}

/** The label shown next to an output port. */
export function portName(def, port, config) {
  if (port === 'error') return 'error'
  const rule = DYNAMIC_PORTS[def?.dynamic_ports]
  if (rule) return rule(config ?? {}).find((p) => p.port === port)?.label ?? port
  return def?.port_labels?.[port] ?? port
}

/** What a link from this port is labelled on the canvas (nothing for a plain "then"). */
export function portLabel(def, port, config) {
  if (port === 'next' && portsOf(def, config).length <= 1 && !def?.dynamic_ports) return undefined
  return portName(def, port, config)
}

/**
 * Links from a trigger that is being switched to `def`: links from outputs the new trigger
 * also has stay; the others move to 'then', or to 'failed' for "When this DAG runs" (incident
 * workflows react to failures). Labels follow the new trigger.
 */
export function retargetTriggerEdges(edges, triggerId, def, config) {
  const ports = portsOf(def, config)
  const fallback = ports.includes('next') ? 'next' : ports.includes('failed') ? 'failed' : ports[0]
  const moved = edges.map((e) => {
    if (e.source !== triggerId) return e
    const port = ports.includes(e.sourceHandle) ? e.sourceHandle : fallback
    return newEdge({ source: e.source, sourceHandle: port, target: e.target }, def, config)
  })
  return moved.filter((e, i) => e.sourceHandle && moved.findIndex((o) => o.id === e.id) === i)
}

/** Links leaving outputs a block no longer has (e.g. after its config changed). */
export function prunePortEdges(node, edges) {
  const ports = new Set(portsOf(node.data.def, node.data.config, node.data.settings))
  return edges.filter((e) => e.source !== node.id || ports.has(e.sourceHandle))
}

const EDGE_CLASS = { good: 'edge-ok', bad: 'edge-bad', neutral: 'edge-neutral' }

export function edgeId(from, port, to) {
  return `${from}:${port}->${to}`
}

function toEdge(from, port, to, def, config) {
  return {
    id: edgeId(from, port, to),
    source: from,
    sourceHandle: port,
    target: to,
    label: portLabel(def, port, config),
    className: EDGE_CLASS[portTone(def, port)],
  }
}

/** Backend graph -> React Flow. Nodes without a saved position are auto-laid out. */
export function toFlow(graph, extraData = () => ({})) {
  const needsLayout = (graph?.nodes ?? []).some((n) => !n.position)
  const auto = needsLayout ? layeredLayout(graph.nodes, graph.edges) : {}
  const nodes = (graph?.nodes ?? []).map((n) => ({
    id: n.id,
    type: BLOCK,
    position: n.position ?? auto[n.id] ?? { x: 0, y: 0 },
    deletable: true,
    data: {
      nodeType: n.type,
      name: n.name ?? '',
      config: n.config ?? {},
      ...(n.settings ? { settings: n.settings } : {}),
      ...extraData(n),
    },
  }))
  const byId = Object.fromEntries(nodes.map((n) => [n.id, n.data]))
  const edges = (graph?.edges ?? []).map((e) => toEdge(e.from, e.port, e.to, byId[e.from]?.def, byId[e.from]?.config))
  return { nodes, edges }
}

export const DEFAULT_RUN_SETTINGS = { retries: 0, retry_delay_seconds: 60, on_error: 'fail' }

/** Whether a block's run settings differ from the defaults (only then are they saved). */
export function hasRunSettings(settings) {
  if (!settings) return false
  return Object.entries(DEFAULT_RUN_SETTINGS).some(([key, value]) => (settings[key] ?? value) !== value)
}

/** React Flow -> backend graph (positions rounded). */
export function fromFlow(nodes, edges) {
  return {
    nodes: nodes.map((n) => ({
      id: n.id,
      type: n.data.nodeType,
      ...(n.data.name?.trim() ? { name: n.data.name.trim() } : {}),
      config: n.data.config ?? {},
      ...(hasRunSettings(n.data.settings) ? { settings: n.data.settings } : {}),
      position: { x: Math.round(n.position.x), y: Math.round(n.position.y) },
    })),
    edges: edges.map((e) => ({ from: e.source, port: e.sourceHandle, to: e.target })),
  }
}

/**
 * `sourceDef` is the catalog entry of the block the link starts from (labels and colour);
 * `sourceConfig` its settings (for blocks whose outputs depend on them).
 */
export function newEdge(connection, sourceDef, sourceConfig) {
  return toEdge(connection.source, connection.sourceHandle, connection.target, sourceDef, sourceConfig)
}

function reaches(start, goal, edges) {
  const next = {}
  edges.forEach((e) => (next[e.source] ??= []).push(e.target))
  const stack = [start]
  const seen = new Set()
  while (stack.length) {
    const id = stack.pop()
    if (id === goal) return true
    if (seen.has(id)) continue
    seen.add(id)
    stack.push(...(next[id] ?? []))
  }
  return false
}

/**
 * Why a connection is not allowed (or null if it is). One output may link to several blocks;
 * they run one after another, top to bottom on the canvas.
 */
export function connectionProblem(connection, nodes, edges) {
  const { source, target, sourceHandle } = connection
  if (!source || !target || !sourceHandle) return 'Connect an output port to a block'
  if (source === target) return 'A block cannot connect to itself'
  const targetNode = nodes.find((n) => n.id === target)
  if (!targetNode) return 'Unknown block'
  if (isTrigger(targetNode.data.nodeType)) return 'Nothing can connect into a trigger'
  if (edges.some((e) => e.source === source && e.sourceHandle === sourceHandle && e.target === target)) {
    return 'That output is already connected to this block'
  }
  if (reaches(target, source, edges)) return 'That would create a loop'
  return null
}

/** Add an edge (an exact duplicate is ignored). */
export function connect(connection, edges, sourceDef, sourceConfig) {
  const edge = newEdge(connection, sourceDef, sourceConfig)
  return edges.some((e) => e.id === edge.id) ? edges : [...edges, edge]
}

/**
 * Run order of blocks linked from the same output, as shown on the canvas: top to bottom, then
 * left to right (the backend uses the same rule). Returns {edgeId: 1-based position} for
 * outputs with more than one link.
 */
export function linkOrder(nodes, edges) {
  const pos = Object.fromEntries(nodes.map((n) => [n.id, n.position]))
  const groups = {}
  edges.forEach((e) => (groups[`${e.source}:${e.sourceHandle}`] ??= []).push(e))
  const order = {}
  Object.values(groups).forEach((group) => {
    if (group.length < 2) return
    const sorted = group
      .map((e, index) => ({ e, index, p: pos[e.target] }))
      .sort((a, b) => (a.p?.y ?? 0) - (b.p?.y ?? 0) || (a.p?.x ?? 0) - (b.p?.x ?? 0) || a.index - b.index)
    sorted.forEach(({ e }, i) => (order[e.id] = i + 1))
  })
  return order
}

/** Readable unique id for a new block, e.g. `notify_2`. */
export function nextNodeId(type, nodes) {
  const base = type.split('.').at(-1).replace(/[^A-Za-z0-9_-]/g, '_')
  const taken = new Set(nodes.map((n) => n.id))
  for (let i = 1; ; i += 1) {
    const id = `${base}_${i}`
    if (!taken.has(id)) return id
  }
}

/**
 * Change a block to another type (e.g. "Run this DAG" -> "Wait for it to succeed").
 * Keeps config keys the new type also has (connection, DAG, SQL), uses defaults for the rest,
 * and drops edges leaving output ports the new type does not have.
 */
export function retypeNode(node, newDef, edges) {
  const defaults = defaultConfig(newDef.config_schema)
  const keep = Object.fromEntries(Object.entries(node.data.config ?? {}).filter(([key]) => key in defaults))
  const config = { ...defaults, ...keep }
  const ports = new Set(portsOf(newDef, config))
  return {
    node: { ...node, data: { ...node.data, nodeType: newDef.type, def: newDef, config } },
    edges: edges.filter((e) => e.source !== node.id || ports.has(e.sourceHandle)),
  }
}

/** Palette drag payload: JSON `{type, name?, config?, idBase?}` or a bare block type. */
export function parseBlockPayload(raw) {
  if (!raw) return null
  if (!raw.startsWith('{')) return { type: raw }
  try {
    const payload = JSON.parse(raw)
    return typeof payload?.type === 'string' ? payload : null
  } catch {
    return null
  }
}

/** Default config values from a Pydantic JSON schema. */
export function defaultConfig(schema) {
  const config = {}
  Object.entries(schema?.properties ?? {}).forEach(([key, prop]) => {
    if (prop.default !== undefined) config[key] = structuredClone(prop.default)
  })
  return config
}
