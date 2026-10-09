// Logic tests for the canvas modules. Run with `npm run test:canvas`
// (bundled by Vite so extensionless imports resolve, then run by node:test).
import assert from 'node:assert/strict'
import { test } from 'node:test'
import {
  attachBody,
  buildPipeline,
  detachBody,
  monitorsOf,
  workflowFeeds,
} from '../../pages/pipelines/pipelineModel'
import {
  connect,
  connectionProblem,
  defaultConfig,
  fromFlow,
  linkOrder,
  nextNodeId,
  parseBlockPayload,
  retypeNode,
  toFlow,
} from './graph'
import { layeredLayout } from './layout'
import { connectionState } from '../../connectionState'

const retryGraph = {
  nodes: [
    { id: 'trigger', type: 'trigger.incident', config: { events: ['opened'], incident_types: ['DAG_RUN_FAILED'] } },
    { id: 'classify', type: 'diagnose.classify_log', config: {} },
    { id: 'filter', type: 'condition.filter', config: { dag_ids: ['orders'], environments: ['DEV'] } },
    { id: 'retry', type: 'action.clear_failed_tasks', config: {} },
    { id: 'notify', type: 'notify', config: {} },
  ],
  edges: [
    { from: 'trigger', port: 'next', to: 'classify' },
    { from: 'classify', port: 'next', to: 'filter' },
    { from: 'filter', port: 'true', to: 'retry' },
    { from: 'retry', port: 'failed', to: 'notify' },
  ],
}

test('graph round-trips through React Flow and gets a layout', () => {
  const flow = toFlow(retryGraph)
  assert.equal(flow.nodes.length, 5)
  assert.deepEqual(flow.nodes[0].position, { x: 0, y: 0 })
  assert.equal(flow.nodes[3].position.x, 900) // depth 3
  const back = fromFlow(flow.nodes, flow.edges)
  assert.deepEqual(back.edges, retryGraph.edges)
  assert.deepEqual(back.nodes[2].config, retryGraph.nodes[2].config)
  assert.ok(back.nodes.every((n) => n.position))
})

test('saved positions are kept', () => {
  const graph = { nodes: [{ id: 't', type: 'trigger.incident', config: {}, position: { x: 7, y: 9 } }], edges: [] }
  assert.deepEqual(toFlow(graph).nodes[0].position, { x: 7, y: 9 })
})

test('connection rules', () => {
  const { nodes, edges } = toFlow(retryGraph)
  const c = (source, sourceHandle, target) => connectionProblem({ source, sourceHandle, target }, nodes, edges)
  assert.equal(c('retry', 'success', 'notify'), null)
  assert.match(c('notify', 'next', 'trigger'), /trigger/)
  assert.match(c('notify', 'next', 'notify'), /itself/)
  assert.match(c('notify', 'next', 'classify'), /loop/)
  assert.match(c('retry', 'failed', 'notify'), /already connected/)
  // An output can link to several blocks; an exact duplicate is ignored.
  assert.equal(c('filter', 'true', 'notify'), null)
  const more = connect({ source: 'filter', sourceHandle: 'true', target: 'notify' }, edges)
  assert.deepEqual(
    more.filter((e) => e.source === 'filter' && e.sourceHandle === 'true').map((e) => e.target),
    ['retry', 'notify'],
  )
  assert.equal(connect({ source: 'filter', sourceHandle: 'true', target: 'notify' }, more), more)
  assert.deepEqual(
    fromFlow(nodes, more).edges.filter((e) => e.from === 'filter'),
    [
      { from: 'filter', port: 'true', to: 'retry' },
      { from: 'filter', port: 'true', to: 'notify' },
    ],
  )
})

test('link order follows the canvas, top to bottom', () => {
  const nodes = [
    { id: 'a', position: { x: 0, y: 0 } },
    { id: 'low', position: { x: 300, y: 200 } },
    { id: 'high', position: { x: 300, y: 50 } },
    { id: 'solo', position: { x: 600, y: 0 } },
  ]
  const edges = [
    { id: 'a:next->low', source: 'a', sourceHandle: 'next', target: 'low' },
    { id: 'a:next->high', source: 'a', sourceHandle: 'next', target: 'high' },
    { id: 'high:next->solo', source: 'high', sourceHandle: 'next', target: 'solo' },
  ]
  assert.deepEqual(linkOrder(nodes, edges), { 'a:next->high': 1, 'a:next->low': 2 })
})

test('ids and defaults', () => {
  const { nodes } = toFlow(retryGraph)
  assert.equal(nextNodeId('notify', nodes), 'notify_1')
  assert.equal(nextNodeId('action.clear_failed_tasks', [{ id: 'clear_failed_tasks_1' }]), 'clear_failed_tasks_2')
  const schema = { properties: { a: { default: [1] }, b: { type: 'string' }, c: { default: null } } }
  assert.deepEqual(defaultConfig(schema), { a: [1], c: null })
})

test('layout handles joins and stray nodes', () => {
  const positions = layeredLayout(
    [{ id: 'a' }, { id: 'b' }, { id: 'c' }, { id: 'lonely' }],
    [
      { from: 'a', to: 'b' },
      { from: 'a', to: 'c' },
      { from: 'b', to: 'c' },
    ],
  )
  assert.equal(positions.c.x, 600) // longest path a→b→c
  assert.equal(positions.lonely.x, 0)
})

// ---------------------------------------------------------------------- pipeline model

const dag = (over = {}) => ({
  id: 'd1',
  dag_id: 'orders',
  is_present: true,
  is_monitored: true,
  detect_failures: true,
  sla_minutes: null,
  ...over,
})

test('monitor blocks mirror DAG settings', () => {
  assert.deepEqual(monitorsOf(dag()), ['failure'])
  assert.deepEqual(monitorsOf(dag({ sla_minutes: 30 })), ['failure', 'sla'])
  assert.deepEqual(monitorsOf(dag({ detect_failures: false, sla_minutes: 30 })), ['sla'])
  assert.deepEqual(monitorsOf(dag({ is_monitored: false, sla_minutes: 30 })), [])
})

test('attach and detach bodies', () => {
  const off = dag({ is_monitored: false, sla_minutes: 45 })
  assert.deepEqual(attachBody(off, 'failure'), { is_monitored: true, detect_failures: true, sla_minutes: null })
  assert.deepEqual(attachBody(off, 'sla', 60), { is_monitored: true, sla_minutes: 60, detect_failures: false })
  assert.deepEqual(attachBody(dag(), 'sla', 60), { is_monitored: true, sla_minutes: 60 })
  assert.deepEqual(detachBody(dag(), 'failure'), { detect_failures: false, is_monitored: false })
  assert.deepEqual(detachBody(dag({ sla_minutes: 5 }), 'failure'), { detect_failures: false })
  assert.deepEqual(detachBody(dag({ sla_minutes: 5, detect_failures: false }), 'sla'), {
    sla_minutes: null,
    is_monitored: false,
  })
})

test('workflow feeds follow trigger types and leading filters', () => {
  const feeds = workflowFeeds({ graph: retryGraph })
  assert.deepEqual([...feeds.types], ['DAG_RUN_FAILED'])
  assert.deepEqual([...feeds.dagIds], ['orders'])
  assert.deepEqual([...feeds.envs], ['DEV'])
  const stale = workflowFeeds({ graph: { nodes: [{ id: 't', type: 'trigger.incident_stale', config: {} }], edges: [] } })
  assert.equal(stale.stale, true)
})

test('pipeline diagram wires connection → DAG → monitors → workflows', () => {
  const conn = { id: 'c1', name: 'dev', environment: 'DEV', is_active: true, is_live: true }
  const { nodes, edges } = buildPipeline({
    connections: [conn],
    dagsByConnection: {
      c1: [dag({ sla_minutes: 30 }), dag({ id: 'd2', dag_id: 'other', is_monitored: false })],
    },
    workflows: [{ id: 'w1', name: 'Retry', mode: 'LIVE', graph: retryGraph }],
    counts: { 'c1:orders:DAG_RUN_FAILED': 2 },
    drafts: [{ id: 'draft:1', kind: 'sla', position: { x: 1, y: 2 } }],
    showUnmonitored: false,
    canEdit: true,
  })
  const ids = nodes.map((n) => n.id).sort()
  assert.deepEqual(ids, ['conn:c1', 'dag:d1', 'draft:1', 'mon:failure:d1', 'mon:sla:d1', 'wf:w1'])
  assert.equal(nodes.find((n) => n.id === 'mon:failure:d1').data.count, 2)
  // The retry workflow listens to failures only, so only the failure monitor feeds it.
  const feeds = edges.filter((e) => e.target === 'wf:w1').map((e) => e.source)
  assert.deepEqual(feeds, ['mon:failure:d1'])
  assert.ok(edges.some((e) => e.source === 'conn:c1' && e.target === 'dag:d1'))
})

test('"When a DAG run finishes" workflows are fed by the DAGs they name', () => {
  const runGraph = (dagIds) => ({
    nodes: [{ id: 't', type: 'trigger.dag_run', config: { states: ['success'], dag_ids: dagIds } }],
    edges: [],
  })
  const named = workflowFeeds({ graph: runGraph(['orders']) })
  assert.equal(named.dagRun, true)
  assert.deepEqual([...named.dagIds], ['orders'])
  assert.equal(workflowFeeds({ graph: runGraph([]) }).dagIds, null)

  const conn = { id: 'c1', name: 'dev', environment: 'DEV', is_active: true, is_live: true }
  const { nodes, edges } = buildPipeline({
    connections: [conn],
    dagsByConnection: { c1: [dag(), dag({ id: 'd2', dag_id: 'other' })] },
    workflows: [
      { id: 'w1', name: 'Validate orders', mode: 'LIVE', graph: runGraph(['orders']) },
      { id: 'w2', name: 'Validate all', mode: 'LIVE', graph: runGraph([]) },
    ],
    counts: {},
    checks: { 'c1:orders': { status: 'FAILED', run_id: 'r1' } },
    drafts: [],
    showUnmonitored: false,
    canEdit: false,
  })
  const sources = (wf) => edges.filter((e) => e.target === wf).map((e) => e.source).sort()
  assert.deepEqual(sources('wf:w1'), ['dag:d1'])
  assert.deepEqual(sources('wf:w2'), ['dag:d1', 'dag:d2'])
  assert.equal(nodes.find((n) => n.id === 'dag:d1').data.lastCheck.status, 'FAILED')
  assert.equal(nodes.find((n) => n.id === 'dag:d2').data.lastCheck, null)
})

test('an offline connection shows alone, without its cached DAGs', () => {
  const conn = {
    id: 'c1',
    name: 'dev',
    environment: 'DEV',
    is_active: true,
    is_live: false,
    status_stale: false,
    last_health_status: 'UNREACHABLE',
    last_health_message: 'Cannot reach http://localhost:8080.',
  }
  const { nodes, edges } = buildPipeline({
    connections: [conn],
    dagsByConnection: { c1: [dag({ last_synced_at: '2026-09-24T09:46:42Z' })] },
    workflows: [{ id: 'w1', name: 'Retry', mode: 'LIVE', graph: retryGraph }],
    counts: {},
    drafts: [],
    showUnmonitored: true,
    canEdit: true,
  })
  assert.deepEqual(
    nodes.map((n) => n.id),
    ['conn:c1', 'wf:w1'],
  )
  assert.equal(edges.length, 0)
  assert.equal(nodes[0].data.state.label, 'Connection lost')
})

test('connection state wording', () => {
  const base = { is_active: true, is_live: false, status_stale: false, airflow_version: null, last_health_message: 'x' }
  const label = (over, opts) => connectionState({ ...base, ...over }, opts).label
  assert.equal(label({ is_live: true, last_health_status: 'HEALTHY' }), 'Connected')
  assert.equal(connectionState({ ...base, is_live: true, last_health_status: 'HEALTHY' }).live, true)
  assert.equal(label({ last_health_status: 'UNREACHABLE' }), 'Airflow not connected')
  assert.equal(label({ last_health_status: 'UNREACHABLE' }, { everConnected: true }), 'Connection lost')
  assert.equal(label({ last_health_status: 'UNREACHABLE', airflow_version: '2.10.5' }), 'Connection lost')
  assert.equal(label({ last_health_status: 'HEALTHY', status_stale: true }), 'Status unknown')
  assert.equal(label({ last_health_status: 'UNKNOWN' }), 'Checking connection…')
  assert.equal(label({ last_health_status: 'UNAUTHORIZED' }), 'Authentication failed')
  assert.equal(label({ is_active: false, last_health_status: 'HEALTHY' }), 'Inactive')
})

test('retypeNode keeps the target, applies new defaults and drops edges from missing ports', () => {
  const runDef = {
    type: 'pipeline.run_dag',
    ports: ['success', 'failed'],
    config_schema: { properties: { connection_id: { default: '' }, dag_id: { default: '' }, timeout_minutes: { default: 60 } } },
  }
  const waitDef = {
    type: 'pipeline.wait_for_dag',
    ports: ['success', 'timeout'],
    config_schema: {
      properties: { connection_id: { default: '' }, dag_id: { default: '' }, success_within_minutes: { default: 60 } },
    },
  }
  const node = {
    id: 'orders_pipeline_1',
    data: { nodeType: runDef.type, def: runDef, name: 'orders_pipeline', config: { connection_id: 'c1', dag_id: 'orders_pipeline', timeout_minutes: 5 } },
  }
  const edges = [
    { id: 'a', source: 'orders_pipeline_1', sourceHandle: 'success', target: 'x' },
    { id: 'b', source: 'orders_pipeline_1', sourceHandle: 'failed', target: 'y' },
    { id: 'c', source: 'trigger', sourceHandle: 'next', target: 'orders_pipeline_1' },
  ]
  const out = retypeNode(node, waitDef, edges)
  assert.equal(out.node.data.nodeType, 'pipeline.wait_for_dag')
  assert.equal(out.node.data.name, 'orders_pipeline')
  assert.deepEqual(out.node.data.config, { connection_id: 'c1', dag_id: 'orders_pipeline', success_within_minutes: 60 })
  assert.deepEqual(out.edges.map((e) => e.id), ['a', 'c'])
})

test('parseBlockPayload accepts JSON presets and bare types', () => {
  assert.deepEqual(parseBlockPayload('notify'), { type: 'notify' })
  assert.deepEqual(parseBlockPayload('{"type":"pipeline.run_dag","name":"d"}'), { type: 'pipeline.run_dag', name: 'd' })
  assert.equal(parseBlockPayload('{broken'), null)
  assert.equal(parseBlockPayload(''), null)
})

test('switching the trigger keeps its id, position and link', () => {
  const { nodes, edges } = toFlow(retryGraph)
  const trigger = nodes.find((n) => n.id === 'trigger')
  const manual = { type: 'trigger.manual', ports: ['next'], config_schema: { properties: {} } }
  const schedule = {
    type: 'trigger.schedule',
    ports: ['next'],
    config_schema: { properties: { every_minutes: { default: 60 } } },
  }
  const toManual = retypeNode(trigger, manual, edges)
  assert.equal(toManual.node.id, 'trigger')
  assert.deepEqual(toManual.node.position, trigger.position)
  assert.deepEqual(toManual.node.data.config, {})
  assert.deepEqual(toManual.edges, edges)
  const toSchedule = retypeNode(toManual.node, schedule, toManual.edges)
  assert.equal(toSchedule.node.data.nodeType, 'trigger.schedule')
  assert.deepEqual(toSchedule.node.data.config, { every_minutes: 60 })
  assert.ok(toSchedule.edges.some((e) => e.source === 'trigger' && e.target === 'classify'))
})
