import { Background, Controls, MarkerType, ReactFlow, ReactFlowProvider } from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import '../../components/flow/FlowCanvas.css'
import { useEffect, useMemo, useState } from 'react'
import { BLOCK, portTone, toFlow } from '../../components/flow/graph'
import { automationApi } from '../../services/endpoints'
import BlockNode from './BlockNode'

const NODE_TYPES = { [BLOCK]: BlockNode }

function stepTone(step, def) {
  if (step.status === 'FAILED') return 'danger'
  if (step.status === 'WAITING' || step.status === 'RUNNING') return 'waiting'
  if (step.status === 'SKIPPED') return 'skipped'
  return portTone(def, step.port) === 'bad' ? 'warning' : 'success'
}

/** Read-only replay of a run on its graph snapshot: executed blocks coloured, taken edges lit. */
export default function RunCanvas({ run }) {
  const [catalog, setCatalog] = useState(null)

  useEffect(() => {
    automationApi
      .nodeTypes()
      .then(setCatalog)
      .catch(() => setCatalog([]))
  }, [])

  const flow = useMemo(() => {
    if (!catalog) return null
    const defs = Object.fromEntries(catalog.map((d) => [d.type, d]))
    const lastStep = {}
    run.steps.forEach((s) => (lastStep[s.node_id] = s))
    const { nodes, edges } = toFlow(run.graph, (n) => {
      const step = lastStep[n.id]
      return {
        def: defs[n.type],
        dimmed: !step,
        run: step ? { tone: stepTone(step, defs[n.type]), label: step.message } : null,
      }
    })
    // A link is lit when its output was taken and the block it leads to ran.
    const taken = new Set(run.steps.filter((s) => s.port).map((s) => `${s.node_id}:${s.port}`))
    const reached = new Set(run.steps.map((s) => s.node_id))
    return {
      nodes: nodes.map((n) => ({ ...n, draggable: false, selectable: false })),
      edges: edges.map((e) => {
        const hit = taken.has(`${e.source}:${e.sourceHandle}`) && reached.has(e.target)
        return {
          ...e,
          animated: hit,
          className: `${e.className} ${hit ? 'edge-taken' : 'edge-idle'}`,
          markerEnd: { type: MarkerType.ArrowClosed, width: 18, height: 18 },
        }
      }),
    }
  }, [catalog, run])

  if (!flow) return <p className="muted">Loading…</p>
  return (
    <div className="run-canvas">
      <ReactFlowProvider>
        <ReactFlow
          nodes={flow.nodes}
          edges={flow.edges}
          nodeTypes={NODE_TYPES}
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable={false}
          deleteKeyCode={null}
          colorMode="system"
          fitView
          fitViewOptions={{ padding: 0.15, maxZoom: 1 }}
          minZoom={0.2}
          proOptions={{ hideAttribution: true }}
        >
          <Background gap={20} />
          <Controls showInteractive={false} />
        </ReactFlow>
      </ReactFlowProvider>
      <div className="run-legend small muted">
        <span className="legend-dot legend-success" /> done <span className="legend-dot legend-warning" /> took a
        &ldquo;no&rdquo; / failed branch <span className="legend-dot legend-waiting" /> waiting{' '}
        <span className="legend-dot legend-danger" /> error · faded blocks were not reached
      </div>
    </div>
  )
}
