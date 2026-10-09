import { Handle, Position } from '@xyflow/react'
import { memo } from 'react'
import { Badge, StatusPill } from '../../components/ui'
import { checkStatus } from '../automation/automationText'

// Node components for the pipeline canvas: Airflow connection → DAG → monitor → automation.

export const ConnectionNode = memo(function ConnectionNode({ data, selected }) {
  const { conn, state } = data
  return (
    <div
      className={`pnode pnode-conn ${state.live ? '' : `pnode-offline pnode-offline-${state.tone}`} ${selected ? 'pnode-selected' : ''}`}
    >
      <div className="pnode-kicker">Airflow</div>
      <div className="pnode-title">{conn.name}</div>
      <div className="pnode-meta">
        <Badge tone={conn.environment === 'PROD' ? 'danger' : 'neutral'}>{conn.environment}</Badge>
        <StatusPill tone={state.tone} label={state.label} title={state.message ?? state.hint} />
      </div>
      {!state.live && <div className="pnode-offline-note small">Pipelines hidden until Airflow is reachable.</div>}
      <Handle type="source" position={Position.Right} isConnectable={false} />
    </div>
  )
})

export const DagNode = memo(function DagNode({ data, selected }) {
  const { dag, canEdit, lastCheck } = data
  const check = lastCheck && checkStatus(lastCheck.status)
  const classes = ['pnode', 'pnode-dag', selected && 'pnode-selected', !dag.is_monitored && 'pnode-faded']
  return (
    <div className={classes.filter(Boolean).join(' ')}>
      <Handle type="target" position={Position.Left} isConnectable={false} />
      <div className="pnode-kicker">DAG</div>
      <div className="pnode-title mono">{dag.dag_id}</div>
      <div className="pnode-meta">
        {dag.schedule_summary && <span className="muted small mono">{dag.schedule_summary}</span>}
        {dag.is_paused && <Badge tone="warning">paused</Badge>}
        {!dag.is_monitored && <span className="muted small">not monitored</span>}
        {check && (
          <Badge tone={check.tone} title={`Latest run ${lastCheck.run_id}${lastCheck.message ? `: ${lastCheck.message}` : ''}`}>
            {check.label}
          </Badge>
        )}
      </div>
      <Handle
        type="source"
        position={Position.Right}
        isConnectable={canEdit}
        className="pnode-handle-out"
        title={canEdit ? 'Drag to a monitor block to attach it' : undefined}
      />
    </div>
  )
})

const MONITOR_TEXT = {
  failure: { icon: '⚠', title: 'Failure monitor', hint: 'Opens an incident when a run fails' },
  sla: { icon: '⏱', title: 'Freshness SLA', hint: 'Opens an incident when no run succeeds in time' },
}

export const MonitorNode = memo(function MonitorNode({ data, selected }) {
  const text = MONITOR_TEXT[data.kind]
  const classes = ['pnode', 'pnode-monitor', `pnode-monitor-${data.kind}`, selected && 'pnode-selected', data.draft && 'pnode-draft']
  return (
    <div className={classes.filter(Boolean).join(' ')}>
      <Handle type="target" position={Position.Left} isConnectable={data.draft} />
      <div className="pnode-head">
        <span className="block-icon" aria-hidden="true">
          {text.icon}
        </span>
        <div>
          <div className="pnode-title">
            {text.title}
            {data.kind === 'sla' && !data.draft && ` · ${data.minutes} min`}
          </div>
          <div className="muted small">{data.draft ? 'Connect a DAG to attach' : text.hint}</div>
        </div>
      </div>
      {!data.draft && (
        <div className="pnode-meta">
          <Badge tone={data.count ? 'danger' : 'success'}>
            {data.count ? `${data.count} open incident${data.count === 1 ? '' : 's'}` : 'healthy'}
          </Badge>
        </div>
      )}
      {!data.draft && <Handle type="source" position={Position.Right} isConnectable={false} />}
    </div>
  )
})

export const WorkflowNode = memo(function WorkflowNode({ data, selected }) {
  const { workflow } = data
  return (
    <div className={`pnode pnode-workflow ${selected ? 'pnode-selected' : ''}`}>
      <Handle type="target" position={Position.Left} isConnectable={false} />
      <div className="pnode-kicker">Automation</div>
      <div className="pnode-title">{workflow.name}</div>
      <div className="pnode-meta">
        {workflow.mode === 'DRY_RUN' ? <Badge tone="info">dry run</Badge> : <Badge tone="success">live</Badge>}
        {data.stale && <span className="muted small">for ignored incidents</span>}
      </div>
    </div>
  )
})
