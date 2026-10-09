// Labels and formatting helpers shared by the automation pages.

export const NODE_LABELS = {
  'trigger.incident': 'When an incident opens or recurs',
  'trigger.incident_stale': 'When an incident is ignored',
  'trigger.dag_run': 'When a DAG run finishes',
  'trigger.manual': 'Run on demand',
  'trigger.schedule': 'On a schedule',
  'pipeline.run_dag': 'Run a DAG',
  'pipeline.wait_for_dag': 'Wait for a DAG',
  'database.run_sql': 'Run SQL',
  'database.check': 'Check data',
  'flow.wait': 'Wait',
  'condition.filter': 'Only if…',
  'diagnose.classify_log': 'Diagnose failure',
  'check.dag_state': 'Check the DAG',
  'decide.choose_fix': 'Choose the fix',
  'approval.request': 'Ask a human',
  'action.clear_failed_tasks': 'Retry failed tasks',
  'action.trigger_dag_run': 'Start a new run',
  'action.set_dag_paused': 'Pause / unpause DAG',
  'verify.run_success': 'Check it worked',
  'incident.update': 'Update the incident',
  'check.record': "Record the run's status",
  notify: 'Tell someone',
}

// The canvas groups the backend's block categories into four stages, one colour each.
export const STAGES = [
  { id: 'start', label: 'Start', title: 'Start when…', categories: ['trigger'] },
  { id: 'do', label: 'Do', title: 'Do something', categories: ['pipeline', 'database', 'action'] },
  { id: 'decide', label: 'Decide', title: 'Decide & check', categories: ['logic', 'approval', 'verify', 'diagnosis'] },
  { id: 'tell', label: 'Tell', title: 'Record & tell', categories: ['output'] },
]

const STAGE_BY_CATEGORY = Object.fromEntries(STAGES.flatMap((s) => s.categories.map((c) => [c, s])))

export function stageOf(category) {
  return STAGE_BY_CATEGORY[category] ?? STAGES[1]
}

export const NODE_ICONS = {
  trigger: '⚡',
  condition: '⋔',
  diagnose: '🔎',
  check: '⋔',
  decide: '⚖',
  approval: '✋',
  action: '⚙',
  verify: '✓',
  incident: '✎',
  notify: '✉',
  pipeline: '▶',
  database: '🗄',
  flow: '⏱',
}

// Workflows with these triggers are started by people or the clock, not by incidents.
const BY_HAND_TRIGGERS = new Set(['trigger.manual', 'trigger.schedule'])

export function triggerType(graph) {
  return graph?.nodes?.find((n) => n.type?.startsWith('trigger.'))?.type ?? null
}

export function startsByHand(graph) {
  return BY_HAND_TRIGGERS.has(triggerType(graph))
}

function shortSql(sql = '') {
  const oneLine = sql.replace(/\s+/g, ' ').trim()
  return oneLine.length > 48 ? `${oneLine.slice(0, 47)}…` : oneLine
}

export function nodeIcon(type) {
  return NODE_ICONS[type.split('.')[0]] ?? '•'
}

export const CATEGORY_LABELS = {
  AUTH: 'Login or permission problem',
  DATA_INTEGRITY: 'Bad data',
  SCHEMA: 'Table or column changed',
  CODE_BUG: 'Code error',
  RESOURCE: 'Out of memory or disk',
  TIMEOUT: 'Timeout',
  TRANSIENT_NETWORK: 'Network glitch',
  UPSTREAM_MISSING: 'Missing input',
  UNKNOWN: 'Unknown',
}

/** Short human summary of a node's config, e.g. "categories: TIMEOUT, …; ≤ 2 occurrences". */
export function describeConfig(type, config = {}) {
  const parts = []
  const list = (values) => values.map((v) => CATEGORY_LABELS[v] ?? v).join(', ')
  switch (type) {
    case 'trigger.incident':
      parts.push(`on ${config.events?.join(' / ')}`)
      if (config.incident_types?.length) parts.push(config.incident_types.join(', '))
      break
    case 'trigger.incident_stale':
      parts.push(`open & unacknowledged for ${config.minutes} min`)
      break
    case 'trigger.dag_run':
      parts.push(`on ${(config.states ?? ['success', 'failed']).join(' / ')}`)
      parts.push(config.dag_ids?.length ? config.dag_ids.join(', ') : 'every monitored DAG')
      break
    case 'check.record':
      parts.push(`mark ${config.result ?? 'passed'}`)
      if (config.result === 'failed' && config.open_incident !== false) parts.push('opens an incident')
      break
    case 'trigger.manual':
      parts.push('when someone presses Run now')
      break
    case 'trigger.schedule':
      parts.push(
        config.every_minutes % 60 === 0
          ? `every ${config.every_minutes / 60 === 1 ? 'hour' : `${config.every_minutes / 60} h`}`
          : `every ${config.every_minutes} min`,
      )
      break
    case 'pipeline.run_dag':
      parts.push(config.dag_id || 'choose a DAG')
      if (config.parameters && config.parameters.trim() !== '{}') parts.push('with parameters')
      parts.push(config.wait_for_completion ? `wait ≤ ${config.timeout_minutes} min` : "don't wait")
      break
    case 'pipeline.wait_for_dag':
      parts.push(config.dag_id || 'choose a DAG')
      parts.push(`success in last ${config.success_within_minutes} min`)
      break
    case 'database.run_sql':
      parts.push(shortSql(config.sql) || 'enter SQL')
      if (config.read_only) parts.push('read only')
      break
    case 'database.check':
      parts.push(`${shortSql(config.sql) || 'enter SQL'} ${config.operator ?? '>'} ${config.expected ?? '0'}`)
      if (config.keep_checking_minutes) parts.push(`keep checking ${config.keep_checking_minutes} min`)
      break
    case 'flow.wait':
      parts.push(`${config.minutes} min`)
      break
    case 'condition.filter':
      if (config.diagnosis_categories?.length) parts.push(list(config.diagnosis_categories))
      if (config.environments?.length) parts.push(`env ${config.environments.join('/')}`)
      if (config.incident_types?.length) parts.push(config.incident_types.join(', '))
      if (config.min_severity) parts.push(`severity ≥ ${config.min_severity}`)
      if (config.min_occurrences) parts.push(`≥ ${config.min_occurrences} failures`)
      if (config.max_occurrences) parts.push(`≤ ${config.max_occurrences} failures`)
      if (config.dag_ids?.length) parts.push(`DAGs ${config.dag_ids.join(', ')}`)
      if (config.tags_any?.length) parts.push(`tags ${config.tags_any.join(', ')}`)
      break
    case 'decide.choose_fix':
      parts.push(`up to ${config.max_attempts ?? 2} automatic fixes`)
      if (config.min_confidence_pct != null) parts.push(`confidence ≥ ${config.min_confidence_pct}%`)
      if (config.pause_on_bad_data) parts.push('pause on bad data')
      if (config.pause_after_failures) parts.push(`pause after ${config.pause_after_failures} failures`)
      if (config.ai_mode === 'suggest') parts.push('AI suggests')
      if (config.ai_mode === 'decide') parts.push('AI decides')
      break
    case 'approval.request':
      parts.push(
        config.required_environments?.length
          ? `required in ${config.required_environments.join('/')}`
          : 'always required',
      )
      parts.push(`expires after ${config.timeout_minutes} min`)
      if (config.send_via) parts.push(config.to?.length ? `asks ${config.to.join(', ')}` : 'request sent out')
      break
    case 'action.clear_failed_tasks':
      parts.push(config.include_downstream ? 'with downstream tasks' : 'failed tasks only')
      break
    case 'action.set_dag_paused':
      parts.push(config.paused ? 'pause' : 'unpause')
      break
    case 'verify.run_success':
      parts.push(`wait up to ${config.timeout_minutes} min`)
      break
    case 'incident.update':
      parts.push(config.operation)
      break
    case 'notify':
      if (config.send_via) parts.push(config.to?.length ? `to ${config.to.join(', ')}` : 'sent out')
      else parts.push(config.channel === 'webhook' ? 'webhook' : 'in-app only')
      parts.push(config.level?.toLowerCase())
      break
    default:
  }
  return parts.filter(Boolean).join(' · ')
}

const TRIGGER_EVENTS = { manual: 'Run now', schedule: 'schedule', dag_run: 'DAG run finished' }

/** How a run started: "Run now", "schedule", or "incident opened" etc. */
export function triggerText(event) {
  return TRIGGER_EVENTS[event] ?? `incident ${event}`
}

// Validation status of a finished DAG run (GET /automation/run-checks).
export const CHECK_STATUS = {
  PENDING: { label: 'Checking', tone: 'info' },
  PASSED: { label: 'Passed', tone: 'success' },
  FAILED: { label: 'Failed', tone: 'danger' },
  NOT_CHECKED: { label: 'Not checked', tone: 'neutral' },
  ERROR: { label: 'Workflow error', tone: 'warning' },
}

export function checkStatus(status) {
  return CHECK_STATUS[status] ?? { label: status, tone: 'neutral' }
}

export function duration(run) {
  if (!run.started_at) return '—'
  const end = run.finished_at ? new Date(run.finished_at) : new Date()
  const seconds = Math.max(0, Math.round((end - new Date(run.started_at)) / 1000))
  if (seconds < 60) return `${seconds} s`
  if (seconds < 3600) return `${Math.round(seconds / 60)} min`
  return `${(seconds / 3600).toFixed(1)} h`
}
