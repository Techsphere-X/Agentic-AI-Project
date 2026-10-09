const TYPE_LABELS = { DAG_RUN_FAILED: 'Run failed', SLA_MISSED: 'SLA missed', DATA_CHECK_FAILED: 'Data check failed' }

export function typeLabel(type) {
  return TYPE_LABELS[type] ?? type
}

export function describeCycle(summary) {
  const parts = [
    `${summary.opened} opened`,
    `${summary.recurred} recurred`,
    `${summary.auto_resolved} auto-resolved`,
  ]
  const errors = summary.errors.length ? ` · ${summary.errors.length} error(s)` : ''
  return `Detection checked ${summary.dags} DAG(s): ${parts.join(', ')}${errors}`
}
