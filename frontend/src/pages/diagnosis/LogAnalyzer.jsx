import { useState } from 'react'
import { DiagnosisSummary } from '../../components/diagnosis/DiagnosisCard'
import { Alert, Badge, Button, Card, Field } from '../../components/ui'
import { diagnosisApi } from '../../services/endpoints'

const MAX_BYTES = 256 * 1024

const STATUS = {
  used: ['info', 'AI Model consulted'],
  skipped: ['neutral', 'Pattern rule matched'],
  unavailable: ['warning', 'Pattern matching fallback'],
  disabled: ['neutral', 'Pattern matching only'],
}

function decisionText(result) {
  const { regex, llm_analysis } = result
  if (llm_analysis) {
    return `Root cause diagnosed via deep AI analysis${regex?.rule ? ` with pattern rule match (${regex.rule})` : ''}.`
  }
  if (regex?.rule) {
    return `Regex matched rule (${regex.rule}), so it is used as-is.`
  }
  return 'Only the pattern matching result is available.'
}

export default function LogAnalyzer() {
  const [log, setLog] = useState('')
  const [useLlm, setUseLlm] = useState(true)
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(false)
  const tooBig = new Blob([log]).size > MAX_BYTES

  async function analyze(event) {
    event.preventDefault()
    if (!log.trim() || tooBig) return
    setLoading(true)
    setError(null)
    try {
      setResult(await diagnosisApi.classifyLog(log, useLlm))
    } catch (err) {
      setError(err.message)
      setResult(null)
    } finally {
      setLoading(false)
    }
  }

  const [tone, statusText] = result
    ? (result.llm_analysis
        ? ['success', 'AI Diagnosed']
        : (STATUS[result.ml_status] ?? ['neutral', result.ml_status]))
    : []

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Log analyzer</h1>
          <p className="muted">
            Paste a task log to see how it is diagnosed: regex pattern rules and deep generative
            root-cause analysis. Nothing is saved.
          </p>
        </div>
      </div>

      <Card>
        <form className="form-grid" onSubmit={analyze}>
          <Field label="Task log" error={tooBig ? 'The log is larger than 256 KB; paste the end of it.' : undefined}>
            {(id) => (
              <textarea
                id={id}
                className="mono analyzer-input"
                rows={12}
                value={log}
                onChange={(e) => setLog(e.target.value)}
                placeholder="[2026-10-01, 12:00:00 UTC] {taskinstance.py:2905} ERROR - Task failed with exception…"
                spellCheck={false}
              />
            )}
          </Field>
          <div className="card-actions" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
              <Button variant="primary" type="submit" loading={loading} disabled={!log.trim() || tooBig}>
                {loading ? 'Analyzing with AI…' : 'Analyze'}
              </Button>
              {log && (
                <Button variant="ghost" onClick={() => { setLog(''); setResult(null) }}>
                  Clear
                </Button>
              )}
            </div>
            <label className="small muted" style={{ display: 'inline-flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}>
              <input
                type="checkbox"
                checked={useLlm}
                onChange={(e) => setUseLlm(e.target.checked)}
              />
              <span>🧠 Deep <strong>AI Root-Cause Analysis</strong></span>
            </label>
          </div>
        </form>
      </Card>

      {error && <Alert tone="danger">{error}</Alert>}

      {result && (
        <>
          {result.llm_analysis && (
            <Card
              className="ai-diagnosis-card"
              title="🧠 Deep AI Diagnosis"
              actions={
                <div className="ai-meta-badges">
                  <Badge tone={result.llm_analysis.retry_safe ? 'success' : 'danger'}>
                    {result.llm_analysis.retry_safe ? 'Safe to retry' : 'Requires fix before retry'}
                  </Badge>
                  <Badge tone="info">
                    {result.llm_analysis.model?.toLowerCase().includes('qwen')
                      ? 'AI Diagnostic Model'
                      : (result.llm_analysis.model || 'AI Diagnostic Model')}
                  </Badge>
                  {result.llm_analysis.latency_ms && (
                    <span className="muted small">· {Math.round(result.llm_analysis.latency_ms)} ms</span>
                  )}
                </div>
              }
            >
              <div className="ai-analysis-grid">
                <div className="ai-analysis-section">
                  <span className="ai-analysis-label">Summary</span>
                  <p style={{ margin: 0 }}>{result.llm_analysis.summary}</p>
                </div>

                <div className="ai-analysis-section">
                  <span className="ai-analysis-label">Root cause</span>
                  <p style={{ margin: 0 }}>{result.llm_analysis.root_cause}</p>
                </div>

                {result.llm_analysis.failed_location && (
                  <div className="ai-analysis-section">
                    <span className="ai-analysis-label">Failed location</span>
                    <p style={{ margin: 0 }}>
                      <code style={{ background: '#1e2430', padding: '0.2rem 0.4rem', borderRadius: '4px' }}>
                        {result.llm_analysis.failed_location}
                      </code>
                    </p>
                  </div>
                )}

                <div className="ai-analysis-section">
                  <span className="ai-analysis-label">Suggested remediation</span>
                  <div className="ai-code-box">{result.llm_analysis.suggested_fix}</div>
                </div>
              </div>
            </Card>
          )}

          <Card title="Operational decision" actions={<Badge tone={tone}>{statusText}</Badge>}>
            <DiagnosisSummary diagnosis={result.final} />
            <p className="small muted">{decisionText(result)}</p>
          </Card>

          <div className="detail-grid">
            <Card title="Pattern matching rules">
              <DiagnosisSummary diagnosis={result.regex} />
              {result.regex.rule && <p className="muted small mono">rule: {result.regex.rule}</p>}
            </Card>
            <Card title="AI Model Assessment">
              {result.llm_analysis ? (
                <>
                  <div className="cell-title">
                    <strong>{result.llm_analysis.retry_safe ? 'Automatic Recovery Safe' : 'Manual Intervention Required'}</strong>
                    <span className="muted small">
                      {Math.round((result.llm_analysis.confidence ?? 0.85) * 100)}% confidence
                    </span>
                  </div>
                  <p className="muted small" style={{ marginTop: '0.5rem', marginBottom: 0 }}>
                    {result.llm_analysis.summary}
                  </p>
                  <p className="muted small mono" style={{ marginTop: '0.5rem', marginBottom: 0 }}>
                    {result.llm_analysis.latency_ms ? `latency: ${Math.round(result.llm_analysis.latency_ms)} ms` : 'local inference'}
                  </p>
                </>
              ) : (
                <p className="muted small">No AI analysis available for this query.</p>
              )}
            </Card>
          </div>
        </>
      )}
    </div>
  )
}
