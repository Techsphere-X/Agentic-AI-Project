import { useState } from 'react'
import { DiagnosisSummary } from '../../components/diagnosis/DiagnosisCard'
import { Alert, Badge, Button, Card, Field } from '../../components/ui'
import { diagnosisApi } from '../../services/endpoints'

const MAX_BYTES = 256 * 1024

const STATUS = {
  used: ['success', 'Qwen3 diagnosed'],
  unavailable: ['warning', 'AI unavailable – pattern rules fallback'],
  disabled: ['neutral', 'Pattern rules only'],
}

function decisionText(result) {
  const { regex, llm_analysis } = result
  if (llm_analysis) {
    const agrees = regex?.rule && regex.category === llm_analysis.category
    return `Diagnosed by ${llm_analysis.model}${regex?.rule ? ` (pattern rule ${regex.rule} ${agrees ? 'agrees' : 'disagrees'})` : ''}.`
  }
  if (result.ml_status === 'unavailable') {
    return 'The AI model could not be reached (is Ollama running?), so the pattern rules result is shown.'
  }
  return regex?.rule ? `Pattern rule (${regex.rule}) matched.` : 'Only the pattern matching result is available.'
}

export default function LogAnalyzer() {
  const [log, setLog] = useState('')
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
      setResult(await diagnosisApi.classifyLog(log))
    } catch (err) {
      setError(err.message)
      setResult(null)
    } finally {
      setLoading(false)
    }
  }

  const [tone, statusText] = result ? (STATUS[result.ml_status] ?? ['neutral', result.ml_status]) : []
  const ai = result?.llm_analysis

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Log analyzer</h1>
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
          <div className="card-actions" style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
            <Button variant="primary" type="submit" loading={loading} disabled={!log.trim() || tooBig}>
              {loading ? 'Analyzing with Qwen3…' : 'Analyze'}
            </Button>
            {log && (
              <Button variant="ghost" onClick={() => { setLog(''); setResult(null) }}>
                Clear
              </Button>
            )}
          </div>
        </form>
      </Card>

      {error && <Alert tone="danger">{error}</Alert>}

      {result && (
        <>
          {ai && (
            <Card
              className="ai-diagnosis-card"
              title="🧠 Deep AI Diagnosis"
              actions={
                <div className="ai-meta-badges">
                  <Badge tone="neutral">{ai.label}</Badge>
                  <Badge tone={ai.retry_safe ? 'success' : 'danger'}>
                    {ai.retry_safe ? 'Safe to retry' : 'Requires fix before retry'}
                  </Badge>
                  <Badge tone="info">{ai.model}</Badge>
                  {ai.latency_ms && (
                    <span className="muted small">· {Math.round(ai.latency_ms)} ms</span>
                  )}
                </div>
              }
            >
              <div className="ai-analysis-grid">
                <div className="ai-analysis-section">
                  <span className="ai-analysis-label">Summary</span>
                  <p style={{ margin: 0 }}>{ai.summary}</p>
                </div>

                <div className="ai-analysis-section">
                  <span className="ai-analysis-label">Root cause</span>
                  <p style={{ margin: 0 }}>{ai.root_cause}</p>
                </div>

                {ai.evidence_line && (
                  <div className="ai-analysis-section">
                    <span className="ai-analysis-label">Evidence from log</span>
                    <div className="ai-code-box mono">{ai.evidence_line}</div>
                  </div>
                )}

                {ai.failed_location && (
                  <div className="ai-analysis-section">
                    <span className="ai-analysis-label">Failed location</span>
                    <p style={{ margin: 0 }}>
                      <code style={{ background: '#1e2430', padding: '0.2rem 0.4rem', borderRadius: '4px' }}>
                        {ai.failed_location}
                      </code>
                    </p>
                  </div>
                )}

                <div className="ai-analysis-section">
                  <span className="ai-analysis-label">Suggested remediation</span>
                  <div className="ai-code-box">{ai.suggested_fix}</div>
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
            <Card title={`AI Model Assessment${ai ? ` · ${ai.model}` : ''}`}>
              {ai ? (
                <>
                  <div className="cell-title">
                    <strong>{ai.label} <span className="muted small mono">({ai.category})</span></strong>
                    <span className="muted small">{Math.round((ai.confidence ?? 0) * 100)}% confidence</span>
                  </div>
                  <p className="muted small" style={{ marginTop: '0.5rem', marginBottom: 0 }}>
                    {ai.retry_safe ? 'Automatic recovery safe' : 'Manual intervention required'}
                  </p>
                  {ai.evidence_line ? (
                    <p className="muted small mono" style={{ marginTop: '0.5rem', marginBottom: 0 }}>
                      evidence: {ai.evidence_line}
                    </p>
                  ) : (
                    <p className="muted small" style={{ marginTop: '0.5rem', marginBottom: 0 }}>
                      No exact evidence line found in the log, so confidence is capped.
                    </p>
                  )}
                  <p className="muted small mono" style={{ marginTop: '0.5rem', marginBottom: 0 }}>
                    {ai.latency_ms ? `latency: ${Math.round(ai.latency_ms)} ms` : 'local inference'}
                  </p>
                </>
              ) : (
                <p className="muted small">
                  {result.ml_status === 'unavailable'
                    ? 'Qwen3 could not be reached. Start Ollama (ollama serve) and try again.'
                    : 'No AI analysis available for this query.'}
                </p>
              )}
            </Card>
          </div>
        </>
      )}
    </div>
  )
}
