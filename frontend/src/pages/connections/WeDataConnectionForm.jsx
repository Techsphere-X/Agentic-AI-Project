import { useState } from 'react'
import { Alert, Button, Field, Modal, StatusPill } from '../../components/ui'
import { useToast } from '../../context/ToastContext'
import { wedataApi } from '../../services/endpoints'

const EMPTY = {
  name: '',
  environment: 'DEV',
  kind: 'LIVE',
  project_id: '',
  region: 'ap-guangzhou',
  endpoint: 'wedata.tencentcloudapi.com',
  secret_id: '',
  secret_key: '',
  is_active: true,
  is_default: false,
}

const REGIONS = [
  { value: 'ap-guangzhou', label: 'Guangzhou (ap-guangzhou)' },
  { value: 'ap-shanghai', label: 'Shanghai (ap-shanghai)' },
  { value: 'ap-beijing', label: 'Beijing (ap-beijing)' },
  { value: 'ap-singapore', label: 'Singapore (ap-singapore)' },
  { value: 'ap-hongkong', label: 'Hong Kong (ap-hongkong)' },
  { value: 'ap-tokyo', label: 'Tokyo (ap-tokyo)' },
  { value: 'ap-bangkok', label: 'Bangkok (ap-bangkok)' },
]

export function TestResult({ result }) {
  if (!result) return null
  return (
    <div className={`test-result ${result.ok ? 'test-ok' : 'test-fail'}`}>
      <StatusPill status={result.status} />
      <p>{result.message}</p>
      <dl className="kv">
        {result.latency_ms != null && (
          <>
            <dt>Latency</dt>
            <dd>{result.latency_ms} ms</dd>
          </>
        )}
        {result.project_id && (
          <>
            <dt>Project ID</dt>
            <dd>{result.project_id}</dd>
          </>
        )}
      </dl>
    </div>
  )
}

export default function WeDataConnectionForm({ connection, allowMock, onClose, onSaved }) {
  const toast = useToast()
  const editing = Boolean(connection)
  const [form, setForm] = useState(() =>
    editing
      ? { ...EMPTY, ...connection, secret_key: '' }
      : EMPTY,
  )
  const [error, setError] = useState(null)
  const [saving, setSaving] = useState(false)
  const [testing, setTesting] = useState(false)
  const [testResult, setTestResult] = useState(null)

  const set = (key) => (e) => {
    const value = e.target.type === 'checkbox' ? e.target.checked : e.target.value
    setForm((f) => ({ ...f, [key]: value }))
    setTestResult(null)
  }

  const isLive = form.kind === 'LIVE'
  const canTest = !isLive || (Boolean(form.project_id) && (editing ? true : Boolean(form.secret_key)))

  function targetPayload() {
    return {
      kind: form.kind,
      project_id: form.project_id.trim(),
      region: form.region,
      endpoint: form.endpoint.trim(),
      secret_id: isLive ? (form.secret_id ? form.secret_id.trim() : null) : null,
      secret_key: isLive && form.secret_key ? form.secret_key.trim() : null,
    }
  }

  async function onTest() {
    setError(null)
    setTesting(true)
    try {
      if (editing && !form.secret_key) {
        const result = await wedataApi.testSaved(connection.id)
        setTestResult(result)
      } else {
        const result = await wedataApi.testUnsaved(targetPayload())
        setTestResult(result)
      }
    } catch (err) {
      setError(err.message)
    } finally {
      setTesting(false)
    }
  }

  async function onSubmit(e) {
    e.preventDefault()
    setError(null)
    setSaving(true)

    const payload = {
      name: form.name.trim(),
      environment: form.environment,
      kind: form.kind,
      project_id: form.project_id.trim(),
      region: form.region,
      endpoint: form.endpoint.trim(),
      secret_id: isLive ? (form.secret_id ? form.secret_id.trim() : null) : null,
      is_active: form.is_active,
      is_default: form.is_default,
    }

    if (form.secret_key) {
      payload.secret_key = form.secret_key.trim()
    }

    try {
      if (editing) {
        await wedataApi.updateConnection(connection.id, payload)
        toast.success(`Updated connection “${form.name}”`)
      } else {
        await wedataApi.createConnection(payload)
        toast.success(`Added connection “${form.name}”`)
      }
      onSaved()
    } catch (err) {
      setError(err.message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <Modal
      title={editing ? `Edit “${connection.name}”` : 'Add Tencent Cloud WeData connection'}
      onClose={onClose}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button
            type="button"
            variant="secondary"
            onClick={onTest}
            disabled={!canTest}
            loading={testing}
          >
            Test connection
          </Button>
          <Button type="submit" form="wedata-form" variant="primary" loading={saving}>
            {editing ? 'Save changes' : 'Add connection'}
          </Button>
        </>
      }
    >
      <form id="wedata-form" onSubmit={onSubmit} className="flow">
        {error && <Alert tone="danger">{error}</Alert>}
        <TestResult result={testResult} />

        <div className="grid grid-2">
          <Field label="Name" required hint="Identifier for this WeData workspace.">
            <input
              type="text"
              required
              value={form.name}
              onChange={set('name')}
              placeholder="e.g. data-lakehouse-prod"
            />
          </Field>
          <Field label="Environment" required>
            <select value={form.environment} onChange={set('environment')}>
              <option value="DEV">DEV</option>
              <option value="STAGING">STAGING</option>
              <option value="PROD">PROD</option>
            </select>
          </Field>
        </div>

        <div className="grid grid-2">
          <Field label="Connection Type" required>
            <select value={form.kind} onChange={set('kind')} disabled={editing}>
              <option value="LIVE">Live Tencent Cloud WeData</option>
              {allowMock && <option value="MOCK">Mock (Offline demo & test)</option>}
            </select>
          </Field>
          <Field label="Project ID / Workspace ID" required hint="WeData Project ID (e.g. 1460947878944567296)">
            <input
              type="text"
              required
              value={form.project_id}
              onChange={set('project_id')}
              placeholder="1460947878944567296"
            />
          </Field>
        </div>

        <div className="grid grid-2">
          <Field label="Region" required>
            <select value={form.region} onChange={set('region')}>
              {REGIONS.map((r) => (
                <option key={r.value} value={r.value}>
                  {r.label}
                </option>
              ))}
            </select>
          </Field>
          <Field label="API Endpoint" required hint="Default: wedata.tencentcloudapi.com">
            <input
              type="text"
              required
              value={form.endpoint}
              onChange={set('endpoint')}
              placeholder="wedata.tencentcloudapi.com"
            />
          </Field>
        </div>

        {isLive && (
          <div className="grid grid-2">
            <Field label="Secret ID" required={isLive} hint="Tencent Cloud CAM SecretId">
              <input
                type="text"
                required={isLive}
                value={form.secret_id}
                onChange={set('secret_id')}
                placeholder="AKID..."
              />
            </Field>
            <Field
              label="Secret Key"
              required={!editing && isLive}
              hint={editing && connection.has_secret ? 'Leave blank to keep stored SecretKey' : 'Tencent Cloud CAM SecretKey'}
            >
              <input
                type="password"
                required={!editing && isLive}
                value={form.secret_key}
                onChange={set('secret_key')}
                placeholder={editing && connection.has_secret ? '••••••••' : 'SecretKey'}
              />
            </Field>
          </div>
        )}

        <div className="flex gap-4">
          <label className="checkbox-label">
            <input type="checkbox" checked={form.is_active} onChange={set('is_active')} />
            Active
          </label>
          <label className="checkbox-label">
            <input type="checkbox" checked={form.is_default} onChange={set('is_default')} />
            Default connection
          </label>
        </div>
      </form>
    </Modal>
  )
}
