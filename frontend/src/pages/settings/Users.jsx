import { useCallback, useEffect, useState } from 'react'
import { Alert, Badge, Button, Card, Field, LoadError, Modal } from '../../components/ui'
import { formatRelative } from '../../format'
import { useAuth } from '../../context/AuthContext'
import { useToast } from '../../context/ToastContext'
import { usersApi } from '../../services/endpoints'
import { ROLES } from '../../types'

const ROLE_HELP = {
  ADMIN: 'Manage users and Airflow connections',
  OPERATOR: 'Test connections, sync DAGs, choose monitored DAGs',
  VIEWER: 'Read-only',
}

function CreateUserModal({ onClose, onCreated }) {
  const [form, setForm] = useState({ email: '', full_name: '', password: '', role: 'VIEWER' })
  const [error, setError] = useState(null)
  const [saving, setSaving] = useState(false)
  const set = (key) => (e) => setForm((f) => ({ ...f, [key]: e.target.value }))

  async function onSubmit(event) {
    event.preventDefault()
    setError(null)
    if (form.password.length < 12) return setError('Password must be at least 12 characters.')
    setSaving(true)
    try {
      onCreated(await usersApi.create({ ...form, email: form.email.trim(), full_name: form.full_name.trim() }))
    } catch (err) {
      setError(err.message)
    } finally {
      setSaving(false)
    }
  }

  return (
    <Modal
      title="New user"
      onClose={onClose}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" type="submit" form="user-form" loading={saving}>
            Create user
          </Button>
        </>
      }
    >
      <form id="user-form" className="form-grid" onSubmit={onSubmit} noValidate>
        {error && <Alert tone="danger">{error}</Alert>}
        <Field label="Full name">
          {(id) => <input id={id} value={form.full_name} onChange={set('full_name')} autoFocus />}
        </Field>
        <Field label="Email">{(id) => <input id={id} type="email" value={form.email} onChange={set('email')} />}</Field>
        <Field label="Initial password" hint="At least 12 characters. Share it securely.">
          {(id) => (
            <input id={id} type="password" value={form.password} onChange={set('password')} autoComplete="new-password" />
          )}
        </Field>
        <Field label="Role" hint={ROLE_HELP[form.role]}>
          {(id) => (
            <select id={id} value={form.role} onChange={set('role')}>
              {ROLES.map((r) => (
                <option key={r} value={r}>
                  {r.toLowerCase()}
                </option>
              ))}
            </select>
          )}
        </Field>
      </form>
    </Modal>
  )
}

export default function Users() {
  const { user: me } = useAuth()
  const toast = useToast()
  const [users, setUsers] = useState(null)
  const [creating, setCreating] = useState(false)

  const [loadError, setLoadError] = useState(null)

  const load = useCallback(async () => {
    setLoadError(null)
    try {
      setUsers((await usersApi.list({ limit: 200 })).items)
    } catch (err) {
      setLoadError(err.message)
      toast.error(err.message)
    }
  }, [toast])

  useEffect(() => {
    // eslint-disable-next-line react/set-state-in-effect -- initial fetch; state is set after await
    load()
  }, [load])

  async function update(user, patch, message) {
    try {
      const updated = await usersApi.update(user.id, patch)
      setUsers((all) => all.map((u) => (u.id === updated.id ? updated : u)))
      toast.success(message)
    } catch (err) {
      toast.error(err.message)
    }
  }

  return (
    <div className="page">
      <div className="page-header">
        <div>
          <h1>Users</h1>
          <p className="muted">Accounts are admin-created; there is no public sign-up.</p>
        </div>
        <Button variant="primary" onClick={() => setCreating(true)}>
          New user
        </Button>
      </div>

      <Card>
        {loadError && !users ? (
          <LoadError message={loadError} onRetry={load} />
        ) : !users ? (
          <p className="muted">Loading…</p>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>User</th>
                  <th>Role</th>
                  <th>Status</th>
                  <th>Last sign-in</th>
                  <th aria-label="Actions" />
                </tr>
              </thead>
              <tbody>
                {users.map((u) => (
                  <tr key={u.id} className={u.is_active ? '' : 'row-muted'}>
                    <td>
                      <div className="cell-title">
                        {u.full_name}
                        {u.id === me.id && <Badge tone="info">you</Badge>}
                      </div>
                      <div className="muted small">{u.email}</div>
                    </td>
                    <td>
                      <select
                        value={u.role}
                        aria-label={`Role for ${u.email}`}
                        onChange={(e) => update(u, { role: e.target.value }, `${u.email} is now ${e.target.value.toLowerCase()}`)}
                      >
                        {ROLES.map((r) => (
                          <option key={r} value={r}>
                            {r.toLowerCase()}
                          </option>
                        ))}
                      </select>
                    </td>
                    <td>{u.is_active ? <Badge tone="success">active</Badge> : <Badge>deactivated</Badge>}</td>
                    <td className="small">{formatRelative(u.last_login_at)}</td>
                    <td className="actions">
                      {u.id !== me.id && (
                        <Button
                          size="sm"
                          variant={u.is_active ? 'ghost-danger' : 'ghost'}
                          onClick={() =>
                            update(u, { is_active: !u.is_active }, `${u.email} ${u.is_active ? 'deactivated' : 'reactivated'}`)
                          }
                        >
                          {u.is_active ? 'Deactivate' : 'Reactivate'}
                        </Button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      {creating && (
        <CreateUserModal
          onClose={() => setCreating(false)}
          onCreated={(u) => {
            setCreating(false)
            setUsers((all) => [...(all ?? []), u])
            toast.success(`Created ${u.email}`)
          }}
        />
      )}
    </div>
  )
}
