import { useEffect, useId, useRef, useState } from 'react'

export function Button({
  variant = 'secondary',
  size,
  loading,
  disabled,
  children,
  className = '',
  ...props
}) {
  const classes = ['btn', `btn-${variant}`, size && `btn-${size}`, className].filter(Boolean).join(' ')
  return (
    <button type="button" className={classes} disabled={loading || disabled} {...props}>
      {loading && <span className="spinner" aria-hidden="true" />}
      {children}
    </button>
  )
}

export function ActionMenu({ label = 'More actions', children }) {
  const [open, setOpen] = useState(false)
  const menuRef = useRef(null)

  useEffect(() => {
    if (!open) return undefined
    const close = (event) => {
      if (!menuRef.current?.contains(event.target)) setOpen(false)
    }
    const onKeyDown = (event) => {
      if (event.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', close)
    document.addEventListener('keydown', onKeyDown)
    return () => {
      document.removeEventListener('mousedown', close)
      document.removeEventListener('keydown', onKeyDown)
    }
  }, [open])

  return (
    <div ref={menuRef} className="action-menu">
      <button
        type="button"
        className="icon-btn action-menu-trigger"
        aria-label={label}
        aria-haspopup="menu"
        aria-expanded={open}
        onClick={() => setOpen((value) => !value)}
      >
        <span aria-hidden="true">⋯</span>
      </button>
      {open && (
        <div className="action-menu-popover" role="menu">
          {typeof children === 'function' ? children(() => setOpen(false)) : children}
        </div>
      )}
    </div>
  )
}

export function ActionMenuItem({ children, tone, onClick, disabled }) {
  return (
    <button
      type="button"
      role="menuitem"
      className={`action-menu-item${tone ? ` action-menu-item-${tone}` : ''}`}
      disabled={disabled}
      onClick={onClick}
    >
      {children}
    </button>
  )
}

export function ConfirmDialog({ open, title = 'Confirm deletion', message, onCancel, onConfirm, loading }) {
  if (!open) return null
  return (
    <Modal title={title} onClose={onCancel} footer={
      <>
        <Button onClick={onCancel} disabled={loading}>Cancel</Button>
        <Button variant="danger" onClick={onConfirm} loading={loading}>Delete</Button>
      </>
    }>
      <p>{message}</p>
    </Modal>
  )
}

export function Field({ label, hint, error, children }) {
  const id = useId()
  const child = typeof children === 'function' ? children(id) : children
  return (
    <div className="field">
      <label htmlFor={id}>{label}</label>
      {child}
      {hint && !error && <small className="field-hint">{hint}</small>}
      {error && <small className="field-error">{error}</small>}
    </div>
  )
}

export function Card({ title, actions, children, className = '' }) {
  return (
    <section className={`card ${className}`}>
      {(title || actions) && (
        <header className="card-header">
          {title && <h2>{title}</h2>}
          {actions && <div className="card-actions">{actions}</div>}
        </header>
      )}
      {children}
    </section>
  )
}

export function Skeleton({ width, height = '1rem', className = '' }) {
  return (
    <span
      className={`skeleton ${className}`}
      style={{ width, height }}
      aria-hidden="true"
    />
  )
}

export function ErrorState({ title = 'Unable to load this section', message, onRetry }) {
  return (
    <div className="error-state" role="alert">
      <strong>{title}</strong>
      {message && <span className="small">{message}</span>}
      {onRetry && (
        <Button size="sm" onClick={onRetry}>
          Retry
        </Button>
      )}
    </div>
  )
}

export function Badge({ tone = 'neutral', children, title }) {
  return (
    <span className={`badge badge-${tone}`} title={title}>
      {children}
    </span>
  )
}

const STATUS_TONES = {
  HEALTHY: 'success',
  ok: 'success',
  UNKNOWN: 'neutral',
  UNAUTHORIZED: 'danger',
  FORBIDDEN: 'warning',
  UNREACHABLE: 'danger',
  TLS_ERROR: 'danger',
  INVALID_ENDPOINT: 'warning',
  AIRFLOW_ERROR: 'danger',
  OK: 'success',
  FAILED: 'danger',
  degraded: 'warning',
  error: 'danger',
}

const STATUS_LABELS = {
  HEALTHY: 'Healthy',
  UNKNOWN: 'Not tested',
  UNAUTHORIZED: 'Unauthorized',
  FORBIDDEN: 'Forbidden',
  UNREACHABLE: 'Unreachable',
  TLS_ERROR: 'TLS error',
  INVALID_ENDPOINT: 'Invalid endpoint',
  AIRFLOW_ERROR: 'Airflow error',
  OK: 'Sent',
  FAILED: 'Failed',
}

export function StatusPill({ status, label, title, tone }) {
  return (
    <span className={`pill pill-${tone ?? STATUS_TONES[status] ?? 'neutral'}`} title={title}>
      <span className="pill-dot" aria-hidden="true" />
      {label ?? STATUS_LABELS[status] ?? status}
    </span>
  )
}

export function Modal({ title, onClose, children, footer, wide }) {
  const dialogRef = useRef(null)
  const previousFocusRef = useRef(null)

  useEffect(() => {
    previousFocusRef.current = document.activeElement
    const focusable = dialogRef.current?.querySelector('button, input, select, textarea, a[href]')
    focusable?.focus()
    const onKey = (e) => e.key === 'Escape' && onClose()
    window.addEventListener('keydown', onKey)
    return () => {
      window.removeEventListener('keydown', onKey)
      previousFocusRef.current?.focus?.()
    }
  }, [onClose])

  return (
    <div className="modal-backdrop" onMouseDown={(e) => e.target === e.currentTarget && onClose()}>
      <div ref={dialogRef} className={`modal ${wide ? 'modal-wide' : ''}`} role="dialog" aria-modal="true" aria-label={title}>
        <header className="modal-header">
          <h2>{title}</h2>
          <button type="button" className="icon-btn" onClick={onClose} aria-label="Close">
            ×
          </button>
        </header>
        <div className="modal-body">{children}</div>
        {footer && <footer className="modal-footer">{footer}</footer>}
      </div>
    </div>
  )
}

export function Toggle({ checked, onChange, disabled, label }) {
  return (
    <label className={`toggle ${disabled ? 'toggle-disabled' : ''}`}>
      <input
        type="checkbox"
        checked={checked}
        disabled={disabled}
        onChange={(e) => onChange(e.target.checked)}
        aria-label={label}
      />
      <span className="toggle-track" aria-hidden="true">
        <span className="toggle-thumb" />
      </span>
    </label>
  )
}

export function EmptyState({ title, children }) {
  return (
    <div className="empty">
      <strong>{title}</strong>
      {children && <div>{children}</div>}
    </div>
  )
}

export function Alert({ tone = 'info', children }) {
  return <div className={`alert alert-${tone}`}>{children}</div>
}
