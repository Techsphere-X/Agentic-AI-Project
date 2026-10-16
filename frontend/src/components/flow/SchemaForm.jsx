import { useEffect, useId, useState } from 'react'
import { airflowApi, channelsApi, databaseApi } from '../../services/endpoints'
import { Toggle } from '../ui'

// Renders a settings form from a Pydantic JSON schema (the node-type catalog's config_schema).
// Supports the shapes our node configs use: booleans, integers, strings, enums ($ref or inline,
// optionally nullable), arrays of enums (checkbox groups) and arrays of strings (comma lists).
// Fields may carry UI hints: `x-label`, `x-hint`, and `x-widget` (see WIDGETS below).

const LONG_TEXT = new Set(['message', 'note', 'description'])

function resolve(schema, root) {
  if (schema?.$ref) {
    const name = schema.$ref.split('/').at(-1)
    return { ...root.$defs?.[name], ...schema, $ref: undefined }
  }
  return schema ?? {}
}

/** Collapse `anyOf: [X, {type: null}]` into X with `nullable: true`. */
function normalize(schema, root) {
  const resolved = resolve(schema, root)
  if (resolved.type === 'array' && resolved.items) {
    return { ...resolved, items: resolve(resolved.items, root) }
  }
  if (resolved.anyOf) {
    const options = resolved.anyOf.map((s) => resolve(s, root))
    const nonNull = options.filter((s) => s.type !== 'null')
    if (nonNull.length === 1) {
      return normalize(
        { ...nonNull[0], nullable: options.length > 1, title: resolved.title, default: resolved.default },
        root,
      )
    }
  }
  return resolved
}

function humanize(key) {
  const text = key.replaceAll('_', ' ')
  return text.charAt(0).toUpperCase() + text.slice(1)
}

function EnumChecks({ options, labels = {}, value, onChange, disabled }) {
  const selected = new Set(value ?? [])
  return (
    <div className="check-group">
      {options.map((option) => (
        <label key={option} className="checkbox-inline">
          <input
            type="checkbox"
            checked={selected.has(option)}
            disabled={disabled}
            onChange={(e) => {
              const next = new Set(selected)
              if (e.target.checked) next.add(option)
              else next.delete(option)
              onChange(options.filter((o) => next.has(o))) // keep schema order
            }}
          />
          <span className="small">{labels[option] ?? option}</span>
        </label>
      ))}
    </div>
  )
}

/** Loads a list once per `key` (null = nothing to load). */
function useList(key, load) {
  const [state, setState] = useState({ key: null, items: [] })
  useEffect(() => {
    if (!key) return undefined
    let alive = true
    load()
      .then((items) => alive && setState({ key, items }))
      .catch(() => alive && setState({ key, items: [] }))
    return () => {
      alive = false
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- reload only when the key changes
  }, [key])
  return state.key === key ? state.items : null
}

function ConnectionSelect({ id, value, onChange, disabled, kind }) {
  const api = kind === 'database' ? databaseApi : airflowApi
  const items = useList(kind, () => api.listConnections().then((r) => r.items))
  const missing = value && items && !items.some((c) => c.id === value)
  return (
    <select id={id} value={value ?? ''} disabled={disabled || !items} onChange={(e) => onChange(e.target.value)}>
      <option value="" disabled>
        {items ? (items.length ? '— choose —' : 'No connections in the catalog') : 'Loading…'}
      </option>
      {missing && <option value={value}>Deleted connection</option>}
      {items?.map((c) => (
        <option key={c.id} value={c.id}>
          {c.name} ({c.environment.toLowerCase()}){c.is_active ? '' : ' — off'}
        </option>
      ))}
    </select>
  )
}

/** DAG id: free text with suggestions from the chosen connection's synced DAGs. */
function DagInput({ id, value, onChange, disabled, connectionId }) {
  const listId = `${id}-dags`
  const dags = useList(connectionId ? `dags:${connectionId}` : null, () =>
    airflowApi.listAllDags(connectionId).then((dags) => dags.map((d) => d.dag_id)),
  )
  return (
    <>
      <input
        id={id}
        type="text"
        list={listId}
        value={value ?? ''}
        disabled={disabled}
        placeholder={connectionId ? 'Type or pick a DAG id' : 'Choose a connection first'}
        onChange={(e) => onChange(e.target.value.trim())}
      />
      <datalist id={listId}>
        {dags?.map((d) => (
          <option key={d} value={d} />
        ))}
      </datalist>
    </>
  )
}

/** A notification channel from the catalog, or none (in-app notification only). */
function ChannelSelect({ id, value, onChange, disabled }) {
  const items = useList('channels', () => channelsApi.listConnections().then((r) => r.items))
  const missing = value && items && !items.some((c) => c.id === value)
  return (
    <select id={id} value={value ?? ''} disabled={disabled || !items} onChange={(e) => onChange(e.target.value)}>
      <option value="">{items ? 'In-app only (no message out)' : 'Loading…'}</option>
      {missing && <option value={value}>Deleted channel</option>}
      {items?.map((c) => (
        <option key={c.id} value={c.id}>
          {c.name} ({c.kind.toLowerCase()}){c.is_active ? '' : ' — off'}
        </option>
      ))}
    </select>
  )
}

const WIDGETS = {
  notification_channel: ChannelSelect,
  airflow_connection: (props) => <ConnectionSelect {...props} kind="airflow" />,
  database_connection: (props) => <ConnectionSelect {...props} kind="database" />,
  dag: (props) => <DagInput {...props} connectionId={props.values?.connection_id} />,
  sql: ({ id, value, onChange, disabled, schema }) => (
    <textarea
      id={id}
      className="code-input"
      rows={6}
      spellCheck={false}
      value={value ?? ''}
      maxLength={schema.maxLength}
      disabled={disabled}
      placeholder="SELECT count(*) FROM orders WHERE order_date = current_date"
      onChange={(e) => onChange(e.target.value)}
    />
  ),
  json: ({ id, value, onChange, disabled, schema }) => (
    <textarea
      id={id}
      className="code-input"
      rows={3}
      spellCheck={false}
      value={value ?? ''}
      maxLength={schema.maxLength}
      disabled={disabled}
      onChange={(e) => onChange(e.target.value)}
    />
  ),
}

function FieldControl({ name, schema, value, values, onChange, disabled, id }) {
  const Widget = WIDGETS[schema['x-widget']]
  if (Widget) return <Widget id={id} schema={schema} value={value} values={values} onChange={onChange} disabled={disabled} />
  if (schema.type === 'boolean') {
    return <Toggle checked={Boolean(value)} disabled={disabled} onChange={onChange} label={schema['x-label'] ?? humanize(name)} />
  }
  if (schema.enum) {
    return (
      <select id={id} value={value ?? ''} disabled={disabled} onChange={(e) => onChange(e.target.value || null)}>
        {schema.nullable && <option value="">— none —</option>}
        {!schema.nullable && value == null && (
          <option value="" disabled>
            — choose —
          </option>
        )}
        {schema.enum.map((option) => (
          <option key={option} value={option}>
            {option}
          </option>
        ))}
      </select>
    )
  }
  if (schema.type === 'array') {
    const items = schema.items ?? {}
    if (items.enum) {
      return (
        <EnumChecks
          options={items.enum}
          labels={schema['x-options']}
          value={value}
          onChange={onChange}
          disabled={disabled}
        />
      )
    }
    return (
      <input
        id={id}
        type="text"
        value={(value ?? []).join(', ')}
        disabled={disabled}
        placeholder="comma, separated"
        onChange={(e) =>
          onChange(
            e.target.value
              .split(',')
              .map((v) => v.trim())
              .filter(Boolean),
          )
        }
      />
    )
  }
  if (schema.type === 'integer' || schema.type === 'number') {
    return (
      <input
        id={id}
        type="number"
        value={value ?? ''}
        min={schema.minimum}
        max={schema.maximum}
        disabled={disabled}
        placeholder={schema.nullable ? 'not set' : undefined}
        onChange={(e) => {
          if (e.target.value === '') return onChange(schema.nullable ? null : undefined)
          const parsed = Number(e.target.value)
          onChange(Number.isNaN(parsed) ? value : parsed)
        }}
      />
    )
  }
  if (LONG_TEXT.has(name)) {
    return (
      <textarea
        id={id}
        rows={3}
        value={value ?? ''}
        maxLength={schema.maxLength}
        disabled={disabled}
        onChange={(e) => onChange(e.target.value)}
      />
    )
  }
  return (
    <input
      id={id}
      type="text"
      value={value ?? ''}
      maxLength={schema.maxLength}
      disabled={disabled}
      onChange={(e) => onChange(schema.nullable && e.target.value === '' ? null : e.target.value)}
    />
  )
}

function SchemaField({ name, schema, value, values, onChange, disabled, required }) {
  const id = useId()
  const hint = [
    schema['x-hint'],
    !schema['x-hint'] && schema.minimum !== undefined && schema.maximum !== undefined && `${schema.minimum}–${schema.maximum}`,
    schema.type === 'array' && !schema.items?.enum && 'Empty = any',
    schema.type === 'array' && schema.items?.enum && 'None selected = any',
  ]
    .filter(Boolean)
    .join(' · ')
  return (
    <div className="field">
      <label htmlFor={id}>
        {schema['x-label'] ?? humanize(name)}
        {required && ' *'}
      </label>
      <FieldControl id={id} name={name} schema={schema} value={value} values={values} onChange={onChange} disabled={disabled} />
      {hint && <small className="field-hint">{hint}</small>}
    </div>
  )
}

export default function SchemaForm({ schema, value, onChange, disabled }) {
  // `x-hidden` fields (kept for old workflows) only show while they hold a non-default value.
  const properties = Object.entries(schema?.properties ?? {}).filter(
    ([name, prop]) => !prop['x-hidden'] || (value?.[name] ?? null) !== (prop.default ?? null),
  )
  if (!properties.length) return <p className="muted small">This block has no settings.</p>
  const required = new Set(schema.required ?? [])
  return (
    <div className="form-grid">
      {properties.map(([name, prop]) => (
        <SchemaField
          key={name}
          name={name}
          schema={normalize(prop, schema)}
          value={value?.[name]}
          values={value}
          required={required.has(name)}
          disabled={disabled}
          onChange={(next) => {
            const updated = { ...value }
            if (next === undefined) delete updated[name]
            else updated[name] = next
            onChange(updated)
          }}
        />
      ))}
    </div>
  )
}
