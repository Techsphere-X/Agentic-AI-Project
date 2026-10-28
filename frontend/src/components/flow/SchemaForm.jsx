import { useEffect, useId, useRef, useState } from 'react'
import { airflowApi, channelsApi, databaseApi } from '../../services/endpoints'
import { Toggle } from '../ui'

// Renders a settings form from a Pydantic JSON schema (the node-type catalog's config_schema).
// Supports the shapes our node configs use: booleans, integers, strings, enums ($ref or inline,
// optionally nullable), arrays of enums (checkbox groups) and arrays of strings (comma lists).
// Fields may carry UI hints: `x-label`, `x-hint`, `x-options` (enum value -> label) and
// `x-widget` (see WIDGETS below). `template` fields get an "Insert variable" menu fed by the
// `variables` prop: [{ group, key }].

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
      // Keep the field's own UI hints (x-label, x-options, …), which sit next to `anyOf`.
      const hints = Object.fromEntries(Object.entries(resolved).filter(([key]) => key.startsWith('x-')))
      return normalize(
        { ...nonNull[0], ...hints, nullable: options.length > 1, title: resolved.title, default: resolved.default },
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

/** Synced DAG ids of every active Airflow connection (for blocks without a connection field). */
async function allDagIds() {
  const { items } = await airflowApi.listConnections()
  const lists = await Promise.allSettled(items.filter((c) => c.is_active).map((c) => airflowApi.listAllDags(c.id)))
  const ids = lists.flatMap((r) => (r.status === 'fulfilled' ? r.value.map((d) => d.dag_id) : []))
  return [...new Set(ids)].sort()
}

/**
 * DAG id: free text with suggestions from the chosen connection's synced DAGs. A block (or
 * template) with no connection field of its own suggests the DAGs of every connection.
 */
function DagInput({ id, value, onChange, disabled, connectionId, anyConnection }) {
  const listId = `${id}-dags`
  const key = connectionId ? `dags:${connectionId}` : anyConnection ? 'dags:all' : null
  const dags = useList(key, () =>
    connectionId ? airflowApi.listAllDags(connectionId).then((dags) => dags.map((d) => d.dag_id)) : allDagIds(),
  )
  return (
    <>
      <input
        id={id}
        type="text"
        list={listId}
        value={value ?? ''}
        disabled={disabled}
        placeholder={connectionId || anyConnection ? 'Type or pick a DAG id' : 'Choose a connection first'}
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

/**
 * Text with {{placeholders}}: an "Insert variable" menu adds one at the cursor. Only values
 * the trigger and earlier blocks provide are listed.
 */
function TemplateInput({ id, name, value, onChange, disabled, schema, variables = [] }) {
  const ref = useRef(null)
  const text = value ?? ''
  const groups = [...new Set(variables.map((v) => v.group))]
  function insert(key) {
    const el = ref.current
    const start = el?.selectionStart ?? text.length
    const end = el?.selectionEnd ?? text.length
    const placeholder = `{{${key}}}`
    onChange(text.slice(0, start) + placeholder + text.slice(end))
    requestAnimationFrame(() => {
      el?.focus()
      el?.setSelectionRange(start + placeholder.length, start + placeholder.length)
    })
  }
  const Input = LONG_TEXT.has(name) ? 'textarea' : 'input'
  return (
    <div className="template-input">
      <Input
        id={id}
        ref={ref}
        {...(Input === 'textarea' ? { rows: 3 } : { type: 'text' })}
        value={text}
        maxLength={schema.maxLength}
        disabled={disabled}
        onChange={(e) => onChange(schema.nullable && e.target.value === '' ? null : e.target.value)}
      />
      {!disabled && variables.length > 0 && (
        <select
          className="template-insert"
          aria-label="Insert variable"
          value=""
          onChange={(e) => e.target.value && insert(e.target.value)}
        >
          <option value="">+ Insert variable…</option>
          {groups.map((group) => (
            <optgroup key={group} label={group}>
              {variables
                .filter((v) => v.group === group)
                .map((v) => (
                  <option key={v.key} value={v.key}>
                    {v.key}
                  </option>
                ))}
            </optgroup>
          ))}
        </select>
      )}
    </div>
  )
}

const WIDGETS = {
  template: TemplateInput,
  notification_channel: ChannelSelect,
  airflow_connection: (props) => <ConnectionSelect {...props} kind="airflow" />,
  database_connection: (props) => <ConnectionSelect {...props} kind="database" />,
  dag: (props) => (
    <DagInput {...props} connectionId={props.values?.connection_id} anyConnection={props.schema['x-any-connection']} />
  ),
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

function FieldControl({ name, schema, value, values, onChange, disabled, id, variables }) {
  const Widget = WIDGETS[schema['x-widget']]
  if (Widget) {
    return (
      <Widget
        id={id}
        name={name}
        schema={schema}
        value={value}
        values={values}
        onChange={onChange}
        disabled={disabled}
        variables={variables}
      />
    )
  }
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
            {schema['x-options']?.[option] ?? option}
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
        placeholder="Any (comma separated)"
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

function SchemaField({ name, schema, value, values, onChange, disabled, required, variables }) {
  const id = useId()
  // Only hints a block defines; the input's placeholder covers "empty = any".
  const hint = schema['x-hint']
  return (
    <div className="field">
      <label htmlFor={id}>
        {schema['x-label'] ?? humanize(name)}
        {required && ' *'}
      </label>
      <FieldControl
        id={id}
        name={name}
        schema={schema}
        value={value}
        values={values}
        onChange={onChange}
        disabled={disabled}
        variables={variables}
      />
      {hint && <small className="field-hint">{hint}</small>}
    </div>
  )
}

export default function SchemaForm({ schema, value, onChange, disabled, variables }) {
  // `x-hidden` fields (kept for old workflows) only show while they hold a non-default value.
  const properties = Object.entries(schema?.properties ?? {}).filter(
    ([name, prop]) => !prop['x-hidden'] || (value?.[name] ?? null) !== (prop.default ?? null),
  )
  if (!properties.length) return <p className="muted small">This block has no settings.</p>
  const required = new Set(schema.required ?? [])
  // A DAG field with no connection field beside it suggests DAGs from every connection.
  const anyConnection = !('connection_id' in (schema.properties ?? {}))
  return (
    <div className="form-grid">
      {properties.map(([name, prop]) => {
        const field = normalize(prop, schema)
        return (
        <SchemaField
          key={name}
          name={name}
          schema={field['x-widget'] === 'dag' && anyConnection ? { ...field, 'x-any-connection': true } : field}
          value={value?.[name]}
          values={value}
          required={required.has(name)}
          disabled={disabled}
          variables={variables}
          onChange={(next) => {
            const updated = { ...value }
            if (next === undefined) delete updated[name]
            else updated[name] = next
            onChange(updated)
          }}
        />
        )
      })}
    </div>
  )
}
