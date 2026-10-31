import { useEffect, useRef, useState } from 'react'
import { Badge, Button } from '../../components/ui'
import { automationApi } from '../../services/endpoints'

const CHANGE_SIGN = { added: '+', changed: '~', removed: '−' }

function ChangeSummary({ changes }) {
  const { nodes = [], edges_added: added = 0, edges_removed: removed = 0 } = changes ?? {}
  if (!nodes.length && !added && !removed) return <p className="muted small">No changes to the graph.</p>
  return (
    <ul className="assistant-changes">
      {nodes.map((c) => (
        <li key={`${c.kind}-${c.node}`} className={`change-${c.kind}`}>
          <span className="mono" aria-hidden="true">
            {CHANGE_SIGN[c.kind]}
          </span>{' '}
          {c.kind[0].toUpperCase() + c.kind.slice(1)} “{c.label}”
        </li>
      ))}
      {(added > 0 || removed > 0) && (
        <li className="muted">
          Links: {added > 0 && `+${added}`} {removed > 0 && `−${removed}`}
        </li>
      )}
    </ul>
  )
}

function ProposalNotes({ title, items }) {
  if (!items?.length) return null
  return (
    <>
      {title && <p className="muted small">{title}</p>}
      <ul className="assistant-problems small">
        {items.slice(0, 5).map((p, i) => (
          <li key={i}>{p.node ? `${p.node}: ${p.message}` : p.message}</li>
        ))}
      </ul>
    </>
  )
}

function ProposalCard({ message, onApply, onDiscard }) {
  const { proposal, valid, problems, needsInput, warnings, status } = message
  return (
    <div className="assistant-proposal">
      <div className="assistant-proposal-head">
        <strong className="small">Proposed change</strong>
        {!valid && <Badge tone="danger">still invalid</Badge>}
        {valid && needsInput?.length > 0 && <Badge tone="warning">needs your input</Badge>}
        {valid && !needsInput?.length && <Badge tone="success">valid</Badge>}
        {status && <Badge>{status}</Badge>}
      </div>
      <ChangeSummary changes={proposal.changes} />
      {!valid && <ProposalNotes items={problems} />}
      <ProposalNotes title="You still need to fill in:" items={needsInput} />
      <ProposalNotes title="Check before saving:" items={warnings} />
      {!status && (
        <div className="assistant-actions">
          <Button
            size="sm"
            variant="primary"
            disabled={!valid}
            title={valid ? 'Replace the canvas with this graph (not saved yet)' : 'The model could not make it valid'}
            onClick={onApply}
          >
            Apply
          </Button>
          <Button size="sm" onClick={onDiscard}>
            Discard
          </Button>
        </div>
      )}
    </div>
  )
}

/**
 * Copilot-style chat for the workflow editor. The assistant proposes a complete graph; Apply puts
 * it on the canvas (unsaved), so the usual Save and its checks still decide what is stored.
 * `fixRequest` changes (a counter) when the Problems panel asks for a fix.
 */
export default function AssistantPanel({
  status,
  getGraph,
  problems,
  warnings,
  fixRequest,
  onApply,
  onUndo,
  canUndo,
  onClose,
}) {
  const [messages, setMessages] = useState([])
  const [draft, setDraft] = useState('')
  const [busy, setBusy] = useState(false)
  const listRef = useRef(null)
  const latest = useRef({ messages, problems, warnings, getGraph })
  latest.current = { messages, problems, warnings, getGraph }

  useEffect(() => {
    listRef.current?.scrollTo({ top: listRef.current.scrollHeight, behavior: 'smooth' })
  }, [messages, busy])

  async function send(mode, text) {
    const { messages: history, problems: current, warnings: warned, getGraph: graphOf } = latest.current
    const userMessage = { role: 'user', content: text }
    const conversation = [...history, userMessage]
    setMessages(conversation)
    setDraft('')
    setBusy(true)
    try {
      const result = await automationApi.askAssistant({
        mode,
        messages: conversation.filter((m) => !m.error).map(({ role, content }) => ({ role, content })),
        graph: graphOf(),
        problems: mode === 'fix' ? current : [],
        warnings: mode === 'fix' ? warned : [],
      })
      setMessages((list) => [
        ...list,
        {
          role: 'assistant',
          content: result.reply,
          proposal: result.proposal,
          valid: result.valid,
          problems: result.problems,
          needsInput: result.needs_input,
          warnings: result.warnings,
        },
      ])
    } catch (err) {
      setMessages((list) => [...list, { role: 'assistant', content: err.message, error: true }])
    } finally {
      setBusy(false)
    }
  }

  // Ask for a fix whenever the Problems panel's button is pressed.
  useEffect(() => {
    if (fixRequest) send('fix', 'Explain the problems in this workflow and fix them.')
    // eslint-disable-next-line react-hooks/exhaustive-deps -- only a new request should send
  }, [fixRequest])

  function mark(index, value) {
    setMessages((list) => list.map((m, i) => (i === index ? { ...m, status: value } : m)))
  }

  function submit(event) {
    event.preventDefault()
    const text = draft.trim()
    if (text && !busy) send('chat', text)
  }

  return (
    <aside className="editor-panel assistant-panel" aria-label="Workflow assistant">
      <div className="assistant-head">
        <strong>✨ Assistant</strong>
        <span className="muted small" title={`Provider: ${status.provider}`}>
          {status.model}
        </span>
        <span className="spacer" />
        {canUndo && (
          <Button size="sm" onClick={onUndo} title="Put back the canvas from before the last Apply">
            Undo apply
          </Button>
        )}
        <button type="button" className="assistant-close" aria-label="Close assistant" onClick={onClose}>
          ×
        </button>
      </div>

      <div className="assistant-messages" ref={listRef} aria-live="polite">
        {messages.map((m, i) => (
          <div key={i} className={`assistant-msg assistant-${m.role} ${m.error ? 'assistant-error' : ''}`}>
            <div className="assistant-text">{m.content}</div>
            {m.proposal && (
              <ProposalCard
                message={m}
                onApply={() => {
                  onApply(m.proposal.graph)
                  mark(i, 'applied')
                }}
                onDiscard={() => mark(i, 'discarded')}
              />
            )}
          </div>
        ))}
        {busy && <div className="assistant-msg assistant-assistant muted small">Thinking…</div>}
      </div>

      <form className="assistant-input" onSubmit={submit}>
        <textarea
          rows={3}
          value={draft}
          placeholder="Describe the workflow…"
          aria-label="Message the assistant"
          maxLength={4000}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && !e.shiftKey) submit(e)
          }}
        />
        <Button type="submit" variant="primary" size="sm" loading={busy} disabled={!draft.trim()}>
          Send
        </Button>
      </form>
    </aside>
  )
}
