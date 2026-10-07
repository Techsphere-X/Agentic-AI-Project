// Smoke tests for the connection catalog and the canvas pages against the seeded e2e backend (mock Airflow, DEV connection
// "mock-dev" with partner_api_sync + orders_pipeline monitored, retry-transient turned on).
import { createServer } from 'node:http'
import { expect, test } from '@playwright/test'

const EMAIL = process.env.E2E_ADMIN_EMAIL ?? 'admin@example.com'
const PASSWORD = process.env.E2E_ADMIN_PASSWORD ?? 'e2e-admin-password-123'
const SHOTS = 'test-results/screens'

async function login(page) {
  await page.goto('/login')
  await page.getByLabel('Email').fill(EMAIL)
  await page.getByLabel('Password').fill(PASSWORD)
  await page.getByRole('button', { name: 'Sign in' }).click()
  await expect(page.getByRole('navigation').getByRole('link', { name: 'Airflow' })).toBeVisible()
}

/** Press on one element and release over another (React Flow connections). */
async function dragBetween(page, from, to) {
  const a = await from.boundingBox()
  const b = await to.boundingBox()
  await page.mouse.move(a.x + a.width / 2, a.y + a.height / 2)
  await page.mouse.down()
  await page.mouse.move(b.x + b.width / 2, b.y + b.height / 2, { steps: 15 })
  await page.mouse.up()
}

const node = (page, id) => page.locator(`.react-flow__node[data-id="${id}"]`)

/** New workflows react to incidents; switch the trigger to "Run on demand" for "Run now" flows. */
async function startByHand(page) {
  const palette = page.locator('.editor-palette')
  await palette.locator('.palette-sub-title', { hasText: 'Orchestration' }).click()
  await palette.getByRole('button', { name: 'Run on demand' }).click()
  await expect(node(page, 'trigger')).toContainText('Run on demand')
}

test.beforeEach(async ({ page }) => {
  await login(page)
})

test('workflow editor: build, save, reload, validate', async ({ page }) => {
  await page.goto('/automation/workflows/new')
  await expect(node(page, 'trigger')).toBeVisible()

  // Monitoring first: a new workflow starts from an incident, so the incident blocks are usable.
  await expect(node(page, 'trigger')).toContainText('When an incident opens or recurs')
  await expect(page.getByRole('button', { name: 'Retry failed tasks' })).toBeEnabled()

  // Drag "Tell someone" from the palette onto the canvas.
  const canvas = page.locator('.editor-canvas')
  await page.getByRole('button', { name: 'Tell someone' }).dragTo(canvas, { targetPosition: { x: 470, y: 80 } })
  await expect(node(page, 'notify_1')).toBeVisible()

  // Wire trigger.next -> notify_1.
  await dragBetween(
    page,
    page.locator('.react-flow__handle[data-nodeid="trigger"][data-handleid="next"]'),
    page.locator('.react-flow__handle.target[data-nodeid="notify_1"]'),
  )
  await expect(page.getByTestId('rf__edge-trigger:next->notify_1')).toBeAttached()

  // The same output can also lead to a second block; both run, top to bottom.
  await page.getByRole('button', { name: 'Tell someone' }).click()
  await expect(node(page, 'notify_2')).toBeVisible()
  await dragBetween(
    page,
    page.locator('.react-flow__handle[data-nodeid="trigger"][data-handleid="next"]'),
    page.locator('.react-flow__handle.target[data-nodeid="notify_2"]'),
  )
  await expect(page.getByTestId('rf__edge-trigger:next->notify_2')).toBeAttached()
  await expect(page.getByTestId('rf__edge-trigger:next->notify_1')).toBeAttached()

  // Configure the block in its settings popover.
  await node(page, 'notify_1').click()
  await page.getByLabel('Display name').fill('Tell the team')
  await expect(node(page, 'notify_1')).toContainText('Tell the team')

  await page.getByLabel('Workflow name').fill('E2E workflow')
  await page.getByRole('button', { name: 'Create workflow' }).click()
  await expect(page).toHaveURL(/\/automation\/workflows\/[0-9a-f-]{36}$/)
  await page.screenshot({ path: `${SHOTS}/workflow-editor.png` })

  // Reload: same blocks, same wiring.
  await page.reload()
  await expect(node(page, 'notify_1')).toContainText('Tell the team')
  await expect(page.getByTestId('rf__edge-trigger:next->notify_1')).toBeAttached()
  await expect(page.getByTestId('rf__edge-trigger:next->notify_2')).toBeAttached()
  await expect(page.getByLabel('Workflow name')).toHaveValue('E2E workflow')

  // Break the wiring: the server reports the orphaned block and the canvas highlights it.
  // Click the middle of the drawn edge (its bounding-box centre can sit under a block).
  const mid = await page.getByTestId('rf__edge-trigger:next->notify_1').locator('path.react-flow__edge-path').evaluate((path) => {
    const p = path.getPointAtLength(path.getTotalLength() / 2)
    const m = path.getScreenCTM()
    return { x: m.a * p.x + m.c * p.y + m.e, y: m.b * p.x + m.d * p.y + m.f }
  })
  await page.mouse.click(mid.x, mid.y)
  await expect(page.getByTestId('rf__edge-trigger:next->notify_1')).toHaveClass(/selected/)
  await page.keyboard.press('Delete')
  await expect(node(page, 'notify_1')).toBeVisible()
  await page.getByRole('button', { name: 'Validate' }).click()
  await expect(page.locator('.editor-problems')).toContainText('not connected to the trigger')
  await expect(node(page, 'notify_1').locator('.block-problem')).toBeVisible()
  await page.screenshot({ path: `${SHOTS}/workflow-editor-problem.png` })
})

test('workflow editor opens a template read from the server', async ({ page }) => {
  await page.goto('/automation/workflows')
  await page
    .locator('.workflow-card', { hasText: 'Auto-retry transient failures' })
    .getByRole('link', { name: 'Edit on canvas' })
    .click()
  await expect(node(page, 'retry')).toBeVisible()
  await expect(node(page, 'approval')).toContainText('Ask a human')
  await expect(page.locator('.react-flow__edge')).toHaveCount(10)
  await page.screenshot({ path: `${SHOTS}/workflow-template.png` })
})

test('Airflow connections: DAGs inline, old links redirect', async ({ page }) => {
  // The removed Pipelines page lands on Airflow connections.
  await page.goto('/pipelines')
  await expect(page).toHaveURL(/\/connections\/airflow$/)
  const row = page.locator('.catalog-table tbody tr', { hasText: 'mock-dev' })
  await expect(row).toContainText('Connected')

  await row.getByRole('button', { name: /DAGs/ }).click()
  await expect(page).toHaveURL(/open=/)
  const dag = page.locator('.dag-panel tbody tr', { hasText: 'partner_api_sync' })
  await expect(dag.getByLabel('Monitor partner_api_sync')).toBeChecked()
  await expect(dag.getByLabel('Alert on failures of partner_api_sync')).toBeChecked()

  await dag.getByLabel('SLA minutes for partner_api_sync').fill('45')
  await dag.getByLabel('SLA minutes for partner_api_sync').press('Enter')
  await expect(page.getByText('SLA for partner_api_sync: 45 min')).toBeVisible()

  await dag.getByLabel('Alert on failures of partner_api_sync').click({ force: true })
  await page.reload()
  const reloaded = page.locator('.dag-panel tbody tr', { hasText: 'partner_api_sync' })
  await expect(reloaded.getByLabel('Alert on failures of partner_api_sync')).not.toBeChecked()
  await expect(reloaded.getByLabel('SLA minutes for partner_api_sync')).toHaveValue('45')
  await page.screenshot({ path: `${SHOTS}/catalog.png`, fullPage: true })

  // Old monitored-DAGs deep link opens the same connection.
  const url = new URL(page.url())
  await page.goto(`/settings/dags?connection=${url.searchParams.get('open')}`)
  const restored = page.locator('.dag-panel tbody tr', { hasText: 'partner_api_sync' })
  await expect(restored).toBeVisible()

  // Put the seeded settings back: the run replay test relies on this DAG's failure alert.
  await restored.getByLabel('Alert on failures of partner_api_sync').click({ force: true })
  await expect(restored.getByLabel('Alert on failures of partner_api_sync')).toBeChecked()
  await restored.getByLabel('SLA minutes for partner_api_sync').fill('')
  await restored.getByLabel('SLA minutes for partner_api_sync').press('Enter')
  await expect(page.getByText('SLA removed for partner_api_sync')).toBeVisible()
})

test('database connections: add a database connection', async ({ page }) => {
  await page.goto('/connections/databases')
  await page.getByRole('button', { name: 'Add database' }).click()
  await page.getByLabel('Name', { exact: true }).fill('local-pg')
  await page.getByLabel('Host', { exact: true }).fill('127.0.0.1')
  await page.getByLabel('Port', { exact: true }).fill('1') // nothing listens here
  await page.getByLabel('Database', { exact: true }).fill('analytics')
  await page.getByLabel('Username', { exact: true }).fill('reader')
  await page.getByRole('button', { name: 'Test connection' }).click()
  await expect(page.locator('.test-result')).toContainText('Unreachable')
  await page.getByRole('button', { name: 'Create connection' }).click()

  const row = page.locator('.catalog-table tbody tr', { hasText: 'local-pg' })
  await expect(row).toContainText('Database')
  await expect(row).toContainText('127.0.0.1:1/analytics')
  await expect(page.locator('.catalog-table tbody tr', { hasText: 'mock-dev' })).toHaveCount(0)
  await page.screenshot({ path: `${SHOTS}/catalog-database.png`, fullPage: true })
})

test('run replay shows the path a real run took', async ({ page }) => {
  await page.goto('/incidents')
  await page.getByRole('button', { name: 'Run detection now' }).click()
  await expect(page.getByText(/Detection checked/)).toBeVisible()

  await page.goto('/automation/runs')
  await page.getByRole('link', { name: 'Auto-retry transient failures' }).first().click()
  const canvas = page.locator('.run-canvas')
  await expect(canvas.locator('.block-run-success').first()).toBeVisible()
  // trigger, classify, filter, approval (auto in DEV), retry -> all done; verify is waiting.
  await expect(canvas.locator('.block-run-success')).toHaveCount(5)
  await expect(canvas.locator('.block-run-waiting')).toHaveCount(1)
  await expect(canvas.locator('.block-dimmed').first()).toBeVisible()
  await page.screenshot({ path: `${SHOTS}/run-replay.png`, fullPage: true })
})

test('orchestration: drag a real DAG from the palette and run it now', async ({ page }) => {
  await page.goto('/automation/workflows/new')
  await startByHand(page)

  // The palette lists the actual DAGs of the live connection; the generic block is gone.
  const palette = page.locator('.editor-palette')
  const account = palette.locator('.palette-account', { hasText: 'mock-dev' })
  await expect(account.locator('.palette-account-head')).toContainText('DEV')
  await expect(account.getByRole('button', { name: 'orders_pipeline', exact: true })).toBeVisible()
  await expect(palette.getByRole('button', { name: 'Run a DAG' })).toHaveCount(0)
  const dagItem = palette.getByRole('button', { name: 'orders_pipeline', exact: true })
  await dagItem.dragTo(page.locator('.editor-canvas'), { targetPosition: { x: 470, y: 80 } })

  const block = node(page, 'orders_pipeline_1')
  await expect(block).toContainText('orders_pipeline')
  await expect(block).toContainText('Run a DAG')
  await dragBetween(
    page,
    page.locator('.react-flow__handle[data-nodeid="trigger"][data-handleid="next"]'),
    page.locator('.react-flow__handle.target[data-nodeid="orders_pipeline_1"]'),
  )
  await expect(page.getByTestId('rf__edge-trigger:next->orders_pipeline_1')).toBeAttached()

  // Incident-only blocks are greyed out under a Run-on-demand trigger.
  await expect(page.getByRole('button', { name: 'Retry failed tasks' })).toBeDisabled()

  // Connection and DAG are already filled in; the block can be switched to "wait for it".
  await block.click()
  await expect(page.getByLabel('Airflow connection')).toHaveValue(/[0-9a-f-]{36}/)
  await expect(page.getByLabel('DAG', { exact: true })).toHaveValue('orders_pipeline')
  await page.getByRole('button', { name: 'Wait for it to succeed' }).click()
  await expect(block).toContainText('Wait for a DAG')
  await expect(block.locator('.block-port-label')).toContainText(['it succeeded', 'gave up'])
  await page.getByRole('button', { name: 'Run this DAG' }).click()
  await expect(block).toContainText('Run a DAG')
  await expect(page.getByLabel('DAG', { exact: true })).toHaveValue('orders_pipeline')

  await page.getByLabel('Wait until the run finishes').click({ force: true })
  await expect(block).toContainText("orders_pipeline · don't wait")
  await page.screenshot({ path: `${SHOTS}/orchestration-palette.png` })

  await page.getByLabel('Workflow name').fill('E2E orchestration')
  await page.getByRole('button', { name: 'Create workflow' }).click()
  await expect(page).toHaveURL(/\/automation\/workflows\/[0-9a-f-]{36}$/)

  await page.getByRole('button', { name: /Run now/ }).click()
  await expect(page).toHaveURL(/\/automation\/runs\/[0-9a-f-]{36}$/)
  await expect(page.getByText('Done: run orders_pipeline')).toBeVisible()
  await expect(page.getByText('Run now', { exact: true })).toBeVisible()
  await page.screenshot({ path: `${SHOTS}/orchestration-run.png`, fullPage: true })
})

test('notification channel: test message and a workflow that sends through it', async ({ page }) => {
  // A local stand-in for Slack/Teams/n8n: records every POST the backend makes.
  const received = []
  const stub = createServer((req, res) => {
    let body = ''
    req.on('data', (chunk) => (body += chunk))
    req.on('end', () => {
      received.push(JSON.parse(body || '{}'))
      res.writeHead(200).end('ok')
    })
  })
  await new Promise((resolve) => stub.listen(0, '127.0.0.1', resolve))
  const hookUrl = `http://127.0.0.1:${stub.address().port}/hook`

  try {
    await page.goto('/connections/channels')
    await page.getByRole('button', { name: 'Add channel' }).click()
    await page.getByLabel('Name', { exact: true }).fill('E2E hook')
    await page.getByLabel('Type').selectOption('WEBHOOK')
    await page.getByLabel('Webhook link').fill(hookUrl)
    await page.getByRole('button', { name: 'Create channel' }).click()

    const row = page.locator('.catalog-table tbody tr', { hasText: 'E2E hook' })
    await expect(row).toContainText('Notification')
    await expect(row).toContainText('127.0.0.1/…') // the link itself stays secret
    await row.getByRole('button', { name: 'Test' }).click()
    await expect(page.locator('.test-result')).toContainText('Sent to E2E hook')
    await expect(row).toContainText('Working')
    expect(received.at(-1).title).toBe('Test message from Agentic Ops')

    // The channel is a palette component; dropping it gives a "Tell someone" that sends out.
    await page.goto('/automation/workflows/new')
    await startByHand(page)
    await page
      .locator('.editor-palette')
      .getByRole('button', { name: /E2E hook/ })
      .dragTo(page.locator('.editor-canvas'), { targetPosition: { x: 470, y: 80 } })
    const block = node(page, 'notify_E2E_hook_1')
    await expect(block).toContainText('E2E hook')
    await dragBetween(
      page,
      page.locator('.react-flow__handle[data-nodeid="trigger"][data-handleid="next"]'),
      page.locator('.react-flow__handle.target[data-nodeid="notify_E2E_hook_1"]'),
    )
    await block.click()
    await expect(page.getByLabel('Send via')).toHaveValue(/[0-9a-f-]{36}/)
    await page.getByLabel('Message').fill('Nightly load finished')

    await page.getByLabel('Workflow name').fill('E2E notify')
    await page.getByRole('button', { name: 'Create workflow' }).click()
    await expect(page).toHaveURL(/\/automation\/workflows\/[0-9a-f-]{36}$/)
    await page.getByRole('button', { name: /Run now/ }).click()
    await expect(page.getByText('Sent via E2E hook: Sent to E2E hook')).toBeVisible()
    await page.screenshot({ path: `${SHOTS}/notify-run.png`, fullPage: true })

    const message = received.at(-1)
    expect(message.title).toBe('E2E notify')
    expect(message.message).toBe('Nightly load finished')
    expect(message.link).toMatch(/\/automation\/runs\/[0-9a-f-]{36}$/)
  } finally {
    stub.close()
  }
})
