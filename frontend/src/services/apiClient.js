/**
 * Minimal fetch wrapper.
 * - Access token lives in memory only (never localStorage).
 * - Refresh token is an httpOnly cookie handled by the browser.
 * - On 401, performs ONE refresh (shared across concurrent callers) and retries once.
 */

const API_BASE = '/api/v1'
const REQUEST_TIMEOUT_MS = 30000
const CONNECTION_FAILED = "Connection failed – can't reach the server"

let accessToken = null
let refreshPromise = null
let onAuthLost = () => {}

export class ApiError extends Error {
  constructor(status, code, message, details = {}) {
    super(message)
    this.status = status
    this.code = code
    this.details = details
  }
}

export function setAccessToken(token) {
  accessToken = token
}

export function setAuthLostHandler(handler) {
  onAuthLost = handler
}

async function parseBody(res) {
  if (res.status === 204) return null
  const text = await res.text()
  if (!text) return null
  try {
    return JSON.parse(text)
  } catch {
    return { raw: text }
  }
}

function toApiError(res, body) {
  const err = body?.error
  if (!err) {
    // Gateways answer 502/503/504 when the backend is down; the Vite dev proxy answers an
    // empty-body 500. Anything else (e.g. a plain-text 500 crash) is a real server error.
    const gateway = [502, 503, 504].includes(res.status)
    const emptyProxy500 = res.status === 500 && (body === null || body === undefined)
    if (gateway || emptyProxy500) return new ApiError(res.status, 'network_error', CONNECTION_FAILED)
    if (res.status >= 500) return new ApiError(res.status, `http_${res.status}`, `Server error (HTTP ${res.status})${res.statusText ? `: ${res.statusText}` : ''}`)
    return new ApiError(res.status, `http_${res.status}`, res.statusText || 'Request failed')
  }
  let message = err.message
  const first = err.details?.errors?.[0]
  if (err.code === 'validation_error' && first) {
    const field = first.loc?.filter((p) => p !== 'body').join('.')
    message = field ? `${field}: ${first.msg}` : first.msg
  }
  return new ApiError(res.status, err.code, message, err.details)
}

/** Exchange the refresh cookie for a new access token. Concurrent callers share one request. */
export function refreshSession() {
  if (!refreshPromise) {
    refreshPromise = fetch(`${API_BASE}/auth/refresh`, { method: 'POST', credentials: 'same-origin' })
      .then(async (res) => {
        const body = await parseBody(res)
        if (!res.ok) throw toApiError(res, body)
        accessToken = body.access_token
        return body
      })
      .finally(() => {
        refreshPromise = null
      })
  }
  return refreshPromise
}

export async function request(path, { method = 'GET', body, query, retry = true } = {}) {
  const url = new URL(`${API_BASE}${path}`, window.location.origin)
  Object.entries(query ?? {}).forEach(([k, v]) => {
    const values = Array.isArray(v) ? v : [v] // arrays become repeated params (?a=1&a=2)
    values.forEach((item) => {
      if (item !== undefined && item !== null && item !== '') url.searchParams.append(k, item)
    })
  })

  const headers = {}
  if (body !== undefined) headers['Content-Type'] = 'application/json'
  if (accessToken) headers.Authorization = `Bearer ${accessToken}`

  // Only reads time out: aborting a mutation could leave it half-done on the server.
  const controller = new AbortController()
  const timer = method === 'GET' ? setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS) : null
  let res
  try {
    res = await fetch(url, {
      method,
      headers,
      credentials: 'same-origin',
      signal: controller.signal,
      body: body === undefined ? undefined : JSON.stringify(body),
    })
  } catch (err) {
    // Network down / backend off: fetch rejects with a raw TypeError; timeouts are AbortError.
    if (err?.name === 'AbortError') {
      throw new ApiError(0, 'timeout', 'Request timed out – the server is taking too long')
    }
    throw new ApiError(0, 'network_error', CONNECTION_FAILED)
  } finally {
    if (timer) clearTimeout(timer)
  }

  if (res.status === 401 && retry && !path.startsWith('/auth/')) {
    try {
      await refreshSession()
    } catch {
      accessToken = null
      onAuthLost()
      throw new ApiError(401, 'unauthorized', 'Your session has expired. Please sign in again.')
    }
    return request(path, { method, body, query, retry: false })
  }

  const data = await parseBody(res)
  if (!res.ok) throw toApiError(res, data)
  return data
}

export const api = {
  get: (path, query) => request(path, { query }),
  post: (path, body) => request(path, { method: 'POST', body }),
  patch: (path, body) => request(path, { method: 'PATCH', body }),
  put: (path, body) => request(path, { method: 'PUT', body }),
  delete: (path) => request(path, { method: 'DELETE' }),
}
