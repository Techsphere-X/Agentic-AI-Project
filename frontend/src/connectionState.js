// One place that turns an Airflow connection's server-reported health into what the UI shows.
// DAGs are shown only while `live` is true: the backend sets `is_live` when Airflow answered
// within the connection monitor's window, so cached DAG lists are never presented as current.

const FAILURES = {
  UNAUTHORIZED: { label: 'Authentication failed', hint: 'Check the username and password/token on the connection.' },
  FORBIDDEN: { label: 'Access denied', hint: 'The Airflow user lacks permission to read DAGs.' },
  TLS_ERROR: { label: 'TLS error', hint: 'The Airflow certificate could not be verified.' },
  INVALID_ENDPOINT: { label: 'Invalid endpoint', hint: 'The base URL does not point at an Airflow REST API.' },
  AIRFLOW_ERROR: { label: 'Airflow error', hint: 'Airflow responded with an error.' },
}

/**
 * @param {object} conn  AirflowConnRead from the API
 * @param {{ everConnected?: boolean }} [options]  true when DAGs were synced from it before,
 *   which turns "not connected" into "connection lost"
 * @returns {{ live: boolean, tone: 'success'|'neutral'|'warning'|'danger', label: string, hint: string, message: string|null }}
 */
export function connectionState(conn, { everConnected = false } = {}) {
  const message = conn.last_health_message ?? null
  if (!conn.is_active) {
    return { live: false, tone: 'neutral', label: 'Inactive', hint: 'This connection is switched off.', message: null }
  }
  if (conn.is_live) {
    return { live: true, tone: 'success', label: 'Connected', hint: '', message: null }
  }
  if (conn.status_stale) {
    return {
      live: false,
      tone: 'warning',
      label: 'Status unknown',
      hint: 'The connection has not been checked recently. Make sure the backend is running.',
      message,
    }
  }
  const status = conn.last_health_status
  if (status === 'UNKNOWN') {
    return { live: false, tone: 'neutral', label: 'Checking connection…', hint: 'Waiting for the first health check.', message: null }
  }
  if (status === 'UNREACHABLE') {
    const lost = everConnected || conn.airflow_version != null
    return {
      live: false,
      tone: 'danger',
      label: lost ? 'Connection lost' : 'Airflow not connected',
      hint: lost
        ? 'Airflow stopped responding. Pipelines reappear automatically when it is back.'
        : 'Airflow could not be reached. Check that it is running and the base URL is correct.',
      message,
    }
  }
  // HEALTHY is always live unless stale (handled above), so everything left is a failure.
  const failure = FAILURES[status] ?? { label: 'Not connected', hint: '' }
  return { live: false, tone: 'danger', ...failure, message }
}

const DB_FAILURES = {
  UNAUTHORIZED: { label: 'Authentication failed', hint: 'Check the username and password on the connection.' },
  FORBIDDEN: { label: 'Access denied', hint: 'The user may not connect to this database.' },
  TLS_ERROR: { label: 'TLS error', hint: 'The SSL/TLS handshake failed. Check the SSL mode.' },
  INVALID_ENDPOINT: { label: 'Database not found', hint: 'The database name does not exist on this server.' },
  UNREACHABLE: { label: 'Not reachable', hint: 'The server did not answer. Check host, port and network access.' },
}

/**
 * Same shape as `connectionState`, for a database connection (DbConnRead). `live` is true when
 * the last health check succeeded.
 */
export function databaseState(conn) {
  const message = conn.last_health_message ?? null
  if (!conn.is_active) {
    return { live: false, tone: 'neutral', label: 'Inactive', hint: 'This connection is switched off.', message: null }
  }
  const status = conn.last_health_status
  if (status === 'HEALTHY') return { live: true, tone: 'success', label: 'Connected', hint: '', message: null }
  if (status === 'UNKNOWN') {
    return { live: false, tone: 'neutral', label: 'Checking connection…', hint: 'Waiting for the first health check.', message: null }
  }
  const failure = DB_FAILURES[status] ?? { label: 'Not connected', hint: '' }
  return { live: false, tone: 'danger', ...failure, message }
}

/**
 * Same shape as `connectionState`, for a notification channel (ChannelRead). Channels are not
 * polled (that would spam people): the status is the last test or real send.
 */
export function channelState(conn) {
  if (!conn.is_active) {
    return { live: false, tone: 'neutral', label: 'Inactive', hint: 'This channel is switched off.', message: null }
  }
  if (conn.last_status === 'OK') return { live: true, tone: 'success', label: 'Working', hint: '', message: conn.last_message }
  if (conn.last_status === 'FAILED') {
    return { live: true, tone: 'danger', label: 'Last send failed', hint: 'Check the channel settings and send a test.', message: conn.last_message }
  }
  return { live: true, tone: 'neutral', label: 'Not tested', hint: 'Send a test message to check it.', message: null }
}
