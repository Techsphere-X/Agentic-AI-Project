import { api, request, setAccessToken } from './apiClient'

export const authApi = {
  async login(email, password) {
    const body = await request('/auth/login', { method: 'POST', body: { email, password } })
    setAccessToken(body.access_token)
    return body
  },
  async logout() {
    try {
      await request('/auth/logout', { method: 'POST' })
    } finally {
      setAccessToken(null)
    }
  },
  me: () => api.get('/auth/me'),
}

export const healthApi = {
  status: () => api.get('/health'),
}

export const usersApi = {
  list: (query) => api.get('/users', query),
  create: (data) => api.post('/users', data),
  update: (id, data) => api.patch(`/users/${id}`, data),
}

export const airflowApi = {
  listConnections: () => api.get('/airflow/connections', { limit: 200 }),
  getConnection: (id) => api.get(`/airflow/connections/${id}`),
  createConnection: (data) => api.post('/airflow/connections', data),
  updateConnection: (id, data) => api.patch(`/airflow/connections/${id}`, data),
  deleteConnection: (id) => api.delete(`/airflow/connections/${id}`),
  testUnsaved: (data) => api.post('/airflow/test-connection', data),
  testSaved: (id) => api.post(`/airflow/connections/${id}/test`),
  refreshConnection: (id) => api.post(`/airflow/connections/${id}/refresh`),
  syncDags: (id) => api.post(`/airflow/connections/${id}/sync-dags`),
  listDags: (id, query) => api.get(`/airflow/connections/${id}/dags`, query),
  /** Every DAG of a connection (the API pages at most 200 at a time). */
  async listAllDags(id, query = {}) {
    const items = []
    for (;;) {
      const page = await api.get(`/airflow/connections/${id}/dags`, { ...query, limit: 200, offset: items.length })
      items.push(...page.items)
      if (!page.items.length || items.length >= page.total) return items
    }
  },
  setMonitored: (dagPk, isMonitored) =>
    api.patch(`/airflow/dags/${dagPk}`, { is_monitored: isMonitored }),
  setSla: (dagPk, slaMinutes) => api.patch(`/airflow/dags/${dagPk}`, { sla_minutes: slaMinutes }),
  updateDag: (dagPk, data) => api.patch(`/airflow/dags/${dagPk}`, data),
}

export const databaseApi = {
  listConnections: () => api.get('/databases/connections', { limit: 200 }),
  createConnection: (data) => api.post('/databases/connections', data),
  updateConnection: (id, data) => api.patch(`/databases/connections/${id}`, data),
  deleteConnection: (id) => api.delete(`/databases/connections/${id}`),
  testUnsaved: (data) => api.post('/databases/test-connection', data),
  testSaved: (id) => api.post(`/databases/connections/${id}/test`),
}

export const channelsApi = {
  listConnections: () => api.get('/channels').then((items) => ({ items })),
  createConnection: (data) => api.post('/channels', data),
  updateConnection: (id, data) => api.patch(`/channels/${id}`, data),
  deleteConnection: (id) => api.delete(`/channels/${id}`),
  testSaved: (id, to = []) => api.post(`/channels/${id}/test`, { to }),
}

export const incidentsApi = {
  list: (query) => api.get('/incidents', query),
  summary: () => api.get('/incidents/summary'),
  get: (id) => api.get(`/incidents/${id}`),
  openCounts: () => api.get('/incidents/open-counts'),
  acknowledge: (id) => api.post(`/incidents/${id}/acknowledge`),
  resolve: (id, note) => api.post(`/incidents/${id}/resolve`, { note }),
  reopen: (id) => api.post(`/incidents/${id}/reopen`),
  setDiagnosis: (id, category, note) => api.put(`/incidents/${id}/diagnosis`, { category, note }),
}

export const diagnosisApi = {
  classifyLog: (log) => api.post('/diagnosis/classify', { log }),
}

export const detectionApi = {
  run: () => api.post('/detection/run'),
  status: () => api.get('/detection/status'),
}

export const automationApi = {
  summary: () => api.get('/automation/summary'),
  nodeTypes: () => api.get('/automation/node-types'),
  listTemplates: () => api.get('/automation/templates'),
  previewTemplate: (key, template_parameters) =>
    api.post(`/automation/templates/${key}/preview`, { template_parameters }),
  listWorkflows: () => api.get('/automation/workflows'),
  getWorkflow: (id) => api.get(`/automation/workflows/${id}`),
  createWorkflow: (data) => api.post('/automation/workflows', data),
  runWorkflow: (id, data = {}) => api.post(`/automation/workflows/${id}/run`, data),
  updateWorkflow: (id, data) => api.patch(`/automation/workflows/${id}`, data),
  deleteWorkflow: (id) => api.delete(`/automation/workflows/${id}`),
  validateGraph: (graph) => api.post('/automation/workflows/validate', { graph }),
  listRuns: (query) => api.get('/automation/runs', query),
  listRunChecks: (query) => api.get('/automation/run-checks', query),
  latestRunChecks: () => api.get('/automation/run-checks/latest'),
  getRun: (id) => api.get(`/automation/runs/${id}`),
  cancelRun: (id) => api.post(`/automation/runs/${id}/cancel`),
  tick: () => api.post('/automation/tick'),
  listApprovals: (query) => api.get('/automation/approvals', query),
  approve: (id, comment) => api.post(`/automation/approvals/${id}/approve`, { comment }),
  reject: (id, comment) => api.post(`/automation/approvals/${id}/reject`, { comment }),
  listNotifications: (query) => api.get('/automation/notifications', query),
  readAllNotifications: () => api.post('/automation/notifications/read-all'),
}
