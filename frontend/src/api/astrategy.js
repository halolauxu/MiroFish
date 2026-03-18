import service from './index.js'

const BASE = '/api/astrategy'

// ── 信号 ──────────────────────────────────────────
export const getActiveSignals = () =>
  service.get(`${BASE}/signals/active`)

export const getSignals = (params = {}) =>
  service.get(`${BASE}/signals`, { params })

export const getSignalTrace = (signalId) =>
  service.get(`${BASE}/signals/${signalId}/trace`)

// ── 事件 ──────────────────────────────────────────
export const getEventsHistory = () =>
  service.get(`${BASE}/events/history`)

export const getEventsLive = () =>
  service.get(`${BASE}/events/live`)

// ── 图谱 ──────────────────────────────────────────
export const getGraphData = (params = {}) =>
  service.get(`${BASE}/graph/data`, { params })

export const getGraphNodeNeighbors = (nodeId) =>
  service.get(`${BASE}/graph/node/${nodeId}/neighbors`)

export const getGraphPath = (fromId, toId) =>
  service.get(`${BASE}/graph/path`, { params: { from: fromId, to: toId } })

export const getGraphStats = () =>
  service.get(`${BASE}/graph/stats`)

// ── 持仓 ──────────────────────────────────────────
export const getPortfolioSummary = () =>
  service.get(`${BASE}/portfolio/summary`)

export const getPortfolioPositions = () =>
  service.get(`${BASE}/portfolio/positions`)

export const getPortfolioHistory = () =>
  service.get(`${BASE}/portfolio/history`)

// ── 回测 ──────────────────────────────────────────
export const getBacktestSummary = () =>
  service.get(`${BASE}/backtest/summary`)

export const getBacktestStrategies = () =>
  service.get(`${BASE}/backtest/strategies`)

export const getBacktestNavCurve = () =>
  service.get(`${BASE}/backtest/nav-curve`)

// ── 系统 ──────────────────────────────────────────
export const getSystemStatus = () =>
  service.get(`${BASE}/system/status`)

export const triggerScheduledTask = (taskName) =>
  service.post(`${BASE}/system/trigger`, { task: taskName })

export const expandGraphEdges = (type) =>
  service.post(`${BASE}/graph/expand`, { type })

export const testLLMConnection = (config) =>
  service.post(`${BASE}/system/test-llm`, config)
