<template>
  <div class="dashboard">
    <!-- Header -->
    <header class="dash-header">
      <div class="brand" @click="$router.push('/')">MIROFISH</div>
      <h1 class="page-title">AStrategy 控制台</h1>
      <div class="header-actions">
        <span class="update-time" v-if="lastUpdate">
          更新于 {{ lastUpdate }}
        </span>
        <button class="btn-refresh" @click="refreshAll" :disabled="loading">
          {{ loading ? '刷新中...' : '刷新' }}
        </button>
      </div>
    </header>

    <!-- 免责声明 -->
    <div class="disclaimer-bar">
      ⚠️ 本系统为量化研究平台，所有信号均为实验性研究结果，不构成投资建议
    </div>

    <main class="dash-content">
      <!-- 1. 组合概览 -->
      <section class="section">
        <h2 class="section-title">组合概览</h2>
        <div class="metric-cards">
          <div class="metric-card">
            <div class="metric-label">总资产</div>
            <div class="metric-value money">
              ¥{{ formatMoney(portfolio.totalAssets) }}
            </div>
          </div>
          <div class="metric-card">
            <div class="metric-label">持仓数 / 最大仓位</div>
            <div class="metric-value">
              <span class="highlight">{{ portfolio.positionCount }}</span>
              <span class="separator">/</span>
              <span>{{ portfolio.maxPosition }}</span>
            </div>
          </div>
          <div class="metric-card">
            <div class="metric-label">今日收益</div>
            <div
              class="metric-value"
              :class="portfolio.todayReturn >= 0 ? 'positive' : 'negative'"
            >
              {{ portfolio.todayReturn >= 0 ? '+' : '' }}{{ formatPercent(portfolio.todayReturn) }}
            </div>
          </div>
          <div class="metric-card">
            <div class="metric-label">组合Sharpe</div>
            <div class="metric-value mono">
              {{ portfolio.sharpe != null ? portfolio.sharpe.toFixed(2) : '--' }}
            </div>
          </div>
        </div>
      </section>

      <!-- 2. 今日活跃信号 -->
      <section class="section">
        <div class="section-header">
          <h2 class="section-title">今日研究信号</h2>
          <router-link to="/signals" class="link-all">全部信号 →</router-link>
        </div>
        <div v-if="signals.length === 0 && !loading" class="empty-state">
          暂无活跃信号
        </div>
        <div v-else class="signal-list">
          <div
            v-for="s in signals"
            :key="s.signal_id || s.id"
            class="signal-row"
            @click="$router.push(`/signals/${s.signal_id || s.id}`)"
          >
            <span class="direction-badge" :class="s.direction">
              {{ s.direction === 'long' ? '做多' : '回避' }}
            </span>
            <div class="signal-info">
              <div class="signal-main">
                <span class="stock-name">{{ s.stock_name }}</span>
                <span class="stock-code">{{ s.stock_code }}</span>
                <span class="signal-source">{{ s.source_cn || strategyCN(s.strategy_name || s.source) }}</span>
              </div>
              <div class="signal-sub" v-if="s.reasoning">
                {{ s.reasoning }}
              </div>
            </div>
            <div class="signal-metrics">
              <div class="confidence-bar-wrap">
                <div class="confidence-label">{{ (s.confidence * 100).toFixed(0) }}%</div>
                <div class="confidence-bar">
                  <div class="confidence-fill" :style="{ width: (s.confidence * 100) + '%' }"></div>
                </div>
              </div>
              <!-- 预期收益已移除，不展示 -->
            </div>
          </div>
        </div>
      </section>

      <!-- 调仓建议区块已移除 -->

      <!-- 4. 系统状态 -->
      <section class="section">
        <h2 class="section-title">系统状态</h2>
        <div class="status-grid">
          <div class="status-item">
            <div class="status-label">事件扫描</div>
            <div class="status-row">
              <span class="status-dot" :class="systemStatus.eventScan?.ok ? 'ok' : 'warn'"></span>
              <span class="status-text">{{ systemStatus.eventScan?.status ?? '未知' }}</span>
            </div>
            <div class="status-time">{{ systemStatus.eventScan?.time ?? '--' }}</div>
          </div>
          <div class="status-item">
            <div class="status-label">日间策略</div>
            <div class="status-row">
              <span class="status-dot" :class="systemStatus.dayStrategy?.ok ? 'ok' : 'warn'"></span>
              <span class="status-text">{{ systemStatus.dayStrategy?.status ?? '未知' }}</span>
            </div>
            <div class="status-time">{{ systemStatus.dayStrategy?.time ?? '--' }}</div>
          </div>
          <div class="status-item">
            <div class="status-label">图谱节点数</div>
            <div class="status-value mono">{{ systemStatus.graphNodes ?? '--' }}</div>
          </div>
          <div class="status-item">
            <div class="status-label">活跃信号数</div>
            <div class="status-value mono">{{ systemStatus.activeSignalCount ?? '--' }}</div>
          </div>
        </div>
      </section>
    </main>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import {
  getActiveSignals,
  getPortfolioSummary,
  getSystemStatus,
  getGraphStats,
} from '../api/astrategy.js'

const loading = ref(false)
const lastUpdate = ref('')

const portfolio = ref({
  totalAssets: 0,
  positionCount: 0,
  maxPosition: 0,
  todayReturn: 0,
  sharpe: null,
})

const _STRATEGY_CN = {
  sector_rotation: '行业轮动', institution_association: '机构关联',
  graph_factors: '图谱因子', sentiment_simulation: '舆情模拟',
  analyst_divergence: '分析师分歧', narrative_tracker: '叙事追踪',
  prosperity_transmission: '景气传导', shock_propagation: '冲击链路',
}
const strategyCN = (name) => _STRATEGY_CN[name] || name || '--'

const signals = ref([])
const adjustments = ref([])

const systemStatus = ref({
  eventScan: null,
  dayStrategy: null,
  graphNodes: null,
  activeSignalCount: null,
})

function formatMoney(v) {
  if (v == null) return '--'
  return Number(v).toLocaleString('zh-CN', {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })
}

function formatPercent(v) {
  if (v == null) return '--'
  return (v * 100).toFixed(2) + '%'
}

function nowStr() {
  return new Date().toLocaleTimeString('zh-CN', { hour12: false })
}

async function refreshAll() {
  loading.value = true
  try {
    const [sigRes, portRes, sysRes, graphRes] = await Promise.allSettled([
      getActiveSignals(),
      getPortfolioSummary(),
      getSystemStatus(),
      getGraphStats(),
    ])

    if (sigRes.status === 'fulfilled') {
      const d = sigRes.value.data ?? sigRes.value
      const list = Array.isArray(d) ? d : d.signals ?? []
      signals.value = list.sort((a, b) => (b.confidence ?? 0) - (a.confidence ?? 0))
    }

    if (portRes.status === 'fulfilled') {
      const d = portRes.value.data ?? portRes.value
      portfolio.value = {
        totalAssets: d.total_assets ?? d.totalAssets ?? 0,
        positionCount: d.position_count ?? d.positionCount ?? 0,
        maxPosition: d.max_position ?? d.maxPosition ?? 0,
        todayReturn: d.today_return ?? d.todayReturn ?? 0,
        sharpe: d.sharpe ?? null,
      }
      adjustments.value = d.adjustments ?? d.recommendations ?? []
    }

    if (sysRes.status === 'fulfilled') {
      const d = sysRes.value.data ?? sysRes.value
      systemStatus.value.eventScan = {
        ok: d.event_scan?.ok ?? d.eventScan?.ok ?? true,
        status: d.event_scan?.status ?? d.eventScan?.status ?? '正常',
        time: d.event_scan?.time ?? d.eventScan?.time ?? null,
      }
      systemStatus.value.dayStrategy = {
        ok: d.day_strategy?.ok ?? d.dayStrategy?.ok ?? true,
        status: d.day_strategy?.status ?? d.dayStrategy?.status ?? '正常',
        time: d.day_strategy?.time ?? d.dayStrategy?.time ?? null,
      }
      systemStatus.value.activeSignalCount = d.signal_count ?? d.signalCount ?? 0
    }

    if (graphRes.status === 'fulfilled') {
      const d = graphRes.value.data ?? graphRes.value
      systemStatus.value.graphNodes = d.node_count ?? d.nodeCount ?? d.nodes ?? 0
    }

    lastUpdate.value = nowStr()
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  refreshAll()
})
</script>

<style scoped>
.dashboard {
  min-height: 100vh;
  background: #FAFAFA;
  font-family: 'Noto Sans SC', 'Space Grotesk', system-ui, sans-serif;
}

/* Header */
.dash-header {
  height: 60px;
  background: #111;
  color: #FFF;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 32px;
}

.brand {
  font-family: 'JetBrains Mono', monospace;
  font-weight: 800;
  font-size: 18px;
  letter-spacing: 1px;
  cursor: pointer;
}

.page-title {
  font-size: 15px;
  font-weight: 600;
  letter-spacing: 0.5px;
}

.header-actions {
  display: flex;
  align-items: center;
  gap: 16px;
}

.update-time {
  font-size: 12px;
  color: #999;
  font-family: 'JetBrains Mono', monospace;
}

.btn-refresh {
  background: #FF4500;
  color: #FFF;
  border: none;
  padding: 6px 18px;
  border-radius: 4px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  transition: opacity 0.2s;
}
.btn-refresh:hover { opacity: 0.85; }
.btn-refresh:disabled { opacity: 0.5; cursor: not-allowed; }

/* Disclaimer */
.disclaimer-bar {
  background: #FFFBEB;
  color: #92400E;
  font-size: 13px;
  text-align: center;
  padding: 8px 16px;
  border-bottom: 1px solid #FDE68A;
}

/* Content */
.dash-content {
  max-width: 1280px;
  margin: 0 auto;
  padding: 24px 32px 48px;
}

/* Section */
.section { margin-bottom: 32px; }

.section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 16px;
}

.section-title {
  font-size: 16px;
  font-weight: 600;
  color: #111;
  margin-bottom: 16px;
  padding-left: 12px;
  border-left: 4px solid #FF4500;
  line-height: 1;
}

.section-header .section-title { margin-bottom: 0; }

.link-all {
  font-size: 13px;
  color: #FF4500;
  text-decoration: none;
  font-weight: 500;
}
.link-all:hover { text-decoration: underline; }

/* Metric Cards */
.metric-cards {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
}

.metric-card {
  background: #FFF;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
  padding: 20px 16px;
  transition: box-shadow 0.2s;
}
.metric-card:hover { box-shadow: 0 2px 12px rgba(0,0,0,0.06); }

.metric-label {
  font-size: 13px;
  color: #666;
  margin-bottom: 8px;
}

.metric-value {
  font-size: 24px;
  font-weight: 700;
  color: #111;
}
.metric-value.money,
.metric-value.mono {
  font-family: 'JetBrains Mono', monospace;
}
.metric-value .highlight {
  color: #FF4500;
  font-family: 'JetBrains Mono', monospace;
}
.metric-value .separator {
  color: #CCC;
  margin: 0 4px;
  font-weight: 400;
}
.metric-value.positive { color: #22C55E; }
.metric-value.negative { color: #EF4444; }

/* Signal List */
.signal-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.signal-row {
  display: flex;
  align-items: center;
  gap: 16px;
  background: #FFF;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
  padding: 14px 16px;
  cursor: pointer;
  transition: box-shadow 0.2s, border-color 0.2s;
}
.signal-row:hover {
  border-color: #FF4500;
  box-shadow: 0 2px 8px rgba(255,69,0,0.08);
}

.direction-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 52px;
  height: 28px;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 700;
  padding: 0 8px;
  flex-shrink: 0;
}
.direction-badge.long { background: #DCFCE7; color: #15803D; }
.direction-badge.avoid,
.direction-badge.short { background: #FEE2E2; color: #B91C1C; }

.signal-info { flex: 1; min-width: 0; }

.signal-main {
  display: flex;
  align-items: center;
  gap: 8px;
}

.stock-name { font-weight: 600; font-size: 14px; color: #111; }
.stock-code { font-family: 'JetBrains Mono', monospace; font-size: 12px; color: #999; }
.signal-source {
  font-size: 11px;
  color: #666;
  background: #F5F5F5;
  padding: 2px 8px;
  border-radius: 3px;
}

.signal-sub {
  font-size: 12px;
  color: #999;
  margin-top: 4px;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.signal-metrics {
  display: flex;
  align-items: center;
  gap: 20px;
  flex-shrink: 0;
}

.confidence-bar-wrap {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 4px;
  width: 100px;
}

.confidence-label {
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px;
  font-weight: 600;
  color: #111;
}

.confidence-bar {
  width: 100%;
  height: 6px;
  background: #F0F0F0;
  border-radius: 3px;
  overflow: hidden;
}

.confidence-fill {
  height: 100%;
  background: #FF4500;
  border-radius: 3px;
  transition: width 0.4s ease;
}

.expected-return {
  font-family: 'JetBrains Mono', monospace;
  font-size: 14px;
  font-weight: 600;
  width: 70px;
  text-align: right;
}
.expected-return.positive { color: #22C55E; }
.expected-return.negative { color: #EF4444; }

/* Adjustments */
.adjustment-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.adjustment-row {
  display: flex;
  align-items: center;
  gap: 12px;
  background: #FFF;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
  padding: 12px 16px;
}

.action-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 48px;
  height: 26px;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 700;
  padding: 0 8px;
  flex-shrink: 0;
}
.action-badge.buy { background: #DCFCE7; color: #15803D; }
.action-badge.sell { background: #FEE2E2; color: #B91C1C; }
.action-badge.reduce { background: #FEF3C7; color: #92400E; }

.adj-stock { font-weight: 600; font-size: 14px; color: #111; min-width: 100px; }
.adj-weight { font-size: 13px; color: #666; min-width: 80px; }
.adj-reason { font-size: 13px; color: #999; flex: 1; }

/* System Status */
.status-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 12px;
}

.status-item {
  background: #FFF;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
  padding: 16px;
}

.status-label { font-size: 13px; color: #666; margin-bottom: 8px; }

.status-row { display: flex; align-items: center; gap: 8px; }

.status-dot {
  width: 8px; height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
}
.status-dot.ok { background: #22C55E; }
.status-dot.warn { background: #F59E0B; }

.status-text { font-size: 14px; font-weight: 600; color: #111; }

.status-time {
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px;
  color: #999;
  margin-top: 4px;
}

.status-value {
  font-size: 28px;
  font-weight: 700;
  color: #111;
}
.status-value.mono { font-family: 'JetBrains Mono', monospace; }

/* Empty State */
.empty-state {
  text-align: center;
  padding: 40px 0;
  color: #999;
  font-size: 14px;
  background: #FFF;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
}

.mono { font-family: 'JetBrains Mono', monospace; }
</style>
