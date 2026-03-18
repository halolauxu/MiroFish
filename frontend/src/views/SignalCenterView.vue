<template>
  <div class="signal-center">
    <!-- Header -->
    <header class="sc-header">
      <div class="brand" @click="$router.push('/')">MIROFISH</div>
      <h1 class="page-title">信号中心</h1>
      <div class="header-actions">
        <router-link to="/" class="nav-link">返回控制台</router-link>
      </div>
    </header>

    <div class="sc-body">
      <!-- Filter Bar -->
      <section class="filter-bar">
        <div class="filter-group">
          <span class="filter-label">策略来源</span>
          <div class="chip-row">
            <button
              v-for="opt in sourceOptions"
              :key="opt.value"
              class="chip"
              :class="{ active: filters.source === opt.value }"
              @click="filters.source = opt.value"
            >{{ opt.label }}</button>
          </div>
        </div>

        <div class="filter-group">
          <span class="filter-label">方向</span>
          <div class="chip-row">
            <button
              v-for="opt in directionOptions"
              :key="opt.value"
              class="chip"
              :class="{ active: filters.direction === opt.value }"
              @click="filters.direction = opt.value"
            >{{ opt.label }}</button>
          </div>
        </div>

        <div class="filter-group">
          <span class="filter-label">最低信号强度</span>
          <div class="chip-row">
            <button
              v-for="opt in confidenceOptions"
              :key="opt.value"
              class="chip"
              :class="{ active: filters.minConfidence === opt.value }"
              @click="filters.minConfidence = opt.value"
            >{{ opt.label }}</button>
          </div>
        </div>

        <button class="btn-search" @click="fetchSignals" :disabled="loading">
          {{ loading ? '加载中...' : '查询' }}
        </button>
      </section>

      <!-- 股票筛选提示 -->
      <div v-if="stockFilter" class="stock-filter-bar">
        <span>当前筛选：<strong>{{ stockFilter }}</strong> 相关信号</span>
        <button class="btn-clear-filter" @click="stockFilter = ''; router.replace({ query: {} })">✕ 清除筛选</button>
      </div>

      <!-- Main content: table + drawer -->
      <div class="sc-main">
        <!-- Signal Table -->
        <div class="table-wrapper" :class="{ 'has-drawer': selectedSignal }">
          <div v-if="filteredSignals.length === 0 && !loading" class="empty-state">
            暂无符合条件的信号
          </div>
          <table v-else class="signal-table">
            <thead>
              <tr>
                <th>时间</th>
                <th>股票</th>
                <th>方向</th>
                <th>信号强度</th>
                <th>来源</th>
                <th>分歧度</th>
                <th>信号类型</th>
                <th>标签</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="s in filteredSignals"
                :key="s.id || s.signal_id"
                class="table-row"
                :class="{ selected: selectedSignal && getSignalId(selectedSignal) === getSignalId(s) }"
                @click="selectSignal(s)"
              >
                <td class="mono">{{ formatTime(s) }}</td>
                <td>
                  <span class="stock-name">{{ s.stock_name }}</span>
                  <span class="stock-code">{{ s.stock_code }}</span>
                </td>
                <td>
                  <span class="direction-badge" :class="directionClass(s.direction)">
                    {{ directionLabel(s.direction) }}
                  </span>
                </td>
                <td>
                  <div class="confidence-cell">
                    <span class="mono">{{ ((s.confidence ?? 0) * 100).toFixed(0) }}%</span>
                    <div class="mini-bar">
                      <div class="mini-fill" :style="{ width: ((s.confidence ?? 0) * 100) + '%' }"></div>
                    </div>
                  </div>
                </td>
                <td>
                  <span class="source-tag">{{ getSourceLabel(s) }}</span>
                </td>
                <td class="mono">{{ getDivergence(s) }}</td>
                <td>
                  <span class="alpha-tag" v-if="getAlphaType(s)">{{ getAlphaTypeDisplay(s) }}</span>
                  <span v-else class="text-muted">--</span>
                </td>
                <td>
                  <span class="strategy-label" :class="getStrategyLabelClass(s.strategy_name)">{{ getStrategyLabel(s.strategy_name) }}</span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <!-- Detail Drawer -->
        <transition name="drawer">
          <div v-if="selectedSignal" class="detail-drawer">
            <div class="drawer-header">
              <div class="drawer-title-row">
                <span class="direction-badge large" :class="directionClass(selectedSignal.direction)">
                  {{ directionLabel(selectedSignal.direction) }}
                </span>
                <div>
                  <div class="drawer-stock-name">{{ selectedSignal.stock_name }}</div>
                  <div class="drawer-stock-code">{{ selectedSignal.stock_code }}</div>
                </div>
              </div>
              <button class="drawer-close" @click="selectedSignal = null">&times;</button>
            </div>

            <div class="drawer-body">
              <!-- 信号强度 -->
              <div class="drawer-metrics">
                <div class="drawer-metric">
                  <div class="dm-label">信号强度</div>
                  <div class="dm-value mono">{{ ((selectedSignal.confidence ?? 0) * 100).toFixed(1) }}%</div>
                  <div class="confidence-bar">
                    <div class="confidence-fill" :style="{ width: ((selectedSignal.confidence ?? 0) * 100) + '%' }"></div>
                  </div>
                </div>
                <div class="drawer-metric">
                  <div class="dm-label">策略标签</div>
                  <span class="strategy-label" :class="getStrategyLabelClass(selectedSignal.strategy_name)">{{ getStrategyLabel(selectedSignal.strategy_name) }}</span>
                </div>
              </div>

              <!-- 来源策略 -->
              <div class="drawer-section">
                <div class="ds-title">来源策略</div>
                <span class="source-tag">{{ getSourceLabel(selectedSignal) }}</span>
              </div>

              <!-- 分歧度 -->
              <div class="drawer-section" v-if="getDivergenceRaw(selectedSignal) != null">
                <div class="ds-title">分歧度</div>
                <div class="divergence-visual">
                  <div class="div-bar">
                    <div
                      class="div-fill"
                      :style="{ width: (getDivergenceRaw(selectedSignal) * 100) + '%' }"
                      :class="getDivergenceRaw(selectedSignal) > 0.5 ? 'high' : 'low'"
                    ></div>
                  </div>
                  <span class="mono">{{ getDivergenceRaw(selectedSignal).toFixed(2) }}</span>
                </div>
              </div>

              <!-- 信号类型 -->
              <div class="drawer-section" v-if="getAlphaType(selectedSignal)">
                <div class="ds-title">信号类型</div>
                <span class="alpha-tag">{{ getAlphaTypeDisplay(selectedSignal) }}</span>
              </div>

              <!-- 触发事件 -->
              <div class="drawer-section" v-if="selectedSignal.event_summary || selectedSignal.metadata?.event_summary">
                <div class="ds-title">触发事件</div>
                <p class="ds-text">{{ selectedSignal.event_summary || selectedSignal.metadata?.event_summary }}</p>
              </div>

              <!-- 传播路径 -->
              <div class="drawer-section" v-if="getPath(selectedSignal)">
                <div class="ds-title">传播路径</div>
                <div class="path-chain mono">{{ getPath(selectedSignal) }}</div>
              </div>

              <!-- 未反应检测 -->
              <div class="drawer-section" v-if="(selectedSignal.reacted != null) || (selectedSignal.metadata?.reacted != null)">
                <div class="ds-title">未反应检测</div>
                <span class="react-badge" :class="(selectedSignal.reacted ?? selectedSignal.metadata?.reacted) ? 'reacted' : 'unreacted'">
                  {{ (selectedSignal.reacted ?? selectedSignal.metadata?.reacted) ? '已反应' : '未反应' }}
                </span>
              </div>

              <!-- 操作按钮 -->
              <div class="drawer-actions">
                <button class="btn-action primary" @click="goToTrace(selectedSignal)">
                  查看完整链路 →
                </button>
                <button class="btn-action secondary" @click="$router.push(`/graph?focus=${selectedSignal.stock_code}`)">
                  在图谱中查看 →
                </button>
              </div>
            </div>
          </div>
        </transition>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { getActiveSignals } from '../api/astrategy.js'

const router = useRouter()
const route = useRoute()

// URL query 参数筛选（从事件流跳转过来时带 stock= 参数）
const stockFilter = ref('')
const loading = ref(false)
const allSignals = ref([])
const selectedSignal = ref(null)

// strategy_name -> 中文标签映射
const strategyNameMap = {
  sector_rotation: '行业轮动',
  institution_association: '机构关联',
  graph_factors: '图谱因子',
  sentiment_simulation: '舆情模拟',
  analyst_divergence: '分析师分歧',
  narrative_tracker: '叙事追踪',
  prosperity_transmission: '景气传导',
  shock_propagation: '冲击链路',
}

// 过滤选项: value 对应 strategy_name
const sourceOptions = [
  { label: '全部', value: '' },
  { label: '冲击链路', value: 'shock_propagation' },
  { label: 'S07图谱因子', value: 'graph_factors' },
  { label: 'S10舆情', value: 'sentiment_simulation' },
  { label: 'S05分歧', value: 'analyst_divergence' },
]

const directionOptions = [
  { label: '全部', value: '' },
  { label: '做多', value: 'long' },
  { label: '回避', value: 'avoid' },
]

const confidenceOptions = [
  { label: '全部', value: 0 },
  { label: '>30%', value: 0.3 },
  { label: '>50%', value: 0.5 },
  { label: '>70%', value: 0.7 },
]

const filters = ref({
  source: '',
  direction: '',
  minConfidence: 0,
})

const filteredSignals = computed(() => {
  let list = allSignals.value
  // 股票代码筛选（从事件流跳转时）
  if (stockFilter.value) {
    list = list.filter(s => s.stock_code === stockFilter.value)
  }
  if (filters.value.source) {
    list = list.filter(s => s.strategy_name === filters.value.source)
  }
  if (filters.value.direction) {
    list = list.filter(s => {
      const dir = s.direction ?? ''
      if (filters.value.direction === 'avoid') {
        return dir === 'avoid' || dir === 'short'
      }
      return dir === filters.value.direction
    })
  }
  if (filters.value.minConfidence > 0) {
    list = list.filter(s => (s.confidence ?? 0) >= filters.value.minConfidence)
  }
  return list
})

// 获取信号唯一标识
function getSignalId(s) {
  return s.signal_id || s.id || ''
}

// 格式化时间: 优先 signal_date, 然后 timestamp, 最后 time/created_at
function formatTime(s) {
  const raw = s.signal_date || s.timestamp || s.time || s.created_at
  if (!raw) return '--'
  const d = new Date(raw)
  if (isNaN(d.getTime())) {
    // 可能是纯日期字符串 "2026-03-17"
    if (typeof raw === 'string' && raw.match(/^\d{4}-\d{2}-\d{2}$/)) {
      const parts = raw.split('-')
      return `${parts[1]}-${parts[2]}`
    }
    return String(raw)
  }
  return `${(d.getMonth()+1).toString().padStart(2,'0')}-${d.getDate().toString().padStart(2,'0')} ${d.getHours().toString().padStart(2,'0')}:${d.getMinutes().toString().padStart(2,'0')}`
}

// 方向 -> 中文
function directionLabel(dir) {
  if (dir === 'long') return '做多'
  if (dir === 'short' || dir === 'avoid') return '回避'
  if (dir === 'neutral') return '中性'
  return dir || '--'
}

// 方向 -> CSS 类
function directionClass(dir) {
  if (dir === 'long') return 'long'
  if (dir === 'short' || dir === 'avoid') return 'avoid'
  if (dir === 'neutral') return 'neutral'
  return ''
}

// 传播路径（安全取值）
function getPath(s) {
  const p = s.propagation_path || (s.metadata && s.metadata.propagation_path)
  if (!p) return ''
  if (typeof p === 'string') return p
  if (Array.isArray(p)) return p.join(' → ')
  return String(p)
}

// 来源标签
function getSourceLabel(s) {
  if (s.source_cn) return s.source_cn
  if (s.strategy_name && strategyNameMap[s.strategy_name]) {
    return strategyNameMap[s.strategy_name]
  }
  if (s.strategy_name) return s.strategy_name
  if (s.source) return s.source
  return '--'
}

// 分歧度
function getDivergenceRaw(s) {
  return s.divergence ?? s.metadata?.divergence ?? null
}
function getDivergence(s) {
  const val = getDivergenceRaw(s)
  return val != null ? Number(val).toFixed(2) : '--'
}

// Alpha类型
function getAlphaType(s) {
  return s.alpha_type || s.metadata?.alpha_type || ''
}

// Alpha类型展示（移除Alpha字眼）
function getAlphaTypeDisplay(s) {
  const raw = getAlphaType(s)
  if (!raw) return ''
  return raw.replace(/Alpha/gi, '').replace(/信息差\s*$/, '信息差').trim() || raw
}

// 策略标签
function getStrategyLabel(strategyName) {
  if (strategyName === 'shock_propagation' || strategyName === 'graph_factors') {
    return '⚠️实验性'
  }
  return '📊辅助参考'
}

// 策略标签样式
function getStrategyLabelClass(strategyName) {
  if (strategyName === 'shock_propagation' || strategyName === 'graph_factors') {
    return 'label-experimental'
  }
  return 'label-reference'
}

function selectSignal(s) {
  const sid = getSignalId(s)
  const currentSid = selectedSignal.value ? getSignalId(selectedSignal.value) : ''
  selectedSignal.value = sid === currentSid ? null : s
}

function goToTrace(s) {
  const sid = getSignalId(s)
  if (sid) {
    router.push(`/signals/${encodeURIComponent(sid)}`)
  }
}

async function fetchSignals() {
  loading.value = true
  try {
    const res = await getActiveSignals()
    const d = res.data?.data ?? res.data ?? res
    allSignals.value = Array.isArray(d) ? d : d.signals ?? []
  } catch (e) {
    console.error('获取信号失败:', e)
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  // 读取 URL query 参数
  if (route.query.stock) {
    stockFilter.value = route.query.stock
  }
  fetchSignals()
})

// 监听路由变化（同一页面不同 query）
watch(() => route.query, (q) => {
  stockFilter.value = q.stock || ''
})
</script>

<style scoped>
.signal-center {
  min-height: 100vh;
  background: #FAFAFA;
  font-family: 'Noto Sans SC', 'Space Grotesk', system-ui, sans-serif;
  display: flex;
  flex-direction: column;
}

/* Header */
.sc-header {
  height: 60px;
  background: #111;
  color: #FFF;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 32px;
  flex-shrink: 0;
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
}

.header-actions { display: flex; gap: 16px; }

.nav-link {
  color: #999;
  text-decoration: none;
  font-size: 13px;
  transition: color 0.2s;
}
.nav-link:hover { color: #FFF; }

/* Body */
.sc-body {
  flex: 1;
  display: flex;
  flex-direction: column;
  max-width: 1440px;
  width: 100%;
  margin: 0 auto;
  padding: 20px 32px 48px;
}

/* Filter Bar */
.filter-bar {
  display: flex;
  align-items: center;
  gap: 24px;
  flex-wrap: wrap;
  background: #FFF;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
  padding: 16px 20px;
  margin-bottom: 20px;
}

.filter-group {
  display: flex;
  align-items: center;
  gap: 8px;
}

.filter-label {
  font-size: 13px;
  color: #666;
  white-space: nowrap;
  font-weight: 500;
}

.chip-row { display: flex; gap: 4px; }

.chip {
  border: 1px solid #E5E5E5;
  background: #FFF;
  padding: 4px 14px;
  border-radius: 16px;
  font-size: 12px;
  color: #666;
  cursor: pointer;
  transition: all 0.2s;
  white-space: nowrap;
}
.chip:hover { border-color: #FF4500; color: #FF4500; }
.chip.active {
  background: #FF4500;
  border-color: #FF4500;
  color: #FFF;
}

.btn-search {
  margin-left: auto;
  background: #111;
  color: #FFF;
  border: none;
  padding: 6px 20px;
  border-radius: 4px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  transition: opacity 0.2s;
}
.btn-search:hover { opacity: 0.85; }
.btn-search:disabled { opacity: 0.5; cursor: not-allowed; }

/* Main area */
.sc-main {
  flex: 1;
  display: flex;
  gap: 0;
  position: relative;
}

.table-wrapper {
  flex: 1;
  overflow-x: auto;
  transition: margin-right 0.3s;
}
.table-wrapper.has-drawer {
  margin-right: 400px;
}

/* Table */
.signal-table {
  width: 100%;
  background: #FFF;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
  border-collapse: separate;
  border-spacing: 0;
  overflow: hidden;
}

.signal-table thead th {
  background: #F9F9F9;
  padding: 12px 14px;
  text-align: left;
  font-size: 12px;
  font-weight: 600;
  color: #666;
  border-bottom: 1px solid #E5E5E5;
  white-space: nowrap;
}

.signal-table tbody tr {
  cursor: pointer;
  transition: background 0.15s;
}
.signal-table tbody tr:hover { background: #FAFAFA; }
.signal-table tbody tr.selected { background: #FFF7ED; }

.signal-table tbody td {
  padding: 12px 14px;
  font-size: 13px;
  color: #333;
  border-bottom: 1px solid #F0F0F0;
  white-space: nowrap;
}

.stock-name { font-weight: 600; margin-right: 6px; }
.stock-code { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: #999; }
.mono { font-family: 'JetBrains Mono', monospace; }

.direction-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 44px;
  height: 24px;
  border-radius: 4px;
  font-size: 11px;
  font-weight: 700;
  padding: 0 8px;
}
.direction-badge.long { background: #DCFCE7; color: #15803D; }
.direction-badge.avoid,
.direction-badge.short { background: #FEE2E2; color: #B91C1C; }
.direction-badge.neutral { background: #F0F0F0; color: #666; }
.direction-badge.large {
  min-width: 56px;
  height: 30px;
  font-size: 13px;
}

.confidence-cell {
  display: flex;
  align-items: center;
  gap: 8px;
}

.mini-bar {
  width: 50px;
  height: 4px;
  background: #F0F0F0;
  border-radius: 2px;
  overflow: hidden;
}
.mini-fill {
  height: 100%;
  background: #FF4500;
  border-radius: 2px;
}

.positive { color: #22C55E; }
.negative { color: #EF4444; }

.source-tag {
  font-size: 11px;
  background: #F5F5F5;
  padding: 2px 8px;
  border-radius: 3px;
  color: #666;
}

.alpha-tag {
  font-size: 11px;
  background: #FFF7ED;
  color: #C2410C;
  padding: 2px 8px;
  border-radius: 3px;
  font-weight: 500;
}

.text-muted { color: #CCC; }

.strategy-label {
  display: inline-block;
  font-size: 11px;
  padding: 2px 8px;
  border-radius: 3px;
  font-weight: 500;
  white-space: nowrap;
}
.strategy-label.label-experimental {
  border: 1px solid #FF4500;
  color: #FF4500;
  background: #FFF7ED;
}
.strategy-label.label-reference {
  border: 1px solid #999;
  color: #666;
  background: #F5F5F5;
}

/* Detail Drawer */
.detail-drawer {
  position: fixed;
  right: 0;
  top: 60px;
  bottom: 0;
  width: 400px;
  background: #FFF;
  border-left: 1px solid #E5E5E5;
  box-shadow: -4px 0 24px rgba(0,0,0,0.06);
  display: flex;
  flex-direction: column;
  z-index: 50;
  overflow-y: auto;
}

.drawer-enter-active,
.drawer-leave-active {
  transition: transform 0.3s ease, opacity 0.3s ease;
}
.drawer-enter-from,
.drawer-leave-to {
  transform: translateX(100%);
  opacity: 0;
}

.drawer-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  padding: 20px;
  border-bottom: 1px solid #F0F0F0;
}

.drawer-title-row {
  display: flex;
  align-items: center;
  gap: 12px;
}

.drawer-stock-name { font-size: 18px; font-weight: 700; color: #111; }
.drawer-stock-code { font-family: 'JetBrains Mono', monospace; font-size: 12px; color: #999; margin-top: 2px; }

.drawer-close {
  background: none;
  border: none;
  font-size: 24px;
  color: #999;
  cursor: pointer;
  line-height: 1;
  padding: 0 4px;
}
.drawer-close:hover { color: #333; }

.drawer-body { padding: 20px; flex: 1; }

.drawer-metrics {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  margin-bottom: 24px;
}

.drawer-metric {}
.dm-label { font-size: 12px; color: #666; margin-bottom: 4px; }
.dm-value { font-size: 20px; font-weight: 700; color: #111; }
.dm-value.positive { color: #22C55E; }
.dm-value.negative { color: #EF4444; }

.confidence-bar {
  width: 100%;
  height: 6px;
  background: #F0F0F0;
  border-radius: 3px;
  overflow: hidden;
  margin-top: 6px;
}
.confidence-fill {
  height: 100%;
  background: #FF4500;
  border-radius: 3px;
  transition: width 0.4s;
}

/* Drawer Sections */
.drawer-section {
  margin-bottom: 20px;
}

.ds-title {
  font-size: 13px;
  font-weight: 600;
  color: #111;
  margin-bottom: 8px;
  padding-left: 10px;
  border-left: 3px solid #FF4500;
}

.ds-text {
  font-size: 13px;
  color: #555;
  line-height: 1.6;
  margin: 0;
}

.path-chain {
  font-size: 13px;
  color: #FF4500;
  background: #FFF7ED;
  padding: 10px 14px;
  border-radius: 6px;
  word-break: break-all;
}

.divergence-visual {
  display: flex;
  align-items: center;
  gap: 12px;
}

.div-bar {
  flex: 1;
  height: 8px;
  background: #F0F0F0;
  border-radius: 4px;
  overflow: hidden;
}
.div-fill {
  height: 100%;
  border-radius: 4px;
  transition: width 0.4s;
}
.div-fill.low { background: #22C55E; }
.div-fill.high { background: #EF4444; }

.react-badge {
  display: inline-block;
  padding: 4px 12px;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 600;
}
.react-badge.reacted { background: #F0F0F0; color: #666; }
.react-badge.unreacted { background: #FEF3C7; color: #92400E; }

/* Drawer Actions */
.drawer-actions {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-top: 28px;
}

.btn-action {
  width: 100%;
  padding: 10px 16px;
  border-radius: 6px;
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  border: none;
  transition: opacity 0.2s;
  text-align: center;
}
.btn-action:hover { opacity: 0.85; }

.btn-action.primary {
  background: #FF4500;
  color: #FFF;
}

.btn-action.secondary {
  background: #FFF;
  border: 1px solid #E5E5E5;
  color: #333;
}
.btn-action.secondary:hover {
  border-color: #FF4500;
  color: #FF4500;
}

/* Empty State */
.stock-filter-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 20px;
  margin: 0 24px;
  background: #FFF3E0;
  border-radius: 6px;
  font-size: 13px;
  color: #E65100;
}
.btn-clear-filter {
  background: none;
  border: 1px solid #E65100;
  color: #E65100;
  border-radius: 4px;
  padding: 4px 12px;
  cursor: pointer;
  font-size: 12px;
}
.btn-clear-filter:hover {
  background: #E65100;
  color: #FFF;
}

.empty-state {
  text-align: center;
  padding: 60px 0;
  color: #999;
  font-size: 14px;
  background: #FFF;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
}
</style>
