<template>
  <div class="event-stream">
    <!-- 数据源说明 -->
    <div class="data-source-bar" v-if="dataSource">
      <div class="data-source-item" v-if="dataSource.source">
        <span class="ds-label">数据来源</span>
        <span class="ds-value">{{ dataSource.source }}</span>
      </div>
      <div class="data-source-item" v-if="dataSource.frequency">
        <span class="ds-label">更新频率</span>
        <span class="ds-value">{{ dataSource.frequency }}</span>
      </div>
      <div class="data-source-item" v-if="dataSource.coverage">
        <span class="ds-label">覆盖范围</span>
        <span class="ds-value">{{ dataSource.coverage }}</span>
      </div>
    </div>

    <!-- 顶部标签栏 -->
    <div class="stream-header">
      <div class="tab-bar">
        <button
          class="tab-btn"
          :class="{ active: activeTab === 'live' }"
          @click="activeTab = 'live'"
        >
          <span class="tab-dot live-dot"></span>
          实时事件
        </button>
        <button
          class="tab-btn"
          :class="{ active: activeTab === 'history' }"
          @click="activeTab = 'history'"
        >
          历史事件库
        </button>
      </div>
    </div>

    <!-- 实时事件 -->
    <div v-if="activeTab === 'live'" class="tab-content">
      <div v-if="liveLoading" class="loading-state">
        <div class="loading-spinner"></div>
        <span>加载中...</span>
      </div>

      <div v-else-if="liveEvents.length === 0" class="empty-state">
        <div class="empty-icon">&#x26A1;</div>
        <p>暂无实时事件</p>
      </div>

      <div v-else class="timeline">
        <div
          v-for="(event, idx) in liveEvents"
          :key="event.id"
          class="timeline-item"
        >
          <div class="timeline-line"></div>
          <div class="timeline-dot" :style="{ background: getEventColor(event.event_type || event.type) }"></div>
          <div
            class="event-card"
            :class="{ expanded: expandedLive.has(event.event_id || event.id || `live_${idx}`) }"
            @click="toggleLiveExpand(event.event_id || event.id || `live_${idx}`)"
          >
            <div class="event-card-header">
              <div class="event-meta">
                <span class="event-date-prominent">{{ formatDate(event.event_date || event.date) }}</span>
                <span class="event-badge" :style="{ background: getEventColor(event.event_type || event.type) }">
                  {{ getEventTypeLabel(event.event_type || event.type) }}
                </span>
                <span class="event-time-ago">{{ formatTime(event.time || event.event_date || event.date) }}</span>
              </div>
              <div class="event-source" v-if="event.stock_name || event.stock_code">
                <span class="source-name">{{ event.stock_name }}</span>
                <span class="source-code" v-if="event.stock_code">({{ event.stock_code }})</span>
              </div>
              <div class="event-title">{{ event.title || event.summary }}</div>
              <div class="event-affected" v-if="event.affected_stocks && event.affected_stocks.length">
                <span class="affected-label">影响:</span>
                <span class="affected-list">
                  {{ formatAffectedStocks(event.affected_stocks) }}
                </span>
              </div>
              <div class="event-signals" v-if="event.signal_count > 0">
                <router-link
                  :to="`/signals?event_id=${event.event_id || event.id}&stock=${event.stock_code || ''}`"
                  class="signals-link"
                  @click.stop
                >
                  已触发 {{ event.signal_count }} 条信号
                </router-link>
              </div>
              <div class="event-expand-hint">
                {{ expandedLive.has(event.event_id || event.id || `live_${idx}`) ? '&#x25B2; 收起' : '&#x25BC; 展开详情' }}
              </div>
            </div>

            <transition name="expand">
              <div v-if="expandedLive.has(event.event_id || event.id || `live_${idx}`)" class="event-card-body">
                <div class="detail-block" v-if="event.summary">
                  <div class="detail-label">事件摘要</div>
                  <p class="event-summary">{{ event.summary }}</p>
                </div>
                <div class="detail-block" v-if="event.affected_stocks && event.affected_stocks.length">
                  <div class="detail-label">受影响标的</div>
                  <div class="stock-tags">
                    <span v-for="s in event.affected_stocks" :key="typeof s === 'string' ? s : s.code" class="stock-tag">{{ typeof s === 'string' ? s : (s.name || s.code) }}</span>
                  </div>
                </div>
                <div class="detail-block" v-if="event.signals && event.signals.length">
                  <div class="detail-label">产生信号</div>
                  <div class="signal-list">
                    <router-link
                      v-for="sig in event.signals"
                      :key="sig.id"
                      :to="`/signals/${sig.id}`"
                      class="signal-item"
                      @click.stop
                    >
                      <span class="signal-dir" :class="sig.direction">{{ sig.direction === 'long' ? '做多' : '回避' }}</span>
                      <span>{{ sig.title || sig.stock }}</span>
                      <span class="signal-arrow">&rarr;</span>
                    </router-link>
                  </div>
                </div>
              </div>
            </transition>
          </div>
        </div>
      </div>
    </div>

    <!-- 历史事件库 -->
    <div v-if="activeTab === 'history'" class="tab-content">
      <!-- 类型过滤 -->
      <div class="history-filters">
        <button
          class="type-chip"
          :class="{ active: historyFilter === null }"
          @click="historyFilter = null"
        >
          全部
        </button>
        <button
          v-for="et in eventTypeList"
          :key="et.key"
          class="type-chip"
          :class="{ active: historyFilter === et.key }"
          @click="historyFilter = et.key"
        >
          <span class="chip-dot" :style="{ background: et.color }"></span>
          {{ et.label }}
        </button>
      </div>

      <div v-if="historyLoading" class="loading-state">
        <div class="loading-spinner"></div>
        <span>加载中...</span>
      </div>

      <div v-else-if="filteredHistory.length === 0" class="empty-state">
        <div class="empty-icon">&#x1F4DA;</div>
        <p>暂无历史事件</p>
      </div>

      <div v-else class="history-list">
        <template v-for="event in filteredHistory" :key="event.id">
          <div
            class="history-card"
            :class="{ expanded: expandedHistory.has(event.id) }"
            @click="toggleHistoryExpand(event.id)"
          >
            <div class="history-card-main">
              <div class="history-card-left">
                <div class="history-dot" :style="{ background: getEventColor(event.event_type || event.type) }"></div>
                <div class="history-date">{{ formatDate(event.event_date || event.date) }}</div>
              </div>
              <div class="history-card-center">
                <div class="history-card-top">
                  <span class="event-badge small" :style="{ background: getEventColor(event.event_type || event.type) }">
                    {{ getEventTypeLabel(event.event_type || event.type) }}
                  </span>
                  <span class="history-source" v-if="event.stock_name || event.stock_code">
                    {{ event.stock_name }}<span class="source-code" v-if="event.stock_code">({{ event.stock_code }})</span>
                  </span>
                </div>
                <div class="history-title">{{ event.title || event.summary }}</div>
                <div class="history-affected" v-if="event.affected_stocks && event.affected_stocks.length">
                  <span class="affected-label">影响:</span>
                  {{ formatAffectedStocks(event.affected_stocks) }}
                </div>
              </div>
              <div class="history-card-right">
                <div class="history-stat" v-if="event.signal_count">
                  <span class="stat-num">{{ event.signal_count }}</span>
                  <span class="stat-label">信号</span>
                </div>
                <div class="history-stat" v-if="event.win_rate != null">
                  <span class="stat-num" :class="getWinrateClass(event.win_rate)">
                    {{ (event.win_rate * 100).toFixed(1) }}%
                  </span>
                  <span class="stat-label">胜率</span>
                </div>
                <div class="expand-icon">{{ expandedHistory.has(event.id) ? '&#x25B2;' : '&#x25BC;' }}</div>
              </div>
            </div>

            <transition name="expand">
              <div v-if="expandedHistory.has(event.id)" class="history-card-detail">
                <div class="detail-block" v-if="event.summary">
                  <div class="detail-label">事件摘要</div>
                  <p class="event-summary">{{ event.summary }}</p>
                </div>
                <div class="detail-block" v-if="event.affected_stocks && event.affected_stocks.length">
                  <div class="detail-label">受影响标的</div>
                  <div class="stock-tags">
                    <span v-for="s in event.affected_stocks" :key="typeof s === 'string' ? s : s.code" class="stock-tag">{{ typeof s === 'string' ? s : (s.name || s.code) }}</span>
                  </div>
                </div>
                <div class="detail-block" v-if="event.signals && event.signals.length">
                  <div class="detail-label">产生信号</div>
                  <div class="signal-list">
                    <router-link
                      v-for="sig in event.signals"
                      :key="sig.id"
                      :to="`/signals/${sig.id}`"
                      class="signal-item"
                      @click.stop
                    >
                      <span class="signal-dir" :class="sig.direction">{{ sig.direction === 'long' ? '做多' : '回避' }}</span>
                      <span>{{ sig.title || sig.stock }}</span>
                      <span class="signal-arrow">&rarr;</span>
                    </router-link>
                  </div>
                </div>
              </div>
            </transition>
          </div>
        </template>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { getEventsLive, getEventsHistory } from '../api/astrategy.js'

// ── 完整事件类型映射（中英文） ──
const eventTypeMap = {
  scandal: { label: '丑闻', color: '#C5283D' },
  earnings_shock: { label: '业绩冲击', color: '#e74c3c' },
  earnings_surprise: { label: '业绩超预期', color: '#1A936F' },
  ma: { label: '并购重组', color: '#8e44ad' },
  policy_risk: { label: '政策风险', color: '#9b59b6' },
  policy_change: { label: '政策变化', color: '#7f8c8d' },
  product_launch: { label: '产品发布', color: '#004E89' },
  technology_breakthrough: { label: '技术突破', color: '#2980b9' },
  management_change: { label: '管理层变动', color: '#E9724C' },
  cooperation: { label: '合作', color: '#3498db' },
  supply_shortage: { label: '供应短缺', color: '#f39c12' },
  buyback: { label: '回购', color: '#27ae60' },
  order_win: { label: '获得订单', color: '#16a085' },
  price_adjustment: { label: '价格调整', color: '#d35400' },
  other: { label: '其他', color: '#666' },
}

const eventTypeList = Object.entries(eventTypeMap).map(([key, val]) => ({
  key,
  ...val,
}))

function getEventColor(type) {
  return eventTypeMap[type]?.color || '#666'
}

function getEventTypeLabel(type) {
  return eventTypeMap[type]?.label || type || '其他'
}

// ── 格式化受影响股票（显示名称，超过3个截断） ──
function formatAffectedStocks(stocks) {
  if (!stocks || stocks.length === 0) return ''
  const names = stocks.map(s => typeof s === 'string' ? s : (s.name || s.code || String(s)))
  if (names.length <= 3) {
    return names.join(', ')
  }
  return names.slice(0, 2).join(', ') + ` 等${names.length}家`
}

// ── 数据源信息 ──
const dataSource = ref(null)

// ── 状态 ──
const activeTab = ref('live')
const liveLoading = ref(false)
const historyLoading = ref(false)
const liveEvents = ref([])
const historyEvents = ref([])
const expandedLive = ref(new Set())
const expandedHistory = ref(new Set())
const historyFilter = ref(null)

// ── 过滤后历史数据 ──
const filteredHistory = computed(() => {
  if (!historyFilter.value) return historyEvents.value
  return historyEvents.value.filter(e => (e.event_type || e.type) === historyFilter.value)
})

// ── 展开/折叠 ──
function toggleLiveExpand(id) {
  const s = new Set(expandedLive.value)
  if (s.has(id)) s.delete(id)
  else s.add(id)
  expandedLive.value = s
}

function toggleHistoryExpand(id) {
  const s = new Set(expandedHistory.value)
  if (s.has(id)) s.delete(id)
  else s.add(id)
  expandedHistory.value = s
}

// ── 格式化 ──
function formatTime(dateStr) {
  if (!dateStr) return ''
  try {
    const d = new Date(dateStr)
    const now = new Date()
    const diff = now - d
    if (diff < 60000) return '刚刚'
    if (diff < 3600000) return `${Math.floor(diff / 60000)}分钟前`
    if (diff < 86400000) return `${Math.floor(diff / 3600000)}小时前`
    if (diff < 604800000) return `${Math.floor(diff / 86400000)}天前`
    return ''
  } catch {
    return ''
  }
}

function formatDate(dateStr) {
  if (!dateStr) return ''
  try {
    const d = new Date(dateStr)
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
  } catch {
    return dateStr
  }
}

function getWinrateClass(rate) {
  if (rate == null) return ''
  if (rate >= 0.6) return 'win-high'
  if (rate >= 0.4) return 'win-mid'
  return 'win-low'
}

// ── 数据加载 ──
async function fetchLive() {
  liveLoading.value = true
  try {
    const resp = await getEventsLive()
    const data = resp.data || resp
    liveEvents.value = Array.isArray(data) ? data : (data.events || [])
    // 提取数据源信息
    if (data.data_source) {
      dataSource.value = data.data_source
    }
  } catch {
    liveEvents.value = []
  } finally {
    liveLoading.value = false
  }
}

async function fetchHistory() {
  historyLoading.value = true
  try {
    const resp = await getEventsHistory()
    const data = resp.data || resp
    historyEvents.value = Array.isArray(data) ? data : (data.events || [])
  } catch {
    historyEvents.value = []
  } finally {
    historyLoading.value = false
  }
}

onMounted(() => {
  fetchLive()
  fetchHistory()
})
</script>

<style scoped>
.event-stream {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: #F5F5F5;
  font-family: 'Noto Sans SC', 'Space Grotesk', sans-serif;
}

/* 数据源说明 */
.data-source-bar {
  display: flex;
  gap: 24px;
  padding: 10px 24px;
  background: #FFFBEB;
  border-bottom: 1px solid #FDE68A;
  font-size: 12px;
  flex-shrink: 0;
}

.data-source-item {
  display: flex;
  align-items: center;
  gap: 6px;
}

.data-source-bar .ds-label {
  color: #92400E;
  font-weight: 600;
}

.data-source-bar .ds-value {
  color: #78350F;
}

/* ── 顶部标签 ── */
.stream-header {
  background: #fff;
  border-bottom: 1px solid #E5E5E5;
  padding: 0 24px;
  flex-shrink: 0;
}

.tab-bar {
  display: flex;
  gap: 0;
}

.tab-btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 14px 20px;
  border: none;
  background: none;
  font-size: 14px;
  font-family: inherit;
  color: #999;
  cursor: pointer;
  border-bottom: 2px solid transparent;
  transition: all 0.2s;
}

.tab-btn:hover {
  color: #666;
}

.tab-btn.active {
  color: #111;
  font-weight: 600;
  border-bottom-color: #FF4500;
}

.tab-dot {
  width: 6px;
  height: 6px;
  border-radius: 50%;
}

.live-dot {
  background: #1A936F;
  animation: pulse 2s ease-in-out infinite;
}

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.3; }
}

/* ── 标签内容 ── */
.tab-content {
  flex: 1;
  overflow-y: auto;
  padding: 20px 24px;
}

/* ── 加载 / 空 ── */
.loading-state,
.empty-state {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 60px 0;
  color: #999;
  font-size: 14px;
  gap: 12px;
}

.loading-spinner {
  width: 32px;
  height: 32px;
  border: 3px solid #E5E5E5;
  border-top-color: #FF4500;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.empty-icon {
  font-size: 36px;
  opacity: 0.3;
}

/* ── 时间线 (实时事件) ── */
.timeline {
  position: relative;
  max-width: 720px;
  margin: 0 auto;
}

.timeline-item {
  position: relative;
  padding-left: 28px;
  padding-bottom: 20px;
}

.timeline-line {
  position: absolute;
  left: 6px;
  top: 10px;
  bottom: 0;
  width: 2px;
  background: #E5E5E5;
}

.timeline-item:last-child .timeline-line {
  display: none;
}

.timeline-dot {
  position: absolute;
  left: 0;
  top: 8px;
  width: 14px;
  height: 14px;
  border-radius: 50%;
  border: 2px solid #fff;
  box-shadow: 0 0 0 2px #E5E5E5;
  z-index: 1;
}

/* ── 事件卡片 ── */
.event-card {
  background: #fff;
  border: 1px solid #E5E5E5;
  border-radius: 10px;
  padding: 16px 20px;
  cursor: pointer;
  transition: all 0.2s;
}

.event-card:hover {
  border-color: #ddd;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.04);
}

.event-card.expanded {
  border-color: #ccc;
}

.event-card-header {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.event-meta {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.event-date-prominent {
  font-size: 13px;
  font-weight: 600;
  color: #333;
  font-family: 'JetBrains Mono', monospace;
}

.event-time-ago {
  font-size: 11px;
  color: #bbb;
  margin-left: auto;
}

.event-badge {
  display: inline-block;
  padding: 2px 10px;
  border-radius: 10px;
  font-size: 11px;
  font-weight: 500;
  color: #fff;
}

.event-badge.small {
  padding: 2px 8px;
  font-size: 10px;
}

.event-source {
  font-size: 14px;
  font-weight: 600;
  color: #111;
}

.source-name {
  color: #111;
}

.source-code {
  color: #999;
  font-family: 'JetBrains Mono', monospace;
  font-size: 12px;
  font-weight: 400;
  margin-left: 2px;
}

.event-title {
  font-size: 13px;
  color: #444;
  line-height: 1.5;
}

.event-affected {
  font-size: 12px;
  color: #666;
  display: flex;
  gap: 4px;
  align-items: baseline;
}

.affected-label {
  color: #999;
  flex-shrink: 0;
  font-size: 11px;
}

.affected-list {
  color: #555;
}

.event-signals {
  margin-top: 2px;
}

.signals-link {
  font-size: 12px;
  color: #FF4500;
  text-decoration: none;
  font-weight: 500;
  transition: opacity 0.2s;
}

.signals-link:hover {
  opacity: 0.7;
}

.event-expand-hint {
  font-size: 11px;
  color: #ccc;
  margin-top: 4px;
  user-select: none;
}

/* ── 展开内容（通用） ── */
.event-card-body,
.history-card-detail {
  margin-top: 12px;
  padding-top: 12px;
  border-top: 1px solid #F0F0F0;
  display: flex;
  flex-direction: column;
  gap: 14px;
}

.detail-block {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.detail-label {
  font-size: 11px;
  font-weight: 600;
  color: #999;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.event-summary {
  font-size: 13px;
  line-height: 1.7;
  color: #555;
  margin: 0;
}

.expand-enter-active,
.expand-leave-active {
  transition: all 0.25s ease;
  overflow: hidden;
}

.expand-enter-from,
.expand-leave-to {
  opacity: 0;
  max-height: 0;
}

.expand-enter-to,
.expand-leave-from {
  opacity: 1;
  max-height: 500px;
}

/* ── 历史过滤 ── */
.history-filters {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-bottom: 16px;
}

.type-chip {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 6px 14px;
  border: 1px solid #E5E5E5;
  border-radius: 20px;
  background: #fff;
  font-size: 12px;
  color: #666;
  cursor: pointer;
  transition: all 0.2s;
  font-family: inherit;
}

.type-chip:hover {
  border-color: #ccc;
}

.type-chip.active {
  background: #111;
  color: #fff;
  border-color: #111;
}

.chip-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  flex-shrink: 0;
}

/* ── 历史事件列表（卡片式） ── */
.history-list {
  display: flex;
  flex-direction: column;
  gap: 10px;
  max-width: 900px;
}

.history-card {
  background: #fff;
  border: 1px solid #E5E5E5;
  border-radius: 10px;
  padding: 16px 20px;
  cursor: pointer;
  transition: all 0.2s;
}

.history-card:hover {
  border-color: #ddd;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.04);
}

.history-card.expanded {
  border-color: #ccc;
}

.history-card-main {
  display: flex;
  gap: 16px;
  align-items: flex-start;
}

.history-card-left {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-shrink: 0;
  min-width: 120px;
}

.history-dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  flex-shrink: 0;
}

.history-date {
  font-size: 13px;
  font-weight: 600;
  color: #333;
  font-family: 'JetBrains Mono', monospace;
  white-space: nowrap;
}

.history-card-center {
  flex: 1;
  min-width: 0;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.history-card-top {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
}

.history-source {
  font-size: 13px;
  font-weight: 600;
  color: #333;
}

.history-title {
  font-size: 13px;
  color: #555;
  line-height: 1.5;
}

.history-affected {
  font-size: 12px;
  color: #888;
}

.history-card-right {
  display: flex;
  align-items: center;
  gap: 16px;
  flex-shrink: 0;
}

.history-stat {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 2px;
}

.stat-num {
  font-size: 16px;
  font-weight: 700;
  color: #333;
  font-family: 'JetBrains Mono', monospace;
}

.stat-label {
  font-size: 10px;
  color: #bbb;
}

.expand-icon {
  font-size: 10px;
  color: #ccc;
  user-select: none;
}

.win-high {
  color: #1A936F !important;
}

.win-mid {
  color: #f39c12 !important;
}

.win-low {
  color: #C5283D !important;
}

/* ── 股票标签 ── */
.stock-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.stock-tag {
  display: inline-block;
  padding: 3px 10px;
  border: 1px solid #E5E5E5;
  border-radius: 12px;
  font-size: 11px;
  color: #444;
  background: #FAFAFA;
}

/* ── 信号列表 ── */
.signal-list {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.signal-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 10px;
  border-radius: 6px;
  background: #FAFAFA;
  border: 1px solid #F0F0F0;
  text-decoration: none;
  color: #333;
  font-size: 12px;
  transition: background 0.2s;
}

.signal-item:hover {
  background: #F0F0F0;
}

.signal-dir {
  font-size: 10px;
  font-weight: 600;
  padding: 2px 6px;
  border-radius: 3px;
  flex-shrink: 0;
}

.signal-dir.long {
  background: #E8F5E9;
  color: #2E7D32;
}

.signal-dir.avoid {
  background: #FFEBEE;
  color: #C62828;
}

.signal-arrow {
  margin-left: auto;
  color: #ccc;
}
</style>
