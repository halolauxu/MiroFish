<template>
  <div class="portfolio-view">
    <!-- 研究模拟持仓警告横幅 -->
    <div class="warning-banner">
      ⚠️ 研究模拟持仓 — 以下数据基于实验性策略生成，非真实交易，不构成投资建议。所有策略尚未通过完整的Walk-Forward验证。
    </div>

    <!-- 持仓总览 -->
    <section class="section summary-section">
      <h2 class="section-title">持仓总览</h2>
      <div class="summary-cards">
        <div class="summary-card">
          <span class="card-label">总市值 <span class="mock-tag">模拟数据</span></span>
          <span class="card-value">{{ formatMoney(summary.totalValue) }}</span>
        </div>
        <div class="summary-card">
          <span class="card-label">现金比例</span>
          <span class="card-value">{{ summary.cashRatio }}%</span>
        </div>
        <div class="summary-card">
          <span class="card-label">持仓数量</span>
          <span class="card-value">{{ summary.positionCount }}</span>
        </div>
        <div class="summary-card">
          <span class="card-label">当日盈亏 <span class="mock-tag">模拟数据</span></span>
          <span class="card-value" :class="summary.dailyPnl >= 0 ? 'text-green' : 'text-red'">
            {{ summary.dailyPnl >= 0 ? '+' : '' }}{{ summary.dailyPnl }}%
          </span>
        </div>
      </div>
      <div class="allocation-bar-wrapper">
        <div class="allocation-label">
          <span>仓位使用率</span>
          <span class="mono">{{ summary.allocation }}%</span>
        </div>
        <div class="allocation-track">
          <div class="allocation-fill" :style="{ width: summary.allocation + '%' }"></div>
        </div>
      </div>
    </section>

    <!-- 持仓列表 -->
    <section class="section">
      <h2 class="section-title">持仓列表</h2>
      <div class="table-wrapper">
        <table class="data-table">
          <thead>
            <tr>
              <th>代码</th>
              <th>名称</th>
              <th>方向</th>
              <th>仓位%</th>
              <th>参考收益</th>
              <th>来源策略</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="pos in positions"
              :key="pos.code"
              class="clickable-row"
              @click="goToSignal(pos.signalId)"
            >
              <td class="mono">{{ pos.code }}</td>
              <td>{{ pos.name }}</td>
              <td>
                <span class="direction-badge" :class="pos.direction === '做多' ? 'long' : 'avoid'">
                  {{ pos.direction }}
                </span>
              </td>
              <td class="mono">{{ pos.weight }}%</td>
              <td class="mono ref-return">
                {{ (pos.pnl || 0) >= 0 ? '+' : '' }}{{ (pos.pnl || 0).toFixed(2) }}%
              </td>
              <td class="strategy-tag">{{ pos.strategy }}</td>
            </tr>
          </tbody>
        </table>
        <div v-if="positions.length === 0" class="empty-state">暂无持仓数据</div>
      </div>
    </section>

    <!-- 仓位分布 -->
    <section class="section">
      <h2 class="section-title">仓位分布</h2>
      <div class="charts-row">
        <div class="chart-card">
          <h3 class="chart-title">个股权重</h3>
          <div ref="pieChartRef" class="chart-container"></div>
        </div>
        <div class="chart-card">
          <h3 class="chart-title">行业集中度</h3>
          <div ref="barChartRef" class="chart-container"></div>
        </div>
      </div>
    </section>

    <!-- 调仓历史 -->
    <section class="section">
      <h2 class="section-title">调仓历史</h2>
      <div class="timeline">
        <div v-for="(entry, idx) in rebalanceHistory" :key="idx" class="timeline-item">
          <div class="timeline-dot"></div>
          <div class="timeline-content">
            <span class="timeline-date mono">{{ entry.date }}</span>
            <span class="timeline-action" :class="actionClass(entry.action)">{{ entry.action }}</span>
            <span class="timeline-stock">{{ entry.stock }}</span>
            <span class="timeline-weight mono">{{ entry.weightChange }}</span>
            <span class="timeline-signal">触发: {{ entry.triggerSignal }}</span>
          </div>
        </div>
        <div v-if="rebalanceHistory.length === 0" class="empty-state">暂无调仓记录</div>
      </div>
    </section>
  </div>
</template>

<script setup>
import { ref, onMounted, nextTick, watch } from 'vue'
import { useRouter } from 'vue-router'
import * as d3 from 'd3'
import { getPortfolioPositions, getPortfolioSummary } from '../api/astrategy'

const router = useRouter()
const pieChartRef = ref(null)
const barChartRef = ref(null)

const summary = ref({
  totalValue: 1520000,
  cashRatio: 28,
  positionCount: 8,
  dailyPnl: 0.73,
  allocation: 72,
})

const positions = ref([])

const rebalanceHistory = ref([])

const PIE_COLORS = ['#FF4500', '#111', '#666', '#999', '#CCC', '#E5E5E5', '#F5A623', '#4A90D9']

const formatMoney = (val) => {
  if (val >= 10000) return (val / 10000).toFixed(2) + '万'
  return val.toLocaleString()
}

const actionClass = (action) => {
  if (action === '买入' || action === '加仓') return 'action-buy'
  return 'action-sell'
}

const goToSignal = (signalId) => {
  if (signalId) router.push(`/signals/${signalId}`)
}

const drawPieChart = () => {
  if (!pieChartRef.value) return
  const el = pieChartRef.value
  el.innerHTML = ''

  const data = positions.value
    .filter(p => p.weight > 0)
    .map(p => ({ label: p.name, value: p.weight }))

  if (data.length === 0) return

  const width = el.clientWidth
  const height = 280
  const radius = Math.min(width, height) / 2 - 40

  const svg = d3.select(el)
    .append('svg')
    .attr('width', width)
    .attr('height', height)

  const g = svg.append('g')
    .attr('transform', `translate(${width / 2 - 60}, ${height / 2})`)

  const pie = d3.pie().value(d => d.value).sort(null)
  const arc = d3.arc().innerRadius(radius * 0.5).outerRadius(radius)

  const arcs = g.selectAll('.arc')
    .data(pie(data))
    .enter()
    .append('g')

  arcs.append('path')
    .attr('d', arc)
    .attr('fill', (d, i) => PIE_COLORS[i % PIE_COLORS.length])
    .attr('stroke', '#FFF')
    .attr('stroke-width', 2)

  // Legend
  const legend = svg.append('g')
    .attr('transform', `translate(${width / 2 + radius - 20}, 30)`)

  data.forEach((d, i) => {
    const ly = legend.append('g').attr('transform', `translate(0, ${i * 24})`)
    ly.append('rect').attr('width', 12).attr('height', 12).attr('fill', PIE_COLORS[i % PIE_COLORS.length]).attr('rx', 2)
    ly.append('text').attr('x', 18).attr('y', 10).text(`${d.label} ${d.value}%`).attr('font-size', '12px').attr('fill', '#333').attr('font-family', "'Noto Sans SC', sans-serif")
  })
}

const drawBarChart = () => {
  if (!barChartRef.value) return
  const el = barChartRef.value
  el.innerHTML = ''

  // Aggregate by industry (mock)
  const industryData = [
    { label: '新能源', value: 37 },
    { label: '金融', value: 19 },
    { label: '消费', value: 12 },
    { label: '医药', value: 8 },
    { label: '现金', value: 28 },
  ].sort((a, b) => b.value - a.value)

  const width = el.clientWidth
  const height = 280
  const margin = { top: 20, right: 40, bottom: 20, left: 70 }
  const innerW = width - margin.left - margin.right
  const innerH = height - margin.top - margin.bottom

  const svg = d3.select(el)
    .append('svg')
    .attr('width', width)
    .attr('height', height)

  const g = svg.append('g')
    .attr('transform', `translate(${margin.left}, ${margin.top})`)

  const y = d3.scaleBand()
    .domain(industryData.map(d => d.label))
    .range([0, innerH])
    .padding(0.3)

  const x = d3.scaleLinear()
    .domain([0, d3.max(industryData, d => d.value)])
    .range([0, innerW])

  g.selectAll('.bar')
    .data(industryData)
    .enter()
    .append('rect')
    .attr('x', 0)
    .attr('y', d => y(d.label))
    .attr('width', d => x(d.value))
    .attr('height', y.bandwidth())
    .attr('fill', (d, i) => i === 0 ? '#FF4500' : '#111')
    .attr('rx', 3)

  g.selectAll('.bar-label')
    .data(industryData)
    .enter()
    .append('text')
    .attr('x', d => x(d.value) + 6)
    .attr('y', d => y(d.label) + y.bandwidth() / 2 + 4)
    .text(d => d.value + '%')
    .attr('font-size', '12px')
    .attr('font-family', "'JetBrains Mono', monospace")
    .attr('fill', '#666')

  g.append('g')
    .call(d3.axisLeft(y).tickSize(0))
    .select('.domain').remove()

  g.selectAll('.tick text')
    .attr('font-size', '13px')
    .attr('font-family', "'Noto Sans SC', sans-serif")
    .attr('fill', '#333')
}

const _STRATEGY_CN = {
  shock_propagation: '冲击链路',
  graph_factors: 'S07图谱因子',
  sector_rotation: '行业轮动',
  institution_association: '机构关联',
  sentiment_simulation: 'S10舆情',
  analyst_divergence: '分析师分歧',
}
const _DIR_CN = { long: '做多', short: '回避', avoid: '回避', neutral: '中性' }

const loadData = async () => {
  // 加载总览（独立 try-catch，不影响持仓加载）
  try {
    const summaryRes = await getPortfolioSummary()
    // axios 拦截器已 unwrap response.data → summaryRes 直接是 {success, data}
    const sd = summaryRes?.data || summaryRes
    if (sd) {
      const d = sd
      summary.value = {
        totalValue: d.total_value || 0,
        cashRatio: d.cash ? Math.round(d.cash / (d.total_value || 1) * 100) : 0,
        positionCount: d.position_count || d.long_count || 0,
        dailyPnl: d.daily_return || 0,
        allocation: d.position_count ? Math.round((1 - (d.cash || 0) / (d.total_value || 1)) * 100) : 0,
      }
    }
  } catch (e) {
    console.warn('加载持仓总览失败:', e.message)
  }

  // 加载持仓列表（独立 try-catch）
  try {
    const positionsRes = await getPortfolioPositions()
    // axios 拦截器已 unwrap → positionsRes 直接是 {success, data, total}
    const raw = positionsRes?.data || []
    console.log('持仓API返回:', positionsRes?.success, '数量:', raw?.length)
    if (Array.isArray(raw) && raw.length > 0) {
      const longCount = raw.filter(r => r.direction === 'long').length || 1
      positions.value = raw.map((p) => ({
        code: p.stock_code || '',
        name: p.stock_name || p.display_name || '',
        direction: _DIR_CN[p.direction] || p.direction || '中性',
        weight: p.direction === 'long' ? Math.round(100 / longCount) : 0,
        costPrice: 0,
        currentPrice: 0,
        pnl: p.expected_return ? +(p.expected_return * 100).toFixed(2) : 0,
        strategy: _STRATEGY_CN[p.strategy] || p.strategy_cn || p.source_cn || p.strategy || '',
        signalId: p.signal_id || '',
      }))
      // 更新总览中的持仓数量
      summary.value.positionCount = raw.length
    }
  } catch (e) {
    console.warn('加载持仓列表失败:', e.message)
  }
}

onMounted(async () => {
  await loadData()
  await nextTick()
  drawPieChart()
  drawBarChart()
})

watch(positions, async () => {
  await nextTick()
  drawPieChart()
  drawBarChart()
}, { deep: true })
</script>

<style scoped>
.warning-banner {
  background: #FEE2E2;
  color: #B91C1C;
  font-size: 13px;
  font-weight: 600;
  text-align: center;
  padding: 12px 16px;
  border: 1px solid #FECACA;
  border-radius: 6px;
  margin-bottom: 24px;
}

.mock-tag {
  display: inline-block;
  font-size: 10px;
  color: #999;
  background: #F0F0F0;
  padding: 1px 6px;
  border-radius: 3px;
  font-weight: 400;
  margin-left: 4px;
  vertical-align: middle;
}

.ref-return {
  color: #999 !important;
}

.portfolio-view {
  padding: 32px;
  max-width: 1200px;
  margin: 0 auto;
  font-family: 'Space Grotesk', 'Noto Sans SC', system-ui, sans-serif;
}

.section {
  margin-bottom: 40px;
}

.section-title {
  font-size: 20px;
  font-weight: 700;
  color: #111;
  margin-bottom: 20px;
  padding-bottom: 8px;
  border-bottom: 2px solid #111;
  display: inline-block;
}

/* Summary Cards */
.summary-cards {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
  margin-bottom: 20px;
}

.summary-card {
  background: #F5F5F5;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
  padding: 20px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.card-label {
  font-size: 13px;
  color: #666;
  font-weight: 500;
}

.card-value {
  font-size: 24px;
  font-weight: 700;
  font-family: 'JetBrains Mono', monospace;
  color: #111;
}

/* Allocation Bar */
.allocation-bar-wrapper {
  margin-top: 8px;
}

.allocation-label {
  display: flex;
  justify-content: space-between;
  font-size: 13px;
  color: #666;
  margin-bottom: 6px;
}

.allocation-track {
  height: 8px;
  background: #E5E5E5;
  border-radius: 4px;
  overflow: hidden;
}

.allocation-fill {
  height: 100%;
  background: #FF4500;
  border-radius: 4px;
  transition: width 0.6s ease;
}

/* Table */
.table-wrapper {
  overflow-x: auto;
}

.data-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 14px;
}

.data-table th {
  text-align: left;
  padding: 12px 16px;
  font-size: 12px;
  font-weight: 600;
  color: #666;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  border-bottom: 1px solid #E5E5E5;
  background: #F5F5F5;
}

.data-table td {
  padding: 12px 16px;
  border-bottom: 1px solid #F0F0F0;
  color: #333;
}

.clickable-row {
  cursor: pointer;
  transition: background 0.15s;
}

.clickable-row:hover {
  background: #FAFAFA;
}

.mono {
  font-family: 'JetBrains Mono', monospace;
}

.text-green { color: #16A34A; }
.text-red { color: #DC2626; }

.direction-badge {
  display: inline-block;
  padding: 2px 10px;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 600;
}

.direction-badge.long {
  background: #DCFCE7;
  color: #16A34A;
}

.direction-badge.avoid {
  background: #FEE2E2;
  color: #DC2626;
}

.strategy-tag {
  font-size: 12px;
  color: #666;
}

.empty-state {
  text-align: center;
  padding: 40px;
  color: #999;
  font-size: 14px;
}

/* Charts */
.charts-row {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 24px;
}

.chart-card {
  background: #F5F5F5;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
  padding: 20px;
}

.chart-title {
  font-size: 14px;
  font-weight: 600;
  color: #333;
  margin-bottom: 12px;
}

.chart-container {
  width: 100%;
  min-height: 280px;
}

/* Timeline */
.timeline {
  position: relative;
  padding-left: 24px;
}

.timeline::before {
  content: '';
  position: absolute;
  left: 5px;
  top: 4px;
  bottom: 4px;
  width: 2px;
  background: #E5E5E5;
}

.timeline-item {
  position: relative;
  margin-bottom: 16px;
}

.timeline-dot {
  position: absolute;
  left: -24px;
  top: 6px;
  width: 12px;
  height: 12px;
  border-radius: 50%;
  background: #111;
  border: 2px solid #FFF;
  box-shadow: 0 0 0 2px #E5E5E5;
}

.timeline-content {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px 16px;
  background: #F5F5F5;
  border: 1px solid #E5E5E5;
  border-radius: 6px;
  font-size: 14px;
  flex-wrap: wrap;
}

.timeline-date {
  color: #666;
  font-size: 13px;
  min-width: 90px;
}

.timeline-action {
  font-weight: 700;
  font-size: 13px;
  padding: 2px 8px;
  border-radius: 3px;
}

.action-buy {
  background: #DCFCE7;
  color: #16A34A;
}

.action-sell {
  background: #FEE2E2;
  color: #DC2626;
}

.timeline-stock {
  font-weight: 600;
  color: #111;
}

.timeline-weight {
  color: #666;
  font-size: 13px;
}

.timeline-signal {
  color: #999;
  font-size: 12px;
}

@media (max-width: 768px) {
  .summary-cards {
    grid-template-columns: repeat(2, 1fr);
  }
  .charts-row {
    grid-template-columns: 1fr;
  }
}
</style>
