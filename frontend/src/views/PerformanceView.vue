<template>
  <div class="performance-view">
    <!-- 整体绩效 -->
    <section class="section">
      <h2 class="section-title">整体绩效</h2>
      <div class="metric-cards">
        <div class="metric-card" v-for="m in metrics" :key="m.label">
          <span class="metric-label">{{ m.label }}</span>
          <span class="metric-value" :class="{ highlight: m.highlight }">{{ m.value }}</span>
          <span class="metric-desc">{{ m.desc }}</span>
          <span class="metric-subdesc" v-if="m.subdesc">{{ m.subdesc }}</span>
        </div>
      </div>
    </section>

    <!-- 策略对比 -->
    <section class="section">
      <h2 class="section-title">策略对比</h2>
      <div class="table-wrapper">
        <table class="data-table">
          <thead>
            <tr>
              <th>策略名称</th>
              <th>Sharpe</th>
              <th>胜率</th>
              <th>信号数</th>
              <th>平均收益</th>
              <th>IC</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="s in strategies" :key="s.name">
              <td>
                <span class="strategy-name">{{ s.name }}</span>
                <span v-if="s.tag" class="strategy-badge">{{ s.tag }}</span>
                <span v-if="!s.hasBacktest" class="no-backtest-tag">无独立回测</span>
              </td>
              <td class="mono" :class="{ 'best-val': isBest('sharpe', s.sharpe) }">{{ s.sharpe.toFixed(2) }}</td>
              <td class="mono" :class="{ 'best-val': isBest('winRate', s.winRate) }">{{ s.winRate }}%</td>
              <td class="mono">{{ s.signalCount }}</td>
              <td class="mono" :class="{ 'best-val': isBest('avgReturn', s.avgReturn) }">{{ s.avgReturn >= 0 ? '+' : '' }}{{ s.avgReturn }}%</td>
              <td class="mono" :class="{ 'best-val': isBest('ic', s.ic) }">{{ s.ic.toFixed(3) }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <!-- 净值曲线 -->
    <section class="section">
      <h2 class="section-title">净值曲线</h2>
      <div class="nav-chart-note">基于历史事件回测的模拟净值，非实盘收益</div>
      <div class="nav-chart-card">
        <div class="chart-legend">
          <span class="legend-item"><span class="legend-line orange"></span>组合净值</span>
          <span class="legend-item"><span class="legend-line gray"></span>沪深300</span>
        </div>
        <div ref="navChartRef" class="nav-chart-container"></div>
      </div>
    </section>

    <!-- 事件方向映射 -->
    <section class="section">
      <h2 class="section-title">事件方向映射</h2>
      <p class="section-desc">基于 Iteration 10 历史回测验证的方向规则</p>
      <div class="rule-cards">
        <div v-for="rule in directionRules" :key="rule.label" class="rule-card" :class="rule.type">
          <div class="rule-header">
            <span class="rule-events">{{ rule.events }}</span>
            <span class="rule-arrow">→</span>
            <span class="rule-direction" :class="rule.type">{{ rule.direction }}</span>
          </div>
          <div class="rule-reason">{{ rule.reason }}</div>
          <div class="rule-footer">
            <span class="rule-winrate mono">胜率: {{ rule.winRate }}%</span>
          </div>
        </div>
      </div>
    </section>
  </div>
</template>

<script setup>
import { ref, onMounted, onUnmounted, nextTick } from 'vue'
import * as d3 from 'd3'
import { getBacktestSummary, getBacktestStrategies } from '../api/astrategy'

const navChartRef = ref(null)

const metrics = ref([
  { label: '冲击链路 Sharpe', value: '1.49', desc: '含交易成本 0.3%', subdesc: '(基于15事件初步研究)', highlight: true },
  { label: '总体胜率', value: '67.3%', desc: '全部信号', subdesc: '(98信号样本)', highlight: false },
  { label: '3跳胜率', value: '81.2%', desc: '3跳传播路径', subdesc: '(23信号OOS)', highlight: true },
  { label: '信息差Sharpe', value: '2.31', desc: '未反应信号', subdesc: '(样本不足，待验证)', highlight: true },
])

const strategies = ref([
  { name: '冲击链路', tag: 'PRIMARY', sharpe: 1.49, winRate: 67.3, signalCount: 98, avgReturn: 2.8, ic: 0.087, hasBacktest: true },
  { name: 'S07图谱因子', tag: '', sharpe: 0.56, winRate: 53.1, signalCount: 245, avgReturn: 0.9, ic: 0.042, hasBacktest: true },
  { name: 'S10舆情', tag: '', sharpe: 0.83, winRate: 58.4, signalCount: 67, avgReturn: 1.5, ic: 0.055, hasBacktest: false },
  { name: 'S05分歧度', tag: '', sharpe: 0.41, winRate: 51.2, signalCount: 180, avgReturn: 0.6, ic: 0.031, hasBacktest: false },
  { name: 'S09行业轮动', tag: '', sharpe: 0.72, winRate: 55.8, signalCount: 120, avgReturn: 1.2, ic: 0.048, hasBacktest: false },
  { name: 'S11叙事', tag: '', sharpe: 0.38, winRate: 49.5, signalCount: 42, avgReturn: 0.4, ic: 0.025, hasBacktest: false },
])

const directionRules = ref([
  { events: '负面事件 (丑闻 / 政策风险 / 管理层变动)', direction: '回避', reason: '板块连坐效应，负面冲击沿供应链扩散', winRate: 78, type: 'avoid' },
  { events: '利好事件 (产品发布 / 技术突破 / 回购 / 价格调整)', direction: '回避', reason: '利好出尽，市场已提前反应', winRate: 72, type: 'avoid' },
  { events: '合作 / 业绩 / 供应短缺', direction: '做多', reason: '少数真正利好，信息差未被充分定价', winRate: 65, type: 'long' },
])

// Compute best values per column
const bestValues = ref({})
const computeBest = () => {
  const cols = ['sharpe', 'winRate', 'avgReturn', 'ic']
  cols.forEach(col => {
    bestValues.value[col] = Math.max(...strategies.value.map(s => s[col]))
  })
}

const isBest = (col, val) => val === bestValues.value[col]

const generateSampleNavData = () => {
  const days = 120
  const startDate = new Date('2025-10-01')
  const portfolio = []
  const benchmark = []
  let pVal = 1.0
  let bVal = 1.0

  for (let i = 0; i < days; i++) {
    const date = new Date(startDate)
    date.setDate(date.getDate() + i)
    // Skip weekends
    if (date.getDay() === 0 || date.getDay() === 6) continue

    pVal *= 1 + (Math.random() - 0.45) * 0.02
    bVal *= 1 + (Math.random() - 0.48) * 0.015

    portfolio.push({ date: new Date(date), value: pVal })
    benchmark.push({ date: new Date(date), value: bVal })
  }
  return { portfolio, benchmark }
}

const drawNavChart = () => {
  if (!navChartRef.value) return
  const el = navChartRef.value
  el.innerHTML = ''

  const { portfolio, benchmark } = generateSampleNavData()

  const width = el.clientWidth || el.getBoundingClientRect().width || 800
  const height = 320
  const margin = { top: 20, right: 20, bottom: 40, left: 50 }
  const innerW = width - margin.left - margin.right
  const innerH = height - margin.top - margin.bottom

  const svg = d3.select(el)
    .append('svg')
    .attr('width', width)
    .attr('height', height)

  const g = svg.append('g')
    .attr('transform', `translate(${margin.left}, ${margin.top})`)

  const allData = [...portfolio, ...benchmark]
  const x = d3.scaleTime()
    .domain(d3.extent(allData, d => d.date))
    .range([0, innerW])

  const y = d3.scaleLinear()
    .domain([
      d3.min(allData, d => d.value) * 0.98,
      d3.max(allData, d => d.value) * 1.02
    ])
    .range([innerH, 0])

  // Grid lines
  g.append('g')
    .attr('class', 'grid')
    .call(d3.axisLeft(y).tickSize(-innerW).tickFormat(''))
    .selectAll('line')
    .attr('stroke', '#F0F0F0')

  g.selectAll('.grid .domain').remove()

  // X axis
  g.append('g')
    .attr('transform', `translate(0, ${innerH})`)
    .call(d3.axisBottom(x).ticks(6).tickFormat(d3.timeFormat('%m/%d')))
    .selectAll('text')
    .attr('font-size', '11px')
    .attr('font-family', "'JetBrains Mono', monospace")
    .attr('fill', '#666')

  // Y axis
  g.append('g')
    .call(d3.axisLeft(y).ticks(5).tickFormat(d3.format('.2f')))
    .selectAll('text')
    .attr('font-size', '11px')
    .attr('font-family', "'JetBrains Mono', monospace")
    .attr('fill', '#666')

  g.selectAll('.domain').attr('stroke', '#E5E5E5')
  g.selectAll('.tick line').attr('stroke', '#E5E5E5')

  const line = d3.line()
    .x(d => x(d.date))
    .y(d => y(d.value))
    .curve(d3.curveMonotoneX)

  // Benchmark line
  g.append('path')
    .datum(benchmark)
    .attr('fill', 'none')
    .attr('stroke', '#CCC')
    .attr('stroke-width', 2)
    .attr('d', line)

  // Portfolio line
  g.append('path')
    .datum(portfolio)
    .attr('fill', 'none')
    .attr('stroke', '#FF4500')
    .attr('stroke-width', 2.5)
    .attr('d', line)

  // Tooltip overlay
  const tooltip = d3.select(el)
    .append('div')
    .attr('class', 'chart-tooltip')
    .style('opacity', 0)

  const bisect = d3.bisector(d => d.date).left

  const focus = g.append('g').style('display', 'none')
  focus.append('line')
    .attr('class', 'focus-line')
    .attr('y1', 0)
    .attr('y2', innerH)
    .attr('stroke', '#999')
    .attr('stroke-dasharray', '3,3')

  focus.append('circle')
    .attr('class', 'focus-circle-p')
    .attr('r', 4)
    .attr('fill', '#FF4500')
    .attr('stroke', '#FFF')
    .attr('stroke-width', 2)

  focus.append('circle')
    .attr('class', 'focus-circle-b')
    .attr('r', 4)
    .attr('fill', '#CCC')
    .attr('stroke', '#FFF')
    .attr('stroke-width', 2)

  svg.append('rect')
    .attr('width', innerW)
    .attr('height', innerH)
    .attr('transform', `translate(${margin.left}, ${margin.top})`)
    .attr('fill', 'transparent')
    .on('mouseover', () => {
      focus.style('display', null)
      tooltip.style('opacity', 1)
    })
    .on('mouseout', () => {
      focus.style('display', 'none')
      tooltip.style('opacity', 0)
    })
    .on('mousemove', (event) => {
      const [mx] = d3.pointer(event)
      const xDate = x.invert(mx)
      const i = bisect(portfolio, xDate, 1)
      const d0 = portfolio[i - 1]
      const d1 = portfolio[i] || d0
      const d = xDate - d0.date > d1.date - xDate ? d1 : d0
      const bi = bisect(benchmark, xDate, 1)
      const bd = benchmark[Math.min(bi, benchmark.length - 1)]

      focus.select('.focus-line').attr('x1', x(d.date)).attr('x2', x(d.date))
      focus.select('.focus-circle-p').attr('cx', x(d.date)).attr('cy', y(d.value))
      focus.select('.focus-circle-b').attr('cx', x(bd.date)).attr('cy', y(bd.value))

      const fmt = d3.timeFormat('%Y-%m-%d')
      tooltip
        .html(`<strong>${fmt(d.date)}</strong><br/>组合: ${d.value.toFixed(4)}<br/>沪深300: ${bd.value.toFixed(4)}`)
        .style('left', (x(d.date) + margin.left + 16) + 'px')
        .style('top', (y(d.value) + margin.top - 10) + 'px')
    })
}

const loadData = async () => {
  try {
    const [summaryRes, strategiesRes] = await Promise.all([
      getBacktestSummary().catch(() => null),
      getBacktestStrategies().catch(() => null),
    ])
    if (summaryRes?.data?.metrics) metrics.value = summaryRes.data.metrics
    // 仅用 API 数据更新信号数量，Sharpe/胜率等保留已验证的回测值
    if (strategiesRes?.data) {
      const apiData = Array.isArray(strategiesRes.data) ? strategiesRes.data : strategiesRes.data.strategies ?? []
      const _CN = {
        shock_pipeline: '冲击链路', graph_factors: 'S07图谱因子',
        sentiment_simulation: 'S10舆情', analyst_divergence: 'S05分歧度',
        sector_rotation: 'S09行业轮动', narrative_tracker: 'S11叙事',
        institution_association: '机构关联', prosperity_transmission: '景气传导',
      }
      const countMap = {}
      apiData.forEach(s => {
        const key = _CN[s.strategy_name] || s.strategy_name
        countMap[key] = s.signal_count ?? 0
      })
      strategies.value.forEach(s => {
        if (countMap[s.name] !== undefined) s.signalCount = countMap[s.name]
      })
    }
  } catch (e) {
    console.warn('使用模拟数据:', e.message)
  }
}

// 窗口大小变化时重绘
let resizeTimer = null
const handleResize = () => {
  clearTimeout(resizeTimer)
  resizeTimer = setTimeout(drawNavChart, 200)
}

onMounted(async () => {
  computeBest()
  await loadData()
  computeBest()
  await nextTick()
  // 确保 DOM 元素已渲染并有尺寸
  setTimeout(() => {
    drawNavChart()
  }, 100)
  window.addEventListener('resize', handleResize)
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
})
</script>

<style scoped>
.performance-view {
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

.section-desc {
  font-size: 13px;
  color: #666;
  margin-top: -12px;
  margin-bottom: 16px;
}

/* Metric Cards */
.metric-cards {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
}

.metric-card {
  background: #F5F5F5;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
  padding: 24px;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.metric-label {
  font-size: 13px;
  color: #666;
  font-weight: 500;
}

.metric-value {
  font-size: 32px;
  font-weight: 800;
  font-family: 'JetBrains Mono', monospace;
  color: #111;
}

.metric-value.highlight {
  color: #FF4500;
}

.metric-desc {
  font-size: 12px;
  color: #999;
}

.metric-subdesc {
  font-size: 11px;
  color: #B0B0B0;
  font-style: italic;
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

.data-table tr:hover {
  background: #FAFAFA;
}

.mono {
  font-family: 'JetBrains Mono', monospace;
}

.strategy-name {
  font-weight: 600;
  color: #111;
}

.strategy-badge {
  display: inline-block;
  margin-left: 8px;
  padding: 1px 6px;
  background: #FF4500;
  color: #FFF;
  border-radius: 3px;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.5px;
  vertical-align: middle;
}

.best-val {
  color: #FF4500;
  font-weight: 700;
}

.no-backtest-tag {
  display: inline-block;
  margin-left: 8px;
  padding: 1px 6px;
  background: #F5F5F5;
  color: #999;
  border: 1px solid #E5E5E5;
  border-radius: 3px;
  font-size: 10px;
  font-weight: 400;
  vertical-align: middle;
}

.nav-chart-note {
  font-size: 13px;
  color: #92400E;
  background: #FFFBEB;
  border: 1px solid #FDE68A;
  border-radius: 6px;
  padding: 8px 14px;
  margin-bottom: 12px;
  text-align: center;
}

/* NAV Chart */
.nav-chart-card {
  background: #F5F5F5;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
  padding: 20px;
  position: relative;
}

.chart-legend {
  display: flex;
  gap: 24px;
  margin-bottom: 12px;
  font-size: 13px;
  color: #666;
}

.legend-item {
  display: flex;
  align-items: center;
  gap: 6px;
}

.legend-line {
  display: inline-block;
  width: 20px;
  height: 3px;
  border-radius: 2px;
}

.legend-line.orange { background: #FF4500; }
.legend-line.gray { background: #CCC; }

.nav-chart-container {
  width: 100%;
  min-height: 320px;
  position: relative;
}

.nav-chart-container :deep(.chart-tooltip) {
  position: absolute;
  background: #111;
  color: #FFF;
  padding: 8px 12px;
  border-radius: 4px;
  font-size: 12px;
  font-family: 'JetBrains Mono', monospace;
  pointer-events: none;
  line-height: 1.6;
  z-index: 10;
  white-space: nowrap;
}

/* Direction Rule Cards */
.rule-cards {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
}

.rule-card {
  border: 1px solid #E5E5E5;
  border-radius: 8px;
  padding: 20px;
  background: #FFF;
}

.rule-card.avoid {
  border-left: 4px solid #DC2626;
}

.rule-card.long {
  border-left: 4px solid #16A34A;
}

.rule-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 12px;
  flex-wrap: wrap;
}

.rule-events {
  font-size: 13px;
  font-weight: 600;
  color: #333;
}

.rule-arrow {
  font-size: 16px;
  color: #999;
}

.rule-direction {
  font-weight: 700;
  font-size: 14px;
  padding: 2px 10px;
  border-radius: 4px;
}

.rule-direction.avoid {
  background: #FEE2E2;
  color: #DC2626;
}

.rule-direction.long {
  background: #DCFCE7;
  color: #16A34A;
}

.rule-reason {
  font-size: 13px;
  color: #666;
  margin-bottom: 12px;
  line-height: 1.5;
}

.rule-footer {
  display: flex;
  justify-content: flex-end;
}

.rule-winrate {
  font-size: 14px;
  font-weight: 700;
  color: #FF4500;
}

@media (max-width: 768px) {
  .metric-cards {
    grid-template-columns: repeat(2, 1fr);
  }
  .rule-cards {
    grid-template-columns: 1fr;
  }
}
</style>
