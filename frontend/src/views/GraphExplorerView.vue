<template>
  <div class="graph-explorer">
    <!-- 关系类型过滤条 -->
    <div class="filter-bar">
      <span class="filter-label">关系类型</span>
      <div class="filter-chips">
        <button
          v-for="rel in relationTypes"
          :key="rel.key"
          class="filter-chip"
          :class="{ active: activeRelations.has(rel.key) }"
          @click="toggleRelation(rel.key)"
        >
          <span class="chip-dot" :style="{ background: rel.color }"></span>
          {{ rel.label }}
        </button>
      </div>
      <div class="filter-stats">
        <span class="stat-item">节点 {{ displayNodeCount }}</span>
        <span class="stat-sep">|</span>
        <span class="stat-item">边 {{ displayEdgeCount }}</span>
        <span class="stat-sep">|</span>
        <span class="stat-item">行业 {{ industryCount }}</span>
      </div>
    </div>

    <!-- 主体区域 -->
    <div class="graph-body">
      <!-- 图谱可视化 -->
      <div class="graph-main" :class="{ 'with-panel': selectedNode }">
        <div class="graph-canvas" ref="graphContainer">
          <svg ref="graphSvg"></svg>
        </div>

        <!-- 加载状态 -->
        <div v-if="loading" class="graph-loading">
          <div class="loading-spinner"></div>
          <span>图谱加载中...</span>
        </div>

        <!-- 空状态 -->
        <div v-if="!loading && displayNodes.length === 0" class="graph-empty">
          <div class="empty-icon">&#x2726;</div>
          <p>暂无图谱数据</p>
        </div>

        <!-- 图例 -->
        <div class="graph-legend" v-if="displayNodes.length > 0">
          <div class="legend-section">
            <span class="legend-section-title">关系</span>
            <div class="legend-item" v-for="rel in relationTypes" :key="rel.key">
              <span class="legend-line" :style="{ background: rel.color }"></span>
              <span class="legend-text">{{ rel.label }}</span>
            </div>
          </div>
          <div class="legend-divider"></div>
          <div class="legend-section">
            <span class="legend-section-title">行业</span>
            <div class="legend-item" v-for="ind in topIndustries" :key="ind.name">
              <span class="legend-dot" :style="{ background: ind.color }"></span>
              <span class="legend-text">{{ ind.name }}</span>
            </div>
          </div>
        </div>

        <!-- 搜索栏 -->
        <div class="graph-search">
          <input
            v-model="searchQuery"
            type="text"
            placeholder="搜索公司名称或代码..."
            @keydown.enter="handleSearch"
          />
          <button class="search-btn" @click="handleSearch" :disabled="!searchQuery.trim()">
            搜索
          </button>
        </div>
      </div>

      <!-- 节点详情面板 -->
      <transition name="panel-slide">
        <div class="detail-panel" v-if="selectedNode">
          <div class="panel-header">
            <div class="panel-title-row">
              <span class="panel-title">{{ selectedNode.displayName || selectedNode.name }}</span>
              <span class="panel-code">{{ selectedNode.code }}</span>
              <span class="panel-industry" v-if="selectedNode.industry">{{ selectedNode.industry }}</span>
            </div>
            <button class="panel-close" @click="closePanel">&times;</button>
          </div>

          <div class="panel-body">
            <!-- 基本信息 -->
            <div class="info-section">
              <div class="info-row">
                <span class="info-label">行业</span>
                <span class="info-value">{{ selectedNode.industry || '未知' }}</span>
              </div>
              <div class="info-row">
                <span class="info-label">连接数</span>
                <span class="info-value signal-count">{{ selectedNode.edgeCount || 0 }}</span>
              </div>
              <div class="info-row">
                <span class="info-label">活跃信号</span>
                <span class="info-value signal-count">{{ selectedNode.signalCount || 0 }}</span>
              </div>
            </div>

            <!-- 活跃信号列表 -->
            <div class="panel-section" v-if="selectedNode.signals && selectedNode.signals.length">
              <div class="section-title">活跃信号</div>
              <div class="signal-list">
                <router-link
                  v-for="sig in selectedNode.signals"
                  :key="sig.id"
                  :to="`/signals/${sig.id}`"
                  class="signal-item"
                >
                  <span class="signal-dir" :class="sig.direction">{{ sig.direction === 'long' ? '做多' : '回避' }}</span>
                  <span class="signal-name">{{ sig.title }}</span>
                  <span class="signal-arrow">&rarr;</span>
                </router-link>
              </div>
            </div>

            <!-- 相邻节点 -->
            <div class="panel-section" v-if="neighborGroups.length">
              <div class="section-title">相邻公司</div>
              <div v-for="group in neighborGroups" :key="group.type" class="neighbor-group">
                <div class="neighbor-group-title">
                  <span class="neighbor-dot" :style="{ background: group.color }"></span>
                  {{ group.label }} ({{ group.nodes.length }})
                </div>
                <div class="neighbor-list">
                  <button
                    v-for="nb in group.nodes"
                    :key="nb.id"
                    class="neighbor-item"
                    @click="selectNodeById(nb.id)"
                  >
                    {{ nb.display_name || nb.name }}
                    <span class="neighbor-code">{{ nb.code }}</span>
                  </button>
                </div>
              </div>
            </div>

            <!-- 查看传播路径 -->
            <div class="panel-actions">
              <button class="action-btn" @click="viewPropagation">
                查看传播路径
              </button>
            </div>
          </div>
        </div>
      </transition>
    </div>
  </div>
</template>

<script setup>
import { ref, reactive, computed, onMounted, onUnmounted, watch, nextTick } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import * as d3 from 'd3'
import { getGraphData, getGraphStats, getGraphNodeNeighbors } from '../api/astrategy.js'

const router = useRouter()
const route = useRoute()

// ── 关系类型定义 ──
const relationTypes = [
  { key: 'SUPPLIES_TO', label: '供应链', color: '#FF6B35' },
  { key: 'CUSTOMER_OF', label: '客户', color: '#004E89' },
  { key: 'COMPETES_WITH', label: '竞争', color: '#C5283D' },
  { key: 'COOPERATES_WITH', label: '合作', color: '#1A936F' },
  { key: 'HOLDS_SHARES', label: '基金持仓', color: '#7B2D8E' },
  { key: 'BELONGS_TO', label: '行业链', color: '#9b59b6' },
]

const relationColorMap = {}
relationTypes.forEach(r => { relationColorMap[r.key] = r.color })

// ── 行业颜色调色板 ──
const industryPalette = [
  '#FF6B35', '#004E89', '#1A936F', '#C5283D', '#9b59b6',
  '#E9724C', '#3498db', '#f39c12', '#2ecc71', '#e74c3c',
  '#1abc9c', '#8e44ad', '#d35400', '#27ae60', '#2980b9',
  '#c0392b', '#16a085', '#7f8c8d', '#f1c40f', '#e67e22',
]
const industryColorMap = ref({})
let industryColorIdx = 0

function getIndustryColor(industry) {
  if (!industry) return '#999'
  if (industryColorMap.value[industry]) return industryColorMap.value[industry]
  const color = industryPalette[industryColorIdx % industryPalette.length]
  industryColorIdx++
  industryColorMap.value[industry] = color
  return color
}

// ── 状态 ──
const activeRelations = ref(new Set(relationTypes.map(r => r.key)))
const loading = ref(false)
const searchQuery = ref('')
const selectedNode = ref(null)
const graphStats = ref(null)
const nodes = ref([])
const edges = ref([])
const neighborGroups = ref([])

const graphContainer = ref(null)
const graphSvg = ref(null)

let simulation = null
let svgSelection = null
let gRoot = null
let nodeElements = null
let linkElements = null
let labelElements = null
let zoomBehavior = null

// ── 计算每个节点的边数 ──
const nodeEdgeCounts = computed(() => {
  const counts = {}
  edges.value.forEach(e => {
    const src = typeof e.source === 'object' ? e.source.id : e.source
    const tgt = typeof e.target === 'object' ? e.target.id : e.target
    counts[src] = (counts[src] || 0) + 1
    counts[tgt] = (counts[tgt] || 0) + 1
  })
  return counts
})

// ── 过滤后的节点：去掉 inst:: 前缀节点，只保留有边的节点 ──
const displayNodes = computed(() => {
  const counts = nodeEdgeCounts.value
  return nodes.value.filter(n => {
    // 过滤 inst:: 开头的节点
    if (n.name && n.name.startsWith('inst::')) return false
    if (n.id && String(n.id).startsWith('inst::')) return false
    // 只保留有边的节点
    return (counts[n.id] || 0) > 0
  })
})

// ── 统计 ──
const displayNodeCount = computed(() => displayNodes.value.length)
const displayEdgeCount = computed(() => filteredEdges.value.length)
const industryCount = computed(() => {
  const industries = new Set()
  displayNodes.value.forEach(n => {
    if (n.industry) industries.add(n.industry)
  })
  return industries.size
})

// ── 图例用: 前几个行业 ──
const topIndustries = computed(() => {
  const counts = {}
  displayNodes.value.forEach(n => {
    if (n.industry) {
      counts[n.industry] = (counts[n.industry] || 0) + 1
    }
  })
  return Object.entries(counts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .map(([name]) => ({ name, color: getIndustryColor(name) }))
})

// ── 过滤关系 ──
function toggleRelation(key) {
  const s = new Set(activeRelations.value)
  if (s.has(key)) {
    s.delete(key)
  } else {
    s.add(key)
  }
  activeRelations.value = s
}

// ── 面板操作 ──
function closePanel() {
  selectedNode.value = null
  neighborGroups.value = []
  // 取消高亮
  if (nodeElements) {
    nodeElements.attr('stroke', '#fff').attr('stroke-width', 2)
  }
  if (linkElements) {
    linkElements.attr('stroke-opacity', 0.6)
  }
}

async function selectNodeById(id) {
  const node = nodes.value.find(n => n.id === id)
  if (node) {
    selectedNode.value = node
    highlightNode(node)
    await loadNeighbors(node.id)
  }
}

function viewPropagation() {
  if (selectedNode.value) {
    router.push(`/signals?source=${selectedNode.value.code}`)
  }
}

async function loadNeighbors(nodeId) {
  try {
    const resp = await getGraphNodeNeighbors(nodeId)
    const data = resp.data || resp
    const groups = {}
    const neighbors = data.neighbors || data || []
    neighbors.forEach(nb => {
      const rel = nb.relation || 'UNKNOWN'
      if (!groups[rel]) {
        const rt = relationTypes.find(r => r.key === rel)
        groups[rel] = {
          type: rel,
          label: rt ? rt.label : rel,
          color: relationColorMap[rel] || '#999',
          nodes: [],
        }
      }
      groups[rel].nodes.push(nb)
    })
    neighborGroups.value = Object.values(groups)
  } catch {
    neighborGroups.value = []
  }
}

// ── 数据加载 ──
async function fetchData() {
  loading.value = true
  try {
    const relTypes = [...activeRelations.value]
    const resp = await getGraphData({ relation_types: relTypes.join(','), limit: 200 })
    const data = resp.data || resp
    nodes.value = (data.nodes || []).map(n => ({
      id: n.id || n.code,
      name: n.name,
      displayName: n.display_name || n.name,
      code: n.code || n.id,
      industry: n.industry || '',
      pagerank: n.pagerank || n.degree || 1,
      signalCount: n.signal_count || 0,
      signals: n.signals || [],
    }))
    edges.value = (data.edges || []).map(e => ({
      source: e.source,
      target: e.target,
      relation: e.relation || e.type || 'UNKNOWN',
      weight: e.weight || 1,
      color: relationColorMap[e.relation || e.type] || '#999',
    }))
    // 预计算行业颜色
    industryColorIdx = 0
    industryColorMap.value = {}
    nodes.value.forEach(n => {
      if (n.industry) getIndustryColor(n.industry)
    })
  } catch {
    nodes.value = []
    edges.value = []
  } finally {
    loading.value = false
  }
}

async function fetchStats() {
  try {
    const resp = await getGraphStats()
    graphStats.value = resp.data || resp
  } catch {
    graphStats.value = null
  }
}

// ── 过滤后的边（也过滤 inst:: 端点） ──
const filteredEdges = computed(() => {
  const validIds = new Set(displayNodes.value.map(n => n.id))
  return edges.value.filter(e => {
    if (!activeRelations.value.has(e.relation)) return false
    const src = typeof e.source === 'object' ? e.source.id : e.source
    const tgt = typeof e.target === 'object' ? e.target.id : e.target
    return validIds.has(src) && validIds.has(tgt)
  })
})

// ── 搜索 ──
function handleSearch() {
  const q = searchQuery.value.trim().toLowerCase()
  if (!q) return
  const found = nodes.value.find(
    n => (n.displayName || n.name).toLowerCase().includes(q) || n.code.toLowerCase().includes(q)
  )
  if (found && gRoot && zoomBehavior && svgSelection) {
    // 居中并高亮
    const width = graphContainer.value.clientWidth
    const height = graphContainer.value.clientHeight
    const transform = d3.zoomIdentity
      .translate(width / 2 - found.x, height / 2 - found.y)
    svgSelection.transition().duration(600).call(zoomBehavior.transform, transform)
    selectedNode.value = found
    highlightNode(found)
    loadNeighbors(found.id)
  }
}

// ── 高亮节点 ──
function highlightNode(node) {
  if (!nodeElements || !linkElements) return
  // 重置
  nodeElements.attr('stroke', '#fff').attr('stroke-width', 2)
  linkElements.attr('stroke-opacity', 0.3)
  // 高亮选中
  nodeElements.filter(d => d.id === node.id)
    .attr('stroke', '#FF4500').attr('stroke-width', 3.5)
  // 高亮相连边和相连节点
  const connectedIds = new Set()
  linkElements.filter(d => {
    const srcId = typeof d.source === 'object' ? d.source.id : d.source
    const tgtId = typeof d.target === 'object' ? d.target.id : d.target
    if (srcId === node.id) { connectedIds.add(tgtId); return true }
    if (tgtId === node.id) { connectedIds.add(srcId); return true }
    return false
  }).attr('stroke-opacity', 1)
  // 高亮相连节点
  nodeElements.filter(d => connectedIds.has(d.id))
    .attr('stroke', '#FF8C00').attr('stroke-width', 2.5)
}

// ── D3 渲染 ──
function renderGraph() {
  if (!graphSvg.value || !graphContainer.value) return

  const width = graphContainer.value.clientWidth
  const height = graphContainer.value.clientHeight
  if (width === 0 || height === 0) return

  // 清理
  if (simulation) simulation.stop()
  const svg = d3.select(graphSvg.value)
  svg.selectAll('*').remove()
  svg.attr('width', width).attr('height', height)
  svgSelection = svg

  // 使用过滤后的节点和边
  const counts = nodeEdgeCounts.value
  const nodeData = displayNodes.value.map(n => ({
    ...n,
    edgeCount: counts[n.id] || 0,
  }))
  const validNodeIds = new Set(nodeData.map(n => n.id))
  const edgeData = filteredEdges.value
    .filter(e => {
      const src = typeof e.source === 'object' ? e.source.id : e.source
      const tgt = typeof e.target === 'object' ? e.target.id : e.target
      return validNodeIds.has(src) && validNodeIds.has(tgt)
    })
    .map(e => ({ ...e }))

  if (nodeData.length === 0) return

  // 节点大小比例 (基于边数)
  const radiusScale = (edgeCount) => Math.max(8, Math.min(30, edgeCount * 3))

  // 箭头 marker
  const defs = svg.append('defs')
  relationTypes.forEach(rel => {
    defs.append('marker')
      .attr('id', `arrow-${rel.key}`)
      .attr('viewBox', '0 0 10 6')
      .attr('refX', 10)
      .attr('refY', 3)
      .attr('markerWidth', 8)
      .attr('markerHeight', 5)
      .attr('orient', 'auto')
      .append('path')
      .attr('d', 'M0,0 L10,3 L0,6 Z')
      .attr('fill', rel.color)
  })

  const g = svg.append('g')
  gRoot = g

  // zoom
  zoomBehavior = d3.zoom()
    .scaleExtent([0.1, 5])
    .on('zoom', (event) => { g.attr('transform', event.transform) })
  svg.call(zoomBehavior)

  // 边
  linkElements = g.append('g').attr('class', 'links')
    .selectAll('line')
    .data(edgeData)
    .enter().append('line')
    .attr('stroke', d => d.color)
    .attr('stroke-width', d => Math.max(1, Math.min(3, d.weight)))
    .attr('stroke-opacity', 0.6)
    .attr('marker-end', d => `url(#arrow-${d.relation})`)

  // 节点 — 颜色按行业
  nodeElements = g.append('g').attr('class', 'nodes')
    .selectAll('circle')
    .data(nodeData)
    .enter().append('circle')
    .attr('r', d => radiusScale(d.edgeCount))
    .attr('fill', d => getIndustryColor(d.industry))
    .attr('stroke', '#fff')
    .attr('stroke-width', 2)
    .attr('opacity', 0.9)
    .style('cursor', 'pointer')
    .call(d3.drag()
      .on('start', (event, d) => {
        if (!event.active) simulation.alphaTarget(0.3).restart()
        d.fx = d.x; d.fy = d.y
      })
      .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y })
      .on('end', (event, d) => {
        if (!event.active) simulation.alphaTarget(0)
        d.fx = null; d.fy = null
      })
    )
    .on('click', async (event, d) => {
      event.stopPropagation()
      selectedNode.value = d
      highlightNode(d)
      await loadNeighbors(d.id)
    })
    .on('mouseenter', (event, d) => {
      if (!selectedNode.value || selectedNode.value.id !== d.id) {
        d3.select(event.target).attr('stroke', '#FF4500').attr('stroke-width', 2.5)
      }
    })
    .on('mouseleave', (event, d) => {
      if (!selectedNode.value || selectedNode.value.id !== d.id) {
        d3.select(event.target).attr('stroke', '#fff').attr('stroke-width', 2)
      }
    })

  // 标签 — 使用 display_name，只给大节点显示标签
  labelElements = g.append('g').attr('class', 'labels')
    .selectAll('text')
    .data(nodeData)
    .enter().append('text')
    .text(d => d.displayName || d.name)
    .attr('font-size', d => d.edgeCount >= 3 ? '11px' : '9px')
    .attr('fill', '#333')
    .attr('font-weight', d => d.edgeCount >= 5 ? '600' : '400')
    .attr('font-family', "'Noto Sans SC', 'Space Grotesk', sans-serif")
    .attr('dx', d => radiusScale(d.edgeCount) + 4)
    .attr('dy', 3)
    .attr('display', d => d.edgeCount >= 2 ? 'block' : 'none')
    .style('pointer-events', 'none')

  // 仿真 — 增大 link distance
  simulation = d3.forceSimulation(nodeData)
    .force('link', d3.forceLink(edgeData).id(d => d.id).distance(180))
    .force('charge', d3.forceManyBody().strength(-400))
    .force('center', d3.forceCenter(width / 2, height / 2))
    .force('collide', d3.forceCollide(d => radiusScale(d.edgeCount) + 12))
    .force('x', d3.forceX(width / 2).strength(0.03))
    .force('y', d3.forceY(height / 2).strength(0.03))

  simulation.on('tick', () => {
    linkElements
      .attr('x1', d => d.source.x)
      .attr('y1', d => d.source.y)
      .attr('x2', d => {
        const dx = d.target.x - d.source.x
        const dy = d.target.y - d.source.y
        const dist = Math.sqrt(dx * dx + dy * dy) || 1
        const r = radiusScale(d.target.edgeCount || 0) + 4
        return d.target.x - (dx / dist) * r
      })
      .attr('y2', d => {
        const dx = d.target.x - d.source.x
        const dy = d.target.y - d.source.y
        const dist = Math.sqrt(dx * dx + dy * dy) || 1
        const r = radiusScale(d.target.edgeCount || 0) + 4
        return d.target.y - (dy / dist) * r
      })

    nodeElements
      .attr('cx', d => d.x)
      .attr('cy', d => d.y)

    labelElements
      .attr('x', d => d.x)
      .attr('y', d => d.y)
  })

  // 点击空白取消选中
  svg.on('click', () => {
    closePanel()
  })

  // 初始缩放适配
  if (nodeData.length > 20) {
    const initialScale = Math.max(0.4, Math.min(1, 30 / nodeData.length * 2))
    const initialTransform = d3.zoomIdentity
      .translate(width / 2, height / 2)
      .scale(initialScale)
      .translate(-width / 2, -height / 2)
    svg.call(zoomBehavior.transform, initialTransform)
  }
}

// ── 监听过滤变化重新渲染 ──
watch(activeRelations, () => {
  nextTick(renderGraph)
}, { deep: true })

// ── 生命周期 ──
const handleResize = () => { nextTick(renderGraph) }

onMounted(async () => {
  window.addEventListener('resize', handleResize)
  await Promise.all([fetchData(), fetchStats()])
  await nextTick()
  renderGraph()
  // 如果 URL 带 focus 参数，自动搜索并定位
  if (route.query.focus) {
    searchQuery.value = route.query.focus
    // 等待仿真稳定后再搜索定位
    setTimeout(() => { handleSearch() }, 800)
  }
})

onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
  if (simulation) simulation.stop()
})
</script>

<style scoped>
.graph-explorer {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: #F5F5F5;
  font-family: 'Noto Sans SC', 'Space Grotesk', sans-serif;
}

/* ── 过滤条 ── */
.filter-bar {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 14px 24px;
  background: #fff;
  border-bottom: 1px solid #E5E5E5;
  flex-shrink: 0;
}

.filter-label {
  font-size: 13px;
  font-weight: 600;
  color: #333;
  white-space: nowrap;
}

.filter-chips {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.filter-chip {
  display: inline-flex;
  align-items: center;
  gap: 6px;
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

.filter-chip:hover {
  border-color: #ccc;
  background: #fafafa;
}

.filter-chip.active {
  background: #111;
  color: #fff;
  border-color: #111;
}

.chip-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
}

.filter-stats {
  margin-left: auto;
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 12px;
  color: #999;
  font-family: 'JetBrains Mono', monospace;
}

.stat-sep {
  color: #ddd;
}

/* ── 主体 ── */
.graph-body {
  flex: 1;
  display: flex;
  position: relative;
  overflow: hidden;
}

.graph-main {
  flex: 1;
  position: relative;
  transition: margin-right 0.3s ease;
}

.graph-main.with-panel {
  margin-right: 320px;
}

.graph-canvas {
  width: 100%;
  height: 100%;
  background-color: #FAFAFA;
  background-image: radial-gradient(#E0E0E0 1px, transparent 1px);
  background-size: 20px 20px;
}

.graph-canvas svg {
  width: 100%;
  height: 100%;
  display: block;
}

/* ── 加载 / 空状态 ── */
.graph-loading,
.graph-empty {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  text-align: center;
  color: #999;
  font-size: 14px;
}

.loading-spinner {
  width: 36px;
  height: 36px;
  border: 3px solid #E5E5E5;
  border-top-color: #FF4500;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
  margin: 0 auto 12px;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.empty-icon {
  font-size: 48px;
  opacity: 0.15;
  margin-bottom: 12px;
}

/* ── 图例 ── */
.graph-legend {
  position: absolute;
  bottom: 64px;
  left: 20px;
  background: rgba(255, 255, 255, 0.95);
  border: 1px solid #E5E5E5;
  border-radius: 8px;
  padding: 10px 14px;
  display: flex;
  flex-direction: column;
  gap: 8px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
  max-width: 260px;
}

.legend-section {
  display: flex;
  flex-wrap: wrap;
  gap: 6px 14px;
  align-items: center;
}

.legend-section-title {
  font-size: 10px;
  font-weight: 600;
  color: #999;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  width: 100%;
  margin-bottom: 2px;
}

.legend-divider {
  height: 1px;
  background: #E5E5E5;
}

.legend-item {
  display: flex;
  align-items: center;
  gap: 5px;
}

.legend-line {
  width: 16px;
  height: 3px;
  border-radius: 1px;
}

.legend-dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  flex-shrink: 0;
}

.legend-text {
  font-size: 11px;
  color: #666;
}

/* ── 搜索栏 ── */
.graph-search {
  position: absolute;
  bottom: 16px;
  left: 50%;
  transform: translateX(-50%);
  display: flex;
  gap: 0;
  width: 380px;
  max-width: calc(100% - 40px);
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
  border-radius: 10px;
  overflow: hidden;
}

.graph-search input {
  flex: 1;
  padding: 10px 16px;
  border: 1px solid #E5E5E5;
  border-right: none;
  border-radius: 10px 0 0 10px;
  font-size: 13px;
  font-family: inherit;
  outline: none;
  background: #fff;
  color: #333;
}

.graph-search input::placeholder {
  color: #bbb;
}

.graph-search input:focus {
  border-color: #FF4500;
}

.search-btn {
  padding: 10px 20px;
  background: #111;
  color: #fff;
  border: none;
  border-radius: 0 10px 10px 0;
  font-size: 13px;
  font-family: inherit;
  cursor: pointer;
  transition: background 0.2s;
}

.search-btn:hover:not(:disabled) {
  background: #333;
}

.search-btn:disabled {
  opacity: 0.5;
  cursor: default;
}

/* ── 详情面板 ── */
.detail-panel {
  position: absolute;
  top: 0;
  right: 0;
  width: 320px;
  height: 100%;
  background: #fff;
  border-left: 1px solid #E5E5E5;
  display: flex;
  flex-direction: column;
  z-index: 20;
  box-shadow: -4px 0 16px rgba(0, 0, 0, 0.05);
}

.panel-slide-enter-active,
.panel-slide-leave-active {
  transition: transform 0.3s ease;
}

.panel-slide-enter-from,
.panel-slide-leave-to {
  transform: translateX(100%);
}

.panel-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  padding: 20px 20px 16px;
  border-bottom: 1px solid #F0F0F0;
  flex-shrink: 0;
}

.panel-title-row {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.panel-title {
  font-size: 16px;
  font-weight: 600;
  color: #111;
  font-family: 'Noto Sans SC', 'Space Grotesk', sans-serif;
}

.panel-code {
  font-size: 12px;
  color: #999;
  font-family: 'JetBrains Mono', monospace;
}

.panel-industry {
  font-size: 11px;
  color: #666;
  background: #F0F0F0;
  padding: 2px 8px;
  border-radius: 4px;
  display: inline-block;
  width: fit-content;
  margin-top: 2px;
}

.panel-close {
  background: none;
  border: none;
  font-size: 22px;
  color: #bbb;
  cursor: pointer;
  line-height: 1;
  padding: 0;
  transition: color 0.2s;
}

.panel-close:hover {
  color: #333;
}

.panel-body {
  flex: 1;
  overflow-y: auto;
  padding: 16px 20px;
}

/* ── 基本信息 ── */
.info-section {
  display: flex;
  flex-direction: column;
  gap: 10px;
  margin-bottom: 20px;
}

.info-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.info-label {
  font-size: 12px;
  color: #999;
}

.info-value {
  font-size: 13px;
  color: #333;
  font-weight: 500;
}

.info-value.signal-count {
  color: #FF4500;
  font-family: 'JetBrains Mono', monospace;
  font-weight: 600;
}

/* ── 分区 ── */
.panel-section {
  margin-bottom: 20px;
}

.section-title {
  font-size: 11px;
  font-weight: 600;
  color: #999;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  margin-bottom: 10px;
  padding-bottom: 6px;
  border-bottom: 1px solid #F0F0F0;
}

/* ── 信号列表 ── */
.signal-list {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.signal-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 10px;
  border-radius: 6px;
  background: #FAFAFA;
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

.signal-name {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.signal-arrow {
  color: #ccc;
  flex-shrink: 0;
}

/* ── 相邻节点 ── */
.neighbor-group {
  margin-bottom: 12px;
}

.neighbor-group-title {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  font-weight: 500;
  color: #555;
  margin-bottom: 6px;
}

.neighbor-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex-shrink: 0;
}

.neighbor-list {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
}

.neighbor-item {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 4px 10px;
  border: 1px solid #E5E5E5;
  border-radius: 14px;
  background: #fff;
  font-size: 11px;
  color: #333;
  cursor: pointer;
  transition: all 0.2s;
  font-family: inherit;
}

.neighbor-item:hover {
  border-color: #FF4500;
  color: #FF4500;
}

.neighbor-code {
  font-family: 'JetBrains Mono', monospace;
  color: #bbb;
  font-size: 10px;
}

/* ── 操作按钮 ── */
.panel-actions {
  margin-top: 20px;
  padding-top: 16px;
  border-top: 1px solid #F0F0F0;
}

.action-btn {
  width: 100%;
  padding: 10px;
  background: #111;
  color: #fff;
  border: none;
  border-radius: 8px;
  font-size: 13px;
  font-family: inherit;
  cursor: pointer;
  transition: background 0.2s;
}

.action-btn:hover {
  background: #333;
}
</style>
