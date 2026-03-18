<template>
  <div class="signal-trace">
    <!-- Header -->
    <header class="st-header">
      <div class="brand" @click="$router.push('/')">MIROFISH</div>
      <h1 class="page-title">信号链路追踪</h1>
      <div class="header-actions">
        <router-link to="/signals" class="nav-link">返回信号中心</router-link>
        <router-link to="/" class="nav-link">控制台</router-link>
      </div>
    </header>

    <main class="st-content">
      <!-- Loading -->
      <div v-if="loading" class="loading-state">
        <div class="spinner"></div>
        <span>加载信号链路...</span>
      </div>

      <!-- Error -->
      <div v-else-if="error" class="error-state">
        <div class="error-content">
          <div class="error-icon">!</div>
          <span class="error-msg">{{ error }}</span>
          <span class="error-id mono" v-if="requestedSignalId">请求的信号ID: {{ requestedSignalId }}</span>
          <div class="error-actions">
            <button class="btn-retry" @click="loadTrace">重试</button>
            <button class="btn-back" @click="$router.push('/signals')">返回信号中心</button>
          </div>
        </div>
      </div>

      <!-- Trace Cards -->
      <template v-else-if="trace">
        <!-- Card 1: 触发事件 -->
        <div class="trace-card">
          <div class="card-step">
            <span class="step-number">1</span>
            <span class="step-label">{{ hasShockPipeline ? '触发事件' : '信号来源' }}</span>
          </div>
          <div class="card-body">
            <!-- 冲击链路信号：展示事件详情 -->
            <template v-if="hasShockPipeline">
              <div class="event-header">
                <h3 class="event-title">{{ trace.event.title }}</h3>
                <div class="event-tags">
                  <span class="tag type">{{ eventTypeCN(trace.event.type) }}</span>
                </div>
              </div>
              <div class="event-date mono">{{ trace.event.date }}</div>
              <p class="event-summary">{{ trace.event.summary }}</p>
            </template>
            <!-- 传统策略信号：结构化展示 + 纯文本 fallback -->
            <template v-else>
              <div class="trad-signal-info">

                <!-- S07 图谱因子信号 -->
                <div class="factor-table" v-if="trace.signal?.factors">
                  <div class="trad-header">
                    <h3 class="event-title">{{ trace.signal.target_name || trace.event.stock_name }} {{ trace.signal.target_code || trace.event.stock_code }}</h3>
                    <span class="tag strategy-tag">S07 图谱因子</span>
                  </div>
                  <div class="event-date mono">{{ trace.event.date }}</div>
                  <div class="rank-info">
                    综合排名: 第{{ trace.signal.rank }}名 / {{ trace.signal.total_stocks }}只 | 得分: {{ trace.signal.composite_score }}
                  </div>
                  <table class="data-table compact">
                    <thead><tr><th>因子</th><th>标准分</th><th>权重</th><th>贡献度</th></tr></thead>
                    <tbody>
                      <template v-for="(val, name) in trace.signal.factors" :key="name">
                        <tr v-if="val != null">
                          <td>{{ name }}</td>
                          <td class="mono">{{ val.z_score != null ? val.z_score.toFixed(3) : '--' }}</td>
                          <td class="mono">{{ val.weight != null ? val.weight.toFixed(1) : '--' }}</td>
                          <td class="mono" :class="(val.contribution || 0) > 0 ? 'text-green' : 'text-red'">
                            {{ val.contribution != null ? val.contribution.toFixed(3) : '--' }}
                          </td>
                        </tr>
                      </template>
                    </tbody>
                  </table>
                </div>

                <!-- S02 机构关联信号 -->
                <div class="peer-table" v-else-if="trace.signal?.peer_detail">
                  <div class="trad-header">
                    <h3 class="event-title">{{ trace.signal.target_name || trace.event.stock_name }} {{ trace.signal.target_code || trace.event.stock_code }}</h3>
                    <span class="tag strategy-tag">S02 机构关联</span>
                  </div>
                  <div class="event-date mono">{{ trace.event.date }}</div>
                  <table class="data-table compact" v-if="trace.signal.peer_detail.peers?.length">
                    <thead><tr><th>同行公司</th><th>代码</th><th>涨跌幅</th><th>关联度</th></tr></thead>
                    <tbody>
                      <tr v-for="(peer, i) in trace.signal.peer_detail.peers" :key="i">
                        <td>{{ peer.name }}</td>
                        <td class="mono">{{ peer.code }}</td>
                        <td class="mono" :class="(peer.change || 0) > 0 ? 'text-green' : 'text-red'">
                          {{ peer.change != null ? (peer.change * 100).toFixed(2) + '%' : '--' }}
                        </td>
                        <td class="mono">{{ peer.correlation?.toFixed(2) ?? '--' }}</td>
                      </tr>
                    </tbody>
                  </table>
                  <div class="inst-flow" v-if="trace.signal.peer_detail.institution_flow">
                    <div class="sub-title" style="margin-top: 12px;">机构持仓变动</div>
                    <div class="inst-flow-grid">
                      <div class="inst-item new" v-for="(name, i) in (trace.signal.peer_detail.institution_flow.new_entry || [])" :key="'n'+i">
                        <span class="inst-badge">新进</span> {{ name }}
                      </div>
                      <div class="inst-item increase" v-for="(name, i) in (trace.signal.peer_detail.institution_flow.increase || [])" :key="'i'+i">
                        <span class="inst-badge">增持</span> {{ name }}
                      </div>
                      <div class="inst-item decrease" v-for="(name, i) in (trace.signal.peer_detail.institution_flow.decrease || [])" :key="'d'+i">
                        <span class="inst-badge">减持</span> {{ name }}
                      </div>
                      <div class="inst-item exit" v-for="(name, i) in (trace.signal.peer_detail.institution_flow.exit || [])" :key="'e'+i">
                        <span class="inst-badge">退出</span> {{ name }}
                      </div>
                    </div>
                  </div>
                </div>

                <!-- S08/S09 行业轮动信号 -->
                <div class="rotation-table" v-else-if="trace.signal?.rotation_factors">
                  <div class="trad-header">
                    <h3 class="event-title">{{ trace.signal.target_name || trace.event.stock_name }} {{ trace.signal.target_code || trace.event.stock_code }}</h3>
                    <span class="tag strategy-tag">行业轮动</span>
                  </div>
                  <div class="event-date mono">{{ trace.event.date }}</div>
                  <div class="rank-info" v-if="trace.signal.industry_rank != null">
                    行业排名: 第{{ trace.signal.industry_rank }}名 / {{ trace.signal.total_industries || '--' }}个行业
                  </div>
                  <table class="data-table compact">
                    <thead><tr><th>轮动因子</th><th>数值</th><th>评级</th></tr></thead>
                    <tbody>
                      <tr v-for="(val, name) in trace.signal.rotation_factors" :key="name">
                        <td>{{ name }}</td>
                        <td class="mono">{{ typeof val === 'object' ? (val.value?.toFixed(3) ?? '--') : (typeof val === 'number' ? val.toFixed(3) : val) }}</td>
                        <td>
                          <span v-if="typeof val === 'object' && val.rating" class="rotation-rating" :class="val.rating">{{ val.rating }}</span>
                          <span v-else>--</span>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>

                <!-- Fallback: 纯文本 reasoning -->
                <div v-else>
                  <div class="trad-header">
                    <h3 class="event-title">{{ trace.signal?.target_name || trace.event.stock_name || '' }} {{ trace.signal?.target_code || trace.event.stock_code || '' }}</h3>
                    <span class="tag strategy-tag">{{ sourceStrategyCN(trace.event.type) }}</span>
                  </div>
                  <div class="event-date mono">{{ trace.event.date }}</div>
                  <div class="trad-reasoning">
                    <span class="trad-label">策略分析：</span>
                    {{ formatReasoning(trace.event.summary || trace.signal?.reasoning || '') }}
                  </div>
                </div>

                <!-- 置信度分解 (通用) -->
                <div class="confidence-breakdown" v-if="trace.signal?.confidence_breakdown">
                  <span class="cb-item">置信度推导: 基础{{ (trace.signal.confidence_breakdown.base * 100).toFixed(0) }}%</span>
                  <span class="cb-item" v-for="(v, k) in confidenceBreakdownItems" :key="k">
                    + {{ k }} {{ (v * 100).toFixed(1) }}%
                  </span>
                  <span class="cb-item cb-final">= 最终 {{ (trace.signal.confidence_breakdown.final * 100).toFixed(1) }}%</span>
                </div>

              </div>
            </template>
          </div>
        </div>

        <!-- 非冲击链路信号提示 -->
        <div v-if="!hasShockPipeline" class="traditional-hint">
          <div class="hint-icon">ℹ</div>
          <span>该信号来自 {{ sourceStrategyCN(trace.event.type) }} 策略。如需查看完整的事件→图谱→辩论链路，请选择冲击链路信号。</span>
        </div>

        <!-- Arrow (仅冲击链路信号显示) -->
        <div class="trace-arrow" v-if="hasShockPipeline">
          <div class="arrow-line"></div>
          <span class="arrow-label">图谱传播</span>
          <div class="arrow-head"></div>
        </div>

        <!-- Card 2: 图谱传播路径 (仅冲击链路信号显示) -->
        <div class="trace-card" v-if="hasShockPipeline">
          <div class="card-step">
            <span class="step-number">2</span>
            <span class="step-label">图谱传播路径</span>
          </div>
          <div class="card-body">
            <!-- 解释性摘要：告诉用户这个传播路径意味着什么 -->
            <div class="propagation-insight">
              <span class="insight-icon">💡</span>
              <span class="insight-text">{{ propagationInsight }}</span>
            </div>

            <!-- Inline SVG propagation graph -->
            <svg
              v-if="trace.propagation?.path?.length"
              class="propagation-svg"
              :viewBox="`0 0 ${Math.max(trace.propagation.path.length * 220, 440)} 200`"
              preserveAspectRatio="xMidYMid meet"
            >
              <!-- Edges with decay visualization -->
              <template v-for="(node, i) in trace.propagation.path" :key="'edge-'+i">
                <g v-if="i < trace.propagation.path.length - 1">
                  <!-- Gradient line showing decay -->
                  <line
                    :x1="pathNodeX(i) + 32"
                    :y1="75"
                    :x2="pathNodeX(i+1) - 32"
                    :y2="75"
                    :stroke="i === 0 ? '#FF4500' : '#999'"
                    :stroke-width="Math.max(1, 3 - i)"
                    :stroke-dasharray="i > 0 ? '6,3' : 'none'"
                  />
                  <!-- Arrow -->
                  <polygon
                    :points="`${pathNodeX(i+1)-36},70 ${pathNodeX(i+1)-26},75 ${pathNodeX(i+1)-36},80`"
                    :fill="i === 0 ? '#FF4500' : '#999'"
                  />
                  <!-- Relation label (Chinese) -->
                  <text
                    :x="(pathNodeX(i) + pathNodeX(i+1)) / 2"
                    :y="58"
                    text-anchor="middle"
                    fill="#666"
                    font-size="11"
                    font-family="Noto Sans SC, sans-serif"
                  >{{ relationLabel(i) }}</text>
                  <!-- Decay weight -->
                  <text
                    :x="(pathNodeX(i) + pathNodeX(i+1)) / 2"
                    :y="100"
                    text-anchor="middle"
                    fill="#BBB"
                    font-size="9"
                    font-family="JetBrains Mono, monospace"
                  >{{ decayAtHop(i+1) }}</text>
                </g>
              </template>
              <!-- Nodes -->
              <g v-for="(node, i) in trace.propagation.path" :key="'node-'+i">
                <!-- Node circle -->
                <circle
                  :cx="pathNodeX(i)"
                  :cy="75"
                  :r="i === 0 ? 26 : i === trace.propagation.path.length - 1 ? 26 : 20"
                  :fill="i === 0 ? '#FF4500' : i === trace.propagation.path.length - 1 ? '#3B82F6' : '#888'"
                />
                <!-- Role text inside circle -->
                <text
                  :x="pathNodeX(i)"
                  :y="80"
                  text-anchor="middle"
                  fill="#FFF"
                  :font-size="i === 0 || i === trace.propagation.path.length - 1 ? '11' : '10'"
                  font-weight="700"
                  font-family="Noto Sans SC, sans-serif"
                >{{ i === 0 ? '事件源' : i === trace.propagation.path.length - 1 ? '标的' : `第${i}跳` }}</text>
                <!-- Stock code -->
                <text
                  :x="pathNodeX(i)"
                  :y="118"
                  text-anchor="middle"
                  fill="#333"
                  font-size="13"
                  font-weight="700"
                  font-family="JetBrains Mono, monospace"
                >{{ node }}</text>
                <!-- Company name -->
                <text
                  :x="pathNodeX(i)"
                  :y="136"
                  text-anchor="middle"
                  fill="#888"
                  font-size="11"
                  font-family="Noto Sans SC, sans-serif"
                >{{ nodeCompanyName(node) }}</text>
              </g>
            </svg>

            <!-- Path info -->
            <div class="path-stats">
              <div class="ps-item">
                <span class="ps-label">传播跳数</span>
                <span class="ps-value mono">{{ trace.propagation.hop }}</span>
              </div>
              <div class="ps-item">
                <span class="ps-label">衰减因子</span>
                <span class="ps-value mono">{{ decayFactor }}</span>
              </div>
              <div class="ps-item">
                <span class="ps-label">最终冲击权重</span>
                <span class="ps-value mono">{{ shockWeight }}</span>
              </div>
            </div>

            <!-- Text path -->
            <div class="path-text">
              <span
                v-for="(node, i) in trace.propagation.path"
                :key="i"
                class="path-node"
              >
                <span
                  class="node-name"
                  :class="nodeClass(i, trace.propagation.path.length)"
                >{{ node }}</span>
                <span v-if="i < trace.propagation.path.length - 1" class="path-arrow-text">
                  →
                  <span class="relation-label" v-if="trace.propagation.relations && trace.propagation.relations[i]">
                    {{ trace.propagation.relations[i] }}
                  </span>
                </span>
              </span>
            </div>
          </div>
        </div>

        <!-- Arrow (仅冲击链路信号显示) -->
        <div class="trace-arrow" v-if="hasShockPipeline">
          <div class="arrow-line"></div>
          <span class="arrow-label">Agent辩论</span>
          <div class="arrow-head"></div>
        </div>

        <!-- Card 3: Agent辩论记录 (仅冲击链路信号显示) -->
        <div class="trace-card" v-if="hasShockPipeline">
          <div class="card-step">
            <span class="step-number">3</span>
            <span class="step-label">Agent辩论记录</span>
          </div>
          <div class="card-body">
            <!-- Debate summary -->
            <div class="debate-summary">
              <div class="ds-item">
                <span class="ds-label">Agent数量</span>
                <span class="ds-val mono">{{ trace.debate.agents?.length ?? '--' }}</span>
              </div>
              <div class="ds-item">
                <span class="ds-label">辩论轮数</span>
                <span class="ds-val mono">{{ trace.debate.rounds ?? '--' }}</span>
              </div>
              <div class="ds-item">
                <span class="ds-label">分歧度</span>
                <span class="ds-val mono">{{ trace.debate.divergence?.toFixed(2) ?? '--' }}</span>
              </div>
            </div>

            <!-- Consensus bar -->
            <div class="consensus-wrap" v-if="trace.debate.divergence != null">
              <div class="consensus-bar">
                <div
                  class="consensus-bull"
                  :style="{ width: bullPercent + '%' }"
                ></div>
                <div
                  class="consensus-bear"
                  :style="{ width: bearPercent + '%' }"
                ></div>
              </div>
              <div class="consensus-labels">
                <span class="bull-label">看多 {{ Math.round(bullPercent) }}%</span>
                <span class="bear-label">看空 {{ Math.round(bearPercent) }}%</span>
              </div>
            </div>

            <!-- Agent columns: 从 agents 列表渲染 -->
            <div class="debate-columns" v-if="trace.debate.agents && trace.debate.agents.length > 0">
              <div class="debate-col bull-col">
                <div class="col-title bull">看多立场</div>
                <div
                  v-for="(a, i) in bullAgents"
                  :key="'bull-'+i"
                  class="agent-card"
                >
                  <div class="agent-header">
                    <span class="agent-name">{{ a.archetype ?? a.name }}</span>
                    <span class="agent-sentiment bull">{{ typeof a.sentiment === 'number' ? a.sentiment.toFixed(2) : a.sentiment }}</span>
                  </div>
                  <p class="agent-reasoning">{{ a.reasoning }}</p>
                </div>
                <div v-if="bullAgents.length === 0" class="no-agents">暂无看多Agent</div>
              </div>

              <div class="debate-col bear-col">
                <div class="col-title bear">看空立场</div>
                <div
                  v-for="(a, i) in bearAgents"
                  :key="'bear-'+i"
                  class="agent-card"
                >
                  <div class="agent-header">
                    <span class="agent-name">{{ a.archetype ?? a.name }}</span>
                    <span class="agent-sentiment bear">{{ typeof a.sentiment === 'number' ? a.sentiment.toFixed(2) : a.sentiment }}</span>
                  </div>
                  <p class="agent-reasoning">{{ a.reasoning }}</p>
                </div>
                <div v-if="bearAgents.length === 0" class="no-agents">暂无看空Agent</div>
              </div>
            </div>

            <!-- Fallback: debate_summary 纯文本展示 -->
            <div class="debate-text-fallback" v-else-if="trace.debate.debate_summary">
              <div class="col-title" style="margin-bottom:12px">辩论详情</div>
              <div
                v-for="(line, i) in (trace.debate.debate_summary || '').split('\n').filter(l => l.trim())"
                :key="'dline-'+i"
                class="debate-line"
                :class="{
                  'bear-line': line.includes('sell') || line.includes('-0.') || line.includes('-1.'),
                  'bull-line': line.includes('buy') || (line.includes('+') && !line.includes('-')),
                  'neutral-line': line.includes('watch') || line.includes('hold')
                }"
              >
                {{ line }}
              </div>
            </div>

            <!-- Agent辩论免责声明 -->
            <div class="debate-disclaimer">
              以上为LLM模拟多视角分析，仅作为参考信息，不构成收益预测
            </div>
          </div>
        </div>

        <!-- Arrow -->
        <div class="trace-arrow">
          <div class="arrow-line"></div>
          <span class="arrow-label">信号生成</span>
          <div class="arrow-head"></div>
        </div>

        <!-- Card 4: 信号生成 -->
        <div class="trace-card">
          <div class="card-step">
            <span class="step-number">4</span>
            <span class="step-label">信号生成</span>
          </div>
          <div class="card-body">
            <!-- Direction + Confidence + Return -->
            <div class="signal-result">
              <span class="direction-badge large" :class="trace.signal.direction">
                {{ trace.signal.direction === 'long' ? '做多' : '回避' }}
              </span>
              <div class="sr-metric">
                <div class="sr-label">信号强度</div>
                <div class="sr-value mono">{{ (trace.signal.confidence * 100).toFixed(1) }}%</div>
              </div>
              <div class="sr-metric">
                <div class="sr-label uncalibrated-label">参考值(未校准)</div>
                <div class="sr-value mono uncalibrated-value">
                  {{ trace.signal.expected_return >= 0 ? '+' : '' }}{{ (trace.signal.expected_return * 100).toFixed(2) }}%
                </div>
              </div>
              <div class="sr-metric" v-if="trace.signal.alpha_type">
                <div class="sr-label">Alpha类型</div>
                <span class="alpha-tag">{{ trace.signal.alpha_type }}</span>
              </div>
            </div>

            <!-- 未反应检测 -->
            <div class="sub-section">
              <div class="sub-title">未反应检测 <span class="tooltip-wrap"><span class="tooltip-icon">i</span><span class="tooltip-text">事件冲击了公司A后，通过供应链/竞争关系传导到公司B。如果B的股价在过去5个交易日内涨跌幅不超过2%，说明市场还没给B定价。这个信息差就是Alpha来源。</span></span></div>
              <div class="react-row">
                <span class="react-badge" :class="trace.signal.reacted ? 'reacted' : 'unreacted'">
                  {{ trace.signal.reacted ? '已反应' : '未反应检测' }}
                </span>
                <span class="mono react-detail" v-if="trace.signal.return_5d != null">
                  5日收益率: {{ (trace.signal.return_5d * 100).toFixed(2) }}%
                </span>
              </div>
            </div>

            <!-- 交叉验证 -->
            <div class="sub-section" v-if="trace.signal.cross_validation && trace.signal.cross_validation.length > 0">
              <div class="sub-title">交叉验证</div>
              <div class="cross-tags">
                <span
                  v-for="(cv, i) in trace.signal.cross_validation"
                  :key="i"
                  class="cross-tag"
                >{{ cv }}</span>
              </div>
            </div>

            <!-- Forward Returns -->
            <div class="sub-section" v-if="trace.signal.forward_returns">
              <div class="sub-title">前瞻收益率</div>
              <div class="forward-bars">
                <div
                  v-for="period in forwardPeriods"
                  :key="period.key"
                  class="fw-item"
                >
                  <span class="fw-label">{{ period.label }}</span>
                  <div class="fw-bar-wrap">
                    <div
                      class="fw-bar"
                      :class="fwReturn(period.key) >= 0 ? 'positive' : 'negative'"
                      :style="{ width: fwBarWidth(period.key) + '%' }"
                    ></div>
                  </div>
                  <span class="fw-value mono" :class="fwReturn(period.key) >= 0 ? 'positive' : 'negative'">
                    {{ fwReturn(period.key) >= 0 ? '+' : '' }}{{ (fwReturn(period.key) * 100).toFixed(2) }}%
                  </span>
                </div>
              </div>
            </div>

            <!-- 信号推理（传统策略信号时显示完整推理） -->
            <div class="sub-section" v-if="trace.signal.reasoning">
              <div class="sub-title">信号推理</div>
              <p class="breakdown-text">{{ trace.signal.reasoning }}</p>
            </div>

            <!-- Confidence breakdown -->
            <div class="sub-section" v-if="trace.signal.confidence_breakdown">
              <div class="sub-title">置信度构成</div>
              <p class="breakdown-text mono">{{ trace.signal.confidence_breakdown }}</p>
            </div>

            <!-- 在图谱中查看 -->
            <div class="sub-section trace-actions">
              <button
                class="btn-graph"
                @click="router.push(`/graph?focus=${trace.signal.target_code || trace.event.stock_code}`)"
              >
                在图谱中查看 →
              </button>
            </div>
          </div>
        </div>
      </template>

      <!-- No data -->
      <div v-else class="empty-state">未找到信号数据</div>
    </main>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, nextTick, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { getSignalTrace } from '../api/astrategy.js'

const route = useRoute()
const router = useRouter()
const loading = ref(false)
const error = ref('')
const trace = ref(null)
const graphContainer = ref(null)
const requestedSignalId = ref('')

const forwardPeriods = [
  { key: '1d', label: '1日' },
  { key: '3d', label: '3日' },
  { key: '5d', label: '5日' },
  { key: '10d', label: '10日' },
  { key: '20d', label: '20日' },
]

const decayFactor = computed(() => {
  const df = trace.value?.propagation?.decay_factor ?? trace.value?.propagation?.decayFactor
  return df != null ? df.toFixed(3) : '--'
})

const shockWeight = computed(() => {
  const sw = trace.value?.propagation?.shock_weight ?? trace.value?.propagation?.shockWeight
  return sw != null ? sw.toFixed(3) : '--'
})

// 传播路径解释性文本
const propagationInsight = computed(() => {
  const p = trace.value?.propagation
  const e = trace.value?.event
  if (!p || !p.path || p.path.length < 2) return ''

  const sourceName = e?.stock_name || p.path[0]
  const targetName = trace.value?.signal?.target_name || p.path[p.path.length - 1]
  const rels = (p.relations || []).map(r => relationLabel(
    (p.relations || []).indexOf(r)
  )).join('→')
  const hop = p.hop || p.path.length - 1
  const sw = p.shock_weight || 0

  const eventTitle = e?.title || ''
  const direction = trace.value?.signal?.direction

  if (direction === 'avoid') {
    return `"${eventTitle}" 发生后，${sourceName} 的负面冲击通过 ${rels} 关系，经 ${hop} 跳传导至 ${targetName}。冲击衰减至 ${(sw*100).toFixed(0)}%。Agent辩论共识：回避该标的。`
  } else {
    return `"${eventTitle}" 发生后，通过 ${rels} 关系，${sourceName} 的影响经 ${hop} 跳传导至 ${targetName}。冲击权重 ${(sw*100).toFixed(0)}%。Agent辩论共识：做多该标的。`
  }
})

// 根据股票代码获取公司名（从 trace 数据中推断）
function nodeCompanyName(code) {
  const e = trace.value?.event
  const s = trace.value?.signal
  // 源头
  if (code === e?.stock_code) return e?.stock_name || ''
  // 标的
  if (code === s?.target_code) return s?.target_name || ''
  // 中间节点：暂无名称映射
  return ''
}

// 计算每跳的衰减值
function decayAtHop(hop) {
  const base = 1.0
  const decay = 0.5
  return `${(base * Math.pow(decay, hop) * 100).toFixed(0)}%`
}

// SVG node X position calculator
function pathNodeX(index) {
  const count = trace.value?.propagation?.path?.length || 1
  const totalWidth = Math.max(count * 200, 400)
  const gap = totalWidth / (count + 1)
  return gap * (index + 1)
}

// 事件类型中文映射
function eventTypeCN(type) {
  const map = {
    'scandal': '丑闻', 'earnings_shock': '业绩冲击', 'earnings_surprise': '业绩超预期',
    'ma': '并购重组', 'policy_risk': '政策风险', 'policy_change': '政策变化',
    'product_launch': '产品发布', 'technology_breakthrough': '技术突破',
    'management_change': '管理层变动', 'cooperation': '合作',
    'supply_shortage': '供应短缺', 'buyback': '回购', 'signal': '策略信号',
  }
  return map[type] || type || '其他'
}

// 来源策略中文映射
function sourceStrategyCN(type) {
  const stratName = trace.value?.signal?.strategy_name || type
  const map = {
    'signal': '多因子策略', 'graph_factors': 'S07 图谱因子', 'sector_rotation': 'S09 行业轮动',
    'institution': 'S02 机构关联', 'institution_association': 'S02 机构关联',
    'sentiment_simulation': 'S10 舆情模拟', 'analyst_divergence': 'S05 分析师分歧',
    'shock_propagation': '冲击链路(PRIMARY)',
  }
  return map[stratName] || map[type] || type || '策略信号'
}

// 将技术性reasoning格式化为人可读文本
function formatReasoning(text) {
  if (!text) return '暂无分析数据'

  // ── S07 图谱因子 ──
  // "Composite rank 30/800, score=0.273. Factors: supply_chain_centrality=0.02(w=+1.5)..."
  if (text.includes('Composite rank') || text.includes('supply_chain_centrality')) {
    const rankM = text.match(/Composite rank (\d+)\/(\d+),\s*score=([\d.]+)/)
    const parts = []
    if (rankM) {
      parts.push(`在 ${rankM[2]} 只股票中综合排名第 ${rankM[1]}（得分 ${rankM[3]}）`)
    }
    const factorMap = {
      'supply_chain_centrality': '供应链重要性', 'betweenness_centrality': '信息枢纽度',
      'institution_concentration': '机构持仓集中度', 'concept_heat': '概念热度',
      'event_exposure': '事件敏感度', 'industry_leadership': '行业龙头地位',
      'peer_return_gap': '相对同行收益差',
    }
    const highlights = []
    for (const [eng, cn] of Object.entries(factorMap)) {
      const re = new RegExp(eng + '=([\\d.]+)')
      const m = text.match(re)
      if (m && parseFloat(m[1]) > 0.01) {
        highlights.push(`${cn} ${m[1]}`)
      }
    }
    if (highlights.length > 0) parts.push('突出因子：' + highlights.join('、'))
    return parts.length > 0 ? parts.join('。') : '基于图谱多因子模型综合评分'
  }

  // ── 机构关联策略 ──
  // "比亚迪(002594): peer group avg -2.17% vs self 15.21% (gap -17.38%). Institutions: 6 holders..."
  if (text.includes('peer group avg') || text.includes('Institutions:')) {
    const parts = []
    const peerM = text.match(/peer group avg ([-\d.]+)%\s*vs self ([-\d.]+)%\s*\(gap ([-\d.]+)%\)/)
    if (peerM) {
      const selfRet = parseFloat(peerM[2])
      const gap = parseFloat(peerM[3])
      if (gap < -5) {
        parts.push(`近期涨幅 ${selfRet.toFixed(1)}%，大幅跑赢同行（领先 ${Math.abs(gap).toFixed(1)}%）`)
      } else if (gap > 5) {
        parts.push(`近期涨幅 ${selfRet.toFixed(1)}%，明显跑输同行（落后 ${Math.abs(gap).toFixed(1)}%）`)
      } else {
        parts.push(`近期涨幅 ${selfRet.toFixed(1)}%，与同行基本持平`)
      }
    }
    const instM = text.match(/Institutions:\s*(\d+)\s*holders/)
    if (instM) parts.push(`${instM[1]} 家机构持仓`)
    const sentM = text.match(/sentiment=(\w+)/)
    if (sentM) {
      const sentMap = { stable: '情绪稳定', positive: '情绪偏多', negative: '情绪偏空' }
      parts.push(sentMap[sentM[1]] || sentM[1])
    }
    if (text.includes('Relative weakness')) parts.push('相对弱势信号')
    if (text.includes('Catch-up opportunity')) parts.push('补涨机会')
    if (text.includes('outperformed peers')) parts.push('机构可能获利了结')
    return parts.length > 0 ? parts.join('；') : text.substring(0, 150)
  }

  // ── 已有中文的（行业轮动等）直接返回 ──
  if (/[\u4e00-\u9fa5]/.test(text)) {
    return text.length > 300 ? text.substring(0, 300) + '...' : text
  }

  // ── 其他英文：截断并返回 ──
  return text.length > 200 ? text.substring(0, 200) + '...' : text
}

// Relation label for edge i
function relationLabel(i) {
  const rels = trace.value?.propagation?.relations || []
  const rel = rels[i] || ''
  // Chinese mapping
  const map = {
    'SUPPLIES_TO': '供应链',
    'CUSTOMER_OF': '客户',
    'COMPETES_WITH': '竞争',
    'COOPERATES_WITH': '合作',
    'HOLDS_SHARES': '持仓',
    'BELONGS_TO': '行业',
  }
  return map[rel] || rel
}

const bullAgents = computed(() => {
  if (!trace.value?.debate?.agents) return []
  return trace.value.debate.agents.filter(a => {
    const s = typeof a.sentiment === 'number' ? a.sentiment : parseFloat(a.sentiment)
    const act = a.action || ''
    return s > 0 || act === 'buy' || act === 'long'
  })
})

const bearAgents = computed(() => {
  if (!trace.value?.debate?.agents) return []
  return trace.value.debate.agents.filter(a => {
    const s = typeof a.sentiment === 'number' ? a.sentiment : parseFloat(a.sentiment)
    const act = a.action || ''
    return s < 0 || act === 'sell' || act === 'avoid' || act === 'short'
  })
})

const neutralAgents = computed(() => {
  if (!trace.value?.debate?.agents) return []
  return trace.value.debate.agents.filter(a => {
    const s = typeof a.sentiment === 'number' ? a.sentiment : parseFloat(a.sentiment)
    const act = a.action || ''
    return (s === 0 || isNaN(s)) && !['buy','sell','long','avoid','short'].includes(act)
  })
})

const bullPercent = computed(() => {
  const total = (bullAgents.value?.length || 0) + (bearAgents.value?.length || 0) + (neutralAgents.value?.length || 0)
  if (total > 0) {
    return Math.round((bullAgents.value.length / total) * 100)
  }
  // fallback: use consensus_direction + divergence
  const dir = trace.value?.debate?.consensus_direction ?? trace.value?.debate?.consensusDirection
  if (dir === 'bullish' || dir === 'long') return 70
  if (dir === 'bearish' || dir === 'avoid' || dir === 'short') return 30
  return 50
})

const bearPercent = computed(() => 100 - bullPercent.value)

// 是否为冲击链路信号（有传播路径数据）
const hasShockPipeline = computed(() => {
  const p = trace.value?.propagation
  return p != null && Array.isArray(p.path) && p.path.length > 0
})

const confidenceBreakdownItems = computed(() => {
  const bd = trace.value?.signal?.confidence_breakdown
  if (!bd) return {}
  const items = {}
  for (const [k, v] of Object.entries(bd)) {
    if (k !== 'base' && k !== 'final' && typeof v === 'number') {
      items[k] = v
    }
  }
  return items
})

function impactClass(impact) {
  if (!impact) return ''
  const l = String(impact).toLowerCase()
  if (l === 'high' || l === '高') return 'high'
  if (l === 'medium' || l === '中') return 'medium'
  return 'low'
}

function nodeClass(index, total) {
  if (index === 0) return 'source'
  if (index === total - 1) return 'target'
  return 'intermediary'
}

function fwReturn(key) {
  return trace.value?.signal?.forward_returns?.[key] ?? 0
}

function fwBarWidth(key) {
  const v = Math.abs(fwReturn(key)) * 100
  return Math.min(v * 5, 100) // scale for visibility
}

async function loadTrace() {
  const signalId = route.params.signalId
  requestedSignalId.value = signalId || ''
  if (!signalId) {
    error.value = '缺少信号ID'
    return
  }
  loading.value = true
  error.value = ''
  try {
    const res = await getSignalTrace(signalId)
    const data = res.data ?? res
    // API 返回格式: {event, propagation, debate, signal}
    // propagation/debate 可能为 null（传统策略信号）
    trace.value = {
      event: data.event ?? { title: '--', type: '--', date: '--', summary: '暂无事件信息' },
      propagation: data.propagation ?? null,
      debate: data.debate ?? null,
      signal: data.signal ?? {},
    }
    await nextTick()
    renderMiniGraph()
  } catch (e) {
    const status = e.response?.status
    if (status === 404) {
      error.value = `未找到信号 "${signalId}" 的链路数据`
    } else {
      error.value = '加载信号链路失败: ' + (e.response?.data?.error || e.message || e)
    }
  } finally {
    loading.value = false
  }
}

function renderMiniGraph() {
  if (!graphContainer.value || !trace.value?.propagation?.path) return

  const container = graphContainer.value
  container.innerHTML = ''

  const path = trace.value.propagation.path
  const relations = trace.value.propagation.relations ?? []

  const width = container.clientWidth || 500
  const height = 180

  // Create SVG manually (no D3 dependency required)
  const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg')
  svg.setAttribute('width', width)
  svg.setAttribute('height', height)
  svg.style.display = 'block'

  const nodeCount = path.length
  const gap = width / (nodeCount + 1)
  const cy = height / 2

  // Draw edges
  for (let i = 0; i < nodeCount - 1; i++) {
    const x1 = gap * (i + 1)
    const x2 = gap * (i + 2)

    const line = document.createElementNS('http://www.w3.org/2000/svg', 'line')
    line.setAttribute('x1', x1 + 20)
    line.setAttribute('y1', cy)
    line.setAttribute('x2', x2 - 20)
    line.setAttribute('y2', cy)
    line.setAttribute('stroke', '#DDD')
    line.setAttribute('stroke-width', '2')
    svg.appendChild(line)

    // Arrow head
    const arrowX = x2 - 24
    const arrow = document.createElementNS('http://www.w3.org/2000/svg', 'polygon')
    arrow.setAttribute('points', `${arrowX},${cy-5} ${arrowX+10},${cy} ${arrowX},${cy+5}`)
    arrow.setAttribute('fill', '#DDD')
    svg.appendChild(arrow)

    // Relation label
    if (relations[i]) {
      const relText = document.createElementNS('http://www.w3.org/2000/svg', 'text')
      relText.setAttribute('x', (x1 + x2) / 2)
      relText.setAttribute('y', cy - 18)
      relText.setAttribute('text-anchor', 'middle')
      relText.setAttribute('fill', '#999')
      relText.setAttribute('font-size', '10')
      relText.setAttribute('font-family', 'Noto Sans SC, sans-serif')
      relText.textContent = relations[i]
      svg.appendChild(relText)
    }
  }

  // Draw nodes
  for (let i = 0; i < nodeCount; i++) {
    const cx = gap * (i + 1)

    // Node circle
    const circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle')
    circle.setAttribute('cx', cx)
    circle.setAttribute('cy', cy)
    circle.setAttribute('r', '18')

    let fill = '#999' // intermediary
    if (i === 0) fill = '#FF4500' // source
    if (i === nodeCount - 1) fill = '#3B82F6' // target

    circle.setAttribute('fill', fill)
    svg.appendChild(circle)

    // Node label
    const text = document.createElementNS('http://www.w3.org/2000/svg', 'text')
    text.setAttribute('x', cx)
    text.setAttribute('y', cy + 36)
    text.setAttribute('text-anchor', 'middle')
    text.setAttribute('fill', '#333')
    text.setAttribute('font-size', '12')
    text.setAttribute('font-weight', '600')
    text.setAttribute('font-family', 'Noto Sans SC, sans-serif')

    const name = path[i]
    text.textContent = name.length > 6 ? name.slice(0, 5) + '...' : name
    svg.appendChild(text)

    // Role label inside circle
    const roleText = document.createElementNS('http://www.w3.org/2000/svg', 'text')
    roleText.setAttribute('x', cx)
    roleText.setAttribute('y', cy + 4)
    roleText.setAttribute('text-anchor', 'middle')
    roleText.setAttribute('fill', '#FFF')
    roleText.setAttribute('font-size', '10')
    roleText.setAttribute('font-weight', '700')
    roleText.setAttribute('font-family', 'JetBrains Mono, monospace')

    if (i === 0) roleText.textContent = '源'
    else if (i === nodeCount - 1) roleText.textContent = '标的'
    else roleText.textContent = `${i}`
    svg.appendChild(roleText)
  }

  container.appendChild(svg)
}

// Re-render graph on resize
let resizeTimer = null
function handleResize() {
  clearTimeout(resizeTimer)
  resizeTimer = setTimeout(renderMiniGraph, 200)
}

onMounted(() => {
  loadTrace()
  window.addEventListener('resize', handleResize)
})

// Cleanup
import { onUnmounted } from 'vue'
onUnmounted(() => {
  window.removeEventListener('resize', handleResize)
})
</script>

<style scoped>
.signal-trace {
  min-height: 100vh;
  background: #FAFAFA;
  font-family: 'Noto Sans SC', 'Space Grotesk', system-ui, sans-serif;
}

/* Header */
.st-header {
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

.page-title { font-size: 15px; font-weight: 600; }
.header-actions { display: flex; gap: 20px; }
.nav-link {
  color: #999;
  text-decoration: none;
  font-size: 13px;
  transition: color 0.2s;
}
.nav-link:hover { color: #FFF; }

/* Content */
.st-content {
  max-width: 900px;
  margin: 0 auto;
  padding: 32px 32px 64px;
}

/* Trace Card */
.trace-card {
  background: #FFF;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
  overflow: hidden;
  transition: box-shadow 0.2s;
}
.trace-card:hover { box-shadow: 0 2px 12px rgba(0,0,0,0.05); }

.card-step {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 14px 20px;
  background: #F9F9F9;
  border-bottom: 1px solid #F0F0F0;
}

.step-number {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 26px;
  height: 26px;
  border-radius: 50%;
  background: #FF4500;
  color: #FFF;
  font-family: 'JetBrains Mono', monospace;
  font-size: 13px;
  font-weight: 700;
}

.step-label {
  font-size: 14px;
  font-weight: 600;
  color: #111;
}

.card-body { padding: 20px; }

/* Card 1: Event */
.event-header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 8px;
}

.event-title {
  font-size: 16px;
  font-weight: 700;
  color: #111;
  margin: 0;
}

.event-tags { display: flex; gap: 6px; flex-shrink: 0; }

.tag {
  display: inline-block;
  padding: 2px 10px;
  border-radius: 3px;
  font-size: 11px;
  font-weight: 600;
}
.tag.type { background: #F5F5F5; color: #666; }
.tag.impact.high { background: #FEE2E2; color: #B91C1C; }
.tag.impact.medium { background: #FEF3C7; color: #92400E; }
.tag.impact.low { background: #DCFCE7; color: #15803D; }

.event-date { font-size: 12px; color: #999; margin-bottom: 10px; }
.event-summary { font-size: 14px; color: #444; line-height: 1.7; margin: 0 0 10px 0; }
.event-source { font-size: 12px; color: #999; }

/* Traditional signal info */
.trad-signal-info { }
.trad-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; }
.strategy-tag { background: #E8F5E9; color: #2E7D32; padding: 3px 10px; border-radius: 4px; font-size: 12px; }
.trad-reasoning {
  font-size: 14px; color: #444; line-height: 1.8; margin-top: 12px;
  padding: 12px 16px; background: #F8F9FA; border-radius: 6px; border-left: 3px solid #FF4500;
}
.trad-label { font-weight: 600; color: #333; }

/* Card 2: Propagation */
.graph-container {
  width: 100%;
  min-height: 180px;
  background: #FAFAFA;
  border-radius: 6px;
  margin-bottom: 16px;
  overflow: hidden;
}
.propagation-insight {
  display: flex;
  align-items: flex-start;
  gap: 8px;
  padding: 12px 16px;
  background: #FFF8F0;
  border: 1px solid #FFE0B2;
  border-radius: 8px;
  margin-bottom: 16px;
  line-height: 1.7;
}
.insight-icon { font-size: 18px; flex-shrink: 0; margin-top: 2px; }
.insight-text {
  font-size: 13px;
  color: #555;
  font-family: 'Noto Sans SC', sans-serif;
}
.propagation-svg {
  width: 100%;
  height: 180px;
  background: #FAFAFA;
  border-radius: 6px;
  margin-bottom: 16px;
}

.path-stats {
  display: flex;
  gap: 24px;
  margin-bottom: 16px;
}

.ps-item { display: flex; flex-direction: column; gap: 2px; }
.ps-label { font-size: 12px; color: #666; }
.ps-value { font-size: 18px; font-weight: 700; color: #111; }

.path-text {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 4px;
  font-size: 14px;
}

.path-node { display: inline-flex; align-items: center; gap: 4px; }

.node-name {
  padding: 4px 10px;
  border-radius: 4px;
  font-weight: 600;
}
.node-name.source { background: #FFF7ED; color: #C2410C; }
.node-name.target { background: #EFF6FF; color: #1D4ED8; }
.node-name.intermediary { background: #F5F5F5; color: #666; }

.path-arrow-text {
  color: #CCC;
  font-weight: 700;
  margin: 0 2px;
}

.relation-label {
  font-size: 10px;
  color: #999;
  font-weight: 400;
}

/* Card 3: Debate */
.debate-summary {
  display: flex;
  gap: 24px;
  margin-bottom: 16px;
}

.ds-item { display: flex; flex-direction: column; gap: 2px; }
.ds-label { font-size: 12px; color: #666; }
.ds-val { font-size: 18px; font-weight: 700; color: #111; }

.consensus-wrap { margin-bottom: 20px; }

.consensus-bar {
  display: flex;
  height: 12px;
  border-radius: 6px;
  overflow: hidden;
  margin-bottom: 4px;
}
.consensus-bull { background: #22C55E; transition: width 0.4s; }
.consensus-bear { background: #EF4444; transition: width 0.4s; }

.consensus-labels {
  display: flex;
  justify-content: space-between;
  font-size: 11px;
  font-weight: 600;
}
.bull-label { color: #15803D; }
.bear-label { color: #B91C1C; }

.debate-columns {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
}

.col-title {
  font-size: 13px;
  font-weight: 700;
  margin-bottom: 10px;
  padding-bottom: 6px;
  border-bottom: 2px solid;
}
.col-title.bull { color: #15803D; border-color: #22C55E; }
.col-title.bear { color: #B91C1C; border-color: #EF4444; }

.agent-card {
  background: #F9F9F9;
  border-radius: 6px;
  padding: 12px;
  margin-bottom: 8px;
}

.agent-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 6px;
}

.agent-name { font-size: 13px; font-weight: 600; color: #111; }
.agent-sentiment {
  font-size: 11px;
  font-weight: 600;
  padding: 2px 8px;
  border-radius: 3px;
}
.agent-sentiment.bull { background: #DCFCE7; color: #15803D; }
.agent-sentiment.bear { background: #FEE2E2; color: #B91C1C; }

.agent-reasoning {
  font-size: 12px;
  color: #666;
  line-height: 1.6;
  margin: 0;
}

.no-agents {
  color: #CCC;
  font-size: 13px;
  text-align: center;
  padding: 20px 0;
}

/* Debate text fallback */
.debate-text-fallback {
  margin-top: 16px;
}
.debate-line {
  padding: 8px 12px;
  margin-bottom: 6px;
  border-radius: 6px;
  font-size: 13px;
  line-height: 1.6;
  font-family: 'Noto Sans SC', sans-serif;
  background: #F9F9F9;
  border-left: 3px solid #DDD;
}
.debate-line.bear-line {
  background: #FFF5F5;
  border-left-color: #E53E3E;
}
.debate-line.bull-line {
  background: #F0FFF4;
  border-left-color: #38A169;
}
.debate-line.neutral-line {
  background: #FFFAF0;
  border-left-color: #DD6B20;
}

/* Card 4: Signal */
.signal-result {
  display: flex;
  align-items: center;
  gap: 24px;
  margin-bottom: 24px;
  padding-bottom: 20px;
  border-bottom: 1px solid #F0F0F0;
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
  padding: 0 10px;
}
.direction-badge.large {
  min-width: 64px;
  height: 36px;
  font-size: 15px;
}
.direction-badge.long { background: #DCFCE7; color: #15803D; }
.direction-badge.avoid,
.direction-badge.short { background: #FEE2E2; color: #B91C1C; }

.sr-metric { display: flex; flex-direction: column; gap: 2px; }
.sr-label { font-size: 12px; color: #666; }
.sr-value { font-size: 20px; font-weight: 700; color: #111; }
.sr-value.positive { color: #22C55E; }
.sr-value.negative { color: #EF4444; }

.uncalibrated-label { color: #999 !important; font-style: italic; }
.uncalibrated-value { color: #999 !important; font-size: 16px !important; font-weight: 400 !important; }

.debate-disclaimer {
  margin-top: 16px;
  padding: 10px 14px;
  background: #FFFBEB;
  border: 1px solid #FDE68A;
  border-radius: 6px;
  font-size: 12px;
  color: #92400E;
  text-align: center;
}

.alpha-tag {
  display: inline-block;
  font-size: 12px;
  background: #FFF7ED;
  color: #C2410C;
  padding: 4px 10px;
  border-radius: 4px;
  font-weight: 600;
  margin-top: 2px;
}

.sub-section { margin-bottom: 20px; }
.sub-title {
  font-size: 13px;
  font-weight: 600;
  color: #111;
  margin-bottom: 8px;
  padding-left: 10px;
  border-left: 3px solid #FF4500;
}

.react-row { display: flex; align-items: center; gap: 16px; }

.react-badge {
  display: inline-block;
  padding: 4px 12px;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 600;
}
.react-badge.reacted { background: #F0F0F0; color: #666; }
.react-badge.unreacted { background: #FEF3C7; color: #92400E; }

.react-detail { font-size: 13px; color: #666; }

.cross-tags { display: flex; gap: 6px; flex-wrap: wrap; }
.cross-tag {
  font-size: 12px;
  background: #EFF6FF;
  color: #1D4ED8;
  padding: 4px 10px;
  border-radius: 4px;
  font-weight: 500;
}

/* Forward Returns */
.forward-bars {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.fw-item {
  display: flex;
  align-items: center;
  gap: 12px;
}

.fw-label {
  width: 32px;
  font-size: 12px;
  color: #666;
  text-align: right;
  flex-shrink: 0;
}

.fw-bar-wrap {
  flex: 1;
  height: 12px;
  background: #F0F0F0;
  border-radius: 6px;
  overflow: hidden;
}

.fw-bar {
  height: 100%;
  border-radius: 6px;
  transition: width 0.4s;
}
.fw-bar.positive { background: #22C55E; }
.fw-bar.negative { background: #EF4444; }

.fw-value {
  width: 72px;
  font-size: 12px;
  font-weight: 600;
  text-align: right;
  flex-shrink: 0;
}

.breakdown-text {
  font-size: 12px;
  color: #666;
  line-height: 1.6;
  background: #F9F9F9;
  padding: 12px;
  border-radius: 6px;
  margin: 0;
  white-space: pre-wrap;
}

/* Trace Arrow */
.trace-arrow {
  display: flex;
  flex-direction: column;
  align-items: center;
  padding: 8px 0;
}

.arrow-line {
  width: 2px;
  height: 20px;
  background: #E5E5E5;
}

.arrow-label {
  font-size: 11px;
  color: #999;
  background: #FAFAFA;
  padding: 2px 12px;
  border: 1px solid #E5E5E5;
  border-radius: 12px;
  margin: 4px 0;
}

.arrow-head {
  width: 0;
  height: 0;
  border-left: 6px solid transparent;
  border-right: 6px solid transparent;
  border-top: 8px solid #E5E5E5;
}

/* States */
.loading-state {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 12px;
  padding: 80px 0;
  color: #999;
  font-size: 14px;
}

.spinner {
  width: 24px;
  height: 24px;
  border: 3px solid #F0F0F0;
  border-top-color: #FF4500;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }

.error-state {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 80px 0;
  font-size: 14px;
}

.error-content {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 12px;
}

.error-icon {
  width: 48px;
  height: 48px;
  border-radius: 50%;
  background: #FEE2E2;
  color: #EF4444;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 24px;
  font-weight: 800;
}

.error-msg {
  color: #EF4444;
  font-size: 15px;
  font-weight: 500;
}

.error-id {
  font-size: 12px;
  color: #999;
  background: #F5F5F5;
  padding: 4px 12px;
  border-radius: 4px;
}

.error-actions {
  display: flex;
  gap: 12px;
  margin-top: 8px;
}

.btn-back {
  background: #FFF;
  color: #333;
  border: 1px solid #E5E5E5;
  padding: 6px 16px;
  border-radius: 4px;
  font-size: 13px;
  cursor: pointer;
}
.btn-back:hover { border-color: #FF4500; color: #FF4500; }

.btn-retry {
  background: #111;
  color: #FFF;
  border: none;
  padding: 6px 16px;
  border-radius: 4px;
  font-size: 13px;
  cursor: pointer;
}
.btn-retry:hover { opacity: 0.85; }

.empty-state {
  text-align: center;
  padding: 80px 0;
  color: #999;
  font-size: 14px;
}

/* Traditional strategy hint */
.traditional-hint {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 14px 20px;
  margin: 16px 0;
  background: #FFF7ED;
  border: 1px solid #FED7AA;
  border-radius: 8px;
  font-size: 13px;
  color: #92400E;
  line-height: 1.5;
}

.hint-icon {
  width: 24px;
  height: 24px;
  border-radius: 50%;
  background: #FDBA74;
  color: #FFF;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 14px;
  font-weight: 800;
  flex-shrink: 0;
}

/* Data table */
.data-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 10px;
  font-size: 13px;
}
.data-table th {
  background: #F1F5F9;
  color: #475569;
  font-weight: 600;
  text-align: left;
  padding: 8px 12px;
  border-bottom: 2px solid #E2E8F0;
}
.data-table td {
  padding: 8px 12px;
  border-bottom: 1px solid #F1F5F9;
  color: #334155;
}
.data-table tbody tr:hover { background: #F8FAFC; }
.data-table.compact th { padding: 5px 10px; font-size: 12px; }
.data-table.compact td { padding: 5px 10px; font-size: 12px; }

.text-green { color: #16A34A; }
.text-red { color: #DC2626; }

/* Rank info */
.rank-info {
  font-size: 13px;
  color: #64748B;
  margin: 8px 0;
  padding: 6px 12px;
  background: #F8FAFC;
  border-radius: 4px;
}

/* Institution flow */
.inst-flow-grid {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-top: 6px;
}
.inst-item {
  font-size: 12px;
  color: #334155;
  padding: 3px 8px;
  background: #F8FAFC;
  border-radius: 4px;
  border: 1px solid #E2E8F0;
}
.inst-badge {
  font-size: 11px;
  font-weight: 600;
  padding: 1px 5px;
  border-radius: 3px;
  margin-right: 4px;
}
.inst-item.new .inst-badge { background: #DCFCE7; color: #15803D; }
.inst-item.increase .inst-badge { background: #DBEAFE; color: #1D4ED8; }
.inst-item.decrease .inst-badge { background: #FEF3C7; color: #92400E; }
.inst-item.exit .inst-badge { background: #FEE2E2; color: #B91C1C; }

/* Rotation rating */
.rotation-rating {
  font-size: 11px;
  font-weight: 600;
  padding: 2px 8px;
  border-radius: 3px;
}
.rotation-rating.strong, .rotation-rating.bullish { background: #DCFCE7; color: #15803D; }
.rotation-rating.neutral { background: #F1F5F9; color: #64748B; }
.rotation-rating.weak, .rotation-rating.bearish { background: #FEE2E2; color: #B91C1C; }

/* Confidence breakdown */
.confidence-breakdown {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 8px;
  margin-top: 14px;
  padding: 8px 12px;
  background: #FFFBEB;
  border: 1px solid #FDE68A;
  border-radius: 6px;
  font-size: 12px;
  color: #78716C;
}
.cb-item { white-space: nowrap; }
.cb-final { font-weight: 700; color: #B45309; }

/* Tooltip for 未反应检测 */
.tooltip-wrap {
  position: relative;
  display: inline-block;
  margin-left: 4px;
  cursor: help;
}
.tooltip-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 16px;
  height: 16px;
  border-radius: 50%;
  background: #CBD5E1;
  color: #FFF;
  font-size: 10px;
  font-weight: 700;
  font-style: italic;
  vertical-align: middle;
}
.tooltip-text {
  visibility: hidden;
  opacity: 0;
  position: absolute;
  left: 50%;
  bottom: 130%;
  transform: translateX(-50%);
  width: 280px;
  padding: 10px 12px;
  background: #1E293B;
  color: #F1F5F9;
  font-size: 12px;
  line-height: 1.6;
  border-radius: 6px;
  z-index: 100;
  transition: opacity 0.2s;
  pointer-events: none;
  font-style: normal;
  font-weight: 400;
}
.tooltip-text::after {
  content: '';
  position: absolute;
  top: 100%;
  left: 50%;
  transform: translateX(-50%);
  border: 6px solid transparent;
  border-top-color: #1E293B;
}
.tooltip-wrap:hover .tooltip-text {
  visibility: visible;
  opacity: 1;
}

/* Trace Actions */
.trace-actions {
  margin-top: 20px;
  padding-top: 16px;
  border-top: 1px solid #F0F0F0;
}

.btn-graph {
  width: 100%;
  padding: 10px 16px;
  background: #FFF;
  border: 1px solid #E5E5E5;
  border-radius: 6px;
  font-size: 13px;
  font-weight: 600;
  color: #333;
  cursor: pointer;
  transition: all 0.2s;
  font-family: inherit;
  text-align: center;
}

.btn-graph:hover {
  border-color: #FF4500;
  color: #FF4500;
}

/* Utilities */
.mono { font-family: 'JetBrains Mono', monospace; }
.positive { color: #22C55E; }
.negative { color: #EF4444; }
</style>
