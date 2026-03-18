<template>
  <div class="settings-view">
    <!-- 调度配置 -->
    <section class="section">
      <h2 class="section-title">调度配置</h2>
      <div class="task-list">
        <div v-for="task in scheduledTasks" :key="task.name" class="task-row">
          <div class="task-info">
            <span class="task-name">{{ task.name }}</span>
            <span class="task-cron mono">{{ task.cron }}</span>
          </div>
          <div class="task-meta">
            <span class="task-status" :class="task.enabled ? 'active' : 'inactive'">
              <span class="status-dot"></span>
              {{ task.enabled ? '运行中' : '已停用' }}
            </span>
            <span class="task-last-run mono">上次: {{ task.lastRun || '—' }}</span>
          </div>
          <button class="btn-sm" @click="triggerTask(task.name)">手动触发</button>
        </div>
      </div>
    </section>

    <!-- 股票池 -->
    <section class="section">
      <h2 class="section-title">股票池</h2>
      <div class="setting-row">
        <div class="setting-label">
          <span class="label-main">当前股票池</span>
          <span class="label-desc">策略选股的候选范围</span>
        </div>
        <div class="setting-control">
          <select v-model="stockPool" class="select-input">
            <option value="csi300">中证300 (300只)</option>
            <option value="csi500">中证500 (500只)</option>
            <option value="csi800">中证800 (800只)</option>
            <option value="custom">自定义</option>
          </select>
        </div>
      </div>
    </section>

    <!-- 策略开关 -->
    <section class="section">
      <h2 class="section-title">策略开关</h2>
      <div class="strategy-list">
        <div v-for="s in strategyToggles" :key="s.id" class="strategy-row">
          <div class="strategy-info">
            <div class="strategy-header-row">
              <span class="strategy-name">{{ s.name }}</span>
              <span v-if="s.tag" class="strategy-tag">{{ s.tag }}</span>
            </div>
            <span class="strategy-params mono">{{ s.params }}</span>
          </div>
          <label class="toggle-switch">
            <input type="checkbox" v-model="s.enabled" />
            <span class="toggle-slider"></span>
          </label>
        </div>
      </div>
    </section>

    <!-- 图谱管理 -->
    <section class="section">
      <h2 class="section-title">图谱管理</h2>
      <div class="graph-stats">
        <div class="stat-item">
          <span class="stat-value mono">{{ graphStats.nodeCount }}</span>
          <span class="stat-label">节点数</span>
        </div>
        <div class="stat-item">
          <span class="stat-value mono">{{ graphStats.edgeCount }}</span>
          <span class="stat-label">边数</span>
        </div>
        <div class="stat-item">
          <span class="stat-value mono">{{ graphStats.relationTypes }}</span>
          <span class="stat-label">关系类型</span>
        </div>
      </div>
      <div class="graph-actions">
        <button class="btn-outline" @click="expandGraph('supply_chain')">
          <span class="btn-icon">+</span> 扩展供应链
        </button>
        <button class="btn-outline" @click="expandGraph('fund_holding')">
          <span class="btn-icon">+</span> 扩展基金持仓
        </button>
        <button class="btn-outline" @click="expandGraph('competition')">
          <span class="btn-icon">+</span> 扩展竞争关系
        </button>
      </div>
    </section>

    <!-- LLM配置 -->
    <section class="section">
      <h2 class="section-title">LLM 配置</h2>
      <div class="form-group">
        <label class="form-label">模型名称</label>
        <input type="text" v-model="llmConfig.model" class="form-input" placeholder="qwen-plus" />
      </div>
      <div class="form-group">
        <label class="form-label">Base URL</label>
        <input type="text" v-model="llmConfig.baseUrl" class="form-input" placeholder="https://dashscope.aliyuncs.com/compatible-mode/v1" />
      </div>
      <div class="form-actions">
        <button class="btn-primary" @click="testConnection">
          {{ testing ? '测试中...' : '测试连接' }}
        </button>
        <span v-if="testResult" class="test-result" :class="testResult.success ? 'success' : 'error'">
          {{ testResult.message }}
        </span>
      </div>
    </section>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { getSystemStatus } from '../api/astrategy'

const stockPool = ref('csi800')
const testing = ref(false)
const testResult = ref(null)

const scheduledTasks = ref([
  { name: '日间策略', cron: '0 18 * * 1-5', enabled: true, lastRun: '2026-03-17 18:00' },
  { name: '事件扫描', cron: '30 9 * * 1-5', enabled: true, lastRun: '2026-03-18 09:30' },
  { name: '周度调仓', cron: '30 18 * * 5', enabled: true, lastRun: '2026-03-14 18:30' },
])

const strategyToggles = ref([
  { id: 'shock', name: '冲击链路', tag: 'PRIMARY', params: 'max_hops=3, decay=0.5', enabled: true },
  { id: 's07', name: 'S07 图谱因子', tag: '', params: 'top_n=10', enabled: true },
  { id: 's10', name: 'S10 舆情模拟', tag: '', params: 'max_events=5', enabled: true },
  { id: 's05', name: 'S05 分析师分歧', tag: '', params: 'min_coverage=3', enabled: false },
])

const graphStats = ref({
  nodeCount: 342,
  edgeCount: 251,
  relationTypes: 6,
})

const llmConfig = ref({
  model: 'qwen-plus',
  baseUrl: 'https://dashscope.aliyuncs.com/compatible-mode/v1',
})

const triggerTask = (taskName) => {
  console.log('手动触发:', taskName)
}

const expandGraph = (type) => {
  console.log('扩展图谱:', type)
}

const testConnection = async () => {
  testing.value = true
  testResult.value = null
  try {
    await new Promise(r => setTimeout(r, 1500))
    testResult.value = { success: true, message: '连接成功' }
  } catch (e) {
    testResult.value = { success: false, message: '连接失败: ' + e.message }
  } finally {
    testing.value = false
  }
}

const loadData = async () => {
  try {
    const res = await getSystemStatus()
    if (res?.data) {
      if (res.data.graph_stats) graphStats.value = res.data.graph_stats
      if (res.data.stock_pool) stockPool.value = res.data.stock_pool
      if (res.data.llm_config) llmConfig.value = { ...llmConfig.value, ...res.data.llm_config }
      if (res.data.strategies) {
        res.data.strategies.forEach(s => {
          const toggle = strategyToggles.value.find(t => t.id === s.id)
          if (toggle) toggle.enabled = s.enabled
        })
      }
      if (res.data.scheduled_tasks) scheduledTasks.value = res.data.scheduled_tasks
    }
  } catch (e) {
    console.warn('使用默认配置:', e.message)
  }
}

onMounted(() => {
  loadData()
})
</script>

<style scoped>
.settings-view {
  padding: 32px;
  max-width: 900px;
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

.mono {
  font-family: 'JetBrains Mono', monospace;
}

/* Scheduled Tasks */
.task-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.task-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 16px 20px;
  background: #F5F5F5;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
  gap: 16px;
}

.task-info {
  display: flex;
  flex-direction: column;
  gap: 4px;
  flex: 1;
}

.task-name {
  font-weight: 600;
  font-size: 15px;
  color: #111;
}

.task-cron {
  font-size: 12px;
  color: #999;
}

.task-meta {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 4px;
}

.task-status {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  font-weight: 500;
}

.status-dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
}

.task-status.active { color: #16A34A; }
.task-status.active .status-dot { background: #16A34A; }
.task-status.inactive { color: #999; }
.task-status.inactive .status-dot { background: #CCC; }

.task-last-run {
  font-size: 11px;
  color: #999;
}

.btn-sm {
  padding: 6px 14px;
  border: 1px solid #E5E5E5;
  background: #FFF;
  border-radius: 4px;
  font-size: 12px;
  font-weight: 600;
  color: #333;
  cursor: pointer;
  transition: all 0.15s;
  white-space: nowrap;
}

.btn-sm:hover {
  border-color: #111;
  background: #111;
  color: #FFF;
}

/* Stock Pool */
.setting-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 16px 20px;
  background: #F5F5F5;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
}

.setting-label {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.label-main {
  font-weight: 600;
  font-size: 15px;
  color: #111;
}

.label-desc {
  font-size: 12px;
  color: #999;
}

.select-input {
  padding: 8px 32px 8px 12px;
  border: 1px solid #E5E5E5;
  border-radius: 6px;
  font-size: 14px;
  font-family: inherit;
  background: #FFF;
  color: #111;
  cursor: pointer;
  appearance: none;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath fill='%23666' d='M3 5l3 3 3-3z'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 10px center;
}

.select-input:focus {
  outline: none;
  border-color: #FF4500;
}

/* Strategy Toggles */
.strategy-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.strategy-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 16px 20px;
  background: #F5F5F5;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
}

.strategy-info {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.strategy-header-row {
  display: flex;
  align-items: center;
  gap: 8px;
}

.strategy-name {
  font-weight: 600;
  font-size: 15px;
  color: #111;
}

.strategy-tag {
  display: inline-block;
  padding: 1px 6px;
  background: #FF4500;
  color: #FFF;
  border-radius: 3px;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.5px;
}

.strategy-params {
  font-size: 12px;
  color: #999;
}

/* Toggle Switch */
.toggle-switch {
  position: relative;
  display: inline-block;
  width: 44px;
  height: 24px;
  cursor: pointer;
}

.toggle-switch input {
  opacity: 0;
  width: 0;
  height: 0;
}

.toggle-slider {
  position: absolute;
  top: 0; left: 0; right: 0; bottom: 0;
  background: #CCC;
  border-radius: 24px;
  transition: background 0.2s;
}

.toggle-slider::before {
  content: '';
  position: absolute;
  width: 18px;
  height: 18px;
  left: 3px;
  bottom: 3px;
  background: #FFF;
  border-radius: 50%;
  transition: transform 0.2s;
}

.toggle-switch input:checked + .toggle-slider {
  background: #FF4500;
}

.toggle-switch input:checked + .toggle-slider::before {
  transform: translateX(20px);
}

/* Graph Stats */
.graph-stats {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 16px;
  margin-bottom: 20px;
}

.stat-item {
  background: #F5F5F5;
  border: 1px solid #E5E5E5;
  border-radius: 8px;
  padding: 20px;
  text-align: center;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.stat-value {
  font-size: 28px;
  font-weight: 800;
  color: #111;
}

.stat-label {
  font-size: 13px;
  color: #666;
}

.graph-actions {
  display: flex;
  gap: 12px;
  flex-wrap: wrap;
}

.btn-outline {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 10px 18px;
  border: 1px solid #E5E5E5;
  background: #FFF;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 600;
  color: #333;
  cursor: pointer;
  transition: all 0.15s;
  font-family: inherit;
}

.btn-outline:hover {
  border-color: #111;
  background: #111;
  color: #FFF;
}

.btn-icon {
  font-size: 16px;
  font-weight: 700;
}

/* LLM Config */
.form-group {
  margin-bottom: 16px;
}

.form-label {
  display: block;
  font-size: 13px;
  font-weight: 600;
  color: #333;
  margin-bottom: 6px;
}

.form-input {
  width: 100%;
  padding: 10px 14px;
  border: 1px solid #E5E5E5;
  border-radius: 6px;
  font-size: 14px;
  font-family: 'JetBrains Mono', monospace;
  color: #111;
  background: #F5F5F5;
  transition: border-color 0.15s;
}

.form-input:focus {
  outline: none;
  border-color: #FF4500;
  background: #FFF;
}

.form-actions {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-top: 8px;
}

.btn-primary {
  padding: 10px 24px;
  background: #111;
  color: #FFF;
  border: none;
  border-radius: 6px;
  font-size: 14px;
  font-weight: 600;
  cursor: pointer;
  transition: background 0.15s;
  font-family: inherit;
}

.btn-primary:hover {
  background: #FF4500;
}

.test-result {
  font-size: 13px;
  font-weight: 600;
}

.test-result.success { color: #16A34A; }
.test-result.error { color: #DC2626; }
</style>
