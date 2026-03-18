<template>
  <div class="app-shell">
    <!-- 左侧导航栏 -->
    <aside class="sidebar">
      <div class="sidebar-logo">
        <svg class="logo-icon" viewBox="0 0 32 32" fill="none">
          <rect width="32" height="32" rx="6" fill="#FF4500" />
          <path d="M8 22V10l8 6-8 6z" fill="#fff" />
          <path d="M16 22V10l8 6-8 6z" fill="rgba(255,255,255,0.5)" />
        </svg>
        <span class="logo-text">MiroFish</span>
      </div>

      <nav class="sidebar-nav">
        <router-link
          v-for="item in navItems"
          :key="item.path"
          :to="item.path"
          class="nav-item"
          :class="{ active: isActive(item.path) }"
        >
          <span class="nav-icon" v-html="item.icon"></span>
          <span class="nav-label">{{ item.label }}</span>
        </router-link>
      </nav>

      <div class="sidebar-footer">
        <div class="version">v0.1</div>
      </div>
    </aside>

    <!-- 右侧主区域 -->
    <div class="main-wrapper">
      <!-- 顶部状态栏 -->
      <header class="topbar">
        <div class="topbar-left">
          <h2 class="page-title">{{ currentPageTitle }}</h2>
        </div>
        <div class="topbar-right">
          <div class="status-item">
            <span
              class="status-dot"
              :class="systemStore.isOnline ? 'online' : 'offline'"
            ></span>
            <span class="status-label">{{ systemStore.isOnline ? '运行中' : '离线' }}</span>
          </div>
          <div class="status-item">
            <span class="status-icon">
              <svg viewBox="0 0 16 16" width="14" height="14" fill="currentColor">
                <path d="M8 1a1 1 0 011 1v5.586l2.707 2.707a1 1 0 01-1.414 1.414l-3-3A1 1 0 017 8V2a1 1 0 011-1z"/>
                <path d="M8 15A7 7 0 108 1a7 7 0 000 14zm0-1.5A5.5 5.5 0 118 2.5a5.5 5.5 0 010 11z" fill-rule="evenodd"/>
              </svg>
            </span>
            <span class="status-value">{{ activeSignalCount }} 信号</span>
          </div>
          <div class="status-item" v-if="lastRunDisplay">
            <span class="status-label-muted">最近运行</span>
            <span class="status-value">{{ lastRunDisplay }}</span>
          </div>
        </div>
      </header>

      <!-- 主内容区 -->
      <main class="main-content">
        <router-view />
      </main>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { useSystemStore } from './stores/system.js'
import { useSignalsStore } from './stores/signals.js'

const route = useRoute()
const systemStore = useSystemStore()
const signalsStore = useSignalsStore()

// ── 导航配置 ──
const navItems = [
  {
    path: '/',
    label: '仪表盘',
    icon: '<svg viewBox="0 0 20 20" width="20" height="20" fill="currentColor"><path d="M3 4a1 1 0 011-1h4a1 1 0 011 1v4a1 1 0 01-1 1H4a1 1 0 01-1-1V4zm8 0a1 1 0 011-1h4a1 1 0 011 1v2a1 1 0 01-1 1h-4a1 1 0 01-1-1V4zM3 12a1 1 0 011-1h4a1 1 0 011 1v2a1 1 0 01-1 1H4a1 1 0 01-1-1v-2zm8-2a1 1 0 011-1h4a1 1 0 011 1v4a1 1 0 01-1 1h-4a1 1 0 01-1-1v-4z"/></svg>',
  },
  {
    path: '/portfolio',
    label: '持仓',
    icon: '<svg viewBox="0 0 20 20" width="20" height="20" fill="currentColor"><path d="M4 4a2 2 0 00-2 2v1h16V6a2 2 0 00-2-2H4z"/><path fill-rule="evenodd" d="M18 9H2v5a2 2 0 002 2h12a2 2 0 002-2V9zM4 13a1 1 0 011-1h1a1 1 0 110 2H5a1 1 0 01-1-1zm5-1a1 1 0 100 2h1a1 1 0 100-2H9z" clip-rule="evenodd"/></svg>',
  },
  {
    path: '/signals',
    label: '信号',
    icon: '<svg viewBox="0 0 20 20" width="20" height="20" fill="currentColor"><path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z"/></svg>',
  },
  {
    path: '/graph',
    label: '图谱',
    icon: '<svg viewBox="0 0 20 20" width="20" height="20" fill="currentColor"><path fill-rule="evenodd" d="M17.778 8.222c-4.296-2.453-7.418-2.557-10.556 0-3.137 2.557-5.555 2.453-5.555 0 0-2.453 2.418-6.667 8.334-6.667 5.916 0 10.741 3.66 10.741 6.667 0 2.453-1.296 1.573-2.964 0z" clip-rule="evenodd"/><circle cx="5" cy="14" r="2.5"/><circle cx="15" cy="14" r="2.5"/><circle cx="10" cy="6" r="2"/></svg>',
  },
  {
    path: '/events',
    label: '事件',
    icon: '<svg viewBox="0 0 20 20" width="20" height="20" fill="currentColor"><path fill-rule="evenodd" d="M11.3 1.046A1 1 0 0112 2v5h4a1 1 0 01.82 1.573l-7 10A1 1 0 018 18v-5H4a1 1 0 01-.82-1.573l7-10a1 1 0 011.12-.38z" clip-rule="evenodd"/></svg>',
  },
  {
    path: '/backtest',
    label: '绩效',
    icon: '<svg viewBox="0 0 20 20" width="20" height="20" fill="currentColor"><path d="M2 11a1 1 0 011-1h2a1 1 0 011 1v5a1 1 0 01-1 1H3a1 1 0 01-1-1v-5zm6-4a1 1 0 011-1h2a1 1 0 011 1v9a1 1 0 01-1 1H9a1 1 0 01-1-1V7zm6-3a1 1 0 011-1h2a1 1 0 011 1v12a1 1 0 01-1 1h-2a1 1 0 01-1-1V4z"/></svg>',
  },
  {
    path: '/settings',
    label: '设置',
    icon: '<svg viewBox="0 0 20 20" width="20" height="20" fill="currentColor"><path fill-rule="evenodd" d="M11.49 3.17c-.38-1.56-2.6-1.56-2.98 0a1.532 1.532 0 01-2.286.948c-1.372-.836-2.942.734-2.106 2.106.54.886.061 2.042-.947 2.287-1.561.379-1.561 2.6 0 2.978a1.532 1.532 0 01.947 2.287c-.836 1.372.734 2.942 2.106 2.106a1.532 1.532 0 012.287.947c.379 1.561 2.6 1.561 2.978 0a1.533 1.533 0 012.287-.947c1.372.836 2.942-.734 2.106-2.106a1.533 1.533 0 01.947-2.287c1.561-.379 1.561-2.6 0-2.978a1.532 1.532 0 01-.947-2.287c.836-1.372-.734-2.942-2.106-2.106a1.532 1.532 0 01-2.287-.947zM10 13a3 3 0 100-6 3 3 0 000 6z" clip-rule="evenodd"/></svg>',
  },
]

// ── 路由匹配 ──
function isActive(path) {
  if (path === '/') return route.path === '/'
  return route.path.startsWith(path)
}

const pageTitleMap = {
  '/': '仪表盘',
  '/portfolio': '持仓管理',
  '/signals': '信号中心',
  '/graph': '图谱探索',
  '/events': '事件流',
  '/backtest': '策略绩效',
  '/settings': '系统设置',
}

const currentPageTitle = computed(() => {
  if (route.path.startsWith('/signals/') && route.params.signalId) {
    return '信号追踪'
  }
  return pageTitleMap[route.path] || '仪表盘'
})

// ── 状态栏数据 ──
const activeSignalCount = computed(() => systemStore.signalCount)

const lastRunDisplay = computed(() => {
  const t = systemStore.lastRunTime
  if (!t) return null
  try {
    const d = new Date(t)
    const now = new Date()
    const diff = Math.floor((now - d) / 60000)
    if (diff < 1) return '刚刚'
    if (diff < 60) return `${diff} 分钟前`
    if (diff < 1440) return `${Math.floor(diff / 60)} 小时前`
    return d.toLocaleDateString('zh-CN', { month: 'short', day: 'numeric' })
  } catch {
    return String(t)
  }
})

// ── 初始化 ──
onMounted(() => {
  systemStore.fetchStatus()
  signalsStore.fetchActive()
})
</script>

<style>
/* ── 全局重置 ── */
* {
  margin: 0;
  padding: 0;
  box-sizing: border-box;
}

html, body {
  height: 100%;
  overflow: hidden;
}

#app {
  height: 100%;
  font-family: 'Space Grotesk', 'Noto Sans SC', -apple-system, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  color: #111;
  background: #fff;
}

code, pre, .mono {
  font-family: 'JetBrains Mono', 'SF Mono', monospace;
}

::-webkit-scrollbar {
  width: 6px;
  height: 6px;
}
::-webkit-scrollbar-track {
  background: transparent;
}
::-webkit-scrollbar-thumb {
  background: #d0d0d0;
  border-radius: 3px;
}
::-webkit-scrollbar-thumb:hover {
  background: #aaa;
}
</style>

<style scoped>
/* ── AppShell 布局 ── */
.app-shell {
  display: flex;
  height: 100vh;
  width: 100vw;
  overflow: hidden;
}

/* ── 左侧边栏 ── */
.sidebar {
  width: 200px;
  min-width: 200px;
  height: 100vh;
  background: #111;
  display: flex;
  flex-direction: column;
  z-index: 100;
}

.sidebar-logo {
  height: 64px;
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 0 16px;
  border-bottom: 1px solid rgba(255, 255, 255, 0.06);
}

.logo-icon {
  width: 28px;
  height: 28px;
  flex-shrink: 0;
}

.logo-text {
  font-family: 'Space Grotesk', sans-serif;
  font-size: 15px;
  font-weight: 700;
  color: #fff;
  letter-spacing: 0.5px;
}

/* ── 导航 ── */
.sidebar-nav {
  flex: 1;
  padding: 12px 8px;
  display: flex;
  flex-direction: column;
  gap: 2px;
  overflow-y: auto;
}

.nav-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  border-radius: 8px;
  color: rgba(255, 255, 255, 0.5);
  text-decoration: none;
  font-size: 13px;
  font-weight: 500;
  transition: all 0.15s ease;
  cursor: pointer;
}

.nav-item:hover {
  color: rgba(255, 255, 255, 0.85);
  background: rgba(255, 255, 255, 0.06);
}

.nav-item.active {
  color: #FF4500;
  background: rgba(255, 69, 0, 0.08);
}

.nav-icon {
  width: 20px;
  height: 20px;
  flex-shrink: 0;
  display: flex;
  align-items: center;
  justify-content: center;
}

.nav-icon :deep(svg) {
  display: block;
}

.nav-label {
  white-space: nowrap;
  font-family: 'Noto Sans SC', sans-serif;
}

/* ── 侧边栏底部 ── */
.sidebar-footer {
  padding: 12px 16px;
  border-top: 1px solid rgba(255, 255, 255, 0.06);
}

.version {
  font-family: 'JetBrains Mono', monospace;
  font-size: 11px;
  color: rgba(255, 255, 255, 0.2);
}

/* ── 右侧主区域 ── */
.main-wrapper {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
  min-width: 0;
}

/* ── 顶部状态栏 ── */
.topbar {
  height: 48px;
  min-height: 48px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 24px;
  border-bottom: 1px solid #eee;
  background: #fff;
}

.topbar-left {
  display: flex;
  align-items: center;
}

.page-title {
  font-family: 'Noto Sans SC', sans-serif;
  font-size: 15px;
  font-weight: 600;
  color: #111;
}

.topbar-right {
  display: flex;
  align-items: center;
  gap: 20px;
}

.status-item {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
}

.status-dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
}

.status-dot.online {
  background: #22c55e;
  box-shadow: 0 0 6px rgba(34, 197, 94, 0.4);
}

.status-dot.offline {
  background: #aaa;
}

.status-label {
  color: #555;
  font-family: 'Noto Sans SC', sans-serif;
}

.status-label-muted {
  color: #aaa;
  font-family: 'Noto Sans SC', sans-serif;
}

.status-icon {
  color: #FF4500;
  display: flex;
  align-items: center;
}

.status-value {
  color: #333;
  font-family: 'JetBrains Mono', monospace;
  font-weight: 500;
}

/* ── 主内容 ── */
.main-content {
  flex: 1;
  overflow-y: auto;
  background: #fafafa;
}
</style>
