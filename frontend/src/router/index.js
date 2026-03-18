import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  {
    path: '/',
    name: 'Dashboard',
    component: () => import('../views/DashboardView.vue'),
  },
  {
    path: '/portfolio',
    name: 'Portfolio',
    component: () => import('../views/PortfolioView.vue'),
  },
  {
    path: '/signals',
    name: 'SignalCenter',
    component: () => import('../views/SignalCenterView.vue'),
  },
  {
    path: '/signals/:signalId',
    name: 'SignalTrace',
    component: () => import('../views/SignalTraceView.vue'),
    props: true,
  },
  {
    path: '/graph',
    name: 'GraphExplorer',
    component: () => import('../views/GraphExplorerView.vue'),
  },
  {
    path: '/events',
    name: 'EventStream',
    component: () => import('../views/EventStreamView.vue'),
  },
  {
    path: '/backtest',
    name: 'Performance',
    component: () => import('../views/PerformanceView.vue'),
    alias: '/performance',
  },
  {
    path: '/settings',
    name: 'Settings',
    component: () => import('../views/SettingsView.vue'),
  },
]

const router = createRouter({
  history: createWebHistory(),
  routes,
})

export default router
