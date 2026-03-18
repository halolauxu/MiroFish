import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { getSystemStatus, getGraphStats } from '../api/astrategy.js'

export const useSystemStore = defineStore('system', () => {
  const status = ref({
    online: false,
    signalCount: 0,
    lastRunTime: null,
    graphStats: null,
  })
  const loading = ref(false)

  const isOnline = computed(() => status.value.online)
  const signalCount = computed(() => status.value.signalCount)
  const lastRunTime = computed(() => status.value.lastRunTime)

  async function fetchStatus() {
    loading.value = true
    try {
      const [sysRes, graphRes] = await Promise.allSettled([
        getSystemStatus(),
        getGraphStats(),
      ])

      if (sysRes.status === 'fulfilled') {
        const d = sysRes.value.data ?? sysRes.value
        status.value.online = d.online ?? true
        status.value.signalCount = d.signal_count ?? d.signalCount ?? 0
        status.value.lastRunTime = d.last_run_time ?? d.lastRunTime ?? null
      }

      if (graphRes.status === 'fulfilled') {
        status.value.graphStats = graphRes.value.data ?? graphRes.value
      }
    } catch (e) {
      console.error('获取系统状态失败:', e)
      status.value.online = false
    } finally {
      loading.value = false
    }
  }

  return {
    status,
    loading,
    isOnline,
    signalCount,
    lastRunTime,
    fetchStatus,
  }
})
