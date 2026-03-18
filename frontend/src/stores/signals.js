import { defineStore } from 'pinia'
import { ref } from 'vue'
import { getActiveSignals, getSignals, getSignalTrace } from '../api/astrategy.js'

export const useSignalsStore = defineStore('signals', () => {
  const activeSignals = ref([])
  const allSignals = ref([])
  const currentTrace = ref(null)
  const loading = ref(false)

  async function fetchActive() {
    loading.value = true
    try {
      const res = await getActiveSignals()
      activeSignals.value = res.data ?? res
    } catch (e) {
      console.error('获取活跃信号失败:', e)
    } finally {
      loading.value = false
    }
  }

  async function fetchAll(params = {}) {
    loading.value = true
    try {
      const res = await getSignals(params)
      allSignals.value = res.data ?? res
    } catch (e) {
      console.error('获取全部信号失败:', e)
    } finally {
      loading.value = false
    }
  }

  async function fetchTrace(signalId) {
    loading.value = true
    try {
      const res = await getSignalTrace(signalId)
      currentTrace.value = res.data ?? res
    } catch (e) {
      console.error('获取信号追踪失败:', e)
    } finally {
      loading.value = false
    }
  }

  return {
    activeSignals,
    allSignals,
    currentTrace,
    loading,
    fetchActive,
    fetchAll,
    fetchTrace,
  }
})
