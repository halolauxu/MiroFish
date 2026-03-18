import { defineStore } from 'pinia'
import { ref } from 'vue'
import { getGraphData, getGraphNodeNeighbors } from '../api/astrategy.js'

export const useGraphStore = defineStore('graph', () => {
  const nodes = ref([])
  const edges = ref([])
  const selectedNode = ref(null)
  const relationFilters = ref([])
  const loading = ref(false)

  async function fetchGraph(params = {}) {
    loading.value = true
    try {
      const res = await getGraphData(params)
      const d = res.data ?? res
      nodes.value = d.nodes ?? []
      edges.value = d.edges ?? d.links ?? []
    } catch (e) {
      console.error('获取图谱数据失败:', e)
    } finally {
      loading.value = false
    }
  }

  async function fetchNeighbors(nodeId) {
    loading.value = true
    try {
      const res = await getGraphNodeNeighbors(nodeId)
      const d = res.data ?? res
      return d
    } catch (e) {
      console.error('获取邻居节点失败:', e)
    } finally {
      loading.value = false
    }
  }

  function selectNode(node) {
    selectedNode.value = node
  }

  function setRelationFilters(filters) {
    relationFilters.value = filters
  }

  return {
    nodes,
    edges,
    selectedNode,
    relationFilters,
    loading,
    fetchGraph,
    fetchNeighbors,
    selectNode,
    setRelationFilters,
  }
})
