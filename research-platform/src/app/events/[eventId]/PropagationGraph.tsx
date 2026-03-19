"use client";

import { useRef, useEffect } from "react";
import cytoscape from "cytoscape";

/** 关系类型中文说明 */
const RELATION_LABELS: Record<string, string> = {
  HOLDS_SHARES: "基金持仓",
  COMPETES_WITH: "竞争关系",
  SUPPLIES_TO: "供应商",
  CUSTOMER_OF: "客户",
  COOPERATES_WITH: "合作伙伴",
  BELONGS_TO: "所属板块",
  RELATES_TO_CONCEPT: "关联概念",
};

interface GraphNodeData {
  id: string;
  label: string;
  hop: number;
  direction: string | null;
  isSource: boolean;
  correct: boolean | null;
}

interface GraphEdgeData {
  id: string;
  source: string;
  target: string;
  relation: string;
  fact: string;
}

interface PropagationGraphProps {
  nodes: GraphNodeData[];
  edges: GraphEdgeData[];
}

export function PropagationGraph({ nodes, edges }: PropagationGraphProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<cytoscape.Core | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;

    // 防御性过滤：只保留 source/target 都存在于节点集的边
    const nodeIds = new Set(nodes.map((n) => n.id));
    const safeEdges = edges.filter((e) => nodeIds.has(e.source) && nodeIds.has(e.target));

    const elements: cytoscape.ElementDefinition[] = [
      ...nodes.map((n) => ({
        data: {
          id: n.id,
          label: n.label,
          hop: n.hop,
          direction: n.direction,
          isSource: n.isSource,
          correct: n.correct,
          // 边标签用中文关系名
          relationLabel: "",
        },
      })),
      ...safeEdges.map((e) => ({
        data: {
          id: e.id,
          source: e.source,
          target: e.target,
          relation: e.relation,
          relationLabel: RELATION_LABELS[e.relation] || e.relation,
          fact: e.fact,
        },
      })),
    ];

    const cy = cytoscape({
      container: containerRef.current,
      elements,
      style: [
        // 默认节点样式
        {
          selector: "node",
          style: {
            label: "data(label)",
            "font-size": 10,
            "text-valign": "bottom",
            "text-margin-y": 4,
            width: 30,
            height: 30,
            "background-color": "#94a3b8",
            "border-width": 2,
            "border-color": "#e2e8f0",
            color: "#374151",
          },
        },
        // 方向颜色（非源头节点）
        {
          selector: 'node[direction = "avoid"][!isSource]',
          style: {
            "background-color": "#ef4444",
            "border-color": "#dc2626",
          },
        },
        {
          selector: 'node[direction = "long"][!isSource]',
          style: {
            "background-color": "#10b981",
            "border-color": "#059669",
          },
        },
        // 源头节点（最高优先级，始终黄色）
        {
          selector: "node[?isSource]",
          style: {
            "background-color": "#f59e0b",
            "border-color": "#d97706",
            width: 40,
            height: 40,
            "font-weight": "bold",
            "font-size": 11,
          },
        },
        // 中介节点（机构 inst:: 开头）
        {
          selector: "node[hop = -1]",
          style: {
            "background-color": "#a78bfa",
            "border-color": "#7c3aed",
            width: 24,
            height: 24,
            shape: "diamond",
            "font-size": 9,
            color: "#6b7280",
          },
        },
        // 边样式
        {
          selector: "edge",
          style: {
            "curve-style": "bezier",
            "target-arrow-shape": "triangle",
            "arrow-scale": 0.8,
            width: 1.5,
            "line-color": "#cbd5e1",
            "target-arrow-color": "#cbd5e1",
            label: "data(relationLabel)",
            "font-size": 8,
            "text-rotation": "autorotate",
            color: "#9ca3af",
            "text-margin-y": -8,
          },
        },
        // 基金持仓边用虚线区分
        {
          selector: 'edge[relation = "HOLDS_SHARES"]',
          style: {
            "line-style": "dashed",
            "line-color": "#c4b5fd",
            "target-arrow-color": "#c4b5fd",
          },
        },
      ],
      layout: {
        name: "concentric",
        concentric(node: cytoscape.NodeSingular) {
          const hop = node.data("hop") as number;
          if (hop === 0) return 4;
          if (hop === -1) return 3; // 中介机构
          if (hop === 1) return 3;
          if (hop === 2) return 2;
          return 1;
        },
        levelWidth() {
          return 1;
        },
        minNodeSpacing: 50,
        animate: false,
      },
      userZoomingEnabled: true,
      userPanningEnabled: true,
      boxSelectionEnabled: false,
    });

    cyRef.current = cy;

    return () => {
      cy.destroy();
      cyRef.current = null;
    };
  }, [nodes, edges]);

  return (
    <div className="relative">
      <div ref={containerRef} className="w-full h-[350px] bg-gray-50 dark:bg-gray-900 rounded-lg" />
      <div className="flex items-center gap-4 mt-2 text-[10px] text-gray-400">
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded-full bg-amber-500 inline-block" /> 源头事件
        </span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded bg-purple-400 inline-block rotate-45 scale-75" /> 中介机构
        </span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded-full bg-red-500 inline-block" /> Avoid(做空/规避)
        </span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded-full bg-emerald-500 inline-block" /> Long(做多)
        </span>
        <span className="flex items-center gap-1">
          <span className="w-3 h-3 rounded-full bg-gray-400 inline-block" /> 无信号
        </span>
        <span className="ml-2 border-l border-gray-300 dark:border-gray-600 pl-2">
          <span className="inline-block w-6 border-t-2 border-dashed border-purple-300 mr-1 align-middle" /> 基金持仓
        </span>
        <span>
          <span className="inline-block w-6 border-t-2 border-gray-300 mr-1 align-middle" /> 产业链
        </span>
      </div>
    </div>
  );
}
