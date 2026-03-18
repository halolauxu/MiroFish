"use client";
import { useState, useRef, useEffect, useCallback, useMemo } from "react";
import Link from "next/link";
import cytoscape from "cytoscape";
import { mockEvents, mockSignals } from "@/lib/mock";
import { getSignalDetail } from "@/lib/mock";
import { fmtReturn, returnColor, fmtPct } from "@/lib/utils/format";
import { DirectionBadge, HopBadge, EventTypeBadge, CorrectnessIndicator, SplitBadge } from "@/components/badges";
import type { SignalRecord } from "@/types";

// ─── Build graph data from event signals ─────────────
function buildGraphElements(signals: SignalRecord[]) {
  const nodes = new Map<string, { code: string; name: string; hop: number; direction: string | null; correct: boolean | null; isSource: boolean }>();
  const edges: Array<{ source: string; target: string; relation: string; isSuspicious: boolean }> = [];

  for (const sig of signals) {
    // Source node
    if (!nodes.has(sig.source_code)) {
      nodes.set(sig.source_code, {
        code: sig.source_code, name: sig.source_name ?? sig.source_code,
        hop: -1, direction: null, correct: null, isSource: true,
      });
    }
    // Target node
    if (!nodes.has(sig.target_code)) {
      nodes.set(sig.target_code, {
        code: sig.target_code, name: sig.target_name,
        hop: sig.hop, direction: sig.signal_direction, correct: sig.correct, isSource: sig.hop === 0,
      });
    }

    // Try to get path segments for edges
    try {
      const detail = getSignalDetail(sig.idx);
      for (const seg of detail.path_segments) {
        const edgeKey = `${seg.from_code}-${seg.to_code}`;
        if (!edges.find((e) => `${e.source}-${e.target}` === edgeKey)) {
          edges.push({
            source: seg.from_code, target: seg.to_code,
            relation: seg.relation, isSuspicious: seg.is_suspicious,
          });
          // Ensure intermediate nodes exist
          if (!nodes.has(seg.from_code)) {
            nodes.set(seg.from_code, { code: seg.from_code, name: seg.from_name, hop: 0, direction: null, correct: null, isSource: false });
          }
          if (!nodes.has(seg.to_code)) {
            nodes.set(seg.to_code, { code: seg.to_code, name: seg.to_name, hop: sig.hop, direction: sig.signal_direction, correct: sig.correct, isSource: false });
          }
        }
      }
    } catch {
      // Fallback: create direct edge from source to target
      if (sig.relation_chain !== "SOURCE") {
        const edgeKey = `${sig.source_code}-${sig.target_code}`;
        if (!edges.find((e) => `${e.source}-${e.target}` === edgeKey)) {
          edges.push({
            source: sig.source_code, target: sig.target_code,
            relation: sig.relation_chain, isSuspicious: false,
          });
        }
      }
    }
  }

  const cyNodes = Array.from(nodes.values()).map((n) => ({
    data: {
      id: n.code,
      label: `${n.name}\n${n.code}`,
      name: n.name,
      hop: n.hop,
      direction: n.direction,
      correct: n.correct,
      isSource: n.isSource,
    },
  }));

  const cyEdges = edges.map((e) => ({
    data: {
      id: `${e.source}-${e.target}`,
      source: e.source,
      target: e.target,
      label: e.relation.replace(/_/g, "\n"),
      relation: e.relation,
      isSuspicious: e.isSuspicious,
    },
  }));

  return { nodes: cyNodes, edges: cyEdges };
}

// ─── Graph Canvas ────────────────────────────────────
function GraphCanvas({
  signals,
  onNodeClick,
}: {
  signals: SignalRecord[];
  onNodeClick: (nodeId: string | null) => void;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<cytoscape.Core | null>(null);

  const elements = useMemo(() => buildGraphElements(signals), [signals]);

  useEffect(() => {
    if (!containerRef.current) return;

    if (cyRef.current) {
      cyRef.current.destroy();
    }

    const cy = cytoscape({
      container: containerRef.current,
      elements: [...elements.nodes, ...elements.edges],
      style: [
        {
          selector: "node",
          style: {
            label: "data(label)",
            "text-valign": "center",
            "text-halign": "center",
            "font-size": "8px",
            "text-wrap": "wrap",
            "text-max-width": "70px",
            width: 60,
            height: 60,
            "background-color": "#e5e7eb",
            "border-width": 2,
            "border-color": "#9ca3af",
          },
        },
        {
          selector: "node[?isSource]",
          style: {
            "background-color": "#fef3c7",
            "border-color": "#f59e0b",
            "border-width": 3,
            width: 70,
            height: 70,
          },
        },
        {
          selector: 'node[direction = "avoid"]',
          style: {
            "background-color": "#fecaca",
            "border-color": "#ef4444",
          },
        },
        {
          selector: 'node[direction = "long"]',
          style: {
            "background-color": "#bbf7d0",
            "border-color": "#22c55e",
          },
        },
        {
          selector: "edge",
          style: {
            label: "data(label)",
            "font-size": "7px",
            "text-wrap": "wrap",
            "text-rotation": "autorotate",
            "curve-style": "bezier",
            "target-arrow-shape": "triangle",
            "arrow-scale": 0.8,
            "line-color": "#9ca3af",
            "target-arrow-color": "#9ca3af",
            width: 1.5,
          },
        },
        {
          selector: "edge[?isSuspicious]",
          style: {
            "line-color": "#ef4444",
            "target-arrow-color": "#ef4444",
            "line-style": "dashed",
            width: 2,
          },
        },
        {
          selector: ":selected",
          style: {
            "border-color": "#3b82f6",
            "border-width": 4,
            "background-color": "#dbeafe",
          },
        },
      ],
      layout: {
        name: "breadthfirst",
        directed: true,
        padding: 30,
        spacingFactor: 1.2,
      },
      userZoomingEnabled: true,
      userPanningEnabled: true,
      boxSelectionEnabled: false,
    });

    cy.on("tap", "node", (e) => {
      onNodeClick(e.target.id());
    });

    cy.on("tap", (e) => {
      if (e.target === cy) onNodeClick(null);
    });

    cyRef.current = cy;

    return () => {
      cy.destroy();
      cyRef.current = null;
    };
  }, [elements, onNodeClick]);

  return <div ref={containerRef} className="w-full h-full" />;
}

// ─── Node Detail Panel ───────────────────────────────
function NodeDetailPanel({ nodeId, signals }: { nodeId: string | null; signals: SignalRecord[] }) {
  if (!nodeId) {
    return (
      <div className="text-xs text-gray-400 text-center mt-8">
        点击图谱节点查看详情
      </div>
    );
  }

  const nodeSignals = signals.filter((s) => s.target_code === nodeId || s.source_code === nodeId);
  const asTarget = signals.find((s) => s.target_code === nodeId);
  const asSource = signals.find((s) => s.source_code === nodeId);

  return (
    <div className="space-y-3">
      <div>
        <div className="text-sm font-bold text-gray-900">
          {asTarget?.target_name || asSource?.source_name || nodeId}
        </div>
        <div className="text-[10px] text-gray-400 font-mono">{nodeId}</div>
      </div>

      {asTarget && (
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <DirectionBadge direction={asTarget.signal_direction} />
            <HopBadge hop={asTarget.hop} />
            <CorrectnessIndicator correct={asTarget.correct} />
          </div>
          <div className="text-xs space-y-1">
            <div className="flex justify-between">
              <span className="text-gray-400">置信度</span>
              <span className="font-mono">{fmtPct(asTarget.confidence)}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-400">调整收益5D</span>
              <span className={`font-mono ${returnColor(asTarget.adj_return_5d)}`}>
                {fmtReturn(asTarget.adj_return_5d)}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-400">超额 5D</span>
              <span className={`font-mono ${returnColor(asTarget.excess_5d)}`}>
                {fmtReturn(asTarget.excess_5d)}
              </span>
            </div>
          </div>
          <Link
            href={`/signals/${asTarget.idx}`}
            className="block text-xs text-blue-600 hover:underline mt-2"
          >
            查看信号详情 →
          </Link>
        </div>
      )}

      {nodeSignals.length > 1 && (
        <div className="pt-2 border-t border-gray-100">
          <div className="text-[11px] text-gray-400 mb-1">相关信号 ({nodeSignals.length})</div>
          {nodeSignals.slice(0, 5).map((s) => (
            <Link
              key={s.idx}
              href={`/signals/${s.idx}`}
              className="block text-[11px] text-gray-600 hover:text-blue-600 py-0.5"
            >
              #{s.idx} → {s.target_name} ({s.signal_direction})
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}

// ─── Page ────────────────────────────────────────────
export default function PropagationPage() {
  const [selectedEventId, setSelectedEventId] = useState<string>("EVT001");
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);

  const eventSignals = useMemo(
    () => mockSignals.filter((s) => s.event_id === selectedEventId),
    [selectedEventId]
  );

  const selectedEvent = mockEvents.find((e) => e.event_id === selectedEventId);

  const handleNodeClick = useCallback((nodeId: string | null) => {
    setSelectedNodeId(nodeId);
  }, []);

  const oosEvents = mockEvents.filter((e) => e.split === "oos");

  return (
    <div className="space-y-4">
      <h1 className="text-lg font-bold text-gray-900">传播图谱</h1>

      <div className="flex flex-col lg:grid lg:grid-cols-12 gap-4 lg:h-[calc(100vh-180px)]">
        {/* Left: Event Selector */}
        <div className="lg:col-span-3 bg-white rounded-lg border border-gray-200 p-4 overflow-y-auto max-h-[250px] lg:max-h-none">
          <h3 className="text-sm font-bold text-gray-900 mb-3">事件选择</h3>
          <div className="space-y-2">
            {oosEvents.map((ev) => (
              <button
                key={ev.event_id}
                onClick={() => { setSelectedEventId(ev.event_id); setSelectedNodeId(null); }}
                className={`w-full text-left px-3 py-2 text-xs border rounded transition-colors ${
                  selectedEventId === ev.event_id
                    ? "border-blue-400 bg-blue-50 text-blue-900"
                    : "border-gray-100 hover:border-blue-300"
                }`}
              >
                <div className="flex items-center gap-1.5 mb-1">
                  <SplitBadge split={ev.split} />
                  <EventTypeBadge type={ev.type} />
                </div>
                <div className="font-medium truncate">{ev.title}</div>
                <div className="text-gray-400 text-[10px] mt-0.5">
                  {ev.signal_count} 信号 · 胜率 {fmtPct(ev.win_rate)}
                </div>
              </button>
            ))}
          </div>
        </div>

        {/* Center: Graph Canvas */}
        <div className="lg:col-span-6 bg-white rounded-lg border border-gray-200 overflow-hidden min-h-[400px]">
          {eventSignals.length > 0 ? (
            <GraphCanvas signals={eventSignals} onNodeClick={handleNodeClick} />
          ) : (
            <div className="flex items-center justify-center h-full text-gray-400 text-sm">
              该事件暂无信号数据
            </div>
          )}
        </div>

        {/* Right: Node Detail */}
        <div className="lg:col-span-3 bg-white rounded-lg border border-gray-200 p-4 overflow-y-auto">
          <h3 className="text-sm font-bold text-gray-900 mb-3">
            {selectedNodeId ? "节点详情" : "图谱说明"}
          </h3>
          {selectedNodeId ? (
            <NodeDetailPanel nodeId={selectedNodeId} signals={eventSignals} />
          ) : (
            <div className="space-y-3 text-xs text-gray-500">
              <p>当前事件: <strong className="text-gray-900">{selectedEvent?.title}</strong></p>
              <p>信号数: <strong className="text-gray-900">{eventSignals.length}</strong></p>
              <div className="pt-2 border-t border-gray-100">
                <div className="font-semibold text-gray-700 mb-1">图例</div>
                <div className="space-y-1.5">
                  <div className="flex items-center gap-2">
                    <span className="w-4 h-4 rounded-full bg-amber-100 border-2 border-amber-400" />
                    <span>源头事件节点</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="w-4 h-4 rounded-full bg-red-100 border-2 border-red-400" />
                    <span>Avoid 信号</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="w-4 h-4 rounded-full bg-green-100 border-2 border-green-400" />
                    <span>Long 信号</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="w-3 h-0.5 bg-red-400 border-dashed border border-red-400" />
                    <span>可疑路径（红色虚线）</span>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
