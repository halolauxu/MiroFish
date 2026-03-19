// ─── Event Types ──────────────────────────────────────
export type EventType =
  | "scandal"
  | "product_launch"
  | "policy_risk"
  | "cooperation"
  | "price_adjustment"
  | "buyback"
  | "earnings_surprise"
  | "order_win"
  | "technology_breakthrough"
  | "management_change"
  | "supply_shortage"
  | "ma";

export type SignalDirection = "avoid" | "long";

// ─── Event (匹配 historical_events.json) ─────────────
export interface EventRecord {
  event_id: string;
  title: string;
  type: EventType;
  stock_code: string;
  stock_name: string;
  event_date: string;
  summary: string;
  key_data: string;
  impact_level: "high" | "medium" | "low";
}

// ─── Signal (匹配 shock_wf_signals 37字段) ───────────
export interface SignalRecord {
  idx: number; // 运行时注入的数组索引
  source_event: string;
  source_code: string;
  source_name: string;
  event_type: string;
  target_code: string;
  target_name: string;
  shock_weight: number;
  hop: number;
  propagation_path: string;
  relation_chain: string;
  consensus_direction: string;
  consensus_sentiment: number;
  divergence: number;
  conviction: number;
  debate_summary: string;
  reacted: boolean;
  return_5d: number;
  volume_change_5d: number;
  signal_direction: string;
  confidence: number;
  alpha_type: string;
  position_hint: string;
  graph_context_used: boolean;
  event_date: string;
  event_id: string;
  entry_price: number;
  fwd_return_1d: number | null;
  fwd_return_3d: number | null;
  fwd_return_5d: number | null;
  fwd_return_10d: number | null;
  fwd_return_20d: number | null;
  benchmark_5d: number | null;
  excess_5d: number | null;
  benchmark_10d: number | null;
  excess_10d: number | null;
  benchmark_20d: number | null;
  excess_20d: number | null;
}

// ─── Graph (匹配 supply_chain.json) ──────────────────
export interface GraphNode {
  name: string;
  labels: string[];
  summary: string;
  attributes: {
    display_name: string;
    industry: string | null;
  };
  display_name: string;
}

export interface GraphEdge {
  source: string;
  target: string;
  source_name: string;
  target_name: string;
  source_display: string;
  target_display: string;
  relation: string;
  fact: string;
  weight: number;
}

export interface GraphStats {
  nodeCount: number;
  edgeCount: number;
  propagationEdgeCount: number;
  relationDistribution: { relation: string; count: number; percentage: number }[];
  dirtyNames: { code: string; displayName: string; issue: string }[];
}

export interface SubgraphData {
  nodes: Record<string, GraphNode>;
  edges: GraphEdge[];
  signals: SignalRecord[];
}

// ─── Report Metrics (从 WF 报告解析) ─────────────────
export interface SplitMetricsRow {
  sharpe_5d: number;
  win_rate_5d: number;
  adj_return_5d: number;
  ic_5d: number;
  max_dd_5d: number;
  pf_5d: number;
  n_5d: number;
  sharpe_10d: number;
  win_rate_10d: number;
  adj_return_10d: number;
  ic_10d: number;
  max_dd_10d: number;
  pf_10d: number;
  n_10d: number;
}

export interface ReportMetrics {
  is: SplitMetricsRow;
  oos: SplitMetricsRow;
  all: SplitMetricsRow;
}
