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
  | "supply_shortage";

export type SignalDirection = "avoid" | "long";
export type SplitType = "is" | "oos" | "all";
export type OutlierMode = "include" | "exclude";

// ─── Event ────────────────────────────────────────────
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
  signal_count: number;
  win_rate: number;
  avg_adj_return: number;
  split: "is" | "oos";
}

// ─── Signal ───────────────────────────────────────────
export interface SignalRecord {
  idx: number;
  event_id: string;
  source_event: string;
  source_code: string;
  source_name: string;
  event_type: EventType;
  event_date: string;
  target_code: string;
  target_name: string;
  hop: number;
  relation_chain: string;
  signal_direction: SignalDirection;
  confidence: number;
  fwd_return_5d: number | null;
  fwd_return_10d: number | null;
  adj_return_5d: number | null;
  excess_5d: number | null;
  reacted: boolean;
  correct: boolean;
  split: "is" | "oos";
}

// ─── Signal Detail ────────────────────────────────────
export interface PathSegment {
  from_code: string;
  from_name: string;
  to_code: string;
  to_name: string;
  relation: string;
  shock_weight_at_hop: number;
  is_suspicious: boolean;
  suspicion_reason?: string;
}

export interface RuleExplanation {
  event_type: EventType;
  matched_rule_set: string;
  inferred_direction: SignalDirection;
  rule_label: string;
}

export interface ConfidenceTerm {
  name: string;
  raw_value: number;
  weight: number;
  contribution: number;
}

export interface SignalDetailPayload extends SignalRecord {
  propagation_path: string;
  path_segments: PathSegment[];
  shock_weight: number;
  consensus_direction: string;
  conviction: number;
  divergence: number;
  debate_summary: string;
  rule_explanation: RuleExplanation;
  confidence_terms: ConfidenceTerm[];
  entry_price: number;
  fwd_return_1d: number | null;
  fwd_return_3d: number | null;
  fwd_return_20d: number | null;
  benchmark_5d: number | null;
  benchmark_10d: number | null;
  excess_10d: number | null;
  volume_change_5d: number | null;
  context_signals: SignalRecord[];
}

// ─── Analysis ─────────────────────────────────────────
export interface PerformanceSlice {
  slice_key: string;
  n: number;
  sharpe: number;
  win_rate: number;
  avg_adj_return: number;
  avg_excess_return: number;
  ic: number;
  max_dd: number;
  profit_factor: number;
}

export type AnalysisDimension = "hop" | "event_type" | "reacted" | "direction";

// ─── Graph Health ─────────────────────────────────────
export interface GraphAuditIssue {
  issue_type: "dirty_name" | "suspicious_path" | "low_win_edge";
  node_code?: string;
  node_name?: string;
  path?: string;
  relation_chain?: string;
  detail: string;
  affected_signal_count: number;
  related_event_id?: string;
}

export interface RelationTypeDistribution {
  relation: string;
  count: number;
  percentage: number;
}

// ─── Propagation ──────────────────────────────────────
export interface PropagationNodeRecord {
  code: string;
  name: string;
  hop: number;
  is_source: boolean;
  signal_direction: SignalDirection | null;
  fwd_return_5d: number | null;
  correct: boolean | null;
  shock_weight: number;
}

export interface PropagationEdgeRecord {
  from_code: string;
  to_code: string;
  relation: string;
  shock_weight: number;
  is_suspicious: boolean;
  suspicion_reason?: string;
}

// ─── Outlier ──────────────────────────────────────────
export interface OutlierImpactSnapshot {
  original_sharpe: number;
  winsorized_sharpe_1pct: number;
  original_profit_factor: number;
  winsorized_profit_factor_1pct: number;
}

// ─── Dashboard Metrics ────────────────────────────────
export interface SplitMetrics {
  sharpe: number;
  win_rate: number;
  avg_adj_return: number;
  ic: number;
  max_dd: number;
  profit_factor: number;
  n: number;
}

export interface HoldingPeriodMetrics {
  period: string;
  sharpe: number;
  win_rate: number;
  ic: number;
}
