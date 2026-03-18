import { PerformanceSlice, AnalysisDimension, SplitMetrics, HoldingPeriodMetrics, OutlierImpactSnapshot, RelationTypeDistribution, GraphAuditIssue } from "@/types";

// ─── Dashboard Metrics ────────────────────────────────
export const mockISMetrics: SplitMetrics = {
  sharpe: 1.80, win_rate: 0.693, avg_adj_return: 0.0098,
  ic: 0.2206, max_dd: 0.1613, profit_factor: 1.95, n: 75,
};

export const mockOOSMetrics: SplitMetrics = {
  sharpe: 3.06, win_rate: 0.826, avg_adj_return: 0.0379,
  ic: 0.0479, max_dd: 0.0412, profit_factor: 8.56, n: 23,
};

export const mockAllMetrics: SplitMetrics = {
  sharpe: 2.11, win_rate: 0.725, avg_adj_return: 0.0164,
  ic: 0.1551, max_dd: 0.1613, profit_factor: 2.82, n: 98,
};

export const mockHoldingPeriodMetrics: HoldingPeriodMetrics[] = [
  { period: "5D", sharpe: 2.11, win_rate: 0.725, ic: 0.1551 },
  { period: "10D", sharpe: 0.70, win_rate: 0.582, ic: 0.2770 },
  { period: "20D", sharpe: 0.49, win_rate: 0.531, ic: 0.0773 },
];

export const mockOutlierImpact: OutlierImpactSnapshot = {
  original_sharpe: 2.11,
  winsorized_sharpe_1pct: 1.85,
  original_profit_factor: 2.82,
  winsorized_profit_factor_1pct: 2.10,
};

// ─── Analysis Slices ──────────────────────────────────
export const mockAnalysisSlices: Record<AnalysisDimension, PerformanceSlice[]> = {
  hop: [
    { slice_key: "hop=0", n: 14, sharpe: -4.51, win_rate: 0.286, avg_adj_return: -0.0158, avg_excess_return: -0.0128, ic: 0, max_dd: 0.2026, profit_factor: 0.32 },
    { slice_key: "hop=1", n: 37, sharpe: 3.65, win_rate: 0.784, avg_adj_return: 0.0168, avg_excess_return: 0.0118, ic: 0.4263, max_dd: 0.1107, profit_factor: 3.20 },
    { slice_key: "hop=2", n: 31, sharpe: 2.60, win_rate: 0.807, avg_adj_return: 0.0301, avg_excess_return: 0.0262, ic: 0.0336, max_dd: 0.1308, profit_factor: 2.80 },
    { slice_key: "hop=3", n: 16, sharpe: 2.90, win_rate: 0.812, avg_adj_return: 0.0173, avg_excess_return: 0.0165, ic: -0.1231, max_dd: 0.0611, profit_factor: 3.10 },
  ],
  event_type: [
    { slice_key: "scandal", n: 9, sharpe: 11.26, win_rate: 0.889, avg_adj_return: 0.0305, avg_excess_return: 0.0052, ic: 0.20, max_dd: 0.007, profit_factor: 12.5 },
    { slice_key: "supply_shortage", n: 7, sharpe: 3.40, win_rate: 0.857, avg_adj_return: 0.0744, avg_excess_return: 0.0683, ic: 0.10, max_dd: 0.028, profit_factor: 5.2 },
    { slice_key: "price_adjustment", n: 6, sharpe: 6.43, win_rate: 0.833, avg_adj_return: 0.0192, avg_excess_return: 0.0232, ic: 0.30, max_dd: 0.015, profit_factor: 8.0 },
    { slice_key: "product_launch", n: 28, sharpe: 4.22, win_rate: 0.821, avg_adj_return: 0.0235, avg_excess_return: 0.0125, ic: 0.15, max_dd: 0.110, profit_factor: 3.5 },
    { slice_key: "management_change", n: 8, sharpe: 1.74, win_rate: 0.750, avg_adj_return: 0.0072, avg_excess_return: 0.0187, ic: 0.12, max_dd: 0.015, profit_factor: 2.1 },
    { slice_key: "earnings_surprise", n: 10, sharpe: -2.16, win_rate: 0.600, avg_adj_return: -0.0090, avg_excess_return: 0.0043, ic: 0.05, max_dd: 0.026, profit_factor: 0.8 },
    { slice_key: "cooperation", n: 16, sharpe: 0.07, win_rate: 0.500, avg_adj_return: 0.0005, avg_excess_return: 0.0063, ic: 0.08, max_dd: 0.050, profit_factor: 1.0 },
    { slice_key: "technology_breakthrough", n: 4, sharpe: 2.04, win_rate: 0.750, avg_adj_return: 0.0118, avg_excess_return: -0.0001, ic: 0.15, max_dd: 0.041, profit_factor: 2.5 },
    { slice_key: "buyback", n: 3, sharpe: 1.68, win_rate: 0.667, avg_adj_return: 0.0098, avg_excess_return: 0.0061, ic: 0.10, max_dd: 0.038, profit_factor: 2.0 },
  ],
  reacted: [
    { slice_key: "已反应", n: 61, sharpe: 2.54, win_rate: 0.705, avg_adj_return: 0.0245, avg_excess_return: 0.0200, ic: -0.1258, max_dd: 0.1613, profit_factor: 3.0 },
    { slice_key: "未反应", n: 37, sharpe: 2.22, win_rate: 0.757, avg_adj_return: 0.0030, avg_excess_return: 0.0030, ic: -0.1303, max_dd: 0.070, profit_factor: 2.2 },
  ],
  direction: [
    { slice_key: "avoid", n: 59, sharpe: 3.10, win_rate: 0.780, avg_adj_return: 0.0210, avg_excess_return: 0.0150, ic: 0.10, max_dd: 0.070, profit_factor: 3.5 },
    { slice_key: "long", n: 39, sharpe: 0.85, win_rate: 0.641, avg_adj_return: 0.0095, avg_excess_return: 0.0050, ic: 0.20, max_dd: 0.1613, profit_factor: 1.8 },
  ],
};

// ─── Graph Health ─────────────────────────────────────
export const mockRelationDistribution: RelationTypeDistribution[] = [
  { relation: "HOLDS_SHARES", count: 4418, percentage: 91.9 },
  { relation: "COMPETES_WITH", count: 96, percentage: 2.0 },
  { relation: "BELONGS_TO", count: 78, percentage: 1.6 },
  { relation: "SUPPLIES_TO", count: 77, percentage: 1.6 },
  { relation: "COOPERATES_WITH", count: 64, percentage: 1.3 },
  { relation: "RELATES_TO_CONCEPT", count: 58, percentage: 1.2 },
  { relation: "CUSTOMER_OF", count: 14, percentage: 0.3 },
];

export const mockGraphAuditIssues: GraphAuditIssue[] = [
  { issue_type: "dirty_name", node_code: "000858", node_name: "五 粮 液", detail: "display_name 含多余空格", affected_signal_count: 12 },
  { issue_type: "dirty_name", node_code: "000002", node_name: "万  科Ａ", detail: "display_name 含全角字符和多余空格", affected_signal_count: 4 },
  { issue_type: "dirty_name", node_code: "000876", node_name: "新 希 望", detail: "display_name 含多余空格", affected_signal_count: 7 },
  { issue_type: "suspicious_path", path: "000876 → 002550", relation_chain: "HOLDS_SHARES → HOLDS_SHARES", detail: "新希望→千红制药：跨行业（养殖→医药），纯持股关系传播", affected_signal_count: 1, related_event_id: "EVT005" },
  { issue_type: "suspicious_path", path: "000876 → 688300", relation_chain: "HOLDS_SHARES → HOLDS_SHARES", detail: "新希望→联瑞新材：跨行业（养殖→新材料），纯持股关系传播，且为异常值(+42.29%)", affected_signal_count: 1, related_event_id: "EVT005" },
  { issue_type: "suspicious_path", path: "000876 → 688083", relation_chain: "HOLDS_SHARES → HOLDS_SHARES", detail: "新希望→中望软件：跨行业（养殖→软件），纯持股关系传播", affected_signal_count: 1, related_event_id: "EVT005" },
  { issue_type: "low_win_edge", path: "601318 → 601628", relation_chain: "COMPETES_WITH", detail: "中国平安→中国人寿：使用3次，胜率33%", affected_signal_count: 3 },
];
