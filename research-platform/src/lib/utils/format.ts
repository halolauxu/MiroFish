/** Format number as percentage string */
export function fmtPct(value: number | null | undefined, decimals = 1): string {
  if (value == null) return "-";
  return `${(value * 100).toFixed(decimals)}%`;
}

/** Format return value with sign and color class */
export function fmtReturn(value: number | null | undefined, decimals = 2): string {
  if (value == null) return "-";
  const pct = (value * 100).toFixed(decimals);
  return value >= 0 ? `+${pct}%` : `${pct}%`;
}

/** Get color class for return value */
export function returnColor(value: number | null | undefined): string {
  if (value == null) return "text-gray-400";
  return value >= 0 ? "text-emerald-600" : "text-red-600";
}

/** Format Sharpe ratio */
export function fmtSharpe(value: number | null | undefined): string {
  if (value == null) return "-";
  return value.toFixed(2);
}

/** Get color class for Sharpe */
export function sharpeColor(value: number | null | undefined): string {
  if (value == null) return "text-gray-400";
  if (value < 0) return "text-red-600 font-semibold";
  if (value > 2) return "text-emerald-600 font-semibold";
  return "text-gray-900";
}

/** Event type display name */
export const EVENT_TYPE_LABELS: Record<string, string> = {
  scandal: "丑闻",
  product_launch: "产品发布",
  policy_risk: "政策风险",
  cooperation: "合作",
  price_adjustment: "价格调整",
  buyback: "回购",
  earnings_surprise: "业绩预增",
  order_win: "获得订单",
  technology_breakthrough: "技术突破",
  management_change: "管理层变动",
  supply_shortage: "供应短缺",
};

/** Event type color */
export const EVENT_TYPE_COLORS: Record<string, string> = {
  scandal: "bg-red-100 text-red-700",
  policy_risk: "bg-orange-100 text-orange-700",
  management_change: "bg-amber-100 text-amber-700",
  product_launch: "bg-blue-100 text-blue-700",
  technology_breakthrough: "bg-indigo-100 text-indigo-700",
  price_adjustment: "bg-violet-100 text-violet-700",
  buyback: "bg-purple-100 text-purple-700",
  cooperation: "bg-emerald-100 text-emerald-700",
  earnings_surprise: "bg-teal-100 text-teal-700",
  supply_shortage: "bg-rose-100 text-rose-700",
  order_win: "bg-cyan-100 text-cyan-700",
};
