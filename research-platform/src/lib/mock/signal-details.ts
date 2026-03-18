import { SignalDetailPayload } from "@/types";
import { mockSignals } from "./signals";

function makeDetail(idx: number, overrides: Partial<SignalDetailPayload>): SignalDetailPayload {
  const base = mockSignals.find((s) => s.idx === idx)!;
  return {
    ...base,
    propagation_path: "",
    path_segments: [],
    shock_weight: 0.5,
    consensus_direction: "neutral",
    conviction: 0.25,
    divergence: 0.0,
    debate_summary: "",
    rule_explanation: { event_type: base.event_type, matched_rule_set: "", inferred_direction: base.signal_direction, rule_label: "" },
    confidence_terms: [],
    entry_price: 0,
    fwd_return_1d: null,
    fwd_return_3d: null,
    fwd_return_20d: null,
    benchmark_5d: null,
    benchmark_10d: null,
    excess_10d: null,
    volume_change_5d: null,
    context_signals: [],
    ...overrides,
  };
}

const evt001Ctx = mockSignals.filter((s) => s.event_id === "EVT001");

export const mockSignalDetails: Record<number, SignalDetailPayload> = {
  // 五粮液 → 古井贡酒 (hop=1, scandal, avoid, correct)
  3: makeDetail(3, {
    propagation_path: "000858 → 000596",
    path_segments: [
      { from_code: "000858", from_name: "五粮液", to_code: "000596", to_name: "古井贡酒", relation: "COMPETES_WITH", shock_weight_at_hop: 0.50, is_suspicious: false },
    ],
    shock_weight: 0.50,
    rule_explanation: { event_type: "scandal", matched_rule_set: "_AVOID_EVENTS", inferred_direction: "avoid", rule_label: "板块连坐" },
    confidence_terms: [
      { name: "conviction", raw_value: 0.40, weight: 0.4, contribution: 0.16 },
      { name: "event_boost", raw_value: 0.35, weight: 1.0, contribution: 0.35 },
      { name: "hop_boost", raw_value: 0.05, weight: 1.0, contribution: 0.05 },
      { name: "shock_weight", raw_value: 0.50, weight: 0.1, contribution: 0.05 },
    ],
    entry_price: 210.50,
    fwd_return_1d: -0.0180,
    fwd_return_3d: -0.0350,
    fwd_return_20d: -0.0680,
    benchmark_5d: -0.0060,
    benchmark_10d: -0.0120,
    excess_10d: -0.0490,
    volume_change_5d: 0.3661,
    context_signals: evt001Ctx,
  }),

  // 五粮液 → 古越龙山 (hop=2, scandal, avoid, correct)
  4: makeDetail(4, {
    propagation_path: "000858 → 000596 → 600059",
    path_segments: [
      { from_code: "000858", from_name: "五粮液", to_code: "000596", to_name: "古井贡酒", relation: "COMPETES_WITH", shock_weight_at_hop: 0.50, is_suspicious: false },
      { from_code: "000596", from_name: "古井贡酒", to_code: "600059", to_name: "古越龙山", relation: "COMPETES_WITH", shock_weight_at_hop: 0.25, is_suspicious: false },
    ],
    shock_weight: 0.25,
    rule_explanation: { event_type: "scandal", matched_rule_set: "_AVOID_EVENTS", inferred_direction: "avoid", rule_label: "板块连坐" },
    entry_price: 15.20,
    fwd_return_1d: -0.0100,
    fwd_return_3d: -0.0210,
    fwd_return_20d: -0.0450,
    benchmark_5d: -0.0060,
    benchmark_10d: -0.0120,
    context_signals: evt001Ctx,
  }),

  // 新希望 → 联瑞新材 (hop=2, supply_shortage, long, correct — 异常值)
  19: makeDetail(19, {
    propagation_path: "000876 → HOLDS_SHARES → 688300",
    path_segments: [
      { from_code: "000876", from_name: "新希望", to_code: "300142", to_name: "沃森生物", relation: "HOLDS_SHARES", shock_weight_at_hop: 0.25, is_suspicious: true, suspicion_reason: "纯持股关系" },
      { from_code: "300142", from_name: "沃森生物", to_code: "688300", to_name: "联瑞新材", relation: "HOLDS_SHARES", shock_weight_at_hop: 0.25, is_suspicious: true, suspicion_reason: "跨行业" },
    ],
    shock_weight: 0.25,
    rule_explanation: { event_type: "supply_shortage", matched_rule_set: "_LONG_EVENTS", inferred_direction: "long", rule_label: "涨价预期" },
    entry_price: 32.80,
    fwd_return_1d: 0.0980,
    fwd_return_3d: 0.2350,
    fwd_return_20d: null,
    benchmark_5d: 0.0060,
    benchmark_10d: 0.0120,
    context_signals: mockSignals.filter((s) => s.event_id === "EVT005"),
  }),

  // 格力回购 → 格力电器 (hop=0, buyback, long, incorrect — 源头误判)
  9: makeDetail(9, {
    propagation_path: "000651 (SOURCE)",
    path_segments: [],
    shock_weight: 1.0,
    rule_explanation: { event_type: "buyback", matched_rule_set: "_NEUTRAL_EVENTS", inferred_direction: "long", rule_label: "利好出尽 → 但源头保留 long" },
    entry_price: 42.10,
    fwd_return_1d: -0.0120,
    fwd_return_3d: -0.0250,
    fwd_return_20d: -0.0410,
    benchmark_5d: 0.0030,
    benchmark_10d: 0.0060,
    context_signals: mockSignals.filter((s) => s.event_id === "EVT003"),
  }),

  // 贵州茅台调价 → 泸州老窖 (IS, hop=1, price_adjustment, avoid, correct)
  24: makeDetail(24, {
    propagation_path: "600519 → 000568",
    path_segments: [
      { from_code: "600519", from_name: "贵州茅台", to_code: "000568", to_name: "泸州老窖", relation: "COMPETES_WITH", shock_weight_at_hop: 0.50, is_suspicious: false },
    ],
    shock_weight: 0.50,
    rule_explanation: { event_type: "price_adjustment", matched_rule_set: "_NEUTRAL_EVENTS", inferred_direction: "avoid", rule_label: "利好出尽" },
    entry_price: 120.0,
    fwd_return_1d: -0.0077,
    fwd_return_3d: -0.005,
    fwd_return_20d: null,
    benchmark_5d: 0.006,
    benchmark_10d: 0.0358,
    excess_10d: -0.0705,
    context_signals: mockSignals.filter((s) => s.event_id === "EVT006"),
  }),
};

/** 获取信号详情（带 fallback） */
export function getSignalDetail(idx: number): SignalDetailPayload {
  if (mockSignalDetails[idx]) return mockSignalDetails[idx];

  // 自动生成 fallback
  const base = mockSignals.find((s) => s.idx === idx);
  if (!base) throw new Error(`Signal idx=${idx} not found`);

  const eventSignals = mockSignals.filter((s) => s.event_id === base.event_id);

  const ruleSetMap: Record<string, string> = {
    scandal: "_AVOID_EVENTS", policy_risk: "_AVOID_EVENTS", management_change: "_AVOID_EVENTS",
    product_launch: "_NEUTRAL_EVENTS", technology_breakthrough: "_NEUTRAL_EVENTS",
    price_adjustment: "_NEUTRAL_EVENTS", buyback: "_NEUTRAL_EVENTS",
    cooperation: "_LONG_EVENTS", earnings_surprise: "_LONG_EVENTS",
    supply_shortage: "_LONG_EVENTS", order_win: "_LONG_EVENTS",
  };
  const ruleLabelMap: Record<string, string> = {
    scandal: "板块连坐", policy_risk: "板块连坐", management_change: "板块连坐",
    product_launch: "利好出尽", technology_breakthrough: "利好出尽",
    price_adjustment: "利好出尽", buyback: "利好出尽",
    cooperation: "合作利好", earnings_surprise: "业绩利好",
    supply_shortage: "涨价预期", order_win: "订单利好",
  };

  return makeDetail(idx, {
    propagation_path: base.relation_chain === "SOURCE" ? `${base.target_code} (SOURCE)` : `${base.source_code} → ${base.target_code}`,
    rule_explanation: {
      event_type: base.event_type,
      matched_rule_set: ruleSetMap[base.event_type] || "_LONG_EVENTS",
      inferred_direction: base.signal_direction,
      rule_label: ruleLabelMap[base.event_type] || "默认规则",
    },
    entry_price: 100,
    context_signals: eventSignals,
  });
}
