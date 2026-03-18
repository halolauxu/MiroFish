"use client";
import { useMemo } from "react";
import Link from "next/link";
import { AlertTriangle, TrendingUp, Target, BarChart3 } from "lucide-react";
import { useGlobalFilter } from "@/lib/query-state";
import { mockISMetrics, mockOOSMetrics, mockAllMetrics, mockHoldingPeriodMetrics, mockOutlierImpact } from "@/lib/mock";
import { mockAnalysisSlices } from "@/lib/mock";
import { fmtPct, fmtReturn, fmtSharpe, sharpeColor, returnColor } from "@/lib/utils/format";
import { MetricCard } from "@/components/metrics";
import { EChart } from "@/components/charts";
import type { SplitMetrics } from "@/types";

// ─── Metrics selector ────────────────────────────────
function useCurrentMetrics(split: string): SplitMetrics {
  if (split === "is") return mockISMetrics;
  if (split === "oos") return mockOOSMetrics;
  return mockAllMetrics;
}

// ─── Outlier Alert ───────────────────────────────────
function OutlierAlert() {
  const impact = mockOutlierImpact;
  const sharpeDelta = impact.original_sharpe - impact.winsorized_sharpe_1pct;
  const pfDelta = impact.original_profit_factor - impact.winsorized_profit_factor_1pct;
  const isWarning = sharpeDelta > 0.3 || pfDelta > 0.5;

  if (!isWarning) return null;

  return (
    <div className="flex items-start gap-2 p-3 bg-amber-50 border border-amber-200 rounded-lg text-xs text-amber-800">
      <AlertTriangle size={16} className="flex-shrink-0 mt-0.5" />
      <div>
        <div className="font-semibold mb-1">异常值影响提醒</div>
        <div className="text-amber-700">
          去极端值后 Sharpe 从 {fmtSharpe(impact.original_sharpe)} → {fmtSharpe(impact.winsorized_sharpe_1pct)}（Δ{sharpeDelta.toFixed(2)}），
          Profit Factor 从 {impact.original_profit_factor.toFixed(2)} → {impact.winsorized_profit_factor_1pct.toFixed(2)}（Δ{pfDelta.toFixed(2)}）。
          建议关注极端收益信号。
        </div>
      </div>
    </div>
  );
}

// ─── IS vs OOS Bar Chart ─────────────────────────────
function SplitComparisonChart() {
  const option = useMemo(() => ({
    tooltip: { trigger: "axis" },
    legend: { top: 0, textStyle: { fontSize: 11 } },
    grid: { top: 30, bottom: 25, left: 50, right: 20 },
    xAxis: {
      type: "category",
      data: ["Sharpe", "胜率(%)", "PF", "IC"],
      axisLabel: { fontSize: 10 },
    },
    yAxis: { type: "value", axisLabel: { fontSize: 10 } },
    series: [
      {
        name: "IS",
        type: "bar",
        data: [mockISMetrics.sharpe, mockISMetrics.win_rate * 100, mockISMetrics.profit_factor, mockISMetrics.ic * 10],
        itemStyle: { color: "#3b82f6" },
        barGap: "10%",
      },
      {
        name: "OOS",
        type: "bar",
        data: [mockOOSMetrics.sharpe, mockOOSMetrics.win_rate * 100, mockOOSMetrics.profit_factor, mockOOSMetrics.ic * 10],
        itemStyle: { color: "#f97316" },
      },
    ],
  }), []);

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <h3 className="text-sm font-bold text-gray-900 mb-2">IS vs OOS 对比</h3>
      <EChart option={option} height={220} />
      <div className="flex items-center justify-center gap-4 mt-1 text-[10px] text-gray-400">
        <span>IS: n={mockISMetrics.n}</span>
        <span>OOS: n={mockOOSMetrics.n}</span>
      </div>
    </div>
  );
}

// ─── Holding Period Chart ────────────────────────────
function HoldingPeriodChart() {
  const option = useMemo(() => ({
    tooltip: { trigger: "axis" },
    grid: { top: 20, bottom: 25, left: 50, right: 20 },
    xAxis: {
      type: "category",
      data: mockHoldingPeriodMetrics.map((h) => h.period),
      axisLabel: { fontSize: 10 },
    },
    yAxis: [
      { type: "value", name: "Sharpe", nameTextStyle: { fontSize: 9 }, axisLabel: { fontSize: 9 } },
      { type: "value", name: "胜率(%)", nameTextStyle: { fontSize: 9 }, axisLabel: { fontSize: 9 }, max: 100 },
    ],
    series: [
      {
        name: "Sharpe",
        type: "bar",
        data: mockHoldingPeriodMetrics.map((h) => h.sharpe),
        itemStyle: {
          color: (params: { dataIndex: number }) => {
            const v = mockHoldingPeriodMetrics[params.dataIndex].sharpe;
            return v > 2 ? "#10b981" : v > 1 ? "#3b82f6" : "#9ca3af";
          },
        },
      },
      {
        name: "胜率",
        type: "line",
        yAxisIndex: 1,
        data: mockHoldingPeriodMetrics.map((h) => +(h.win_rate * 100).toFixed(1)),
        lineStyle: { color: "#f59e0b" },
        itemStyle: { color: "#f59e0b" },
        symbol: "circle",
        symbolSize: 6,
      },
    ],
  }), []);

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <h3 className="text-sm font-bold text-gray-900 mb-2">持仓期对比</h3>
      <EChart option={option} height={220} />
    </div>
  );
}

// ─── Hop Sharpe Chart ────────────────────────────────
function HopSharpeChart() {
  const hopSlices = mockAnalysisSlices.hop;
  const option = useMemo(() => ({
    tooltip: { trigger: "axis" },
    grid: { top: 20, bottom: 25, left: 50, right: 20 },
    xAxis: {
      type: "category",
      data: hopSlices.map((s) => s.slice_key),
      axisLabel: { fontSize: 10 },
    },
    yAxis: { type: "value", name: "Sharpe", nameTextStyle: { fontSize: 9 }, axisLabel: { fontSize: 9 } },
    series: [{
      type: "bar",
      data: hopSlices.map((s) => ({
        value: s.sharpe,
        itemStyle: { color: s.sharpe < 0 ? "#ef4444" : s.sharpe > 2 ? "#10b981" : "#3b82f6" },
      })),
      label: { show: true, position: "top", fontSize: 10, formatter: (p: { value: number }) => p.value.toFixed(2) },
    }],
  }), [hopSlices]);

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <h3 className="text-sm font-bold text-gray-900 mb-2">跳数维度 Sharpe</h3>
      <EChart option={option} height={200} />
    </div>
  );
}

// ─── Quick Entry Cards ───────────────────────────────
function QuickEntryCards() {
  const cards = [
    { href: "/signals", icon: Target, label: "信号列表", desc: "查看所有信号", count: "98" },
    { href: "/events", icon: TrendingUp, label: "事件列表", desc: "查看事件列表", count: "10" },
    { href: "/analysis", icon: BarChart3, label: "维度分析", desc: "维度分析", count: "4维度" },
  ];

  return (
    <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
      {cards.map((c) => (
        <Link
          key={c.href}
          href={c.href}
          className="bg-white rounded-lg border border-gray-200 p-4 hover:border-blue-300 hover:shadow-sm transition-all group"
        >
          <div className="flex items-center gap-2 mb-2">
            <c.icon size={18} className="text-gray-400 group-hover:text-blue-500 transition-colors" />
            <span className="text-sm font-bold text-gray-900">{c.label}</span>
            <span className="ml-auto text-xs text-gray-400 font-mono">{c.count}</span>
          </div>
          <p className="text-xs text-gray-500">{c.desc}</p>
        </Link>
      ))}
    </div>
  );
}

// ─── Page ────────────────────────────────────────────
export default function DashboardPage() {
  const { split } = useGlobalFilter();
  const metrics = useCurrentMetrics(split);

  return (
    <div className="space-y-4 max-w-5xl">
      <h1 className="text-lg font-bold text-gray-900">仪表盘</h1>

      {/* Core Metrics */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <MetricCard label="Sharpe" value={fmtSharpe(metrics.sharpe)} valueClass={sharpeColor(metrics.sharpe)} />
        <MetricCard label="胜率" value={fmtPct(metrics.win_rate)} valueClass={metrics.win_rate >= 0.7 ? "text-emerald-600" : ""} />
        <MetricCard label="平均调整收益" value={fmtReturn(metrics.avg_adj_return)} valueClass={returnColor(metrics.avg_adj_return)} />
        <MetricCard label="盈亏比" value={metrics.profit_factor.toFixed(2)} valueClass={metrics.profit_factor > 2 ? "text-emerald-600" : ""} />
      </div>

      {/* Outlier Warning */}
      <OutlierAlert />

      {/* Charts */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <SplitComparisonChart />
        <HoldingPeriodChart />
      </div>

      {/* Hop Sharpe */}
      <HopSharpeChart />

      {/* Quick Entry */}
      <QuickEntryCards />
    </div>
  );
}
