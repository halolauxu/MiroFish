"use client";
import { useState, useMemo } from "react";
import Link from "next/link";
import { mockAnalysisSlices } from "@/lib/mock";
import { fmtPct, fmtReturn, returnColor, fmtSharpe, sharpeColor } from "@/lib/utils/format";
import { EChart } from "@/components/charts";
import type { AnalysisDimension, PerformanceSlice } from "@/types";

const DIMENSION_TABS: { key: AnalysisDimension; label: string }[] = [
  { key: "hop", label: "Hop" },
  { key: "event_type", label: "事件类型" },
  { key: "reacted", label: "已反应" },
  { key: "direction", label: "方向" },
];

/** 将 slice_key 映射为 /signals 页可识别的 URL query params */
function sliceKeyToParams(dim: AnalysisDimension, sliceKey: string): string {
  switch (dim) {
    case "hop": {
      const m = sliceKey.match(/hop=(\d+)/);
      return m ? `hop=${m[1]}` : "";
    }
    case "event_type":
      return `eventType=${sliceKey}`;
    case "reacted":
      return "";
    case "direction":
      return `direction=${sliceKey}`;
    default:
      return "";
  }
}

// ─── Slice Chart ─────────────────────────────────────
function SliceChart({ slices, dim }: { slices: PerformanceSlice[]; dim: AnalysisDimension }) {
  const option = useMemo(() => ({
    tooltip: {
      trigger: "axis",
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      formatter: (params: any) => {
        if (!Array.isArray(params)) return "";
        return params.map((p: { seriesName: string; value: number; marker: string }) => {
          const val = p.seriesName === "胜率" ? `${p.value}%` : p.value.toFixed(2);
          return `${p.marker} ${p.seriesName}: ${val}`;
        }).join("<br/>");
      },
    },
    legend: { top: 0, textStyle: { fontSize: 10 } },
    grid: { top: 30, bottom: 30, left: 50, right: 40 },
    xAxis: {
      type: "category",
      data: slices.map((s) => s.slice_key),
      axisLabel: { fontSize: 9, rotate: dim === "event_type" ? 30 : 0 },
    },
    yAxis: [
      { type: "value", name: "Sharpe / PF", nameTextStyle: { fontSize: 9 }, axisLabel: { fontSize: 9 } },
      { type: "value", name: "胜率(%)", nameTextStyle: { fontSize: 9 }, axisLabel: { fontSize: 9 }, max: 100, min: 0 },
    ],
    series: [
      {
        name: "Sharpe",
        type: "bar",
        data: slices.map((s) => ({
          value: s.sharpe,
          itemStyle: { color: s.sharpe < 0 ? "#ef4444" : s.sharpe > 2 ? "#10b981" : "#3b82f6" },
        })),
      },
      {
        name: "PF",
        type: "bar",
        data: slices.map((s) => ({
          value: s.profit_factor,
          itemStyle: { color: s.profit_factor < 1 ? "#fca5a5" : "#93c5fd" },
        })),
      },
      {
        name: "胜率",
        type: "line",
        yAxisIndex: 1,
        data: slices.map((s) => +(s.win_rate * 100).toFixed(1)),
        lineStyle: { color: "#f59e0b" },
        itemStyle: { color: "#f59e0b" },
        symbol: "circle",
        symbolSize: 6,
      },
    ],
  }), [slices, dim]);

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <EChart option={option} height={dim === "event_type" ? 320 : 260} />
    </div>
  );
}

function SliceTable({ slices, dim }: { slices: PerformanceSlice[]; dim: AnalysisDimension }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs text-left">
        <thead>
          <tr className="text-gray-400 border-b border-gray-100">
            <th className="py-2 px-3">切片</th>
            <th className="py-2 px-3 text-right">N</th>
            <th className="py-2 px-3 text-right">Sharpe</th>
            <th className="py-2 px-3 text-right">胜率</th>
            <th className="py-2 px-3 text-right">平均调整</th>
            <th className="py-2 px-3 text-right">平均超额</th>
            <th className="py-2 px-3 text-right">IC</th>
            <th className="py-2 px-3 text-right">最大回撤</th>
            <th className="py-2 px-3 text-right">PF</th>
            <th className="py-2 px-3"></th>
          </tr>
        </thead>
        <tbody>
          {slices.map((s) => (
            <tr key={s.slice_key} className="border-b border-gray-50 hover:bg-blue-50/30">
              <td className="py-2 px-3 font-semibold text-gray-900">{s.slice_key}</td>
              <td className="py-2 px-3 text-right text-gray-600">{s.n}</td>
              <td className={`py-2 px-3 text-right font-mono ${sharpeColor(s.sharpe)}`}>{fmtSharpe(s.sharpe)}</td>
              <td className={`py-2 px-3 text-right ${s.win_rate >= 0.7 ? "text-emerald-600 font-semibold" : s.win_rate < 0.5 ? "text-red-600" : ""}`}>
                {fmtPct(s.win_rate)}
              </td>
              <td className={`py-2 px-3 text-right font-mono ${returnColor(s.avg_adj_return)}`}>
                {fmtReturn(s.avg_adj_return)}
              </td>
              <td className={`py-2 px-3 text-right font-mono ${returnColor(s.avg_excess_return)}`}>
                {fmtReturn(s.avg_excess_return)}
              </td>
              <td className="py-2 px-3 text-right font-mono text-gray-600">{s.ic.toFixed(4)}</td>
              <td className="py-2 px-3 text-right text-red-600 font-mono">{fmtPct(s.max_dd)}</td>
              <td className={`py-2 px-3 text-right font-mono ${s.profit_factor >= 2 ? "text-emerald-600" : s.profit_factor < 1 ? "text-red-600" : ""}`}>
                {s.profit_factor.toFixed(2)}
              </td>
              <td className="py-2 px-3">
                {sliceKeyToParams(dim, s.slice_key) ? (
                  <Link
                    href={`/signals?${sliceKeyToParams(dim, s.slice_key)}`}
                    className="text-blue-600 hover:underline text-[11px]"
                  >
                    查看信号 →
                  </Link>
                ) : (
                  <span className="text-gray-300 text-[11px]">—</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function AnalysisPage() {
  const [dim, setDim] = useState<AnalysisDimension>("hop");

  const slices = mockAnalysisSlices[dim];

  return (
    <div className="space-y-4 max-w-6xl">
      <h1 className="text-lg font-bold text-gray-900">Analysis</h1>

      {/* Dimension tabs */}
      <div className="flex items-center gap-1 bg-gray-100 rounded-lg p-1 w-fit">
        {DIMENSION_TABS.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setDim(tab.key)}
            className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
              dim === tab.key
                ? "bg-white text-gray-900 shadow-sm"
                : "text-gray-500 hover:text-gray-700"
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* Chart */}
      <SliceChart slices={slices} dim={dim} />

      {/* Slice Table */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <SliceTable slices={slices} dim={dim} />
      </div>

      {/* Insights */}
      {dim === "hop" && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-xs text-blue-800">
          <strong>洞察:</strong> Hop=0（源头）Sharpe=-4.51，建议过滤。1-3跳信号全部为正Sharpe，
          其中1跳最强(3.65)，验证了信息差假设——下游传播信号比源头信号有更高的超额收益。
        </div>
      )}
      {dim === "event_type" && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-xs text-blue-800">
          <strong>洞察:</strong> 丑闻(scandal) Sharpe=11.26 最强，但样本仅9个。
          cooperation 和 earnings_surprise 信号质量较低（Sharpe≤0 或 PF&lt;1），可考虑过滤。
        </div>
      )}
      {dim === "direction" && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 text-xs text-blue-800">
          <strong>洞察:</strong> Avoid(Sharpe=3.10) 显著优于 Long(Sharpe=0.85)。
          A股&ldquo;利好出尽&rdquo;效应使得 avoid 类信号是主要 alpha 来源。
        </div>
      )}
    </div>
  );
}
