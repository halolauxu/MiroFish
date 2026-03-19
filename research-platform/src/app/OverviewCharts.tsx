"use client";

import { EChart } from "@/components/charts";
import { EVENT_TYPE_LABELS } from "@/lib/utils/format";

interface SliceData {
  key: string;
  n: number;
  sharpe: number;
  winRate: number;
  avgReturn: number;
}

interface OverviewChartsProps {
  hopSlices: SliceData[];
  typeSlices: SliceData[];
  directionSlices: SliceData[];
}

export function OverviewCharts({ hopSlices, typeSlices, directionSlices }: OverviewChartsProps) {
  // Hop × Sharpe 柱状图
  const hopSorted = [...hopSlices].sort((a, b) => Number(a.key) - Number(b.key));
  const hopOption = {
    tooltip: {
      trigger: "axis",
      formatter(params: { name: string; value: number; dataIndex: number }[]) {
        const p = params[0];
        const slice = hopSorted[p.dataIndex];
        return `跳数 ${p.name}<br/>Sharpe: ${slice.sharpe.toFixed(2)}<br/>胜率: ${(slice.winRate * 100).toFixed(1)}%<br/>n=${slice.n}`;
      },
    },
    grid: { left: 50, right: 20, top: 30, bottom: 30 },
    xAxis: {
      type: "category",
      data: hopSorted.map((s) => `${s.key}跳`),
      axisLabel: { fontSize: 11 },
    },
    yAxis: {
      type: "value",
      name: "Sharpe",
      nameTextStyle: { fontSize: 10 },
      axisLabel: { fontSize: 10 },
    },
    series: [
      {
        type: "bar",
        data: hopSorted.map((s) => ({
          value: Number(s.sharpe.toFixed(2)),
          itemStyle: {
            color: s.sharpe < 0 ? "#ef4444" : s.sharpe > 2 ? "#10b981" : "#6366f1",
          },
        })),
        barMaxWidth: 40,
      },
    ],
  };

  // 事件类型 × Sharpe 柱状图
  const typeSorted = [...typeSlices].sort((a, b) => b.sharpe - a.sharpe);
  const typeOption = {
    tooltip: {
      trigger: "axis",
      formatter(params: { name: string; value: number; dataIndex: number }[]) {
        const p = params[0];
        const slice = typeSorted[p.dataIndex];
        return `${p.name}<br/>Sharpe: ${slice.sharpe.toFixed(2)}<br/>胜率: ${(slice.winRate * 100).toFixed(1)}%<br/>n=${slice.n}`;
      },
    },
    grid: { left: 80, right: 20, top: 10, bottom: 30 },
    yAxis: {
      type: "category",
      data: typeSorted.map((s) => EVENT_TYPE_LABELS[s.key] || s.key),
      axisLabel: { fontSize: 10 },
      inverse: true,
    },
    xAxis: {
      type: "value",
      name: "Sharpe",
      nameTextStyle: { fontSize: 10 },
      axisLabel: { fontSize: 10 },
    },
    series: [
      {
        type: "bar",
        data: typeSorted.map((s) => ({
          value: Number(s.sharpe.toFixed(2)),
          itemStyle: {
            color: s.sharpe < 0 ? "#ef4444" : s.sharpe > 2 ? "#10b981" : "#6366f1",
          },
        })),
        barMaxWidth: 20,
      },
    ],
  };

  // 方向统计
  const avoidSlice = directionSlices.find((s) => s.key === "avoid");
  const longSlice = directionSlices.find((s) => s.key === "long");

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
      {/* Hop × Sharpe */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
        <h3 className="text-xs font-semibold text-gray-600 dark:text-gray-300 mb-2">跳数 × Sharpe (5D)</h3>
        <EChart option={hopOption} height={220} />
      </div>

      {/* 事件类型 × Sharpe */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
        <h3 className="text-xs font-semibold text-gray-600 dark:text-gray-300 mb-2">事件类型 × Sharpe (5D)</h3>
        <EChart option={typeOption} height={220} />
      </div>

      {/* 方向统计 */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4 lg:col-span-2">
        <h3 className="text-xs font-semibold text-gray-600 dark:text-gray-300 mb-3">方向统计 (5D)</h3>
        <div className="grid grid-cols-2 gap-4">
          <DirectionStat
            label="AVOID（规避）"
            count={avoidSlice?.n ?? 0}
            sharpe={avoidSlice?.sharpe ?? 0}
            winRate={avoidSlice?.winRate ?? 0}
            color="red"
          />
          <DirectionStat
            label="LONG（做多）"
            count={longSlice?.n ?? 0}
            sharpe={longSlice?.sharpe ?? 0}
            winRate={longSlice?.winRate ?? 0}
            color="green"
          />
        </div>
      </div>
    </div>
  );
}

function DirectionStat({
  label,
  count,
  sharpe,
  winRate,
  color,
}: {
  label: string;
  count: number;
  sharpe: number;
  winRate: number;
  color: "red" | "green";
}) {
  const borderColor = color === "red" ? "border-l-red-400" : "border-l-emerald-400";
  const bgColor = color === "red" ? "bg-red-50 dark:bg-red-950/30" : "bg-emerald-50 dark:bg-emerald-950/30";

  return (
    <div className={`border-l-4 ${borderColor} ${bgColor} rounded-r-lg p-4`}>
      <div className="text-sm font-medium text-gray-700 dark:text-gray-200 mb-2">{label}</div>
      <div className="grid grid-cols-3 gap-2 text-xs">
        <div>
          <div className="text-gray-400">信号数</div>
          <div className="text-lg font-bold text-gray-900 dark:text-gray-100">{count}</div>
        </div>
        <div>
          <div className="text-gray-400">Sharpe</div>
          <div className={`text-lg font-bold ${sharpe > 2 ? "text-emerald-600" : sharpe < 0 ? "text-red-600" : "text-gray-900 dark:text-gray-100"}`}>
            {sharpe.toFixed(2)}
          </div>
        </div>
        <div>
          <div className="text-gray-400">胜率</div>
          <div className={`text-lg font-bold ${winRate > 0.7 ? "text-emerald-600" : "text-gray-900 dark:text-gray-100"}`}>
            {(winRate * 100).toFixed(1)}%
          </div>
        </div>
      </div>
    </div>
  );
}
