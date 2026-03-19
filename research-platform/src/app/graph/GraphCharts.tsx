"use client";

import { EChart } from "@/components/charts";

interface RelationDist {
  relation: string;
  count: number;
  percentage: number;
}

export function GraphCharts({ relationDistribution }: { relationDistribution: RelationDist[] }) {
  // 关系类型分布饼图
  const pieOption = {
    tooltip: {
      trigger: "item",
      formatter: "{b}: {c} ({d}%)",
    },
    legend: {
      type: "scroll",
      orient: "vertical",
      right: 10,
      top: 20,
      bottom: 20,
      textStyle: { fontSize: 10 },
    },
    series: [
      {
        type: "pie",
        radius: ["35%", "65%"],
        center: ["35%", "50%"],
        avoidLabelOverlap: true,
        itemStyle: {
          borderRadius: 4,
          borderColor: "#fff",
          borderWidth: 2,
        },
        label: {
          show: false,
        },
        emphasis: {
          label: {
            show: true,
            fontSize: 12,
            fontWeight: "bold",
          },
        },
        data: relationDistribution.map((r) => ({
          name: r.relation,
          value: r.count,
        })),
      },
    ],
    color: ["#6366f1", "#ef4444", "#f59e0b", "#10b981", "#3b82f6", "#8b5cf6", "#ec4899"],
  };

  // 传播关系柱状图（排除 HOLDS_SHARES）
  const propagationRelations = relationDistribution
    .filter((r) => r.relation !== "HOLDS_SHARES")
    .sort((a, b) => b.count - a.count);

  const barOption = {
    tooltip: {
      trigger: "axis",
    },
    grid: { left: 130, right: 30, top: 10, bottom: 30 },
    yAxis: {
      type: "category",
      data: propagationRelations.map((r) => r.relation),
      axisLabel: { fontSize: 10 },
      inverse: true,
    },
    xAxis: {
      type: "value",
      axisLabel: { fontSize: 10 },
    },
    series: [
      {
        type: "bar",
        data: propagationRelations.map((r) => ({
          value: r.count,
          itemStyle: { color: "#6366f1" },
        })),
        barMaxWidth: 20,
      },
    ],
  };

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
        <h3 className="text-xs font-semibold text-gray-600 dark:text-gray-300 mb-2">
          关系类型分布
        </h3>
        <EChart option={pieOption} height={280} />
      </div>
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
        <h3 className="text-xs font-semibold text-gray-600 dark:text-gray-300 mb-2">
          传播相关边（排除 HOLDS_SHARES）
        </h3>
        <EChart option={barOption} height={280} />
      </div>
    </div>
  );
}
