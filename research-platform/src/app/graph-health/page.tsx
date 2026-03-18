"use client";
import { useMemo } from "react";
import { mockRelationDistribution, mockGraphAuditIssues } from "@/lib/mock";
import { EChart } from "@/components/charts";
import type { GraphAuditIssue } from "@/types";

// ─── Relation Pie Chart ──────────────────────────────
function RelationPieChart() {
  const option = useMemo(() => ({
    tooltip: {
      trigger: "item",
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      formatter: (params: any) =>
        `${params.name}<br/>${params.value} 条 (${params.percent}%)`,
    },
    legend: {
      orient: "vertical",
      right: 10,
      top: "center",
      textStyle: { fontSize: 10 },
    },
    series: [{
      type: "pie",
      radius: ["35%", "65%"],
      center: ["35%", "50%"],
      avoidLabelOverlap: true,
      label: { show: false },
      emphasis: {
        label: { show: true, fontSize: 12, fontWeight: "bold" },
      },
      data: mockRelationDistribution.map((r, i) => ({
        value: r.count,
        name: r.relation.replace(/_/g, " "),
        itemStyle: {
          color: [
            "#3b82f6", "#10b981", "#f59e0b", "#ef4444",
            "#8b5cf6", "#ec4899", "#06b6d4",
          ][i % 7],
        },
      })),
    }],
  }), []);

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <h3 className="text-sm font-bold text-gray-900 mb-3">关系类型分布</h3>
      <EChart option={option} height={280} />
    </div>
  );
}

// ─── Relation Bar Chart ──────────────────────────────
function RelationBarChart() {
  const option = useMemo(() => ({
    tooltip: { trigger: "axis" },
    grid: { top: 10, bottom: 30, left: 120, right: 20 },
    xAxis: {
      type: "value",
      axisLabel: { fontSize: 9 },
    },
    yAxis: {
      type: "category",
      data: mockRelationDistribution.map((r) => r.relation.replace(/_/g, " ")).reverse(),
      axisLabel: { fontSize: 9 },
    },
    series: [{
      type: "bar",
      data: mockRelationDistribution.map((r) => r.count).reverse(),
      itemStyle: { color: "#3b82f6", borderRadius: [0, 4, 4, 0] },
      label: {
        show: true,
        position: "right",
        fontSize: 9,
        formatter: (p: { value: number }) => `${p.value}`,
      },
    }],
  }), []);

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <h3 className="text-sm font-bold text-gray-900 mb-3">关系数量排行</h3>
      <EChart option={option} height={280} />
    </div>
  );
}

// ─── Health Overview ─────────────────────────────────
function HealthOverview() {
  const dirtyNames = mockGraphAuditIssues.filter((i) => i.issue_type === "dirty_name").length;
  const suspiciousPaths = mockGraphAuditIssues.filter((i) => i.issue_type === "suspicious_path").length;
  const lowWinEdges = mockGraphAuditIssues.filter((i) => i.issue_type === "low_win_edge").length;
  const totalAffected = mockGraphAuditIssues.reduce((s, i) => s + i.affected_signal_count, 0);

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <h3 className="text-sm font-bold text-gray-900 mb-3">图谱健康概览</h3>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div>
          <div className="text-[11px] text-gray-400">总问题数</div>
          <div className="text-2xl font-bold text-gray-900">{mockGraphAuditIssues.length}</div>
        </div>
        <div>
          <div className="text-[11px] text-gray-400">脏名称</div>
          <div className="text-2xl font-bold text-amber-600">{dirtyNames}</div>
        </div>
        <div>
          <div className="text-[11px] text-gray-400">可疑路径</div>
          <div className="text-2xl font-bold text-red-600">{suspiciousPaths}</div>
        </div>
        <div>
          <div className="text-[11px] text-gray-400">低胜率边</div>
          <div className="text-2xl font-bold text-orange-600">{lowWinEdges}</div>
        </div>
      </div>
      <div className="mt-2 text-xs text-gray-400">
        影响 {totalAffected} 个信号
      </div>
    </div>
  );
}

function IssueTypeIcon({ type }: { type: GraphAuditIssue["issue_type"] }) {
  const styles: Record<string, string> = {
    dirty_name: "bg-amber-100 text-amber-700",
    suspicious_path: "bg-red-100 text-red-700",
    low_win_edge: "bg-orange-100 text-orange-700",
  };
  const labels: Record<string, string> = {
    dirty_name: "脏名称",
    suspicious_path: "可疑路径",
    low_win_edge: "低胜率",
  };
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium ${styles[type]}`}>
      {labels[type]}
    </span>
  );
}

function AuditTable() {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <h3 className="text-sm font-bold text-gray-900 mb-3">审计问题列表</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-left">
          <thead>
            <tr className="text-gray-400 border-b border-gray-100">
              <th className="py-2 px-2">类型</th>
              <th className="py-2 px-2">节点/路径</th>
              <th className="py-2 px-2">详情</th>
              <th className="py-2 px-2 text-right">影响信号</th>
            </tr>
          </thead>
          <tbody>
            {mockGraphAuditIssues.map((issue, i) => (
              <tr key={i} className="border-b border-gray-50">
                <td className="py-2 px-2"><IssueTypeIcon type={issue.issue_type} /></td>
                <td className="py-2 px-2 font-mono text-gray-700">
                  {issue.node_code ? `${issue.node_code} ${issue.node_name}` : issue.path}
                </td>
                <td className="py-2 px-2 text-gray-600 max-w-[300px]">{issue.detail}</td>
                <td className="py-2 px-2 text-right font-mono">{issue.affected_signal_count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function GraphHealthPage() {
  return (
    <div className="space-y-4 max-w-5xl">
      <h1 className="text-lg font-bold text-gray-900">图谱健康</h1>

      <HealthOverview />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <RelationPieChart />
        <RelationBarChart />
      </div>

      <AuditTable />
    </div>
  );
}
