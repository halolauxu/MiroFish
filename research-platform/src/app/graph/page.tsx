import { loadGraphStats } from "@/lib/data/loader";
import { GraphCharts } from "./GraphCharts";

export default function GraphPage() {
  const stats = loadGraphStats();

  // 传播关系（排除 HOLDS_SHARES）
  const propagationRelations = stats.relationDistribution.filter(
    (r) => r.relation !== "HOLDS_SHARES"
  );

  return (
    <div className="space-y-6 max-w-5xl">
      {/* 标题 */}
      <div>
        <h1 className="text-xl font-bold text-gray-900 dark:text-gray-100">图谱总览</h1>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          供应链知识图谱统计与数据质量审计
        </p>
      </div>

      {/* 统计卡片 */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <StatCard label="总节点数" value={stats.nodeCount.toLocaleString()} sub="公司实体" />
        <StatCard label="总边数" value={stats.edgeCount.toLocaleString()} sub="关系连接" />
        <StatCard
          label="传播相关边"
          value={stats.propagationEdgeCount.toLocaleString()}
          sub="排除 HOLDS_SHARES"
          highlight
        />
        <StatCard
          label="关系类型"
          value={String(stats.relationDistribution.length)}
          sub="不同关系种类"
        />
      </div>

      {/* 图表（Client Component） */}
      <GraphCharts relationDistribution={stats.relationDistribution} />

      {/* 传播关系统计表 */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-5">
        <h2 className="text-sm font-bold text-gray-900 dark:text-gray-100 mb-3">
          传播关系统计（排除 HOLDS_SHARES）
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400 dark:text-gray-500 border-b border-gray-100 dark:border-gray-700">
                <th className="py-2 px-3 text-left font-medium">关系类型</th>
                <th className="py-2 px-3 text-right font-medium">边数</th>
                <th className="py-2 px-3 text-right font-medium">占比</th>
                <th className="py-2 px-3 text-left font-medium">说明</th>
              </tr>
            </thead>
            <tbody>
              {propagationRelations.map((r) => (
                <tr
                  key={r.relation}
                  className="border-b border-gray-50 dark:border-gray-700/50"
                >
                  <td className="py-2 px-3 font-mono text-gray-900 dark:text-gray-100">
                    {r.relation}
                  </td>
                  <td className="py-2 px-3 text-right font-mono text-gray-600 dark:text-gray-300">
                    {r.count}
                  </td>
                  <td className="py-2 px-3 text-right text-gray-500 dark:text-gray-400">
                    {r.percentage.toFixed(1)}%
                  </td>
                  <td className="py-2 px-3 text-gray-500 dark:text-gray-400">
                    {RELATION_LABELS[r.relation] || "-"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* 数据质量审计 */}
      {stats.dirtyNames.length > 0 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-5">
          <h2 className="text-sm font-bold text-gray-900 dark:text-gray-100 mb-1">
            数据质量审计
          </h2>
          <p className="text-xs text-gray-500 dark:text-gray-400 mb-3">
            以下节点的 display_name 含异常空格或全角字符，可能导致传播路径匹配失败
          </p>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="text-gray-400 dark:text-gray-500 border-b border-gray-100 dark:border-gray-700">
                  <th className="py-2 px-3 text-left font-medium">代码</th>
                  <th className="py-2 px-3 text-left font-medium">当前名称</th>
                  <th className="py-2 px-3 text-left font-medium">问题</th>
                </tr>
              </thead>
              <tbody>
                {stats.dirtyNames.map((d) => (
                  <tr
                    key={d.code}
                    className="border-b border-gray-50 dark:border-gray-700/50"
                  >
                    <td className="py-1.5 px-3 font-mono text-gray-900 dark:text-gray-100">
                      {d.code}
                    </td>
                    <td className="py-1.5 px-3 text-amber-600 dark:text-amber-400">
                      &quot;{d.displayName}&quot;
                    </td>
                    <td className="py-1.5 px-3 text-gray-500 dark:text-gray-400">{d.issue}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

// ─── 子组件 ──────────────────────────────────────────

function StatCard({
  label,
  value,
  sub,
  highlight,
}: {
  label: string;
  value: string;
  sub: string;
  highlight?: boolean;
}) {
  return (
    <div className={`rounded-lg border p-4 ${
      highlight
        ? "bg-blue-50 dark:bg-blue-900/20 border-blue-200 dark:border-blue-800"
        : "bg-white dark:bg-gray-800 border-gray-200 dark:border-gray-700"
    }`}>
      <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">{label}</div>
      <div className={`text-2xl font-bold ${
        highlight ? "text-blue-700 dark:text-blue-400" : "text-gray-900 dark:text-gray-100"
      }`}>
        {value}
      </div>
      <div className="text-[10px] text-gray-400 dark:text-gray-500 mt-1">{sub}</div>
    </div>
  );
}

const RELATION_LABELS: Record<string, string> = {
  HOLDS_SHARES: "持股关系（基金持仓）",
  COMPETES_WITH: "竞争关系（同行业竞品）",
  BELONGS_TO: "所属关系（板块/行业）",
  SUPPLIES_TO: "供应关系（上下游供应链）",
  COOPERATES_WITH: "合作关系（联营/合资）",
  RELATES_TO_CONCEPT: "概念关联（题材/热点）",
  CUSTOMER_OF: "客户关系（下游客户）",
};
