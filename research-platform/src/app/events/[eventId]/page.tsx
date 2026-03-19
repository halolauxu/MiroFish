import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { loadEvent, loadSignalsByEvent, loadSubgraph, isCorrect, computeTradeInfo } from "@/lib/data/loader";
import { fmtReturn, fmtPct, returnColor } from "@/lib/utils/format";
import { EventTypeBadge, DirectionBadge, HopBadge, CorrectnessIndicator } from "@/components/badges";
import { PropagationGraph } from "./PropagationGraph";
import type { SignalRecord } from "@/types";

export default function EventDetailPage({ params }: { params: { eventId: string } }) {
  const { eventId } = params;
  const event = loadEvent(eventId);

  if (!event) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400 dark:text-gray-500">
        事件 {eventId} 不存在
      </div>
    );
  }

  const signals = loadSignalsByEvent(eventId);
  const subgraph = loadSubgraph(eventId);

  // 计算聚合指标
  const withReturn = signals.filter((s) => s.fwd_return_5d != null);
  const correctCount = withReturn.filter(isCorrect).length;
  const winRate = withReturn.length > 0 ? correctCount / withReturn.length : 0;
  const avgExcess = withReturn.length > 0
    ? withReturn.reduce((sum, s) => sum + (s.excess_5d ?? 0), 0) / withReturn.length
    : 0;
  const downstreamSignals = signals.filter((s) => s.hop > 0);
  const downstreamWithReturn = downstreamSignals.filter((s) => s.fwd_return_5d != null);
  const downstreamCorrect = downstreamWithReturn.filter(isCorrect).length;
  const downstreamWinRate = downstreamWithReturn.length > 0 ? downstreamCorrect / downstreamWithReturn.length : 0;

  // Hop 分布
  const hopDist = [0, 1, 2, 3].map((h) => signals.filter((s) => s.hop === h).length);

  // 为 Cytoscape 准备数据
  const graphNodes = Object.entries(subgraph.nodes).map(([code, node]) => {
    const sig = signals.find((s) => s.target_code === code);
    const isSource = code === event.stock_code;
    return {
      id: code,
      label: node.display_name || code,
      hop: sig?.hop ?? (isSource ? 0 : -1),
      direction: sig?.signal_direction ?? null,
      isSource,
      correct: sig ? isCorrect(sig) : null,
    };
  });

  const graphEdges = subgraph.edges.map((e, i) => ({
    id: `e${i}`,
    source: e.source,
    target: e.target,
    relation: e.relation,
    fact: e.fact,
  }));

  // 分析传播关系质量
  const holdsSharesSignals = signals.filter((s) => s.relation_chain && s.relation_chain.includes("HOLDS_SHARES"));
  const holdsSharesPct = signals.length > 0 ? holdsSharesSignals.length / signals.length : 0;

  return (
    <div className="space-y-4 max-w-6xl">
      {/* 传播质量警告 */}
      {holdsSharesPct > 0.5 && (
        <div className="bg-amber-50 dark:bg-amber-950/20 border border-amber-300 dark:border-amber-700 rounded-lg p-3 text-xs text-amber-700 dark:text-amber-400 flex items-start gap-2">
          <span className="text-amber-500 shrink-0">&#9888;</span>
          <div>
            <strong>传播质量低：</strong>该事件 {(holdsSharesPct * 100).toFixed(0)}% 的信号通过「基金持仓」(HOLDS_SHARES) 间接关系传播。
            目标公司与事件源头之间无实际业务关联，仅因被同一基金持有而被关联。此类信号缺乏因果逻辑。
            <span className="text-amber-500 font-medium"> 策略WF验证已FAIL，以下数据仅供研究参考。</span>
          </div>
        </div>
      )}

      {/* 事件头部 */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-5">
        <div className="flex items-center gap-3 mb-3">
          <Link href="/" className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300">
            <ArrowLeft size={18} />
          </Link>
          <h1 className="text-lg font-bold text-gray-900 dark:text-gray-100">{event.title}</h1>
          <EventTypeBadge type={event.type} />
          <span className={`text-xs px-2 py-0.5 rounded ${
            event.impact_level === "high"
              ? "bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400"
              : event.impact_level === "medium"
              ? "bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400"
              : "bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-400"
          }`}>
            {event.impact_level}
          </span>
        </div>
        <p className="text-sm text-gray-600 dark:text-gray-400 mb-3">{event.summary}</p>
        <div className="flex items-center gap-4 text-xs text-gray-500 dark:text-gray-400">
          <span>
            <span className="text-gray-400 dark:text-gray-500">标的 </span>
            <span className="font-mono font-medium text-gray-900 dark:text-gray-100">
              {event.stock_code} {event.stock_name}
            </span>
          </span>
          <span>
            <span className="text-gray-400 dark:text-gray-500">日期 </span>
            <span className="font-medium">{event.event_date}</span>
          </span>
          {event.key_data && (
            <span className="text-gray-400 dark:text-gray-500">{event.key_data}</span>
          )}
        </div>
      </div>

      {/* 事件级指标 */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-5">
        <h2 className="text-sm font-bold text-gray-900 dark:text-gray-100 mb-3">事件级指标</h2>
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
          <StatItem label="信号数" value={String(signals.length)} />
          <StatItem
            label="胜率"
            value={fmtPct(winRate)}
            color={winRate > 0.7 ? "text-emerald-600" : winRate < 0.5 ? "text-red-600" : undefined}
          />
          <StatItem
            label="平均5D超额"
            value={fmtReturn(avgExcess)}
            color={returnColor(avgExcess)}
          />
          <StatItem
            label="下游胜率(hop>0)"
            value={fmtPct(downstreamWinRate)}
            color={downstreamWinRate > 0.7 ? "text-emerald-600" : undefined}
          />
          <div>
            <div className="text-[11px] text-gray-400 dark:text-gray-500 mb-1">Hop 分布</div>
            <div className="flex items-center gap-1">
              {hopDist.map((count, i) => (
                count > 0 && (
                  <div key={i} className="flex items-center gap-0.5">
                    <HopBadge hop={i} />
                    <span className="text-xs text-gray-500">{count}</span>
                  </div>
                )
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* 传播图谱 */}
      {graphNodes.length > 1 && (
        <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
          <h2 className="text-sm font-bold text-gray-900 dark:text-gray-100 mb-3">
            传播图谱
            <span className="text-xs font-normal text-gray-400 ml-2">
              {graphNodes.length} 节点 · {graphEdges.length} 边
            </span>
          </h2>
          <PropagationGraph nodes={graphNodes} edges={graphEdges} />
        </div>
      )}

      {/* ═══════ 交易信号明细表（完整持仓视图）═══════ */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
        <div className="px-4 py-3 border-b border-gray-100 dark:border-gray-700 flex items-center justify-between">
          <h2 className="text-sm font-bold text-gray-900 dark:text-gray-100">
            交易信号（{signals.length} 条）
          </h2>
          <span className="text-[10px] text-gray-400">开仓 → 持仓(T+1~T+5) → 平仓结算</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="bg-gray-50 dark:bg-gray-900 text-gray-400 dark:text-gray-500 text-[11px]">
                <th className="px-3 py-2 text-left font-medium sticky left-0 bg-gray-50 dark:bg-gray-900">目标</th>
                <th className="px-2 py-2 text-center font-medium">跳</th>
                <th className="px-2 py-2 text-center font-medium">方向</th>
                {/* 开仓区 */}
                <th className="px-2 py-2 text-left font-medium border-l-2 border-blue-200 dark:border-blue-800">
                  <span className="text-blue-500">开仓日</span>
                </th>
                <th className="px-2 py-2 text-right font-medium">入场价</th>
                {/* 持仓区 */}
                <th className="px-2 py-2 text-right font-medium border-l-2 border-amber-200 dark:border-amber-800">
                  <span className="text-amber-500">T+1</span>
                </th>
                <th className="px-2 py-2 text-right font-medium"><span className="text-amber-500">T+3</span></th>
                <th className="px-2 py-2 text-right font-medium"><span className="text-amber-500">T+5</span></th>
                {/* 平仓区 */}
                <th className="px-2 py-2 text-right font-medium border-l-2 border-emerald-200 dark:border-emerald-800">出场价</th>
                <th className="px-2 py-2 text-right font-medium"><span className="text-emerald-600">策略盈亏</span></th>
                <th className="px-2 py-2 text-right font-medium">vs沪深300</th>
                <th className="px-2 py-2 text-center font-medium">仓位</th>
                <th className="px-2 py-2 text-center font-medium">结果</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50 dark:divide-gray-700/50">
              {signals.map((s) => (
                <SignalTradeRow key={s.idx} signal={s} />
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

// ─── 子组件 ──────────────────────────────────────────

function SignalTradeRow({ signal: s }: { signal: SignalRecord }) {
  const trade = computeTradeInfo(s);
  const correct = isCorrect(s);
  const isHop0 = s.hop === 0;

  // 持仓期间的方向调整收益
  const holdingReturns = [
    { day: "T+1", raw: s.fwd_return_1d },
    { day: "T+3", raw: s.fwd_return_3d },
    { day: "T+5", raw: s.fwd_return_5d },
  ].map((t) => {
    const adj = t.raw != null
      ? (s.signal_direction === "avoid" ? -(t.raw) : t.raw)
      : null;
    return { ...t, adj };
  });

  return (
    <tr className={`hover:bg-blue-50/30 dark:hover:bg-blue-900/10 group ${isHop0 ? "opacity-40" : ""}`}>
      {/* 目标 */}
      <td className="px-3 py-2.5 sticky left-0 bg-white dark:bg-gray-800 group-hover:bg-blue-50/30 dark:group-hover:bg-blue-900/10">
        <Link href={`/signals/${s.idx}`} className="hover:underline">
          <div className="font-medium text-gray-900 dark:text-gray-100 text-[12px]">{s.target_name}</div>
          <div className="text-[10px] text-gray-400 font-mono">{s.target_code}</div>
        </Link>
      </td>
      {/* 跳数 */}
      <td className="px-2 py-2.5 text-center"><HopBadge hop={s.hop} /></td>
      {/* 方向 */}
      <td className="px-2 py-2.5 text-center">
        <DirectionBadge direction={s.signal_direction as "avoid" | "long"} />
      </td>
      {/* ── 开仓区（蓝色边框） ── */}
      <td className="px-2 py-2.5 border-l-2 border-blue-100 dark:border-blue-900 text-gray-600 dark:text-gray-300 text-[11px]">
        {s.event_date}
      </td>
      <td className="px-2 py-2.5 text-right font-mono text-gray-700 dark:text-gray-300">
        ¥{s.entry_price.toFixed(2)}
      </td>
      {/* ── 持仓区（琥珀色边框）── */}
      {holdingReturns.map((t, i) => (
        <td key={t.day} className={`px-2 py-2.5 text-right font-mono ${i === 0 ? "border-l-2 border-amber-100 dark:border-amber-900" : ""}`}>
          {t.adj != null ? (
            <span className={t.adj > 0 ? "text-emerald-600" : t.adj < 0 ? "text-red-500" : "text-gray-400"}>
              {t.adj > 0 ? "+" : ""}{(t.adj * 100).toFixed(1)}%
            </span>
          ) : (
            <span className="text-gray-300">-</span>
          )}
        </td>
      ))}
      {/* ── 平仓区（绿色边框）── */}
      <td className="px-2 py-2.5 text-right font-mono text-gray-700 dark:text-gray-300 border-l-2 border-emerald-100 dark:border-emerald-900">
        {s.fwd_return_5d != null ? `¥${trade.exitPrice.toFixed(2)}` : "-"}
      </td>
      <td className="px-2 py-2.5 text-right">
        {s.fwd_return_5d != null ? (
          <span className={`font-mono font-semibold ${trade.pnlPercent > 0 ? "text-emerald-600" : "text-red-600"}`}>
            {trade.pnlPercent > 0 ? "+" : ""}{(trade.pnlPercent * 100).toFixed(2)}%
          </span>
        ) : <span className="text-gray-400">-</span>}
      </td>
      <td className={`px-2 py-2.5 text-right font-mono ${returnColor(s.excess_5d)}`}>{fmtReturn(s.excess_5d)}</td>
      <td className="px-2 py-2.5 text-center">
        <span className={`text-[10px] px-1.5 py-0.5 rounded ${
          trade.confidenceLevel === "不建议" ? "bg-gray-100 text-gray-500 dark:bg-gray-700 dark:text-gray-400" :
          trade.confidenceLevel === "重仓" ? "bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-400" :
          trade.confidenceLevel === "中仓" ? "bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-400" :
          "bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-400"
        }`}>{trade.confidenceLevel}</span>
      </td>
      <td className="px-2 py-2.5 text-center">
        {s.fwd_return_5d != null ? <CorrectnessIndicator correct={correct} /> : <span className="text-gray-400">-</span>}
      </td>
    </tr>
  );
}

function StatItem({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div>
      <div className="text-[11px] text-gray-400 dark:text-gray-500">{label}</div>
      <div className={`text-xl font-bold ${color ?? "text-gray-900 dark:text-gray-100"}`}>{value}</div>
    </div>
  );
}
