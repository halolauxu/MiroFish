import Link from "next/link";
import { loadEvents, loadSignals, loadReport, isCorrect, computeSliceStats, computePortfolioStats, computeTradeInfo, computeSharpe } from "@/lib/data/loader";
import { computeEventStats } from "@/lib/data/loader";
import { EventTypeBadge, DirectionBadge, HopBadge, CorrectnessIndicator } from "@/components/badges";
import { fmtPct, fmtReturn, fmtSharpe, sharpeColor, returnColor, EVENT_TYPE_LABELS } from "@/lib/utils/format";
import { OverviewCharts } from "./OverviewCharts";
import type { SplitMetricsRow, SignalRecord } from "@/types";

export default function HomePage() {
  const events = loadEvents();
  const signals = loadSignals();
  const report = loadReport();
  const eventStats = computeEventStats(signals);
  const eventsWithSignals = events.filter((e) => eventStats[e.event_id]);
  const totalSignals = signals.length;
  const portfolio = computePortfolioStats(signals);

  // 维度切片
  const hopSlices = computeSliceStats(signals, (s) => String(s.hop));
  const typeSlices = computeSliceStats(signals, (s) => s.event_type);
  const directionSlices = computeSliceStats(signals, (s) => s.signal_direction);

  // ═══ 按关系链类型分组 ═══
  const relationSlices = computeSliceStats(
    signals.filter((s) => s.hop > 0),
    (s) => {
      if (!s.relation_chain) return "unknown";
      if (s.relation_chain.includes("COMPETES_WITH")) return "COMPETES_WITH";
      if (s.relation_chain.includes("SUPPLIES_TO") || s.relation_chain.includes("CUSTOMER_OF")) return "SUPPLY_CHAIN";
      if (s.relation_chain.includes("COOPERATES_WITH")) return "COOPERATES_WITH";
      if (s.relation_chain.includes("HOLDS_SHARES")) return "HOLDS_SHARES_ONLY";
      return "MIXED";
    }
  ).sort((a, b) => b.sharpe - a.sharpe);

  // Scandal 板块连坐子策略
  const scandalSignals = signals.filter((s) => s.event_type === "scandal" && s.hop > 0 && s.fwd_return_5d != null);
  const scandalReturns = scandalSignals.map((s) => s.signal_direction === "avoid" ? -(s.fwd_return_5d!) : s.fwd_return_5d!);
  const scandalSharpe = computeSharpe(scandalReturns);
  const scandalWinRate = scandalSignals.length > 0 ? scandalSignals.filter(isCorrect).length / scandalSignals.length : 0;
  const scandalAvgReturn = scandalReturns.length > 0 ? scandalReturns.reduce((a, b) => a + b, 0) / scandalReturns.length : 0;

  // Agent辩论覆盖率
  const debateActive = signals.filter((s) => s.divergence > 0).length;

  // 有效交易列表
  const tradeableSignals = signals.filter((s) => s.hop > 0 && s.fwd_return_5d != null)
    .sort((a, b) => {
      const dateDiff = b.event_date.localeCompare(a.event_date);
      if (dateDiff !== 0) return dateDiff;
      return computeTradeInfo(b).pnlPercent - computeTradeInfo(a).pnlPercent;
    });

  const RELATION_LABELS: Record<string, string> = {
    HOLDS_SHARES_ONLY: "纯基金持仓", COMPETES_WITH: "竞争关系", SUPPLY_CHAIN: "供应链(上下游)",
    COOPERATES_WITH: "合作关系", MIXED: "混合关系", unknown: "未知",
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-bold text-gray-900 dark:text-gray-100">策略研究仪表盘</h1>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          冲击传播链路 · {totalSignals} 条信号 · {events.length} 个事件 · 目标: 发现可交易因子
        </p>
      </div>

      {/* ═══ 模块状态 ═══ */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        <StatusCard label="图谱传播" value="已接入" sub="4948节点 / 4805边 · 5种关系类型" color="text-emerald-600" />
        <StatusCard label="Agent辩论" value={debateActive > 0 ? `${debateActive}信号触发` : "回测中跳过"} sub={`skip_debate=True · 3轮LLM需单独开启`} color={debateActive > 0 ? "text-emerald-600" : "text-amber-600"} />
        <StatusCard label="舆情检测(S10)" value="降级" sub={`仅${events.length}个事件 · 需扩大事件源`} color="text-amber-600" />
      </div>

      {/* ═══ 子策略因子对比 ═══ */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
        <div className="px-4 py-3 border-b border-gray-100 dark:border-gray-700">
          <h2 className="text-sm font-bold text-gray-900 dark:text-gray-100">因子拆解：哪些事件类型有效？</h2>
          <p className="text-[10px] text-gray-400 mt-0.5">按事件类型拆分 · hop&gt;0 · 5D方向调整收益</p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="bg-gray-50 dark:bg-gray-900 text-gray-400">
                <th className="px-4 py-2 text-left font-medium">事件类型</th>
                <th className="px-3 py-2 text-right font-medium">n</th>
                <th className="px-3 py-2 text-right font-medium">Sharpe</th>
                <th className="px-3 py-2 text-right font-medium">胜率</th>
                <th className="px-3 py-2 text-right font-medium">平均收益</th>
                <th className="px-3 py-2 text-left font-medium">判断</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50 dark:divide-gray-700/50">
              {typeSlices.sort((a, b) => b.sharpe - a.sharpe).map((s) => {
                const good = s.sharpe > 1.5 && s.n >= 5;
                const promising = s.sharpe > 0.8 && s.n >= 3;
                const bad = s.sharpe < 0 || s.winRate < 0.5;
                return (
                  <tr key={s.key} className={good ? "bg-emerald-50/50 dark:bg-emerald-950/20" : ""}>
                    <td className="px-4 py-2 font-medium text-gray-900 dark:text-gray-100">
                      <EventTypeBadge type={s.key} /> <span className="ml-1">{EVENT_TYPE_LABELS[s.key] || s.key}</span>
                    </td>
                    <td className="px-3 py-2 text-right font-mono">{s.n}</td>
                    <td className={`px-3 py-2 text-right font-mono font-semibold ${sharpeColor(s.sharpe)}`}>{fmtSharpe(s.sharpe)}</td>
                    <td className={`px-3 py-2 text-right font-mono ${s.winRate > 0.7 ? "text-emerald-600" : s.winRate < 0.5 ? "text-red-600" : ""}`}>{fmtPct(s.winRate)}</td>
                    <td className={`px-3 py-2 text-right font-mono ${returnColor(s.avgReturn)}`}>{fmtReturn(s.avgReturn)}</td>
                    <td className="px-3 py-2">
                      {good ? <Badge text="有效" color="emerald" /> : promising ? <Badge text="待验证" color="blue" /> : bad ? <Badge text="无效" color="red" /> : <Badge text="样本不足" color="gray" />}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* ═══ 关系类型分析 ═══ */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
        <div className="px-4 py-3 border-b border-gray-100 dark:border-gray-700">
          <h2 className="text-sm font-bold text-gray-900 dark:text-gray-100">传播关系质量：产业链 vs 基金持仓</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="bg-gray-50 dark:bg-gray-900 text-gray-400">
                <th className="px-4 py-2 text-left font-medium">关系类型</th>
                <th className="px-3 py-2 text-right font-medium">n</th>
                <th className="px-3 py-2 text-right font-medium">Sharpe</th>
                <th className="px-3 py-2 text-right font-medium">胜率</th>
                <th className="px-3 py-2 text-right font-medium">平均收益</th>
                <th className="px-3 py-2 text-left font-medium">因果性</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50 dark:divide-gray-700/50">
              {relationSlices.map((s) => {
                const causal = s.key === "SUPPLY_CHAIN" || s.key === "COMPETES_WITH" || s.key === "COOPERATES_WITH";
                return (
                  <tr key={s.key} className={causal ? "" : "opacity-50"}>
                    <td className="px-4 py-2 font-medium text-gray-900 dark:text-gray-100">
                      {RELATION_LABELS[s.key] || s.key}
                      {!causal && <span className="text-[9px] text-amber-500 ml-1">(无因果)</span>}
                    </td>
                    <td className="px-3 py-2 text-right font-mono">{s.n}</td>
                    <td className={`px-3 py-2 text-right font-mono font-semibold ${sharpeColor(s.sharpe)}`}>{fmtSharpe(s.sharpe)}</td>
                    <td className={`px-3 py-2 text-right font-mono ${s.winRate > 0.7 ? "text-emerald-600" : ""}`}>{fmtPct(s.winRate)}</td>
                    <td className={`px-3 py-2 text-right font-mono ${returnColor(s.avgReturn)}`}>{fmtReturn(s.avgReturn)}</td>
                    <td className="px-3 py-2 text-[10px]">{causal ? <span className="text-emerald-600 font-medium">强因果</span> : <span className="text-gray-400">间接</span>}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>

      {/* ═══ Scandal板块连坐重点 ═══ */}
      {scandalSignals.length > 0 && (
        <div className="bg-emerald-50 dark:bg-emerald-950/20 rounded-lg border-2 border-emerald-300 dark:border-emerald-800 p-4">
          <h2 className="text-sm font-bold text-emerald-700 dark:text-emerald-400 mb-2">重点: Scandal 板块连坐</h2>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-3">
            <MiniStat label="信号数" value={String(scandalSignals.length)} />
            <MiniStat label="Sharpe" value={fmtSharpe(scandalSharpe)} color={sharpeColor(scandalSharpe)} />
            <MiniStat label="胜率" value={fmtPct(scandalWinRate)} color={scandalWinRate > 0.7 ? "text-emerald-600 font-semibold" : undefined} />
            <MiniStat label="平均收益" value={fmtReturn(scandalAvgReturn)} color={returnColor(scandalAvgReturn)} />
          </div>
          <div className="text-xs text-emerald-700 dark:text-emerald-400 space-y-1">
            <p><strong>假说：</strong>竞争对手出丑闻 → 板块情绪性下跌（A股板块连坐效应）</p>
            <p><strong>下一步：</strong>需500+独立事件大样本验证</p>
          </div>
        </div>
      )}

      {/* 组合统计 */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200">组合交易统计</h2>
          <span className="text-[10px] text-gray-400">hop&gt;0 · 5天持仓</span>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-6 gap-3">
          <MiniStat label="有效交易" value={String(portfolio.totalTrades)} />
          <MiniStat label="盈利/亏损" value={`${portfolio.winners}/${portfolio.losers}`} color={portfolio.winners > portfolio.losers ? "text-emerald-600" : "text-red-600"} />
          <MiniStat label="胜率" value={fmtPct(portfolio.winRate)} color={portfolio.winRate > 0.7 ? "text-emerald-600 font-semibold" : undefined} />
          <MiniStat label="平均盈亏" value={fmtReturn(portfolio.avgPnl)} color={portfolio.avgPnl > 0 ? "text-emerald-600" : "text-red-600"} />
          <MiniStat label="盈亏比" value={portfolio.profitFactor.toFixed(2)} color={portfolio.profitFactor > 1.5 ? "text-emerald-600 font-semibold" : undefined} />
          <MiniStat label="平均超额" value={fmtReturn(portfolio.avgExcess)} color={portfolio.avgExcess > 0 ? "text-emerald-600" : "text-red-600"} />
        </div>
      </div>

      <OverviewCharts hopSlices={hopSlices} typeSlices={typeSlices} directionSlices={directionSlices} />

      {/* 交易流水 */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
        <div className="px-4 py-3 border-b border-gray-100 dark:border-gray-700 flex items-center justify-between">
          <h2 className="text-sm font-bold text-gray-900 dark:text-gray-100">交易流水 <span className="text-gray-400 font-normal">({tradeableSignals.length}笔)</span></h2>
          <span className="text-[10px] text-gray-400">开仓→持仓→平仓</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="bg-gray-50 dark:bg-gray-900 text-gray-400 text-[11px]">
                <th className="px-3 py-2 text-left font-medium sticky left-0 bg-gray-50 dark:bg-gray-900">标的</th>
                <th className="px-2 py-2 text-center font-medium">方向</th>
                <th className="px-2 py-2 text-left font-medium border-l border-gray-200 dark:border-gray-600">开仓日</th>
                <th className="px-2 py-2 text-right font-medium">入场价</th>
                <th className="px-2 py-2 text-right font-medium border-l border-gray-200 dark:border-gray-600">T+1</th>
                <th className="px-2 py-2 text-right font-medium">T+3</th>
                <th className="px-2 py-2 text-right font-medium">T+5</th>
                <th className="px-2 py-2 text-right font-medium border-l border-gray-200 dark:border-gray-600">出场价</th>
                <th className="px-2 py-2 text-right font-medium">策略盈亏</th>
                <th className="px-2 py-2 text-right font-medium">vs300</th>
                <th className="px-2 py-2 text-center font-medium">关系</th>
                <th className="px-2 py-2 text-center font-medium">结果</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-50 dark:divide-gray-700/50">
              {tradeableSignals.map((s) => <TradeRow key={s.idx} signal={s} />)}
            </tbody>
          </table>
        </div>
      </div>

      {/* IS vs OOS */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-100 dark:border-gray-700">
          <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200">IS vs OOS 对比</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="bg-gray-50 dark:bg-gray-900 text-gray-500">
                <th className="px-4 py-2 text-left font-medium">持仓期</th>
                <th className="px-4 py-2 text-right font-medium">IS Sharpe</th>
                <th className="px-4 py-2 text-right font-medium">OOS Sharpe</th>
                <th className="px-4 py-2 text-right font-medium">全量</th>
                <th className="px-4 py-2 text-right font-medium">IS 胜率</th>
                <th className="px-4 py-2 text-right font-medium">OOS 胜率</th>
                <th className="px-4 py-2 text-right font-medium">IS n</th>
                <th className="px-4 py-2 text-right font-medium">OOS n</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100 dark:divide-gray-700">
              <ReportRow label="5D" is={report.is} oos={report.oos} all={report.all} period="5d" />
              <ReportRow label="10D" is={report.is} oos={report.oos} all={report.all} period="10d" />
            </tbody>
          </table>
        </div>
      </div>

      {/* 事件列表 */}
      <div>
        <h2 className="text-sm font-semibold text-gray-700 dark:text-gray-200 mb-3">事件列表</h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {eventsWithSignals.sort((a, b) => b.event_date.localeCompare(a.event_date)).map((event) => {
            const stats = eventStats[event.event_id];
            return (
              <Link key={event.event_id} href={`/events/${event.event_id}`}
                className="block bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4 hover:border-blue-300 hover:shadow-sm transition-all">
                <div className="flex items-start justify-between gap-2 mb-2">
                  <h3 className="text-sm font-medium text-gray-900 dark:text-gray-100 line-clamp-1">{event.title}</h3>
                  <EventTypeBadge type={event.type} />
                </div>
                <div className="text-xs text-gray-500 mb-2">{event.stock_code} · {event.event_date}</div>
                <div className="flex items-center gap-4 text-xs">
                  <span>信号 <strong>{stats?.signalCount ?? 0}</strong></span>
                  <span>胜率 <strong className={stats?.winRate > 0.7 ? "text-emerald-600" : ""}>{fmtPct(stats?.winRate)}</strong></span>
                  <span>超额 <strong className={stats?.avgExcess5d >= 0 ? "text-emerald-600" : "text-red-600"}>{fmtReturn(stats?.avgExcess5d)}</strong></span>
                </div>
              </Link>
            );
          })}
        </div>
      </div>
    </div>
  );
}

// ─── 子组件 ──────────────────────────────────────────

function StatusCard({ label, value, sub, color }: { label: string; value: string; sub: string; color: string }) {
  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
      <div className="text-[10px] text-gray-400 mb-1">{label}</div>
      <div className={`text-sm font-bold ${color}`}>{value}</div>
      <div className="text-[10px] text-gray-500 mt-1">{sub}</div>
    </div>
  );
}

function MiniStat({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div>
      <div className="text-[10px] text-gray-400">{label}</div>
      <div className={`text-lg font-bold ${color ?? "text-gray-900 dark:text-gray-100"}`}>{value}</div>
    </div>
  );
}

function Badge({ text, color }: { text: string; color: "emerald" | "blue" | "red" | "gray" }) {
  const cls = color === "emerald" ? "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-400" :
    color === "blue" ? "bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-400" :
    color === "red" ? "bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-400" :
    "bg-gray-100 text-gray-500 dark:bg-gray-700 dark:text-gray-400";
  return <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${cls}`}>{text}</span>;
}

function TradeRow({ signal: s }: { signal: SignalRecord }) {
  const trade = computeTradeInfo(s);
  const correct = isCorrect(s);
  const holdingReturns = [
    { day: "T+1", raw: s.fwd_return_1d },
    { day: "T+3", raw: s.fwd_return_3d },
    { day: "T+5", raw: s.fwd_return_5d },
  ].map((t) => ({ ...t, adj: t.raw != null ? (s.signal_direction === "avoid" ? -(t.raw) : t.raw) : null }));

  const relLabel = !s.relation_chain ? "-" :
    s.relation_chain.includes("COMPETES_WITH") ? "竞争" :
    s.relation_chain.includes("SUPPLIES_TO") || s.relation_chain.includes("CUSTOMER_OF") ? "供应链" :
    s.relation_chain.includes("COOPERATES_WITH") ? "合作" :
    s.relation_chain.includes("HOLDS_SHARES") ? "持仓" : "其他";
  const relColor = relLabel === "持仓" ? "text-gray-400" : relLabel === "竞争" ? "text-purple-600" : relLabel === "供应链" ? "text-blue-600" : "text-gray-600";

  return (
    <tr className="hover:bg-blue-50/30 dark:hover:bg-blue-900/10 group">
      <td className="px-3 py-2.5 sticky left-0 bg-white dark:bg-gray-800 group-hover:bg-blue-50/30">
        <Link href={`/signals/${s.idx}`} className="hover:underline">
          <div className="font-medium text-gray-900 dark:text-gray-100 text-[12px]">{s.target_name}</div>
          <div className="text-[10px] text-gray-400 font-mono flex items-center gap-1">{s.target_code} <HopBadge hop={s.hop} /></div>
        </Link>
      </td>
      <td className="px-2 py-2.5 text-center"><DirectionBadge direction={s.signal_direction as "avoid" | "long"} /></td>
      <td className="px-2 py-2.5 border-l border-gray-100 dark:border-gray-700 text-gray-600 dark:text-gray-300">{s.event_date}</td>
      <td className="px-2 py-2.5 text-right font-mono text-gray-700 dark:text-gray-300">¥{s.entry_price.toFixed(2)}</td>
      {holdingReturns.map((t) => (
        <td key={t.day} className={`px-2 py-2.5 text-right font-mono ${t.day === "T+1" ? "border-l border-gray-100 dark:border-gray-700" : ""}`}>
          {t.adj != null ? <span className={t.adj > 0 ? "text-emerald-600" : t.adj < 0 ? "text-red-500" : "text-gray-400"}>{t.adj > 0 ? "+" : ""}{(t.adj * 100).toFixed(1)}%</span> : <span className="text-gray-300">-</span>}
        </td>
      ))}
      <td className="px-2 py-2.5 text-right font-mono text-gray-700 dark:text-gray-300 border-l border-gray-100 dark:border-gray-700">¥{trade.exitPrice.toFixed(2)}</td>
      <td className="px-2 py-2.5 text-right">
        <span className={`font-mono font-semibold ${trade.pnlPercent > 0 ? "text-emerald-600" : "text-red-600"}`}>{trade.pnlPercent > 0 ? "+" : ""}{(trade.pnlPercent * 100).toFixed(2)}%</span>
      </td>
      <td className={`px-2 py-2.5 text-right font-mono ${returnColor(s.excess_5d)}`}>{fmtReturn(s.excess_5d)}</td>
      <td className={`px-2 py-2.5 text-center text-[10px] font-medium ${relColor}`}>{relLabel}</td>
      <td className="px-2 py-2.5 text-center"><CorrectnessIndicator correct={correct} /></td>
    </tr>
  );
}

function ReportRow({ label, is, oos, all, period }: { label: string; is: SplitMetricsRow; oos: SplitMetricsRow; all: SplitMetricsRow; period: "5d" | "10d" }) {
  const sharpe = (m: SplitMetricsRow) => period === "5d" ? m.sharpe_5d : m.sharpe_10d;
  const winRate = (m: SplitMetricsRow) => period === "5d" ? m.win_rate_5d : m.win_rate_10d;
  const n = (m: SplitMetricsRow) => period === "5d" ? m.n_5d : m.n_10d;
  return (
    <tr className="text-gray-700 dark:text-gray-300">
      <td className="px-4 py-2 font-medium">{label}</td>
      <td className={`px-4 py-2 text-right ${sharpeColor(sharpe(is))}`}>{fmtSharpe(sharpe(is))}</td>
      <td className={`px-4 py-2 text-right ${sharpeColor(sharpe(oos))}`}>{fmtSharpe(sharpe(oos))}</td>
      <td className={`px-4 py-2 text-right ${sharpeColor(sharpe(all))}`}>{fmtSharpe(sharpe(all))}</td>
      <td className="px-4 py-2 text-right">{winRate(is)}%</td>
      <td className="px-4 py-2 text-right">{winRate(oos)}%</td>
      <td className="px-4 py-2 text-right text-gray-400">{n(is)}</td>
      <td className="px-4 py-2 text-right text-gray-400">{n(oos)}</td>
    </tr>
  );
}
