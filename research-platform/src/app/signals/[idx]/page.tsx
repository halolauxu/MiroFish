import Link from "next/link";
import { ArrowLeft, ArrowRight, Check, X, TrendingUp, TrendingDown, AlertTriangle, Info } from "lucide-react";
import { loadSignal, isCorrect, computeTradeInfo } from "@/lib/data/loader";
import { fmtReturn, returnColor } from "@/lib/utils/format";
import { EventTypeBadge, DirectionBadge, HopBadge } from "@/components/badges";
import type { EventType } from "@/types";

// ─── 关系类型含义 ────────────────────────────────────
const RELATION_LABELS: Record<string, { label: string; desc: string; quality: "strong" | "weak" | "indirect" }> = {
  HOLDS_SHARES: { label: "基金持仓", desc: "同一基金/机构持有两只股票，不代表业务相关", quality: "indirect" },
  COMPETES_WITH: { label: "竞争关系", desc: "同行业竞争对手，事件可能产生替代效应", quality: "strong" },
  SUPPLIES_TO: { label: "供应商→客户", desc: "上游供应商向下游客户供货，产业链直接传导", quality: "strong" },
  CUSTOMER_OF: { label: "客户→供应商", desc: "下游需求变化影响上游供应商", quality: "strong" },
  COOPERATES_WITH: { label: "合作伙伴", desc: "业务合作关系，影响可能传导", quality: "strong" },
  BELONGS_TO: { label: "同板块", desc: "属于同一行业板块，可能有板块联动效应", quality: "weak" },
  RELATES_TO_CONCEPT: { label: "概念关联", desc: "共享某个市场概念/题材，关联度较弱", quality: "weak" },
};

// ─── A股方向映射规则 ──────────────────────────────────
const DIRECTION_RULES: Record<string, { rule: string; explain: string }> = {
  scandal: {
    rule: "板块连坐",
    explain: "A股特色：某公司出丑闻 → 整个板块情绪性下跌，竞争对手/上下游一起被砸",
  },
  policy_risk: {
    rule: "板块连坐",
    explain: "政策风险波及同行业所有公司，市场恐慌不分个股",
  },
  management_change: {
    rule: "板块连坐",
    explain: "管理层变动引发行业不确定性，市场倾向规避整个板块",
  },
  product_launch: {
    rule: "利好出尽",
    explain: "新产品发布后，利好已被提前计价，短期反而可能获利了结回调",
  },
  technology_breakthrough: {
    rule: "利好出尽",
    explain: "技术突破公告后，市场预期已反映在股价中，短期易回调",
  },
  price_adjustment: {
    rule: "利好出尽",
    explain: "涨价预期兑现后，资金倾向于获利了结",
  },
  buyback: {
    rule: "利好出尽",
    explain: "回购方案落地即利好消化，短期无进一步催化剂",
  },
  cooperation: {
    rule: "正面传导",
    explain: "合作关系可能为双方带来正向预期，利好传导至合作伙伴",
  },
  earnings_surprise: {
    rule: "正面传导",
    explain: "龙头业绩超预期 → 市场重估整个产业链定价",
  },
  supply_shortage: {
    rule: "涨价预期",
    explain: "供应短缺 → 产品涨价预期 → 相关公司（包括竞品/替代品）受益",
  },
  order_win: {
    rule: "正面传导",
    explain: "获得大单利好供应链上下游公司",
  },
  ma: {
    rule: "并购重组",
    explain: "产业整合带来估值重塑，并购标的和关联方都可能受益",
  },
};

export default function SignalDetailPage({ params }: { params: { idx: string } }) {
  const idx = parseInt(params.idx, 10);
  const signal = loadSignal(idx);

  if (!signal) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400 dark:text-gray-500">
        信号 #{idx} 不存在
      </div>
    );
  }

  const correct = isCorrect(signal);
  const trade = computeTradeInfo(signal);
  const directionInfo = DIRECTION_RULES[signal.event_type] || { rule: "未知", explain: "无映射规则" };

  // 解析传播路径和关系链
  const pathNodes = signal.propagation_path
    ? signal.propagation_path.split(/\s*→\s*/).filter(Boolean)
    : [signal.source_code, signal.target_code];

  const relations = signal.relation_chain
    ? signal.relation_chain.split(/\s*→\s*/).filter(Boolean)
    : [];

  // 分析关系链质量
  const relationInfos = relations.map((r) => RELATION_LABELS[r] || { label: r, desc: "未知关系类型", quality: "weak" as const });
  const hasWeakLink = relationInfos.some((r) => r.quality === "indirect");
  const allIndirect = relationInfos.every((r) => r.quality === "indirect");

  // Agent 辩论状态分析
  const debateActive = signal.divergence > 0 || (signal.debate_summary && signal.debate_summary.length > 10);
  const debateStatus = !debateActive
    ? { label: "未触发", color: "text-gray-400", explain: "Agent辩论模块未运行（可能输入不足或被跳过），共识方向和信念度为默认值" }
    : signal.divergence > 0.4
      ? { label: "高分歧", color: "text-red-600", explain: "多个Agent对方向判断不一致，信号可靠性较低" }
      : signal.divergence > 0.15
        ? { label: "中等分歧", color: "text-amber-600", explain: "Agent之间有一定分歧，需要谨慎参考" }
        : { label: "高共识", color: "text-emerald-600", explain: "Agent们对方向判断一致，信号可靠性较高" };

  // 置信度公式各项
  const convictionPart = signal.conviction * 0.4;
  const shockPart = signal.shock_weight * 0.1;
  const penalty = signal.reacted ? 0.5 : 1.0;

  return (
    <div className="space-y-4 max-w-4xl">
      {/* 信号头部 */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-5">
        <div className="flex items-center gap-3 mb-3">
          <Link href={`/events/${signal.event_id}`} className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300">
            <ArrowLeft size={18} />
          </Link>
          <h1 className="text-lg font-bold text-gray-900 dark:text-gray-100">信号 #{idx}</h1>
          <ArrowRight size={14} className="text-gray-300" />
          <span className="font-medium text-gray-900 dark:text-gray-100">{signal.target_name}</span>
          <span className="font-mono text-xs text-gray-400">{signal.target_code}</span>
          <DirectionBadge direction={signal.signal_direction as "avoid" | "long"} size="lg" />
        </div>

        {/* 关系链质量警告 */}
        {allIndirect && (
          <div className="flex items-start gap-2 mt-2 p-2.5 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-800 rounded text-xs text-amber-700 dark:text-amber-400">
            <AlertTriangle size={14} className="shrink-0 mt-0.5" />
            <div>
              <strong>传播质量警告：</strong>该信号完全通过「基金持仓」关系传播（{signal.source_name} → 基金 → {signal.target_name}），
              两者之间无实际业务关联，仅因被同一基金持有而被关联。此类信号的逻辑基础较弱。
            </div>
          </div>
        )}
        {hasWeakLink && !allIndirect && (
          <div className="flex items-start gap-2 mt-2 p-2.5 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded text-xs text-blue-700 dark:text-blue-400">
            <Info size={14} className="shrink-0 mt-0.5" />
            <div>
              传播链路中包含「基金持仓」间接关系，信号逻辑强度中等。
            </div>
          </div>
        )}
      </div>

      {/* ══════ 交易卡片区：开仓 → 持仓 → 平仓 ══════ */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {/* 开仓卡 */}
        <div className="bg-white dark:bg-gray-800 rounded-lg border-2 border-blue-200 dark:border-blue-800 p-4">
          <div className="flex items-center gap-2 mb-3">
            <div className="w-6 h-6 rounded-full bg-blue-100 dark:bg-blue-900 flex items-center justify-center text-blue-600 text-xs font-bold">1</div>
            <h3 className="text-sm font-bold text-blue-700 dark:text-blue-400">开仓</h3>
          </div>
          <div className="space-y-2 text-xs">
            <div className="flex justify-between">
              <span className="text-gray-500 dark:text-gray-400">日期</span>
              <span className="font-medium text-gray-900 dark:text-gray-100">{trade.entryDate}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500 dark:text-gray-400">入场价</span>
              <span className="font-mono font-bold text-gray-900 dark:text-gray-100">¥{trade.entryPrice.toFixed(2)}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500 dark:text-gray-400">方向</span>
              <span className={`font-medium ${trade.direction === "long" ? "text-emerald-600" : "text-red-600"}`}>
                {trade.direction === "long" ? "↑ 做多" : "↓ 做空/规避"}
              </span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500 dark:text-gray-400">仓位建议</span>
              <span className={`font-medium ${
                trade.confidenceLevel === "重仓" ? "text-red-600" :
                trade.confidenceLevel === "中仓" ? "text-amber-600" :
                trade.confidenceLevel === "不建议" ? "text-gray-400" :
                "text-blue-600"
              }`}>{trade.confidenceLevel}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-gray-500 dark:text-gray-400">置信度</span>
              <span className="font-mono">{(signal.confidence * 100).toFixed(1)}%</span>
            </div>
          </div>
        </div>

        {/* 持仓卡 */}
        <div className="bg-white dark:bg-gray-800 rounded-lg border-2 border-amber-200 dark:border-amber-800 p-4">
          <div className="flex items-center gap-2 mb-3">
            <div className="w-6 h-6 rounded-full bg-amber-100 dark:bg-amber-900 flex items-center justify-center text-amber-600 text-xs font-bold">2</div>
            <h3 className="text-sm font-bold text-amber-700 dark:text-amber-400">持仓 {trade.holdingDays} 天</h3>
          </div>
          <div className="space-y-1.5 text-xs">
            {trade.timeline.map((t) => (
              <div key={t.day} className={`flex justify-between items-center py-0.5 ${t.day === "T+5" ? "bg-amber-50 dark:bg-amber-900/20 px-1.5 rounded font-medium" : ""}`}>
                <div className="flex items-center gap-2">
                  <span className="text-gray-500 dark:text-gray-400 w-8">{t.day}</span>
                  <span className="text-gray-400 text-[10px]">{t.label}</span>
                </div>
                <span className={`font-mono ${returnColor(t.ret)}`}>
                  {t.ret != null ? fmtReturn(t.ret) : "-"}
                </span>
              </div>
            ))}
            <div className="border-t border-gray-100 dark:border-gray-700 pt-1.5 mt-1.5">
              <div className="flex justify-between">
                <span className="text-gray-500 dark:text-gray-400">成交量变化(5D)</span>
                <span className={`font-mono ${returnColor(signal.volume_change_5d)}`}>
                  {fmtReturn(signal.volume_change_5d)}
                </span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500 dark:text-gray-400">信息差状态</span>
                <span className={signal.alpha_type === "未反应" ? "text-emerald-600 font-medium" : "text-amber-600"}>
                  {signal.alpha_type}
                </span>
              </div>
            </div>
          </div>
        </div>

        {/* 平仓卡 */}
        <div className={`rounded-lg border-2 p-4 ${
          signal.fwd_return_5d == null
            ? "bg-gray-50 dark:bg-gray-800 border-gray-200 dark:border-gray-700"
            : trade.pnlPercent > 0
              ? "bg-emerald-50 dark:bg-emerald-950/30 border-emerald-300 dark:border-emerald-800"
              : "bg-red-50 dark:bg-red-950/30 border-red-300 dark:border-red-800"
        }`}>
          <div className="flex items-center gap-2 mb-3">
            <div className={`w-6 h-6 rounded-full flex items-center justify-center text-xs font-bold ${
              signal.fwd_return_5d == null
                ? "bg-gray-200 dark:bg-gray-700 text-gray-400"
                : trade.pnlPercent > 0
                  ? "bg-emerald-200 dark:bg-emerald-800 text-emerald-700 dark:text-emerald-300"
                  : "bg-red-200 dark:bg-red-800 text-red-700 dark:text-red-300"
            }`}>3</div>
            <h3 className={`text-sm font-bold ${
              signal.fwd_return_5d == null
                ? "text-gray-400"
                : trade.pnlPercent > 0
                  ? "text-emerald-700 dark:text-emerald-400"
                  : "text-red-700 dark:text-red-400"
            }`}>
              {signal.fwd_return_5d == null ? "待平仓" : "平仓结算"}
            </h3>
          </div>
          {signal.fwd_return_5d != null ? (
            <div className="space-y-2 text-xs">
              <div className="flex justify-between">
                <span className="text-gray-500 dark:text-gray-400">出场日期</span>
                <span className="font-medium text-gray-900 dark:text-gray-100">{trade.exitDate}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-500 dark:text-gray-400">出场价</span>
                <span className="font-mono font-bold text-gray-900 dark:text-gray-100">¥{trade.exitPrice.toFixed(2)}</span>
              </div>
              <div className="border-t border-gray-200 dark:border-gray-600 pt-2 mt-2 space-y-1.5">
                <div className="flex justify-between items-center">
                  <span className="text-gray-500 dark:text-gray-400">股价变动</span>
                  <span className={`font-mono font-bold ${returnColor(signal.fwd_return_5d)}`}>
                    {fmtReturn(signal.fwd_return_5d)}
                  </span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-gray-500 dark:text-gray-400">策略盈亏</span>
                  <div className="flex items-center gap-1">
                    {trade.pnlPercent > 0 ? <TrendingUp size={12} className="text-emerald-600" /> : <TrendingDown size={12} className="text-red-600" />}
                    <span className={`font-mono font-bold text-sm ${trade.pnlPercent > 0 ? "text-emerald-600" : "text-red-600"}`}>
                      {trade.pnlPercent > 0 ? "+" : ""}{(trade.pnlPercent * 100).toFixed(2)}%
                    </span>
                  </div>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-gray-500 dark:text-gray-400">每股盈亏</span>
                  <span className={`font-mono ${trade.pnlPerShare >= 0 ? "text-emerald-600" : "text-red-600"}`}>
                    {trade.pnlPerShare >= 0 ? "+" : ""}¥{trade.pnlPerShare.toFixed(2)}
                  </span>
                </div>
                <div className="flex justify-between items-center">
                  <span className="text-gray-500 dark:text-gray-400">vs 沪深300</span>
                  <span className={`font-mono ${returnColor(trade.excessReturn)}`}>
                    {fmtReturn(trade.excessReturn)}
                  </span>
                </div>
              </div>
              {/* 交易判定 */}
              <div className={`flex items-center gap-2 mt-2 p-2 rounded ${
                correct
                  ? "bg-emerald-100 dark:bg-emerald-900/40"
                  : "bg-red-100 dark:bg-red-900/40"
              }`}>
                {correct ? <Check size={14} className="text-emerald-600" /> : <X size={14} className="text-red-600" />}
                <span className={`text-xs font-medium ${correct ? "text-emerald-700 dark:text-emerald-300" : "text-red-700 dark:text-red-300"}`}>
                  {correct ? "交易方向正确" : "交易方向错误"}
                </span>
              </div>
            </div>
          ) : (
            <p className="text-xs text-gray-400">等待 T+5 后验证</p>
          )}
        </div>
      </div>

      {/* ══════ 推理链路：信号是如何产生的 ══════ */}
      <div className="bg-white dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-5">
        <h2 className="text-sm font-bold text-gray-900 dark:text-gray-100 mb-4">信号推理链路</h2>

        <div className="space-y-0">
          {/* Step 1: 源头事件 */}
          <TimelineStep number={1} title="源头事件" status="completed">
            <div className="flex items-center gap-2 mb-2">
              <EventTypeBadge type={signal.event_type as EventType} />
              <span className="text-sm font-medium text-gray-900 dark:text-gray-100">{signal.source_event}</span>
            </div>
            <div className="text-xs text-gray-500 dark:text-gray-400 space-y-1">
              <div>源头标的: <span className="font-mono">{signal.source_code}</span> {signal.source_name} · {signal.event_date}</div>
              <Link href={`/events/${signal.event_id}`} className="text-blue-600 hover:underline inline-flex items-center gap-1">
                查看事件详情 <ArrowRight size={12} />
              </Link>
            </div>
          </TimelineStep>

          {/* Step 2: 图谱传播（增加关系含义解释） */}
          <TimelineStep number={2} title="图谱传播" status="completed">
            {/* 传播路径可视化 */}
            <div className="flex flex-wrap items-center gap-1 text-sm mb-3">
              {pathNodes.map((code, i) => {
                const isInst = code.startsWith("inst::");
                return (
                  <span key={i} className="flex items-center gap-1">
                    {i > 0 && (
                      <span className="flex flex-col items-center mx-1">
                        <ArrowRight size={14} className="text-gray-300" />
                        {relations[i - 1] && (
                          <span className={`text-[9px] mt-0.5 ${
                            RELATION_LABELS[relations[i - 1]]?.quality === "indirect" ? "text-amber-500" :
                            RELATION_LABELS[relations[i - 1]]?.quality === "strong" ? "text-emerald-500" :
                            "text-gray-400"
                          }`}>
                            {RELATION_LABELS[relations[i - 1]]?.label || relations[i - 1]}
                          </span>
                        )}
                      </span>
                    )}
                    <span className={`font-mono px-2 py-1 rounded text-xs ${
                      isInst
                        ? "bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300 border border-purple-200 dark:border-purple-700"
                        : i === 0
                          ? "bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300 border border-amber-200 dark:border-amber-700"
                          : "bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300"
                    }`}>
                      {isInst ? `🏦 ${code.replace("inst::", "机构")}` : code}
                    </span>
                  </span>
                );
              })}
            </div>

            {/* 关系链详解 */}
            {relations.length > 0 && (
              <div className="mb-3 space-y-1.5">
                {relationInfos.map((info, i) => (
                  <div key={i} className={`flex items-start gap-2 text-xs p-2 rounded ${
                    info.quality === "indirect" ? "bg-amber-50 dark:bg-amber-900/10 border border-amber-100 dark:border-amber-800" :
                    info.quality === "strong" ? "bg-emerald-50 dark:bg-emerald-900/10 border border-emerald-100 dark:border-emerald-800" :
                    "bg-gray-50 dark:bg-gray-900 border border-gray-100 dark:border-gray-700"
                  }`}>
                    <span className={`shrink-0 px-1.5 py-0.5 rounded text-[10px] font-medium ${
                      info.quality === "indirect" ? "bg-amber-200 text-amber-800 dark:bg-amber-800 dark:text-amber-200" :
                      info.quality === "strong" ? "bg-emerald-200 text-emerald-800 dark:bg-emerald-800 dark:text-emerald-200" :
                      "bg-gray-200 text-gray-600 dark:bg-gray-700 dark:text-gray-300"
                    }`}>
                      {info.quality === "indirect" ? "间接" : info.quality === "strong" ? "强关联" : "弱关联"}
                    </span>
                    <div>
                      <span className="font-medium text-gray-700 dark:text-gray-300">{info.label}</span>
                      <span className="text-gray-500 dark:text-gray-400 ml-1">— {info.desc}</span>
                    </div>
                  </div>
                ))}
              </div>
            )}

            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
              <KVItem label="跳数（经过几层关系）" value={<><HopBadge hop={signal.hop} /> <span className="text-gray-400 text-[10px] ml-1">{signal.hop === 0 ? "源头本身" : signal.hop === 1 ? "直接关联" : `${signal.hop}层传播`}</span></>} />
              <KVItem label="冲击权重（传播衰减后）" value={<><span className="font-mono">{signal.shock_weight.toFixed(2)}</span> <span className="text-gray-400 text-[10px] ml-1">衰减{((1 - signal.shock_weight) * 100).toFixed(0)}%</span></>} />
              <KVItem label="关系链" value={<span className="font-mono text-gray-600 dark:text-gray-300">{signal.relation_chain}</span>} />
              <KVItem label="传播路径" value={<span className="font-mono text-gray-600 dark:text-gray-300 text-[10px]">{signal.propagation_path || "-"}</span>} />
            </div>
          </TimelineStep>

          {/* Step 3: 方向推断（增加解释） */}
          <TimelineStep number={3} title="方向推断" status="completed">
            <div className="mb-3">
              <div className="flex items-center gap-2 text-sm mb-1.5">
                <DirectionBadge direction={signal.signal_direction as "avoid" | "long"} size="lg" />
                <span className="text-xs font-medium text-gray-700 dark:text-gray-300">规则: {directionInfo.rule}</span>
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400 bg-blue-50 dark:bg-blue-900/10 border border-blue-100 dark:border-blue-800 p-2 rounded">
                <strong>映射逻辑：</strong>{directionInfo.explain}
              </div>
            </div>

            {/* Agent 辩论状态 */}
            <div className="border-t border-gray-100 dark:border-gray-700 pt-3 mt-3">
              <div className="text-[10px] text-gray-400 dark:text-gray-500 mb-2">Agent 多空辩论</div>
              <div className="flex items-center gap-3 mb-2">
                <span className={`text-xs font-medium ${debateStatus.color}`}>● {debateStatus.label}</span>
                <span className="text-[10px] text-gray-400">共识方向: {signal.consensus_direction || "无"}</span>
                <span className="text-[10px] text-gray-400">分歧度: {signal.divergence.toFixed(3)}</span>
                <span className="text-[10px] text-gray-400">信念度: {signal.conviction.toFixed(3)}</span>
              </div>
              <div className="text-[11px] text-gray-500 dark:text-gray-400 bg-gray-50 dark:bg-gray-900 p-2 rounded">
                {debateStatus.explain}
              </div>
              {signal.debate_summary && signal.debate_summary.length > 10 && (
                <div className="mt-2 text-xs text-gray-500 dark:text-gray-400 bg-gray-50 dark:bg-gray-900 p-2 rounded">
                  <span className="font-medium text-gray-600 dark:text-gray-300">辩论摘要: </span>{signal.debate_summary}
                </div>
              )}
            </div>
          </TimelineStep>

          {/* Step 4: 置信度 → 仓位（增加公式解释） */}
          <TimelineStep number={4} title="置信度 → 仓位" status="completed" isLast>
            <div className="flex items-center gap-4 mb-3">
              <div className="text-2xl font-bold text-gray-900 dark:text-gray-100">{(signal.confidence * 100).toFixed(1)}%</div>
              <span className={`px-2 py-1 rounded text-xs font-medium ${
                trade.confidenceLevel === "重仓" ? "bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-400" :
                trade.confidenceLevel === "中仓" ? "bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-400" :
                trade.confidenceLevel === "不建议" ? "bg-gray-100 text-gray-500" :
                "bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-400"
              }`}>
                {trade.confidenceLevel}
              </span>
              <span className={`text-xs ${signal.alpha_type === "未反应" ? "text-emerald-600" : "text-amber-600"}`}>
                {signal.alpha_type}
              </span>
            </div>

            {/* 置信度公式（带解释） */}
            <div className="bg-gray-50 dark:bg-gray-900 rounded p-3 space-y-2">
              <div className="text-[10px] text-gray-400 dark:text-gray-500 font-medium">置信度 = (信念度×0.4 + 事件加成 + 跳数加成 + 冲击权重×0.1) × 惩罚系数</div>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 text-xs">
                <div className="bg-white dark:bg-gray-800 rounded p-2 border border-gray-100 dark:border-gray-700">
                  <div className="text-[9px] text-gray-400">信念度 × 0.4</div>
                  <div className="font-mono font-medium text-gray-900 dark:text-gray-100">{convictionPart.toFixed(3)}</div>
                  <div className="text-[9px] text-gray-400">conviction={signal.conviction.toFixed(2)}</div>
                </div>
                <div className="bg-white dark:bg-gray-800 rounded p-2 border border-gray-100 dark:border-gray-700">
                  <div className="text-[9px] text-gray-400">冲击权重 × 0.1</div>
                  <div className="font-mono font-medium text-gray-900 dark:text-gray-100">{shockPart.toFixed(3)}</div>
                  <div className="text-[9px] text-gray-400">shock={signal.shock_weight.toFixed(2)}</div>
                </div>
                <div className="bg-white dark:bg-gray-800 rounded p-2 border border-gray-100 dark:border-gray-700">
                  <div className="text-[9px] text-gray-400">事件+跳数加成</div>
                  <div className="font-mono font-medium text-gray-900 dark:text-gray-100">
                    {(signal.confidence / penalty - convictionPart - shockPart).toFixed(3)}
                  </div>
                  <div className="text-[9px] text-gray-400">按事件类型和hop级别</div>
                </div>
                <div className={`rounded p-2 border ${
                  signal.reacted
                    ? "bg-amber-50 dark:bg-amber-900/20 border-amber-200 dark:border-amber-700"
                    : "bg-emerald-50 dark:bg-emerald-900/20 border-emerald-200 dark:border-emerald-700"
                }`}>
                  <div className="text-[9px] text-gray-400">惩罚系数</div>
                  <div className={`font-mono font-medium ${signal.reacted ? "text-amber-700 dark:text-amber-400" : "text-emerald-700 dark:text-emerald-400"}`}>
                    ×{penalty.toFixed(1)}
                  </div>
                  <div className="text-[9px] text-gray-400">
                    {signal.reacted ? "已反应 → 打5折" : "未反应 → 不打折"}
                  </div>
                </div>
              </div>
              <div className="text-[10px] text-gray-400 pt-1 border-t border-gray-100 dark:border-gray-700">
                仓位分级: ≥45% → 重仓 | ≥25% → 中仓 | &lt;25% → 轻仓 | hop=0 → 不建议（源头信号不交易）
              </div>
            </div>
          </TimelineStep>
        </div>
      </div>
    </div>
  );
}

// ─── 子组件 ──────────────────────────────────────────

function TimelineStep({
  number: num,
  title,
  status,
  children,
  isLast,
}: {
  number: number;
  title: string;
  status: "completed" | "pending";
  children: React.ReactNode;
  isLast?: boolean;
}) {
  return (
    <div className="flex gap-3">
      <div className="flex flex-col items-center">
        <div className={`w-7 h-7 rounded-full flex items-center justify-center text-[10px] font-bold shrink-0 ${
          status === "completed" ? "bg-blue-600 text-white" : "bg-gray-200 dark:bg-gray-700 text-gray-500"
        }`}>{num}</div>
        {!isLast && <div className="w-0.5 flex-1 bg-gray-200 dark:bg-gray-700 min-h-[12px]" />}
      </div>
      <div className="flex-1 pb-4">
        <div className="text-xs font-semibold text-gray-700 dark:text-gray-300 mb-1.5">{title}</div>
        <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-3">{children}</div>
      </div>
    </div>
  );
}

function KVItem({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div>
      <div className="text-[10px] text-gray-400 dark:text-gray-500 mb-0.5">{label}</div>
      <div className="text-gray-900 dark:text-gray-100">{value}</div>
    </div>
  );
}
