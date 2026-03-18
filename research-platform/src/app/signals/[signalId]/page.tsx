"use client";
import Link from "next/link";
import { ArrowLeft, AlertTriangle } from "lucide-react";
import { getSignalDetail } from "@/lib/mock";
import { fmtReturn, returnColor, fmtPct, EVENT_TYPE_LABELS } from "@/lib/utils/format";
import { DirectionBadge, HopBadge, EventTypeBadge, SplitBadge, CorrectnessIndicator } from "@/components/badges";
import { PropagationPathStrip } from "@/components/paths";
import type { SignalDetailPayload, ConfidenceTerm } from "@/types";

// ─── Section: Header ─────────────────────────────────
function SignalHeader({ d }: { d: SignalDetailPayload }) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <div className="flex items-center gap-3 mb-3">
        <Link href="/signals" className="text-gray-400 hover:text-gray-600">
          <ArrowLeft size={18} />
        </Link>
        <h1 className="text-lg font-bold text-gray-900">
          信号 #{d.idx} · {d.target_name}
          <span className="text-sm font-normal text-gray-400 ml-2">{d.target_code}</span>
        </h1>
        <SplitBadge split={d.split} />
        <CorrectnessIndicator correct={d.correct} size="lg" />
      </div>
      <div className="flex items-center gap-4 flex-wrap text-sm">
        <div className="flex items-center gap-1.5">
          <span className="text-gray-400">方向</span>
          <DirectionBadge direction={d.signal_direction} size="lg" />
        </div>
        <div className="flex items-center gap-1.5">
          <span className="text-gray-400">跳数</span>
          <HopBadge hop={d.hop} />
        </div>
        <div className="flex items-center gap-1.5">
          <span className="text-gray-400">事件</span>
          <EventTypeBadge type={d.event_type} />
        </div>
        <div className="flex items-center gap-1.5">
          <span className="text-gray-400">置信度</span>
          <span className="font-mono font-semibold text-gray-900">{fmtPct(d.confidence)}</span>
        </div>
        <div className="flex items-center gap-1.5">
          <span className="text-gray-400">源事件</span>
          <Link
            href={`/events/${d.event_id}`}
            className="text-blue-600 hover:underline text-xs"
          >
            {d.source_event}
          </Link>
        </div>
      </div>
    </div>
  );
}

// ─── Section: Propagation Path ───────────────────────
function PathSection({ d }: { d: SignalDetailPayload }) {
  const hasSuspicious = d.path_segments.some((s) => s.is_suspicious);
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <h2 className="text-sm font-bold text-gray-900 mb-3">传播路径</h2>
      {d.path_segments.length > 0 ? (
        <>
          <PropagationPathStrip segments={d.path_segments} sourceCode={d.source_code} sourceName={d.source_name ?? d.source_code} targetCode={d.target_code} />
          {hasSuspicious && (
            <div className="mt-3 flex items-center gap-2 text-xs text-amber-700 bg-amber-50 rounded px-3 py-2 border border-amber-200">
              <AlertTriangle size={14} />
              <span>该路径包含可疑传播段（纯持股 / 跨行业）</span>
            </div>
          )}
        </>
      ) : (
        <div className="text-xs text-gray-400">源头信号（hop=0），无传播路径</div>
      )}
      <div className="mt-2 text-[11px] text-gray-400 font-mono">{d.propagation_path}</div>
    </div>
  );
}

// ─── Section: Direction Explanation ──────────────────
function RuleSection({ d }: { d: SignalDetailPayload }) {
  const r = d.rule_explanation;
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <h2 className="text-sm font-bold text-gray-900 mb-3">方向推断</h2>
      <div className="grid grid-cols-2 gap-4 text-xs">
        <div>
          <span className="text-gray-400">事件类型</span>
          <div className="mt-1 font-medium">{EVENT_TYPE_LABELS[r.event_type] || r.event_type}</div>
        </div>
        <div>
          <span className="text-gray-400">匹配规则集</span>
          <div className="mt-1 font-mono text-gray-700">{r.matched_rule_set}</div>
        </div>
        <div>
          <span className="text-gray-400">推断方向</span>
          <div className="mt-1"><DirectionBadge direction={r.inferred_direction} /></div>
        </div>
        <div>
          <span className="text-gray-400">规则标签</span>
          <div className="mt-1 font-medium text-gray-900">{r.rule_label}</div>
        </div>
      </div>

      {/* Confidence Breakdown */}
      {d.confidence_terms.length > 0 && (
        <div className="mt-4 pt-3 border-t border-gray-100">
          <h3 className="text-xs font-semibold text-gray-600 mb-2">置信度分解</h3>
          <table className="w-full text-xs">
            <thead>
              <tr className="text-gray-400">
                <th className="text-left py-1">项目</th>
                <th className="text-right py-1">原始值</th>
                <th className="text-right py-1">权重</th>
                <th className="text-right py-1">贡献</th>
              </tr>
            </thead>
            <tbody>
              {d.confidence_terms.map((t: ConfidenceTerm) => (
                <tr key={t.name} className="border-t border-gray-50">
                  <td className="py-1 font-mono text-gray-700">{t.name}</td>
                  <td className="py-1 text-right text-gray-600">{t.raw_value.toFixed(2)}</td>
                  <td className="py-1 text-right text-gray-400">{t.weight}</td>
                  <td className="py-1 text-right font-semibold text-gray-900">{t.contribution.toFixed(2)}</td>
                </tr>
              ))}
              <tr className="border-t border-gray-200">
                <td className="py-1 font-semibold">总计</td>
                <td colSpan={2}></td>
                <td className="py-1 text-right font-bold text-blue-600">
                  {d.confidence_terms.reduce((s, t) => s + t.contribution, 0).toFixed(2)}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// ─── Section: Price Outcome ──────────────────────────
function PriceOutcome({ d }: { d: SignalDetailPayload }) {
  const rows = [
    { label: "入场价", value: d.entry_price ? `¥${d.entry_price.toFixed(2)}` : "-", cls: "text-gray-900" },
    { label: "1D 收益", value: fmtReturn(d.fwd_return_1d), cls: returnColor(d.fwd_return_1d) },
    { label: "3D 收益", value: fmtReturn(d.fwd_return_3d), cls: returnColor(d.fwd_return_3d) },
    { label: "5D 调整收益", value: fmtReturn(d.adj_return_5d), cls: returnColor(d.adj_return_5d) },
    { label: "20D 收益", value: fmtReturn(d.fwd_return_20d), cls: returnColor(d.fwd_return_20d) },
    { label: "基准 5D", value: fmtReturn(d.benchmark_5d), cls: returnColor(d.benchmark_5d) },
    { label: "基准 10D", value: fmtReturn(d.benchmark_10d), cls: returnColor(d.benchmark_10d) },
    { label: "超额 10D", value: fmtReturn(d.excess_10d), cls: returnColor(d.excess_10d) },
    { label: "成交量变化 5D", value: d.volume_change_5d != null ? fmtPct(d.volume_change_5d) : "-", cls: "text-gray-900" },
  ];

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <h2 className="text-sm font-bold text-gray-900 mb-3">价格结果</h2>
      <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
        {rows.map((r) => (
          <div key={r.label} className="flex flex-col">
            <span className="text-[11px] text-gray-400">{r.label}</span>
            <span className={`text-sm font-mono font-semibold ${r.cls}`}>{r.value}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ─── Section: Context Signals ────────────────────────
function ContextSignals({ d }: { d: SignalDetailPayload }) {
  if (d.context_signals.length === 0) return null;

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <h2 className="text-sm font-bold text-gray-900 mb-3">
        同事件信号
        <span className="text-gray-400 font-normal ml-2">({d.context_signals.length})</span>
      </h2>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-left">
          <thead>
            <tr className="text-gray-400 border-b border-gray-100">
              <th className="py-1.5 px-2">#</th>
              <th className="py-1.5 px-2">目标</th>
              <th className="py-1.5 px-2">跳数</th>
              <th className="py-1.5 px-2">方向</th>
              <th className="py-1.5 px-2">调整收益5D</th>
              <th className="py-1.5 px-2">正确</th>
            </tr>
          </thead>
          <tbody>
            {d.context_signals.map((s) => (
              <tr
                key={s.idx}
                className={`border-b border-gray-50 ${s.idx === d.idx ? "bg-blue-50" : "hover:bg-gray-50"}`}
              >
                <td className="py-1.5 px-2">
                  <Link href={`/signals/${s.idx}`} className="text-blue-600 hover:underline font-mono">
                    {s.idx}
                  </Link>
                </td>
                <td className="py-1.5 px-2 font-medium">{s.target_name}</td>
                <td className="py-1.5 px-2"><HopBadge hop={s.hop} /></td>
                <td className="py-1.5 px-2"><DirectionBadge direction={s.signal_direction} /></td>
                <td className={`py-1.5 px-2 font-mono ${returnColor(s.adj_return_5d)}`}>
                  {fmtReturn(s.adj_return_5d)}
                </td>
                <td className="py-1.5 px-2"><CorrectnessIndicator correct={s.correct} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Page ────────────────────────────────────────────
export default function SignalDetailPage({ params }: { params: { signalId: string } }) {
  const { signalId } = params;
  const idx = Number(signalId);

  let detail: SignalDetailPayload;
  try {
    detail = getSignalDetail(idx);
  } catch {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400">
        信号 #{signalId} 不存在
      </div>
    );
  }

  return (
    <div className="space-y-4 max-w-5xl">
      <SignalHeader d={detail} />
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <PathSection d={detail} />
        <RuleSection d={detail} />
      </div>
      <PriceOutcome d={detail} />
      <ContextSignals d={detail} />
    </div>
  );
}
