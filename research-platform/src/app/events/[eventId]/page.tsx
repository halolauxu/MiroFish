"use client";
import { useMemo } from "react";
import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { mockEvents, mockSignals } from "@/lib/mock";
import { fmtReturn, returnColor, fmtPct } from "@/lib/utils/format";
import { EventTypeBadge, SplitBadge, DirectionBadge, HopBadge, CorrectnessIndicator } from "@/components/badges";
import { buildHref } from "@/lib/query-state";
import type { EventRecord, SignalRecord } from "@/types";

// ─── Event Header ────────────────────────────────────
function EventHeader({ event }: { event: EventRecord }) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <div className="flex items-center gap-3 mb-3">
        <Link href="/events" className="text-gray-400 hover:text-gray-600">
          <ArrowLeft size={18} />
        </Link>
        <h1 className="text-lg font-bold text-gray-900">{event.title}</h1>
        <SplitBadge split={event.split} />
        <EventTypeBadge type={event.type} />
      </div>
      <p className="text-sm text-gray-600 mb-3">{event.summary}</p>
      <div className="flex items-center gap-4 text-xs text-gray-500">
        <span>
          <span className="text-gray-400">标的 </span>
          <span className="font-mono font-medium text-gray-900">{event.stock_code} {event.stock_name}</span>
        </span>
        <span>
          <span className="text-gray-400">日期 </span>
          <span className="font-medium">{event.event_date}</span>
        </span>
        <span>
          <span className="text-gray-400">影响 </span>
          <span className={`font-medium ${event.impact_level === "high" ? "text-red-600" : event.impact_level === "medium" ? "text-amber-600" : "text-gray-600"}`}>
            {event.impact_level}
          </span>
        </span>
        {event.key_data && (
          <span className="text-gray-400">{event.key_data}</span>
        )}
      </div>
    </div>
  );
}

// ─── Event Metrics ───────────────────────────────────
function EventMetrics({ signals }: { signals: SignalRecord[] }) {
  const winCount = signals.filter((s) => s.correct).length;
  const winRate = signals.length > 0 ? winCount / signals.length : 0;
  const avgReturn = signals.length > 0 ? signals.reduce((s, sig) => s + (sig.adj_return_5d ?? 0), 0) / signals.length : 0;
  const hopDist = [0, 1, 2, 3].map((h) => signals.filter((s) => s.hop === h).length);

  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <h2 className="text-sm font-bold text-gray-900 mb-3">事件级指标</h2>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div>
          <div className="text-[11px] text-gray-400">信号数</div>
          <div className="text-xl font-bold text-gray-900">{signals.length}</div>
        </div>
        <div>
          <div className="text-[11px] text-gray-400">胜率</div>
          <div className={`text-xl font-bold ${winRate >= 0.7 ? "text-emerald-600" : winRate < 0.5 ? "text-red-600" : "text-gray-900"}`}>
            {fmtPct(winRate)}
          </div>
        </div>
        <div>
          <div className="text-[11px] text-gray-400">平均调整收益</div>
          <div className={`text-xl font-bold font-mono ${returnColor(avgReturn)}`}>
            {fmtReturn(avgReturn)}
          </div>
        </div>
        <div>
          <div className="text-[11px] text-gray-400">Hop 分布</div>
          <div className="flex items-center gap-1 mt-1">
            {hopDist.map((count, i) => (
              <div key={i} className="flex items-center gap-0.5">
                <HopBadge hop={i} />
                <span className="text-xs text-gray-500">{count}</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

// ─── Signals Table ───────────────────────────────────
function EventSignalsTable({ signals, eventId }: { signals: SignalRecord[]; eventId: string }) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-5">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-sm font-bold text-gray-900">传播信号</h2>
        <Link
          href={buildHref("/signals", { event_id: eventId })}
          className="text-xs text-blue-600 hover:underline"
        >
          在 Signals 页查看 →
        </Link>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-left">
          <thead>
            <tr className="text-gray-400 border-b border-gray-100">
              <th className="py-2 px-2">#</th>
              <th className="py-2 px-2">目标</th>
              <th className="py-2 px-2">Hop</th>
              <th className="py-2 px-2">关系链</th>
              <th className="py-2 px-2">方向</th>
              <th className="py-2 px-2">置信度</th>
              <th className="py-2 px-2">调整收益5D</th>
              <th className="py-2 px-2">超额5D</th>
              <th className="py-2 px-2">已反应</th>
              <th className="py-2 px-2">正确</th>
            </tr>
          </thead>
          <tbody>
            {signals.map((s) => (
              <tr key={s.idx} className="border-b border-gray-50 hover:bg-blue-50/30">
                <td className="py-2 px-2">
                  <Link href={`/signals/${s.idx}`} className="text-blue-600 hover:underline font-mono">
                    {s.idx}
                  </Link>
                </td>
                <td className="py-2 px-2">
                  <span className="font-medium">{s.target_name}</span>
                  <span className="text-gray-400 ml-1 font-mono text-[10px]">{s.target_code}</span>
                </td>
                <td className="py-2 px-2"><HopBadge hop={s.hop} /></td>
                <td className="py-2 px-2 font-mono text-gray-500 text-[11px]">{s.relation_chain}</td>
                <td className="py-2 px-2"><DirectionBadge direction={s.signal_direction} /></td>
                <td className="py-2 px-2 font-mono">{fmtPct(s.confidence)}</td>
                <td className={`py-2 px-2 font-mono ${returnColor(s.adj_return_5d)}`}>
                  {fmtReturn(s.adj_return_5d)}
                </td>
                <td className={`py-2 px-2 font-mono ${returnColor(s.excess_5d)}`}>
                  {fmtReturn(s.excess_5d)}
                </td>
                <td className="py-2 px-2">
                  <span className={s.reacted ? "text-amber-600" : "text-gray-400"}>
                    {s.reacted ? "是" : "否"}
                  </span>
                </td>
                <td className="py-2 px-2"><CorrectnessIndicator correct={s.correct} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ─── Page ────────────────────────────────────────────
export default function EventDetailPage({ params }: { params: { eventId: string } }) {
  const { eventId } = params;

  const event = mockEvents.find((e) => e.event_id === eventId);
  const signals = useMemo(
    () => mockSignals.filter((s) => s.event_id === eventId),
    [eventId]
  );

  if (!event) {
    return (
      <div className="flex items-center justify-center h-64 text-gray-400">
        Event {eventId} 不存在
      </div>
    );
  }

  return (
    <div className="space-y-4 max-w-5xl">
      <EventHeader event={event} />
      <EventMetrics signals={signals} />
      <EventSignalsTable signals={signals} eventId={eventId} />
    </div>
  );
}
