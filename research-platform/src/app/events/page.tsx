"use client";
import { useMemo } from "react";
import Link from "next/link";
import { useGlobalFilter } from "@/lib/query-state";
import { mockEvents } from "@/lib/mock";
import { fmtPct, fmtReturn, returnColor } from "@/lib/utils/format";
import { EventTypeBadge, SplitBadge } from "@/components/badges";
import type { EventRecord } from "@/types";

function EventCard({ event }: { event: EventRecord }) {
  return (
    <Link
      href={`/events/${event.event_id}`}
      className="block bg-white rounded-lg border border-gray-200 p-4 hover:border-blue-300 hover:shadow-sm transition-all"
    >
      <div className="flex items-center gap-2 mb-2">
        <SplitBadge split={event.split} />
        <EventTypeBadge type={event.type} />
        <span className={`text-[10px] px-1.5 py-0.5 rounded ${
          event.impact_level === "high" ? "bg-red-100 text-red-700" :
          event.impact_level === "medium" ? "bg-amber-100 text-amber-700" :
          "bg-gray-100 text-gray-600"
        }`}>
          {event.impact_level}
        </span>
      </div>

      <h3 className="text-sm font-bold text-gray-900 mb-1 line-clamp-1">{event.title}</h3>
      <p className="text-xs text-gray-500 mb-2 line-clamp-2">{event.summary}</p>

      <div className="flex items-center gap-1.5 text-[11px] text-gray-400 mb-3">
        <span className="font-mono">{event.stock_code}</span>
        <span>{event.stock_name}</span>
        <span className="ml-auto">{event.event_date}</span>
      </div>

      <div className="flex items-center gap-4 pt-2 border-t border-gray-100 text-xs">
        <div>
          <span className="text-gray-400">信号数 </span>
          <span className="font-semibold text-gray-900">{event.signal_count}</span>
        </div>
        <div>
          <span className="text-gray-400">胜率 </span>
          <span className={`font-semibold ${event.win_rate >= 0.7 ? "text-emerald-600" : event.win_rate < 0.5 ? "text-red-600" : "text-gray-900"}`}>
            {fmtPct(event.win_rate)}
          </span>
        </div>
        <div>
          <span className="text-gray-400">平均调整 </span>
          <span className={`font-semibold font-mono ${returnColor(event.avg_adj_return)}`}>
            {fmtReturn(event.avg_adj_return)}
          </span>
        </div>
      </div>
    </Link>
  );
}

export default function EventsPage() {
  const { split } = useGlobalFilter();

  const filteredEvents = useMemo(() => {
    if (split === "all") return mockEvents;
    return mockEvents.filter((e) => e.split === split);
  }, [split]);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-bold text-gray-900">事件列表</h1>
        <span className="text-xs text-gray-400">{filteredEvents.length} 个事件</span>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {filteredEvents.map((event) => (
          <EventCard key={event.event_id} event={event} />
        ))}
      </div>

      {filteredEvents.length === 0 && (
        <div className="text-center py-12 text-gray-400 text-sm">
          暂无事件数据
        </div>
      )}
    </div>
  );
}
