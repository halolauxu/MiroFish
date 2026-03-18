"use client";
import { useMemo, useState } from "react";
import Link from "next/link";
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  getFilteredRowModel,
  flexRender,
  createColumnHelper,
  type SortingState,
} from "@tanstack/react-table";
import { useRouter } from "next/navigation";
import { useGlobalFilter, useQueryParam, useSetQueryParams } from "@/lib/query-state";
import { mockSignals } from "@/lib/mock";
import { fmtReturn, returnColor, fmtPct, EVENT_TYPE_LABELS } from "@/lib/utils/format";
import { DirectionBadge, HopBadge, EventTypeBadge, SplitBadge, CorrectnessIndicator } from "@/components/badges";
import { ConfidenceCell } from "@/components/tables";
import type { SignalRecord } from "@/types";

const col = createColumnHelper<SignalRecord>();

const columns = [
  col.accessor("idx", {
    header: "#",
    cell: (info) => (
      <Link
        href={`/signals/${info.getValue()}`}
        className="text-blue-600 hover:underline font-mono text-xs"
      >
        {info.getValue()}
      </Link>
    ),
    size: 40,
  }),
  col.accessor("split", {
    header: "数据集",
    cell: (info) => <SplitBadge split={info.getValue()} />,
    size: 60,
  }),
  col.accessor("event_type", {
    header: "事件类型",
    cell: (info) => <EventTypeBadge type={info.getValue()} />,
    size: 100,
  }),
  col.accessor("source_event", {
    header: "源事件",
    cell: (info) => (
      <span className="text-xs text-gray-700 truncate max-w-[140px] block" title={info.getValue()}>
        {info.getValue()}
      </span>
    ),
    size: 150,
  }),
  col.accessor("target_name", {
    header: "目标股",
    cell: (info) => (
      <div className="flex items-center gap-1">
        <span className="text-xs font-medium text-gray-900">{info.getValue()}</span>
        <span className="text-[10px] text-gray-400 font-mono">{info.row.original.target_code}</span>
      </div>
    ),
    size: 130,
  }),
  col.accessor("hop", {
    header: "跳数",
    cell: (info) => <HopBadge hop={info.getValue()} />,
    size: 50,
  }),
  col.accessor("relation_chain", {
    header: "关系链",
    cell: (info) => (
      <span className="text-[11px] text-gray-500 font-mono truncate max-w-[120px] block">
        {info.getValue()}
      </span>
    ),
    size: 120,
  }),
  col.accessor("signal_direction", {
    header: "方向",
    cell: (info) => <DirectionBadge direction={info.getValue()} />,
    size: 70,
  }),
  col.accessor("confidence", {
    header: "置信度",
    cell: (info) => <ConfidenceCell value={info.getValue()} />,
    size: 80,
  }),
  col.accessor("adj_return_5d", {
    header: "调整收益5D",
    cell: (info) => (
      <span className={`text-xs font-mono ${returnColor(info.getValue())}`}>
        {fmtReturn(info.getValue())}
      </span>
    ),
    sortDescFirst: true,
    size: 80,
  }),
  col.accessor("excess_5d", {
    header: "超额5D",
    cell: (info) => (
      <span className={`text-xs font-mono ${returnColor(info.getValue())}`}>
        {fmtReturn(info.getValue())}
      </span>
    ),
    size: 80,
  }),
  col.accessor("reacted", {
    header: "已反应",
    cell: (info) => (
      <span className={`text-[11px] ${info.getValue() ? "text-amber-600" : "text-gray-400"}`}>
        {info.getValue() ? "是" : "否"}
      </span>
    ),
    size: 50,
  }),
  col.accessor("correct", {
    header: "正确",
    cell: (info) => <CorrectnessIndicator correct={info.getValue()} />,
    size: 50,
  }),
];

// ─── Filter Bar ──────────────────────────────────────
function SignalsLocalFilterBar() {
  const setParams = useSetQueryParams();
  const eventType = useQueryParam("eventType");
  const direction = useQueryParam("direction");
  const hop = useQueryParam("hop");

  const eventTypes = Array.from(new Set(mockSignals.map((s) => s.event_type)));

  return (
    <div className="flex items-center gap-3 flex-wrap">
      <select
        value={eventType}
        onChange={(e) => setParams({ eventType: e.target.value || null })}
        className="text-xs border border-gray-200 rounded px-2 py-1.5 bg-white"
      >
        <option value="">全部事件类型</option>
        {eventTypes.map((t) => (
          <option key={t} value={t}>{EVENT_TYPE_LABELS[t] || t}</option>
        ))}
      </select>

      <select
        value={direction}
        onChange={(e) => setParams({ direction: e.target.value || null })}
        className="text-xs border border-gray-200 rounded px-2 py-1.5 bg-white"
      >
        <option value="">全部方向</option>
        <option value="avoid">Avoid</option>
        <option value="long">Long</option>
      </select>

      <select
        value={hop}
        onChange={(e) => setParams({ hop: e.target.value || null })}
        className="text-xs border border-gray-200 rounded px-2 py-1.5 bg-white"
      >
        <option value="">全部 Hop</option>
        <option value="0">Hop 0</option>
        <option value="1">Hop 1</option>
        <option value="2">Hop 2</option>
        <option value="3">Hop 3</option>
      </select>
    </div>
  );
}

// ─── Summary Bar ─────────────────────────────────────
function SignalsSummaryBar({ data }: { data: SignalRecord[] }) {
  const winCount = data.filter((s) => s.correct).length;
  const winRate = data.length > 0 ? winCount / data.length : 0;
  const avgReturn = data.length > 0 ? data.reduce((sum, s) => sum + (s.adj_return_5d ?? 0), 0) / data.length : 0;

  return (
    <div className="flex items-center gap-6 text-xs text-gray-500 py-2 px-1 border-t border-gray-100">
      <span>共 <strong className="text-gray-900">{data.length}</strong> 条信号</span>
      <span>胜率 <strong className={winRate >= 0.7 ? "text-emerald-600" : winRate < 0.5 ? "text-red-600" : "text-gray-900"}>{fmtPct(winRate)}</strong></span>
      <span>平均调整收益 <strong className={returnColor(avgReturn)}>{fmtReturn(avgReturn)}</strong></span>
    </div>
  );
}

// ─── Main Page ───────────────────────────────────────
export default function SignalsPage() {
  const router = useRouter();
  const { split } = useGlobalFilter();
  const eventType = useQueryParam("eventType");
  const direction = useQueryParam("direction");
  const hop = useQueryParam("hop");
  const eventId = useQueryParam("event_id");

  const [sorting, setSorting] = useState<SortingState>([]);

  const filteredData = useMemo(() => {
    let data = mockSignals;
    if (split !== "all") data = data.filter((s) => s.split === split);
    if (eventType) data = data.filter((s) => s.event_type === eventType);
    if (direction) data = data.filter((s) => s.signal_direction === direction);
    if (hop) data = data.filter((s) => s.hop === Number(hop));
    if (eventId) data = data.filter((s) => s.event_id === eventId);
    return data;
  }, [split, eventType, direction, hop, eventId]);

  const table = useReactTable({
    data: filteredData,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getFilteredRowModel: getFilteredRowModel(),
  });

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-lg font-bold text-gray-900">信号列表</h1>
        <SignalsLocalFilterBar />
      </div>

      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-left">
            <thead>
              {table.getHeaderGroups().map((hg) => (
                <tr key={hg.id} className="border-b border-gray-100 bg-gray-50">
                  {hg.headers.map((header) => (
                    <th
                      key={header.id}
                      onClick={header.column.getToggleSortingHandler()}
                      className="px-3 py-2 text-[11px] font-semibold text-gray-500 uppercase tracking-wider cursor-pointer hover:bg-gray-100 select-none whitespace-nowrap"
                      style={{ width: header.getSize() }}
                    >
                      <div className="flex items-center gap-1">
                        {flexRender(header.column.columnDef.header, header.getContext())}
                        {{ asc: " ↑", desc: " ↓" }[header.column.getIsSorted() as string] ?? ""}
                      </div>
                    </th>
                  ))}
                </tr>
              ))}
            </thead>
            <tbody>
              {table.getRowModel().rows.map((row) => (
                <tr
                  key={row.id}
                  className="border-b border-gray-50 hover:bg-blue-50/30 transition-colors cursor-pointer"
                  onClick={() => router.push(`/signals/${row.original.idx}`)}
                >
                  {row.getVisibleCells().map((cell) => (
                    <td key={cell.id} className="px-3 py-2 whitespace-nowrap">
                      {flexRender(cell.column.columnDef.cell, cell.getContext())}
                    </td>
                  ))}
                </tr>
              ))}
              {table.getRowModel().rows.length === 0 && (
                <tr>
                  <td colSpan={columns.length} className="px-3 py-8 text-center text-sm text-gray-400">
                    暂无匹配信号
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        <SignalsSummaryBar data={filteredData} />
      </div>
    </div>
  );
}
