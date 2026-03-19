import { EVENT_TYPE_LABELS, EVENT_TYPE_COLORS } from "@/lib/utils/format";
export function EventTypeBadge({ type }: { type: string }) {
  const label = EVENT_TYPE_LABELS[type] ?? type;
  const color = EVENT_TYPE_COLORS[type] ?? "bg-gray-100 text-gray-600";
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-[11px] font-medium ${color}`}>
      {label}
    </span>
  );
}
