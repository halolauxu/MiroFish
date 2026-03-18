import type { SignalDirection } from "@/types";

const STYLES: Record<SignalDirection, string> = {
  avoid: "bg-red-100 text-red-700 border-red-200",
  long: "bg-emerald-100 text-emerald-700 border-emerald-200",
};

export function DirectionBadge({ direction, size = "sm" }: { direction: SignalDirection; size?: "sm" | "lg" }) {
  const base = size === "lg" ? "px-3 py-1 text-sm font-semibold" : "px-1.5 py-0.5 text-[11px] font-medium";
  return (
    <span className={`inline-flex items-center rounded border ${base} ${STYLES[direction]}`}>
      {direction.toUpperCase()}
    </span>
  );
}
