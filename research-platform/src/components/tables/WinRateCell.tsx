import { fmtPct } from "@/lib/utils/format";

export function WinRateCell({ value }: { value: number | null | undefined }) {
  if (value == null) return <span className="text-gray-400 text-[12px]">-</span>;
  const color = value < 0.5 ? "text-red-600 font-semibold" : value >= 0.8 ? "text-emerald-600 font-semibold" : "text-gray-900";
  return <span className={`font-mono text-[12px] ${color}`}>{fmtPct(value)}</span>;
}
