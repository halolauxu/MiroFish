import { fmtSharpe, sharpeColor } from "@/lib/utils/format";

export function SharpeValueCell({ value }: { value: number | null | undefined }) {
  return <span className={`font-mono text-[12px] ${sharpeColor(value)}`}>{fmtSharpe(value)}</span>;
}
