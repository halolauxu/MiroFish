import { fmtReturn, returnColor } from "@/lib/utils/format";

export function ReturnValueCell({ value }: { value: number | null | undefined }) {
  return <span className={`font-mono text-[12px] ${returnColor(value)}`}>{fmtReturn(value)}</span>;
}
