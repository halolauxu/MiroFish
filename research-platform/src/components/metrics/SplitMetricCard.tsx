interface SplitMetricCardProps {
  label: string;
  isValue: string;
  oosValue: string;
  isLabel?: string;
  oosLabel?: string;
  highlightOOS?: boolean;
}

export function SplitMetricCard({
  label,
  isValue,
  oosValue,
  isLabel = "IS",
  oosLabel = "OOS",
  highlightOOS = false,
}: SplitMetricCardProps) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <div className="text-[11px] text-gray-500 font-medium uppercase tracking-wide mb-2">{label}</div>
      <div className="flex items-end gap-4">
        <div>
          <div className="text-[10px] text-blue-500 font-medium">{isLabel}</div>
          <div className="text-xl font-bold text-gray-700">{isValue}</div>
        </div>
        <div className="text-gray-300 text-lg">/</div>
        <div>
          <div className="text-[10px] text-orange-500 font-medium">{oosLabel}</div>
          <div className={`text-xl font-bold ${highlightOOS ? "text-emerald-600" : "text-gray-900"}`}>
            {oosValue}
          </div>
        </div>
      </div>
    </div>
  );
}
