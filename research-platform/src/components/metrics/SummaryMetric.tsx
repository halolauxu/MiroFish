export function SummaryMetric({ label, value, className = "" }: { label: string; value: string; className?: string }) {
  return (
    <div className={`flex items-center gap-1.5 ${className}`}>
      <span className="text-[11px] text-gray-500">{label}:</span>
      <span className="text-[12px] font-semibold text-gray-800">{value}</span>
    </div>
  );
}
