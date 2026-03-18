interface MetricCardProps {
  label: string;
  value: string;
  subtitle?: string;
  highlight?: boolean;
  valueClass?: string;
  className?: string;
}

export function MetricCard({ label, value, subtitle, highlight, valueClass, className = "" }: MetricCardProps) {
  const colorClass = valueClass || (highlight ? "text-emerald-600" : "text-gray-900");
  return (
    <div className={`bg-white rounded-lg border border-gray-200 p-4 ${className}`}>
      <div className="text-[11px] text-gray-500 font-medium uppercase tracking-wide">{label}</div>
      <div className={`text-2xl font-bold mt-1 ${colorClass}`}>
        {value}
      </div>
      {subtitle && <div className="text-[11px] text-gray-400 mt-0.5">{subtitle}</div>}
    </div>
  );
}
