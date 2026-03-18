export function ConfidenceCell({ value }: { value: number }) {
  const width = Math.min(100, Math.round(value * 200)); // scale 0-0.5 to 0-100%
  return (
    <div className="flex items-center gap-1.5">
      <span className="text-[11px] font-mono text-gray-700 w-8">{value.toFixed(2)}</span>
      <div className="flex-1 h-1.5 bg-gray-100 rounded-full overflow-hidden max-w-[40px]">
        <div className="h-full bg-blue-400 rounded-full" style={{ width: `${width}%` }} />
      </div>
    </div>
  );
}
