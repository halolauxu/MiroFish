const HOP_COLORS: Record<number, string> = {
  0: "bg-gray-200 text-gray-600",
  1: "bg-blue-100 text-blue-700",
  2: "bg-blue-200 text-blue-800",
  3: "bg-blue-300 text-blue-900",
};

export function HopBadge({ hop }: { hop: number }) {
  const color = HOP_COLORS[hop] ?? "bg-gray-100 text-gray-600";
  return (
    <span className={`inline-flex items-center justify-center w-6 h-5 rounded text-[10px] font-bold ${color}`}>
      {hop}
    </span>
  );
}
