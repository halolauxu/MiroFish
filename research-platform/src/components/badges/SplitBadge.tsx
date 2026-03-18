export function SplitBadge({ split }: { split: "is" | "oos" }) {
  const styles = split === "oos"
    ? "bg-orange-100 text-orange-700 border-orange-200"
    : "bg-blue-100 text-blue-700 border-blue-200";
  return (
    <span className={`inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-semibold border ${styles}`}>
      {split.toUpperCase()}
    </span>
  );
}
