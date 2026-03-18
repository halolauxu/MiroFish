const REL_COLORS: Record<string, string> = {
  COMPETES_WITH: "text-red-600",
  SUPPLIES_TO: "text-blue-600",
  CUSTOMER_OF: "text-cyan-600",
  COOPERATES_WITH: "text-emerald-600",
  HOLDS_SHARES: "text-purple-600",
  PARENT_OF: "text-orange-600",
  CHILD_OF: "text-amber-600",
  SOURCE: "text-gray-500",
};

export function RelationChainCell({ chain }: { chain: string }) {
  if (chain === "SOURCE") {
    return <span className="text-[11px] text-gray-400 italic">源头</span>;
  }

  const parts = chain.split(" → ");
  return (
    <span className="text-[10px] font-mono">
      {parts.map((rel, i) => (
        <span key={i}>
          {i > 0 && <span className="text-gray-300 mx-0.5">→</span>}
          <span className={REL_COLORS[rel] ?? "text-gray-600"}>{rel.replace(/_/g, " ")}</span>
        </span>
      ))}
    </span>
  );
}
