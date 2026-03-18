"use client";
import { useGlobalFilter } from "@/lib/query-state";
import type { SplitType } from "@/types";

const SPLIT_OPTIONS: { value: SplitType; label: string }[] = [
  { value: "all", label: "全量" },
  { value: "is", label: "IS" },
  { value: "oos", label: "OOS" },
];

export function GlobalFilterBar() {
  const { split, outlier, setSplit, setOutlier } = useGlobalFilter();

  return (
    <div className="flex items-center gap-4">
      {/* IS / OOS / 全量 */}
      <div className="flex items-center gap-1 bg-gray-100 rounded-md p-0.5">
        {SPLIT_OPTIONS.map((opt) => (
          <button
            key={opt.value}
            onClick={() => setSplit(opt.value)}
            className={`px-2.5 py-1 text-xs font-medium rounded transition-colors ${
              split === opt.value
                ? opt.value === "oos"
                  ? "bg-orange-500 text-white"
                  : opt.value === "is"
                  ? "bg-blue-500 text-white"
                  : "bg-white text-gray-900 shadow-sm"
                : "text-gray-500 hover:text-gray-700"
            }`}
          >
            {opt.label}
          </button>
        ))}
      </div>

      {/* Outlier toggle */}
      <button
        onClick={() => setOutlier(outlier === "include" ? "exclude" : "include")}
        className={`flex items-center gap-1.5 px-2.5 py-1 text-xs rounded-md border transition-colors ${
          outlier === "exclude"
            ? "border-amber-300 bg-amber-50 text-amber-700"
            : "border-gray-200 bg-white text-gray-500 hover:border-gray-300"
        }`}
      >
        <span className={`w-2 h-2 rounded-full ${outlier === "exclude" ? "bg-amber-400" : "bg-gray-300"}`} />
        {outlier === "exclude" ? "已去极值" : "含极端值"}
      </button>
    </div>
  );
}
