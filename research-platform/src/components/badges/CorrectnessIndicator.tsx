import { Check, X } from "lucide-react";

export function CorrectnessIndicator({ correct, size = "sm" }: { correct: boolean; size?: "sm" | "lg" }) {
  const iconSize = size === "lg" ? 20 : 14;
  if (correct) {
    return (
      <span className="inline-flex items-center justify-center w-5 h-5 rounded-full bg-emerald-100">
        <Check size={iconSize} className="text-emerald-600" strokeWidth={3} />
      </span>
    );
  }
  return (
    <span className="inline-flex items-center justify-center w-5 h-5 rounded-full bg-red-100">
      <X size={iconSize} className="text-red-600" strokeWidth={3} />
    </span>
  );
}
