"use client";
import { AlertTriangle } from "lucide-react";

export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  return (
    <div className="flex flex-col items-center justify-center min-h-[50vh] gap-4">
      <AlertTriangle size={48} className="text-red-400" />
      <h2 className="text-lg font-bold text-gray-900">页面发生错误</h2>
      <p className="text-sm text-gray-500 max-w-md text-center">
        {error.message || "未知错误"}
      </p>
      <button
        onClick={reset}
        className="px-4 py-2 text-sm bg-blue-500 text-white rounded-md hover:bg-blue-600 transition-colors"
      >
        重试
      </button>
    </div>
  );
}
