import { Inbox } from "lucide-react";

export function EmptyState({ message = "暂无数据" }: { message?: string }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-gray-400">
      <Inbox size={36} strokeWidth={1.5} />
      <p className="mt-2 text-sm">{message}</p>
    </div>
  );
}
