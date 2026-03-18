import { TableSkeleton } from "@/components/feedback/Skeleton";

export default function SignalsLoading() {
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div className="h-7 w-24 bg-gray-200 rounded animate-pulse" />
        <div className="flex gap-3">
          <div className="h-8 w-28 bg-gray-200 rounded animate-pulse" />
          <div className="h-8 w-24 bg-gray-200 rounded animate-pulse" />
          <div className="h-8 w-24 bg-gray-200 rounded animate-pulse" />
        </div>
      </div>
      <TableSkeleton rows={12} cols={8} />
    </div>
  );
}
