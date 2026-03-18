import { CardSkeleton, TableSkeleton } from "@/components/feedback/Skeleton";

export default function Loading() {
  return (
    <div className="space-y-4 max-w-5xl animate-in fade-in duration-300">
      <div className="h-7 w-32 bg-gray-200 rounded animate-pulse" />
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        <CardSkeleton />
        <CardSkeleton />
        <CardSkeleton />
        <CardSkeleton />
      </div>
      <TableSkeleton rows={8} cols={5} />
    </div>
  );
}
