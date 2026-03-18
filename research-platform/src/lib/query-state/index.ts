"use client";
import { useSearchParams, useRouter, usePathname } from "next/navigation";
import { useCallback } from "react";
import type { SplitType, OutlierMode } from "@/types";

/** Read a single query param */
export function useQueryParam(key: string, defaultValue: string = ""): string {
  const params = useSearchParams();
  return params.get(key) ?? defaultValue;
}

/** Set / update query params without full navigation */
export function useSetQueryParams() {
  const router = useRouter();
  const pathname = usePathname();
  const params = useSearchParams();

  return useCallback(
    (updates: Record<string, string | null>) => {
      const next = new URLSearchParams(params.toString());
      for (const [k, v] of Object.entries(updates)) {
        if (v === null || v === "") next.delete(k);
        else next.set(k, v);
      }
      const qs = next.toString();
      router.replace(qs ? `${pathname}?${qs}` : pathname, { scroll: false });
    },
    [router, pathname, params]
  );
}

/** Global filter hook */
export function useGlobalFilter() {
  const split = useQueryParam("split", "all") as SplitType;
  const outlier = useQueryParam("outlier", "include") as OutlierMode;
  const setParams = useSetQueryParams();

  const setSplit = (v: SplitType) => setParams({ split: v === "all" ? null : v });
  const setOutlier = (v: OutlierMode) => setParams({ outlier: v === "include" ? null : v });

  return { split, outlier, setSplit, setOutlier };
}

/** Navigate to another page with query params */
export function buildHref(path: string, params?: Record<string, string>): string {
  if (!params) return path;
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v) qs.set(k, v);
  }
  const str = qs.toString();
  return str ? `${path}?${str}` : path;
}
