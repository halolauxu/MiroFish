/**
 * API 对接层
 *
 * 目前使用 mock 数据，后续切换真实后端时只需修改此文件。
 * 页面组件统一通过此层获取数据，不直接引用 mock。
 *
 * 后端接口约定 (待实现):
 *   GET /api/events            → EventRecord[]
 *   GET /api/events/:id        → EventRecord
 *   GET /api/signals           → SignalRecord[]
 *   GET /api/signals/:idx      → SignalDetailPayload
 *   GET /api/analysis/:dim     → PerformanceSlice[]
 *   GET /api/graph-health      → { distribution, issues }
 *   GET /api/metrics/:split    → SplitMetrics
 */

import {
  mockEvents,
  mockSignals,
  mockISMetrics,
  mockOOSMetrics,
  mockAllMetrics,
  mockHoldingPeriodMetrics,
  mockOutlierImpact,
  mockAnalysisSlices,
  mockRelationDistribution,
  mockGraphAuditIssues,
} from "@/lib/mock";
import { getSignalDetail } from "@/lib/mock";
import type {
  EventRecord,
  SignalRecord,
  SignalDetailPayload,
  SplitMetrics,
  HoldingPeriodMetrics,
  OutlierImpactSnapshot,
  PerformanceSlice,
  AnalysisDimension,
  RelationTypeDistribution,
  GraphAuditIssue,
} from "@/types";

// ─── 环境变量 ───────────────────────────────────────
const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "";
const USE_MOCK = !API_BASE; // 无后端地址时使用 mock

// ─── 通用 fetch 包装 ────────────────────────────────
async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
  });
  if (!res.ok) throw new Error(`API error: ${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

// ─── Events ─────────────────────────────────────────
export async function fetchEvents(): Promise<EventRecord[]> {
  if (USE_MOCK) return mockEvents;
  return apiFetch<EventRecord[]>("/api/events");
}

export async function fetchEvent(eventId: string): Promise<EventRecord | undefined> {
  if (USE_MOCK) return mockEvents.find((e) => e.event_id === eventId);
  return apiFetch<EventRecord>(`/api/events/${eventId}`);
}

// ─── Signals ────────────────────────────────────────
export async function fetchSignals(): Promise<SignalRecord[]> {
  if (USE_MOCK) return mockSignals;
  return apiFetch<SignalRecord[]>("/api/signals");
}

export async function fetchSignalDetail(idx: number): Promise<SignalDetailPayload> {
  if (USE_MOCK) return getSignalDetail(idx);
  return apiFetch<SignalDetailPayload>(`/api/signals/${idx}`);
}

// ─── Metrics ────────────────────────────────────────
export function fetchMetrics(split: string): SplitMetrics {
  // 同步版本（mock 数据无需 async）
  if (split === "is") return mockISMetrics;
  if (split === "oos") return mockOOSMetrics;
  return mockAllMetrics;
}

export function fetchHoldingPeriodMetrics(): HoldingPeriodMetrics[] {
  return mockHoldingPeriodMetrics;
}

export function fetchOutlierImpact(): OutlierImpactSnapshot {
  return mockOutlierImpact;
}

// ─── Analysis ───────────────────────────────────────
export function fetchAnalysisSlices(dim: AnalysisDimension): PerformanceSlice[] {
  if (USE_MOCK) return mockAnalysisSlices[dim];
  // TODO: apiFetch(`/api/analysis/${dim}`)
  return mockAnalysisSlices[dim];
}

export function fetchAllAnalysisSlices(): Record<AnalysisDimension, PerformanceSlice[]> {
  return mockAnalysisSlices;
}

// ─── Graph Health ───────────────────────────────────
export function fetchRelationDistribution(): RelationTypeDistribution[] {
  return mockRelationDistribution;
}

export function fetchGraphAuditIssues(): GraphAuditIssue[] {
  return mockGraphAuditIssues;
}
