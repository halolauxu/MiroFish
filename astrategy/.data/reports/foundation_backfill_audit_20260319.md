# Foundation Two-Year Backfill Audit

**Anchor Date**: 2026-03-19
**Required Start**: 2024-03-20
**Required Span**: 730 days
**Universe**: csi800

## Component Summary

| 组件 | Files | Populated | 首日 | 末日 | SpanDays | TwoYearReady | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| prices | 800 | 800 | 2024-03-20 | 2026-03-19 | 729 | NO | 789/800 个本地价格文件覆盖 2024-03-20 -> 2026-03-19 |
| news | 838 | 800 | 2025-10-30 | 2026-03-19 | 140 | NO | 公司新闻按 ticker 存档，当前是增量最近新闻而不是完整历史新闻库。 |
| filings | 800 | 661 | 2026-02-24 | 2026-03-19 | 23 | NO | 公告层当前默认是 rolling window，不是两年全量归档。 |
| sentiment | 800 | 800 | 2025-10-30 | 2026-03-19 | 140 | NO | 情绪层是 ingest 当天快照聚合，不是连续两年日历快照。 |
| events | 1 | 1 | 2025-10-30 | 2026-03-19 | 140 | NO | 事件主表来自当前增量事件汇总，历史跨度受上游公告/新闻窗口约束。 |
| graph | 1 | 1 | 2026-03-19 | 2026-03-19 | 0 | NO | 图谱层当前是单日状态快照，不是两年连续历史图谱。 |