# A股生产 Charter 搜索报告

**Run Date**: 2026-03-23 09:21 CST
**Search Scope**: regime overlay / concentrated overlay / event mainbook / order_win targeted mainbook
**Result**: 本轮无候选满足 production charter；最佳可保留候选仍为 `事件延续 / risk overlay`，下一搜索前沿为 `policy_risk reacted source buckets` 与 `ma source-specific buckets`。

## 先回填后搜索

本轮在任何搜索/比较前先执行了以下回填与审计：

1. `python3 -m astrategy.archive.authoritative_history --backfill-existing`
2. `python3 -m astrategy.archive.foundation_backfill_audit --anchor-date 2026-03-20 --universe csi800`
3. `PYTHONPATH=. python3 -m astrategy.trading_desk_framework --as-of-date 2026-03-20`

最新产物：

- `astrategy/.data/reports/authoritative_archive_audit_20260323.md`
- `astrategy/.data/reports/foundation_backfill_audit_20260320.md`
- `astrategy/.data/reports/trading_desk_framework_20260320.json`

## Exact Data Sources

本轮只使用仓库内已落盘、可追溯、时间对齐的数据：

- 价格层: `astrategy/.data/datahub/market/price_manifest.json` 以及本地 prices 档案；foundation audit 显示覆盖 `2024-03-20` 到 `2026-03-19`，跨度 `729` 天。
- 公告层: CNInfo authoritative replay，源定义为 `cninfo.disclosure_report`。
- 新闻层: 本地增量新闻档案，来源代码指向 AkShare `stock_news_em` / Baidu 搜索 / 雪球热榜聚合，但当前不是两年全量历史库。
- 情绪层: 由 filings + company news + hot-topic 增量聚合而成的本地 derived sentiment snapshot，不是两年连续日历快照。
- 事件主表: `astrategy/.data/event_master/historical_event_master.json` 与 shock 研究产物。
- 图谱层: `astrategy/.data/datahub/graph/graph_manifest.json` 与单日图谱快照。
- 严格事件线验证文件:
  - `astrategy/.data/reports/shock_wf_signals_20260320_local_refresh.json`
  - `astrategy/.data/reports/shock_wf_backtest_20260320_local_refresh.md`
  - `astrategy/.data/reports/shock_bucket_study_20260320_local_refresh.json`

## 时间窗口与成本

- Foundation 审计锚点: `2026-03-20`
- Two-year required window: `2024-03-21` 到 `2026-03-20`
- 当前事件线信号窗口: `2026-02-04` 到 `2026-03-15`
- 当前 5D 有效样本窗口: `2026-02-04` 到 `2026-03-11`
- 回测持有期: `5` 个交易日
- 成本假设: `40 bps` round-trip
  - `30 bps` 交易成本
  - `10 bps` 滑点
- 执行假设: `T+1` 开盘入场，`T+5` 收盘离场

## Frontier Scan

| Frontier | Best observable candidate | Evidence | Charter status |
| --- | --- | --- | --- |
| regime overlay | `Regime 轮动` | authoritative archive 仅 `2` 个 signal dates / `2` 个 factor dates | `FAIL`，无 strict WF |
| concentrated overlay | `事件延续 / risk overlay` | strict OOS Sharpe `2.50`，但历史仅 `136` 天 | `FAIL` |
| event mainbook | `policy_risk reacted source bucket` | 分桶 Sharpe `4.69`，avg adj return `+5.22%/5D` | `FAIL`，非 strict OOS 且覆盖不足 |
| order_win targeted mainbook | `order_win` | local-refresh 全样本仅 `29` signals / `22` tickers / `13` active days；有效 5D 样本 `16` 条、Sharpe `-1.52` | `FAIL` |

## Best Candidate

**保留候选**: `事件延续 / risk overlay`

选择理由：

- 它仍是仓库内唯一拿到 strict OOS 指标的子线。
- `trading_desk_framework_20260320.json` 仍把它标为当前最严格可比验证线。
- 其余 frontiers 本轮仍停留在 authoritative history 不足、无法做 strict WF 的阶段。

可核对指标：

| Metric | Value |
| --- | --- |
| Validation mode | `strict_wf` |
| History span | `136` days |
| OOS eval count | `28` |
| OOS Sharpe | `2.50` |
| OOS hit rate | `85.71%` |
| OOS avg adjusted return | `+1.88% / 5D` |
| OOS simple annualized proxy | `+94.75%` |
| OOS compound annualized proxy | `+155.67%` |
| OOS MaxDD | `16.01%` |
| Total signals | `321` |
| All rows unique tickers | `151` |
| All rows active days | `23` |
| Valid 5D unique tickers | `113` |
| Valid 5D active days | `19` |

## WF Window Metrics

production charter 要求:

- 至少 `4/5` 个 WF 窗口为正
- 最新 `3` 个 WF 窗口为正

本轮可用的 strict WF 事件线窗口只有 `1` 个，不足以宣称通过 `5-window` charter。

| Strict WF window | Sharpe | Avg adj return | MaxDD | Positive |
| --- | --- | --- | --- | --- |
| available OOS window | `2.50` | `+1.88% / 5D` | `16.01%` | `YES` |

结论：

- `4/5 positive`: `FAIL`，当前仅有 `1/5` 可用窗口
- `latest 3 positive`: `FAIL`，当前无 `3` 个 strict WF 窗口可核验

## Why Not Promote The Stronger Bucket

`policy_risk reacted source bucket` 是本轮最强的搜索前沿，但还不能替代上面的最佳候选，因为它来自全样本分桶研究，不是 strict OOS WF。

当前硬指标：

| Slice | n | Sharpe | Hit rate | Avg adj return | Unique tickers | Active days |
| --- | --- | --- | --- | --- | --- | --- |
| policy_risk 全量 | `96` | `3.93` | `69.79%` | `+4.19% / 5D` | `58` | `15` |
| policy_risk hop=0 | `89` | `4.13` | `70.79%` | `+4.47% / 5D` | `58` | `15` |
| policy_risk reacted | `80` | `4.69` | `80.00%` | `+5.22% / 5D` | `52` | `15` |
| ma 全量 | `26` | `1.32` | `80.77%` | `+0.62% / 5D` | `2` | `4` |
| order_win 全量行 | `29` | N/A | N/A | N/A | `22` | `13` |
| order_win 有效5D样本 | `16` | `-1.52` | `50.00%` | `-1.51% / 5D` | `14` | `9` |

## Charter Alignment

| Charter Target | Requirement | Best candidate | Status |
| --- | --- | --- | --- |
| Net OOS CAGR | `> 80%` | proxy `+94.75%` | `PASS (proxy only)` |
| Net avg daily return | `> 1%` | `+1.88% / 5D` | `FAIL` |
| Net Sharpe | `> 2` | `2.50` | `PASS` |
| MaxDD | `< 15%` | `16.01%` | `FAIL` |
| 4 of 5 WF positive | `>= 4/5` | only `1` strict window | `FAIL` |
| Latest 3 WF positive | `3/3` | insufficient windows | `FAIL` |
| Unique tickers | `>= 80` | `151` all rows / `113` valid 5D | `PASS` |
| Active days | `>= 120` | `23` all rows / `19` valid 5D | `FAIL` |

最终判断：**仍不满足 production charter，不可升格为生产主策略。**

## Search Decision By Track

- `regime overlay`: 暂停性能宣称，先补 `sector_rotation` 的 authoritative 历史链。
- `concentrated overlay`: 保留 `事件延续 / risk overlay` 作为当前最强 strict OOS 候选，但仅可作为防守/过滤 overlay，不可当主策略。
- `event mainbook`: 下一轮优先把 `policy_risk reacted source bucket` 做成 strict OOS slice，验证它是否能在不放大回撤的前提下保留高 Sharpe。
- `order_win targeted mainbook`: 降级处理，不再作为 primary frontier。

## Next Search Frontier

按优先级：

1. `policy_risk reacted source buckets`
2. `ma source-specific buckets`
3. `events/news/sentiment` 的历史归档扩展到足以支撑 strict multi-window WF
4. 只有在 `order_win` 样本显著扩容后，才重新评估 targeted mainbook

下一轮的明确 gating：

- 先补齐至少 `120` active days 的历史事件/新闻/情绪主表
- 再对 `policy_risk reacted source bucket` 做 strict multi-window WF
- 若不能形成 `5` 个可核验窗口，则继续视为研究线，不做生产声明
