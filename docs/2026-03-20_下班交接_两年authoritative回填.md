# 2026-03-20 下班交接：两年 Authoritative 回填进度

## 当前仓库状态

- 项目路径：`/Users/xukun/Documents/freqtrade/MiroFish`
- 当前分支：`main`
- 本次交接重点：把“最近两年 authoritative 历史回填”从口头要求，落成了可执行的价格补数链路 + 严格审计链路

## 今天完成的核心工作

### 1. 修掉价格层的伪缓存问题

之前价格层有两个关键问题：

- 只要本地 `daily/<ticker>.json` 存在，就直接当作可用缓存
- `price_manifest.json` 不会被真实文件跨度自动刷新

今天已经修正：

- [market_data.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/data_collector/market_data.py)
  - 新增“本地覆盖窗是否满足请求区间”的严格检查
  - 本地只覆盖 1 年时，不再假装满足 2 年，而是继续远端补数
  - 远端成功后会和本地数据合并去重
- [prices.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/datahub/market/prices.py)
  - `build_price_layer` 不再把“行数>0”的旧 manifest 当成两年缓存
  - manifest 会按真实文件的 `row_count / price_start / price_end` 自动刷新
- [fetch_price_cache.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/scripts/fetch_price_cache.py)
  - 支持 `--universe`
  - 支持 `--skip-covered`
  - 输出 `ok / partial`
  - 可以精确按时间窗回填

### 2. 实际跑了两轮全量价格回填

已执行：

```bash
python3 astrategy/scripts/fetch_price_cache.py \
  --universe csi800 \
  --lookback-days 730 \
  --end-date 2026-03-20 \
  --skip-covered
```

以及严格对齐交易窗的第二轮：

```bash
python3 astrategy/scripts/fetch_price_cache.py \
  --universe csi800 \
  --start-date 2024-03-20 \
  --end-date 2026-03-19 \
  --skip-covered
```

另外单独补了：

```bash
python3 astrategy/scripts/fetch_price_cache.py \
  000300 000800 \
  --start-date 2024-03-20 \
  --end-date 2026-03-19
```

### 3. 新增 authoritative foundation 审计器

新增：

- [foundation_backfill_audit.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/archive/foundation_backfill_audit.py)

作用：

- 直接扫描本地 authoritative 产物
- 输出每个组件的真实历史跨度
- 明确区分：
  - 价格层是否已满两年
  - 新闻 / 公告 / 情绪 / 事件 / 图谱 是否也已满足两年

产物：

- [foundation_backfill_audit_20260319.md](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/.data/reports/foundation_backfill_audit_20260319.md)
- [foundation_backfill_audit.json](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/.data/authoritative_archive/audit/foundation_backfill_audit.json)

## 截至目前的真实数据结论

### 价格层：已大幅推进，但还不是 800/800

按 `CSI800` 宇宙、严格窗口 `2024-03-20 -> 2026-03-19`：

- `CSI800` 总数：`800`
- 满足两年价格窗：`789`
- 仍未满足：`11`

剩余 11 个：

- `001221`
- `001389`
- `001391`
- `301536`
- `301611`
- `600584`
- `600930`
- `600988`
- `603049`
- `688615`
- `688692`

其中大部分明显像新股/次新股窗口不足：

- `001221`: `2025-07-30 -> 2026-03-19`
- `001391`: `2024-12-30 -> 2026-03-19`
- `600930`: `2025-07-16 -> 2026-03-19`
- `603049`: `2025-06-05 -> 2026-03-19`
- `688615`: `2024-09-26 -> 2026-03-19`
- `688692`: `2024-06-12 -> 2026-03-19`

两个需要特别留意：

- `600584`: `2024-03-27 -> 2026-03-19`
- `600988`: `2024-03-20 -> 2026-03-18`

这两个更像交易日缺口/停牌，而不是脚本根本没补。

另外：

- `000300` 已补齐：`483` 行，`2024-03-20 -> 2026-03-19`
- `000800` 已补齐：`483` 行，`2024-03-20 -> 2026-03-19`

### 其他 authoritative 源：还远没到两年

这是今天最重要的“不能自欺”的结论。

按 [foundation_backfill_audit_20260319.md](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/.data/reports/foundation_backfill_audit_20260319.md)：

- `news`: `2025-10-30 -> 2026-03-19`
- `sentiment`: `2025-10-30 -> 2026-03-19`
- `events`: `2025-10-30 -> 2026-03-19`
- `filings`: `2026-02-24 -> 2026-03-19`
- `graph`: 只有 `2026-03-19` 单日快照

所以：

- 价格 authoritative 两年回填已经基本打通
- 但**整套回测相关数据**还没有满足“两年 authoritative”标准
- 现在绝对不能把“价格层补齐”误写成“整个研究系统已经两年 strict-ready”

## 当前研究方向判断

### 1. 研究方向没有变，但严格性要求提高了

目标仍然是：

- 搭建 `regime -> 主线 -> 催化剂 -> 资金/预期差 -> 图谱/舆情过滤 -> 执行映射`
  的交易员总控框架

但现在必须遵守一条硬规则：

- **没有两年 authoritative 历史，就不出最终 WF Sharpe / 年化结论**

### 2. 当前最真实的状态

- `shock / event continuation` 这条线依然有严格样本，但历史跨度远不够两年
- 非 shock 子线仍然缺 authoritative 历史快照
- 今天的推进主要是把价格层这块“最基础、最卡回测复现”的部分打通

### 3. 接下来最该做的，不是再硬跑回测

而是两件事：

1. 补其余 authoritative 源的历史回放能力
2. 让策略/runner 支持按 `as_of_date` 真正重放，而不是用 `datetime.now()`

## 目前还存在的明确工程缺口

### 缺口 1：新闻/公告/情绪/事件/图谱 仍不是两年历史库

现有 collector 更像“今天抓最近一段时间”，不是“按历史日期回放”：

- [news.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/data_collector/news.py)
- [research.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/data_collector/research.py)
- [fundamental.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/data_collector/fundamental.py)

### 缺口 2：很多策略仍然使用 `datetime.now`

也就是说，就算底层数据准备好了，策略本身也还没有全面支持历史 `as_of_date` 重放。

### 缺口 3：当前审计里 `TwoYearReady` 仍然是 `NO`

不是因为审计器太苛刻，而是因为上游历史确实不够。

## 明天/回家后建议接着做的顺序

### 优先级 1

先把今天这批代码和数据同步下来，确保另一台机器可直接复现：

- pull 最新 `main`
- 确认以下目录存在：
  - [astrategy/.data/datahub](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/.data/datahub)
  - [astrategy/.data/authoritative_archive](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/.data/authoritative_archive)
  - [astrategy/.data/reports](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/.data/reports)

### 优先级 2

从“底层 authoritative 源的两年化”继续补：

- 公告历史扩窗
- 新闻历史采集
- 情绪日历快照归档
- 图谱按日/按周快照归档

### 优先级 3

再做策略重放改造：

- 给关键策略加 `as_of_date`
- 让 scheduler / archive 支持历史重放

### 优先级 4

等 authoritative 历史真正够长，再恢复 strict WF 研究，不再接受 proxy / degraded 结论。

## 可复跑命令

### 价格两年审计

```bash
python3 -m astrategy.archive.foundation_backfill_audit \
  --anchor-date 2026-03-19 \
  --universe csi800
```

### 价格两年补数

```bash
python3 astrategy/scripts/fetch_price_cache.py \
  --universe csi800 \
  --start-date 2024-03-20 \
  --end-date 2026-03-19 \
  --skip-covered
```

### 单独补 benchmark / 额外代码

```bash
python3 astrategy/scripts/fetch_price_cache.py \
  000300 000800 \
  --start-date 2024-03-20 \
  --end-date 2026-03-19
```

## 一句话结论

今天真正完成的是：

- 把价格层 authoritative 两年回填打通到 `CSI800 789/800`
- 把 benchmark / 缺失点位补齐
- 把 foundation 严格审计器做出来

但今天**没有**完成的是：

- 新闻 / 公告 / 情绪 / 事件 / 图谱 的两年 authoritative 历史

所以下一台机器继续接手时，最正确的认知是：

- 价格层已经基本就位
- 整体 strict 回测底座还没完全两年化
- 下一步应继续补上游 authoritative 历史，而不是急着产出新的收益结论
