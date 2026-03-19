# MiroFish 底层数据建设蓝图

> 生成日期: 2026-03-19
> 状态: Draft v1
> 目标: 先统一股票池与底层数据覆盖，再恢复策略研究和严格回测
> 原则: 数据主权优先于策略细节，覆盖率审计优先于样本扩容，PIT 规范优先于模型复杂度

---

## 一、问题定义

当前系统已经验证出一条有研究价值的策略链路，但还不具备进入生产级研究的前提。

核心矛盾不在于“事件数量只有 84 条”，而在于：

1. 研究股票池没有被唯一、明确地定义。
2. 底层数据没有按股票池维度完成覆盖率审计。
3. 历史事件库只是小样本样机，不是股票池级别的连续事件主数据。
4. 现有严格 WF 更多是在验证“策略链路是否有轮廓”，不是在验证“一个完整股票池上的生产研究系统”。

因此，下一阶段的正确顺序不是继续扩策略或单纯把事件数从 84 提到 1000，而是：

1. 定义唯一股票池
2. 做覆盖率审计
3. 建底层主数据
4. 通过数据门禁
5. 再恢复策略研究和严格回测

---

## 二、当前现状审计

基于当前仓库实际数据，已经确认的事实如下：

- 当前事件主表: [historical_event_master.json](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/.data/event_master/historical_event_master.json)
- 历史事件总数: `84`
- 事件日期范围: `2024-01-15` 到 `2026-03-17`
- 事件源股票数: `51`
- 图谱文件: [supply_chain.json](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/.data/local_graph/supply_chain.json)
- 图谱中可识别的 6 位股票代码节点: 约 `4698`
- 当前事件源对图谱股票节点覆盖率: 约 `1.1%`
- 当前事件月均密度: `84 / 27 ≈ 3.11` 条

这说明：

- 当前图谱层比事件层大很多。
- 当前事件层远未覆盖当前隐含股票池。
- 当前“回测通过”只代表研究主链方向大体正确，不代表股票池级别可实盘。

---

## 三、总体目标

本蓝图的目标不是直接产出交易 alpha，而是建设一套可支撑 alpha 研究的底层数据底座。

阶段目标：

1. 统一股票池定义
2. 建立股票池主表和证券主数据
3. 建立事件主表和多源事件摄取链路
4. 建立价格、公告、财报、新闻、图谱、舆情的 PIT 数据层
5. 建立覆盖率审计体系和准入门禁
6. 只有在数据门禁通过后，才允许恢复策略研究和严格回测

---

## 四、股票池定义

### 4.1 原则

必须停止“文档里写全 A 股、回测里跑小样本、图谱里又像全市场”的混合状态。

股票池必须满足：

- 唯一口径
- 可版本化
- 可追溯变更
- 可区分研究池与生产池

### 4.2 三层股票池

建议采用三层股票池体系：

1. `L0 全市场注册池`
   - 范围: 全 A 股股票主数据
   - 作用: 做证券主表、退市历史、行业映射、图谱映射
   - 目标规模: 4500-5500

2. `L1 研究池`
   - 第一阶段建议口径: `CSI800`
   - 原因: 流动性较好、覆盖范围够广、能兼顾研究效率
   - 作用: 第一阶段严格 WF 与策略研究的标准股票池

3. `L2 生产候选池`
   - 从 `L1` 中筛出“底层数据覆盖率达标”的股票
   - 作用: 只有该池股票允许进入生产候选研究

### 4.3 明确建议

第一阶段应采用：

- `L0 = 全 A 股证券注册池`
- `L1 = CSI800 标准研究池`
- `L2 = CSI800 中覆盖率达标子集`

这样可以避免一开始就被全市场数据质量拖死，同时保留未来向全 A 扩展的路径。

---

## 五、必须先建设的数据域

按研究需要，底层数据至少拆成 8 个数据域。

### D1. 证券主数据

每只股票必须有：

- `ticker`
- `exchange`
- `company_name`
- `list_date`
- `delist_date`
- `status`
- `industry_level_1/2/3`
- `concept_tags`
- `index_membership`

### D2. 交易与价格数据

每只股票必须有：

- 日频 OHLCV
- 复权口径
- 停牌状态
- 涨跌停状态
- 交易日历
- corporate actions

这是所有回测的最低前提。

### D3. 公告与事件数据

这不是“挑 84 个大事件”，而是股票池内逐日事件流：

- 公告标题
- 公告正文引用
- 财报/快报/预告
- 政策命中
- 回购
- 管理层变更
- 合作
- 订单
- 调价
- 风险事件
- 并购重组

### D4. 新闻与舆情数据

必须区分：

- 正式公告
- 新闻媒体
- 实时舆情流

舆情层至少要有：

- 新闻时间
- 抓取时间
- 发布源
- 去重指纹
- 情绪分数
- 热度
- 主题标签

### D5. 财务与预期数据

每只股票至少要有：

- 财报发布日期
- 核心财务指标
- 盈利预测
- 一致预期
- 预期修正

### D6. 图谱数据

图谱必须服务于股票池，而不是独立膨胀。

至少包含：

- 供应链
- 客户
- 竞争
- 行业
- 概念
- 持股
- 管理层

每条边必须具备：

- `valid_from`
- `valid_to`
- `source`
- `confidence`

### D7. 状态数据

这是动态层，不是静态描述：

- 风险状态
- 景气状态
- 动量状态
- 分析师状态
- 经营状态

### D8. 覆盖率审计数据

这是独立数据域，不是附带统计：

- 每只股票每个数据域是否覆盖
- 时间连续性是否完整
- `available_at` 是否存在
- 是否允许进入研究池

---

## 六、PIT 与时间规范

这一部分是底层数据建设的硬约束。

任何进入回测的数据记录都必须有：

- `event_time`
- `discover_time`
- `available_at`
- `ingested_at`
- `source`

必须遵守：

1. 回测只能看到 `available_at <= t` 的数据
2. 不允许用事后修订后的数据覆盖历史可见版本
3. 图谱边和状态也必须具备时间有效性
4. 任何缺少 `available_at` 的事件都不能直接进入严格 WF

---

## 七、底层数据模型

建议新增一个统一数据域目录：

```text
astrategy/datahub/
  universe/
    security_master.py
    universe_registry.py
    universe_snapshots.py
  market/
    prices.py
    corporate_actions.py
    calendars.py
  events/
    event_master.py
    event_sources.py
    event_deduper.py
    event_linker.py
  news/
    news_master.py
    sentiment_master.py
  fundamentals/
    filings.py
    earnings.py
    expectations.py
  graph/
    graph_master.py
    edge_registry.py
    graph_snapshots.py
  audit/
    coverage_audit.py
    pit_audit.py
    freshness_audit.py
```

建议新增的核心主表：

### 7.1 `security_master`

```python
{
    "ticker": str,
    "company_name": str,
    "exchange": str,
    "list_date": str,
    "delist_date": str | None,
    "status": str,
    "industry_l1": str,
    "industry_l2": str,
    "industry_l3": str,
    "concept_tags": list[str],
}
```

### 7.2 `universe_membership`

```python
{
    "universe_id": str,         # all_a / csi800 / production_candidate
    "ticker": str,
    "valid_from": str,
    "valid_to": str | None,
    "membership_source": str,
}
```

### 7.3 `event_master`

```python
{
    "event_id": str,
    "ticker": str,
    "event_type": str,
    "event_subtype": str,
    "title": str,
    "summary": str,
    "event_time": str,
    "discover_time": str,
    "available_at": str,
    "source": str,
    "severity": float,
    "surprise_score": float,
    "tradability_score": float,
    "raw_ref": str,
}
```

### 7.4 `coverage_audit_snapshot`

```python
{
    "audit_date": str,
    "universe_id": str,
    "ticker": str,
    "has_prices": bool,
    "has_events": bool,
    "has_filings": bool,
    "has_news": bool,
    "has_sentiment": bool,
    "has_graph": bool,
    "has_available_at": bool,
    "coverage_score": float,
    "research_ready": bool,
}
```

---

## 八、覆盖率审计体系

策略研究恢复前，必须先有覆盖率评分体系。

### 8.1 股票级覆盖率

对每只股票打以下分：

- `price_coverage`
- `event_coverage`
- `filing_coverage`
- `news_coverage`
- `sentiment_coverage`
- `graph_coverage`
- `pit_completeness`

综合成：

- `coverage_score`
- `research_ready`

### 8.2 股票池级覆盖率

对整个股票池至少输出这些指标：

- 股票总数
- 有价格数据的股票数
- 有事件数据的股票数
- 有图谱映射的股票数
- 有公告数据的股票数
- 有舆情数据的股票数
- 可进入研究池的股票数

### 8.3 事件级覆盖率

对事件域输出：

- 每月事件数
- 每类事件数
- 每只股票的事件月均数
- 缺失 `available_at` 的事件比例
- 未成功映射股票代码的事件比例

### 8.4 门禁阈值

第一阶段建议阈值：

- `price_coverage >= 99%`
- `pit_completeness >= 95%`
- `graph_coverage >= 70%`
- `event_coverage >= 80%`
- `research_ready 股票数 >= 研究池 70%`

未达标时：

- 不允许恢复严格 WF
- 不允许继续做策略优选结论

---

## 九、数据建设顺序

### 阶段 0: 股票池统一

目标：

- 建立唯一股票池定义
- 统一 `all_a / csi800 / production_candidate`
- 生成可版本化 universe snapshot

交付物：

- `security_master`
- `universe_membership`
- universe 版本快照

### 阶段 1: 价格与日历底座

目标：

- 股票池内每只股票都有连续日频价格
- corporate actions 可回溯
- 交易日历统一

交付物：

- `prices`
- `corporate_actions`
- `trading_calendar`
- price coverage audit

### 阶段 2: 事件主表建设

目标：

- 不再依赖 hand-curated 小样本事件
- 股票池内形成逐日事件流

交付物：

- `event_master`
- 事件去重与实体映射
- event coverage audit

### 阶段 3: 新闻/舆情/公告/财务层

目标：

- 公告、新闻、舆情、财务层统一接入
- 全部具备 `available_at`

交付物：

- `news_master`
- `sentiment_master`
- `filings`
- `earnings`
- pit audit

### 阶段 4: 图谱与状态层清洗

目标：

- 图谱与研究池对齐
- 边具有时间有效性
- 状态层可回放

交付物：

- `graph_master`
- `graph_snapshot`
- `state_snapshot`
- graph coverage audit

### 阶段 5: 数据门禁通过后恢复策略研究

目标：

- 在达标股票池上重新启动严格 WF
- 先验证数据覆盖扩展后的基线表现

交付物：

- 新版研究基线报告
- 数据就绪审计报告

---

## 十、恢复策略研究前的硬门槛

只有满足以下条件，才能恢复“策略回测结论驱动”的工作流：

1. 股票池定义已经冻结
2. 股票池覆盖率审计已经自动化
3. 价格与事件层 PIT 完整
4. `research_ready` 股票数达到阈值
5. 月度事件密度达到可研究水平

建议最低研究密度：

- `CSI800` 研究池中，月均事件数至少 `200+`
- 单只股票年均事件数至少 `3-5`
- 重点事件类型不得长期断档

在这之前，所有回测结论都必须被标记为：

- `Research Prototype Result`
- 不得解释为生产结论

---

## 十一、第一批必须落地的任务

### T1. 定义唯一股票池口径

- 固化 `L0/L1/L2`
- 第一阶段冻结 `CSI800` 作为标准研究池

### T2. 建 `security_master`

- 从全 A 股证券名录生成主表
- 保留上市/退市历史

### T3. 建 `universe_membership`

- 写入 `all_a` 与 `csi800`
- 支持按日期回放

### T4. 建价格覆盖率审计

- 每只股票检查价格连续性
- 输出 price coverage dashboard

### T5. 建事件覆盖率审计

- 每只股票统计事件量
- 输出股票池级 coverage report

### T6. 重建 `event_master`

- 以股票池为中心，不再以手工事件为中心

### T7. 建 `available_at` 规则

- 所有事件与新闻入库必须带时间可见性

### T8. 对齐图谱与股票池

- 清理无法映射 ticker 的边
- 统计每只股票 graph coverage

### T9. 建 `research_ready` 评分

- 把股票是否允许进入研究池做成自动判断

### T10. 暂停新增策略优选

- 在 `research_ready` 未达标前，不再把回测通过解释为生产可用

---

## 十二、阶段验收标准

### 验收 A: 数据底座初步可用

- `security_master` 完成
- `csi800` 研究池可回放
- 价格覆盖率审计完成

### 验收 B: 事件层可研究

- 研究池内事件主表形成稳定增量
- 月均事件密度达到第一阶段目标
- `available_at` 缺失率低于阈值

### 验收 C: 数据门禁通过

- `research_ready` 股票数达到阈值
- 图谱与事件层对齐
- 新闻/公告/财报层能做 PIT 回放

### 验收 D: 恢复策略研究

- 在达标股票池上重跑严格 WF
- 重建基线，不沿用当前 84 事件小样本结论

---

## 十三、最终判断

当前系统最需要的不是更复杂的策略，而是更真实的研究地基。

后续工作的优先级应当改成：

1. 先统一股票池
2. 再建设股票池级主数据
3. 再做覆盖率审计与 PIT 门禁
4. 最后才恢复策略研究与严格回测

换句话说，下一阶段的核心任务不是“找更强信号”，而是：

**把“策略样机”升级成“股票池级、数据完备、时间可审计”的研究系统。**
