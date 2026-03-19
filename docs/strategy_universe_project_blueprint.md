# MiroFish 策略宇宙研究项目执行蓝图

> 生成日期: 2026-03-19
> 状态: Draft v1
> 目标: 从“多策略集合”升级为“事件驱动的策略宇宙研究工厂”
> 原则: 暂停前端优化，优先投入事件宇宙、图谱边际、Agent 辩论、严格 Walk-Forward 验证

---

## 一、项目定位

本阶段不再以页面呈现和交互优化为优先事项，而是集中构建一条完整的研究主链：

1. 扩大事件量级
2. 扩大事件多样性
3. 扩大图谱边际
4. 优化 Agent 辩论能力
5. 强化图谱与舆情/事件关联
6. 让系统对未来股票信息形成可回测的“涌现判断”
7. 所有模块必须先过严格 WF，再进入组合层和生产层

项目终态不是“再多几个策略”，而是形成一个可持续迭代的研究平台：

- 输入: 多源事件、公告、财报、舆情、政策、价格异常、机构行为
- 中间层: 图谱传播、动态状态、叙事识别、Agent 辩论、市场未反应检测
- 输出: 开仓、平仓、轮动、回避、减仓、观察
- 验证: point-in-time 回测 + Walk-Forward + 消融实验 + 组合稳定性审计

---

## 二、现状判断

当前仓库已经具备较好的研究骨架，尤其是以下模块已经存在：

- 统一信号与策略接口: [astrategy/strategies/base.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/strategies/base.py)
- 冲击传播主链: [astrategy/shock_pipeline.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/shock_pipeline.py)
- 动态状态图谱更新: [astrategy/graph/dynamic_updater.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/graph/dynamic_updater.py)
- 严格 point-in-time 回测: [astrategy/research/backtest_engine.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/research/backtest_engine.py)
- Walk-Forward 验证: [astrategy/research/walk_forward.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/research/walk_forward.py)
- 消融实验: [astrategy/research/ablation.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/research/ablation.py)
- 一键完整验证: [astrategy/research/run_full_validation.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/research/run_full_validation.py)

但当前仍有四个主要短板：

1. 事件宇宙规模偏小，历史样本不足，类型覆盖不够广。
2. 图谱偏静态，时间有效性、状态层、叙事层和事件层尚未完全成型。
3. Agent 辩论更多像解释模块，还不是标准化、可校准、可消融的研究部件。
4. 舆情、事件、图谱三者的联合打分与门禁不够严格，尚未形成统一信号工厂。

---

## 三、核心目标

### G1. 事件宇宙扩张

将历史事件库从当前“小样本精选”扩展到“可持续增量的大样本研究底座”。

目标：

- 历史可回测事件数达到 3,000-10,000 条
- 覆盖 12+ 类事件类型
- 每条事件可追溯 discover_time 和 available_at
- 支持按事件类型、行业、传播跳数、舆情状态分层回测

### G2. 图谱边际扩张

将图谱升级为四层结构：

- StructureGraph: 公司、行业、供应链、竞争、机构持股、管理层
- StateGraph: 公司状态、风险、动量、分析师趋势、景气变化
- EventGraph: 事件与公司/行业/主题之间的时序映射
- NarrativeGraph: 叙事主题、传播节点、拥挤度、相似话术

### G3. Agent 辩论升级

将多 Agent 机制从“生成长解释”升级为“输出结构化决策要素”的研究委员会。

输出目标：

- consensus_direction
- conviction
- divergence
- evidence_density
- invalidators
- expected_holding_days
- scenario_probs

### G4. 严格 WF 门禁

任何新增模块和新因子必须先过：

1. PIT 数据可见性检查
2. 单模块回测
3. Walk-Forward 验证
4. 消融实验
5. 组合稳定性审计

未通过门禁的模块不得进入生产信号池。

---

## 四、目标架构

### 4.1 总体架构

```text
Data Sources
  -> Event Ingestion
  -> Event Normalization
  -> Graph Update
  -> Agent Debate
  -> Signal Factory
  -> Portfolio Decision
  -> Backtest / WF / Ablation / Audit
```

### 4.2 研究主链

统一采用以下五段式信号工厂：

1. Trigger
   事件是否足够重要，是否具备交易价值
2. Propagation
   图谱如何传播，候选股票如何筛出
3. Debate
   多 Agent 如何裁决方向、确定性和失效条件
4. Market Check
   市场是否已反应，当前是否拥挤，是否还能交易
5. Action
   输出开仓、平仓、轮动、回避、减仓或观察

### 4.3 动作类型

项目输出不再局限于 `long/avoid`，升级为：

- `open_long`
- `add_long`
- `trim_long`
- `close_long`
- `rotate_into`
- `rotate_out`
- `avoid`
- `observe`

其中：

- `observe` 用于高分歧、证据不足但有潜在线索的事件
- `rotate_into` / `rotate_out` 用于主线拥挤后的支线轮动

---

## 五、模块拆分

### M1. Event Master 事件主表

目标：统一所有事件的标准化入口。

建议新增目录：

```text
astrategy/events/
  __init__.py
  schemas.py
  registry.py
  normalizer.py
  deduper.py
  scorer.py
  loaders/
    announcements.py
    earnings.py
    policy.py
    news.py
    sentiment.py
    capital_flow.py
    price_anomaly.py
```

核心数据结构建议：

```python
EventMasterRecord = {
    "event_id": str,
    "event_type": str,
    "event_subtype": str,
    "title": str,
    "summary": str,
    "source": str,
    "entity_codes": list[str],
    "industry_codes": list[str],
    "theme_tags": list[str],
    "event_time": str,
    "discover_time": str,
    "available_at": str,
    "severity": float,
    "surprise_score": float,
    "tradability_score": float,
    "novelty_score": float,
    "crowding_risk": float,
    "confidence": float,
    "raw_payload_ref": str,
}
```

必须满足：

- 事件时间和可见时间分离
- 原始文本保留引用
- 可以去重和事件合并
- 支持一个事件命中多个实体和主题

### M2. Event Taxonomy 事件分类体系

第一版建议至少覆盖以下 12 类：

1. earnings_surprise
2. announcement_sentiment
3. supply_disruption
4. price_adjustment
5. order_win
6. policy_support
7. policy_risk
8. management_change
9. capital_flow_shift
10. narrative_breakout
11. sentiment_reversal
12. overseas_mapping

后续扩展类：

- regulation_enforcement
- product_launch
- ma_restructuring
- technology_breakthrough
- analyst_revision
- valuation_dislocation

### M3. Graph Layer 图谱层升级

建议新增目录：

```text
astrategy/graph/
  structure_store.py
  state_store.py
  event_store.py
  narrative_store.py
  temporal_edges.py
  edge_scorer.py
```

每条边统一补充字段：

```python
TemporalEdge = {
    "source": str,
    "target": str,
    "relation": str,
    "weight": float,
    "confidence": float,
    "valid_from": str,
    "valid_to": str | None,
    "evidence_count": int,
    "source_type": str,
    "last_verified_at": str,
}
```

关键要求：

- 没有时间有效性的边，不进入严格研究层
- 没有置信度的边，只能作为探索层，不得直接入组合
- 需要支持 snapshot 查询，确保回测时只看到当时已存在的边

### M4. Narrative Layer 叙事层

目标：把“主题热度”升级为“叙事生命周期与拥挤度模型”。

建议新增：

```text
astrategy/narratives/
  taxonomy.py
  extractor.py
  crowding.py
  phase_model.py
  relation_builder.py
```

核心输出：

- narrative_score
- phase
- crowding_score
- media_breadth
- retail_attention
- institution_attention
- skepticism_score
- leader_stocks
- laggard_stocks

### M5. Debate Engine 多 Agent 辩论引擎

目标：将辩论从“自然语言展示”改造成“结构化研究器”。

建议新增：

```text
astrategy/debate/
  schemas.py
  orchestrator.py
  agents/
    fundamental.py
    causality.py
    sentiment.py
    risk.py
    pm.py
  calibrator.py
```

标准输出结构：

```python
DebateResult = {
    "target_code": str,
    "consensus_direction": str,
    "conviction": float,
    "divergence": float,
    "evidence_density": float,
    "scenario_probs": dict,
    "invalidators": list[str],
    "expected_holding_days": int,
    "agent_votes": list[dict],
    "debate_summary": str,
}
```

研究要求：

- 记录每个 Agent 投票
- 记录分歧来源
- 记录最终失效条件
- 后续回测能验证“高 conviction 是否真的更优”

### M6. Market Check 市场反应检查层

目标：把“是否已反应”从简单阈值判断，升级为统一门禁。

建议新增：

```text
astrategy/market_checks/
  reaction.py
  crowding.py
  liquidity.py
  gap_risk.py
  tradability.py
```

检查维度：

- 已反应程度
- 近 1/3/5/10 日价格和成交量异常
- 是否一字板/低流动性/难成交
- 是否舆情拥挤
- 是否存在冲高兑现风险

### M7. Signal Factory 统一信号工厂

目标：将所有策略和传播主线统一接入同一生产框架。

建议新增：

```text
astrategy/signals/
  factory.py
  ranker.py
  actions.py
  filters.py
  explain.py
```

统一信号结构：

```python
UnifiedSignal = {
    "signal_id": str,
    "event_id": str,
    "target_code": str,
    "action": str,
    "score": float,
    "confidence": float,
    "expected_return": float,
    "expected_holding_days": int,
    "source_modules": list[str],
    "trigger_strength": float,
    "propagation_score": float,
    "debate_score": float,
    "market_check_score": float,
    "risk_penalty": float,
    "reasoning": str,
}
```

### M8. Portfolio Engine 组合决策层

目标：让系统能从单票判断，升级为组合层动作。

建议新增：

```text
astrategy/portfolio/
  allocator.py
  constraints.py
  rotation.py
  risk_budget.py
  execution_plan.py
```

约束维度：

- 单票风险预算
- 单事件簇暴露
- 单叙事暴露
- 单行业暴露
- 流动性约束
- 高相关信号合并

---

## 六、数据与存储设计

### 6.1 数据分层

```text
astrategy/.data/
  raw/
    events/
    announcements/
    news/
    sentiment/
  curated/
    event_master/
    graph_snapshots/
    narrative_states/
  research/
    backtests/
    walk_forward/
    ablation/
    calibration/
  production/
    signals/
    portfolios/
```

### 6.2 必须落盘的研究资产

以下内容必须持久化，不能只在内存里跑：

- 原始事件与归一化事件
- 每日图谱快照或可重放增量
- 每次 Agent 投票明细
- 每次市场反应检查结果
- 每次信号工厂中间分数
- 每次 WF 结果与审计报告

### 6.3 PIT 原则

所有研究数据必须满足以下字段至少其一：

- `available_at`
- `as_of_date`
- `snapshot_date`

如果某模块无法提供可见性时间戳，则该模块只能进入探索层，不能进入严格回测层。

---

## 七、回测与 WF 门禁设计

### 7.1 统一研究门禁

任何新模块上线前必须依次通过：

1. 数据可见性检查
2. 单模块回测
3. Walk-Forward 验证
4. 消融实验
5. 组合稳定性检验

### 7.2 基础准入标准

第一版建议沿用并略增强现有标准：

- OOS Sharpe >= 0.8
- OOS 胜率 >= 55%
- OOS 信号数 >= 50
- OOS Max Drawdown <= 15%

新增辅助准入：

- 月均有效事件数 >= 20
- 至少覆盖 4 类事件类型
- 高置信度分组表现优于低置信度分组
- 有图谱传播组表现优于无图谱组

### 7.3 研究指标体系

在现有收益指标外，新增长期研究指标：

- Coverage: 月均事件覆盖数
- Breadth: 事件类型分布熵
- Calibration: 置信度与实际收益的一致性
- Debate Edge: 使用辩论 vs 跳过辩论的增量
- Graph Edge Value: 有传播路径 vs 无传播路径的增量
- Narrative Crowding Penalty: 拥挤度过高的收益衰减
- Latency Edge: 未反应标的相对已反应标的的超额收益

### 7.4 消融实验矩阵

第二阶段后，消融必须覆盖：

1. 去图谱
2. 去动态状态图
3. 去叙事层
4. 去 Agent 辩论
5. 去未反应过滤
6. 去舆情拥挤门禁
7. 去轮动逻辑

---

## 八、优先级排序

### P0. 研究底盘优先

先做：

1. 扩历史事件库
2. 建统一事件 schema
3. 增加时间有效性和 snapshot 能力
4. 让现有 WF 框架跑更大的事件样本

不先做：

- 前端改版
- 展示层重构
- 大规模 UI 指标墙

### P1. 事件-图谱-舆情联动

重点完成：

- Event Master
- EventGraph
- NarrativeGraph
- 市场反应门禁
- 统一信号工厂

### P2. 辩论与轮动

重点完成：

- Debate Engine
- Conviction/Divgergence 校准
- 轮动逻辑
- 回避与减仓逻辑

### P3. 组合与生产

重点完成：

- 组合约束
- 仓位引擎
- 自动审计
- 研究到生产的门禁发布流

---

## 九、12 周实施路线图

### Phase 1. 事件底盘与样本扩张

周期：第 1-3 周

目标：

- 历史事件库扩到 1,000+ 条
- 建立统一事件 schema
- 完成事件去重、合并、标准化
- 将现有回测主链切换到 Event Master

交付物：

- `astrategy/events/` 目录
- `event_master` 数据表或 Parquet
- 历史事件扩展脚本升级版
- 第一版事件分类字典

验收：

- 至少 8 类事件类型
- 至少 1,000 条事件
- 90% 以上事件有 `available_at`
- 可通过现有回测引擎读取

### Phase 2. 图谱四层化

周期：第 4-6 周

目标：

- 完成 Structure/State/Event/Narrative 四层图谱
- 支持时间有效边和 snapshot 查询
- 动态状态更新纳入回测可见性

交付物：

- 四层图谱存储模块
- temporal edge schema
- graph snapshot loader
- 事件到图谱的写入通道

验收：

- 图谱边具备时间字段
- 能按任意历史日期重放图谱状态
- 事件传播路径可追溯

### Phase 3. Debate Engine 与信号工厂

周期：第 7-9 周

目标：

- 统一 Agent 投票格式
- 引入 divergence、conviction、invalidators
- 统一 Market Check 门禁
- 产出可执行动作级信号

交付物：

- `astrategy/debate/`
- `astrategy/market_checks/`
- `astrategy/signals/factory.py`

验收：

- 每个信号可追溯到 Trigger/Propagation/Debate/Market Check
- 高 conviction 分组优于低 conviction 分组
- 高频分歧组默认降权

### Phase 4. 组合与审计

周期：第 10-12 周

目标：

- 完成组合风险预算
- 完成轮动和回避逻辑
- 建自动化 WF 审计

交付物：

- `astrategy/portfolio/`
- 组合层回测
- 审计报告模板升级

验收：

- 支持开仓、减仓、平仓、轮动、回避
- 组合级 OOS 指标稳定
- 每周自动生成审计报告

---

## 十、代码落位建议

### 新增目录

```text
astrategy/
  events/
  narratives/
  debate/
  market_checks/
  signals/
  portfolio/
```

### 优先改造的现有文件

- [astrategy/expand_historical_events.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/expand_historical_events.py)
  - 从“扩几个事件”升级为历史事件标准化入口
- [astrategy/shock_pipeline.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/shock_pipeline.py)
  - 升级为标准信号工厂主链
- [astrategy/graph/dynamic_updater.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/graph/dynamic_updater.py)
  - 纳入可见时间和状态快照能力
- [astrategy/research/backtest_engine.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/research/backtest_engine.py)
  - 对接 Event Master 与动作级信号
- [astrategy/research/ablation.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/research/ablation.py)
  - 扩展到图谱/叙事/辩论/拥挤门禁四类消融
- [astrategy/research/run_full_validation.py](/Users/xukun/Documents/freqtrade/MiroFish/astrategy/research/run_full_validation.py)
  - 升级为研究总审计入口

---

## 十一、关键研究假设

本项目的研究 alpha 假设分为五类：

1. 信息扩散存在时间差，市场对传播链二跳、三跳标的定价更慢。
2. 叙事扩散具有生命周期，萌芽期与扩散早期存在更高 alpha。
3. 多 Agent 分歧本身是风险刻画工具，不只是解释变量。
4. 舆情不是单独的买卖信号，而是图谱传播与事件定价速度的调节器。
5. 真正可交易的 alpha 来自“事件强度 x 图谱传播 x 市场未反应 x 辩论共识”的交集。

---

## 十二、风险清单

### R1. 事件样本污染

风险：

- 事后挑选事件
- 事件日期与可见日期混淆

应对：

- 强制保留 `available_at`
- 所有历史事件保留原始来源引用

### R2. 图谱事后增强

风险：

- 使用未来才知道的关系边参与历史传播

应对：

- 强制时间边
- 强制 snapshot 回放

### R3. Agent 幻觉

风险：

- 辩论结果看似合理但不可验证

应对：

- 只采纳结构化输出
- 做 conviction / divergence 校准曲线

### R4. 舆情过拟合

风险：

- 热度指标解释力不稳定

应对：

- 只作为调节项，不单独成为主决策因子
- 在 WF 中单独做舆情消融

### R5. 组合拥挤

风险：

- 多条信号实质指向同一事件簇

应对：

- 事件簇约束
- 叙事簇约束
- 高相关信号合并

---

## 十三、第一批具体任务

建议立即启动以下 8 个任务，按顺序推进：

1. 重构历史事件扩展脚本为 Event Master Loader。
2. 定义统一事件 schema 与事件分类字典。
3. 建立 `available_at` / `discover_time` 的 PIT 校验器。
4. 为图谱边增加时间有效字段和 snapshot 查询能力。
5. 将 `shock_pipeline` 重构为 Trigger/Propagation/Debate/Market Check/Action 五段式。
6. 建立结构化 DebateResult 与投票持久化。
7. 扩展 WF 审计指标，增加 coverage、breadth、calibration。
8. 建立第二版消融矩阵，验证图谱、辩论、舆情、未反应过滤的增量价值。

---

## 十四、阶段成功标准

当以下条件同时满足时，视为“策略宇宙研究底盘建立完成”：

- 历史事件样本 >= 3,000
- 至少 12 类事件纳入统一 schema
- 图谱支持 snapshot 与 temporal edge
- Agent 辩论结果结构化并可校准
- 统一信号工厂支持动作级输出
- 至少 4 组关键消融实验显示正增量
- 组合级 OOS 指标连续多个窗口稳定达标

---

## 十五、结论

本阶段的项目主线已经明确：

- 不再优先做前端改造
- 先把研究底盘做厚
- 先把事件宇宙做大
- 先把图谱做成时间化、状态化、叙事化
- 先把 Agent 辩论做成可回测、可校准、可淘汰的模块
- 再在严格 WF 基础上做组合决策和生产化

后续一切开发任务，都应优先回答两个问题：

1. 这是否在扩大“有效策略宇宙”的边界？
2. 这是否能在严格 WF 下证明增量价值？

若答案是否定的，则优先级应下调。
