# Phase 0 审计总结 — 关键缺口分析与MVP路径

**审计日期**: 2026-03-19
**审计人**: Claude (Phase 0 自动审计)

---

## 一、审计问答

### Q1: 当前研究页面哪些指标有真实回测来源？

**有真实来源但已过期(STALE)的**:
- 所有98条信号的字段数据(entry_price, fwd_return_5d, excess_5d等) — 来自akshare真实行情
- IS/OOS对比表 — 来自`shock_wf_backtest_20260318.md`(15事件版)
- 交易流水的盈亏计算 — 逻辑正确,基于真实价格

**已有更新版本但前端未同步**:
- 84事件/529信号的完整验证已在2026-03-18 21:02完成
- 验证结果: **WF全面FAIL**, OOS Sharpe=-0.07
- 前端仍展示15事件版的Sharpe=2.11/OOS=3.06

### Q2: 哪些是前端mock/假数据/无法追溯？

| 指标 | 问题 |
|------|------|
| "有效"/"待验证"/"无效" badge | 前端硬编码规则(sharpe>1.5→有效),无OOS验证 |
| "4948节点/4805边" | 硬编码数字,未动态读取 |
| 模块状态"已接入"/"降级" | UI文案,非量化指标 |

**注: 没有mock/伪造数据**, 问题不在"假数据"而在"过期数据+过度解读"

### Q3: 舆情/图谱/辩论哪些真正进入了交易决策？

**答: 几乎都没有。**

| 模块 | 对信号方向的影响 | 对置信度的影响 | 对仓位的影响 | 结论 |
|------|----------------|--------------|------------|------|
| 舆情(S10) | ❌ 仅提供event_type | ❌ | ❌ | 数据采集工具 |
| 图谱(Graph) | ❌ 不决定方向 | ❌ shock_weight未使用 | ❌ | 仅提供target列表(劣于随机) |
| Agent辩论 | ❌ skip_debate=True | ❌ divergence全部=0 | ❌ | 完全未执行 |

**实际决定交易的是**:
- 方向: `_AVOID_EVENTS` / `_LONG_EVENTS` 硬编码映射
- 标的: 图谱BFS 3跳传播(但消融证明不如随机)
- 持有期: 固定5日
- 入场价: event_date收盘价(存在point-in-time问题)

### Q4: 当前系统距离"真钱策略系统"还有哪些关键缺口？

#### 缺口矩阵

| # | 缺口 | 严重性 | 当前状态 | 目标状态 |
|---|------|--------|---------|---------|
| G1 | **核心alpha不存在** | 🔴P0 | 全样本Sharpe=0.47,OOS=-0.07 | OOS Sharpe≥1.0 |
| G2 | **图谱选股无优势** | 🔴P0 | 劣于随机(p=1.0) | 优于随机(p<0.05) |
| G3 | **入场价=收盘价** | 🟡P1 | event_date收盘 | T+1开盘价(可执行) |
| G4 | **固定5日持有** | 🟡P1 | 无动态出场 | 止盈止损/动态持有 |
| G5 | **Agent辩论未测试** | 🟡P1 | skip_debate=True | 开启并消融验证 |
| G6 | **事件时间模糊** | 🟡P1 | 只有event_date | 需要盘前/盘中/盘后区分 |
| G7 | **无涨跌停过滤** | 🟡P1 | 假设都能成交 | 过滤涨停不可买/跌停不可卖 |
| G8 | **无流动性约束** | 🟠P2 | 无限流动性 | 日成交量/市值约束 |
| G9 | **无组合级回测** | 🟠P2 | 单信号event-study | 真实组合(冲突/上限/资金分配) |
| G10 | **信号数不足** | 🟠P2 | 529信号(84事件) | 需2000+信号(500+事件) |
| G11 | **无bootstrap检验** | 🟠P2 | 单次WF | 多次bootstrap/permutation |
| G12 | **方向规则未优化** | 🟡P1 | 硬编码,84事件后earnings_surprise方向错误 | 数据驱动验证 |

---

### Q5: 最小可落地版本(MVP)路径 — 2周内跑出第一个严格OOS候选策略

#### Week 1: 数据+回测框架 (Day 1-7)

**Day 1-2: 修复回测框架**
```
任务:
1. 入场价改为T+1开盘价 (fix point-in-time bias)
2. 加入涨跌停过滤 (涨停不可买入, 跌停不可卖出)
3. 确认事件时间处理 (盘后事件→T+1, 盘中事件→T+1)
4. 交易成本维持40bps, 加入2x压力测试
验收: 重跑84事件, 确认指标变化幅度
```

**Day 3-4: 事件清洗+扩充**
```
任务:
1. 审查84事件的event_type标签准确性
   - EVT_AK109 "34只个股突破半年线" 被标为technology_breakthrough → 错误
   - EVT_AK110 "万科业主爆料楼板有洞" 被标为buyback → 错误
   - EVT_AK112/113 长安汽车回购被标为product_launch → 错误
2. 修正标签后重跑
3. 分析: 修正标签后哪些事件类型仍有效?
验收: 清洗后的事件数据库 + 重跑指标
```

**Day 5-7: 三条主线的event study**
```
任务:
1. S1(竞争对手负面→板块连坐): 过滤scandal事件, 仅看COMPETES_WITH关系
   - 84事件中scandal=67信号, 需分离COMPETES_WITH vs HOLDS_SHARES
2. S2(供应链冲击): 过滤supply_shortage/policy_risk, 仅看SUPPLIES_TO/CUSTOMER_OF
3. S3(产品发布/技术突破): 过滤product_launch/technology_breakthrough, 仅看产业链
每条主线输出:
- n(信号数)
- 5D/10D方向调整收益分布
- Sharpe/胜率/盈亏比
- vs随机选股的p-value
验收: 3条主线的event study报告
```

#### Week 2: WF验证+消融+决策 (Day 8-14)

**Day 8-10: Walk-Forward验证**
```
任务:
1. 对Week1中Sharpe>0.8的子策略做3窗口rolling WF
2. 窗口设计: 60%训练/40%测试, 滚动3次
3. 准入门槛:
   - 3个OOS窗口平均Sharpe≥0.8
   - 3个OOS窗口胜率均>50%
   - 3个OOS窗口MaxDD均<30%
   - 图谱优于随机(p<0.05)
验收: WALK_FORWARD_RESULTS.csv
```

**Day 11-12: 消融实验**
```
任务:
1. 图谱 vs 随机: 去掉图谱用随机同行业, Sharpe是否下降?
2. 关系类型: 仅产业链 vs 含HOLDS_SHARES, 差多少?
3. hop: 1跳 vs 2跳 vs 3跳, 哪个最优?
4. 方向规则 vs 反向 vs 随机, 规则有没有预测力?
5. (可选)Agent辩论: 开启3轮辩论, direction用consensus, Sharpe是否改善?
验收: ABLATION_MATRIX.csv
```

**Day 13-14: 最终决策**
```
任务:
1. 汇总所有结果, 输出候选策略卡片
2. 通过部署门槛的策略→进入模拟交易
3. 未通过的→记录失败原因, 提出下一步方向
4. 输出 FINAL_DEPLOYMENT_DECISION.md
验收: 策略卡片 + 部署决策
```

---

## 二、基于84事件数据的初步策略线索

从 `full_validation_20260318_2102.md` 可以提取的线索:

### 有前景的子策略

| 子策略 | n | Sharpe | 胜率 | 平均收益 | 判断 | 下一步 |
|--------|---|--------|------|---------|------|--------|
| **scandal板块连坐** | 67 | 3.90 | 68.7% | +3.09% | ⭐最佳 | 分离关系类型,WF验证 |
| cooperation | 69 | 2.47 | 69.6% | +2.11% | 值得 | 需WF验证 |
| price_adjustment | 29 | 2.25 | 69.0% | +1.14% | 值得 | 样本偏小 |
| buyback | 32 | 1.83 | 62.5% | +1.42% | 待验证 | 需清洗(部分标签错误) |
| ma(并购重组) | 8 | 1.63 | 62.5% | +0.83% | 样本不足 | 扩充事件 |

### 无效/有害的子策略

| 子策略 | n | Sharpe | 问题 |
|--------|---|--------|------|
| earnings_surprise | 73 | -3.45 | 方向映射错误(避开→实际应做多?) |
| technology_breakthrough | 45 | -2.20 | "利好出尽是利空"假设在大样本下不成立 |
| management_change | 33 | -1.13 | 方向不稳定 |

### Hop分布启示

| Hop | Sharpe | 启示 |
|-----|--------|------|
| 源头(0) | -1.04 | 必须过滤 |
| 1跳 | 0.27 | 效果差(可能已定价) |
| **2跳** | **1.24** | **最佳**(信息差可能存在) |
| 3跳 | -0.39 | 过度扩散,噪音太多 |

**启示**: 如果只保留 hop=2 的信号, 可能有更好的Sharpe, 但需验证。

---

## 三、84事件数据质量问题

审计发现以下事件标签可能错误:

| event_id | title | 当前标签 | 可能正确标签 |
|----------|-------|---------|-------------|
| EVT_AK109 | 34只个股突破半年线 | technology_breakthrough | **price_adjustment** 或删除(非事件) |
| EVT_AK110 | 万科业主爆料楼板有洞 | buyback | **scandal** |
| EVT_AK111 | 南玻A柔性玻璃 | technology_breakthrough | ✅正确 但stock_code=000002(万科)是错的 |
| EVT_AK112 | 长安汽车回购 | product_launch | **buyback** |
| EVT_AK113 | 长安汽车回购(另一条) | product_launch | **buyback** |
| EVT_AK114 | 33股获机构买入评级 | buyback | 删除(非单一事件) |
| EVT_AK115 | 潍柴动力回购 | product_launch | **buyback** |
| EVT_AK116 | 9股获机构买入评级 | buyback | 删除(非单一事件) |

**影响**: 标签错误直接导致方向映射错误 → 回测失真。这是84事件验证FAIL的部分原因。

---

## 四、部署门槛(与用户需求对齐)

| 准入指标 | 门槛 | 说明 |
|---------|------|------|
| OOS Sharpe | ≥1.0 | 年化, 3个WF窗口平均 |
| OOS 胜率 | >50% | 3个窗口均需满足 |
| OOS MaxDD | <30% | 事件级(非组合级) |
| 图谱vs随机 | p<0.10 | 至少10%显著性(放宽) |
| 2x成本后Sharpe | ≥0.5 | 80bps往返 |
| 最小信号数 | ≥30 per OOS window | 统计显著性 |
| 消融: 核心模块贡献 | Sharpe drop ≥0.3 when removed | 模块真实有用 |

**如果没有候选达到门槛, 结论必须是:**
> "当前没有可上线策略。Scandal板块连坐是最有前景的方向,但需独立大样本验证。继续研究,不准包装成交易系统。"

---

## 五、文件输出清单

Phase 0 已完成:
- [x] `AUDIT_METRIC_LINEAGE.md` — 指标数据血缘审计
- [x] `MODULE_TRADING_INTEGRATION_AUDIT.md` — 模块交易集成审计
- [x] `PHASE0_AUDIT_SUMMARY.md` — 本文件(缺口分析+MVP路径)

Phase 1-5 待输出:
- [ ] `EVENT_BACKTEST_SPEC.md` — 事件回测规格说明
- [ ] `SIGNAL_SCHEMA.md` — 信号定义schema
- [ ] `STRATEGY_CANDIDATES.md` — 候选策略卡片
- [ ] `ABLATION_MATRIX.csv` — 消融实验矩阵
- [ ] `WALK_FORWARD_RESULTS.csv` — WF验证结果
- [ ] `FINAL_DEPLOYMENT_DECISION.md` — 最终部署决策
