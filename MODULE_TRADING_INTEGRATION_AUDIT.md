# MODULE_TRADING_INTEGRATION_AUDIT.md — 模块交易集成审计

**审计日期**: 2026-03-19
**审计范围**: 舆情检测 / 图谱传播 / Agent辩论 三大模块
**审计结论**: **三个模块均未真正参与交易决策。** 当前信号方向和置信度完全由硬编码规则决定，与LLM/图谱/辩论输出无关。

---

## 一、模块级审计矩阵

| 审计维度 | 舆情检测(S10) | 图谱传播(Graph) | Agent辩论(Debate) |
|---------|---------------|----------------|-------------------|
| **是否参与信号生成?** | ❌ 仅提供事件源 | ⚠️ 仅提供传播路径(target选择) | ❌ 完全跳过 |
| **是否只是UI文案?** | 部分(前端显示"降级") | 部分(前端显示图谱可视化) | 是("未触发"/"高共识"等) |
| **是否进入回测撮合?** | ❌ 不影响任何回测参数 | ⚠️ 仅决定target_code选择 | ❌ skip_debate=True |
| **是否参与入场决策?** | ❌ | ⚠️ 间接(决定了哪些股票入池) | ❌ |
| **是否参与出场决策?** | ❌ 固定5日持有 | ❌ 固定5日持有 | ❌ 固定5日持有 |
| **是否参与仓位管理?** | ❌ | ❌ | ❌ |
| **是否有独立消融实验?** | ✅ 有(Iter4, n=4信号) | ✅ 有(84事件, p=1.0劣于随机) | ❌ 无(因从未开启) |
| **消融结论** | 样本不足(仅4信号) | **无alpha**(劣于随机) | 不可测(未参与) |

---

## 二、各模块详细审计

### A. 舆情检测(S10)

**代码路径**: `strategies/s10_sentiment_simulation.py`

**实际功能**:
- 从新闻/公告中提取事件 → 写入 `historical_events.json`
- 提供 `event_type` (scandal/product_launch等) 和 `impact_level`
- **不参与信号方向判定** — 方向由shock_pipeline中的硬编码规则决定

**消融实验结果(Iter4)**:
```
S10独立信号: 4条(3只股票)
胜率: 66.7%
结论: 样本不足，无法判定
```

**真实角色**: 事件数据采集工具，不是alpha来源。

**信号生成中的参与度**:
```python
# shock_pipeline.py 中，S10仅提供event作为输入
# 实际信号方向完全由以下硬编码规则决定:
_AVOID_EVENTS = {"scandal", "policy_risk", "management_change",
                  "product_launch", "technology_breakthrough",
                  "price_adjustment", "buyback"}
_LONG_EVENTS = {"cooperation", "earnings_surprise", "supply_shortage", "order_win"}
# ↑ 这些规则与S10的LLM分析完全无关
```

---

### B. 图谱传播(Graph)

**代码路径**: `graph/topology.py::propagate_shock()`, `shock_pipeline.py`

**实际功能**:
- 从source_code出发，沿图谱边传播找到downstream target_code列表
- 提供 `propagation_path`, `relation_chain`, `hop`, `shock_weight`
- **间接影响入场** — 决定了"哪些股票会被选为交易标的"

**传播参数**:
```python
max_hops = 3
decay_per_hop = 0.5
max_targets = 15
关系类型 = {SUPPLIES_TO, CUSTOMER_OF, COOPERATES_WITH, COMPETES_WITH, HOLDS_SHARES}
双向关系 = {COOPERATES_WITH, COMPETES_WITH, HOLDS_SHARES}
```

**关键问题: HOLDS_SHARES 污染**
- HOLDS_SHARES(基金持仓)关系无因果逻辑
- "新希望猪瘟→(基金持仓)→中望软件" = 无商业关联
- 但图谱传播**不区分**因果关系和统计关系

**84事件消融实验结果**:
```
图谱选股 Sharpe: 0.47
随机同行业选股 Sharpe: 0.66 (std=0.00)
图谱优势: -0.19 (负值!)
p-value: 1.0000
结论: 图谱传播选股 **劣于** 随机选股
```

**真实角色**: 提供target候选列表，但选择质量不如随机。原因可能是:
1. HOLDS_SHARES边引入大量无关标的
2. 图谱结构本身不反映事件冲击传播路径
3. 3跳传播过度扩散

---

### C. Agent辩论(Debate)

**代码路径**: `strategies/s10_sentiment_simulation.py::run_debate()`

**设计功能(文档描述)**:
- 6个投资者角色(价值/技术/量化/散户/机构/宏观)
- 3轮LLM辩论级联
- 输出 consensus_direction, divergence, conviction

**实际执行状态**:
```python
# backtest_engine.py 中:
pipeline.run_historical(..., skip_debate=True)  # 永远跳过

# 因此所有98/529信号中:
divergence = 0.0  # 硬编码
conviction ∈ {0.25, 0.3, 0.4, 1.0}  # 来自规则而非LLM
debate_summary = "" 或 "事件源头"  # 空字符串
```

**独立消融实验**: **不存在**（因从未开启，无法做消融）

**真实角色**: 纯粹的代码存在(dead code in backtest context)。前端将其展示为"未触发"是准确的，但在首页"模块状态"卡片中展示"Agent辩论"暗示它是系统核心模块，这是**误导性的**。

---

## 三、信号方向和置信度的真实来源

### 方向(signal_direction)

| 来源 | 参与度 |
|------|-------|
| 事件类型规则映射 | **100%** — 唯一来源 |
| Agent辩论consensus | 0% — 完全未使用 |
| 图谱结构分析 | 0% — 不影响方向 |
| 舆情情感分析 | 0% — 不影响方向 |
| 价格反应 | 0% — 不影响方向(仅影响reacted标记) |

**代码证据** (`shock_pipeline.py` _build_shock_signal):
```python
# 方向完全由event_type决定，不看图谱、辩论、舆情
if event_type in _AVOID_EVENTS:
    direction = "avoid"
elif event_type in _LONG_EVENTS:
    direction = "long"
else:
    direction = "avoid"  # conservative default
```

### 置信度(confidence)

| 来源 | 参与度 |
|------|-------|
| 事件类型bonus(查表) | ~40% |
| hop bonus(规则) | ~30% |
| reacted bonus(规则) | ~20% |
| base常数(0.3) | ~10% |
| Agent conviction | 0% |
| 图谱shock_weight | 0% — 字段存在但未进入confidence公式 |
| 舆情强度 | 0% |

**代码证据**:
```python
# confidence计算完全是规则硬编码
base = 0.3
bonus = EVENT_TYPE_BONUS.get(event_type, 0.05)  # 0.05-0.25
if hop >= 1: bonus += 0.10
if hop >= 2: bonus += 0.05
if not reacted: bonus += 0.15
confidence = min(0.9, max(0.1, base + bonus))
```

### 仓位(position_hint)

| 来源 | 参与度 |
|------|-------|
| confidence阈值(前端规则) | **100%** |
| 图谱/辩论/舆情 | 0% |

---

## 四、前端"核心alpha来源"伪装审计

前端页面暗示以下模块是策略核心，实际均不是:

| 前端展示 | 暗示 | 真实情况 |
|---------|------|---------|
| "图谱传播: 已接入, 4948节点/4805边" | 图谱是alpha来源 | **图谱选股劣于随机**(p=1.0) |
| "Agent辩论: 回测中跳过" | 辩论将会贡献alpha | **从未参与，且无独立验证** |
| "舆情检测: 降级" | 曾经有效现在降级 | **仅4个信号，从未证明有效** |
| "因子拆解: 有效/待验证" | 事件类型经过验证 | **badge基于15事件过拟合，84事件全FAIL** |
| 传播路径可视化(Cytoscape) | 展示信号传播逻辑 | **传播选股不如随机** |
| 置信度公式分解 | 公式精心设计 | **IC=-0.04(84事件), 置信度无预测力** |

---

## 五、模块对交易决策的真实贡献矩阵

```
                信号生成    入场    出场    仓位    方向    alpha贡献
事件类型规则      ★★★★★     ★★★★★    -       -      ★★★★★   ⚠️未验证
图谱传播(target)  ★★★★      ★★★★     -       -      -       ❌负贡献
5日固定持有       -         -       ★★★★★   -      -       ✅最佳持有期
akshare价格数据   -         ★★★★★    ★★★★★   -      -       ✅真实数据
hop过滤(>0)      ★★★       ★★★     -       -      -       ✅去源头有效
Agent辩论        -         -       -       -      -       ❌未参与
S10舆情          ★(事件源)  -       -       -      -       ❌未证明
置信度公式       -         -       -       ★★     -       ❌IC≈0
```

---

## 六、结论与建议

### 核心结论
> **当前系统不是"舆情+图谱+辩论→交易策略"系统。**
> **它实际上是: "事件类型硬编码规则 + 图谱BFS选股(不如随机) + 5日固定持有" 系统。**
> **LLM/辩论/舆情/置信度都是装饰，不影响任何交易参数。**

### 必须做的事
1. **承认现状**: 当前无可交易策略，三大模块均未贡献alpha
2. **停止伪装**: 前端不应暗示图谱/辩论/舆情是"核心模块"
3. **重新设计**: 如果要让这些模块真正有用，必须:
   - 让Agent辩论的consensus_direction **替代** 硬编码规则
   - 让图谱传播 **过滤掉HOLDS_SHARES** 并验证是否改善
   - 让舆情检测 **扩大事件覆盖** 至500+事件
4. **严格消融**: 每个模块必须独立证明对OOS Sharpe有正贡献才能保留
