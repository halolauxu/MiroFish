# AStrategy Sharpe/胜率/一致性达标路线图

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将三条产线从当前各自为战、Sharpe 不达标的状态，推进到闭环协同、OOS Sharpe > 1.0。

**Architecture:** 分 6 个阶段推进。Phase 1-2 修基础（数据+度量），Phase 3 做消融实验验证每个能力的独立价值，Phase 4 优化已验证有效的能力，Phase 5 构建闭环协同，Phase 6 OOS 验证。阶段间严格串行。

**Tech Stack:** Python, pandas, akshare, LocalGraphStore, LLM (qwen-plus), 无新依赖

---

## 一、愿景：三个核心能力各自应该做什么

### 当前状态（各自为战）

```
图谱 ──→ 静态因子 ──→ S07 composite score
舆情 ──→ 事件检测 ──→ S10 独立信号
Agent ──→ 单轮投票 ──→ S10 加权平均
                          ↓
                    Alpha Factory (拼接，无优化)
```

### 目标状态（闭环协同）

```
           ┌──── 舆情检测事件 ────┐
           │   (信息差检测器)       │
           ▼                      │
      图谱推导传播路径              │
      (A出事 → 谁被波及？)          │
           │                      │
           ▼                      │
      Agent 辩论定价               │
      (分歧本身 = alpha)           │
           │                      │
           ▼                      │
      传播路径上的分歧信号簇         │
           │                      │
           ▼                      │
      回测验证 → 反馈 ─────────────┘
```

### 每个能力的正确定位

| 能力 | 当前用法（错） | 正确用法 | 核心价值 |
|------|--------------|---------|---------|
| **图谱** | PageRank 等静态因子 | 传播路径推导：A 出事 → 谁被波及 | 结构化因果推理 |
| **舆情** | 事件 → 情绪分数 | 信息差检测：市场还没消化的信息 | 时间优势 |
| **Agent 辩论** | 6 个 agent 投票取平均 | 分歧挖掘：机构-散户分歧 = alpha | 行为金融 alpha |

### 一个完整闭环的例子

```
1. 舆情检测到："台积电涨价 10%"
2. 图谱推导：台积电 → SUPPLIES_TO → [苹果, 英伟达]
             → COMPETES_WITH → 国内替代 [中芯国际, 华虹半导体]
3. Agent 辩论：
   - 量化基金: "中芯 PE 太高，不追"
   - 游资: "短线博弈中芯"
   - 价值投资者: "华虹估值合理，买入"
   - 散户: "追涨中芯"
4. 信号：华虹 long（机构偏好 + 散户忽略 = 信息差）
         中芯 neutral（机构-散户分歧大但方向不一致）
```

---

## 二、现状诊断

### S07 因子覆盖率 (quick mode, 30 stocks)

| 图因子 | 非零覆盖率 | 原因 | 影响 |
|--------|-----------|------|------|
| supply_chain_centrality | 40.0% | 仅 63 条 SUPPLIES_TO 边 | 有效但稀疏 |
| betweenness_centrality | 46.7% | 基于供应链+竞争网络 | 有效 |
| institution_concentration | **0.0%** | 名称匹配 BUG | **数据管线 BUG** |
| concept_heat | 50.0% | 58 条 RELATES_TO_CONCEPT | 有效 |
| event_exposure | **0.0%** | 图中无 Event 节点 | **缺失数据源** |
| industry_leadership | **0.0%** | API 回退失败 | **数据管线 BUG** |
| peer_return_gap | 90.0% | 行情 API | 良好 |

### 回测系统缺陷

| 缺陷 | 影响 | 严重度 |
|------|------|--------|
| 无交易成本 | Sharpe 虚高 | 高 |
| 无真实 OOS | 过拟合风险未知 | 高 |
| 权重手动设定 | 未针对 alpha 优化 | 中 |
| 三线仅拼接 | 无协同增益 | 中 |

---

## 三、路线图总览

```
Phase 1: 修复数据管线             → 覆盖率 >60%, 6/7 因子非零
Phase 2: 加入交易成本             → 度量真实化, 建立可信基线
Phase 3: 消融实验                 → 验证图谱/舆情/Agent各自的独立alpha
Phase 4: 优化已验证有效的能力      → 单线 Sharpe >0.5
Phase 5: 构建闭环协同             → 多线 Sharpe >0.8, 分歧信号有效
Phase 6: OOS 验证 + 最终调优      → OOS Sharpe >1.0, 胜率 >55%
```

每个 Phase 完成后必须：
1. 跑回测验证
2. 记录到 research_log.md
3. 与前一轮对比，确认改善

---

## Phase 1: 修复数据管线 (预计 3 轮迭代)

**目标：** 6/7 图因子非零，整体覆盖率 >60%

**原则：** 只修数据获取层，不改因子逻辑或信号逻辑

### Task 1.1: 修复 institution_concentration (优先级最高)

**问题根因：** `_compute_institution_concentration()` 通过 HOLDS_SHARES 边计数机构。4,418 条边存在（source=`inst::N`, target=股票代码），但 `_build_code_name_maps` 通过 `target_name` 做查找——如果 `target_name` 存的是代码字符串而 `name_to_code` 只映射中文名→代码，则匹配失败。需先运行诊断脚本确认实际失败点，再修复。

**Files:**
- Investigate: `astrategy/strategies/s07_graph_factors.py` — `_compute_institution_concentration()` (line ~368)
- Investigate: `astrategy/graph/local_store.py` — 图数据加载层
- Investigate: `astrategy/.data/local_graph/supply_chain.json` — HOLDS_SHARES 边字段内容
- Possibly fix: `astrategy/expand_graph.py` — 边写入方向

- [ ] **Step 1: 诊断边方向和字段内容**

```bash
python3 -c "
import json
with open('astrategy/.data/local_graph/supply_chain.json') as f:
    g = json.load(f)
holds = [e for e in g['edges'] if e['relation'] == 'HOLDS_SHARES']
print(f'Total HOLDS_SHARES: {len(holds)}')
print('Sample edges (first 5):')
for e in holds[:5]:
    print(f'  {e[\"source\"]} -> {e[\"target\"]} ({e.get(\"source_name\",\"?\")} -> {e.get(\"target_name\",\"?\")})')
"
```

- [ ] **Step 2: 读取 `_compute_institution_concentration` 方法，确认查询逻辑是按 source 还是 target 匹配**
- [ ] **Step 3: 修复匹配逻辑** (预计改 3-5 行)
- [ ] **Step 4: 验证非零**

```bash
python3 -c "
from astrategy.strategies.s07_graph_factors import GraphFactorsStrategy
s = GraphFactorsStrategy()
codes = ['601318','600036','000858','002594','300750']
gf = s.compute_graph_factors(codes)
print('institution_concentration:')
print(gf['institution_concentration'])
print(f'Non-zero: {(gf[\"institution_concentration\"].abs() > 1e-9).sum()}/{len(gf)}')
"
```

- [ ] **Step 5: 跑 S07 quick 回测，记录覆盖率变化**
- [ ] **Step 6: 追加结果到 research_log.md**

---

### Task 1.2: 修复 industry_leadership

**问题根因：** 节点无 `market_cap` 属性，`_compute_industry_leadership()` 回退到 akshare API (`ak.stock_individual_info_em`)，但 API 限流或格式变化导致失败。

**Files:**
- Investigate: `astrategy/strategies/s07_graph_factors.py` — `_compute_industry_leadership()` 方法
- Possibly fix: API 调用逻辑或回退策略

- [ ] **Step 1: 定位失败点** — 加临时 debug 日志
- [ ] **Step 2: 单独测试 akshare API 调用**
- [ ] **Step 3: 修复获取逻辑** (预计改 5-10 行)
- [ ] **Step 4: 验证非零覆盖率 > 10%**
- [ ] **Step 5: 跑回测，记录结果**

---

### Task 1.3: 补充 Event 节点 (event_exposure)

**问题根因：** `expand_graph.py` 没有创建 Event 节点。

**Files:**
- Modify: `astrategy/expand_graph.py` — 增加事件节点构建（无 `build_graph.py`，所有图构建在此）
- Reference: `astrategy/data_collector/news.py` — 获取新闻数据

- [ ] **Step 1: 评估 ROI** — event_exposure 权重仅 0.5，如果 Task 1.1 + 1.2 已推到 >60% 覆盖率，可跳过
- [ ] **Step 2: 如果 ROI 足够，用 NewsCollector 获取近 30 天新闻，创建 Event 节点**
- [ ] **Step 3: 为相关股票创建 TRIGGERS 边**
- [ ] **Step 4: 验证 event_exposure 非零**

---

### Phase 1 门槛

| 指标 | 当前 | 目标 |
|------|------|------|
| 非零因子数 | 4/7 | >= 6/7 |
| 整体图覆盖率 | 33% | > 60% |
| Sharpe | -0.15 | 改善即可 |

**如果覆盖率未达 50%：** 停止，重新评估图谱数据源本身是否可行。

---

## Phase 2: 加入交易成本 (1 轮迭代)

**目标：** 所有后续 Sharpe/胜率数字反映真实可实现收益

### Task 2.1: 在回测评估中加入交易成本

**Files:**
- Modify: `astrategy/run_backtest.py` — `RollingEvaluator.evaluate_signal_rolling()` (~258-326 行)
- Modify: `astrategy/backtest/evaluator.py` — `evaluate_signal()` 方法

**A 股往返成本 ≈ 0.3%**（佣金 0.025%×2 + 印花税 0.05% + 滑点 0.05%×2）

- [ ] **Step 1: `actual_return` 计算后减去往返成本**

```python
_ROUND_TRIP_COST = 0.003  # 0.3%
actual_return = actual_return - _ROUND_TRIP_COST
```

注意：不同 holding_period 下成本对 alpha 的侵蚀不同（10d 更严重），Phase 6 测试不同 holding_period 时需留意。

- [ ] **Step 2: 同步修改 evaluator.py**
- [ ] **Step 3: 跑回测，记录 TC-adjusted baseline**
- [ ] **Step 4: 追加到 research_log.md**

### Phase 2 门槛

| 指标 | 目标 |
|------|------|
| 回测包含交易成本 | Yes |
| TC-adjusted 基线已记录 | Yes |

---

## Phase 3: 消融实验 (关键决策点)

**目标：** 用 ablation test 回答三个根本问题，决定后续投入方向

这是整个路线图最重要的 Phase。在优化任何东西之前，必须先知道每个能力到底有没有用。

### Task 3.1: 图谱消融 — "图谱比没有图谱好多少？"

**方法：** 跑两组 S07 回测，对比有图因子 vs 仅传统因子

- [ ] **Step 1: 创建 "no-graph" 权重配置**

```python
no_graph_weights = {
    "supply_chain_centrality": 0, "betweenness_centrality": 0,
    "institution_concentration": 0, "concept_heat": 0,
    "event_exposure": 0, "industry_leadership": 0, "peer_return_gap": 0,
    # 保留传统因子
    "momentum_20d": 1.0, "reversal_5d": 1.0, "volatility_20d": -1.0,
    "turnover_rate": 1.0, "pe_percentile": -1.0, "roe": 1.0,
}
```

- [ ] **Step 2: 跑 no-graph S07 回测**

```bash
python3 -c "
from astrategy.strategies.s07_graph_factors import GraphFactorsStrategy
# ... 用 no_graph_weights 初始化，跑 quick 回测
"
```

- [ ] **Step 3: 跑 full S07 回测（含图因子）**
- [ ] **Step 4: 计算 Sharpe 差（graph_delta_sharpe）**

**达标判据：**

| 结果 | 判定 | 下一步 |
|------|------|--------|
| graph_delta_sharpe > 0.2 | 图谱有显著增量 alpha | Phase 4 继续优化图因子权重 |
| 0 < graph_delta_sharpe < 0.2 | 图谱有微弱增量 | 保留但降低投入，优先优化传统因子 |
| graph_delta_sharpe <= 0 | 图谱无增量甚至有害 | 停止图因子开发，图谱仅用于传播路径（Phase 5） |

- [ ] **Step 5: 记录消融结果**

---

### Task 3.2: 舆情消融 — "舆情信号比随机好多少？"

**方法：** 评估 S10 信号的独立预测力和时效性

- [ ] **Step 1: 跑 S10 quick 回测**

```bash
python3 astrategy/run_backtest.py --strategies S10 --quick
```

- [ ] **Step 2: 计算高置信度信号（confidence > 0.6）的胜率**
- [ ] **Step 3: 评估 S10 与 S07 信号的相关性（Spearman 相关系数）**

**达标判据：**

| 结果 | 判定 | 下一步 |
|------|------|--------|
| S10 胜率 > 55% 且与 S07 相关性 < 0.3 | 独立有效 | Phase 5 纳入闭环 |
| S10 胜率 > 55% 但与 S07 相关性 > 0.5 | 有效但冗余 | 仅在 S07 无信号时使用 |
| S10 胜率 <= 50% | 无效 | 暂停 Line C 开发 |

- [ ] **Step 4: 记录消融结果**

---

### Task 3.3: Agent 分歧消融 — "分歧比共识更有价值吗？"

**方法：** 将 S10 的信号分为"高分歧"和"低分歧"两组，对比 Sharpe

- [ ] **Step 1: 读取 S10 最近的信号文件，提取各 agent 的反应**

```bash
python3 -c "
import json, glob
files = sorted(glob.glob('astrategy/.data/signals/sentiment_simulation/*.json'))
for f in files[-3:]:
    with open(f) as fh:
        data = json.load(fh)
    if isinstance(data, list):
        for sig in data:
            meta = sig.get('metadata', {})
            print(sig['stock_code'], sig['direction'],
                  'agents:', meta.get('agent_reactions', 'N/A'))
"
```

- [ ] **Step 2: 定义分歧度计算方式**

```python
def divergence_score(agent_reactions: dict) -> float:
    """分歧度 = 方向不一致的agent对数 / 总对数"""
    directions = [r['action'] for r in agent_reactions.values()]
    buy = directions.count('buy')
    sell = directions.count('sell')
    # 分歧度: 0 = 全部一致, 1 = 完全分裂
    return min(buy, sell) / max(len(directions) - 1, 1)
```

- [ ] **Step 3: 分组对比**

| 分组 | 条件 | 期望 |
|------|------|------|
| 高分歧组 | divergence > 0.3 | Sharpe 更高（分歧=错误定价=alpha） |
| 低分歧组 | divergence < 0.1 | Sharpe 较低（共识=已定价=无alpha） |

- [ ] **Step 4: 如果高分歧组 Sharpe > 低分歧组，验证假设成立**

**达标判据：**

| 结果 | 判定 | 下一步 |
|------|------|--------|
| 高分歧 Sharpe > 低分歧 + 0.2 | 分歧有显著 alpha | Phase 5 用分歧度作为信号权重 |
| 差距 < 0.2 | 分歧无显著增量 | Agent 辩论仅保留共识功能 |

- [ ] **Step 5: 记录消融结果**

---

### Phase 3 门槛与决策矩阵

Phase 3 完成后，根据消融结果决定后续路径：

| 图谱有效？ | 舆情有效？ | 分歧有效？ | 路径 |
|-----------|-----------|-----------|------|
| Yes | Yes | Yes | 最优路径：Phase 4 全面优化 → Phase 5 闭环 |
| Yes | Yes | No | Phase 4 优化图+舆情 → Phase 5 闭环但不用分歧信号 |
| Yes | No | * | Phase 4 只优化图因子 → 跳过 Phase 5 直接 Phase 6 |
| No | Yes | * | 切换方向：放弃 S07 多因子，转 S10 事件驱动为主 |
| No | No | * | 根本性反思：因子集合需要重新设计 |

**关键：不要在消融实验证明有效之前做任何优化。**

---

## Phase 4: 优化已验证有效的能力 (预计 2-3 轮迭代)

**目标：** 单线 Sharpe > 0.5（含 TC）

**前提：** Phase 3 消融实验已完成，知道哪些能力有效

### Task 4.1: 因子权重优化（如果图谱消融通过）

**Files:**
- Create: `astrategy/backtest/weight_optimizer.py`
- Use: `astrategy/strategies/s07_graph_factors.py` — `__init__` 已支持 `weights` 参数

- [ ] **Step 1: 确认 weight 注入可用**（`__init__` line 113-126 已支持）
- [ ] **Step 2: 逐因子单变量搜索**

```python
# 13 因子 × 5 候选值 = 65 次评估（非全网格）
for factor in all_factors:
    best_val, best_sharpe = current_weight, current_sharpe
    for candidate in [-1.5, -0.5, 0.0, 0.5, 1.5]:
        weights[factor] = candidate
        sharpe = quick_evaluate(weights)
        if sharpe > best_sharpe:
            best_val, best_sharpe = candidate, sharpe
    weights[factor] = best_val
```

- [ ] **Step 3: 对优化后权重跑 rolling backtest**
- [ ] **Step 4: 如果改善，替换 DEFAULT_WEIGHTS**
- [ ] **Step 5: 记录结果**

### Task 4.2: 调整 graph_scale 阈值（如果图覆盖率 40-50%）

- [ ] **Step 1: 检查 Phase 1 后的覆盖率**
- [ ] **Step 2: 如果 40-50%，尝试阈值从 0.50 调到 0.30**
- [ ] **Step 3: 对比回测**

### Task 4.3: 舆情信号质量优化（如果舆情消融通过）

- [ ] **Step 1: 在 S10 中增加 confidence 阈值过滤** — 只输出 confidence > 0.5 的信号
- [ ] **Step 2: 测试多轮模式 `use_multi_round=True`** — 对比单轮
- [ ] **Step 3: 记录结果**

### Phase 4 门槛

| 指标 | 当前 | 目标 |
|------|------|------|
| 最佳单线 Sharpe (含 TC) | 预计 < 0 | > 0.5 |
| 胜率 | 53.3% | > 53% (不恶化) |

---

## Phase 5: 构建闭环协同 (预计 2-3 轮迭代)

**目标：** 三线闭环协同，联合 Sharpe > 0.8

**前提：** Phase 4 最佳单线 Sharpe 已 > 0.5

### Task 5.1: 评估各线独立 Sharpe

- [ ] **Step 1: 分别跑 S01, S07, S10 回测，记录独立指标**
- [ ] **Step 2: 计算各线间的信号相关性**

### Task 5.2: 实现图谱传播路径信号（图谱的正确用法）

**核心改动：** 将图谱从"静态因子提供者"升级为"事件传播路径推导器"

**Files:**
- Modify: `astrategy/strategies/s07_graph_factors.py` 或新建辅助模块

- [ ] **Step 1: 实现传播路径查询**

```python
def find_impact_path(self, event_stock: str, graph) -> list[dict]:
    """给定事件股票，找到所有可能受影响的股票及路径"""
    # 1-hop: SUPPLIES_TO/CUSTOMER_OF 直接关联
    # 2-hop: 同行业竞争对手
    paths = []
    for edge in graph.edges:
        if edge.source == event_stock and edge.relation in ('SUPPLIES_TO', 'CUSTOMER_OF'):
            paths.append({
                'target': edge.target,
                'relation': edge.relation,
                'hop': 1,
                'expected_direction': 'same' if edge.relation == 'CUSTOMER_OF' else 'opposite'
            })
    return paths
```

- [ ] **Step 2: 将传播路径与 S10 事件信号关联**

当 S10 检测到某股票的事件 → 用图谱推导受影响股票 → 为传播路径上的股票生成衍生信号

- [ ] **Step 3: 验证衍生信号的 IC**

**达标判据：** 二阶冲击 IC > 0.05（当 A 跌 >3% 时，传播路径上的股票 T+1~T+5 收益与预测方向的相关性）

### Task 5.3: 实现分歧加权信号（Agent 辩论的正确用法）

**核心改动：** 不再用 agent 反应的加权平均，而是用分歧度作为信号强度

- [ ] **Step 1: 在 S10 信号中加入 divergence_score 字段**
- [ ] **Step 2: Alpha Factory 中按分歧度调整 confidence**

```python
# 高分歧 = 高 alpha 预期 → 提升 confidence
if divergence_score > 0.3:
    adjusted_confidence = confidence * (1 + divergence_score * 0.5)
# 全票通过 = 市场已定价 → 降低 confidence
elif divergence_score < 0.1:
    adjusted_confidence = confidence * 0.7
```

- [ ] **Step 3: 验证分歧加权信号 Sharpe > 原始信号**

### Task 5.4: 实现加权共识聚合（Alpha Factory 升级）

**Files:**
- Modify: `astrategy/alpha_factory.py` — 从拼接改为加权共识

- [ ] **Step 1: 按各线 Sharpe 分配权重（负 Sharpe 权重为 0）**

```python
clipped = {k: max(v, 0) for k, v in {'A': sharpe_a, 'B': sharpe_b, 'C': sharpe_c}.items()}
total = sum(clipped.values()) or 1.0
line_weights = {k: v / total for k, v in clipped.items()}
```

- [ ] **Step 2: 同股票多线信号取加权 confidence + 多数方向**
- [ ] **Step 3: 跑联合回测，对比最佳单线**
- [ ] **Step 4: 记录结果**

### Phase 5 门槛

| 指标 | 目标 |
|------|------|
| 联合 Sharpe (含 TC) | > 0.8 |
| 联合 Sharpe 增量 | > 最佳单线 Sharpe + 0.1（确认协同有增益） |
| 联合胜率 | > 54% |

---

## Phase 6: OOS 验证 + 最终调优 (预计 2 轮迭代)

**目标：** 在真实 OOS 条件下确认 Sharpe > 1.0

### Task 6.1: Walk-Forward 验证

**Files:**
- Create: `astrategy/backtest/walk_forward.py`

- [ ] **Step 1: 实现 expanding window 回测**

```
训练期: T-120 → T-60 (优化权重)
测试期: T-60 → T-30 (用优化后权重评估)
验证期: T-30 → T (最终 OOS 检验)
```

注意：Sharpe 的年化因子 (252/hold_days) 要随 holding_period 变化而调整。

- [ ] **Step 2: 跑 WFO，记录 OOS Sharpe**
- [ ] **Step 3: 如果 OOS Sharpe > IS Sharpe × 0.6，确认无严重过拟合**

### Task 6.2: 最终调优

- [ ] **Step 1: 微调 long/short 分位阈值**
- [ ] **Step 2: 测试不同 holding_period (10d vs 20d vs 30d)** — 注意 10d 下 TC 侵蚀更大
- [ ] **Step 3: 确认最终参数组合**
- [ ] **Step 4: 全量回测 (CSI800, 非 quick mode)**

### Phase 6 最终门槛

| 指标 | 目标 | 判定 |
|------|------|------|
| OOS Sharpe (含 TC) | > 1.0 | 最终目标 |
| 胜率 | > 55% | 必须达标 |
| 一致性 | > 50% | 必须达标 |
| 图覆盖率 | > 60% | 必须达标 |

---

## 四、每个能力的愿景级达标标准

工程指标（覆盖率、信号数）只是前提。真正的达标标准是 **alpha 指标**：

### 图谱达标标准

| 维度 | 指标 | 测试方法 | 达标线 |
|------|------|---------|--------|
| 增量 alpha | graph_delta_sharpe | Phase 3 消融: 有图 vs 无图 | > 0.2 |
| 传播预测力 | 二阶冲击 IC | A 跌 >3% 时，图谱预测的受影响股 T+1~T+5 收益相关性 | > 0.05 |
| 因子有效性 | 单因子 IC > 0 的数量 | 每个图因子单独算 IC | >= 4/7 |

### 舆情达标标准

| 维度 | 指标 | 测试方法 | 达标线 |
|------|------|---------|--------|
| 信号纯度 | 高置信度胜率 | confidence > 0.6 的信号 hit rate | > 60% |
| 独立贡献 | 与 S07 的相关性 | Spearman 相关系数 | < 0.3 |
| 时效性 | 信号→市场反应时滞 | 信号产出到标的 >1% 波动的时间差（需要 tick 数据，可后置） | 中位数 > 2h |

### Agent 辩论达标标准

| 维度 | 指标 | 测试方法 | 达标线 |
|------|------|---------|--------|
| 分歧价值 | 高分歧 vs 低分歧 Sharpe 差 | Phase 3 消融 | > 0.2 |
| 多轮收益 | 多轮 vs 单轮 Sharpe | 开启 multi_round=True 对比 | 多轮 > 单轮 + 0.1 |
| 角色区分度 | 各 agent 独立 IC | 单独评估每个 agent 的 IC | >= 2 个 agent IC > 0.03 |

---

## 五、执行纪律

1. **一次只攻一个 Task**
2. **每轮只改 1-2 个文件**
3. **改完必须跑回测验证**
4. **结果必须写入 research_log.md**
5. **连续 3 轮无改进 → 切换方向**
6. **不允许跳过 Phase（Phase N 未达标不得进入 Phase N+1）**
7. **不允许在修复数据管线时顺手改信号逻辑**
8. **Phase 3 消融实验期间不做任何优化——只观察**

---

## 六、如果路线图失败

### Phase 1 后覆盖率仍 < 50%
→ 停止，重新评估图谱数据源可行性。考虑从东方财富/同花顺爬取更完整的行业链数据。

### Phase 3 消融实验全部失败（图/舆情/分歧均无效）
→ 因子集合需要重新设计。可能需要：
- 新因子来源：北向资金、大单净流入、融资融券余额
- 或传统因子（momentum, reversal）在当前 A 股市场已失效
- 切换到纯事件驱动或纯机器学习框架

### Phase 4 后单线 Sharpe 仍 < 0.3
→ 放弃 S07 多因子框架，转向 S10 事件驱动为主线

### Phase 5 后联合 Sharpe < 单线 Sharpe
→ 协同增益不存在，放弃多线融合，聚焦最强单线
