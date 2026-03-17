# S07 Iteration 1: 剔除零值因子，提升 Sharpe

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 通过剔除数据稀疏导致全零的图因子，减少噪声，提升 S07 Sharpe ratio 从 0.88 → >1.0

**Architecture:** 在 `compute_composite_score` 中增加"零方差因子剔除"逻辑——如果某个图因子在当前 universe 中全为零（或非零占比 <10%），则自动跳过该因子的权重贡献，避免 z-score 归一化后产生虚假区分度。

**Tech Stack:** Python, pandas, 无新依赖

---

## 瓶颈分析

当前 S07 Sharpe = 0.88，根因是图谱数据稀疏：

| 图因子 | 非零边数 | 覆盖股票数 | 占 4698 股比例 |
|--------|---------|-----------|---------------|
| supply_chain_centrality | 63 SUPPLIES_TO | ~37 | 0.8% |
| concept_heat | 58 RELATES_TO_CONCEPT | ~50 | 1.1% |
| event_exposure | 0 TRIGGERS | 0 | 0% |
| institution_concentration | 4418 HOLDS_SHARES | ~4000+ | 85%+ |
| industry_leadership | 78 BELONGS_TO + API | ~大部分 | ~80% |

**问题：** `event_exposure` 全为零，`supply_chain_centrality` 和 `concept_heat` 覆盖率 <2%。z-score 归一化时，这些列只有极少数非零值会获得极端 z-score，其余全部为零——但 z-score 为零不代表"中性"，它代表"缺失数据被填充为零后恰好落在均值"。这引入了虚假信号噪声。

## 假设

**如果在 `compute_composite_score` 中跳过非零覆盖率 <10% 的图因子，那么：**
1. 消除 event_exposure（0%）、supply_chain_centrality（0.8%）、concept_heat（1.1%）的噪声贡献
2. 图因子权重集中在 institution_concentration 和 industry_leadership 这两个数据充足的因子上
3. Sharpe ratio 应该提升，因为信号噪声降低

**只改 1 个点：** `compute_composite_score` 方法中增加 per-factor 覆盖率检查。

---

## File Structure

- **Modify:** `astrategy/strategies/s07_graph_factors.py` — `compute_composite_score` 方法（约第 830-870 行）

---

### Task 1: 在 compute_composite_score 中增加零因子剔除

**Files:**
- Modify: `astrategy/strategies/s07_graph_factors.py:830-870`

- [ ] **Step 1: 定位修改点**

阅读 `compute_composite_score` 方法，确认 weighted composite 循环位于约第 852-859 行：

```python
# 当前代码（约第 852-859 行）:
composite = pd.Series(0.0, index=z_scored.index, dtype=float)
total_abs_weight = 0.0
for col in z_scored.columns:
    w = self._weights.get(col, 0.0)
    if col in _GRAPH_FACTOR_COLS:
        w = w * graph_scale
    composite += z_scored[col] * w
    total_abs_weight += abs(w)
```

- [ ] **Step 2: 添加 per-factor 覆盖率检查**

在 weighted composite 循环**之前**，计算每个图因子的非零占比，构建一个 skip set：

```python
# --- Per-factor sparsity filter ---
# Skip graph factors where <10% of stocks have non-zero raw values.
# These produce misleading z-scores (a handful of extreme values, rest at zero).
_COVERAGE_THRESHOLD = 0.10
skip_factors: set[str] = set()
for col in graph_cols:
    if col in combined.columns:
        nonzero_ratio = (combined[col].abs() > 1e-9).mean()
        if nonzero_ratio < _COVERAGE_THRESHOLD:
            skip_factors.add(col)
            logger.info(
                "Skipping sparse graph factor '%s' (%.1f%% non-zero, threshold %.0f%%)",
                col, nonzero_ratio * 100, _COVERAGE_THRESHOLD * 100,
            )
```

- [ ] **Step 3: 在权重循环中跳过 skip_factors**

修改 weighted composite 循环，跳过 skip set 中的因子：

```python
composite = pd.Series(0.0, index=z_scored.index, dtype=float)
total_abs_weight = 0.0
for col in z_scored.columns:
    if col in skip_factors:
        continue
    w = self._weights.get(col, 0.0)
    if col in _GRAPH_FACTOR_COLS:
        w = w * graph_scale
    composite += z_scored[col] * w
    total_abs_weight += abs(w)
```

- [ ] **Step 4: 验证改动正确性**

Run:
```bash
cd /Users/xukun/Documents/freqtrade/MiroFish && python -c "
from astrategy.strategies.s07_graph_factors import GraphFactorsStrategy
s = GraphFactorsStrategy()
print('Strategy created OK, weights:', len(s._weights))
"
```
Expected: 无报错，打印权重数量

- [ ] **Step 5: 运行 S07 快速回测**

Run:
```bash
cd /Users/xukun/Documents/freqtrade/MiroFish && python astrategy/run_backtest.py --strategies S07 --quick --no-llm
```

Expected output 中关注：
- 日志应显示 "Skipping sparse graph factor 'event_exposure'" 等跳过信息
- Sharpe ratio 对比基线 0.88

- [ ] **Step 6: 记录结果到 research_log.md**

在 `research_log.md` 追加 Iteration 1 结果：

```markdown
## Iteration 1
- Hypothesis: 剔除非零覆盖率 <10% 的图因子可减少噪声，提升 Sharpe
- Change: compute_composite_score 中增加 per-factor 覆盖率检查，跳过稀疏因子
- Files changed: strategies/s07_graph_factors.py
- Results:
  - S07 Sharpe: ??? (was 0.88)
  - Win rate: ??? (was 51.7%)
  - Consistency: ??? (was 30%)
  - Skipped factors: ???
- Conclusion: ???
- Next: ???
```

- [ ] **Step 7: Commit**

```bash
git add astrategy/strategies/s07_graph_factors.py research_log.md
git commit -m "exp: S07 skip sparse graph factors (<10% coverage) to reduce noise"
```

---

## 成功标准

| 指标 | 基线 | 目标 | 判定 |
|------|------|------|------|
| Sharpe ratio | 0.88 | > 0.95 | 主要目标 |
| Win rate | 51.7% | >= 51.7% | 不应恶化 |
| Consistency | 30% | >= 30% | 不应恶化 |

## 如果失败

如果 Sharpe 未提升或下降：
- 说明噪声源不在稀疏因子，而在权重分配或传统因子
- 下一步切换到调整 graph_scale 阈值（当前 50% → 尝试 30%）或重新标定权重
