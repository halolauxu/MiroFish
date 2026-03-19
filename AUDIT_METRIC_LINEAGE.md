# AUDIT_METRIC_LINEAGE.md — 页面指标数据血缘审计

**审计日期**: 2026-03-19
**审计范围**: research-platform 前端所有展示指标
**审计结论**: 页面存在**两套数据源严重矛盾**，前端展示的是15事件/98信号的旧数据，而后端已完成84事件/529信号的完整验证且结论为 **FAIL**

---

## 一、核心矛盾：两份回测报告的致命冲突

| 项目 | 前端展示数据(旧) | 后端真实验证(新) | 冲突严重性 |
|------|-----------------|-----------------|-----------|
| **数据源文件** | `shock_wf_signals_20260318.json` (00:22生成) | `full_validation_signals_20260318_2102.csv` (21:02生成) | - |
| **事件数** | 15 (仅EVT001-015) | **84** (含EVT_M/AK系列) | 🔴致命 |
| **信号数** | 98 | **529** | 🔴致命 |
| **全样本 Sharpe** | 2.11 | **0.47** | 🔴致命 |
| **OOS Sharpe** | 3.06 | **-0.07** | 🔴致命 |
| **OOS 胜率** | 82.6% | **53.7%** | 🔴致命 |
| **最大回撤** | 16.13% | **-96.65%** | 🔴致命 |
| **WF准入** | "通过" | **FAIL(全部不通过)** | 🔴致命 |
| **图谱vs随机** | 未做 | p=1.0 (图谱**劣于**随机) | 🔴致命 |
| **未反应假说** | 看似成立 | **证伪** (未反应Sharpe=-0.23 < 已反应0.86) | 🔴致命 |

**结论: 前端页面展示的所有"策略有效"结论，在大样本验证后全部被推翻。15事件/98信号是严重的小样本过拟合。**

---

## 二、逐项指标血缘追踪

### A. 首页仪表盘指标

| # | 页面显示指标 | 显示值 | 数据来源 | 计算路径 | 真实回测artifact | 是否可复现 | 判定 |
|---|------------|--------|---------|---------|-----------------|-----------|------|
| 1 | 信号数 | 98 | `shock_wf_signals_20260318.json` | `signals.length` | ✓ 00:22产出 | ✓ | ⚠️ STALE_DATA — 已被529信号版本取代 |
| 2 | 事件数 | 15→18(含AK) | `historical_events.json` | `events.length` | ✓ | ✓ | ⚠️ 文件已扩充至84事件，但信号未重跑 |
| 3 | 图谱节点数 | 4948 | **硬编码在page.tsx:70** | 无动态计算 | `supply_chain.json`实际值=4948 | ✓当前 | ⚠️ HARDCODED — 图变则失效 |
| 4 | 图谱边数 | 4805 | **硬编码在page.tsx:70** | 无动态计算 | `supply_chain.json`实际值=4805 | ✓当前 | ⚠️ HARDCODED |
| 5 | 因子拆解-Sharpe | 各值 | 前端聚合自98信号 | `computeSliceStats()` | 底层数据真实 | ✓ | ⚠️ STALE_DATA — 基于15事件的过拟合子集 |
| 6 | 因子拆解-胜率 | 各值 | 前端聚合自98信号 | `computeSliceStats()` | 底层数据真实 | ✓ | ⚠️ STALE_DATA |
| 7 | 因子拆解-判断(有效/无效) | 基于阈值 | **前端硬编码规则** | `sharpe>1.5&&n>=5`→有效 | ❌无独立验证 | N/A | 🔴 FAKE_UI_DATA — 阈值分类无OOS验证，基于已被推翻的子集 |
| 8 | Scandal Sharpe | ~4.42 | 前端聚合自98信号 | `computeSharpe(scandal过滤)` | 部分真实 | ✓ | ⚠️ UNVERIFIED — 84事件验证中scandal Sharpe=3.90(67信号)，前端值基于9信号过拟合 |
| 9 | 组合交易统计-84笔 | 84 | 前端聚合 | `signals.filter(hop>0&&fwd!=null).length` | ✓ | ✓ | ⚠️ STALE_DATA — 实际应为463笔(84事件hop>0) |
| 10 | 组合-胜率79.8% | 79.8% | 前端聚合 | `computePortfolioStats()` | ✓ | ✓ | 🔴 MISLEADING — 84事件OOS胜率仅53.7% |
| 11 | 组合-盈亏比 | 2.82 | 前端聚合 | `computePortfolioStats()` | ✓ | ✓ | 🔴 MISLEADING — 84事件OOS盈亏比0.97 |
| 12 | IS vs OOS Sharpe | IS=1.80/OOS=3.06 | 解析`shock_wf_backtest_20260318.md` | `parseWFReport()` | ✓ 15事件版本 | ✓ | 🔴 STALE_DATA — 84事件版本OOS=-0.07 |
| 13 | 关系质量分析 | 各值 | 前端聚合自98信号 | `computeSliceStats(按relation_chain)` | ✓ | ✓ | ⚠️ STALE_DATA |
| 14 | 模块状态-Agent辩论 | "回测中跳过" | 前端硬编码逻辑 | `divergence>0`检查 | ✓ 真实反映 | N/A | ✅ 真实 — 确实跳过 |
| 15 | 模块状态-舆情(S10) | "降级" | **硬编码** | 无计算 | N/A | N/A | ⚠️ 状态正确但属于UI文案 |

### B. 事件详情页指标

| # | 指标 | 来源 | 判定 |
|---|------|------|------|
| 16 | 事件信号数 | `loadSignalsByEvent()` 从98信号过滤 | ⚠️ STALE_DATA |
| 17 | 事件胜率 | 前端聚合 `isCorrect()` | ⚠️ STALE_DATA |
| 18 | 事件超额收益 | `excess_5d`字段聚合 | ⚠️ STALE_DATA — 84事件版5D超额平均=-0.0042(负值！) |
| 19 | Hop分布 | 前端计数 | ✅ 真实(来源正确,但基于旧数据) |
| 20 | 交易流水-入场价 | `entry_price`字段 | ✅ 真实 — 来自akshare实盘数据 |
| 21 | 交易流水-盈亏% | `computeTradeInfo()` | ✅ 计算逻辑正确 |

### C. 信号详情页指标

| # | 指标 | 来源 | 判定 |
|---|------|------|------|
| 22 | 方向推断规则 | DIRECTION_RULES硬编码 | ⚠️ 规则本身是代码定义，但规则有效性未经84事件OOS验证 |
| 23 | 置信度公式 | 公式系数硬编码(0.4/0.1/0.5) | ⚠️ 系数来源于代码，IC=-0.0404(84事件)→置信度预测力为零 |
| 24 | 辩论状态 | divergence字段(全部=0.0) | ✅ 真实 — 辩论确实被跳过 |
| 25 | 关系链解释 | relation_chain字段 | ✅ 真实 |

---

## 三、判定汇总

| 判定类别 | 数量 | 说明 |
|---------|------|------|
| 🔴 MISLEADING / FAKE_UI_DATA | 4 | 基于15事件过拟合子集得出"有效"结论，大样本已证伪 |
| ⚠️ STALE_DATA | 10 | 数据来源真实但已过期，84事件版本结果完全不同 |
| ⚠️ HARDCODED | 3 | 硬编码数值或状态文案 |
| ✅ 真实且当前有效 | 8 | 入场价、关系链、辩论状态等结构性字段 |

---

## 四、关键发现

### 发现1: 小样本过拟合 (最严重)
- 15事件→98信号: Sharpe=2.11, 胜率=72.5%
- 84事件→529信号: Sharpe=0.47, 胜率=56.3%
- OOS验证: Sharpe从3.06崩溃到**-0.07**
- **这意味着页面上展示的所有"策略结论"都是虚假的**

### 发现2: 图谱传播无alpha
- 84事件消融实验: 图谱Sharpe=0.47 vs 随机Sharpe=0.66
- 图谱传播选股**劣于**随机同行业选股(p=1.0)
- 图谱边、节点数虽然真实(4948/4805)，但**图谱传播本身不产生alpha**

### 发现3: 信息差假说被证伪
- 假说: 未反应标的有信息差→alpha
- 84事件结果: 未反应Sharpe=-0.23，已反应Sharpe=0.86
- **完全相反**: 已反应标的反而更好（可能是动量效应）

### 发现4: 前端数据源未同步
- `historical_events.json`: 84事件(已扩充)
- `shock_wf_signals_20260318.json`: 98信号(仅15事件子集)
- `full_validation_signals_20260318_2102.csv`: 529信号(84事件)
- **前端读取的是旧版98信号，而84事件验证已在同一天晚上完成**

### 发现5: Agent辩论完全未参与
- 所有98条信号: divergence=0.0 (skip_debate=True)
- conviction只有4个离散值: {0.25, 0.3, 0.4, 1.0}
- debate_summary为空字符串或"事件源头"
- **辩论模块在回测中完全不存在**

---

## 五、价格数据验证

| 项目 | 结论 |
|------|------|
| 数据源 | akshare (新浪API) `stock_zh_a_daily()` |
| 真实性 | ✅ 真实A股日线数据(前复权) |
| 入场价 | event_date当日或次一交易日收盘价 |
| 远期收益 | T+1/3/5/10/20日真实收益率 |
| 基准 | 沪深300 `ak.stock_zh_index_daily(symbol='sh000300')` |
| 交易成本 | 40bps(0.4%往返) — 已扣除 |
| 涨跌停 | ❌ 未处理 |
| 流动性 | ❌ 未处理(假设无限流动性) |
| T+1开盘入场 | ❌ 使用收盘价而非T+1开盘价 |

---

## 六、METRIC_LINEAGE.csv 数据

```csv
metric_id,page,display_value,source_file,compute_script,artifact_path,sample_period,is_real_backtest,is_mock,is_reproducible,status
M01,homepage,98,shock_wf_signals_20260318.json,loader.ts:signals.length,reports/shock_wf_signals_20260318.json,15events,YES,NO,YES,STALE_DATA
M02,homepage,2.11(Sharpe),shock_wf_signals_20260318.json,loader.ts:computeSharpe(),reports/shock_wf_backtest_20260318.md,15events,YES,NO,YES,STALE_DATA
M03,homepage,72.5%(胜率),shock_wf_signals_20260318.json,loader.ts:computeSliceStats(),reports/shock_wf_backtest_20260318.md,15events,YES,NO,YES,STALE_DATA
M04,homepage,3.06(OOS Sharpe),shock_wf_backtest_20260318.md,loader.ts:parseWFReport(),reports/shock_wf_backtest_20260318.md,15events/5OOS,YES,NO,YES,STALE_DATA
M05,homepage,有效/无效badge,N/A(前端规则),page.tsx:sharpe>1.5,N/A,N/A,NO,YES(规则硬编码),N/A,FAKE_UI_DATA
M06,homepage,4948节点,hardcoded,page.tsx:70,supply_chain.json,N/A,NO(hardcoded),YES,N/A,HARDCODED
M07,homepage,Scandal ~4.42,shock_wf_signals_20260318.json,page.tsx:computeSharpe(filter),N/A,9signals,YES(小样本),NO,YES,STALE_DATA
M08,homepage,84笔交易,shock_wf_signals_20260318.json,loader.ts:filter(hop>0),reports/shock_wf_signals_20260318.json,15events,YES,NO,YES,STALE_DATA
M09,homepage,79.8%(组合胜率),shock_wf_signals_20260318.json,loader.ts:computePortfolioStats(),N/A,15events,YES,NO,YES,MISLEADING
M10,homepage,IS=1.80/OOS=3.06,shock_wf_backtest_20260318.md,loader.ts:parseWFReport(),reports/shock_wf_backtest_20260318.md,15events,YES,NO,YES,STALE_DATA
M11,event,各事件胜率,shock_wf_signals_20260318.json,loader.ts:computeEventStats(),N/A,15events,YES,NO,YES,STALE_DATA
M12,signal,divergence=0.0,shock_wf_signals_20260318.json,直接显示,N/A,ALL,YES,NO,YES,REAL
M13,signal,entry_price,shock_wf_signals_20260318.json,直接显示,N/A,ALL,YES(akshare),NO,YES,REAL
M14,signal,relation_chain,shock_wf_signals_20260318.json,直接显示,N/A,ALL,YES,NO,YES,REAL
```

---

## 七、整改建议（按优先级）

### P0 — 立即停止展示
1. 停止将15事件结果包装为"策略结论"
2. 移除所有"有效"/"待验证"的badge判定
3. 移除OOS Sharpe=3.06的展示（已被-0.07取代）

### P1 — 数据源切换
1. 前端应读取`full_validation_signals_20260318_2102.csv`(529信号/84事件)
2. 前端应读取`full_validation_20260318_2102.md`(真实WF结论)
3. 硬编码的4948/4805应改为动态读取

### P2 — 回测框架补全
1. 入场价应改为T+1开盘价（当前用event_date收盘价=未来函数）
2. 增加涨跌停过滤
3. 增加流动性约束
4. 走Forward用3窗口rolling而非单次split
