# EVENT_BACKTEST_SPEC.md — 事件回测规格说明

**版本**: v2.0 (Phase 1)
**日期**: 2026-03-19

---

## 一、回测框架

### 1.1 回测类型
- **Level 1**: Event Study — 单信号级别收益统计
- **Level 2**: Portfolio Backtest — 组合级(Phase 2实现)

当前实现: Level 1 (Event Study)

### 1.2 时间线规范

```
事件发生(T+0)
  ├─ 盘前/盘中发布 → 信号生成于T+0收盘后
  ├─ 盘后发布 → 信号生成于T+0收盘后 (等同)
  ├─ 非交易日发布 → 等待下一交易日
  └─ entry_price = T+1 开盘价

T+1: 入场(开盘价)
T+2~T+N: 持有期
T+N+1: 出场(收盘价), N=持有天数(默认5)
```

### 1.3 交易成本
| 项目 | 默认值 | 压力测试 |
|------|--------|---------|
| 单边佣金 | 0.03% (万三) | 0.05% |
| 印花税(卖出) | 0.05% | 0.05% |
| 滑点 | 0.10% | 0.20% |
| **往返总成本** | **0.40%** (40bps) | **0.80%** (80bps) |

### 1.4 涨跌停规则
```python
# A股涨跌停判定
limit_pct = 0.10  # 主板±10%
# T+1开盘涨停 → long信号不可执行
# T+1开盘跌停 → avoid信号不可执行
# 判定: |T+1_open / T+0_close - 1| >= 9.8%
```

### 1.5 信号冲突处理(v2.0暂不处理)
- 同一标的同一日多个信号 → 取confidence最高
- 已持仓标的新信号 → 忽略(不加仓不反向)
- (Phase 2 实现)

---

## 二、事件数据规范

### 2.1 事件类型枚举(12种)

| event_type | 中文 | 默认方向 | 84事件Sharpe | 清洗后事件数 |
|-----------|------|---------|-------------|------------|
| scandal | 丑闻/负面 | avoid | 3.90 ⭐ | 11 |
| cooperation | 合作 | long | 2.47 | 8 |
| price_adjustment | 提价/调价 | avoid | 2.25 | 5 |
| buyback | 回购 | avoid | 1.83 | 8 |
| ma | 并购重组 | long | 1.63 | 5 |
| supply_shortage | 供给短缺 | long | 1.39 | 4 |
| policy_risk | 政策风险 | avoid | 1.05 | 7 |
| product_launch | 产品发布 | avoid | 0.77 | 8 |
| management_change | 管理层变动 | avoid | -1.13 | 4 |
| technology_breakthrough | 技术突破 | avoid | -2.20 | 7 |
| earnings_surprise | 业绩超预期 | long | -3.45 | 9 |
| order_win | 大单中标 | long | 46.58(n=3) | 3 |

### 2.2 事件清洗规则(Phase 1已执行)

**删除条件**:
- 批量筛选类新闻(如"34只个股突破半年线") → 不是独立事件
- 同一事件的重复报道(保留最早一条)

**标签修正**:
- 标题含"回购"但type≠buyback → 修正为buyback
- 质量问题/负面新闻被标为buyback → 修正为scandal
- stock_code与标题主体不匹配 → 修正stock_code

**结果**: 84事件 → 79事件(删5, 修正5标签, 修正1个stock_code)

### 2.3 必须字段

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| event_id | string | ✓ | 唯一ID |
| title | string | ✓ | 事件标题 |
| type | EventType | ✓ | 12种之一 |
| stock_code | string | ✓ | 6位A股代码 |
| stock_name | string | ✓ | 股票简称 |
| event_date | YYYY-MM-DD | ✓ | 事件公告日 |
| summary | string | ✓ | 事件摘要 |
| impact_level | enum | ✓ | high/medium/low |
| source | string | 可选 | 数据来源 |
| clean_status | string | 可选 | reviewed/auto |

---

## 三、Walk-Forward 验证规范

### 3.1 方案
```
方法: 滚动时间序列分割
窗口数: 3 (最少)
训练比例: 60%
测试比例: 40%
排列: 按 event_date 升序

Split 0: 训练=事件[0:N*0.6], 测试=事件[N*0.6:N*0.6+N*0.4/3]
Split 1: 训练=事件[0:N*0.6+shift], 测试=下一段
Split 2: 训练=事件[0:N*0.6+2*shift], 测试=最后一段
```

### 3.2 准入门槛

| 指标 | 门槛 | 说明 |
|------|------|------|
| OOS Sharpe(平均) | ≥ 1.0 | 3窗口平均 |
| OOS 胜率(每窗口) | > 50% | 不允许任何窗口<50% |
| OOS MaxDD(每窗口) | < 30% | 事件级非组合级 |
| OOS 信号数(合计) | ≥ 30 | 统计显著性最低要求 |
| 图谱 vs 随机 | p < 0.10 | 放宽到10%显著性 |
| 2x成本后Sharpe | ≥ 0.5 | 80bps往返 |

### 3.3 消融实验矩阵

| 实验 | 对照 | 处理 | 比较指标 |
|------|------|------|---------|
| A1 | 图谱选股 | 随机同行业 | Sharpe差, p-value |
| A2 | 含HOLDS_SHARES | 仅产业链 | Sharpe, 胜率 |
| A3 | hop=1,2,3 | 各hop单独 | 各hop的Sharpe |
| A4 | 含源头(hop=0) | 去源头 | Sharpe |
| A5 | 当前方向规则 | 反向 | Sharpe(验证规则有效性) |
| A6 | 含earnings_surprise | 去掉 | 整体Sharpe(该类型Sharpe=-3.45) |
| A7 | 5D持有 | 3D/10D持有 | 最优持有期 |

---

## 四、v2.0 变更日志

| 变更 | 旧值 | 新值 | 原因 |
|------|------|------|------|
| 入场价 | T+0收盘价 | **T+1开盘价** | 消除point-in-time偏差 |
| 涨跌停 | 不处理 | **T+1开盘涨跌停过滤** | 可执行性 |
| 事件数 | 84(含错误标签) | **79(清洗后)** | 数据质量 |
| forward_return基准 | T+0收盘 | **T+1开盘** | 与入场价一致 |
| 新字段 | - | t0_close, hit_limit_up/down | 审计需要 |
