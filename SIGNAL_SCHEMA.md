# SIGNAL_SCHEMA.md — 信号定义规格

**版本**: v2.0 (Phase 1)
**日期**: 2026-03-19
**变更**: T+1开盘入场 + 涨跌停过滤 + 事件标签清洗

---

## 一、通用信号字段

| 字段 | 类型 | 说明 | 来源 |
|------|------|------|------|
| `event_id` | string | 事件唯一ID | historical_events.json |
| `event_date` | string | 事件公告日(T+0) | historical_events.json |
| `event_type` | enum | 事件类型(12种) | historical_events.json |
| `source_code` | string | 事件主体股票代码 | historical_events.json |
| `source_name` | string | 事件主体名称 | historical_events.json |
| `target_code` | string | 信号标的股票代码 | 图谱传播 |
| `target_name` | string | 信号标的名称 | 图谱传播 |
| `hop` | int | 传播跳数(0=源头, 1-3=下游) | 图谱传播 |
| `relation_chain` | string | 传播关系链(e.g. COMPETES_WITH→SUPPLIES_TO) | 图谱传播 |
| `signal_direction` | enum | "long" 或 "avoid" | 规则映射 |
| `confidence` | float | 置信度(0.1-0.9) | 规则计算 |
| `entry_price` | float | **T+1开盘价**(可执行入场价) | akshare |
| `t0_close` | float | T+0收盘价(用于涨跌停判断) | akshare |
| `hit_limit_up` | bool | T+1开盘涨停 | 计算 |
| `hit_limit_down` | bool | T+1开盘跌停 | 计算 |
| `fwd_return_1d` | float? | T+1开盘→T+2收盘收益 | akshare |
| `fwd_return_3d` | float? | T+1开盘→T+4收盘收益 | akshare |
| `fwd_return_5d` | float? | T+1开盘→T+6收盘收益 | akshare |
| `fwd_return_10d` | float? | T+1开盘→T+11收盘收益 | akshare |
| `fwd_return_20d` | float? | T+1开盘→T+21收盘收益 | akshare |
| `reacted` | bool | 主回报是否超过2%阈值 | 计算 |
| `alpha_type` | string | "已反应" / "未反应(信息差)" / "事件源头" | 计算 |

---

## 二、入场规则(v2.0)

```
入场时间 = T+1 开盘
入场价格 = T+1 开盘价 (akshare "开盘" 列)
涨跌停过滤:
  - long方向 + T+1涨停开盘 → 不入场(买不进)
  - avoid方向 + T+1跌停开盘 → 不入场(卖不出)
  - 涨跌停判定: |T+1开盘/T+0收盘 - 1| >= 9.8%
```

## 三、出场规则

```
出场时间 = T+1+N 收盘 (N=持有天数, 默认5个交易日)
出场价格 = T+1+N 收盘价 (akshare "收盘" 列)
无止盈止损 (v2.0暂不加入, Phase 2考虑)
```

## 四、收益计算

```
gross_return = exit_price / entry_price - 1
cost = 0.004 (40bps往返)

if direction == "long":
    net_return = gross_return - cost
    direction_adjusted_return = gross_return - cost
elif direction == "avoid":
    net_return = -gross_return - cost
    direction_adjusted_return = -gross_return - cost
```

## 五、三条策略主线信号定义

### S1: 竞争对手负面 → 板块连坐

```yaml
strategy_id: S1_SCANDAL_CONTAGION
universe: A股全市场
trigger:
  event_type: [scandal, policy_risk]
  hop: [1, 2, 3]  # 排除源头
  relation_chain: 必须包含 COMPETES_WITH
  # 排除纯HOLDS_SHARES链路
entry: T+1开盘
direction: avoid  # 板块连坐=负面传导
holding: 5D
exit: T+6收盘
cost: 40bps
涨跌停: 过滤T+1跌停(avoid卖不出)
```

### S2: 供应链冲击 → 上下游传播

```yaml
strategy_id: S2_SUPPLY_CHAIN_SHOCK
universe: A股全市场
trigger:
  event_type: [supply_shortage, policy_risk, price_adjustment]
  hop: [1, 2]  # 不超过2跳
  relation_chain: 必须包含 SUPPLIES_TO 或 CUSTOMER_OF
  # 排除 HOLDS_SHARES 和 COMPETES_WITH
entry: T+1开盘
direction: 规则映射(supply_shortage→long, policy_risk→avoid)
holding: 5D
exit: T+6收盘
cost: 40bps
```

### S3: 产品发布/技术突破 → 产业链扩散

```yaml
strategy_id: S3_PRODUCT_LAUNCH_DIFFUSION
universe: A股全市场
trigger:
  event_type: [product_launch, technology_breakthrough, cooperation]
  hop: [1, 2]
  relation_chain: 必须包含产业链关系(SUPPLIES_TO/CUSTOMER_OF/COOPERATES_WITH)
  # 排除 HOLDS_SHARES
entry: T+1开盘
direction: 规则映射(product_launch→avoid[利好出尽], cooperation→long)
holding: 5D
exit: T+6收盘
cost: 40bps
```

---

## 六、部署前必须验证

每条主线必须通过:
- [ ] 3窗口 Walk-Forward OOS Sharpe ≥ 1.0
- [ ] 每窗口 OOS 胜率 > 50%
- [ ] 2x成本(80bps)后 Sharpe ≥ 0.5
- [ ] 图谱选股 vs 随机选股 p < 0.10
- [ ] 消融: 移除图谱后 Sharpe 下降 ≥ 0.3
