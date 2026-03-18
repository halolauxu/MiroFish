# MiroFish AStrategy

## 底层要求
- **必须使用中文回复**

## Mission
目标：推进 A 股量化研究。核心 alpha 来自冲击传播链路（信息差），辅以传统因子。

## Architecture (v2)
```
PRIMARY:  冲击传播链路 (shock_pipeline.py)
          事件 → 图谱传播 → Agent辩论 → 未反应检测 → Alpha
SECONDARY: S07 图谱多因子 (传统因子辅助)
TERTIARY:  S10 舆情模拟 (交叉验证)
```

## Current status
- 冲击链路: 历史回测验证通过 (Iteration 10)
- **回测 Sharpe: +1.49 (含TC 0.3%)**
- **回测胜率: 67.3% (全部) / 81.2% (3跳)**
- 历史事件数据库: 15个事件, 98个信号
- 信息差Alpha: 未反应信号 Sharpe=2.31
- 供应链相关边: 251条
- S07 OOS Sharpe: +0.56 (含TC)

## Key files
- **shock_pipeline.py** ← 核心链路 (含 run_historical 回测方法)
- **run_shock_backtest.py** ← 历史事件回测脚本
- **.data/historical_events.json** ← 历史事件数据库
- alpha_factory.py (v2, 冲击链路为主线)
- graph/topology.py (含 propagate_shock)
- strategies/s10_sentiment_simulation.py (含分歧度+图谱注入)
- strategies/s07_graph_factors.py
- strategies/s01_supply_chain.py
- graph/local_store.py
- run_backtest.py
- backtest/evaluator.py

## Research rules
1. 一次只攻一个瓶颈
2. 每轮只允许 1-2 个最小改动
3. 先提出假设，再改代码
4. 改完必须跑验证
5. 必须记录本轮结果
6. 不允许无关重构
7. 不允许跳过回测审计
8. 连续 3 轮无改进，必须切换方向

## Current bottlenecks
1. 源头信号胜率仅 28.6%（需 Agent 辩论纠偏或过滤）
2. 历史事件仅 15 个，需扩展至 30+ 验证结论稳定性
3. 图谱节点名与公司名不匹配导致部分传播路径丢失
4. 供应链边可继续扩展(目标500+)
5. 东方财富 API 限流影响 enrichment

## A股方向映射 (Iteration 10 回测验证)
- 负面事件(scandal/policy_risk/management_change) → avoid (板块连坐)
- 利好事件(product_launch/tech/buyback/price_adj) → avoid (利好出尽)
- 合作/业绩/供应短缺 → long (少数真正利好)
