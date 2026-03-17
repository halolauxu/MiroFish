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
- 冲击链路: 端到端可运行，已验证 (Iteration 8)
- S07 OOS Sharpe: +0.56 (含TC)
- 胜率: 62.5%
- 图谱覆盖率: 100% (7/7因子非零)
- Line C 信号: 4 (从0到有)
- Agent 分歧度: 0.30 (中等, 已输出)

## Key files
- **shock_pipeline.py** ← 核心链路（新）
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
1. 供应链/合作边只有124条，图谱传播路径覆盖率有限
2. S10 多轮模拟 Round 2/3 Agent 匹配率不足
3. 冲击链路无历史回测框架（需历史事件数据库）
4. 东方财富 API 限流影响 enrichment
