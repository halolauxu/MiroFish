# AStrategy — A股策略研究平台 架构与进展文档

> 最后更新: 2026-03-16
> 用途: 在新机器上继续自动化研究时，将此文档作为上下文提供给 Claude

---

## 一、项目概述

AStrategy 是基于 MiroFish 群体智能预测引擎构建的 **A股多策略量化研究平台**。实现了 11 个独立交易策略 (S01-S11)，涵盖：
- 知识图谱传导 (供应链、事件传播)
- LLM 辅助分析 (财报解读、公告情绪、行业轮动)
- 纯量化因子 (机构持仓、分析师分歧、图谱多因子)
- 宏观传导 (景气度传导、情绪模拟)
- **MiroFish多轮群体模拟** (S10升级：3轮级联，图谱上下文注入)
- **投资叙事追踪** (S11：叙事生命周期定位，萌芽期Alpha)
- **动态图谱** (新闻/公告驱动的公司状态实时更新)

平台包含完整的数据采集 → 策略执行 → 信号聚合 → 回测验证 → 组合优化 流水线。

---

## 二、目录结构

```
astrategy/
├── config.py                    # 统一配置 (数据源/LLM/图谱/策略/存储)
├── ontology.yaml                # A股知识图谱 schema (10实体/12关系)
├── pyproject.toml               # 项目元数据 & 依赖
├── run_backtest.py              # 全策略回测入口 (含滚动回测)
├── run_demo.py                  # 快速验证脚本 (S07)
│
├── strategies/                  # 11个交易策略
│   ├── base.py                  # StrategySignal 数据模型 + BaseStrategy 抽象类
│   ├── s01_supply_chain.py      # 供应链传导 (需图谱)
│   ├── s02_institution.py       # 机构持仓博弈 (纯量化)
│   ├── s03_event_propagation.py # 事件传播 (需图谱)
│   ├── s04_earnings_surprise.py # 财报超预期 (LLM)
│   ├── s05_analyst_divergence.py# 分析师分歧 (纯量化)
│   ├── s06_announcement_sentiment.py # 公告情绪 (LLM)
│   ├── s07_graph_factors.py     # 图谱增强多因子 (纯量化)
│   ├── s08_sector_rotation.py   # 行业轮动 (LLM)
│   ├── s09_prosperity_transmission.py # 景气传导 (LLM)
│   ├── s10_sentiment_simulation.py   # MiroFish多轮情绪模拟 (LLM+图谱)
│   └── s11_narrative_tracker.py     # 叙事生命周期追踪 (LLM) ★新增
│
├── data_collector/              # 数据采集层
│   ├── market_data.py           # K线/实时行情/北向资金/资金流
│   ├── fundamental.py           # 财务摘要/十大持仓/基金持仓
│   ├── announcement.py          # 公司公告 (关键词过滤)
│   ├── news.py                  # 新闻/热点
│   ├── macro.py                 # 宏观指标 (PMI/CPI/GDP/M2/LPR)
│   ├── research.py              # 分析师评级/研报
│   └── em_fallback.py           # 东方财富API替代 (新浪源)
│
├── llm/                         # LLM 基础设施
│   ├── client.py                # OpenAI 兼容客户端 (重试/token估算)
│   ├── cache.py                 # SQLite 持久化缓存
│   ├── cost_tracker.py          # 成本追踪 (按策略/按日)
│   ├── batch_scheduler.py       # 并发批量调度
│   └── __init__.py              # 工厂函数: create_llm_client()
│
├── graph/                       # 知识图谱
│   ├── builder.py               # Zep Cloud 图谱构建
│   ├── local_store.py           # 本地JSON图谱 (Zep替代)
│   ├── dynamic_updater.py       # 动态状态更新 (新闻驱动) ★新增
│   ├── topology.py              # 纯Python拓扑分析 (中心性/社区)
│   └── updater.py               # 增量更新
│
├── aggregator/                  # 信号聚合
│   ├── signal_aggregator.py     # 多策略加权共识
│   └── portfolio_optimizer.py   # 组合优化 (风险约束)
│
├── scheduler/                   # 调度编排
│   ├── master_scheduler.py      # 主调度器
│   ├── daily_runner.py          # 日频 (S06/S07)
│   ├── weekly_runner.py         # 周频 (S05/S08)
│   └── event_runner.py          # 事件驱动 (S01/S03)
│
├── backtest/                    # 回测基础设施
│   ├── evaluator.py             # 信号评估 (命中率/夏普/IC)
│   └── freqtrade_bridge.py      # Freqtrade 格式转换
│
├── prompt_templates/            # LLM 提示词模板
│   ├── earnings_analysis.txt
│   ├── industry_rotation.txt
│   ├── prosperity_assessment.txt
│   ├── sentiment_analysis.txt
│   └── supply_chain_eval.txt
│
└── .data/                       # 数据缓存目录
```

---

## 三、策略分类与状态

### 按数据依赖分类

| 类别 | 策略 | 状态 |
|------|------|------|
| **纯量化 (无LLM)** | S02 机构持仓, S05 分析师分歧, S07 图谱多因子 | ✅ 可直接运行 |
| **LLM辅助** | S04 财报超预期, S06 公告情绪, S08 行业轮动, S09 景气传导 | ✅ 需 DeepSeek API Key |
| **MiroFish模拟** | S10 多轮情绪模拟 (3轮级联+图谱上下文), S11 叙事追踪 | ✅ 需 DeepSeek API Key |
| **需知识图谱** | S01 供应链传导, S03 事件传播 | ⚠️ 需 Zep Cloud 配置 |

### 各策略信号产出情况 (2026-03-16 测试)

| 策略 | 信号数 | 方向分布 | 说明 |
|------|--------|----------|------|
| S02 | 14 | 7 long / 7 avoid | 正常, 回测评级 F |
| S04 | 0 | - | 需近期财报窗口期 |
| S05 | 3 | 3 neutral | 无方向性信号 |
| S06 | 0 | - | 需重大公告触发 |
| S07 | 30 | 10 long / 10 avoid / 10 neutral | 回测评级 F, 需优化 |
| S08 | 4 | 3 long / 1 avoid | 回测评级 D, T-20胜率100% |
| S09 | 0 | - | 需历史景气数据积累 |
| S10 | 1 | 1 avoid (五粮液) | 回测评级 C, 样本不足 |

---

## 四、关键技术决策

### 4.1 数据源策略

- **主数据源**: AkShare → 新浪财经 API (稳定可靠)
- **已屏蔽**: 东方财富 API (系统代理 127.0.0.1:7890 导致超时)
- **替代方案**: `em_fallback.py` 提供:
  - 31个申万一级行业 × 3只代表性股票 → 行业收益率计算
  - 股票名称/行业查询的超时包装
  - `scan_industry_returns_sina()` 替代 `stock_board_industry_hist_em()`
- **其他可用**: 金十数据(宏观), 同花顺(行业), 巨潮(公告)

### 4.2 LLM 配置

```
API: https://api.deepseek.com/v1
Model: deepseek-chat
成本: ~$0.27/$1.10 per 1M tokens (input/output)
缓存: SQLite 持久化 (避免重复调用)
```

所有 LLM 策略使用统一工厂: `create_llm_client(strategy_name=self.name)`

### 4.3 A股适配

- **方向信号**: `short` → `avoid` (回避/减仓), 因 A 股无做空
- `StrategySignal.__post_init__()` 自动映射 `short` → `avoid`
- 三种方向: `long` (买入), `avoid` (回避), `neutral` (中性)

### 4.4 股票池

- **快速模式 (--quick)**: 30只代表性股票
- **完整模式**: 动态获取中证800全部800只成分股（CSI800 = 沪深300 + 中证500）
  - 主源: `ak.index_stock_cons_csindex(symbol="000906")`
  - 后备: 350+ 只硬编码股票列表（含CSI300+CSI500代表性股票）

---

## 五、非纯数字Alpha架构 (核心差异化)

本项目的定位是验证**纯数字方向以外的Alpha**，利用MiroFish的群体智能能力。
核心逻辑：信息在不同类型投资者之间扩散存在时间差，这个时间差就是Alpha来源。

### 5.1 三层非纯数字Alpha

**层1 — 信息传导时间差** (S01/S03/S10)
- 事件发生 → 快钱(游资/量化)第一时间反应 → 中速(趋势/散户)跟进 → 机构深思熟虑确认
- S10多轮模拟捕捉每一层的传导过程, 在市场尚未完全反应时入场

**层2 — 叙事扩散周期** (S11)
- 投资主题从专业报告 → 主流媒体 → 散户认知的扩散过程
- 萌芽期(0-30)的领先标的是最高Alpha来源
- 成熟期(50-75)已是共识, 衰退期(75+)应回避

**层3 — 供应链认知差** (S01/S03 + 图谱上下文)
- 上游事件对下游的影响, 市场往往滞后1-3周才反应
- 本地图谱编码供应链关系, 动态图谱追踪各公司当前状态

### 5.2 信号优先级

| 信号来源 | 特点 | 建议权重 |
|---------|------|---------|
| S11 萌芽期信号 | 超前市场, 高不确定性 | 中等仓位, 止损严格 |
| S10 多轮模拟 (amplifying) | 机构确认快钱方向 | 较高仓位 |
| S10 多轮模拟 (reversal) | 机构纠偏快钱 | 逆向仓位 |
| S01/S03 供应链传导 | 2-3阶效应, 滞后Alpha | 耐心持仓20-40天 |
| S02/S05/S07 纯量化 | 已有其他项目覆盖 | 辅助验证用 |

---

## 六、回测框架

### 5.1 核心组件

`run_backtest.py` 实现了完整回测系统:

1. **RollingEvaluator**: 多窗口滚动回测
   - 默认窗口: T-20, T-40, T-60 个交易日
   - 对每个信号在多个历史窗口验证一致性
   - 输出: 命中率, 平均收益, 夏普比率, 最大回撤, 利润因子, 一致性

2. **BacktestEngine**: 回测引擎
   - `run_strategy()`: 运行单个策略
   - `evaluate_rolling()`: 滚动评估
   - `run_all()`: 批量运行
   - `_rate_strategy()`: A/B/C/D/F 评级
   - `generate_report()`: 8段式 Markdown 报告

3. **评级标准**:
   - **A**: 命中率>60% & 夏普>1.0 & 一致性>60% & 利润因子>1.5
   - **B**: 命中率>55% & 夏普>0.5 & 一致性>50%
   - **C**: 命中率>50% | 夏普>0.3
   - **D**: 命中率>45%
   - **F**: 其他

### 5.2 运行方式

```bash
# 快速回测 (30只股票, 跳过图谱策略)
python astrategy/run_backtest.py --quick --no-graph

# 完整回测 (沪深300全部)
python astrategy/run_backtest.py --no-graph

# 指定策略
python astrategy/run_backtest.py --strategies S02 S07 S08

# 自定义窗口
python astrategy/run_backtest.py --quick --no-graph --windows 10 20 30
```

---

## 六、环境配置

### 6.1 必需环境变量 (.env)

```env
# LLM (必需, 用于S04/S06/S08/S09/S10)
LLM_API_KEY=sk-xxx            # DeepSeek API Key
LLM_BASE_URL=https://api.deepseek.com/v1
LLM_MODEL=deepseek-chat

# 图谱 (可选, 用于S01/S03)
ZEP_API_KEY=xxx               # Zep Cloud API Key

# MiroFish 原始配置 (前端/后端)
OPENAI_API_KEY=xxx
OPENAI_BASE_URL=xxx
```

### 6.2 Python 依赖

```bash
cd astrategy
pip install -e ".[dev]"
# 或直接安装核心依赖:
pip install akshare openai pyyaml pandas python-dotenv tqdm
# 可选 (图谱功能):
pip install zep-cloud
```

### 6.3 注意事项

- Python >= 3.10
- 如有系统代理, 东方财富API会超时 → em_fallback.py 已处理
- 数据缓存在 `astrategy/.data/`
- LLM 缓存在 SQLite (自动管理)
- 信号输出在 `astrategy/.data/signals/{strategy_name}/{date}.json`

---

## 七、当前进展 (截至 2026-03-16)

### ✅ 已完成

1. **11个策略全部实现** — S01-S11 代码编写完毕, 9个已验证运行 (S01/S03 需图谱)
2. **数据采集层** — 6个采集器 + 东方财富替代方案, 全部可用
3. **LLM 基础设施** — 客户端/缓存/成本追踪/批量调度, 已验证 DeepSeek 集成
4. **A股适配** — short→avoid 方向映射, 全系统一致
5. **股票池扩展** — 中证800动态获取 (CSI800=沪深300+中证500) + 30只快速模式
6. **滚动回测框架** — RollingEvaluator + BacktestEngine + 评级系统 (代码就绪)
7. **信号聚合/组合优化** — 多策略共识 + 风险约束仓位管理
8. **调度系统** — 日频/周频/事件驱动 + 主调度器
9. **本地图谱** — LocalGraphStore JSON持久化, 600节点/96供应链关系, 无需Zep
10. **S10升级(MiroFish多轮模拟)** — 3轮级联 (快钱→中速→深思) + 图谱上下文注入, 捕捉信息扩散动态
11. **S11叙事追踪** — 14个预定义叙事主题, 生命周期评分 (萌芽/扩散/成熟/衰退), 早期Alpha信号
12. **动态图谱更新器** — `graph/dynamic_updater.py` 新闻/公告驱动公司状态节点实时更新

### ✅ 已完成 (历史)

- **快速回测完成 (2026-03-16)** — 30只股票, 8策略全部通过, 详见 `astrategy/.data/backtest_report.md`
  - S08 行业轮动: T-20窗口胜率100%, 但长窗口不稳定 → 评级 D
  - S10 情绪模拟: 胜率100%但样本仅1只 → 评级 C
  - S02/S07: 胜率低于50% → 评级 F, 需要参数优化
  - LLM成本: 51次调用, $0.026

### ⏳ 待完成

1. **S10/S11回测** — 对多轮模拟和叙事追踪进行完整CSI800回测
2. **动态图谱日更** — 将 `DynamicGraphUpdater.run_daily()` 接入调度系统
3. **完整中证800回测** — 用800只股票运行全部策略, 生成完整评估
4. **策略参数优化** — 根据回测结论调整策略参数 (重点S02/S07)
5. **多日历史回测** — 用过去 N 天的数据分别运行策略, 验证时间稳定性
6. **图谱策略 (S01/S03)** — 改用本地图谱 (local_store.py 已就绪)
7. **实盘/模拟盘对接** — 通过 Freqtrade Bridge 接入交易系统
8. **前端可视化** — 叙事扩散仪表盘 + 回测结果对比 Dashboard

---

## 八、继续研究的操作步骤

### 在新机器上快速恢复

```bash
# 1. 克隆仓库
git clone <your-repo-url>
cd MiroFish

# 2. 配置环境
cp .env.example .env
# 编辑 .env, 填入 LLM_API_KEY

# 3. 安装依赖
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cd ../astrategy
pip install -e .

# 4. 快速验证
python astrategy/run_backtest.py --quick --no-graph --strategies S02 S07

# 5. 完整回测
python astrategy/run_backtest.py --no-graph

# 6. 查看报告
cat astrategy/.data/backtest_report_*.md
```

### 给 Claude 的提示词

在新机器上启动 Claude 时, 可以这样说:

> 请阅读 `docs/astrategy_architecture.md` 了解项目架构和进展。
> 当前任务是: [你要做的事情]
>
> 关键文件:
> - 策略代码: astrategy/strategies/s01-s10
> - 回测入口: astrategy/run_backtest.py
> - 数据采集: astrategy/data_collector/
> - 配置: astrategy/config.py + .env

---

## 九、已知问题与 Workaround

| 问题 | Workaround |
|------|-----------|
| 东方财富API超时 | `em_fallback.py` 用新浪替代 |
| S09 首次运行0信号 | 正常 — 需历史景气度积累 |
| S04/S06 信号少 | 事件驱动型, 非每日产生信号 |
| 行业轮动模板KeyError | `str.replace()` 替代 `str.format()` |
| akshare FutureWarning | pandas 兼容警告, 不影响功能 |
| LLM 成本 | DeepSeek 极低成本 (~$0.01/次回测) |

---

## 十、技术栈汇总

| 组件 | 技术 |
|------|------|
| 语言 | Python 3.10+ |
| 数据源 | AkShare (新浪/同花顺/巨潮/金十) |
| LLM | DeepSeek Chat (OpenAI 兼容) |
| 图谱 | Zep Cloud (可选) |
| 缓存 | SQLite (LLM) + 内存 TTL (数据) |
| 回测 | 自研 RollingEvaluator + Freqtrade Bridge |
| 前端 | Vue 3 + D3.js (MiroFish 原始) |
| 后端 | Flask (MiroFish) + 独立 CLI (AStrategy) |
