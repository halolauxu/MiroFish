# A股交易员总控框架报告

**日期**: 2026-03-20
**Shock 校准源**: `/Users/xukun/Documents/freqtrade/MiroFish/astrategy/.data/reports/shock_signals_20260318.json`
**历史事件桶数**: 5

## Regime

- 风险模式: `defensive_barbell`
- 宏观相位分布: `{'contraction': 4}`

| Favored Industries | Score |
| --- | --- |
| 电力设备 | +1.70 |
| 煤炭 | +0.85 |

## Mainline

| Top Themes | Score |
| --- | --- |
| 光伏储能 | +0.55 |

## Catalyst

| Code | Name | Event | Dir | Conf | Prior | Title |
| --- | --- | --- | --- | --- | --- | --- |
| 600059 | 古越龙山 | scandal | avoid | 0.61 | +0.00 | 五粮液董事长曾从钦被立案调查 |
| 000596 | 古井贡酒 | scandal | avoid | 0.58 | +0.00 | 五粮液董事长曾从钦被立案调查 |
| 600809 | 山西汾酒 | scandal | avoid | 0.56 | +0.00 | 五粮液董事长曾从钦被立案调查 |
| 000568 | 泸州老窖 | scandal | neutral | 0.55 | +0.00 | 五粮液董事长曾从钦被立案调查 |
| 000858 | 五 粮 液 | scandal | avoid | 0.35 | +0.00 | 五粮液董事长曾从钦被立案调查 |
| 601888 | 中国中免 | ma | long | 0.24 | +0.20 | 601888，宣布重要收购！ |
| 600519 | 贵州茅台 | scandal | avoid | 0.28 | +0.00 | 五粮液董事长曾从钦被立案调查 |
| 002230 | 科大讯飞 | earnings_shock | long | 0.22 | +0.00 | 业绩预喜！002230 直线涨停 |

## Flow / Expectation

| Code | Name | Source | Dir | Score | Detail |
| --- | --- | --- | --- | --- | --- |
| 601888 | 中国中免 | institution_association | long | +0.96 | gap=19.80%, holders=11 |
| 601088 | 中国神华 | institution_association | avoid | -0.76 | gap=-17.97%, holders=11 |
| 002594 | 比亚迪 | institution_association | avoid | -0.75 | gap=-17.38%, holders=6 |
| 300750 | 宁德时代 | institution_association | avoid | -0.68 | gap=-14.18%, holders=9 |
| 603259 | 药明康德 | institution_association | long | +0.68 | gap=9.25%, holders=11 |
| 601318 | 中国平安 | institution_association | long | +0.67 | gap=9.04%, holders=11 |
| 002714 | 牧原股份 | institution_association | avoid | -0.64 | gap=-11.83%, holders=9 |
| 600030 | 中信证券 | institution_association | long | +0.61 | gap=7.36%, holders=11 |
| 000568 | 泸州老窖 | institution_association | long | +0.61 | gap=7.09%, holders=11 |
| 601728 | 中国电信 | institution_association | avoid | -0.59 | gap=-9.26%, holders=11 |
| 600309 | 万华化学 | institution_association | long | +0.58 | gap=6.31%, holders=11 |
| 600760 | 中航沈飞 | institution_association | long | +0.57 | gap=5.96%, holders=11 |

## Graph / Sentiment Filter

| Code | Name | GraphPct | Bucket | Quality |
| --- | --- | --- | --- | --- |
| 002594 | 比亚迪 | 1.00 | +0.00 | 0.70 |
| 601888 | 中国中免 | 0.56 | +0.20 | 0.68 |
| 601012 | 隆基绿能 | 0.33 | +0.00 | 0.56 |
| 002230 | 科大讯飞 | 0.50 | +0.00 | 0.56 |
| 300750 | 宁德时代 | 0.78 | +0.00 | 0.54 |
| 601088 | 中国神华 | 0.89 | +0.00 | 0.54 |
| 002415 | 海康威视 | 0.50 | +0.00 | 0.50 |
| 600030 | 中信证券 | 0.50 | +0.00 | 0.50 |
| 000568 | 泸州老窖 | 0.50 | +0.00 | 0.50 |
| 603259 | 药明康德 | 0.50 | +0.00 | 0.50 |
| 600900 | 长江电力 | 0.50 | +0.00 | 0.50 |
| 002714 | 牧原股份 | 0.50 | +0.00 | 0.50 |

## Execution Mapping

- Primary line: `主线趋势多头 + selective catalyst`
- Secondary line: `机构/预期差补涨`
- Route mix: `{'预期差/补涨': 5, '主线趋势多头': 1, '事件驱动多头': 1, '主题降温回避': 2, '事件防守/回避': 3, '观察池': 9}`

### Core Longs

| Code | Name | Route | Score | R | M | C | F | Q |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 601888 | 中国中免 | 预期差/补涨 | +0.34 | +0.00 | +0.00 | +0.26 | +0.96 | 0.68 |
| 601012 | 隆基绿能 | 主线趋势多头 | +0.28 | +0.77 | +0.55 | +0.00 | +0.00 | 0.56 |
| 002230 | 科大讯飞 | 事件驱动多头 | +0.21 | +0.00 | +0.00 | +0.22 | +0.56 | 0.56 |
| 603259 | 药明康德 | 预期差/补涨 | +0.16 | +0.00 | +0.00 | +0.00 | +0.68 | 0.50 |
| 601318 | 中国平安 | 预期差/补涨 | +0.16 | +0.00 | +0.00 | +0.00 | +0.67 | 0.42 |
| 600030 | 中信证券 | 预期差/补涨 | +0.15 | +0.00 | +0.00 | +0.00 | +0.61 | 0.50 |
| 000568 | 泸州老窖 | 预期差/补涨 | +0.15 | +0.00 | +0.00 | +0.00 | +0.61 | 0.50 |

### Avoid Overlay

| Code | Name | Route | Score | R | M | C | F | Q |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 002594 | 比亚迪 | 主题降温回避 | -0.22 | +0.00 | -0.11 | +0.00 | -0.75 | 0.70 |
| 000596 | 古井贡酒 | 事件防守/回避 | -0.19 | +0.00 | +0.00 | -0.55 | +0.00 | 0.50 |
| 600059 | 古越龙山 | 事件防守/回避 | -0.17 | +0.00 | +0.00 | -0.49 | +0.00 | 0.50 |
| 002714 | 牧原股份 | 主题降温回避 | -0.15 | +0.00 | +0.00 | +0.00 | -0.64 | 0.50 |
| 600809 | 山西汾酒 | 事件防守/回避 | -0.15 | +0.00 | +0.00 | -0.45 | +0.00 | 0.50 |

### Watchlist

| Code | Name | Route | Score | Thesis |
| --- | --- | --- | --- | --- |
| 601728 | 中国电信 | 观察池 | -0.14 | 机构共振 avoid 0.59, catch-up gap=-9.26% |
| 600309 | 万华化学 | 观察池 | +0.14 | 最近 11 条事件进入观察池 | 机构共振 long 0.53, catch-up gap=6.31% |
| 601390 | 中国中铁 | 观察池 | -0.13 | 机构共振 avoid 0.55, catch-up gap=-7.34% |
| 600900 | 长江电力 | 观察池 | -0.13 | 机构共振 avoid 0.54, catch-up gap=-6.95% |
| 000858 | 五 粮 液 | 观察池 | -0.12 | S10 scandal avoid 0.35: 五粮液董事长曾从钦被立案调查 | 图谱结构偏弱 (rank 9/10) |
| 600519 | 贵州茅台 | 观察池 | -0.09 | Shock scandal hop=1 avoid 0.28 | 图谱结构偏弱 (rank 10/10) |
| 600760 | 中航沈飞 | 观察池 | +0.03 | 国防军工 轮动信号 avoid 0.55 | 机构共振 long 0.52, catch-up gap=5.96% | 多阶段方 |
| 300750 | 宁德时代 | 观察池 | -0.02 | 电力设备 轮动信号 long 0.85 | 新能源汽车 成熟期 avoid 0.54 | 最近 2 条事件进入观察池 | 机构共 |

## Validation Blueprint

- Focus lines: `[]`
- Deferred lines: `['资金/预期差', '情绪催化', '图谱过滤', '主线叙事', 'Regime 轮动']`

| 子线 | 模式 | WF/OOS Sharpe | 年化代理 | 胜率 | 样本 | 角色 | 状态 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 事件延续 | strict_wf | 2.50 | +94.75% | 85.7% | 28 | 防守/overlay | 历史不足2年，不出最终结论 |
| 资金/预期差 | authoritative_missing | N/A | N/A | N/A | 0 | 轮动副引擎 | 缺历史，不出结论 |
| 情绪催化 | authoritative_missing | N/A | N/A | N/A | 0 | 短线催化器 | 缺历史，不出结论 |
| 图谱过滤 | authoritative_missing | N/A | N/A | N/A | 0 | 过滤与加权器 | 缺历史，不出结论 |
| 主线叙事 | authoritative_missing | N/A | N/A | N/A | 0 | 进攻主引擎 | 缺历史，不出结论 |
| Regime 轮动 | authoritative_missing | N/A | N/A | N/A | 0 | 战场选择器 | 缺历史，不出结论 |

### Monetization Paths

| 子线 | 赚钱路径 | 备注 |
| --- | --- | --- |
| 事件延续 | 负向 continuation 优先货币化为减仓、回避、新仓过滤与对冲篮子；正向 | 历史跨度只有 136 天，未达到两年门槛 (730 天)。 OOS 有效样本仍偏 |
| 资金/预期差 | 机构共振与预期差补涨轮动；顺主线做 catch-up，不逆着主线硬做左侧抄底。 | institution_association: signal_dates=2, |
| 情绪催化 | 事件触发单与观察池晋级；只有在主线或资金共振时才放大仓位，单独使用以试单为主。 | sentiment_simulation: signal_dates=2, fa |
| 图谱过滤 | 只用于候选筛选、仓位加权和补涨线索发掘，不单独裸开仓。 | graph_factors: signal_dates=3, factor_da |
| 主线叙事 | 围绕主线龙头与补涨梯队分层开仓；进入衰退阶段时切回减仓与观察池。 | narrative_tracker: signal_dates=1, facto |
| Regime 轮动 | 行业ETF/板块龙头轮动；收缩期优先高景气龙头，弱势行业做减仓与禁入。 | sector_rotation: signal_dates=2, factor_ |