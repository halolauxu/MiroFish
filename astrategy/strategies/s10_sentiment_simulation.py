"""
Sentiment Simulation Strategy (S10) — MiroFish Multi-Round Simulation
=======================================================================

Mirrors MiroFish's group-intelligence simulation approach:
- Round 1 (T+0~3h)  : fast-money agents (游资/量化) react to the raw event
- Round 2 (T+1~2d)  : mid-speed agents (趋势跟随/散户) observe Round-1 price
                       movement and decide whether to amplify or fade
- Round 3 (T+3~10d) : deliberate agents (公募/价值) digest the full picture and
                       determine whether the initial market reaction was correct

This 3-round cascade captures the *information diffusion* mechanism that
MiroFish models with OASIS agents.  It is fully LLM-based (no subprocess),
but the sequential, cross-round influence is the key conceptual upgrade.

Additional: Knowledge-graph context is injected per event so that supply-chain
second-order effects are surfaced to agents that would not otherwise know them.

Core idea
---------
1. Detect simulation-worthy events (high-impact only).
2. Query local graph for supply-chain / industry context around the stock.
3. Generate event-specific agent profiles (with graph context) for 6 archetypes.
4. Round-1 simulate fast-money reactions.
5. Round-2 simulate mid-speed reactions, observing Round-1 outcome.
6. Round-3 simulate deliberate reactions, observing Round-1+2 outcomes.
7. Aggregate across rounds with time-decay weights.
8. Compare simulated consensus with actual price movement.
9. Large gap = potential opportunity (overreaction / underreaction).

Typical holding period: 5-15 trading days.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from astrategy.config import settings
from astrategy.data_collector.market_data import MarketDataCollector
from astrategy.data_collector.news import NewsCollector
from astrategy.llm import create_llm_client
from astrategy.strategies.base import BaseStrategy, StrategySignal

# Optional: local graph (no hard dependency)
try:
    from astrategy.graph.local_store import LocalGraphStore
    _GRAPH_AVAILABLE = True
except ImportError:
    _GRAPH_AVAILABLE = False

logger = logging.getLogger("astrategy.strategies.s10_sentiment_simulation")

_CST = timezone(timedelta(hours=8))

# ---------------------------------------------------------------------------
# Agent archetypes
# ---------------------------------------------------------------------------

AGENT_ARCHETYPES: Dict[str, Dict[str, Any]] = {
    "价值投资者": {
        "description": (
            "保守型投资者，关注基本面（PE、PB、ROE、现金流），偏好低估值蓝筹。"
            "对短期波动容忍度高，不追涨杀跌。决策周期长，通常等季报/年报确认后才行动。"
        ),
        "reaction_speed": "slow",
        "risk_tolerance": "low",
        "decision_style": "fundamental",
        "market_influence_weight": 0.15,
    },
    "趋势跟随者": {
        "description": (
            "基于技术指标（均线、MACD、成交量）做决策的交易者。"
            "关注趋势延续性，突破买入、跌破卖出。对消息面只看是否影响趋势。"
            "持仓周期中等，通常数周到数月。"
        ),
        "reaction_speed": "medium",
        "risk_tolerance": "medium",
        "decision_style": "technical",
        "market_influence_weight": 0.10,
    },
    "游资/短线客": {
        "description": (
            "激进型交易者，专注事件驱动和短期博弈。追求涨停板、打板战术。"
            "对消息极度敏感，第一时间反应。高换手、高仓位集中度。"
            "持仓极短，T+1到几天。善于利用情绪差和信息差。"
        ),
        "reaction_speed": "immediate",
        "risk_tolerance": "high",
        "decision_style": "event_driven",
        "market_influence_weight": 0.15,
    },
    "量化基金": {
        "description": (
            "系统化交易，基于统计模型和因子。关注均值回归、动量、波动率等量化指标。"
            "不受情绪影响，纯数据驱动。对异常偏离会反向操作（均值回归）。"
            "持仓分散，单票仓位小但整体影响力大。"
        ),
        "reaction_speed": "immediate",
        "risk_tolerance": "medium",
        "decision_style": "systematic",
        "market_influence_weight": 0.25,
    },
    "公募基金经理": {
        "description": (
            "机构投资者，管理规模大，受基准约束（沪深300/中证500）。"
            "决策需要内部流程，不能快速大幅调仓。注重行业配置和个股精选。"
            "关注相对收益，既看基本面也看市场共识。容易形成抱团行为。"
        ),
        "reaction_speed": "slow",
        "risk_tolerance": "low",
        "decision_style": "balanced",
        "market_influence_weight": 0.30,
    },
    "散户": {
        "description": (
            "个人投资者，信息获取滞后，容易受新闻标题和社交媒体影响。"
            "决策情绪化，追涨杀跌倾向明显。资金量小但人数众多。"
            "喜欢跟风热门题材，容易在高位接盘、低位割肉。"
        ),
        "reaction_speed": "medium",
        "risk_tolerance": "high",
        "decision_style": "emotional",
        "market_influence_weight": 0.05,
    },
}

# Market influence weights for aggregation (must sum to 1.0)
_INFLUENCE_WEIGHTS: Dict[str, float] = {
    k: v["market_influence_weight"] for k, v in AGENT_ARCHETYPES.items()
}

# Multi-round simulation: which archetypes react in which round
# Round 1: immediate fast-money (T+0 to T+3h)
_ROUND1_ARCHETYPES = ["游资/短线客", "量化基金"]
# Round 2: medium-speed, observes round-1 price impact (T+1 to T+2d)
_ROUND2_ARCHETYPES = ["趋势跟随者", "散户"]
# Round 3: deliberate, sees the full picture (T+3 to T+10d)
_ROUND3_ARCHETYPES = ["公募基金经理", "价值投资者"]

# ---------------------------------------------------------------------------
# Event detection keywords
# ---------------------------------------------------------------------------

_HIGH_IMPACT_KEYWORDS = [
    "业绩预增", "业绩预减", "业绩预亏", "业绩暴雷", "超预期", "低于预期",
    "重大合同", "收购", "并购", "重组", "借壳", "退市",
    "立案调查", "行政处罚", "财务造假", "违规",
    "政策利好", "政策利空", "降准", "降息", "加征关税",
    "涨停", "跌停", "暴涨", "暴跌", "闪崩",
    "高管辞职", "实控人变更", "股权转让", "大额减持", "清仓减持",
    "战略投资", "引入战投", "国资入驻",
]

# ---------------------------------------------------------------------------
# Prompt templates
# ---------------------------------------------------------------------------

_EVENT_DETECTION_PROMPT = """\
你是A股市场事件分析师。请从以下新闻中筛选出值得进行多参与者情绪模拟的高影响力事件。

高影响力事件标准:
- 业绩大幅超预期或不及预期（变动>20%）
- 重大政策变化（行业政策、货币政策）
- 公司丑闻、违规、立案调查
- 重大并购重组
- 突发黑天鹅事件

以下是近期新闻:
{news_text}

请以JSON格式输出，包含 "events" 数组:
{{
  "events": [
    {{
      "title": "事件标题",
      "type": "earnings_shock/policy_change/scandal/ma/black_swan/other",
      "stock_code": "相关股票代码（6位）",
      "stock_name": "股票名称",
      "impact_level": "high/critical",
      "summary": "事件摘要（50字内）",
      "key_data": "关键数据点（如业绩变动幅度）"
    }}
  ]
}}

只保留真正高影响力的事件（通常不超过5个）。如果没有值得模拟的事件，返回空数组。
"""

_AGENT_PROFILE_PROMPT = """\
你是A股市场参与者画像专家。针对以下事件，为"{archetype_name}"类型的投资者生成具体的投资画像。

事件: {event_title}
事件类型: {event_type}
相关股票: {stock_code} {stock_name}
事件摘要: {event_summary}

投资者类型描述: {archetype_description}

请以JSON格式输出:
{{
  "current_position": "已持有/未持有/少量持有",
  "position_reason": "持仓或不持仓的原因",
  "investment_thesis": "对该股票的投资逻辑（50字内）",
  "key_concerns": ["关注点1", "关注点2", "关注点3"],
  "pre_event_sentiment": -1.0到1.0之间的数值
}}
"""

# Round-2/3 observe prior-round results
_ROUND2_REACTIONS_PROMPT = """\
你是A股市场多参与者博弈模拟专家。正在进行第二轮模拟——中等速度的投资者已经观察到了
早期快钱（游资/量化）的初步反应，现在决定是否跟进或逆向。

## 事件信息
事件: {event_title}
股票: {stock_code} {stock_name}
摘要: {event_summary}

## 第一轮结果（快钱初步反应）
{round1_summary}

## 要求
现在模拟以下中等速度投资者的决策（他们看到了第一轮的价格变动）：
{target_archetypes}

每位投资者的画像：
{profiles_text}

他们会跟进还是逆向第一轮？还是独立判断？考虑：
1. 第一轮反应是否合理（是情绪化还是有基本面支撑）
2. 该类型投资者的决策风格
3. 当前已出现的价格变动是否影响其风险收益比

请以JSON格式输出，包含 "reactions" 数组（结构同第一轮）。
"""

_ROUND3_REACTIONS_PROMPT = """\
你是A股市场多参与者博弈模拟专家。正在进行第三轮模拟——深思熟虑的机构投资者（公募/价值）
已经观察了前两轮（快钱+中速）的完整市场反应，现在做出最终判断。

## 事件信息
事件: {event_title}
股票: {stock_code} {stock_name}
摘要: {event_summary}

## 前两轮综合反应
{prior_rounds_summary}

## 要求
现在模拟以下深思熟虑型投资者的决策：
{target_archetypes}

画像：
{profiles_text}

关键问题：前两轮的市场反应正确定价了吗？这些机构投资者会：
- 确认（买入/减仓跟随市场方向）
- 纠偏（认为市场过度反应，逆向操作）
- 观望（等待更多信息）

请以JSON格式输出，包含 "reactions" 数组。
"""

_SIMULATE_REACTIONS_PROMPT = """\
你是A股市场多参与者博弈模拟专家。请模拟以下不同类型投资者对事件的反应。

## 事件信息
事件: {event_title}
事件类型: {event_type}
相关股票: {stock_code} {stock_name}
事件摘要: {event_summary}
关键数据: {key_data}

## 投资者画像
{profiles_text}

## 要求
对每位投资者，模拟其在获知该事件后的交易决策。考虑:
1. 该类型投资者的典型反应模式
2. 当前市场环境和投资者已有仓位
3. 事件的性质和严重程度
4. A股市场的特殊机制（涨跌停、T+1等）

请以JSON格式输出，包含 "reactions" 数组:
{{
  "reactions": [
    {{
      "archetype": "投资者类型名称",
      "action": "buy/sell/hold/watch",
      "urgency": "immediate/within_days/wait_for_confirmation",
      "size": "heavy/moderate/light",
      "sentiment_score": -1.0到1.0之间的数值,
      "reasoning": "决策逻辑（80字内）",
      "price_target_change_pct": 预期涨跌幅百分比（如5.0表示预期涨5%）,
      "time_horizon_days": 预期持有/观察天数
    }}
  ]
}}

每个投资者类型必须有且仅有一条反应。确保不同类型的反应体现出差异化。
"""


# ---------------------------------------------------------------------------
# Strategy implementation
# ---------------------------------------------------------------------------


class SentimentSimulationStrategy(BaseStrategy):
    """LLM-based multi-agent sentiment simulation strategy.

    Simulates how different market participant archetypes would react to
    high-impact events, then compares the simulated consensus with actual
    market movement to identify mispricing opportunities.

    Parameters
    ----------
    holding_days : int
        Default holding period for generated signals (default 10).
    max_events_per_run : int
        Maximum number of events to simulate per run (default 3).
    signal_dir : Path | str | None
        Override signal persistence directory.
    """

    def __init__(
        self,
        holding_days: int = 10,
        max_events_per_run: int = 3,
        signal_dir: Path | str | None = None,
        use_multi_round: bool = True,
    ) -> None:
        super().__init__(signal_dir=signal_dir)
        self._holding_days = holding_days
        self._max_events = max_events_per_run
        self._use_multi_round = use_multi_round

        self._news = NewsCollector()
        self._market = MarketDataCollector()
        self._llm = create_llm_client(strategy_name=self.name)

        # Load local graph if available (supply-chain context)
        self._graph: Optional[Any] = None
        if _GRAPH_AVAILABLE:
            try:
                store = LocalGraphStore()
                if store.load("astrategy"):
                    self._graph = store
                    logger.info("[%s] Local graph loaded for context enrichment.", self.name)
            except Exception as exc:
                logger.debug("[%s] Local graph unavailable: %s", self.name, exc)

    # ── BaseStrategy interface ────────────────────────────────────────

    @property
    def name(self) -> str:
        return "sentiment_simulation"

    # ==================================================================
    # 1. detect_simulation_worthy_events
    # ==================================================================

    def detect_simulation_worthy_events(
        self, stock_codes: list[str] | None = None, date: str | None = None,
    ) -> list[dict]:
        """Scan recent news for high-impact events worth simulating.

        Only events that justify the simulation cost (~10K tokens each) are
        returned: earnings miss/beat >20%, major policy shifts, scandals,
        M&A announcements, black-swan incidents.

        Parameters
        ----------
        stock_codes :
            Optional list of stock codes to focus on.  If ``None``, scans
            market-wide hot topics.
        date :
            Date string for context (unused by data source, kept for API
            consistency).

        Returns
        -------
        list[dict]
            Each dict contains: title, type, stock_code, stock_name,
            impact_level, summary, key_data.
        """
        # Gather news from multiple sources
        all_news: list[dict] = []

        # Per-stock news via ak.stock_news_em (stable API)
        # Scan all supplied codes; batch in groups to stay within rate limits.
        if stock_codes:
            import time as _time
            for i, code in enumerate(stock_codes):
                try:
                    news = self._news.get_company_news(code, limit=10)
                    for item in news:
                        item["_stock_code"] = code
                    all_news.extend(news)
                except Exception as exc:
                    logger.debug("News fetch failed %s: %s", code, exc)
                # Gentle rate-limit: brief pause every 50 stocks
                if i > 0 and i % 50 == 0:
                    _time.sleep(0.5)

        # Fallback: market hot topics (may fail due to network restrictions)
        if len(all_news) < 10:
            try:
                hot = self._news.get_market_hot_topics(limit=30)
                all_news.extend(hot)
            except Exception as exc:
                logger.debug("Hot topics fallback failed: %s", exc)

        logger.info("[%s] Collected %d news items from %d stocks",
                    self.name, len(all_news), len(stock_codes) if stock_codes else 0)

        if not all_news:
            logger.info("[%s] No news collected; no events to detect.", self.name)
            return []

        # Pre-filter with keywords to reduce LLM cost
        filtered = self._keyword_prefilter(all_news)
        if not filtered:
            logger.info("[%s] No high-impact news after keyword prefilter.", self.name)
            return []

        # Format news for LLM — send up to 50 filtered items in one batch call
        news_lines: list[str] = []
        for i, item in enumerate(filtered[:50]):
            title = item.get("新闻标题", item.get("标题", item.get("名称", str(item)[:100])))
            source = item.get("文章来源", item.get("来源", ""))
            time_str = item.get("发布时间", item.get("时间", ""))
            code = item.get("_stock_code", item.get("代码", ""))
            news_lines.append(f"[{i}] {title} | 来源:{source} | 时间:{time_str} | 代码:{code}")

        prompt = _EVENT_DETECTION_PROMPT.format(news_text="\n".join(news_lines))

        messages = [
            {"role": "system", "content": "你是A股市场高影响力事件检测专家。严格以JSON格式输出。"},
            {"role": "user", "content": prompt},
        ]

        try:
            result = self._llm.chat_json(messages=messages, max_tokens=1500)
            events = result.get("events", [])
        except Exception as exc:
            logger.error("[%s] Event detection LLM call failed: %s", self.name, exc)
            events = self._rule_based_event_detection(filtered)

        # Validate and limit
        valid_events: list[dict] = []
        for event in events:
            if not event.get("stock_code") or not event.get("title"):
                continue
            event.setdefault("type", "other")
            event.setdefault("stock_name", event.get("stock_code", ""))
            event.setdefault("impact_level", "high")
            event.setdefault("summary", event.get("title", ""))
            event.setdefault("key_data", "")
            valid_events.append(event)

        valid_events = valid_events[: self._max_events]
        logger.info(
            "[%s] Detected %d simulation-worthy events.", self.name, len(valid_events),
        )
        return valid_events

    # ==================================================================
    # 2. generate_agent_profiles
    # ==================================================================

    def generate_agent_profiles(
        self, event: dict, stock_code: str,
    ) -> list[dict]:
        """Generate event-specific agent profiles for each archetype.

        Uses LLM to contextualise each archetype's position and thesis for
        the specific stock and event.

        Parameters
        ----------
        event :
            Event dict from ``detect_simulation_worthy_events``.
        stock_code :
            6-digit stock code.

        Returns
        -------
        list[dict]
            One profile per archetype, each containing: archetype, description,
            current_position, investment_thesis, key_concerns,
            pre_event_sentiment, reaction_speed, risk_tolerance, decision_style.
        """
        stock_name = event.get("stock_name", stock_code)
        event_title = event.get("title", "未知事件")
        event_type = event.get("type", "other")
        event_summary = event.get("summary", event_title)

        # Batch all archetypes into a single prompt for efficiency
        archetype_sections: list[str] = []
        archetype_order: list[str] = list(AGENT_ARCHETYPES.keys())

        for arch_name in archetype_order:
            arch = AGENT_ARCHETYPES[arch_name]
            archetype_sections.append(
                f"### {arch_name}\n"
                f"描述: {arch['description']}\n"
                f"反应速度: {arch['reaction_speed']}, "
                f"风险容忍: {arch['risk_tolerance']}, "
                f"决策风格: {arch['decision_style']}"
            )

        batch_prompt = (
            f"你是A股市场参与者画像专家。针对以下事件，为每种投资者类型生成投资画像。\n\n"
            f"## 事件\n"
            f"事件: {event_title}\n"
            f"类型: {event_type}\n"
            f"股票: {stock_code} {stock_name}\n"
            f"摘要: {event_summary}\n\n"
            f"## 投资者类型\n"
            + "\n\n".join(archetype_sections)
            + "\n\n请以JSON格式输出，包含 \"profiles\" 数组，每个元素对应一种投资者类型:\n"
            "{\n"
            '  "profiles": [\n'
            "    {\n"
            '      "archetype": "投资者类型名称",\n'
            '      "current_position": "已持有/未持有/少量持有",\n'
            '      "position_reason": "原因（30字内）",\n'
            '      "investment_thesis": "投资逻辑（50字内）",\n'
            '      "key_concerns": ["关注点1", "关注点2"],\n'
            '      "pre_event_sentiment": -1.0到1.0之间的数值\n'
            "    }\n"
            "  ]\n"
            "}\n\n"
            f"必须包含全部{len(archetype_order)}种类型，顺序与上文一致。"
        )

        messages = [
            {"role": "system", "content": "你是A股市场参与者画像生成专家。严格以JSON格式输出。"},
            {"role": "user", "content": batch_prompt},
        ]

        try:
            result = self._llm.chat_json(messages=messages, max_tokens=2000)
            raw_profiles = result.get("profiles", [])
        except Exception as exc:
            logger.warning(
                "[%s] Profile generation LLM failed: %s; using defaults.", self.name, exc,
            )
            raw_profiles = []

        # Build final profiles, filling in archetype metadata
        profiles: list[dict] = []
        for i, arch_name in enumerate(archetype_order):
            arch = AGENT_ARCHETYPES[arch_name]

            # Match LLM output by index or name
            llm_profile: dict = {}
            if i < len(raw_profiles):
                llm_profile = raw_profiles[i]
            else:
                # Try matching by name
                for rp in raw_profiles:
                    if rp.get("archetype") == arch_name:
                        llm_profile = rp
                        break

            profile = {
                "archetype": arch_name,
                "description": arch["description"],
                "reaction_speed": arch["reaction_speed"],
                "risk_tolerance": arch["risk_tolerance"],
                "decision_style": arch["decision_style"],
                "current_position": llm_profile.get("current_position", "未持有"),
                "position_reason": llm_profile.get("position_reason", ""),
                "investment_thesis": llm_profile.get("investment_thesis", "待分析"),
                "key_concerns": llm_profile.get("key_concerns", []),
                "pre_event_sentiment": _clamp(
                    llm_profile.get("pre_event_sentiment", 0.0), -1.0, 1.0,
                ),
            }
            profiles.append(profile)

        logger.info(
            "[%s] Generated %d agent profiles for event '%s'.",
            self.name, len(profiles), event_title[:30],
        )
        return profiles

    # ==================================================================
    # 3. simulate_reactions
    # ==================================================================

    def simulate_reactions(
        self, event: dict, profiles: list[dict],
    ) -> list[dict]:
        """Simulate each agent archetype's reaction to the event.

        All agents are batched into a single LLM call for efficiency.

        Parameters
        ----------
        event :
            Event dict.
        profiles :
            Agent profiles from ``generate_agent_profiles``.

        Returns
        -------
        list[dict]
            One reaction per agent: archetype, action, urgency, size,
            sentiment_score, reasoning, price_target_change_pct,
            time_horizon_days.
        """
        stock_code = event.get("stock_code", "")
        stock_name = event.get("stock_name", stock_code)

        # Build profiles text block
        profile_lines: list[str] = []
        for p in profiles:
            profile_lines.append(
                f"### {p['archetype']}\n"
                f"描述: {p['description'][:60]}\n"
                f"当前持仓: {p['current_position']}（{p.get('position_reason', '')}）\n"
                f"投资逻辑: {p['investment_thesis']}\n"
                f"关注点: {', '.join(p.get('key_concerns', []))}\n"
                f"事件前情绪: {p['pre_event_sentiment']}"
            )

        prompt = _SIMULATE_REACTIONS_PROMPT.format(
            event_title=event.get("title", ""),
            event_type=event.get("type", "other"),
            stock_code=stock_code,
            stock_name=stock_name,
            event_summary=event.get("summary", ""),
            key_data=event.get("key_data", "无"),
            profiles_text="\n\n".join(profile_lines),
        )

        messages = [
            {
                "role": "system",
                "content": (
                    "你是A股市场多参与者博弈模拟专家。"
                    "模拟不同类型投资者对事件的差异化反应。严格以JSON格式输出。"
                ),
            },
            {"role": "user", "content": prompt},
        ]

        try:
            result = self._llm.chat_json(messages=messages, max_tokens=2500)
            raw_reactions = result.get("reactions", [])
        except Exception as exc:
            logger.error(
                "[%s] Reaction simulation LLM failed: %s; using rule-based fallback.",
                self.name, exc,
            )
            raw_reactions = self._rule_based_reactions(event, profiles)

        # Validate and normalise
        reactions: list[dict] = []
        archetype_names = {p["archetype"] for p in profiles}
        matched_names: set[str] = set()

        for r in raw_reactions:
            arch = r.get("archetype", "")
            if arch not in archetype_names or arch in matched_names:
                continue
            matched_names.add(arch)

            reaction = {
                "archetype": arch,
                "action": r.get("action", "hold"),
                "urgency": r.get("urgency", "within_days"),
                "size": r.get("size", "light"),
                "sentiment_score": _clamp(r.get("sentiment_score", 0.0), -1.0, 1.0),
                "reasoning": r.get("reasoning", ""),
                "price_target_change_pct": r.get("price_target_change_pct", 0.0),
                "time_horizon_days": r.get("time_horizon_days", 10),
            }
            # Validate action
            if reaction["action"] not in ("buy", "sell", "hold", "watch"):
                reaction["action"] = "hold"
            if reaction["urgency"] not in ("immediate", "within_days", "wait_for_confirmation"):
                reaction["urgency"] = "within_days"
            if reaction["size"] not in ("heavy", "moderate", "light"):
                reaction["size"] = "light"

            reactions.append(reaction)

        # Fill missing archetypes with neutral defaults
        for p in profiles:
            if p["archetype"] not in matched_names:
                reactions.append({
                    "archetype": p["archetype"],
                    "action": "watch",
                    "urgency": "wait_for_confirmation",
                    "size": "light",
                    "sentiment_score": 0.0,
                    "reasoning": "模拟结果缺失，默认观望",
                    "price_target_change_pct": 0.0,
                    "time_horizon_days": 10,
                })

        logger.info(
            "[%s] Simulated %d agent reactions for '%s'.",
            self.name, len(reactions), event.get("title", "")[:30],
        )
        return reactions

    # ==================================================================
    # 4. aggregate_simulation
    # ==================================================================

    def aggregate_simulation(self, reactions: list[dict]) -> dict:
        """Aggregate individual agent reactions into a market consensus.

        Weights each archetype by its typical market influence:
        - 公募基金: 30%
        - 量化基金: 25%
        - 价值投资者: 15%
        - 游资/短线客: 15%
        - 趋势跟随者: 10%
        - 散户: 5%

        Returns
        -------
        dict
            weighted_sentiment (-1 to 1), consensus_action, conviction_level,
            agreement (bool), agent_summary, weighted_price_target_pct.
        """
        if not reactions:
            return {
                "weighted_sentiment": 0.0,
                "consensus_action": "hold",
                "conviction_level": 0.0,
                "agreement": False,
                "agent_summary": {},
                "weighted_price_target_pct": 0.0,
            }

        # Compute weighted sentiment
        total_weight = 0.0
        weighted_sentiment = 0.0
        weighted_price_target = 0.0

        action_votes: Dict[str, float] = {"buy": 0.0, "sell": 0.0, "hold": 0.0, "watch": 0.0}
        sentiments: list[float] = []

        agent_summary: Dict[str, dict] = {}

        for r in reactions:
            arch = r.get("archetype", "")
            weight = _INFLUENCE_WEIGHTS.get(arch, 0.05)
            sentiment = r.get("sentiment_score", 0.0)
            action = r.get("action", "hold")
            price_target = r.get("price_target_change_pct", 0.0)

            try:
                price_target = float(price_target)
            except (ValueError, TypeError):
                price_target = 0.0

            weighted_sentiment += sentiment * weight
            weighted_price_target += price_target * weight
            total_weight += weight
            sentiments.append(sentiment)

            if action in action_votes:
                action_votes[action] += weight

            agent_summary[arch] = {
                "action": action,
                "urgency": r.get("urgency", "within_days"),
                "size": r.get("size", "light"),
                "sentiment": round(sentiment, 2),
                "reasoning": r.get("reasoning", ""),
            }

        # Normalise
        if total_weight > 0:
            weighted_sentiment /= total_weight
            weighted_price_target /= total_weight

        weighted_sentiment = _clamp(weighted_sentiment, -1.0, 1.0)

        # Consensus action: highest weighted vote
        consensus_action = max(action_votes, key=action_votes.get)  # type: ignore[arg-type]

        # Conviction level: how strongly aligned are the agents?
        # High conviction = all agents agree on direction
        if len(sentiments) >= 2:
            # Standard deviation of sentiments (lower = more agreement)
            mean_s = sum(sentiments) / len(sentiments)
            variance = sum((s - mean_s) ** 2 for s in sentiments) / len(sentiments)
            std_dev = math.sqrt(variance)
            # Conviction is inverse of dispersion, scaled to 0-1
            conviction_level = max(0.0, 1.0 - std_dev)
        else:
            conviction_level = abs(weighted_sentiment)

        # Agreement: all agents on the same side (all positive or all negative)
        all_positive = all(s >= 0 for s in sentiments)
        all_negative = all(s <= 0 for s in sentiments)
        agreement = all_positive or all_negative

        return {
            "weighted_sentiment": round(weighted_sentiment, 4),
            "consensus_action": consensus_action,
            "conviction_level": round(conviction_level, 4),
            "agreement": agreement,
            "agent_summary": agent_summary,
            "weighted_price_target_pct": round(weighted_price_target, 2),
        }

    # ==================================================================
    # 5. compare_with_reality
    # ==================================================================

    def compare_with_reality(
        self,
        stock_code: str,
        simulation: dict,
        event_date: str,
    ) -> dict:
        """Compare simulated sentiment with actual market reaction.

        Parameters
        ----------
        stock_code :
            6-digit stock code.
        simulation :
            Aggregated simulation result from ``aggregate_simulation``.
        event_date :
            Date of the event (``YYYYMMDD`` or ``YYYY-MM-DD``).

        Returns
        -------
        dict
            actual_price_change_pct, actual_direction, reality_gap,
            opportunity_type, days_measured.
        """
        # Compute date range: event_date to event_date + 5 trading days
        date_norm = event_date.replace("-", "")
        try:
            event_dt = datetime.strptime(date_norm, "%Y%m%d")
        except ValueError:
            event_dt = datetime.now(tz=_CST)

        # Look at a window around the event
        start_dt = event_dt - timedelta(days=3)
        end_dt = event_dt + timedelta(days=10)
        start_str = start_dt.strftime("%Y%m%d")
        end_str = end_dt.strftime("%Y%m%d")

        try:
            df = self._market.get_daily_quotes(stock_code, start_str, end_str)
        except Exception as exc:
            logger.warning("Price data unavailable for %s: %s", stock_code, exc)
            return self._empty_reality_comparison()

        if df is None or df.empty or len(df) < 2:
            return self._empty_reality_comparison()

        # Find the event date row (or nearest trading day after)
        df = df.copy()
        date_col = "日期"
        if date_col not in df.columns:
            # Try first column
            date_col = df.columns[0]

        df["_date_str"] = df[date_col].astype(str).str.replace("-", "")

        # Find rows on or after the event date
        event_mask = df["_date_str"] >= date_norm
        if not event_mask.any():
            return self._empty_reality_comparison()

        post_event = df[event_mask].copy()
        if len(post_event) < 1:
            return self._empty_reality_comparison()

        # Price at event vs latest available
        close_col = "收盘"
        if close_col not in post_event.columns:
            return self._empty_reality_comparison()

        event_close = float(post_event[close_col].iloc[0])
        latest_close = float(post_event[close_col].iloc[-1])
        days_measured = len(post_event)

        if event_close == 0:
            return self._empty_reality_comparison()

        actual_change_pct = (latest_close / event_close - 1.0) * 100.0

        # Determine actual direction
        if actual_change_pct > 1.0:
            actual_direction = "up"
        elif actual_change_pct < -1.0:
            actual_direction = "down"
        else:
            actual_direction = "flat"

        # Compute reality gap
        simulated_sentiment = simulation.get("weighted_sentiment", 0.0)
        simulated_price_target = simulation.get("weighted_price_target_pct", 0.0)

        # Gap between what simulation predicted and what actually happened
        # Positive gap: market moved more positively than simulated
        # Negative gap: market moved more negatively than simulated
        if simulated_price_target != 0:
            reality_gap = actual_change_pct - simulated_price_target
        else:
            # Use sentiment as proxy: sentiment * 10 as rough expected move
            expected_move = simulated_sentiment * 10.0
            reality_gap = actual_change_pct - expected_move

        # Classify opportunity
        abs_gap = abs(reality_gap)
        if abs_gap < 2.0:
            opportunity_type = "none"
        elif reality_gap > 0 and simulated_sentiment < 0:
            opportunity_type = "underreaction_positive"  # market shrugged off bad news
        elif reality_gap < 0 and simulated_sentiment > 0:
            opportunity_type = "underreaction_negative"  # market ignored good news
        elif reality_gap > 0 and simulated_sentiment > 0:
            opportunity_type = "overreaction_positive"  # market overreacted to good news
        elif reality_gap < 0 and simulated_sentiment < 0:
            opportunity_type = "overreaction_negative"  # market overreacted to bad news
        else:
            opportunity_type = "divergence"

        return {
            "actual_price_change_pct": round(actual_change_pct, 2),
            "actual_direction": actual_direction,
            "reality_gap": round(reality_gap, 2),
            "opportunity_type": opportunity_type,
            "days_measured": days_measured,
        }

    # ==================================================================
    # 6. run
    # ==================================================================

    def run(self, stock_codes: list[str] | None = None) -> list[StrategySignal]:
        """Full sentiment simulation pipeline.

        1. Detect worthy events.
        2. For each event: generate profiles -> simulate reactions -> aggregate.
        3. Compare with reality.
        4. Generate signals where significant gaps are found.

        Parameters
        ----------
        stock_codes :
            Universe of stock codes.  If ``None``, scans market-wide.
        """
        logger.info(
            "[%s] Starting sentiment simulation for %s stocks ...",
            self.name,
            len(stock_codes) if stock_codes else "all",
        )

        # 1. Detect events
        events = self.detect_simulation_worthy_events(stock_codes=stock_codes)
        if not events:
            logger.info("[%s] No simulation-worthy events detected.", self.name)
            return []

        # 2-4. Process each event
        signals: list[StrategySignal] = []
        for event in events:
            try:
                event_signals = self._process_event(event)
                signals.extend(event_signals)
            except Exception as exc:
                logger.error(
                    "[%s] Failed to process event '%s': %s",
                    self.name, event.get("title", "?")[:30], exc,
                )

        logger.info("[%s] Generated %d signals total.", self.name, len(signals))
        return signals

    # ==================================================================
    # 7. run_single
    # ==================================================================

    def run_single(self, stock_code: str) -> list[StrategySignal]:
        """Run sentiment simulation for a single stock.

        Detects events specific to this stock, simulates agent reactions,
        and generates signals.

        Parameters
        ----------
        stock_code :
            6-digit stock code.

        Returns
        -------
        list[StrategySignal]
        """
        logger.info("[%s] Running simulation for %s ...", self.name, stock_code)

        events = self.detect_simulation_worthy_events(stock_codes=[stock_code])
        if not events:
            logger.info("[%s] No events for %s.", self.name, stock_code)
            return []

        # Filter to events matching this stock
        stock_events = [
            e for e in events if e.get("stock_code", "") == stock_code
        ]
        if not stock_events:
            # If no exact match, still try all detected events
            stock_events = events

        signals: list[StrategySignal] = []
        for event in stock_events:
            try:
                event_signals = self._process_event(event)
                signals.extend(event_signals)
            except Exception as exc:
                logger.error(
                    "[%s] Failed for event '%s': %s",
                    self.name, event.get("title", "?")[:30], exc,
                )

        return signals

    # ==================================================================
    # Graph context enrichment
    # ==================================================================

    def _get_graph_context(self, stock_code: str, stock_name: str) -> str:
        """Query local graph for supply-chain / industry context.

        Returns a text block injected into agent profiles so agents are
        aware of second-order effects that raw news may not mention.
        """
        if self._graph is None:
            return ""
        try:
            results = self._graph.search("astrategy", f"{stock_code} {stock_name}", limit=8)
            if not results:
                return ""
            lines = ["【知识图谱上下文】"]
            for r in results[:6]:
                fact = r.get("fact", "")
                src = r.get("source", "")
                tgt = r.get("target", "")
                rel = r.get("relation", "")
                if fact and len(fact) > 10:
                    lines.append(f"- {fact[:120]}")
                elif src and tgt:
                    lines.append(f"- {src} → [{rel}] → {tgt}")
            return "\n".join(lines) if len(lines) > 1 else ""
        except Exception as exc:
            logger.debug("[%s] Graph context query failed: %s", self.name, exc)
            return ""

    # ==================================================================
    # Multi-round simulation (MiroFish cascade model)
    # ==================================================================

    def simulate_multi_round_reactions(
        self, event: dict, profiles: list[dict],
    ) -> Dict[str, List[dict]]:
        """Three-round cascade simulation mirroring MiroFish's OASIS model.

        Round 1 — fast-money (游资/量化): react to raw event (no prior info)
        Round 2 — mid-speed (趋势/散户): observe round-1 outcome, decide
        Round 3 — deliberate (公募/价值): full picture, confirm or correct

        Returns
        -------
        dict with keys "round1", "round2", "round3", each a list of reactions.
        """
        profile_map = {p["archetype"]: p for p in profiles}

        # ── Round 1: fast-money ───────────────────────────────────────
        r1_profiles = [profile_map[a] for a in _ROUND1_ARCHETYPES if a in profile_map]
        reactions_r1 = self._simulate_round(event, r1_profiles, round_num=1)
        logger.info("[%s] Round1 done: %d reactions", self.name, len(reactions_r1))

        # Build round-1 summary for round-2 context
        r1_summary = self._format_round_summary(reactions_r1, "第一轮（快钱 T+0~3h）")

        # ── Round 2: mid-speed with round-1 context ───────────────────
        r2_profiles = [profile_map[a] for a in _ROUND2_ARCHETYPES if a in profile_map]
        reactions_r2 = self._simulate_observed_round(
            event=event,
            profiles=r2_profiles,
            prior_summary=r1_summary,
            prompt_template=_ROUND2_REACTIONS_PROMPT,
            round_num=2,
        )
        logger.info("[%s] Round2 done: %d reactions", self.name, len(reactions_r2))

        # Combined prior rounds for round 3
        r12_summary = (
            self._format_round_summary(reactions_r1, "第一轮（快钱）")
            + "\n"
            + self._format_round_summary(reactions_r2, "第二轮（中速）")
        )

        # ── Round 3: deliberate with full context ─────────────────────
        r3_profiles = [profile_map[a] for a in _ROUND3_ARCHETYPES if a in profile_map]
        reactions_r3 = self._simulate_observed_round(
            event=event,
            profiles=r3_profiles,
            prior_summary=r12_summary,
            prompt_template=_ROUND3_REACTIONS_PROMPT,
            round_num=3,
        )
        logger.info("[%s] Round3 done: %d reactions", self.name, len(reactions_r3))

        return {"round1": reactions_r1, "round2": reactions_r2, "round3": reactions_r3}

    def _simulate_round(self, event: dict, profiles: list[dict], round_num: int) -> list[dict]:
        """Run a single LLM call for a subset of archetypes (first-mover round)."""
        if not profiles:
            return []

        profile_lines = self._format_profile_lines(profiles)
        prompt = _SIMULATE_REACTIONS_PROMPT.format(
            event_title=event.get("title", ""),
            event_type=event.get("type", "other"),
            stock_code=event.get("stock_code", ""),
            stock_name=event.get("stock_name", ""),
            event_summary=event.get("summary", ""),
            key_data=event.get("key_data", "无"),
            profiles_text="\n\n".join(profile_lines),
        )
        messages = [
            {"role": "system", "content": "你是A股多参与者博弈模拟专家。严格以JSON格式输出。"},
            {"role": "user", "content": prompt},
        ]
        try:
            result = self._llm.chat_json(messages=messages, max_tokens=1500)
            raw = result.get("reactions", [])
        except Exception as exc:
            logger.warning("[%s] Round%d LLM failed: %s", self.name, round_num, exc)
            raw = self._rule_based_reactions(event, profiles)

        return self._validate_reactions(raw, profiles)

    def _simulate_observed_round(
        self,
        event: dict,
        profiles: list[dict],
        prior_summary: str,
        prompt_template: str,
        round_num: int,
    ) -> list[dict]:
        """Run a round where agents observe prior-round results."""
        if not profiles:
            return []

        profile_lines = self._format_profile_lines(profiles)
        archetype_list = ", ".join(p["archetype"] for p in profiles)
        prompt = prompt_template.format(
            event_title=event.get("title", ""),
            stock_code=event.get("stock_code", ""),
            stock_name=event.get("stock_name", ""),
            event_summary=event.get("summary", ""),
            round1_summary=prior_summary,
            prior_rounds_summary=prior_summary,
            target_archetypes=archetype_list,
            profiles_text="\n\n".join(profile_lines),
        )
        messages = [
            {"role": "system", "content": "你是A股多参与者博弈模拟专家。严格以JSON格式输出。"},
            {"role": "user", "content": prompt},
        ]
        try:
            result = self._llm.chat_json(messages=messages, max_tokens=1500)
            raw = result.get("reactions", [])
        except Exception as exc:
            logger.warning("[%s] Round%d LLM failed: %s", self.name, round_num, exc)
            raw = self._rule_based_reactions(event, profiles)

        return self._validate_reactions(raw, profiles)

    def _format_profile_lines(self, profiles: list[dict]) -> list[str]:
        lines = []
        for p in profiles:
            lines.append(
                f"### {p['archetype']}\n"
                f"描述: {p['description'][:60]}\n"
                f"当前持仓: {p['current_position']}（{p.get('position_reason', '')}）\n"
                f"投资逻辑: {p['investment_thesis']}\n"
                f"关注点: {', '.join(p.get('key_concerns', []))}\n"
                f"事件前情绪: {p['pre_event_sentiment']}"
            )
        return lines

    def _format_round_summary(self, reactions: list[dict], label: str) -> str:
        if not reactions:
            return f"{label}: 无反应"
        lines = [f"{label}:"]
        for r in reactions:
            arch = r.get("archetype", "")
            action = r.get("action", "hold")
            sentiment = r.get("sentiment_score", 0.0)
            price_target = r.get("price_target_change_pct", 0.0)
            reasoning = r.get("reasoning", "")[:60]
            lines.append(
                f"  [{arch}] {action} | 情绪{sentiment:+.2f} | "
                f"目标涨跌{price_target:+.1f}% | {reasoning}"
            )
        # Add weighted consensus
        if reactions:
            avg_s = sum(r.get("sentiment_score", 0.0) for r in reactions) / len(reactions)
            lines.append(f"  → 本轮均值情绪: {avg_s:+.3f}")
        return "\n".join(lines)

    def _validate_reactions(self, raw: list[dict], profiles: list[dict]) -> list[dict]:
        """Validate and normalise a list of raw LLM reactions."""
        archetype_names = {p["archetype"] for p in profiles}
        matched: set[str] = set()
        reactions: list[dict] = []

        for r in raw:
            arch = r.get("archetype", "")
            if arch not in archetype_names or arch in matched:
                continue
            matched.add(arch)
            reaction = {
                "archetype": arch,
                "action": r.get("action", "hold"),
                "urgency": r.get("urgency", "within_days"),
                "size": r.get("size", "light"),
                "sentiment_score": _clamp(r.get("sentiment_score", 0.0), -1.0, 1.0),
                "reasoning": r.get("reasoning", ""),
                "price_target_change_pct": r.get("price_target_change_pct", 0.0),
                "time_horizon_days": r.get("time_horizon_days", 10),
            }
            if reaction["action"] not in ("buy", "sell", "hold", "watch"):
                reaction["action"] = "hold"
            reactions.append(reaction)

        # Fill missing archetypes
        for p in profiles:
            if p["archetype"] not in matched:
                reactions.append({
                    "archetype": p["archetype"],
                    "action": "watch",
                    "urgency": "wait_for_confirmation",
                    "size": "light",
                    "sentiment_score": 0.0,
                    "reasoning": "模拟结果缺失，默认观望",
                    "price_target_change_pct": 0.0,
                    "time_horizon_days": 10,
                })
        return reactions

    def aggregate_multi_round(self, round_reactions: Dict[str, List[dict]]) -> dict:
        """Aggregate multi-round reactions into a single market consensus.

        Weights: Round-1 (fast-money) has lower final weight because they
        set the initial price; Rounds 2-3 determine whether the move sustains.
        The *convergence* across rounds is the key signal:
        - All rounds agree → high conviction
        - Round-3 reverses Round-1 → correction signal

        Round weights: R1=0.25, R2=0.35, R3=0.40 (later rounds more informative)
        """
        round_weights = {"round1": 0.25, "round2": 0.35, "round3": 0.40}
        all_flat: list[dict] = []  # for backward-compat aggregate_simulation
        round_sentiments: Dict[str, float] = {}

        for round_key, reactions in round_reactions.items():
            if not reactions:
                continue
            rw = round_weights.get(round_key, 0.33)
            round_avg = sum(r.get("sentiment_score", 0.0) for r in reactions) / len(reactions)
            round_sentiments[round_key] = round_avg
            # Inject round weight into each reaction for flat aggregation
            for r in reactions:
                r_copy = dict(r)
                r_copy["_round_weight"] = rw
                all_flat.append(r_copy)

        if not all_flat:
            return self.aggregate_simulation([])

        # Weighted aggregation
        total_weight = 0.0
        weighted_sentiment = 0.0
        weighted_price_target = 0.0
        action_votes: Dict[str, float] = {"buy": 0.0, "sell": 0.0, "hold": 0.0, "watch": 0.0}
        agent_summary: Dict[str, dict] = {}

        for r in all_flat:
            arch = r.get("archetype", "")
            influence = _INFLUENCE_WEIGHTS.get(arch, 0.05)
            rw = r.get("_round_weight", 0.33)
            w = influence * rw
            sentiment = r.get("sentiment_score", 0.0)
            action = r.get("action", "hold")
            price_target = float(r.get("price_target_change_pct", 0.0) or 0.0)

            weighted_sentiment += sentiment * w
            weighted_price_target += price_target * w
            total_weight += w
            if action in action_votes:
                action_votes[action] += w

            # Keep last round's reaction per archetype for summary
            agent_summary[arch] = {
                "action": action,
                "urgency": r.get("urgency", "within_days"),
                "size": r.get("size", "light"),
                "sentiment": round(sentiment, 2),
                "reasoning": r.get("reasoning", ""),
            }

        if total_weight > 0:
            weighted_sentiment /= total_weight
            weighted_price_target /= total_weight

        weighted_sentiment = _clamp(weighted_sentiment, -1.0, 1.0)
        consensus_action = max(action_votes, key=action_votes.get)  # type: ignore

        # Conviction: how much do rounds agree with each other?
        if len(round_sentiments) >= 2:
            vs = list(round_sentiments.values())
            mean_v = sum(vs) / len(vs)
            variance = sum((v - mean_v) ** 2 for v in vs) / len(vs)
            import math as _math
            std_dev = _math.sqrt(variance)
            conviction_level = max(0.0, 1.0 - std_dev * 1.5)
        else:
            conviction_level = abs(weighted_sentiment)

        # Trend: is later-round sentiment stronger/weaker than earlier?
        trend = "stable"
        r1s = round_sentiments.get("round1", 0.0)
        r3s = round_sentiments.get("round3", 0.0)
        if abs(r3s) > abs(r1s) + 0.15:
            trend = "amplifying"   # institutions confirm fast-money direction
        elif abs(r3s) < abs(r1s) - 0.15:
            trend = "fading"        # deliberate money reverses fast-money
        elif r3s * r1s < 0:
            trend = "reversal"      # direction flips by round 3

        all_sentiments = [r.get("sentiment_score", 0.0) for r in all_flat]
        agreement = all(s >= 0 for s in all_sentiments) or all(s <= 0 for s in all_sentiments)

        return {
            "weighted_sentiment": round(weighted_sentiment, 4),
            "consensus_action": consensus_action,
            "conviction_level": round(conviction_level, 4),
            "agreement": agreement,
            "agent_summary": agent_summary,
            "weighted_price_target_pct": round(weighted_price_target, 2),
            "round_sentiments": round_sentiments,
            "simulation_trend": trend,
        }

    # ==================================================================
    # Internal: process a single event end-to-end
    # ==================================================================

    def _process_event(self, event: dict) -> list[StrategySignal]:
        """Run the full simulation pipeline for one event.

        Returns
        -------
        list[StrategySignal]
            Zero or one signal.
        """
        stock_code = event.get("stock_code", "")
        stock_name = event.get("stock_name", stock_code)
        event_title = event.get("title", "未知事件")

        # 2. Enrich event with graph context (supply-chain second-order effects)
        graph_ctx = self._get_graph_context(stock_code, stock_name)
        if graph_ctx:
            event = dict(event)
            event["graph_context"] = graph_ctx
            event["summary"] = event.get("summary", "") + f"\n{graph_ctx}"
            logger.info("[%s] Injected graph context (%d chars) for %s",
                        self.name, len(graph_ctx), stock_code)

        # 3. Generate profiles
        profiles = self.generate_agent_profiles(event, stock_code)

        # 4. Simulate reactions (multi-round or single-round)
        if self._use_multi_round:
            round_reactions = self.simulate_multi_round_reactions(event, profiles)
            simulation = self.aggregate_multi_round(round_reactions)
            # Flatten for downstream signal building
            reactions = (
                round_reactions.get("round1", [])
                + round_reactions.get("round2", [])
                + round_reactions.get("round3", [])
            )
            simulation["multi_round"] = True
            simulation["round_reactions"] = {
                k: [r["archetype"] + "=" + r["action"] for r in v]
                for k, v in round_reactions.items()
            }
        else:
            reactions = self.simulate_reactions(event, profiles)
            simulation = self.aggregate_simulation(reactions)
            simulation["multi_round"] = False

        # 5. Compare with reality
        now = datetime.now(tz=_CST)
        event_date = now.strftime("%Y%m%d")
        reality = self.compare_with_reality(stock_code, simulation, event_date)

        # 6. Build signal
        signal = self._build_signal(
            stock_code=stock_code,
            stock_name=stock_name,
            event=event,
            simulation=simulation,
            reactions=reactions,
            reality=reality,
        )

        if signal is None:
            return []
        return [signal]

    # ==================================================================
    # Signal construction
    # ==================================================================

    def _build_signal(
        self,
        stock_code: str,
        stock_name: str,
        event: dict,
        simulation: dict,
        reactions: list[dict],
        reality: dict,
    ) -> Optional[StrategySignal]:
        """Build a StrategySignal from simulation results."""
        weighted_sentiment = simulation.get("weighted_sentiment", 0.0)
        consensus_action = simulation.get("consensus_action", "hold")
        conviction = simulation.get("conviction_level", 0.0)
        agreement = simulation.get("agreement", False)
        reality_gap = reality.get("reality_gap", 0.0)
        opportunity_type = reality.get("opportunity_type", "none")
        actual_change = reality.get("actual_price_change_pct")

        # Direction
        if consensus_action == "buy" and weighted_sentiment > 0.1:
            direction = "long"
        elif consensus_action == "sell" and weighted_sentiment < -0.1:
            direction = "short"
        elif weighted_sentiment > 0.3:
            direction = "long"
        elif weighted_sentiment < -0.3:
            direction = "short"
        else:
            direction = "neutral"

        # Adjust direction based on reality gap (contrarian on mispricing)
        if opportunity_type == "overreaction_positive" and actual_change is not None:
            # Market overshoot to upside -> short opportunity
            direction = "short"
        elif opportunity_type == "overreaction_negative" and actual_change is not None:
            # Market overshoot to downside -> long opportunity
            direction = "long"
        elif opportunity_type == "underreaction_positive" and actual_change is not None:
            # Bad news not priced in -> short
            direction = "short"
        elif opportunity_type == "underreaction_negative" and actual_change is not None:
            # Good news not priced in -> long
            direction = "long"

        # Skip neutral signals with no mispricing
        if direction == "neutral" and opportunity_type == "none":
            logger.debug(
                "[%s] %s: neutral consensus, no mispricing -> skip", self.name, stock_code,
            )
            return None

        # Confidence
        base_confidence = abs(weighted_sentiment) * 0.5
        conviction_bonus = conviction * 0.2
        agreement_bonus = 0.1 if agreement else 0.0
        gap_bonus = min(0.2, abs(reality_gap) / 20.0) if reality_gap else 0.0

        confidence = base_confidence + conviction_bonus + agreement_bonus + gap_bonus
        confidence = max(0.10, min(1.0, confidence))

        # Expected return
        price_target = simulation.get("weighted_price_target_pct", 0.0)
        if abs(price_target) > 0:
            expected_return = price_target / 100.0
        else:
            expected_return = weighted_sentiment * 0.05

        if direction == "short":
            expected_return = -abs(expected_return)
        elif direction == "long":
            expected_return = abs(expected_return)

        expected_return = max(-0.15, min(0.15, expected_return))

        # Holding period: faster reaction events get shorter holding periods
        avg_horizon = 0
        count = 0
        for r in reactions:
            h = r.get("time_horizon_days", 10)
            try:
                avg_horizon += int(h)
                count += 1
            except (ValueError, TypeError):
                pass
        holding_days = int(avg_horizon / count) if count > 0 else self._holding_days

        # Build reasoning
        sim_trend = simulation.get("simulation_trend", "stable")
        trend_label = {"amplifying": "机构确认", "fading": "情绪消退", "reversal": "方向反转", "stable": "稳定"}.get(sim_trend, sim_trend)
        multi_tag = "[多轮]" if simulation.get("multi_round") else "[单轮]"
        graph_tag = "[图谱]" if event.get("graph_context") else ""
        reasoning_parts = [
            f"{multi_tag}{graph_tag}事件: {event.get('title', '?')[:40]}",
            f"模拟共识: {consensus_action}({weighted_sentiment:+.2f}) 趋势:{trend_label}",
            f"一致性: {'高' if agreement else '低'}(conviction={conviction:.2f})",
        ]
        if actual_change is not None:
            reasoning_parts.append(f"实际涨跌: {actual_change:+.2f}%")
        if opportunity_type != "none":
            label_map = {
                "overreaction_positive": "市场过度上涨",
                "overreaction_negative": "市场过度下跌",
                "underreaction_positive": "利空未充分反映",
                "underreaction_negative": "利好未充分反映",
                "divergence": "模拟与现实背离",
            }
            reasoning_parts.append(f"机会: {label_map.get(opportunity_type, opportunity_type)}")

        reasoning = "; ".join(reasoning_parts)

        # Agent reactions summary
        agent_reactions = simulation.get("agent_summary", {})

        # Construct LLM reasoning from individual agent viewpoints
        llm_reasoning_parts: list[str] = []
        for r in reactions:
            arch = r.get("archetype", "")
            action = r.get("action", "hold")
            reasoning_text = r.get("reasoning", "")
            llm_reasoning_parts.append(f"[{arch}] {action}: {reasoning_text}")

        metadata: Dict[str, Any] = {
            "event_title": event.get("title", ""),
            "event_type": event.get("type", "other"),
            "simulated_sentiment": round(weighted_sentiment, 4),
            "simulated_action": consensus_action,
            "conviction_level": round(conviction, 4),
            "agent_reactions": agent_reactions,
            "actual_market_reaction": actual_change,
            "reality_gap": round(reality_gap, 2) if reality_gap else 0.0,
            "opportunity_type": opportunity_type,
            "llm_reasoning": "\n".join(llm_reasoning_parts),
            # Multi-round fields (populated when use_multi_round=True)
            "multi_round": simulation.get("multi_round", False),
            "round_sentiments": simulation.get("round_sentiments", {}),
            "simulation_trend": simulation.get("simulation_trend", "stable"),
            "round_reactions": simulation.get("round_reactions", {}),
            "graph_context_used": bool(event.get("graph_context")),
        }

        return StrategySignal(
            strategy_name=self.name,
            stock_code=stock_code,
            stock_name=stock_name,
            direction=direction,
            confidence=round(confidence, 2),
            expected_return=round(expected_return, 4),
            holding_period_days=holding_days,
            reasoning=reasoning,
            metadata=metadata,
        )

    # ==================================================================
    # Keyword pre-filter
    # ==================================================================

    def _keyword_prefilter(self, news_items: list[dict]) -> list[dict]:
        """Pre-filter news by high-impact keywords (no LLM cost)."""
        filtered: list[dict] = []
        for item in news_items:
            text = json.dumps(item, ensure_ascii=False)
            if any(kw in text for kw in _HIGH_IMPACT_KEYWORDS):
                filtered.append(item)

        # If no keyword matches, return top items anyway (LLM will filter)
        if not filtered and news_items:
            filtered = news_items[:15]

        return filtered

    # ==================================================================
    # Rule-based fallbacks
    # ==================================================================

    def _rule_based_event_detection(self, news_items: list[dict]) -> list[dict]:
        """Fallback event detection when LLM is unavailable."""
        events: list[dict] = []

        for item in news_items[:5]:
            title = item.get("新闻标题", item.get("标题", item.get("名称", "")))
            code = item.get("_stock_code", item.get("代码", ""))
            name = item.get("股票简称", item.get("名称", code))

            if not title or not code:
                continue

            # Classify by keywords
            event_type = "other"
            for kw, etype in [
                ("业绩", "earnings_shock"),
                ("政策", "policy_change"),
                ("处罚", "scandal"),
                ("立案", "scandal"),
                ("收购", "ma"),
                ("并购", "ma"),
                ("重组", "ma"),
            ]:
                if kw in title:
                    event_type = etype
                    break

            events.append({
                "title": str(title)[:100],
                "type": event_type,
                "stock_code": str(code),
                "stock_name": str(name),
                "impact_level": "high",
                "summary": str(title)[:100],
                "key_data": "",
            })

        return events

    def _rule_based_reactions(
        self, event: dict, profiles: list[dict],
    ) -> list[dict]:
        """Fallback reaction simulation when LLM is unavailable."""
        event_type = event.get("type", "other")

        # Default sentiment by event type
        base_sentiment: Dict[str, float] = {
            "earnings_shock": 0.0,   # depends on direction
            "policy_change": 0.1,
            "scandal": -0.6,
            "ma": 0.2,
            "black_swan": -0.5,
            "other": 0.0,
        }
        base = base_sentiment.get(event_type, 0.0)

        reactions: list[dict] = []
        for p in profiles:
            arch = p["archetype"]
            # Adjust based on archetype
            if arch == "游资/短线客":
                sentiment = base * 1.5  # amplify
                action = "buy" if sentiment > 0 else "sell"
                urgency = "immediate"
                size = "heavy"
            elif arch == "量化基金":
                sentiment = -base * 0.5  # contrarian / mean-revert
                action = "buy" if sentiment > 0 else "sell" if sentiment < -0.2 else "hold"
                urgency = "immediate"
                size = "moderate"
            elif arch == "散户":
                sentiment = base * 1.2  # overreact
                action = "buy" if sentiment > 0.1 else "sell" if sentiment < -0.1 else "watch"
                urgency = "within_days"
                size = "light"
            elif arch == "公募基金经理":
                sentiment = base * 0.7
                action = "hold"
                urgency = "wait_for_confirmation"
                size = "moderate"
            elif arch == "价值投资者":
                sentiment = base * 0.3
                action = "watch"
                urgency = "wait_for_confirmation"
                size = "light"
            else:  # 趋势跟随者
                sentiment = base * 0.8
                action = "watch"
                urgency = "within_days"
                size = "moderate"

            sentiment = _clamp(sentiment, -1.0, 1.0)

            reactions.append({
                "archetype": arch,
                "action": action,
                "urgency": urgency,
                "size": size,
                "sentiment_score": round(sentiment, 2),
                "reasoning": f"基于{event_type}事件的规则化模拟",
                "price_target_change_pct": round(sentiment * 5.0, 1),
                "time_horizon_days": 10,
            })

        return reactions

    @staticmethod
    def _empty_reality_comparison() -> dict:
        """Return a neutral reality comparison when data is unavailable."""
        return {
            "actual_price_change_pct": None,
            "actual_direction": "unknown",
            "reality_gap": 0.0,
            "opportunity_type": "none",
            "days_measured": 0,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clamp(value: Any, min_val: float, max_val: float) -> float:
    """Clamp a numeric value to [min_val, max_val]."""
    try:
        v = float(value)
    except (ValueError, TypeError):
        v = 0.0
    return max(min_val, min(max_val, v))
