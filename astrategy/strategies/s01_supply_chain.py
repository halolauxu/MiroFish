"""
Supply Chain Transmission Strategy (供应链传导策略)
====================================================
When an upstream company announces a price hike, supply shortage, capacity
expansion, or production halt, traverse the supply chain graph to find
affected downstream companies and evaluate the impact.

Signal logic
------------
1. Scan daily announcements and news for supply-chain keywords
   (涨价, 调价, 提价, 限产, 停产, 扩产, 产能, 缺货, 原材料).
2. Use LLM to confirm and classify the event.
3. Query the knowledge graph for downstream/partner companies via
   SUPPLIES_TO and COOPERATES_WITH edges.
4. Use LLM to evaluate cost impact, pass-through ability, alternative
   supplier availability, and estimated stock price impact for each
   downstream company.
5. Check whether the downstream stock has already reacted.
6. Generate signals for unreacted downstream companies.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from astrategy.data_collector.announcement import AnnouncementCollector
from astrategy.data_collector.market_data import MarketDataCollector
from astrategy.data_collector.news import NewsCollector
from astrategy.graph.builder import GraphBuilder
from astrategy.graph.local_store import LocalGraphStore
from astrategy.graph.topology import TopologyAnalyzer
from astrategy.llm import create_llm_client
from astrategy.strategies.base import BaseStrategy, StrategySignal

logger = logging.getLogger(__name__)

_CST = timezone(timedelta(hours=8))

# ---------------------------------------------------------------------------
# Supply-chain event keywords
# ---------------------------------------------------------------------------
SUPPLY_CHAIN_KEYWORDS: list[str] = [
    "涨价", "调价", "提价", "限产", "停产",
    "扩产", "产能", "缺货", "原材料",
    "供应紧张", "供不应求", "产能不足",
    "原料价格", "成本上升", "减产",
    "供应中断", "供应链", "断供",
]

# Mapping from keyword to a normalised event type
_KEYWORD_TO_EVENT_TYPE: dict[str, str] = {
    "涨价": "price_hike",
    "调价": "price_adjustment",
    "提价": "price_hike",
    "限产": "production_cut",
    "停产": "production_halt",
    "扩产": "capacity_expansion",
    "产能": "capacity_change",
    "缺货": "supply_shortage",
    "原材料": "raw_material",
    "供应紧张": "supply_shortage",
    "供不应求": "supply_shortage",
    "产能不足": "supply_shortage",
    "原料价格": "raw_material",
    "成本上升": "cost_increase",
    "减产": "production_cut",
    "供应中断": "supply_disruption",
    "供应链": "supply_chain",
    "断供": "supply_disruption",
}


def _today_str() -> str:
    """Return today's date in YYYYMMDD format (CST)."""
    return datetime.now(tz=_CST).strftime("%Y%m%d")


def _date_offset(date_str: str, days: int) -> str:
    """Offset a YYYYMMDD date string by *days* and return YYYYMMDD."""
    dt = datetime.strptime(date_str.replace("-", ""), "%Y%m%d")
    return (dt + timedelta(days=days)).strftime("%Y%m%d")


def _extract_title(item: dict) -> str:
    """Extract a title/headline from an announcement or news dict."""
    for key in ("公告标题", "标题", "title", "新闻标题"):
        val = item.get(key, "")
        if val:
            return str(val)
    return ""


def _extract_content(item: dict) -> str:
    """Extract body content from an announcement or news dict."""
    for key in ("新闻内容", "content", "内容", "摘要"):
        val = item.get(key, "")
        if val:
            return str(val)
    return ""


def _extract_code(item: dict) -> str:
    """Extract stock code from an announcement or news dict."""
    for key in ("代码", "code", "_stock_code", "股票代码"):
        val = item.get(key, "")
        if val:
            return str(val).strip()
    return ""


def _extract_name(item: dict) -> str:
    """Extract company/stock name from an announcement or news dict."""
    for key in ("名称", "name", "股票简称", "公司名称"):
        val = item.get(key, "")
        if val:
            return str(val).strip()
    return ""


def _match_keywords(text: str) -> list[str]:
    """Return supply-chain keywords found in *text*."""
    return [kw for kw in SUPPLY_CHAIN_KEYWORDS if kw in text]


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------


class SupplyChainStrategy(BaseStrategy):
    """Detect upstream supply-chain events and generate signals for
    affected downstream companies that have not yet reacted."""

    def __init__(
        self,
        graph_id: str = "supply_chain",
        signal_dir: Path | str | None = None,
        llm_client: LLMClient | None = None,
        graph_builder: GraphBuilder | None = None,
        lookback_days: int = 3,
        reaction_check_days: int = 5,
        min_confidence: float = 0.3,
    ) -> None:
        super().__init__(signal_dir=signal_dir)
        self._graph_id = graph_id
        self._lookback_days = lookback_days
        self._reaction_check_days = reaction_check_days
        self._min_confidence = min_confidence

        # Lazily initialised collaborators
        self._llm = llm_client
        self._graph = graph_builder
        self._announcements = AnnouncementCollector()
        self._news = NewsCollector()
        self._market = MarketDataCollector()
        self._topology = TopologyAnalyzer()

    # ── lazy init ──────────────────────────────────────────────

    @property
    def _llm_client(self):
        if self._llm is None:
            self._llm = create_llm_client(strategy_name=self.name)
        return self._llm

    @property
    def _graph_builder(self):
        if self._graph is None:
            # Prefer local graph store (no Zep dependency)
            local = LocalGraphStore()
            if local.load(self._graph_id):
                logger.info("Using local graph store for '%s'", self._graph_id)
                self._graph = local
            else:
                logger.info("Local graph not found, falling back to Zep GraphBuilder")
                self._graph = GraphBuilder()
        return self._graph

    # ── identity ───────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "s01_supply_chain"

    # ================================================================
    # 1. detect_supply_events
    # ================================================================

    def detect_supply_events(self, date: str | None = None) -> list[dict]:
        """Scan announcements and news for supply-chain events.

        Parameters
        ----------
        date : str | None
            Date to scan (YYYYMMDD).  Defaults to today (CST).

        Returns
        -------
        list[dict]
            Each dict has keys: event_company, event_code, event_type,
            event_detail, source, matched_keywords, date.
        """
        if date is None:
            date = _today_str()

        candidates: list[dict] = []

        # --- scan announcements ---
        try:
            announcements = self._announcements.get_daily_announcements(date)
            logger.info(
                "Fetched %d announcements for %s", len(announcements), date
            )
        except Exception as exc:
            logger.error("Failed to fetch announcements for %s: %s", date, exc)
            announcements = []

        for ann in announcements:
            title = _extract_title(ann)
            content = _extract_content(ann)
            text = f"{title} {content}"
            matched = _match_keywords(text)
            if matched:
                candidates.append({
                    "event_company": _extract_name(ann),
                    "event_code": _extract_code(ann),
                    "title": title,
                    "content": content[:500],
                    "matched_keywords": matched,
                    "source": "announcement",
                    "date": date,
                })

        # --- scan recent news (hot topics may contain supply-chain events) ---
        try:
            hot_topics = self._news.get_market_hot_topics(limit=50)
        except Exception:
            hot_topics = []

        for item in hot_topics:
            title = _extract_title(item)
            content = _extract_content(item)
            text = f"{title} {content}"
            matched = _match_keywords(text)
            if matched:
                candidates.append({
                    "event_company": _extract_name(item),
                    "event_code": _extract_code(item),
                    "title": title,
                    "content": content[:500],
                    "matched_keywords": matched,
                    "source": "news",
                    "date": date,
                })

        if not candidates:
            logger.info("No supply-chain event candidates found for %s", date)
            return []

        # --- LLM confirmation and classification ---
        confirmed_events = self._confirm_events_with_llm(candidates)
        logger.info(
            "Confirmed %d / %d supply-chain events for %s",
            len(confirmed_events), len(candidates), date,
        )
        return confirmed_events

    def _confirm_events_with_llm(self, candidates: list[dict]) -> list[dict]:
        """Use LLM to confirm and classify candidate supply-chain events."""
        confirmed: list[dict] = []

        # Process in batches to reduce LLM calls
        batch_size = 5
        for i in range(0, len(candidates), batch_size):
            batch = candidates[i : i + batch_size]
            events_text = ""
            for idx, c in enumerate(batch):
                events_text += (
                    f"\n事件{idx + 1}:\n"
                    f"  公司: {c.get('event_company', '未知')}\n"
                    f"  标题: {c.get('title', '')}\n"
                    f"  内容: {c.get('content', '')[:300]}\n"
                    f"  匹配关键词: {', '.join(c.get('matched_keywords', []))}\n"
                )

            messages = [
                {
                    "role": "system",
                    "content": (
                        "你是供应链分析专家。判断以下事件是否为真实的供应链事件"
                        "（涨价/供应短缺/产能变化等），并分类。\n"
                        "对于每个事件，返回JSON数组，每个元素包含:\n"
                        '  "index": 事件编号(从1开始),\n'
                        '  "is_supply_event": true/false,\n'
                        '  "event_type": 事件类型(price_hike/supply_shortage/'
                        "production_cut/production_halt/capacity_expansion/"
                        'cost_increase/supply_disruption/other),\n'
                        '  "event_detail": 简要描述事件对供应链的影响,\n'
                        '  "confidence": 置信度(0.0-1.0)\n'
                        '返回格式: {"events": [...]}'
                    ),
                },
                {
                    "role": "user",
                    "content": f"请分析以下事件:\n{events_text}",
                },
            ]

            try:
                result = self._llm_client.chat_json(
                    messages=messages, temperature=0.2, max_tokens=2000,
                )
                llm_events = result.get("events", [])
            except Exception as exc:
                logger.warning("LLM event confirmation failed: %s", exc)
                # Fall back to keyword-based classification
                for c in batch:
                    kws = c.get("matched_keywords", [])
                    if kws:
                        event_type = _KEYWORD_TO_EVENT_TYPE.get(kws[0], "other")
                        confirmed.append({
                            "event_company": c.get("event_company", ""),
                            "event_code": c.get("event_code", ""),
                            "event_type": event_type,
                            "event_detail": c.get("title", ""),
                            "source": c.get("source", ""),
                            "date": c.get("date", ""),
                            "confidence": 0.5,
                        })
                continue

            for ev in llm_events:
                idx = ev.get("index", 0) - 1
                if 0 <= idx < len(batch) and ev.get("is_supply_event"):
                    c = batch[idx]
                    confirmed.append({
                        "event_company": c.get("event_company", ""),
                        "event_code": c.get("event_code", ""),
                        "event_type": ev.get("event_type", "other"),
                        "event_detail": ev.get("event_detail", c.get("title", "")),
                        "source": c.get("source", ""),
                        "date": c.get("date", ""),
                        "confidence": float(ev.get("confidence", 0.6)),
                    })

        return confirmed

    # ================================================================
    # 2. find_affected_companies
    # ================================================================

    def find_affected_companies(
        self,
        event_company: str,
        graph_id: str | None = None,
    ) -> list[dict]:
        """Find downstream / partner companies affected by the event.

        Queries the knowledge graph for:
        - event_company -> SUPPLIES_TO -> downstream companies
        - event_company -> COOPERATES_WITH -> partners

        Uses multi-hop topology traversal for indirect effects.

        Parameters
        ----------
        event_company : str
            Name of the company that triggered the event.
        graph_id : str | None
            Graph to query.  Defaults to ``self._graph_id``.

        Returns
        -------
        list[dict]
            Each dict: company_name, relationship_type, relationship_detail,
            hop_distance, supply_ratio (if available).
        """
        gid = graph_id or self._graph_id
        affected: list[dict] = []
        seen_names: set[str] = set()

        # --- direct graph search for supply relationships ---
        try:
            supply_results = self._graph_builder.search(
                graph_id=gid,
                query=f"{event_company} 供应 下游 客户",
                limit=20,
            )
            for r in supply_results:
                source = r.get("source", "")
                target = r.get("target", "")
                relation = r.get("relation", "")
                fact = r.get("fact", "")

                # Identify downstream company
                downstream = ""
                if event_company.lower() in source.lower():
                    downstream = target
                elif event_company.lower() in target.lower():
                    # Reverse relationship — event_company is downstream
                    # Skip: we want companies that event_company supplies TO
                    continue

                if downstream and downstream not in seen_names:
                    seen_names.add(downstream)
                    affected.append({
                        "company_name": downstream,
                        "relationship_type": relation or "SUPPLIES_TO",
                        "relationship_detail": fact,
                        "hop_distance": 1,
                        "supply_ratio": None,
                    })
        except Exception as exc:
            logger.warning("Graph supply search failed: %s", exc)

        # --- search for cooperating partners ---
        try:
            partner_results = self._graph_builder.search(
                graph_id=gid,
                query=f"{event_company} 合作 伙伴 合资",
                limit=10,
            )
            for r in partner_results:
                source = r.get("source", "")
                target = r.get("target", "")
                fact = r.get("fact", "")

                partner = ""
                if event_company.lower() in source.lower():
                    partner = target
                elif event_company.lower() in target.lower():
                    partner = source

                if partner and partner not in seen_names:
                    seen_names.add(partner)
                    affected.append({
                        "company_name": partner,
                        "relationship_type": "COOPERATES_WITH",
                        "relationship_detail": fact,
                        "hop_distance": 1,
                        "supply_ratio": None,
                    })
        except Exception as exc:
            logger.warning("Graph partner search failed: %s", exc)

        # --- multi-hop neighbours via topology ---
        try:
            all_edges = self._graph_builder.get_all_edges(graph_id=gid)
            neighbours_2hop = self._topology.get_neighbors(
                edges=all_edges, node_id=event_company, depth=2,
            )
            for n in neighbours_2hop:
                if n not in seen_names:
                    seen_names.add(n)
                    # Determine hop distance
                    path = self._topology.shortest_path(
                        edges=all_edges, source=event_company, target=n,
                    )
                    hop = len(path) - 1 if path else 2
                    affected.append({
                        "company_name": n,
                        "relationship_type": "INDIRECT",
                        "relationship_detail": (
                            f"间接关联 (路径: {' -> '.join(path)})" if path else "间接关联"
                        ),
                        "hop_distance": hop,
                        "supply_ratio": None,
                    })
        except Exception as exc:
            logger.warning("Multi-hop topology search failed: %s", exc)

        logger.info(
            "Found %d affected companies for event_company='%s'",
            len(affected), event_company,
        )
        return affected

    # ================================================================
    # 3. evaluate_impact
    # ================================================================

    def evaluate_impact(self, event: dict, downstream: dict) -> dict:
        """Use LLM to evaluate the impact of a supply-chain event on a
        downstream company.

        Parameters
        ----------
        event : dict
            Supply-chain event dict (from ``detect_supply_events``).
        downstream : dict
            Downstream company dict (from ``find_affected_companies``).

        Returns
        -------
        dict
            Keys: direction, confidence, expected_return_pct,
            cost_impact_pct, pass_through_ability, alternative_suppliers,
            transmission_delay_days, holding_period_days, reasoning.
        """
        messages = [
            {
                "role": "system",
                "content": (
                    "你是资深A股供应链分析师。请评估上游供应链事件对下游公司股价的影响。\n\n"
                    "分析维度:\n"
                    "1. 成本冲击幅度: 估计对下游公司成本端的影响百分比\n"
                    "2. 成本转嫁能力: 下游公司是否能将成本上涨转嫁给终端客户 (high/medium/low)\n"
                    "3. 替代供应商: 下游公司是否有替代供应来源，数量估计\n"
                    "4. 股价影响: 预计对股价的影响方向和幅度\n"
                    "5. 传导时滞: 从上游事件到下游股价反应的预估天数\n\n"
                    "返回JSON格式:\n"
                    "{\n"
                    '  "direction": "long"或"short"或"neutral",\n'
                    '  "confidence": 0.0-1.0,\n'
                    '  "expected_return_pct": 预期收益率(如0.05表示5%),\n'
                    '  "cost_impact_pct": 成本影响百分比(如0.03表示3%),\n'
                    '  "pass_through_ability": "high"/"medium"/"low",\n'
                    '  "alternative_suppliers": 替代供应商数量(整数),\n'
                    '  "transmission_delay_days": 传导时滞天数(整数),\n'
                    '  "holding_period_days": 建议持仓天数(整数),\n'
                    '  "reasoning": "分析推理过程"\n'
                    "}"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"上游事件:\n"
                    f"  公司: {event.get('event_company', '未知')}\n"
                    f"  事件类型: {event.get('event_type', '未知')}\n"
                    f"  事件详情: {event.get('event_detail', '无')}\n"
                    f"  日期: {event.get('date', '')}\n\n"
                    f"下游公司:\n"
                    f"  公司: {downstream.get('company_name', '未知')}\n"
                    f"  供应关系: {downstream.get('relationship_type', '未知')}\n"
                    f"  关系详情: {downstream.get('relationship_detail', '无')}\n"
                    f"  跳数距离: {downstream.get('hop_distance', 1)}\n"
                    f"  供应占比: {downstream.get('supply_ratio', '未知')}\n\n"
                    f"请评估此供应链事件对下游公司的影响。"
                ),
            },
        ]

        try:
            result = self._llm_client.chat_json(
                messages=messages, temperature=0.3, max_tokens=1500,
            )
        except Exception as exc:
            logger.warning(
                "LLM impact evaluation failed for %s: %s",
                downstream.get("company_name", "?"), exc,
            )
            # Return a conservative default assessment
            return {
                "direction": "neutral",
                "confidence": 0.3,
                "expected_return_pct": 0.0,
                "cost_impact_pct": 0.0,
                "pass_through_ability": "medium",
                "alternative_suppliers": 0,
                "transmission_delay_days": 5,
                "holding_period_days": 10,
                "reasoning": f"LLM评估失败: {exc}",
            }

        # Normalise and validate
        direction = result.get("direction", "neutral")
        if direction not in ("long", "short", "neutral"):
            direction = "neutral"

        confidence = max(0.0, min(1.0, float(result.get("confidence", 0.5))))
        expected_return = float(result.get("expected_return_pct", 0.0))
        cost_impact = float(result.get("cost_impact_pct", 0.0))
        pass_through = result.get("pass_through_ability", "medium")
        if pass_through not in ("high", "medium", "low"):
            pass_through = "medium"
        alt_suppliers = int(result.get("alternative_suppliers", 0))
        delay_days = int(result.get("transmission_delay_days", 5))
        holding_days = int(result.get("holding_period_days", 10))
        reasoning = result.get("reasoning", "")

        return {
            "direction": direction,
            "confidence": confidence,
            "expected_return_pct": expected_return,
            "cost_impact_pct": cost_impact,
            "pass_through_ability": pass_through,
            "alternative_suppliers": alt_suppliers,
            "transmission_delay_days": delay_days,
            "holding_period_days": holding_days,
            "reasoning": reasoning,
        }

    # ================================================================
    # 4. check_price_reaction
    # ================================================================

    def check_price_reaction(
        self,
        stock_code: str,
        event_date: str,
        days: int = 5,
    ) -> dict:
        """Check whether *stock_code* has already reacted to an event.

        Parameters
        ----------
        stock_code : str
            6-digit A-share stock code.
        event_date : str
            Date of the supply-chain event (YYYYMMDD).
        days : int
            Number of trading days after the event to check.

        Returns
        -------
        dict
            Keys: reacted (bool), post_event_return (float),
            max_drawdown (float), avg_volume_change (float).
        """
        start = event_date
        end = _date_offset(event_date, days + 5)  # extra buffer for non-trading days

        try:
            df = self._market.get_daily_quotes(
                code=stock_code, start=start, end=end,
            )
        except Exception as exc:
            logger.warning(
                "Failed to fetch quotes for %s: %s", stock_code, exc,
            )
            return {
                "reacted": False,
                "post_event_return": 0.0,
                "max_drawdown": 0.0,
                "avg_volume_change": 0.0,
            }

        if df is None or df.empty or len(df) < 2:
            return {
                "reacted": False,
                "post_event_return": 0.0,
                "max_drawdown": 0.0,
                "avg_volume_change": 0.0,
            }

        # Use the first row as the baseline (event date or next trading day)
        close_col = "收盘" if "收盘" in df.columns else "close"
        volume_col = "成交量" if "成交量" in df.columns else "volume"
        pct_col = "涨跌幅" if "涨跌幅" in df.columns else None

        try:
            closes = df[close_col].astype(float).tolist()
        except (KeyError, ValueError):
            return {
                "reacted": False,
                "post_event_return": 0.0,
                "max_drawdown": 0.0,
                "avg_volume_change": 0.0,
            }

        base_price = closes[0]
        if base_price <= 0:
            return {
                "reacted": False,
                "post_event_return": 0.0,
                "max_drawdown": 0.0,
                "avg_volume_change": 0.0,
            }

        # Post-event return
        end_price = closes[min(days, len(closes) - 1)]
        post_event_return = (end_price - base_price) / base_price

        # Max drawdown from base
        cumulative_returns = [(c - base_price) / base_price for c in closes]
        max_drawdown = min(cumulative_returns) if cumulative_returns else 0.0

        # Volume change
        avg_volume_change = 0.0
        if volume_col in df.columns:
            try:
                volumes = df[volume_col].astype(float).tolist()
                if len(volumes) >= 2 and volumes[0] > 0:
                    post_volumes = volumes[1 : days + 1]
                    if post_volumes:
                        avg_post_vol = sum(post_volumes) / len(post_volumes)
                        avg_volume_change = (avg_post_vol - volumes[0]) / volumes[0]
            except (ValueError, ZeroDivisionError):
                pass

        # Consider the stock "reacted" if it moved more than 3% in either
        # direction or volume surged by more than 50%
        reacted = abs(post_event_return) > 0.03 or avg_volume_change > 0.5

        return {
            "reacted": reacted,
            "post_event_return": round(post_event_return, 4),
            "max_drawdown": round(max_drawdown, 4),
            "avg_volume_change": round(avg_volume_change, 4),
        }

    # ================================================================
    # 5. run
    # ================================================================

    def run(self, stock_codes: list[str] | None = None) -> list[StrategySignal]:
        """Run the full supply-chain transmission strategy.

        Pipeline:
        1. Detect supply-chain events over the lookback window.
        2. For each event, find affected downstream companies.
        3. Evaluate impact on each downstream company.
        4. Check whether the stock has already reacted.
        5. Generate signals for unreacted companies that pass the
           confidence threshold.

        Parameters
        ----------
        stock_codes : list[str] | None
            If provided, only generate signals for these stock codes.
            Otherwise, generate signals for all affected companies found.

        Returns
        -------
        list[StrategySignal]
        """
        signals: list[StrategySignal] = []
        today = _today_str()

        # Step 1: Detect events over the lookback window
        all_events: list[dict] = []
        for offset in range(self._lookback_days):
            scan_date = _date_offset(today, -offset)
            events = self.detect_supply_events(date=scan_date)
            all_events.extend(events)

        if not all_events:
            logger.info("No supply-chain events detected in the lookback window.")
            return signals

        logger.info("Detected %d supply-chain events total.", len(all_events))

        # Deduplicate events by (company, event_type)
        seen_events: set[tuple[str, str]] = set()
        unique_events: list[dict] = []
        for ev in all_events:
            key = (ev.get("event_company", ""), ev.get("event_type", ""))
            if key not in seen_events and key[0]:
                seen_events.add(key)
                unique_events.append(ev)

        # Step 2-5: Process each event
        for event in unique_events:
            event_company = event.get("event_company", "")
            if not event_company:
                continue

            # Find affected downstream companies
            affected = self.find_affected_companies(event_company)
            if not affected:
                logger.info(
                    "No affected companies found for '%s'", event_company
                )
                continue

            for downstream in affected:
                company_name = downstream.get("company_name", "")
                if not company_name:
                    continue

                # If stock_codes filter is provided, try to match
                # We need to resolve company_name -> stock_code
                stock_code = self._resolve_stock_code(company_name)
                if stock_codes is not None and stock_code not in stock_codes:
                    continue

                if not stock_code:
                    logger.debug(
                        "Could not resolve stock code for '%s', skipping.",
                        company_name,
                    )
                    continue

                # Evaluate impact
                impact = self.evaluate_impact(event, downstream)

                # Check price reaction
                event_date = event.get("date", today)
                reaction = self.check_price_reaction(
                    stock_code=stock_code,
                    event_date=event_date,
                    days=self._reaction_check_days,
                )

                # Generate signal only if stock has NOT reacted and
                # confidence exceeds threshold
                if reaction.get("reacted", False):
                    logger.debug(
                        "Stock %s has already reacted (return=%.2f%%), skipping.",
                        stock_code,
                        reaction.get("post_event_return", 0) * 100,
                    )
                    continue

                confidence = impact.get("confidence", 0.0)
                if confidence < self._min_confidence:
                    continue

                direction = impact.get("direction", "neutral")
                if direction == "neutral":
                    continue

                stock_name = self._resolve_stock_name(stock_code) or company_name

                signal = StrategySignal(
                    strategy_name=self.name,
                    stock_code=stock_code,
                    stock_name=stock_name,
                    direction=direction,
                    confidence=confidence,
                    expected_return=impact.get("expected_return_pct", 0.0),
                    holding_period_days=impact.get("holding_period_days", 10),
                    reasoning=(
                        f"上游{event_company}发生{event.get('event_type', '')}事件"
                        f"({event.get('event_detail', '')})，"
                        f"通过{downstream.get('relationship_type', '')}关系影响"
                        f"{company_name}。"
                        f"成本冲击{impact.get('cost_impact_pct', 0) * 100:.1f}%，"
                        f"转嫁能力{impact.get('pass_through_ability', '')}。"
                    ),
                    metadata={
                        "upstream_company": event_company,
                        "event_type": event.get("event_type", ""),
                        "event_detail": event.get("event_detail", ""),
                        "supply_relationship": downstream.get(
                            "relationship_type", ""
                        ),
                        "cost_impact_pct": impact.get("cost_impact_pct", 0.0),
                        "pass_through_ability": impact.get(
                            "pass_through_ability", ""
                        ),
                        "alternative_suppliers": impact.get(
                            "alternative_suppliers", 0
                        ),
                        "transmission_delay_days": impact.get(
                            "transmission_delay_days", 5
                        ),
                        "hop_distance": downstream.get("hop_distance", 1),
                        "post_event_return": reaction.get(
                            "post_event_return", 0.0
                        ),
                        "llm_reasoning": impact.get("reasoning", ""),
                    },
                )
                signals.append(signal)

        logger.info(
            "Supply chain strategy generated %d signals.", len(signals)
        )
        return signals

    # ================================================================
    # 6. run_single
    # ================================================================

    def run_single(self, stock_code: str) -> list[StrategySignal]:
        """Check if *stock_code* is downstream of any recent supply events.

        Instead of scanning all events and finding downstream companies,
        this method:
        1. Looks up the company in the graph.
        2. Finds its upstream suppliers.
        3. Checks if any of those suppliers have recent supply-chain events.
        4. Evaluates impact and generates signals.

        Parameters
        ----------
        stock_code : str
            6-digit A-share stock code.

        Returns
        -------
        list[StrategySignal]
        """
        signals: list[StrategySignal] = []
        today = _today_str()

        stock_name = self._resolve_stock_name(stock_code) or stock_code

        # Find upstream suppliers for this stock via graph search
        upstream_companies = self._find_upstream_suppliers(stock_name)
        if not upstream_companies:
            logger.info(
                "No upstream suppliers found for %s (%s).",
                stock_code, stock_name,
            )
            return signals

        # Detect events over the lookback window
        all_events: list[dict] = []
        for offset in range(self._lookback_days):
            scan_date = _date_offset(today, -offset)
            events = self.detect_supply_events(date=scan_date)
            all_events.extend(events)

        if not all_events:
            return signals

        # Check if any upstream supplier has a supply-chain event
        for upstream in upstream_companies:
            upstream_name = upstream.get("company_name", "")
            matching_events = [
                ev for ev in all_events
                if ev.get("event_company", "") and (
                    upstream_name in ev.get("event_company", "")
                    or ev.get("event_company", "") in upstream_name
                )
            ]

            for event in matching_events:
                # Build a downstream dict for this stock
                downstream = {
                    "company_name": stock_name,
                    "relationship_type": upstream.get(
                        "relationship_type", "SUPPLIES_TO"
                    ),
                    "relationship_detail": upstream.get(
                        "relationship_detail", ""
                    ),
                    "hop_distance": upstream.get("hop_distance", 1),
                    "supply_ratio": upstream.get("supply_ratio"),
                }

                impact = self.evaluate_impact(event, downstream)

                event_date = event.get("date", today)
                reaction = self.check_price_reaction(
                    stock_code=stock_code,
                    event_date=event_date,
                    days=self._reaction_check_days,
                )

                if reaction.get("reacted", False):
                    continue

                confidence = impact.get("confidence", 0.0)
                if confidence < self._min_confidence:
                    continue

                direction = impact.get("direction", "neutral")
                if direction == "neutral":
                    continue

                signal = StrategySignal(
                    strategy_name=self.name,
                    stock_code=stock_code,
                    stock_name=stock_name,
                    direction=direction,
                    confidence=confidence,
                    expected_return=impact.get("expected_return_pct", 0.0),
                    holding_period_days=impact.get("holding_period_days", 10),
                    reasoning=(
                        f"上游{upstream_name}发生{event.get('event_type', '')}事件，"
                        f"通过{downstream['relationship_type']}关系影响{stock_name}。"
                    ),
                    metadata={
                        "upstream_company": upstream_name,
                        "event_type": event.get("event_type", ""),
                        "event_detail": event.get("event_detail", ""),
                        "supply_relationship": downstream["relationship_type"],
                        "cost_impact_pct": impact.get("cost_impact_pct", 0.0),
                        "pass_through_ability": impact.get(
                            "pass_through_ability", ""
                        ),
                        "alternative_suppliers": impact.get(
                            "alternative_suppliers", 0
                        ),
                        "transmission_delay_days": impact.get(
                            "transmission_delay_days", 5
                        ),
                        "hop_distance": downstream["hop_distance"],
                        "post_event_return": reaction.get(
                            "post_event_return", 0.0
                        ),
                        "llm_reasoning": impact.get("reasoning", ""),
                    },
                )
                signals.append(signal)

        logger.info(
            "Supply chain strategy (single) generated %d signals for %s.",
            len(signals), stock_code,
        )
        return signals

    # ================================================================
    # Internal helpers
    # ================================================================

    def _find_upstream_suppliers(self, company_name: str) -> list[dict]:
        """Find upstream suppliers for a company via graph search."""
        suppliers: list[dict] = []
        seen: set[str] = set()

        try:
            results = self._graph_builder.search(
                graph_id=self._graph_id,
                query=f"{company_name} 的上游供应商 原材料供应",
                limit=15,
            )
            for r in results:
                source = r.get("source", "")
                target = r.get("target", "")
                fact = r.get("fact", "")
                relation = r.get("relation", "")

                # The supplier is whoever supplies TO this company
                upstream = ""
                if company_name.lower() in target.lower():
                    upstream = source
                elif company_name.lower() in source.lower():
                    upstream = target

                if upstream and upstream not in seen:
                    seen.add(upstream)
                    suppliers.append({
                        "company_name": upstream,
                        "relationship_type": relation or "SUPPLIES_TO",
                        "relationship_detail": fact,
                        "hop_distance": 1,
                        "supply_ratio": None,
                    })
        except Exception as exc:
            logger.warning(
                "Upstream supplier search failed for '%s': %s",
                company_name, exc,
            )

        return suppliers

    def _resolve_stock_code(self, company_name: str) -> str:
        """Attempt to resolve a company name to its 6-digit stock code.

        Uses the graph search first, then falls back to a realtime quote
        lookup to find a matching name.
        """
        # Try graph search for stock code
        try:
            results = self._graph_builder.search(
                graph_id=self._graph_id,
                query=f"{company_name} 股票代码",
                limit=3,
            )
            for r in results:
                fact = r.get("fact", "")
                # Try to extract a 6-digit code from the fact
                match = re.search(r"\b(\d{6})\b", fact)
                if match:
                    return match.group(1)
        except Exception:
            pass

        # Fallback: search realtime quotes by name
        try:
            df = self._market.get_realtime_quotes()
            if df is not None and not df.empty and "名称" in df.columns:
                mask = df["名称"].str.contains(company_name[:2], na=False)
                matches = df[mask]
                if not matches.empty and "代码" in matches.columns:
                    return str(matches.iloc[0]["代码"])
        except Exception:
            pass

        return ""

    def _resolve_stock_name(self, stock_code: str) -> str:
        """Resolve a stock code to its short name via realtime quotes."""
        try:
            df = self._market.get_realtime_quotes(codes=[stock_code])
            if df is not None and not df.empty and "名称" in df.columns:
                return str(df.iloc[0]["名称"])
        except Exception:
            pass
        return ""
