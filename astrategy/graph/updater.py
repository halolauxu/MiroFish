"""
Incremental graph updater for A-share knowledge graphs.

Provides methods to update company data, institutional holdings,
events, and price snapshots without full graph rebuilds.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from .builder import GraphBuilder, _rate_limited_call

logger = logging.getLogger("astrategy.graph.updater")


class GraphUpdater:
    """
    Incremental update interface for A-share knowledge graphs.

    Wraps GraphBuilder to provide domain-specific update operations
    that translate structured data into Zep graph episodes.
    """

    def __init__(self, builder: Optional[GraphBuilder] = None):
        self._builder = builder or GraphBuilder()

    @property
    def client(self):
        return self._builder.client

    # ── company data ───────────────────────────────────────────

    def update_company_data(
        self,
        graph_id: str,
        code: str,
        data: Dict[str, Any],
    ) -> None:
        """
        Update a company's fundamental or profile data.

        Args:
            graph_id: Target graph.
            code: Stock code (e.g. "600519").
            data: Updated fields, e.g.:
                  {"name": "贵州茅台", "pe_ratio": 25.3,
                   "market_cap": 22000, "revenue_growth": 0.15}
        """
        parts = [f"[公司数据更新] 股票代码: {code}"]
        for key, value in data.items():
            parts.append(f"{key}: {value}")
        parts.append(f"更新时间: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')}")

        text = "; ".join(parts)

        _rate_limited_call(
            self._builder.client.graph.add,
            user_id=graph_id,
            data=text,
            type="text",
            operation_name=f"update company {code}",
        )
        logger.info("Updated company data for %s in graph '%s'", code, graph_id)

    # ── institutional holdings ─────────────────────────────────

    def update_holdings(
        self,
        graph_id: str,
        institution: str,
        holdings: List[Dict[str, Any]],
    ) -> None:
        """
        Update institutional holding relationships.

        Args:
            graph_id: Target graph.
            institution: Institution name (e.g. "社保基金一零三组合").
            holdings: List of holding dicts, each with:
                      {"code": "600519", "name": "贵州茅台",
                       "shares": 1500000, "pct": 0.012,
                       "change": "增持"}
        """
        lines = [f"[机构持仓更新] 机构: {institution}"]
        for h in holdings:
            line = (
                f"持有 {h.get('name', h.get('code', ''))} "
                f"({h.get('code', '')}): "
                f"{h.get('shares', 0)} 股, "
                f"占比 {h.get('pct', 0):.2%}"
            )
            change = h.get("change", "")
            if change:
                line += f", 变动: {change}"
            lines.append(line)

        lines.append(f"更新时间: {datetime.utcnow().strftime('%Y-%m-%d')}")
        text = "\n".join(lines)

        _rate_limited_call(
            self._builder.client.graph.add,
            user_id=graph_id,
            data=text,
            type="text",
            operation_name=f"update holdings for {institution}",
        )
        logger.info(
            "Updated %d holdings for %s in graph '%s'",
            len(holdings),
            institution,
            graph_id,
        )

    # ── events ─────────────────────────────────────────────────

    def update_events(
        self,
        graph_id: str,
        events: List[Dict[str, Any]],
    ) -> None:
        """
        Ingest event data (news, announcements, policy changes).

        Args:
            graph_id: Target graph.
            events: List of event dicts, each with:
                    {"date": "2025-03-15", "type": "announcement",
                     "title": "...", "content": "...",
                     "entities": ["贵州茅台", "白酒行业"]}
        """
        texts = []
        for event in events:
            parts = [
                f"[事件] 日期: {event.get('date', '')}",
                f"类型: {event.get('type', '新闻')}",
                f"标题: {event.get('title', '')}",
            ]
            content = event.get("content", "")
            if content:
                parts.append(f"内容: {content}")
            entities = event.get("entities", [])
            if entities:
                parts.append(f"相关实体: {', '.join(entities)}")
            texts.append("; ".join(parts))

        # Use batch episode ingestion
        self._builder.add_episodes(graph_id, texts, batch_size=3)
        logger.info("Updated %d events in graph '%s'", len(events), graph_id)

    # ── price snapshots ────────────────────────────────────────

    def update_prices(
        self,
        graph_id: str,
        prices: List[Dict[str, Any]],
    ) -> None:
        """
        Ingest daily price snapshots as graph episodes.

        Price data enriches company nodes with temporal market context.

        Args:
            graph_id: Target graph.
            prices: List of price dicts, each with:
                    {"code": "600519", "name": "贵州茅台",
                     "date": "2025-03-14", "close": 1580.0,
                     "change_pct": 0.023, "volume": 15000000,
                     "turnover_rate": 0.012}
        """
        texts = []
        for p in prices:
            text = (
                f"[行情快照] {p.get('name', '')} ({p.get('code', '')}) "
                f"日期: {p.get('date', '')}; "
                f"收盘价: {p.get('close', 0)}; "
                f"涨跌幅: {p.get('change_pct', 0):.2%}; "
                f"成交量: {p.get('volume', 0)}; "
                f"换手率: {p.get('turnover_rate', 0):.2%}"
            )
            texts.append(text)

        self._builder.add_episodes(graph_id, texts, batch_size=5)
        logger.info("Updated %d price snapshots in graph '%s'", len(prices), graph_id)

    # ── generic batch update ───────────────────────────────────

    def batch_update(
        self,
        graph_id: str,
        updates: List[Dict[str, Any]],
    ) -> None:
        """
        Process a mixed batch of updates.

        Each update dict must have a "type" field:
          - "company": calls update_company_data(code=..., data=...)
          - "holdings": calls update_holdings(institution=..., holdings=...)
          - "event": calls update_events(events=[...])
          - "price": calls update_prices(prices=[...])

        Args:
            graph_id: Target graph.
            updates: List of update dicts.
        """
        # Group by type
        companies = []
        holdings = []
        events = []
        prices = []

        for u in updates:
            utype = u.get("type", "")
            if utype == "company":
                companies.append(u)
            elif utype == "holdings":
                holdings.append(u)
            elif utype == "event":
                events.append(u)
            elif utype == "price":
                prices.append(u)
            else:
                logger.warning("Unknown update type: %s", utype)

        for c in companies:
            self.update_company_data(
                graph_id, code=c.get("code", ""), data=c.get("data", {})
            )

        for h in holdings:
            self.update_holdings(
                graph_id,
                institution=h.get("institution", ""),
                holdings=h.get("holdings", []),
            )

        if events:
            event_list = [e.get("event", e) for e in events]
            self.update_events(graph_id, event_list)

        if prices:
            price_list = [p.get("price", p) for p in prices]
            self.update_prices(graph_id, price_list)

        logger.info(
            "Batch update complete for graph '%s': %d companies, %d holdings, "
            "%d events, %d prices",
            graph_id,
            len(companies),
            len(holdings),
            len(events),
            len(prices),
        )
