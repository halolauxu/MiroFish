"""
Dynamic Graph Updater
======================
Pulls daily news + announcements for CSI800 stocks and uses LLM to extract
company-level state changes, then writes them as time-stamped episodes into a
dedicated "astrategy_dynamic" local graph.

This fills the gap between the *static* supply-chain graph (astrategy) and the
real-time market environment: strategies that query the dynamic graph can see:
  - momentum trend (accelerating growth / decelerating / reversal)
  - recent major events (M&A, earnings beat/miss, regulatory risk)
  - narrative alignment (which investment themes each company is linked to)
  - analyst trend (upgrade / downgrade / initiation)

The static graph (astrategy) is not modified; the dynamic graph is separate.

Usage
-----
    from astrategy.graph.dynamic_updater import DynamicGraphUpdater

    updater = DynamicGraphUpdater()
    summary = updater.update_company_state(stock_codes=["600519", "000858"])
    state = updater.get_company_state("600519")
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from astrategy.data_collector.announcement import AnnouncementCollector
from astrategy.data_collector.news import NewsCollector
from astrategy.graph.local_store import LocalGraphStore
from astrategy.llm import create_llm_client

logger = logging.getLogger("astrategy.graph.dynamic_updater")

_CST = timezone(timedelta(hours=8))
_DYNAMIC_GRAPH_ID = "astrategy_dynamic"

# ---------------------------------------------------------------------------
# LLM prompt
# ---------------------------------------------------------------------------

_STATE_EXTRACTION_PROMPT = """\
你是A股公司状态分析师。根据以下新闻和公告，提取该公司当前的业务状态变化。

公司: {stock_code} {stock_name}
日期: {date}

## 新闻/公告内容
{news_text}

请以JSON格式输出公司状态：
{{
  "momentum": -1.0到1.0之间（-1=明显恶化，0=无变化，1=明显改善），
  "momentum_reason": "动量变化原因（30字内）",
  "risk_level": "low/medium/high",
  "risk_reason": "风险原因（30字内，无风险则留空）",
  "key_events": ["事件1", "事件2"],
  "narrative_alignment": ["叙事主题1", "叙事主题2"],
  "analyst_trend": "upgrade/downgrade/neutral/initiation",
  "summary": "一句话总结当前状态（50字内）",
  "has_meaningful_update": true/false
}}

只有存在实质性信息时才设置 has_meaningful_update=true。
无实质内容时返回 has_meaningful_update=false，其他字段可为空/零。
"""


# ---------------------------------------------------------------------------
# DynamicGraphUpdater
# ---------------------------------------------------------------------------


class DynamicGraphUpdater:
    """Pull news/announcements for a list of stocks and update the dynamic graph.

    Parameters
    ----------
    data_dir :
        Directory for the dynamic graph JSON file.
        Defaults to ``astrategy/.data/local_graph/``.
    llm_strategy_name :
        LLM cost-tracker label.
    max_news_per_stock :
        Maximum news items to feed to the LLM per stock.
    """

    def __init__(
        self,
        data_dir: str | Path | None = None,
        llm_strategy_name: str = "dynamic_graph_updater",
        max_news_per_stock: int = 8,
    ) -> None:
        self._store = LocalGraphStore(data_dir=data_dir)
        self._llm = create_llm_client(strategy_name=llm_strategy_name)
        self._news = NewsCollector()
        self._announcements = AnnouncementCollector()
        self._max_news = max_news_per_stock

        # Try to load existing dynamic graph
        loaded = self._store.load(_DYNAMIC_GRAPH_ID)
        if not loaded:
            self._store.create_graph(_DYNAMIC_GRAPH_ID)
            logger.info("Created new dynamic graph '%s'", _DYNAMIC_GRAPH_ID)
        else:
            logger.info("Loaded existing dynamic graph '%s'", _DYNAMIC_GRAPH_ID)

    # ── public API ─────────────────────────────────────────────────────

    def update_company_state(
        self,
        stock_codes: list[str],
        date: str | None = None,
        stock_names: dict[str, str] | None = None,
    ) -> dict:
        """Process news/announcements for a list of stocks and update the graph.

        Parameters
        ----------
        stock_codes :
            List of 6-digit stock codes.
        date :
            Date string (YYYYMMDD). Defaults to today (CST).
        stock_names :
            Optional mapping of code → name. Used for LLM context.

        Returns
        -------
        dict
            Summary: {updated: int, skipped: int, errors: int, states: [...]}
        """
        if date is None:
            date = datetime.now(tz=_CST).strftime("%Y%m%d")

        names = stock_names or {}
        updated, skipped, errors = 0, 0, 0
        states: list[dict] = []

        for code in stock_codes:
            name = names.get(code, code)
            try:
                state = self._update_single(code, name, date)
                if state.get("has_meaningful_update"):
                    updated += 1
                    states.append(state)
                    logger.info("[dynamic] Updated %s (%s): %s", code, name,
                                state.get("summary", ""))
                else:
                    skipped += 1
                    logger.debug("[dynamic] No update for %s", code)
            except Exception as exc:
                errors += 1
                logger.warning("[dynamic] Error updating %s: %s", code, exc)

        # Persist after batch
        try:
            self._store.save(_DYNAMIC_GRAPH_ID)
        except Exception as exc:
            logger.warning("[dynamic] Save failed: %s", exc)

        logger.info("[dynamic] Batch done: %d updated, %d skipped, %d errors",
                    updated, skipped, errors)
        return {"updated": updated, "skipped": skipped, "errors": errors, "states": states}

    def get_company_state(self, stock_code: str) -> dict:
        """Retrieve the latest state record for a stock from the dynamic graph.

        Returns an empty dict if no state exists.
        """
        # Search by stock code in the dynamic graph
        results = self._store.search(_DYNAMIC_GRAPH_ID, stock_code, limit=20)

        # Filter to STATE_UPDATE episodes for this code
        state_episodes = [
            r for r in results
            if r.get("relation") == "STATE_UPDATE" and stock_code in r.get("fact", "")
        ]

        if not state_episodes:
            return {}

        # Most recent episode (by created_at or just last in list)
        latest = state_episodes[-1]
        try:
            return json.loads(latest["fact"])
        except (json.JSONDecodeError, KeyError):
            return {"summary": latest.get("fact", ""), "stock_code": stock_code}

    def get_states_batch(self, stock_codes: list[str]) -> dict[str, dict]:
        """Retrieve the latest state for multiple stocks at once."""
        return {code: self.get_company_state(code) for code in stock_codes}

    # ── internal ────────────────────────────────────────────────────────

    def _update_single(self, stock_code: str, stock_name: str, date: str) -> dict:
        """Pull data for one stock, extract state, write to graph."""
        # Gather news (limit to avoid excessive tokens)
        news_items: list[dict] = []
        try:
            company_news = self._news.get_company_news(stock_code, limit=self._max_news)
            news_items.extend(company_news)
        except Exception as exc:
            logger.debug("[dynamic] News fetch failed for %s: %s", stock_code, exc)

        # Gather announcements
        try:
            date_str = f"{date[:4]}-{date[4:6]}-{date[6:]}"
            lookback = (
                datetime.strptime(date, "%Y%m%d") - timedelta(days=7)
            ).strftime("%Y-%m-%d")
            announcements = self._announcements.get_company_announcements(
                stock_code, start_date=lookback, end_date=date_str
            )
            # Only keep important ones
            important = self._announcements.filter_important_announcements(announcements)
            news_items.extend(important[:5])
        except Exception as exc:
            logger.debug("[dynamic] Announcement fetch failed for %s: %s", stock_code, exc)

        if not news_items:
            return {"stock_code": stock_code, "has_meaningful_update": False}

        # Format for LLM
        news_lines: list[str] = []
        for item in news_items[:12]:
            title = (
                item.get("新闻标题")
                or item.get("标题")
                or item.get("公告标题")
                or item.get("名称", "")
            )
            time_str = item.get("发布时间") or item.get("时间") or item.get("公告日期", "")
            if title:
                news_lines.append(f"- [{time_str}] {str(title)[:120]}")

        if not news_lines:
            return {"stock_code": stock_code, "has_meaningful_update": False}

        prompt = _STATE_EXTRACTION_PROMPT.format(
            stock_code=stock_code,
            stock_name=stock_name,
            date=date,
            news_text="\n".join(news_lines),
        )
        messages = [
            {"role": "system", "content": "你是A股公司状态分析师。严格以JSON格式输出。"},
            {"role": "user", "content": prompt},
        ]

        try:
            state = self._llm.chat_json(messages=messages, max_tokens=600)
        except Exception as exc:
            logger.warning("[dynamic] LLM extraction failed for %s: %s", stock_code, exc)
            return {"stock_code": stock_code, "has_meaningful_update": False}

        if not state.get("has_meaningful_update"):
            return {"stock_code": stock_code, "has_meaningful_update": False}

        # Enrich state with metadata
        state["stock_code"] = stock_code
        state["stock_name"] = stock_name
        state["date"] = date
        state["updated_at"] = datetime.now(tz=_CST).isoformat()

        # Write to graph as a timestamped episode
        fact_json = json.dumps(state, ensure_ascii=False)
        self._store.add_edge(
            _DYNAMIC_GRAPH_ID,
            source=stock_code,
            target="STATE",
            relation="STATE_UPDATE",
            fact=fact_json,
            weight=abs(state.get("momentum", 0.0)),
        )

        # Also add/update the company node with latest state summary
        self._store.add_node(
            _DYNAMIC_GRAPH_ID,
            name=stock_code,
            labels=["Company"],
            summary=f"{stock_name}({stock_code}): {state.get('summary', '')} [{date}]",
            code=stock_code,
            display_name=stock_name,
            latest_momentum=state.get("momentum", 0.0),
            latest_risk=state.get("risk_level", "low"),
            latest_date=date,
        )

        return state

    # ── convenience: batch update from run_backtest universe ────────────

    @classmethod
    def run_daily(
        cls,
        stock_codes: list[str],
        stock_names: dict[str, str] | None = None,
        batch_size: int = 50,
    ) -> dict:
        """Run a daily update for the full stock universe in batches.

        Parameters
        ----------
        stock_codes :
            Full list of CSI800 stock codes.
        stock_names :
            Optional code→name mapping.
        batch_size :
            How many stocks per LLM batch (limits API rate).

        Returns
        -------
        dict
            Aggregated summary across all batches.
        """
        updater = cls()
        today = datetime.now(tz=_CST).strftime("%Y%m%d")
        total = {"updated": 0, "skipped": 0, "errors": 0, "states": []}

        for i in range(0, len(stock_codes), batch_size):
            batch = stock_codes[i: i + batch_size]
            logger.info("[dynamic] Processing batch %d/%d (%d stocks)",
                        i // batch_size + 1,
                        (len(stock_codes) + batch_size - 1) // batch_size,
                        len(batch))
            result = updater.update_company_state(batch, date=today, stock_names=stock_names)
            total["updated"] += result["updated"]
            total["skipped"] += result["skipped"]
            total["errors"] += result["errors"]
            total["states"].extend(result["states"])

        logger.info("[dynamic] Daily run complete: %d updated, %d skipped, %d errors",
                    total["updated"], total["skipped"], total["errors"])
        return total
