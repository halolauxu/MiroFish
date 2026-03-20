"""
Event Runner -- trigger event-driven strategies (S01, S03, S10) on
detected market events.

Can be triggered manually with an event dict or auto-triggered by the
daily event scanner.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from astrategy.aggregator.signal_aggregator import SignalAggregator
from astrategy.archive.authoritative_history import archive_authoritative_signals
from astrategy.config import settings
from astrategy.strategies.base import BaseStrategy, StrategySignal, _now_cst

logger = logging.getLogger("astrategy.scheduler.event_runner")

_EVENT_STRATEGY_CLASSES: Dict[str, str] = {
    "s01_supply_chain": "astrategy.strategies.s01_supply_chain.SupplyChainStrategy",
    "s03_event_propagation": "astrategy.strategies.s03_event_propagation.EventPropagationStrategy",
    # S10 (simulation) may not always be available
    # "s10_simulation": "astrategy.strategies.s10_simulation.SimulationStrategy",
}

# Keywords that indicate a material market event (Chinese + English)
_EVENT_KEYWORDS = [
    # Supply chain
    "涨价", "调价", "提价", "限产", "停产", "扩产", "产能", "缺货", "原材料",
    "price hike", "supply shortage", "capacity expansion",
    # Corporate events
    "重组", "并购", "收购", "增持", "减持", "回购", "分红",
    "merger", "acquisition", "buyback",
    # Regulatory
    "政策", "监管", "调控", "制裁", "关税",
    "regulation", "sanction", "tariff",
    # Market events
    "黑天鹅", "暴跌", "暴涨", "熔断", "利好", "利空",
    "black swan", "crash", "surge",
]


def _load_strategy(dotted_path: str) -> BaseStrategy:
    """Dynamically import and instantiate a strategy class."""
    module_path, class_name = dotted_path.rsplit(".", 1)
    import importlib
    mod = importlib.import_module(module_path)
    cls = getattr(mod, class_name)
    return cls()


class EventRunner:
    """Run event-driven strategies in response to market events.

    Strategies: S01 (supply chain), S03 (event propagation), optionally
    S10 (simulation).
    """

    def __init__(
        self,
        strategies: Optional[Dict[str, str]] = None,
        max_workers: int = 3,
    ) -> None:
        self._strategy_map = strategies or _EVENT_STRATEGY_CLASSES
        self._max_workers = max_workers
        self._aggregator = SignalAggregator()

    # ── main entry point ──────────────────────────────────────────────

    def run_event_strategies(
        self,
        event: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Run event-driven strategies, optionally scoped by *event*.

        Parameters
        ----------
        event:
            Optional event context dict with keys like ``type``,
            ``description``, ``stock_codes``, ``keywords``.  If ``None``,
            all event strategies run with their default universes.

        Returns
        -------
        dict with date, event info, strategy results, composite signals.
        """
        now = _now_cst()
        date_str = now.strftime("%Y%m%d")
        timestamp = now.isoformat()

        logger.info(
            "=== Event run started: %s | event=%s ===",
            date_str,
            event.get("type", "manual") if event else "scan",
        )

        start_time = time.time()
        all_signals: List[StrategySignal] = []
        strategy_results: Dict[str, int] = {}
        errors: Dict[str, str] = {}

        # Determine stock universe from event if provided
        stock_codes = None
        if event:
            stock_codes = event.get("stock_codes")

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {}
            for name, dotted_path in self._strategy_map.items():
                try:
                    strategy = _load_strategy(dotted_path)
                    future = executor.submit(
                        self._run_strategy, strategy, name, date_str,
                        stock_codes,
                    )
                    futures[future] = name
                except Exception as exc:
                    logger.error("Failed to load strategy %s: %s", name, exc)
                    errors[name] = str(exc)

            for future in as_completed(futures):
                name = futures[future]
                try:
                    signals = future.result()
                    strategy_results[name] = len(signals)
                    all_signals.extend(signals)
                except Exception as exc:
                    logger.error("Strategy %s failed: %s", name, exc)
                    errors[name] = str(exc)
                    strategy_results[name] = 0

        # Aggregate
        self._aggregator.clear()
        self._aggregator.add_signals(all_signals)
        composites = self._aggregator.aggregate_signals(method="weighted")

        # Persist
        self._save_event_results(date_str, timestamp, event, composites, all_signals)

        elapsed = time.time() - start_time
        logger.info(
            "=== Event run complete: %d signals in %.1fs ===",
            len(all_signals),
            elapsed,
        )

        return {
            "date": date_str,
            "timestamp": timestamp,
            "event": event,
            "strategies_run": list(strategy_results.keys()),
            "signals_per_strategy": strategy_results,
            "total_signals": len(all_signals),
            "composite_signals": len(composites),
            "top_signals": [c.to_dict() for c in composites[:10]],
            "errors": errors,
            "elapsed_seconds": round(elapsed, 2),
        }

    # ── event scanning ────────────────────────────────────────────────

    def scan_for_events(self) -> List[Dict[str, Any]]:
        """Scan for recent events that warrant event-driven strategies.

        Checks recent announcement/news files in the data directory for
        keyword matches.  Returns a list of detected event dicts.

        Each event dict has: type, description, stock_codes, keywords,
        source, timestamp.
        """
        events: List[Dict[str, Any]] = []
        now = _now_cst()

        # Scan recent files in the market data directory
        data_dir = settings.storage.data_dir
        announcements_dir = data_dir / "announcements"

        if not announcements_dir.exists():
            logger.info("No announcements directory found; skipping scan.")
            return events

        # Check files from last 3 days
        for days_ago in range(3):
            date_str = (now - timedelta(days=days_ago)).strftime("%Y%m%d")
            for json_file in announcements_dir.glob(f"*{date_str}*.json"):
                try:
                    content = json.loads(json_file.read_text("utf-8"))
                    detected = self._scan_content(content, json_file.name)
                    events.extend(detected)
                except Exception as exc:
                    logger.warning("Failed to scan %s: %s", json_file, exc)

        # Deduplicate by event type + stock codes
        events = self._deduplicate_events(events)

        logger.info("Event scan found %d events", len(events))
        return events

    def scan_and_run(self) -> List[Dict[str, Any]]:
        """Convenience: scan for events and run strategies for each.

        Returns a list of run-result dicts (one per detected event), or
        an empty list if no events are found.
        """
        events = self.scan_for_events()
        results = []
        for event in events:
            try:
                result = self.run_event_strategies(event=event)
                results.append(result)
            except Exception as exc:
                logger.error("Event run failed for %s: %s", event, exc)
        return results

    # ── internal ──────────────────────────────────────────────────────

    def _run_strategy(
        self,
        strategy: BaseStrategy,
        name: str,
        date_str: str,
        stock_codes: Optional[List[str]],
    ) -> List[StrategySignal]:
        logger.info("Running event strategy: %s", name)
        signals = strategy.run(stock_codes=stock_codes)
        if signals:
            out_path = strategy.save_signals(signals, date=date_str)
            archive_authoritative_signals(
                strategy_name=strategy.name,
                signals=signals,
                as_of_date=date_str,
                source_path=str(out_path),
                run_context="event_runner",
            )
        return signals

    @staticmethod
    def _scan_content(
        content: Any,
        source_name: str,
    ) -> List[Dict[str, Any]]:
        """Scan a loaded JSON structure for event keywords."""
        events: List[Dict[str, Any]] = []

        items = content if isinstance(content, list) else [content]
        for item in items:
            if not isinstance(item, dict):
                continue

            # Combine all text fields for keyword matching
            text_fields = []
            for key in ("title", "content", "summary", "description", "text"):
                val = item.get(key, "")
                if isinstance(val, str):
                    text_fields.append(val)
            combined = " ".join(text_fields)

            matched_keywords = [
                kw for kw in _EVENT_KEYWORDS if kw in combined
            ]
            if not matched_keywords:
                continue

            stock_code = item.get("stock_code") or item.get("code", "")
            stock_codes = [stock_code] if stock_code else []

            # Classify event type
            event_type = "general"
            supply_kw = {"涨价", "调价", "提价", "限产", "停产", "扩产", "产能", "缺货", "原材料",
                         "price hike", "supply shortage", "capacity expansion"}
            corp_kw = {"重组", "并购", "收购", "增持", "减持", "回购",
                       "merger", "acquisition", "buyback"}
            if any(k in matched_keywords for k in supply_kw):
                event_type = "supply_chain"
            elif any(k in matched_keywords for k in corp_kw):
                event_type = "corporate_action"

            events.append({
                "type": event_type,
                "description": combined[:200],
                "stock_codes": stock_codes,
                "keywords": matched_keywords,
                "source": source_name,
                "timestamp": _now_cst().isoformat(),
            })

        return events

    @staticmethod
    def _deduplicate_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate events (same type + overlapping stock codes)."""
        seen: set = set()
        unique: List[Dict[str, Any]] = []
        for ev in events:
            key = (ev["type"], tuple(sorted(ev.get("stock_codes", []))))
            if key not in seen:
                seen.add(key)
                unique.append(ev)
        return unique

    def _save_event_results(
        self,
        date_str: str,
        timestamp: str,
        event: Optional[Dict[str, Any]],
        composites: list,
        raw_signals: List[StrategySignal],
    ) -> None:
        out_dir = settings.storage.signal_dir / "_event_runs"
        out_dir.mkdir(parents=True, exist_ok=True)

        # Use timestamp in filename to allow multiple event runs per day
        safe_ts = timestamp.replace(":", "").replace("+", "_")
        out_path = out_dir / f"{date_str}_{safe_ts}.json"

        payload = {
            "date": date_str,
            "timestamp": timestamp,
            "event": event,
            "composite_signals": [c.to_dict() for c in composites],
            "raw_signal_count": len(raw_signals),
        }
        out_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        logger.info("Event results saved to %s", out_path)
