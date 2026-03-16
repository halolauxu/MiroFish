"""
Daily Runner -- execute daily strategies (S06, S07) concurrently.

Runs after market close each trading day, aggregates signals, and
persists results.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from astrategy.aggregator.signal_aggregator import SignalAggregator
from astrategy.config import settings
from astrategy.strategies.base import BaseStrategy, StrategySignal, _CST, _now_cst

logger = logging.getLogger("astrategy.scheduler.daily_runner")

# Strategy import registry -- lazy-loaded to avoid circular imports
_DAILY_STRATEGY_CLASSES: Dict[str, str] = {
    "s06_announcement_sentiment": "astrategy.strategies.s06_announcement_sentiment.AnnouncementSentimentStrategy",
    "s07_graph_factors": "astrategy.strategies.s07_graph_factors.GraphFactorStrategy",
}


def _load_strategy(dotted_path: str) -> BaseStrategy:
    """Dynamically import and instantiate a strategy class."""
    module_path, class_name = dotted_path.rsplit(".", 1)
    import importlib
    mod = importlib.import_module(module_path)
    cls = getattr(mod, class_name)
    return cls()


class DailyRunner:
    """Run daily strategies and aggregate their signals.

    Default strategies: S06 (announcement sentiment), S07 (graph factors).
    """

    def __init__(
        self,
        strategies: Optional[Dict[str, str]] = None,
        stock_codes: Optional[List[str]] = None,
        max_workers: int = 2,
    ) -> None:
        """
        Parameters
        ----------
        strategies:
            ``{name: dotted_class_path}`` overrides.  Defaults to S06/S07.
        stock_codes:
            Universe of stocks to analyse.  ``None`` = strategy default.
        max_workers:
            Thread pool size for concurrent execution.
        """
        self._strategy_map = strategies or _DAILY_STRATEGY_CLASSES
        self._stock_codes = stock_codes
        self._max_workers = max_workers
        self._aggregator = SignalAggregator()

    # ── main entry point ──────────────────────────────────────────────

    def run_daily_strategies(self) -> Dict[str, Any]:
        """Execute all daily strategies and return aggregated results.

        Returns
        -------
        dict with keys: date, strategies_run, signals_per_strategy,
        total_signals, composite_signals, cost_summary, elapsed_seconds.
        """
        date_str = _now_cst().strftime("%Y%m%d")
        logger.info("=== Daily run started: %s ===", date_str)

        start_time = time.time()
        all_signals: List[StrategySignal] = []
        strategy_results: Dict[str, int] = {}
        errors: Dict[str, str] = {}

        # Run strategies concurrently
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {}
            for name, dotted_path in self._strategy_map.items():
                try:
                    strategy = _load_strategy(dotted_path)
                    future = executor.submit(
                        self._run_strategy, strategy, name, date_str
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
                    logger.info(
                        "Strategy %s produced %d signals", name, len(signals)
                    )
                except Exception as exc:
                    logger.error("Strategy %s failed: %s", name, exc)
                    errors[name] = str(exc)
                    strategy_results[name] = 0

        # Aggregate
        self._aggregator.clear()
        self._aggregator.add_signals(all_signals)
        composites = self._aggregator.aggregate_signals(method="weighted")

        # Save aggregated results
        self._save_daily_results(date_str, composites, all_signals)

        elapsed = time.time() - start_time
        logger.info(
            "=== Daily run complete: %d signals from %d strategies in %.1fs ===",
            len(all_signals),
            len(strategy_results),
            elapsed,
        )

        return {
            "date": date_str,
            "strategies_run": list(strategy_results.keys()),
            "signals_per_strategy": strategy_results,
            "total_signals": len(all_signals),
            "composite_signals": len(composites),
            "top_signals": [c.to_dict() for c in composites[:10]],
            "errors": errors,
            "elapsed_seconds": round(elapsed, 2),
        }

    # ── internal ──────────────────────────────────────────────────────

    def _run_strategy(
        self,
        strategy: BaseStrategy,
        name: str,
        date_str: str,
    ) -> List[StrategySignal]:
        """Run a single strategy and persist its signals."""
        logger.info("Running strategy: %s", name)
        signals = strategy.run(stock_codes=self._stock_codes)
        if signals:
            strategy.save_signals(signals, date=date_str)
        return signals

    def _save_daily_results(
        self,
        date_str: str,
        composites: list,
        raw_signals: List[StrategySignal],
    ) -> None:
        """Save aggregated daily results to disk."""
        out_dir = settings.storage.signal_dir / "_daily_aggregate"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{date_str}.json"

        payload = {
            "date": date_str,
            "composite_signals": [c.to_dict() for c in composites],
            "raw_signal_count": len(raw_signals),
        }
        out_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("Daily aggregate saved to %s", out_path)
