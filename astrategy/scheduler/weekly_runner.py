"""
Weekly Runner -- execute weekly strategies (S05, S08) and merge with
active daily signals.
"""

from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from astrategy.aggregator.signal_aggregator import SignalAggregator
from astrategy.config import settings
from astrategy.strategies.base import BaseStrategy, StrategySignal, _now_cst

logger = logging.getLogger("astrategy.scheduler.weekly_runner")

_WEEKLY_STRATEGY_CLASSES: Dict[str, str] = {
    "s05_analyst_divergence": "astrategy.strategies.s05_analyst_divergence.AnalystDivergenceStrategy",
    "s08_sector_rotation": "astrategy.strategies.s08_sector_rotation.SectorRotationStrategy",
}


def _load_strategy(dotted_path: str) -> BaseStrategy:
    """Dynamically import and instantiate a strategy class."""
    module_path, class_name = dotted_path.rsplit(".", 1)
    import importlib
    mod = importlib.import_module(module_path)
    cls = getattr(mod, class_name)
    return cls()


class WeeklyRunner:
    """Run weekly strategies and merge with active daily signals.

    Default strategies: S05 (analyst divergence), S08 (sector rotation).
    """

    def __init__(
        self,
        strategies: Optional[Dict[str, str]] = None,
        stock_codes: Optional[List[str]] = None,
        max_workers: int = 2,
        lookback_days: int = 5,
    ) -> None:
        """
        Parameters
        ----------
        strategies:
            Override strategy map.  Defaults to S05/S08.
        stock_codes:
            Stock universe.  ``None`` = strategy default.
        max_workers:
            Concurrent execution threads.
        lookback_days:
            How many days of daily signals to merge with weekly results.
        """
        self._strategy_map = strategies or _WEEKLY_STRATEGY_CLASSES
        self._stock_codes = stock_codes
        self._max_workers = max_workers
        self._lookback_days = lookback_days
        self._aggregator = SignalAggregator()

    # ── main entry point ──────────────────────────────────────────────

    def run_weekly_strategies(self) -> Dict[str, Any]:
        """Run all weekly strategies and merge with recent daily signals.

        Returns
        -------
        dict with date, strategy results, composite signals, weekly
        summary text, and timing.
        """
        now = _now_cst()
        date_str = now.strftime("%Y%m%d")
        logger.info("=== Weekly run started: %s ===", date_str)

        start_time = time.time()
        all_signals: List[StrategySignal] = []
        strategy_results: Dict[str, int] = {}
        errors: Dict[str, str] = {}

        # Run weekly strategies concurrently
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
                except Exception as exc:
                    logger.error("Strategy %s failed: %s", name, exc)
                    errors[name] = str(exc)
                    strategy_results[name] = 0

        # Load recent daily signals
        daily_signals = self._load_recent_daily_signals()
        daily_count = len(daily_signals)
        all_signals.extend(daily_signals)

        # Aggregate
        self._aggregator.clear()
        self._aggregator.add_signals(all_signals)
        composites = self._aggregator.aggregate_signals(method="weighted")

        # Generate weekly summary
        summary = self._generate_weekly_summary(
            strategy_results, composites, daily_count
        )

        # Persist
        self._save_weekly_results(date_str, composites, summary, all_signals)

        elapsed = time.time() - start_time
        logger.info(
            "=== Weekly run complete: %d signals (incl. %d daily) in %.1fs ===",
            len(all_signals),
            daily_count,
            elapsed,
        )

        return {
            "date": date_str,
            "strategies_run": list(strategy_results.keys()),
            "signals_per_strategy": strategy_results,
            "daily_signals_merged": daily_count,
            "total_signals": len(all_signals),
            "composite_signals": len(composites),
            "top_signals": [c.to_dict() for c in composites[:10]],
            "weekly_summary": summary,
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
        logger.info("Running weekly strategy: %s", name)
        signals = strategy.run(stock_codes=self._stock_codes)
        if signals:
            strategy.save_signals(signals, date=date_str)
        return signals

    def _load_recent_daily_signals(self) -> List[StrategySignal]:
        """Load daily aggregate signals from the last N days."""
        signals: List[StrategySignal] = []
        daily_dir = settings.storage.signal_dir / "_daily_aggregate"
        if not daily_dir.exists():
            return signals

        now = _now_cst()
        for days_ago in range(self._lookback_days):
            date_str = (now - timedelta(days=days_ago)).strftime("%Y%m%d")

            # Load raw signals from each strategy sub-dir
            signal_root = settings.storage.signal_dir
            if not signal_root.exists():
                continue

            for strat_dir in signal_root.iterdir():
                if not strat_dir.is_dir() or strat_dir.name.startswith("_"):
                    continue
                sig_file = strat_dir / f"{date_str}.json"
                if sig_file.exists():
                    try:
                        raw = json.loads(sig_file.read_text("utf-8"))
                        for item in raw:
                            sig = StrategySignal.from_dict(item)
                            if not sig.is_expired:
                                signals.append(sig)
                    except Exception as exc:
                        logger.warning(
                            "Failed to load signals from %s: %s", sig_file, exc
                        )

        logger.info(
            "Loaded %d active daily signals from last %d days",
            len(signals),
            self._lookback_days,
        )
        return signals

    def _generate_weekly_summary(
        self,
        strategy_results: Dict[str, int],
        composites: list,
        daily_count: int,
    ) -> str:
        """Generate a text summary of the weekly run."""
        now = _now_cst()
        lines = [
            f"# Weekly Strategy Summary - {now.strftime('%Y-%m-%d')}",
            "",
            "## Strategies Executed",
        ]
        for name, count in strategy_results.items():
            lines.append(f"- {name}: {count} signals")

        lines.append(f"- Daily signals merged: {daily_count}")
        lines.append("")
        lines.append(f"## Composite Signals: {len(composites)}")

        if composites:
            lines.append("")
            lines.append("### Top Recommendations")
            lines.append("")
            lines.append("| Stock | Direction | Confidence | Expected Return |")
            lines.append("|-------|-----------|------------|-----------------|")
            for cs in composites[:10]:
                lines.append(
                    f"| {cs.stock_name} ({cs.stock_code}) "
                    f"| {cs.direction} "
                    f"| {cs.composite_confidence:.2%} "
                    f"| {cs.expected_return:.2%} |"
                )

        # Direction breakdown
        long_count = sum(1 for c in composites if c.direction == "long")
        short_count = sum(1 for c in composites if c.direction == "short")
        neutral_count = sum(1 for c in composites if c.direction == "neutral")
        lines.extend([
            "",
            "## Direction Breakdown",
            f"- Long: {long_count}",
            f"- Short: {short_count}",
            f"- Neutral: {neutral_count}",
        ])

        return "\n".join(lines)

    def _save_weekly_results(
        self,
        date_str: str,
        composites: list,
        summary: str,
        raw_signals: List[StrategySignal],
    ) -> None:
        out_dir = settings.storage.signal_dir / "_weekly_aggregate"
        out_dir.mkdir(parents=True, exist_ok=True)

        # JSON
        json_path = out_dir / f"{date_str}.json"
        payload = {
            "date": date_str,
            "composite_signals": [c.to_dict() for c in composites],
            "raw_signal_count": len(raw_signals),
        }
        json_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        # Markdown summary
        md_path = out_dir / f"{date_str}_summary.md"
        md_path.write_text(summary, encoding="utf-8")

        logger.info("Weekly results saved to %s", out_dir)
