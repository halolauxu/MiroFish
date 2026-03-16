"""
Master Scheduler -- orchestrate all strategy runners on cron-like
schedules.

Uses ``threading.Timer`` for lightweight scheduling without external
dependencies.  For production deployments, consider replacing with
APScheduler or a system cron.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

from astrategy.aggregator.portfolio_optimizer import PortfolioOptimizer
from astrategy.aggregator.signal_aggregator import CompositeSignal, SignalAggregator
from astrategy.config import settings
from astrategy.scheduler.daily_runner import DailyRunner
from astrategy.scheduler.event_runner import EventRunner
from astrategy.scheduler.weekly_runner import WeeklyRunner
from astrategy.strategies.base import _CST, _now_cst

logger = logging.getLogger("astrategy.scheduler.master_scheduler")


# ---------------------------------------------------------------------------
# Cron-like schedule parser (minimal subset)
# ---------------------------------------------------------------------------

def _parse_cron_field(field: str, min_val: int, max_val: int) -> List[int]:
    """Parse a single cron field into a list of matching integers.

    Supports: ``*``, ``N``, ``N-M``, ``*/N``, ``N,M``.
    """
    if field == "*":
        return list(range(min_val, max_val + 1))

    if "/" in field:
        base, step_str = field.split("/", 1)
        step = int(step_str)
        start = min_val if base == "*" else int(base)
        return list(range(start, max_val + 1, step))

    if "-" in field:
        lo, hi = field.split("-", 1)
        return list(range(int(lo), int(hi) + 1))

    if "," in field:
        return [int(x) for x in field.split(",")]

    return [int(field)]


def _cron_matches(cron_expr: str, dt: datetime) -> bool:
    """Check if *dt* matches a 5-field cron expression.

    Fields: minute hour day-of-month month day-of-week (0=Mon..6=Sun).
    """
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        logger.warning("Invalid cron expression: %s", cron_expr)
        return False

    minute, hour, dom, month, dow = parts

    minutes = _parse_cron_field(minute, 0, 59)
    hours = _parse_cron_field(hour, 0, 23)
    doms = _parse_cron_field(dom, 1, 31)
    months = _parse_cron_field(month, 1, 12)
    # Cron: 0 or 7 = Sun, 1 = Mon ... 6 = Sat
    # Python: 0 = Mon ... 6 = Sun
    dows_raw = _parse_cron_field(dow, 0, 7)
    dows = set()
    for d in dows_raw:
        if d == 7:
            dows.add(6)  # Sun
        elif d == 0:
            dows.add(6)  # Sun (cron 0 = Sun)
        else:
            dows.add(d - 1)  # cron 1=Mon -> python 0=Mon

    return (
        dt.minute in minutes
        and dt.hour in hours
        and dt.day in doms
        and dt.month in months
        and dt.weekday() in dows
    )


# ---------------------------------------------------------------------------
# Task wrapper
# ---------------------------------------------------------------------------

class _ScheduledTask:
    """Metadata for a scheduled task."""

    def __init__(
        self,
        name: str,
        cron: str,
        runner: Callable[[], Dict[str, Any]],
    ) -> None:
        self.name = name
        self.cron = cron
        self.runner = runner
        self.last_run: Optional[datetime] = None
        self.last_result: Optional[Dict[str, Any]] = None
        self.run_count: int = 0
        self.error_count: int = 0
        self.is_running: bool = False


# ---------------------------------------------------------------------------
# Master Scheduler
# ---------------------------------------------------------------------------

class MasterScheduler:
    """Cron-like scheduler for all strategy runners.

    Default schedule (all times CST):

    - **daily**: 18:00 Mon-Fri  (``0 18 * * 1-5``)
    - **weekly**: 18:30 every Friday (``30 18 * * 5``)
    - **event_scan**: 09:30 Mon-Fri (``30 9 * * 1-5``)
    """

    DEFAULT_SCHEDULE: Dict[str, str] = {
        "daily": "0 18 * * 1-5",
        "weekly": "30 18 * * 5",
        "event_scan": "30 9 * * 1-5",
    }

    def __init__(
        self,
        schedule: Optional[Dict[str, str]] = None,
        stock_codes: Optional[List[str]] = None,
    ) -> None:
        self._schedule = schedule or dict(self.DEFAULT_SCHEDULE)
        self._stock_codes = stock_codes
        self._running = False
        self._timer: Optional[threading.Timer] = None
        self._lock = threading.Lock()

        # Runners
        self._daily_runner = DailyRunner(stock_codes=stock_codes)
        self._weekly_runner = WeeklyRunner(stock_codes=stock_codes)
        self._event_runner = EventRunner()
        self._optimizer = PortfolioOptimizer()
        self._aggregator = SignalAggregator()

        # Task registry
        self._tasks: Dict[str, _ScheduledTask] = {
            "daily": _ScheduledTask(
                "daily", self._schedule.get("daily", "0 18 * * 1-5"),
                self._run_daily,
            ),
            "weekly": _ScheduledTask(
                "weekly", self._schedule.get("weekly", "30 18 * * 5"),
                self._run_weekly,
            ),
            "event_scan": _ScheduledTask(
                "event_scan", self._schedule.get("event_scan", "30 9 * * 1-5"),
                self._run_event_scan,
            ),
        }

    # ── lifecycle ─────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the scheduler loop (runs in a background thread)."""
        if self._running:
            logger.warning("Scheduler is already running.")
            return

        self._running = True
        logger.info("Master scheduler started. Schedule: %s", self._schedule)
        self._tick()

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        logger.info("Master scheduler stopped.")

    @property
    def is_running(self) -> bool:
        return self._running

    # ── manual triggers ───────────────────────────────────────────────

    def run_all(self) -> Dict[str, Any]:
        """Run all strategies once (for testing / manual invocation).

        Returns a combined result dict.
        """
        logger.info("=== Running all strategies (manual) ===")
        results: Dict[str, Any] = {}

        for task_name, task in self._tasks.items():
            logger.info("Running task: %s", task_name)
            try:
                task.is_running = True
                result = task.runner()
                task.last_result = result
                task.last_run = _now_cst()
                task.run_count += 1
                results[task_name] = result
            except Exception as exc:
                logger.error("Task %s failed: %s", task_name, exc)
                task.error_count += 1
                results[task_name] = {"error": str(exc)}
            finally:
                task.is_running = False

        # Final portfolio recommendation
        portfolio = self._generate_portfolio()
        results["portfolio"] = portfolio

        return results

    def run_task(self, task_name: str) -> Dict[str, Any]:
        """Run a single named task."""
        task = self._tasks.get(task_name)
        if task is None:
            raise ValueError(
                f"Unknown task: {task_name}. "
                f"Available: {list(self._tasks.keys())}"
            )

        logger.info("Manual run: %s", task_name)
        task.is_running = True
        try:
            result = task.runner()
            task.last_result = result
            task.last_run = _now_cst()
            task.run_count += 1
            return result
        except Exception as exc:
            task.error_count += 1
            raise
        finally:
            task.is_running = False

    # ── status ────────────────────────────────────────────────────────

    def get_status(self) -> Dict[str, Any]:
        """Return the status of all scheduled tasks."""
        status: Dict[str, Any] = {
            "scheduler_running": self._running,
            "schedule": dict(self._schedule),
            "tasks": {},
        }
        for name, task in self._tasks.items():
            status["tasks"][name] = {
                "cron": task.cron,
                "is_running": task.is_running,
                "last_run": task.last_run.isoformat() if task.last_run else None,
                "run_count": task.run_count,
                "error_count": task.error_count,
                "has_result": task.last_result is not None,
            }
        return status

    # ── internal scheduling loop ──────────────────────────────────────

    def _tick(self) -> None:
        """Check if any task should run at the current minute, then
        schedule the next tick."""
        if not self._running:
            return

        now = _now_cst()

        for task_name, task in self._tasks.items():
            if task.is_running:
                continue

            if _cron_matches(task.cron, now):
                # Prevent double-firing within the same minute
                if (
                    task.last_run is not None
                    and (now - task.last_run).total_seconds() < 60
                ):
                    continue

                # Run in a separate thread to not block the timer
                thread = threading.Thread(
                    target=self._execute_task,
                    args=(task,),
                    name=f"scheduler-{task_name}",
                    daemon=True,
                )
                thread.start()

        # Schedule next tick in 30 seconds
        self._timer = threading.Timer(30.0, self._tick)
        self._timer.daemon = True
        self._timer.start()

    def _execute_task(self, task: _ScheduledTask) -> None:
        """Execute a scheduled task with error handling."""
        with self._lock:
            if task.is_running:
                return
            task.is_running = True

        try:
            logger.info("Scheduled execution: %s", task.name)
            result = task.runner()
            task.last_result = result
            task.last_run = _now_cst()
            task.run_count += 1
            logger.info("Task %s completed successfully.", task.name)
        except Exception as exc:
            task.error_count += 1
            logger.error("Scheduled task %s failed: %s", task.name, exc)
        finally:
            task.is_running = False

    # ── runner wrappers ───────────────────────────────────────────────

    def _run_daily(self) -> Dict[str, Any]:
        return self._daily_runner.run_daily_strategies()

    def _run_weekly(self) -> Dict[str, Any]:
        return self._weekly_runner.run_weekly_strategies()

    def _run_event_scan(self) -> Dict[str, Any]:
        """Scan for events; if any found, run event strategies."""
        events = self._event_runner.scan_for_events()
        if not events:
            return {
                "date": _now_cst().strftime("%Y%m%d"),
                "events_detected": 0,
                "message": "No actionable events detected.",
            }

        results = []
        for event in events:
            try:
                result = self._event_runner.run_event_strategies(event=event)
                results.append(result)
            except Exception as exc:
                logger.error("Event strategy run failed: %s", exc)
                results.append({"error": str(exc), "event": event})

        return {
            "date": _now_cst().strftime("%Y%m%d"),
            "events_detected": len(events),
            "events": events,
            "run_results": results,
        }

    # ── portfolio aggregation ─────────────────────────────────────────

    def _generate_portfolio(self) -> Dict[str, Any]:
        """Aggregate all recent signals into a final portfolio
        recommendation."""
        # Collect all composite signals from task results
        all_composites: List[CompositeSignal] = []

        for task in self._tasks.values():
            if task.last_result is None:
                continue
            # The runners store top_signals as dicts; we need CompositeSignal
            # objects. Re-aggregate from stored signals instead.

        # Re-aggregate from stored signal files (last 5 days)
        from astrategy.strategies.base import StrategySignal

        aggregator = SignalAggregator()
        signal_dir = settings.storage.signal_dir

        if signal_dir.exists():
            now = _now_cst()
            for days_ago in range(5):
                date_str = (now - timedelta(days=days_ago)).strftime("%Y%m%d")
                for strat_dir in signal_dir.iterdir():
                    if not strat_dir.is_dir() or strat_dir.name.startswith("_"):
                        continue
                    sig_file = strat_dir / f"{date_str}.json"
                    if sig_file.exists():
                        try:
                            raw = json.loads(sig_file.read_text("utf-8"))
                            signals = [
                                StrategySignal.from_dict(s) for s in raw
                            ]
                            aggregator.add_signals(signals)
                        except Exception:
                            pass

        composites = aggregator.aggregate_signals(method="weighted")

        if not composites:
            return {
                "message": "No signals available for portfolio construction.",
                "positions": [],
            }

        # Optimize
        portfolio = self._optimizer.optimize(
            signals=composites,
            budget=1.0,
            max_positions=20,
            method="confidence_weighted",
        )

        return portfolio.to_dict()
