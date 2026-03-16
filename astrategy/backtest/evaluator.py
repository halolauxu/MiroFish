"""
Signal Evaluator for backtesting signal quality.

Compares predicted signals against actual market returns to compute
hit rate, Sharpe, IC, and other performance metrics.
"""

from __future__ import annotations

import logging
import math
import statistics
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from astrategy.strategies.base import StrategySignal

logger = logging.getLogger("astrategy.backtest.evaluator")


class Evaluator:
    """Evaluate signal quality against realised returns.

    Requires a price-fetcher callback that, given a stock code and date
    range, returns a pandas DataFrame with at least ``date`` and
    ``close`` columns.
    """

    def __init__(
        self,
        price_fetcher: Optional[Callable[[str, str, str], pd.DataFrame]] = None,
    ) -> None:
        """
        Parameters
        ----------
        price_fetcher:
            ``(stock_code, start_date_str, end_date_str) -> DataFrame``
            with columns ``['date', 'close']``.  If ``None``, a stub
            that returns empty frames is used (useful for unit tests).
        """
        self._fetch = price_fetcher or self._stub_fetcher

    # ── single signal evaluation ──────────────────────────────────────

    def evaluate_signal(self, signal: StrategySignal) -> Dict[str, Any]:
        """Evaluate one signal against realised price data.

        Returns a dict with keys:
            strategy_name, stock_code, stock_name, direction, confidence,
            expected_return, holding_period_days, entry_date, exit_date,
            entry_price, exit_price, actual_return, hit, magnitude_error
        """
        entry_date = signal.timestamp.strftime("%Y%m%d")
        exit_dt = signal.timestamp + timedelta(days=signal.holding_period_days)
        # Fetch a few extra days to handle non-trading days
        fetch_end = (exit_dt + timedelta(days=10)).strftime("%Y%m%d")

        prices = self._fetch(signal.stock_code, entry_date, fetch_end)

        result: Dict[str, Any] = {
            "strategy_name": signal.strategy_name,
            "stock_code": signal.stock_code,
            "stock_name": signal.stock_name,
            "direction": signal.direction,
            "confidence": signal.confidence,
            "expected_return": signal.expected_return,
            "holding_period_days": signal.holding_period_days,
            "entry_date": entry_date,
            "exit_date": exit_dt.strftime("%Y%m%d"),
        }

        if prices.empty or len(prices) < 2:
            result.update({
                "entry_price": None,
                "exit_price": None,
                "actual_return": None,
                "hit": None,
                "magnitude_error": None,
            })
            return result

        entry_price = float(prices.iloc[0]["close"])

        # Find closest available price to exit date
        target_idx = min(signal.holding_period_days, len(prices) - 1)
        exit_price = float(prices.iloc[target_idx]["close"])

        actual_return = (exit_price - entry_price) / entry_price

        # Determine if direction was correct
        if signal.direction == "long":
            hit = actual_return > 0
        elif signal.direction in ("short", "avoid"):
            # avoid = 回避 — 如果股价下跌则"回避正确"
            hit = actual_return < 0
        else:
            hit = abs(actual_return) < 0.02  # neutral: stock didn't move much

        # Magnitude accuracy
        magnitude_error = abs(signal.expected_return - abs(actual_return))

        result.update({
            "entry_price": entry_price,
            "exit_price": exit_price,
            "actual_return": actual_return,
            "hit": hit,
            "magnitude_error": magnitude_error,
        })
        return result

    # ── batch evaluation ──────────────────────────────────────────────

    def evaluate_batch(self, signals: List[StrategySignal]) -> pd.DataFrame:
        """Evaluate a list of signals and return results as a DataFrame."""
        results = []
        for sig in signals:
            try:
                res = self.evaluate_signal(sig)
                results.append(res)
            except Exception as exc:
                logger.warning(
                    "Failed to evaluate signal %s/%s: %s",
                    sig.strategy_name,
                    sig.stock_code,
                    exc,
                )
                continue

        if not results:
            return pd.DataFrame()

        df = pd.DataFrame(results)
        return df

    # ── metric computation ────────────────────────────────────────────

    def compute_metrics(self, results: pd.DataFrame) -> Dict[str, Any]:
        """Compute performance metrics from evaluation results.

        Parameters
        ----------
        results:
            DataFrame from ``evaluate_batch()`` with at least
            ``actual_return``, ``hit``, ``confidence`` columns.

        Returns
        -------
        dict with hit_rate, avg_return, sharpe_ratio, max_drawdown, IC,
        IR, profit_factor, avg_holding_days.
        """
        # Drop rows without actual returns
        valid = results.dropna(subset=["actual_return", "hit"])
        if valid.empty:
            return self._empty_metrics()

        returns = valid["actual_return"].tolist()
        hits = valid["hit"].tolist()

        # Hit rate
        hit_rate = sum(1 for h in hits if h) / len(hits)

        # Average return
        avg_return = statistics.mean(returns)

        # Sharpe ratio (annualised, assuming daily signals)
        avg_hold = valid["holding_period_days"].mean()
        periods_per_year = 252.0 / max(avg_hold, 1)
        if len(returns) > 1:
            ret_std = statistics.stdev(returns)
            sharpe = (avg_return * periods_per_year) / max(ret_std * math.sqrt(periods_per_year), 1e-9)
        else:
            sharpe = 0.0

        # Max drawdown (sequential)
        max_dd = self._max_drawdown(returns)

        # IC: rank correlation between confidence and actual return
        ic = self._information_coefficient(valid)

        # IR: IC / std(IC) -- approximate using rolling IC
        ir = self._information_ratio(valid)

        # Profit factor
        winning = [r for r in returns if r > 0]
        losing = [r for r in returns if r < 0]
        sum_win = sum(winning) if winning else 0.0
        sum_loss = abs(sum(losing)) if losing else 0.0
        profit_factor = sum_win / max(sum_loss, 1e-9)

        return {
            "hit_rate": round(hit_rate, 4),
            "avg_return": round(avg_return, 6),
            "sharpe_ratio": round(sharpe, 4),
            "max_drawdown": round(max_dd, 6),
            "IC": round(ic, 4),
            "IR": round(ir, 4),
            "profit_factor": round(profit_factor, 4),
            "avg_holding_days": round(avg_hold, 1),
            "total_signals": len(valid),
            "winning_signals": len(winning),
            "losing_signals": len(losing),
        }

    # ── strategy comparison ───────────────────────────────────────────

    def compare_strategies(
        self, results_by_strategy: Dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """Side-by-side comparison of multiple strategies.

        Parameters
        ----------
        results_by_strategy:
            ``{strategy_name: evaluation_results_df}``

        Returns
        -------
        DataFrame with one row per strategy and metric columns.
        """
        rows = []
        for name, df in results_by_strategy.items():
            metrics = self.compute_metrics(df)
            metrics["strategy"] = name
            rows.append(metrics)

        if not rows:
            return pd.DataFrame()

        comparison = pd.DataFrame(rows)
        # Reorder columns
        cols = ["strategy"] + [c for c in comparison.columns if c != "strategy"]
        comparison = comparison[cols]
        comparison = comparison.sort_values("sharpe_ratio", ascending=False)
        return comparison.reset_index(drop=True)

    # ── report generation ─────────────────────────────────────────────

    def generate_report(self, metrics: Dict[str, Any]) -> str:
        """Generate a Markdown summary of backtest metrics."""
        lines = [
            "# Signal Backtest Report",
            "",
            "## Performance Metrics",
            "",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Hit Rate | {metrics.get('hit_rate', 'N/A'):.2%} |"
            if isinstance(metrics.get("hit_rate"), (int, float))
            else f"| Hit Rate | N/A |",
            f"| Avg Return | {metrics.get('avg_return', 0):.4%} |",
            f"| Sharpe Ratio | {metrics.get('sharpe_ratio', 0):.2f} |",
            f"| Max Drawdown | {metrics.get('max_drawdown', 0):.4%} |",
            f"| IC | {metrics.get('IC', 0):.4f} |",
            f"| IR | {metrics.get('IR', 0):.4f} |",
            f"| Profit Factor | {metrics.get('profit_factor', 0):.2f} |",
            f"| Avg Holding Days | {metrics.get('avg_holding_days', 0):.1f} |",
            "",
            "## Signal Summary",
            "",
            f"- Total Signals: {metrics.get('total_signals', 0)}",
            f"- Winning Signals: {metrics.get('winning_signals', 0)}",
            f"- Losing Signals: {metrics.get('losing_signals', 0)}",
            "",
            "## Interpretation",
            "",
        ]

        # Add interpretation
        hit_rate = metrics.get("hit_rate", 0)
        sharpe = metrics.get("sharpe_ratio", 0)
        ic = metrics.get("IC", 0)

        if hit_rate > 0.6 and sharpe > 1.0:
            lines.append("Strategy shows **strong** predictive power with high "
                         "hit rate and risk-adjusted return.")
        elif hit_rate > 0.5 and sharpe > 0.5:
            lines.append("Strategy shows **moderate** predictive power. "
                         "Consider combining with other strategies.")
        else:
            lines.append("Strategy shows **weak** predictive power. "
                         "Review signal generation logic and parameters.")

        if abs(ic) > 0.05:
            lines.append(f"\nIC of {ic:.4f} indicates that signal confidence "
                         f"has {'positive' if ic > 0 else 'negative'} "
                         f"correlation with actual returns.")
        else:
            lines.append("\nIC is near zero, suggesting signal confidence is "
                         "not informative about return magnitude.")

        return "\n".join(lines)

    # ── internal helpers ──────────────────────────────────────────────

    @staticmethod
    def _max_drawdown(returns: List[float]) -> float:
        """Compute max drawdown from a sequence of period returns."""
        if not returns:
            return 0.0

        cumulative = 1.0
        peak = 1.0
        max_dd = 0.0

        for r in returns:
            cumulative *= (1.0 + r)
            peak = max(peak, cumulative)
            dd = (peak - cumulative) / peak
            max_dd = max(max_dd, dd)

        return max_dd

    @staticmethod
    def _information_coefficient(df: pd.DataFrame) -> float:
        """Rank correlation between confidence and actual return."""
        if len(df) < 3:
            return 0.0
        try:
            return float(df["confidence"].corr(df["actual_return"], method="spearman"))
        except Exception:
            return 0.0

    @staticmethod
    def _information_ratio(df: pd.DataFrame, window: int = 20) -> float:
        """IC / std(IC) estimated from rolling windows."""
        if len(df) < window * 2:
            # Not enough data for rolling IC
            ic = Evaluator._information_coefficient(df)
            return ic  # Degenerate case: IR = IC when std is unknown

        ics = []
        for i in range(0, len(df) - window + 1, max(window // 2, 1)):
            chunk = df.iloc[i : i + window]
            if len(chunk) >= 5:
                ic = float(
                    chunk["confidence"].corr(chunk["actual_return"], method="spearman")
                )
                if not math.isnan(ic):
                    ics.append(ic)

        if not ics:
            return 0.0

        mean_ic = statistics.mean(ics)
        std_ic = statistics.stdev(ics) if len(ics) > 1 else 1e-9
        return mean_ic / max(std_ic, 1e-9)

    @staticmethod
    def _empty_metrics() -> Dict[str, Any]:
        return {
            "hit_rate": 0.0,
            "avg_return": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "IC": 0.0,
            "IR": 0.0,
            "profit_factor": 0.0,
            "avg_holding_days": 0.0,
            "total_signals": 0,
            "winning_signals": 0,
            "losing_signals": 0,
        }

    @staticmethod
    def _stub_fetcher(stock_code: str, start: str, end: str) -> pd.DataFrame:
        """Stub price fetcher that returns an empty DataFrame."""
        return pd.DataFrame(columns=["date", "close"])
