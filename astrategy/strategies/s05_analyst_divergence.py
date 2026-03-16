"""
Analyst Divergence Strategy (S05)
==================================

When analyst opinions on a stock converge (especially bears turning bullish),
it is a strong buy signal.  High divergence means uncertainty.  This strategy
tracks changes in analyst divergence over time to detect convergence shifts.

Core logic:
1. Collect individual analyst ratings and target prices.
2. Compute a divergence score (std of ratings, CV of target prices).
3. Compare current divergence against historical snapshots to detect
   convergence or divergence trends.
4. Detect upgrade / downgrade waves (3+ changes in a short period).
5. Generate signals: convergence + consensus upgrading + price below target
   => long; convergence on downgrade + price above target => short.

Mostly quantitative -- no LLM required for the basic version.

Typical holding period: 30-60 trading days.
"""

from __future__ import annotations

import logging
import math
import statistics
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from astrategy.config import settings
from astrategy.data_collector.market_data import MarketDataCollector
from astrategy.data_collector.research import ResearchCollector
from astrategy.strategies.base import BaseStrategy, StrategySignal

logger = logging.getLogger("astrategy.strategies.s05_analyst_divergence")

_CST = timezone(timedelta(hours=8))

# Rating text -> numeric mapping (Chinese broker convention)
RATING_MAP: Dict[str, float] = {
    "买入": 5.0,
    "强烈推荐": 5.0,
    "强推": 5.0,
    "推荐": 4.5,
    "增持": 4.0,
    "优于大市": 4.0,
    "跑赢行业": 4.0,
    "谨慎增持": 3.5,
    "中性": 3.0,
    "持有": 3.0,
    "同步大市": 3.0,
    "观望": 3.0,
    "减持": 2.0,
    "回避": 2.0,
    "卖出": 1.0,
    "强烈卖出": 1.0,
}

# Minimum number of analysts required for meaningful divergence analysis
MIN_ANALYST_COUNT = 3

# Upgrade/downgrade wave: minimum changes in the window
WAVE_MIN_COUNT = 3
WAVE_WINDOW_DAYS = 30

# Default holding period
DEFAULT_HOLDING_DAYS = 45


def _safe_float(val: Any) -> Optional[float]:
    """Attempt to parse *val* as a float; return ``None`` on failure."""
    if val is None:
        return None
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (ValueError, TypeError):
        return None


def _normalize_rating(text: str) -> Optional[float]:
    """Convert a Chinese rating string to a numeric 1-5 score."""
    if not text:
        return None
    text = text.strip()
    # Exact match first
    if text in RATING_MAP:
        return RATING_MAP[text]
    # Substring match
    for key, score in RATING_MAP.items():
        if key in text:
            return score
    return None


class AnalystDivergenceStrategy(BaseStrategy):
    """Analyst divergence strategy based on convergence/divergence of opinions.

    Parameters
    ----------
    holding_days : int
        Default holding period for signals (default 45 days).
    signal_dir : Path | str | None
        Override for the signal persistence directory.
    """

    def __init__(
        self,
        holding_days: int = DEFAULT_HOLDING_DAYS,
        signal_dir: Path | str | None = None,
    ) -> None:
        super().__init__(signal_dir=signal_dir)
        self._holding_days = holding_days
        self._research = ResearchCollector()
        self._market = MarketDataCollector()

    # ── BaseStrategy interface ────────────────────────────────────────

    @property
    def name(self) -> str:
        return "analyst_divergence"

    # ==================================================================
    # 1. get_analyst_data
    # ==================================================================

    def get_analyst_data(self, stock_code: str) -> dict:
        """Fetch analyst ratings and consensus forecast for *stock_code*.

        Returns a dict with:
            stock_code, stock_name, ratings (list[dict]), target_prices (list[float]),
            numeric_ratings (list[float]), consensus_forecast (dict),
            has_data (bool)
        """
        result: Dict[str, Any] = {
            "stock_code": stock_code,
            "stock_name": stock_code,
            "ratings": [],
            "target_prices": [],
            "numeric_ratings": [],
            "consensus_forecast": {},
            "brokers": [],
            "rating_changes": [],
            "has_data": False,
        }

        # ── Individual analyst ratings ────────────────────────────────
        raw_ratings = self._research.get_analyst_ratings(stock_code)
        if not raw_ratings:
            logger.warning("No analyst ratings for %s", stock_code)
            return result

        numeric_ratings: List[float] = []
        target_prices: List[float] = []
        brokers: List[str] = []
        rating_changes: List[dict] = []

        for entry in raw_ratings:
            # Try to extract stock name
            for name_key in ("股票简称", "股票名称", "名称"):
                if name_key in entry and entry[name_key]:
                    result["stock_name"] = str(entry[name_key])
                    break

            # Extract and normalise the rating
            rating_text = None
            for key in ("评级", "最新评级", "投资评级", "评级名称", "综合评级"):
                if key in entry and entry[key]:
                    rating_text = str(entry[key])
                    break

            if rating_text:
                numeric = _normalize_rating(rating_text)
                if numeric is not None:
                    numeric_ratings.append(numeric)

            # Extract target price
            for key in ("目标价", "最高目标价", "目标价(元)", "预测目标价"):
                tp = _safe_float(entry.get(key))
                if tp is not None and tp > 0:
                    target_prices.append(tp)
                    break

            # Extract broker / institution
            for key in ("研究机构", "机构名称", "券商", "机构"):
                if key in entry and entry[key]:
                    brokers.append(str(entry[key]))
                    break

            # Detect rating changes (previous vs current)
            prev_rating_text = None
            for key in ("前次评级", "上次评级", "调整前评级"):
                if key in entry and entry[key]:
                    prev_rating_text = str(entry[key])
                    break

            if prev_rating_text and rating_text:
                prev_numeric = _normalize_rating(prev_rating_text)
                curr_numeric = _normalize_rating(rating_text)
                if prev_numeric is not None and curr_numeric is not None:
                    change_type = "upgrade" if curr_numeric > prev_numeric else (
                        "downgrade" if curr_numeric < prev_numeric else "maintain"
                    )
                    # Extract date
                    report_date = None
                    for dkey in ("报告日期", "日期", "发布日期", "研报日期"):
                        if dkey in entry and entry[dkey]:
                            report_date = str(entry[dkey])
                            break

                    broker_name = ""
                    for bkey in ("研究机构", "机构名称", "券商", "机构"):
                        if bkey in entry and entry[bkey]:
                            broker_name = str(entry[bkey])
                            break

                    rating_changes.append({
                        "broker": broker_name,
                        "prev_rating": prev_numeric,
                        "curr_rating": curr_numeric,
                        "change": curr_numeric - prev_numeric,
                        "change_type": change_type,
                        "date": report_date,
                    })

        result["ratings"] = raw_ratings
        result["numeric_ratings"] = numeric_ratings
        result["target_prices"] = target_prices
        result["brokers"] = brokers
        result["rating_changes"] = rating_changes

        # ── Consensus forecast ────────────────────────────────────────
        consensus = self._research.get_consensus_forecast(stock_code)
        result["consensus_forecast"] = consensus

        result["has_data"] = len(numeric_ratings) >= MIN_ANALYST_COUNT
        return result

    # ==================================================================
    # 2. compute_divergence
    # ==================================================================

    def compute_divergence(self, ratings: List[float], target_prices: List[float]) -> dict:
        """Compute divergence metrics from analyst ratings and target prices.

        Returns:
            rating_std: standard deviation of numeric ratings
            price_divergence: coefficient of variation of target prices
            consensus_score: mean of numeric ratings (1-5 scale)
            bull_bear_ratio: count(>=4) / count(<=2), inf if no bears
        """
        result: Dict[str, Any] = {
            "rating_std": 0.0,
            "price_divergence": 0.0,
            "consensus_score": 3.0,
            "bull_bear_ratio": 1.0,
            "analyst_count": len(ratings),
            "target_price_count": len(target_prices),
        }

        if not ratings:
            return result

        # Consensus score (mean rating)
        result["consensus_score"] = round(statistics.mean(ratings), 2)

        # Rating standard deviation
        if len(ratings) >= 2:
            result["rating_std"] = round(statistics.stdev(ratings), 4)
        else:
            result["rating_std"] = 0.0

        # Bull / bear ratio
        bulls = sum(1 for r in ratings if r >= 4.0)
        bears = sum(1 for r in ratings if r <= 2.0)
        if bears > 0:
            result["bull_bear_ratio"] = round(bulls / bears, 2)
        elif bulls > 0:
            result["bull_bear_ratio"] = float("inf")
        else:
            result["bull_bear_ratio"] = 1.0

        # Price divergence (coefficient of variation)
        if len(target_prices) >= 2:
            tp_mean = statistics.mean(target_prices)
            if tp_mean > 0:
                tp_std = statistics.stdev(target_prices)
                result["price_divergence"] = round(tp_std / tp_mean, 4)
        elif len(target_prices) == 1:
            result["price_divergence"] = 0.0

        return result

    # ==================================================================
    # 3. detect_convergence_shift
    # ==================================================================

    def detect_convergence_shift(
        self,
        stock_code: str,
        current: dict,
        historical: List[dict],
    ) -> dict:
        """Compare current divergence against historical snapshots to detect shifts.

        Parameters
        ----------
        stock_code : str
            Stock code for logging.
        current : dict
            Current divergence metrics (from ``compute_divergence``).
        historical : list[dict]
            List of past divergence snapshots, ordered newest-first.
            Each item has keys: rating_std, consensus_score, price_divergence,
            and optionally a ``date`` field.

        Returns
        -------
        dict
            shift_type: "converging" | "diverging" | "stable"
            shift_magnitude: float (positive = converging, negative = diverging)
            signal_strength: float (0-1)
            consensus_direction: "upgrading" | "downgrading" | "stable"
            convergence_type: "bear_to_bull" | "bull_to_bear" | "stable"
        """
        result: Dict[str, Any] = {
            "shift_type": "stable",
            "shift_magnitude": 0.0,
            "signal_strength": 0.0,
            "consensus_direction": "stable",
            "convergence_type": "stable",
            "divergence_change_1m": 0.0,
            "divergence_change_3m": 0.0,
            "consensus_change_1m": 0.0,
            "consensus_change_3m": 0.0,
        }

        if not historical:
            return result

        cur_std = current.get("rating_std", 0.0)
        cur_consensus = current.get("consensus_score", 3.0)

        # Compare with 1-month-ago snapshot (index 0 = most recent historical)
        hist_1m = historical[0] if len(historical) >= 1 else None
        hist_3m = historical[2] if len(historical) >= 3 else (
            historical[-1] if len(historical) >= 2 else None
        )

        if hist_1m:
            prev_std_1m = hist_1m.get("rating_std", cur_std)
            prev_cons_1m = hist_1m.get("consensus_score", cur_consensus)
            result["divergence_change_1m"] = round(cur_std - prev_std_1m, 4)
            result["consensus_change_1m"] = round(cur_consensus - prev_cons_1m, 4)

        if hist_3m:
            prev_std_3m = hist_3m.get("rating_std", cur_std)
            prev_cons_3m = hist_3m.get("consensus_score", cur_consensus)
            result["divergence_change_3m"] = round(cur_std - prev_std_3m, 4)
            result["consensus_change_3m"] = round(cur_consensus - prev_cons_3m, 4)

        # Determine shift type using the best available comparison
        divergence_change = result["divergence_change_1m"]
        if abs(result["divergence_change_3m"]) > abs(divergence_change):
            divergence_change = result["divergence_change_3m"]

        if divergence_change < -0.1:
            result["shift_type"] = "converging"
            result["shift_magnitude"] = abs(divergence_change)
        elif divergence_change > 0.1:
            result["shift_type"] = "diverging"
            result["shift_magnitude"] = -abs(divergence_change)
        else:
            result["shift_type"] = "stable"
            result["shift_magnitude"] = 0.0

        # Consensus direction
        consensus_change = result["consensus_change_1m"]
        if abs(result["consensus_change_3m"]) > abs(consensus_change):
            consensus_change = result["consensus_change_3m"]

        if consensus_change > 0.2:
            result["consensus_direction"] = "upgrading"
        elif consensus_change < -0.2:
            result["consensus_direction"] = "downgrading"
        else:
            result["consensus_direction"] = "stable"

        # Special signal: bears turning bullish (consensus rising + divergence falling)
        if (result["shift_type"] == "converging"
                and result["consensus_direction"] == "upgrading"):
            result["convergence_type"] = "bear_to_bull"
        elif (result["shift_type"] == "converging"
              and result["consensus_direction"] == "downgrading"):
            result["convergence_type"] = "bull_to_bear"
        else:
            result["convergence_type"] = "stable"

        # Signal strength: combination of convergence magnitude and consensus change
        convergence_strength = min(1.0, abs(result["shift_magnitude"]) / 0.5)
        consensus_strength = min(1.0, abs(consensus_change) / 1.0)
        result["signal_strength"] = round(
            0.6 * convergence_strength + 0.4 * consensus_strength, 2
        )

        logger.debug(
            "[%s] Convergence shift: type=%s, magnitude=%.3f, "
            "consensus_dir=%s, convergence_type=%s, strength=%.2f",
            stock_code,
            result["shift_type"],
            result["shift_magnitude"],
            result["consensus_direction"],
            result["convergence_type"],
            result["signal_strength"],
        )

        return result

    # ==================================================================
    # 4. detect_upgrade_downgrade_waves
    # ==================================================================

    def detect_upgrade_downgrade_waves(self, stock_code: str, rating_changes: List[dict]) -> dict:
        """Detect upgrade or downgrade waves from recent rating changes.

        A "wave" is defined as WAVE_MIN_COUNT or more rating changes in the
        same direction within WAVE_WINDOW_DAYS.

        Parameters
        ----------
        stock_code : str
            Stock code for logging.
        rating_changes : list[dict]
            Rating change records, each with keys: change_type, broker, date.

        Returns
        -------
        dict
            wave_type: "upgrade_wave" | "downgrade_wave" | "mixed" | "none"
            wave_count: int (total directional changes in the wave)
            upgrade_count: int
            downgrade_count: int
            leading_brokers: list[str]
        """
        result: Dict[str, Any] = {
            "wave_type": "none",
            "wave_count": 0,
            "upgrade_count": 0,
            "downgrade_count": 0,
            "leading_brokers": [],
        }

        if not rating_changes:
            return result

        # Filter to recent changes (within the wave window)
        cutoff = datetime.now(tz=_CST) - timedelta(days=WAVE_WINDOW_DAYS)
        recent_changes: List[dict] = []

        for rc in rating_changes:
            date_str = rc.get("date")
            if date_str:
                try:
                    dt = pd.Timestamp(date_str)
                    if dt.tzinfo is None:
                        dt = dt.tz_localize(_CST)
                    if dt >= cutoff:
                        recent_changes.append(rc)
                        continue
                except Exception:
                    pass
            # If no parseable date, include it (conservative)
            recent_changes.append(rc)

        upgrades = [rc for rc in recent_changes if rc.get("change_type") == "upgrade"]
        downgrades = [rc for rc in recent_changes if rc.get("change_type") == "downgrade"]

        result["upgrade_count"] = len(upgrades)
        result["downgrade_count"] = len(downgrades)

        if len(upgrades) >= WAVE_MIN_COUNT and len(upgrades) > len(downgrades) * 2:
            result["wave_type"] = "upgrade_wave"
            result["wave_count"] = len(upgrades)
            result["leading_brokers"] = [
                rc.get("broker", "") for rc in upgrades if rc.get("broker")
            ][:5]
        elif len(downgrades) >= WAVE_MIN_COUNT and len(downgrades) > len(upgrades) * 2:
            result["wave_type"] = "downgrade_wave"
            result["wave_count"] = len(downgrades)
            result["leading_brokers"] = [
                rc.get("broker", "") for rc in downgrades if rc.get("broker")
            ][:5]
        elif len(upgrades) + len(downgrades) >= WAVE_MIN_COUNT:
            result["wave_type"] = "mixed"
            result["wave_count"] = len(upgrades) + len(downgrades)
            result["leading_brokers"] = [
                rc.get("broker", "") for rc in recent_changes if rc.get("broker")
            ][:5]

        logger.debug(
            "[%s] Wave detection: type=%s, upgrades=%d, downgrades=%d, brokers=%s",
            stock_code,
            result["wave_type"],
            result["upgrade_count"],
            result["downgrade_count"],
            result["leading_brokers"],
        )

        return result

    # ==================================================================
    # 5. compute_target_price_gap
    # ==================================================================

    def compute_target_price_gap(self, stock_code: str, target_prices: List[float]) -> dict:
        """Compare current market price against analyst target prices.

        Returns:
            current_price, consensus_target, upside_pct,
            target_price_high, target_price_low, target_price_median,
            pct_above_target (fraction of targets below current price)
        """
        result: Dict[str, Any] = {
            "current_price": None,
            "consensus_target": None,
            "upside_pct": None,
            "target_price_high": None,
            "target_price_low": None,
            "target_price_median": None,
            "pct_above_target": None,
        }

        # Get current price
        try:
            quotes = self._market.get_realtime_quotes([stock_code])
            if quotes is not None and not quotes.empty:
                price_col = "最新价" if "最新价" in quotes.columns else "收盘"
                current_price = _safe_float(quotes.iloc[0].get(price_col))
                result["current_price"] = current_price
            else:
                current_price = None
        except Exception as exc:
            logger.warning("Failed to get current price for %s: %s", stock_code, exc)
            current_price = None

        if not target_prices:
            return result

        consensus_target = round(statistics.mean(target_prices), 2)
        result["consensus_target"] = consensus_target
        result["target_price_high"] = round(max(target_prices), 2)
        result["target_price_low"] = round(min(target_prices), 2)
        result["target_price_median"] = round(statistics.median(target_prices), 2)

        if current_price is not None and current_price > 0:
            upside = (consensus_target / current_price - 1.0) * 100.0
            result["upside_pct"] = round(upside, 2)

            # What fraction of target prices are below current price?
            below_count = sum(1 for tp in target_prices if tp < current_price)
            result["pct_above_target"] = round(below_count / len(target_prices), 2)

        return result

    # ==================================================================
    # 6. run
    # ==================================================================

    def run(self, stock_codes: List[str] | None = None) -> List[StrategySignal]:
        """Full analyst divergence pipeline.

        For each stock:
        1. Fetch analyst data.
        2. Compute divergence metrics.
        3. Detect convergence shifts (using rating changes as historical proxy).
        4. Detect upgrade/downgrade waves.
        5. Compute target price gap.
        6. Generate signals.

        Parameters
        ----------
        stock_codes:
            Universe of stock codes.  If ``None``, returns empty.

        Returns
        -------
        list[StrategySignal]
        """
        if not stock_codes:
            logger.warning("[%s] No stock codes provided; returning empty.", self.name)
            return []

        logger.info(
            "[%s] Starting analyst divergence analysis for %d stocks ...",
            self.name, len(stock_codes),
        )

        max_stocks = getattr(settings.strategy, "max_stocks_per_run", 30)
        codes_to_run = stock_codes[:max_stocks]

        signals: List[StrategySignal] = []
        for code in codes_to_run:
            try:
                code_signals = self.run_single(code)
                signals.extend(code_signals)
            except Exception as exc:
                logger.error("[%s] Failed to analyse %s: %s", self.name, code, exc)

        logger.info("[%s] Generated %d signals from %d stocks.", self.name, len(signals), len(codes_to_run))
        return signals

    # ==================================================================
    # 7. run_single
    # ==================================================================

    def run_single(self, stock_code: str) -> List[StrategySignal]:
        """Run analyst divergence analysis for a single stock.

        Returns a list with zero or one StrategySignal.
        """
        logger.info("[%s] Analysing %s ...", self.name, stock_code)

        # Step 1: Get analyst data
        data = self.get_analyst_data(stock_code)
        if not data.get("has_data"):
            logger.info(
                "[%s] Insufficient analyst data for %s (need >= %d ratings).",
                self.name, stock_code, MIN_ANALYST_COUNT,
            )
            return []

        numeric_ratings = data["numeric_ratings"]
        target_prices = data["target_prices"]
        rating_changes = data.get("rating_changes", [])

        # Step 2: Compute current divergence
        divergence = self.compute_divergence(numeric_ratings, target_prices)

        # Step 3: Detect convergence shift
        # Build a historical proxy from rating changes: group changes by
        # time bucket and compute implied past divergence.
        historical = self._build_historical_divergence(rating_changes, numeric_ratings)
        shift = self.detect_convergence_shift(stock_code, divergence, historical)

        # Step 4: Detect upgrade/downgrade waves
        wave = self.detect_upgrade_downgrade_waves(stock_code, rating_changes)

        # Step 5: Compute target price gap
        price_gap = self.compute_target_price_gap(stock_code, target_prices)

        # Step 6: Generate signal
        signal = self._generate_signal(
            stock_code=stock_code,
            stock_name=data.get("stock_name", stock_code),
            divergence=divergence,
            shift=shift,
            wave=wave,
            price_gap=price_gap,
        )

        return [signal] if signal is not None else []

    # ==================================================================
    # Internal helpers
    # ==================================================================

    def _build_historical_divergence(
        self,
        rating_changes: List[dict],
        current_ratings: List[float],
    ) -> List[dict]:
        """Synthesise approximate historical divergence snapshots from rating changes.

        The idea: reconstruct what the ratings *were* before each change,
        then compute divergence for that earlier state.  This is an
        approximation since we only have the changes, not full snapshots.

        Returns a list of divergence dicts, newest first.
        """
        if not rating_changes or not current_ratings:
            return []

        # Sort changes newest first
        def _parse_date(rc: dict) -> datetime:
            ds = rc.get("date", "")
            try:
                return pd.Timestamp(ds).to_pydatetime()
            except Exception:
                return datetime.min

        sorted_changes = sorted(rating_changes, key=_parse_date, reverse=True)

        snapshots: List[dict] = []
        # Walk backward, un-applying each change to reconstruct previous state
        reconstructed = list(current_ratings)

        for i, rc in enumerate(sorted_changes):
            change = rc.get("change", 0.0)
            if change != 0 and reconstructed:
                # Find a rating in reconstructed that matches curr_rating and
                # revert it to prev_rating.
                curr = rc.get("curr_rating")
                prev = rc.get("prev_rating")
                if curr is not None and prev is not None:
                    try:
                        idx = reconstructed.index(curr)
                        reconstructed[idx] = prev
                    except ValueError:
                        pass

            # Compute divergence for this historical state
            if len(reconstructed) >= MIN_ANALYST_COUNT:
                mean_r = statistics.mean(reconstructed)
                std_r = statistics.stdev(reconstructed) if len(reconstructed) >= 2 else 0.0
                snapshots.append({
                    "rating_std": round(std_r, 4),
                    "consensus_score": round(mean_r, 2),
                    "price_divergence": 0.0,  # not available historically
                })

            # Limit to a few snapshots
            if len(snapshots) >= 4:
                break

        return snapshots

    def _generate_signal(
        self,
        stock_code: str,
        stock_name: str,
        divergence: dict,
        shift: dict,
        wave: dict,
        price_gap: dict,
    ) -> Optional[StrategySignal]:
        """Combine all analysis components into a single StrategySignal.

        Direction logic:
        - long:  convergence + consensus upgrading + price below target
        - short: convergence on downgrade + price above target
        - neutral: otherwise

        Confidence is derived from signal_strength, consensus_score,
        bull_bear_ratio, and upside/downside potential.
        """
        consensus = divergence.get("consensus_score", 3.0)
        rating_std = divergence.get("rating_std", 0.0)
        bull_bear = divergence.get("bull_bear_ratio", 1.0)
        convergence_type = shift.get("convergence_type", "stable")
        signal_strength = shift.get("signal_strength", 0.0)
        shift_type = shift.get("shift_type", "stable")
        consensus_dir = shift.get("consensus_direction", "stable")
        upside_pct = price_gap.get("upside_pct")
        wave_type = wave.get("wave_type", "none")

        # ── Direction ─────────────────────────────────────────────────
        direction = "neutral"
        reasoning_parts: List[str] = []

        # Strong bull signal: bears turning bullish
        if convergence_type == "bear_to_bull":
            direction = "long"
            reasoning_parts.append("Analysts converging bullish (bear-to-bull shift)")
        # Convergence + upgrading + price below target
        elif (shift_type == "converging"
              and consensus_dir == "upgrading"
              and upside_pct is not None and upside_pct > 5):
            direction = "long"
            reasoning_parts.append(
                f"Analyst convergence with upgrading consensus; "
                f"target upside {upside_pct:+.1f}%"
            )
        # Upgrade wave
        elif wave_type == "upgrade_wave" and consensus >= 4.0:
            direction = "long"
            reasoning_parts.append(
                f"Upgrade wave detected ({wave.get('wave_count', 0)} upgrades)"
            )
        # Strong bear signal: convergence on downgrade
        elif convergence_type == "bull_to_bear":
            direction = "short"
            reasoning_parts.append("Analysts converging bearish (bull-to-bear shift)")
        elif (shift_type == "converging"
              and consensus_dir == "downgrading"
              and upside_pct is not None and upside_pct < -5):
            direction = "short"
            reasoning_parts.append(
                f"Analyst convergence on downgrade; "
                f"target downside {upside_pct:+.1f}%"
            )
        # Downgrade wave
        elif wave_type == "downgrade_wave" and consensus <= 2.5:
            direction = "short"
            reasoning_parts.append(
                f"Downgrade wave detected ({wave.get('wave_count', 0)} downgrades)"
            )
        # Bullish consensus with significant upside
        elif consensus >= 4.0 and upside_pct is not None and upside_pct > 15:
            direction = "long"
            reasoning_parts.append(
                f"Strong consensus (score={consensus:.1f}) with {upside_pct:+.1f}% upside"
            )
        # Bearish consensus with downside
        elif consensus <= 2.5 and upside_pct is not None and upside_pct < -10:
            direction = "short"
            reasoning_parts.append(
                f"Weak consensus (score={consensus:.1f}) with {upside_pct:+.1f}% downside"
            )
        else:
            direction = "neutral"
            reasoning_parts.append(
                f"No strong directional signal (consensus={consensus:.1f}, "
                f"shift={shift_type})"
            )

        # Additional reasoning context
        reasoning_parts.append(
            f"Divergence: std={rating_std:.3f}, bull/bear={bull_bear:.1f}"
        )
        if wave_type != "none":
            reasoning_parts.append(
                f"Wave: {wave_type} ({wave.get('upgrade_count', 0)} up, "
                f"{wave.get('downgrade_count', 0)} down)"
            )

        # ── Confidence ────────────────────────────────────────────────
        # Base confidence from signal strength
        base_conf = signal_strength

        # Boost for strong consensus
        if consensus >= 4.5:
            base_conf += 0.15
        elif consensus >= 4.0:
            base_conf += 0.1
        elif consensus <= 2.0:
            base_conf += 0.1
        elif consensus <= 2.5:
            base_conf += 0.05

        # Boost for low divergence (high agreement)
        if rating_std < 0.3:
            base_conf += 0.1
        elif rating_std < 0.5:
            base_conf += 0.05

        # Boost for upgrade/downgrade waves
        if wave_type in ("upgrade_wave", "downgrade_wave"):
            base_conf += 0.1

        # Boost for strong target price gap
        if upside_pct is not None and abs(upside_pct) > 20:
            base_conf += 0.1

        # Analyst coverage factor (more analysts = more reliable)
        analyst_count = divergence.get("analyst_count", 0)
        coverage_bonus = min(0.1, analyst_count * 0.01)
        base_conf += coverage_bonus

        # Penalise neutral signals
        if direction == "neutral":
            base_conf *= 0.5

        confidence = max(0.05, min(0.95, base_conf))

        # ── Expected return ───────────────────────────────────────────
        if upside_pct is not None:
            # Use target price upside as expected return, dampened
            expected_return = upside_pct * 0.005  # 5% of the upside gap
            expected_return = max(-0.15, min(0.15, expected_return))
            if direction == "short":
                expected_return = -abs(expected_return)
            elif direction == "neutral":
                expected_return = 0.0
        else:
            expected_return = 0.03 if direction == "long" else (
                -0.03 if direction == "short" else 0.0
            )

        # ── Metadata ──────────────────────────────────────────────────
        # Serialise bull_bear_ratio: JSON cannot handle inf
        serialisable_bull_bear = bull_bear if math.isfinite(bull_bear) else 999.0

        metadata: Dict[str, Any] = {
            "consensus_rating": consensus,
            "divergence_score": rating_std,
            "divergence_change": shift.get("divergence_change_1m", 0.0),
            "bull_bear_ratio": serialisable_bull_bear,
            "recent_upgrades": wave.get("upgrade_count", 0),
            "recent_downgrades": wave.get("downgrade_count", 0),
            "target_price_upside": upside_pct,
            "convergence_type": convergence_type,
            "leading_brokers": wave.get("leading_brokers", []),
            "analyst_count": analyst_count,
            "price_divergence": divergence.get("price_divergence", 0.0),
            "consensus_target": price_gap.get("consensus_target"),
            "current_price": price_gap.get("current_price"),
            "wave_type": wave_type,
        }

        reasoning = "; ".join(reasoning_parts)

        return StrategySignal(
            strategy_name=self.name,
            stock_code=stock_code,
            stock_name=stock_name,
            direction=direction,
            confidence=round(confidence, 2),
            expected_return=round(expected_return, 4),
            holding_period_days=self._holding_days,
            reasoning=reasoning,
            metadata=metadata,
        )
