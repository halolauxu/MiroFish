"""
Advanced Signal Aggregator for multi-strategy consensus.

Extends the basic aggregator in ``strategies/base.py`` with weighted
aggregation, conflict handling, and signal decay.
"""

from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from astrategy.strategies.base import CompositeSignal as _BaseCompositeSignal
from astrategy.strategies.base import StrategySignal, _CST, _now_cst

logger = logging.getLogger("astrategy.aggregator.signal_aggregator")


# ---------------------------------------------------------------------------
# Extended composite signal
# ---------------------------------------------------------------------------

@dataclass
class CompositeSignal:
    """Consensus signal with richer metadata than the base version."""

    stock_code: str
    stock_name: str
    direction: str  # "long", "short", "neutral"
    composite_confidence: float  # 0.0 – 1.0
    contributing_strategies: List[str]
    strategy_agreement_ratio: float  # 0.0 – 1.0
    expected_return: float
    risk_score: float  # 0.0 – 1.0, higher = riskier
    # Extra detail
    composite_score: float = 0.0
    avg_holding_period_days: float = 0.0
    signals: List[StrategySignal] = field(default_factory=list, repr=False)

    def to_dict(self) -> dict:
        return {
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "direction": self.direction,
            "composite_confidence": round(self.composite_confidence, 4),
            "contributing_strategies": self.contributing_strategies,
            "strategy_agreement_ratio": round(self.strategy_agreement_ratio, 4),
            "expected_return": round(self.expected_return, 4),
            "risk_score": round(self.risk_score, 4),
            "composite_score": round(self.composite_score, 4),
            "avg_holding_period_days": round(self.avg_holding_period_days, 1),
        }


# ---------------------------------------------------------------------------
# Strategy performance record (for weighted aggregation)
# ---------------------------------------------------------------------------

@dataclass
class _StrategyPerf:
    """Historical performance record used for weighting."""

    hit_rate: float = 0.5
    avg_return: float = 0.0
    sharpe: float = 0.0
    signal_count: int = 0

    @property
    def weight(self) -> float:
        """Derive a weight from hit-rate and Sharpe, clamped to [0.1, 3.0]."""
        base = self.hit_rate * 2.0  # 50 % hit-rate -> weight 1.0
        if self.sharpe > 0:
            base *= 1.0 + min(self.sharpe, 2.0) * 0.25
        return max(0.1, min(3.0, base))


# ---------------------------------------------------------------------------
# Signal Aggregator
# ---------------------------------------------------------------------------

class SignalAggregator:
    """Advanced multi-strategy signal aggregator.

    Supports equal-weight, performance-weighted, and voting aggregation.
    Handles conflicts, signal decay, and confidence filtering.
    """

    def __init__(self) -> None:
        self._signals: List[StrategySignal] = []
        self._strategy_perf: Dict[str, _StrategyPerf] = {}

    # ── ingestion ──────────────────────────────────────────────────────

    def add_signals(self, signals: List[StrategySignal]) -> None:
        self._signals.extend(signals)

    def clear(self) -> None:
        self._signals.clear()

    @property
    def all_signals(self) -> List[StrategySignal]:
        return list(self._signals)

    def set_strategy_performance(
        self,
        strategy_name: str,
        hit_rate: float = 0.5,
        avg_return: float = 0.0,
        sharpe: float = 0.0,
        signal_count: int = 0,
    ) -> None:
        """Register historical performance for a strategy (used by
        ``weighted`` aggregation)."""
        self._strategy_perf[strategy_name] = _StrategyPerf(
            hit_rate=hit_rate,
            avg_return=avg_return,
            sharpe=sharpe,
            signal_count=signal_count,
        )

    # ── core aggregation ──────────────────────────────────────────────

    def aggregate_signals(
        self,
        signals: Optional[List[StrategySignal]] = None,
        method: str = "weighted",
    ) -> List[CompositeSignal]:
        """Aggregate signals into per-stock composite signals.

        Parameters
        ----------
        signals:
            Signals to aggregate.  If ``None``, uses internally stored
            signals.
        method:
            ``'equal_weight'``, ``'weighted'`` (by strategy performance),
            or ``'voting'`` (majority rules).
        """
        source = signals if signals is not None else self._signals
        if not source:
            return []

        # Apply decay before aggregation
        source = self._decay_old_signals(source)

        # Group by stock
        grouped: Dict[str, List[StrategySignal]] = defaultdict(list)
        for sig in source:
            if not sig.is_expired:
                grouped[sig.stock_code].append(sig)

        composites: List[CompositeSignal] = []
        for stock_code, stock_signals in grouped.items():
            cs = self._aggregate_stock(stock_code, stock_signals, method)
            if cs is not None:
                composites.append(cs)

        # Sort by absolute composite confidence descending
        composites.sort(
            key=lambda c: (abs(c.composite_confidence), c.expected_return),
            reverse=True,
        )
        return composites

    def _aggregate_stock(
        self,
        stock_code: str,
        signals: List[StrategySignal],
        method: str,
    ) -> Optional[CompositeSignal]:
        if not signals:
            return None

        stock_name = signals[0].stock_name

        # ── compute direction & weights by method ──
        if method == "voting":
            return self._voting_aggregate(stock_code, stock_name, signals)
        elif method == "equal_weight":
            weights = {s.strategy_name: 1.0 for s in signals}
        elif method == "weighted":
            weights = {}
            for s in signals:
                perf = self._strategy_perf.get(s.strategy_name)
                weights[s.strategy_name] = perf.weight if perf else 1.0
        else:
            raise ValueError(f"Unknown aggregation method: {method}")

        return self._weighted_aggregate(stock_code, stock_name, signals, weights)

    def _weighted_aggregate(
        self,
        stock_code: str,
        stock_name: str,
        signals: List[StrategySignal],
        weights: Dict[str, float],
    ) -> CompositeSignal:
        """Produce a composite signal using per-strategy weights."""
        total_weight = 0.0
        weighted_score = 0.0
        weighted_conf = 0.0
        weighted_return = 0.0
        weighted_hold = 0.0

        for sig in signals:
            w = weights.get(sig.strategy_name, 1.0)
            total_weight += w
            weighted_score += sig.signed_score * w
            weighted_conf += sig.confidence * w
            weighted_return += sig.expected_return * w
            weighted_hold += sig.holding_period_days * w

        if total_weight == 0:
            total_weight = 1.0

        composite_score = weighted_score / total_weight
        avg_conf = weighted_conf / total_weight
        avg_return = weighted_return / total_weight
        avg_hold = weighted_hold / total_weight

        # Determine direction
        direction = self._score_to_direction(composite_score)

        # Check for conflicts and adjust confidence
        agreement_ratio, risk_score = self._compute_agreement(signals)
        adjusted_conf = avg_conf * agreement_ratio  # reduce confidence when strategies disagree

        strategies = sorted({s.strategy_name for s in signals})

        return CompositeSignal(
            stock_code=stock_code,
            stock_name=stock_name,
            direction=direction,
            composite_confidence=max(0.0, min(1.0, adjusted_conf)),
            contributing_strategies=strategies,
            strategy_agreement_ratio=agreement_ratio,
            expected_return=avg_return,
            risk_score=risk_score,
            composite_score=composite_score,
            avg_holding_period_days=avg_hold,
            signals=signals,
        )

    def _voting_aggregate(
        self,
        stock_code: str,
        stock_name: str,
        signals: List[StrategySignal],
    ) -> CompositeSignal:
        """Majority-vote aggregation: direction with most votes wins."""
        votes: Dict[str, int] = defaultdict(int)
        for sig in signals:
            votes[sig.direction] += 1

        total = len(signals)
        majority_dir = max(votes, key=votes.get)  # type: ignore[arg-type]
        majority_count = votes[majority_dir]
        agreement_ratio = majority_count / total

        # If no clear majority (< 50%), abstain
        if agreement_ratio < 0.5:
            majority_dir = "neutral"
            agreement_ratio = 0.0

        # Confidence = average confidence of signals in majority direction
        majority_signals = [s for s in signals if s.direction == majority_dir]
        if majority_signals:
            avg_conf = statistics.mean(s.confidence for s in majority_signals)
            avg_return = statistics.mean(s.expected_return for s in majority_signals)
        else:
            avg_conf = statistics.mean(s.confidence for s in signals)
            avg_return = statistics.mean(s.expected_return for s in signals)

        avg_hold = statistics.mean(s.holding_period_days for s in signals)
        strategies = sorted({s.strategy_name for s in signals})

        _, risk_score = self._compute_agreement(signals)

        return CompositeSignal(
            stock_code=stock_code,
            stock_name=stock_name,
            direction=majority_dir,
            composite_confidence=max(0.0, min(1.0, avg_conf * agreement_ratio)),
            contributing_strategies=strategies,
            strategy_agreement_ratio=agreement_ratio,
            expected_return=avg_return,
            risk_score=risk_score,
            composite_score=avg_conf if majority_dir == "long" else -avg_conf,
            avg_holding_period_days=avg_hold,
            signals=signals,
        )

    # ── helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _score_to_direction(score: float, threshold: float = 0.05) -> str:
        if score > threshold:
            return "long"
        elif score < -threshold:
            return "short"
        return "neutral"

    @staticmethod
    def _compute_agreement(signals: List[StrategySignal]) -> tuple[float, float]:
        """Return (agreement_ratio, risk_score) for a group of signals.

        agreement_ratio: fraction of signals agreeing with the majority.
        risk_score: 0 = full agreement, 1 = maximum disagreement.
        """
        if not signals:
            return 0.0, 1.0

        votes: Dict[str, int] = defaultdict(int)
        for sig in signals:
            votes[sig.direction] += 1

        total = len(signals)
        majority_count = max(votes.values())
        agreement = majority_count / total

        # Risk increases with disagreement and when confidence is spread widely
        confidences = [s.confidence for s in signals]
        conf_std = statistics.stdev(confidences) if len(confidences) > 1 else 0.0
        risk_score = (1.0 - agreement) * 0.7 + min(conf_std, 0.5) * 0.6

        return agreement, max(0.0, min(1.0, risk_score))

    def _decay_old_signals(
        self, signals: List[StrategySignal]
    ) -> List[StrategySignal]:
        """Apply time-based decay: signals past 50 % of their holding period
        get reduced confidence (in-place on copies)."""
        now = _now_cst()
        result: List[StrategySignal] = []

        for sig in signals:
            half_life = timedelta(days=sig.holding_period_days * 0.5)
            ts = sig.timestamp if sig.timestamp.tzinfo else sig.timestamp.replace(tzinfo=_CST)
            age = now - ts

            if age > half_life:
                # Decay factor: linearly reduce from 1.0 at half-life to 0.2
                # at expiry
                remaining_frac = max(
                    0.0,
                    1.0 - (age - half_life).total_seconds()
                    / half_life.total_seconds(),
                )
                decay = 0.2 + 0.8 * remaining_frac
                # Create a shallow copy with reduced confidence
                decayed = StrategySignal(
                    strategy_name=sig.strategy_name,
                    stock_code=sig.stock_code,
                    stock_name=sig.stock_name,
                    direction=sig.direction,
                    confidence=sig.confidence * decay,
                    expected_return=sig.expected_return,
                    holding_period_days=sig.holding_period_days,
                    reasoning=sig.reasoning,
                    metadata=sig.metadata,
                    timestamp=sig.timestamp,
                    expires_at=sig.expires_at,
                )
                result.append(decayed)
            else:
                result.append(sig)

        return result

    # ── filtering ──────────────────────────────────────────────────────

    def filter_by_confidence(
        self,
        composites: List[CompositeSignal],
        min_confidence: float = 0.5,
    ) -> List[CompositeSignal]:
        """Keep only composite signals above *min_confidence*."""
        return [c for c in composites if c.composite_confidence >= min_confidence]
