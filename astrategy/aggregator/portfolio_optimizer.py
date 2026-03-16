"""
Portfolio Optimizer for position sizing and allocation.

Takes composite signals and produces a portfolio with position weights
subject to risk constraints (max single position, sector concentration).
"""

from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from astrategy.aggregator.signal_aggregator import CompositeSignal

logger = logging.getLogger("astrategy.aggregator.portfolio_optimizer")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class PositionSizing:
    """A single position recommendation within a portfolio."""

    stock_code: str
    stock_name: str
    direction: str  # "long" or "short"
    weight: float  # portfolio weight, 0.0 – 1.0
    confidence: float
    risk_contribution: float
    expected_return: float = 0.0
    sector: str = ""

    def to_dict(self) -> dict:
        return {
            "stock_code": self.stock_code,
            "stock_name": self.stock_name,
            "direction": self.direction,
            "weight": round(self.weight, 6),
            "confidence": round(self.confidence, 4),
            "risk_contribution": round(self.risk_contribution, 4),
            "expected_return": round(self.expected_return, 4),
            "sector": self.sector,
        }


@dataclass
class PortfolioSummary:
    """High-level portfolio metrics."""

    positions: List[PositionSizing]
    total_weight: float
    num_positions: int
    expected_return: float
    risk_score: float
    long_weight: float
    short_weight: float

    def to_dict(self) -> dict:
        return {
            "positions": [p.to_dict() for p in self.positions],
            "total_weight": round(self.total_weight, 4),
            "num_positions": self.num_positions,
            "expected_return": round(self.expected_return, 4),
            "risk_score": round(self.risk_score, 4),
            "long_weight": round(self.long_weight, 4),
            "short_weight": round(self.short_weight, 4),
        }


# ---------------------------------------------------------------------------
# Sector mapping helper
# ---------------------------------------------------------------------------

# Simplified sector mapping based on stock code prefix (A-share conventions).
# In production this would come from a data provider.
_SECTOR_PREFIXES: Dict[str, str] = {
    "600": "main_board_sh",
    "601": "main_board_sh",
    "603": "main_board_sh",
    "605": "main_board_sh",
    "000": "main_board_sz",
    "001": "main_board_sz",
    "002": "sme_board",
    "003": "sme_board",
    "300": "chinext",
    "301": "chinext",
    "688": "star_market",
    "689": "star_market",
}


def _infer_sector(stock_code: str) -> str:
    """Infer a coarse sector/board from the stock code prefix."""
    code = stock_code.split(".")[0]
    for prefix, sector in _SECTOR_PREFIXES.items():
        if code.startswith(prefix):
            return sector
    return "other"


# ---------------------------------------------------------------------------
# Portfolio Optimizer
# ---------------------------------------------------------------------------

class PortfolioOptimizer:
    """Optimize position sizing from composite signals.

    Supports equal-weight, confidence-weighted, and risk-parity allocation.
    Enforces constraints on maximum single-position size and sector
    concentration.
    """

    def __init__(
        self,
        max_single_weight: float = 0.10,
        max_sector_weight: float = 0.30,
        min_confidence: float = 0.3,
    ) -> None:
        self.max_single_weight = max_single_weight
        self.max_sector_weight = max_sector_weight
        self.min_confidence = min_confidence

    # ── main entry point ──────────────────────────────────────────────

    def optimize(
        self,
        signals: List[CompositeSignal],
        budget: float = 1.0,
        max_positions: int = 20,
        method: str = "confidence_weighted",
        volatilities: Optional[Dict[str, float]] = None,
        sector_map: Optional[Dict[str, str]] = None,
    ) -> PortfolioSummary:
        """Produce a sized portfolio from composite signals.

        Parameters
        ----------
        signals:
            Ranked composite signals (from SignalAggregator).
        budget:
            Total weight budget (1.0 = 100 % invested).
        max_positions:
            Maximum number of positions.
        method:
            ``'equal_weight'``, ``'confidence_weighted'``, or
            ``'risk_parity'``.
        volatilities:
            Optional ``{stock_code: annualised_vol}`` for risk-parity.
            If not supplied and method is ``'risk_parity'``, falls back
            to ``'confidence_weighted'``.
        sector_map:
            Optional ``{stock_code: sector_name}``.  If not supplied,
            sectors are inferred from stock code prefixes.
        """
        # Filter by confidence and direction
        eligible = [
            s for s in signals
            if s.composite_confidence >= self.min_confidence
            and s.direction in ("long", "short")
        ]

        if not eligible:
            return PortfolioSummary(
                positions=[], total_weight=0.0, num_positions=0,
                expected_return=0.0, risk_score=0.0,
                long_weight=0.0, short_weight=0.0,
            )

        # Trim to max_positions
        eligible = eligible[:max_positions]

        # Compute raw weights
        if method == "equal_weight":
            raw = self._equal_weight(eligible)
        elif method == "confidence_weighted":
            raw = self._confidence_weighted(eligible)
        elif method == "risk_parity":
            if volatilities:
                raw = self._risk_parity(eligible, volatilities)
            else:
                logger.warning(
                    "No volatilities supplied for risk_parity; "
                    "falling back to confidence_weighted."
                )
                raw = self._confidence_weighted(eligible)
        else:
            raise ValueError(f"Unknown optimization method: {method}")

        # Build position objects with sectors
        _sec_map = sector_map or {}
        positions: List[PositionSizing] = []
        for sig, w in zip(eligible, raw):
            sector = _sec_map.get(sig.stock_code, _infer_sector(sig.stock_code))
            positions.append(PositionSizing(
                stock_code=sig.stock_code,
                stock_name=sig.stock_name,
                direction=sig.direction,
                weight=w,
                confidence=sig.composite_confidence,
                risk_contribution=sig.risk_score * w,
                expected_return=sig.expected_return,
                sector=sector,
            ))

        # Apply constraints
        positions = self._apply_constraints(positions, budget)

        # Build summary
        return self._build_summary(positions)

    # ── allocation methods ────────────────────────────────────────────

    @staticmethod
    def _equal_weight(signals: List[CompositeSignal]) -> List[float]:
        n = len(signals)
        return [1.0 / n] * n

    @staticmethod
    def _confidence_weighted(signals: List[CompositeSignal]) -> List[float]:
        total_conf = sum(s.composite_confidence for s in signals)
        if total_conf == 0:
            return [1.0 / len(signals)] * len(signals)
        return [s.composite_confidence / total_conf for s in signals]

    @staticmethod
    def _risk_parity(
        signals: List[CompositeSignal],
        volatilities: Dict[str, float],
    ) -> List[float]:
        """Weight inversely proportional to volatility."""
        inv_vols: List[float] = []
        for sig in signals:
            vol = volatilities.get(sig.stock_code, 0.3)  # default 30 %
            inv_vols.append(1.0 / max(vol, 0.01))

        total = sum(inv_vols)
        if total == 0:
            return [1.0 / len(signals)] * len(signals)
        return [iv / total for iv in inv_vols]

    # ── constraint enforcement ────────────────────────────────────────

    def _apply_constraints(
        self,
        positions: List[PositionSizing],
        budget: float,
    ) -> List[PositionSizing]:
        """Enforce max single position and sector concentration limits."""
        # Pass 1: cap individual positions
        for pos in positions:
            if pos.weight > self.max_single_weight:
                pos.weight = self.max_single_weight

        # Pass 2: cap sector concentration
        sector_weights: Dict[str, float] = defaultdict(float)
        for pos in positions:
            sector_weights[pos.sector] += pos.weight

        for sector, sw in sector_weights.items():
            if sw > self.max_sector_weight:
                scale = self.max_sector_weight / sw
                for pos in positions:
                    if pos.sector == sector:
                        pos.weight *= scale

        # Pass 3: normalise to budget
        total = sum(p.weight for p in positions)
        if total > 0:
            scale = budget / total
            for pos in positions:
                pos.weight *= scale

        # Re-cap after normalisation (iterative)
        for _ in range(5):
            capped = False
            for pos in positions:
                if pos.weight > self.max_single_weight:
                    excess = pos.weight - self.max_single_weight
                    pos.weight = self.max_single_weight
                    # Redistribute excess proportionally
                    others = [p for p in positions if p is not pos]
                    other_total = sum(p.weight for p in others)
                    if other_total > 0:
                        for p in others:
                            p.weight += excess * (p.weight / other_total)
                    capped = True
            if not capped:
                break

        # Recalculate risk contribution
        for pos in positions:
            pos.risk_contribution = pos.confidence * pos.weight

        return positions

    # ── summary ───────────────────────────────────────────────────────

    @staticmethod
    def _build_summary(positions: List[PositionSizing]) -> PortfolioSummary:
        if not positions:
            return PortfolioSummary(
                positions=[], total_weight=0.0, num_positions=0,
                expected_return=0.0, risk_score=0.0,
                long_weight=0.0, short_weight=0.0,
            )

        total_weight = sum(p.weight for p in positions)
        long_weight = sum(p.weight for p in positions if p.direction == "long")
        short_weight = sum(p.weight for p in positions if p.direction == "short")

        # Weighted expected return
        expected_return = sum(
            p.weight * p.expected_return for p in positions
        ) / max(total_weight, 1e-9)

        # Portfolio risk: weighted average of risk contributions
        risk_score = sum(p.risk_contribution for p in positions) / max(total_weight, 1e-9)

        return PortfolioSummary(
            positions=positions,
            total_weight=total_weight,
            num_positions=len(positions),
            expected_return=expected_return,
            risk_score=max(0.0, min(1.0, risk_score)),
            long_weight=long_weight,
            short_weight=short_weight,
        )
