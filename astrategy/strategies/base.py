"""
Strategy base classes and signal data model.

Every concrete strategy inherits from ``BaseStrategy`` and produces
``StrategySignal`` instances.  The ``SignalAggregator`` merges signals
from multiple strategies and exposes consensus rankings.
"""

from __future__ import annotations

import json
import statistics
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Signal data model
# ---------------------------------------------------------------------------

_CST = timezone(timedelta(hours=8))  # China Standard Time


def _now_cst() -> datetime:
    return datetime.now(tz=_CST)


@dataclass
class StrategySignal:
    """A single directional signal emitted by a strategy for one stock."""

    strategy_name: str
    stock_code: str
    stock_name: str
    direction: str  # "long", "avoid", or "neutral"
    confidence: float  # 0.0 – 1.0
    expected_return: float  # e.g. 0.05 = +5 %
    holding_period_days: int
    reasoning: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=_now_cst)
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        # Clamp confidence
        self.confidence = max(0.0, min(1.0, self.confidence))
        # Validate direction — A股无做空，用 avoid(回避) 替代 short
        # 兼容旧代码：自动将 "short" 映射为 "avoid"
        if self.direction == "short":
            self.direction = "avoid"
        if self.direction not in ("long", "avoid", "neutral"):
            raise ValueError(
                f"direction must be 'long', 'avoid', or 'neutral', got '{self.direction}'"
            )
        # Auto-compute expiry if not provided
        if self.expires_at is None:
            ts = self.timestamp if self.timestamp.tzinfo else self.timestamp.replace(tzinfo=_CST)
            self.expires_at = ts + timedelta(days=self.holding_period_days)

    @property
    def is_expired(self) -> bool:
        return _now_cst() > self.expires_at  # type: ignore[operator]

    @property
    def signed_score(self) -> float:
        """Directional score: positive for long, negative for avoid, zero for neutral."""
        multiplier = {"long": 1.0, "avoid": -1.0, "neutral": 0.0}
        return self.confidence * multiplier[self.direction]

    # ── serialisation helpers ──────────────────────────────────────────
    def to_dict(self) -> dict:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        d["expires_at"] = self.expires_at.isoformat() if self.expires_at else None
        return d

    @classmethod
    def from_dict(cls, data: dict) -> StrategySignal:
        data = dict(data)  # shallow copy
        if isinstance(data.get("timestamp"), str):
            data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        if isinstance(data.get("expires_at"), str):
            data["expires_at"] = datetime.fromisoformat(data["expires_at"])
        return cls(**data)


# ---------------------------------------------------------------------------
# Strategy base class
# ---------------------------------------------------------------------------


class BaseStrategy(ABC):
    """Abstract base that every concrete strategy must extend."""

    def __init__(self, signal_dir: Path | str | None = None) -> None:
        if signal_dir is not None:
            self._signal_dir = Path(signal_dir)
        else:
            # Import here to avoid circular dependency at module-load time.
            from astrategy.config import settings
            self._signal_dir = settings.storage.signal_dir

    # ── identity ───────────────────────────────────────────────────────
    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name for this strategy (snake_case by convention)."""
        ...

    # ── execution ──────────────────────────────────────────────────────
    @abstractmethod
    def run(self, stock_codes: list[str] | None = None) -> list[StrategySignal]:
        """Run the strategy across an optional universe of stocks.

        If *stock_codes* is ``None`` the strategy should use its own
        default universe (e.g. the full CSI-300).
        """
        ...

    @abstractmethod
    def run_single(self, stock_code: str) -> list[StrategySignal]:
        """Run the strategy for a single stock and return its signals."""
        ...

    # ── persistence ────────────────────────────────────────────────────
    def save_signals(self, signals: list[StrategySignal], date: str | None = None) -> Path:
        """Persist *signals* as a JSON file and return the written path.

        Parameters
        ----------
        signals:
            List of signals to save.
        date:
            Date label for the output file (``YYYYMMDD``).  Defaults to
            today (CST).
        """
        if date is None:
            date = _now_cst().strftime("%Y%m%d")

        out_dir = self._signal_dir / self.name
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{date}.json"

        payload = [sig.to_dict() for sig in signals]
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return out_path

    @staticmethod
    def load_signals(strategy_name: str, date: str, signal_dir: Path | str | None = None) -> list[StrategySignal]:
        """Load previously-saved signals from disk.

        Parameters
        ----------
        strategy_name:
            Strategy name (matches the sub-directory).
        date:
            Date label (``YYYYMMDD``).
        signal_dir:
            Root signal directory.  Defaults to ``settings.storage.signal_dir``.
        """
        if signal_dir is None:
            from astrategy.config import settings
            signal_dir = settings.storage.signal_dir
        else:
            signal_dir = Path(signal_dir)

        path = signal_dir / strategy_name / f"{date}.json"
        if not path.exists():
            return []

        raw = json.loads(path.read_text(encoding="utf-8"))
        return [StrategySignal.from_dict(item) for item in raw]


# ---------------------------------------------------------------------------
# Signal aggregator
# ---------------------------------------------------------------------------


@dataclass
class CompositeSignal:
    """Consensus signal produced by aggregating multiple strategy signals."""

    stock_code: str
    stock_name: str
    direction: str  # majority-voted direction
    composite_score: float  # weighted average of signed_score
    confidence: float  # average confidence
    strategy_count: int
    strategies: list[str]
    avg_expected_return: float
    avg_holding_period_days: float
    signals: list[StrategySignal] = field(default_factory=list, repr=False)


class SignalAggregator:
    """Collects signals from multiple strategies and derives consensus views."""

    def __init__(self) -> None:
        self._signals: list[StrategySignal] = []

    # ── ingestion ──────────────────────────────────────────────────────
    def add_signals(self, signals: list[StrategySignal]) -> None:
        self._signals.extend(signals)

    def clear(self) -> None:
        self._signals.clear()

    @property
    def all_signals(self) -> list[StrategySignal]:
        return list(self._signals)

    # ── grouping ───────────────────────────────────────────────────────
    def aggregate_by_stock(self) -> dict[str, list[StrategySignal]]:
        """Group all collected signals by ``stock_code``."""
        result: dict[str, list[StrategySignal]] = {}
        for sig in self._signals:
            result.setdefault(sig.stock_code, []).append(sig)
        return result

    # ── consensus ──────────────────────────────────────────────────────
    def get_consensus(self, stock_code: str) -> CompositeSignal | None:
        """Build a composite signal for *stock_code* using multi-strategy voting.

        The algorithm:
        1. Filter non-expired signals for the given stock.
        2. Compute the composite score as the mean of each signal's
           ``signed_score`` (confidence * direction multiplier).
        3. The consensus direction is determined by the sign of the
           composite score (positive -> long, negative -> short, zero ->
           neutral).
        4. Confidence is the mean of individual confidences.
        """
        stock_signals = [
            s for s in self._signals
            if s.stock_code == stock_code and not s.is_expired
        ]
        if not stock_signals:
            return None

        signed_scores = [s.signed_score for s in stock_signals]
        composite_score = statistics.mean(signed_scores)
        avg_confidence = statistics.mean(s.confidence for s in stock_signals)
        avg_return = statistics.mean(s.expected_return for s in stock_signals)
        avg_hold = statistics.mean(s.holding_period_days for s in stock_signals)

        if composite_score > 0.05:
            direction = "long"
        elif composite_score < -0.05:
            direction = "avoid"
        else:
            direction = "neutral"

        strategies = sorted({s.strategy_name for s in stock_signals})
        stock_name = stock_signals[0].stock_name

        return CompositeSignal(
            stock_code=stock_code,
            stock_name=stock_name,
            direction=direction,
            composite_score=composite_score,
            confidence=avg_confidence,
            strategy_count=len(stock_signals),
            strategies=strategies,
            avg_expected_return=avg_return,
            avg_holding_period_days=avg_hold,
            signals=stock_signals,
        )

    # ── ranking ────────────────────────────────────────────────────────
    def rank_stocks(self, top_n: int = 20) -> list[CompositeSignal]:
        """Return the top *top_n* stocks ranked by absolute composite score.

        Stocks with a stronger directional conviction (either long or
        short) appear first.  Only non-expired signals are considered.
        """
        by_stock = self.aggregate_by_stock()
        composites: list[CompositeSignal] = []

        for code in by_stock:
            cs = self.get_consensus(code)
            if cs is not None:
                composites.append(cs)

        # Sort by absolute composite score descending, then by confidence
        composites.sort(key=lambda c: (abs(c.composite_score), c.confidence), reverse=True)
        return composites[:top_n]
