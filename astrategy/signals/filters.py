"""Signal emission filters."""

from __future__ import annotations


def should_emit_signal(score: float, confidence: float, tradable: bool) -> bool:
    """Whether a candidate signal should be emitted."""
    return tradable and score >= 0.25 and confidence >= 0.25
