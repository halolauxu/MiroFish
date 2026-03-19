"""Signal ranking helpers."""

from __future__ import annotations


def compute_signal_score(
    trigger_score: float,
    propagation_score: float,
    debate_score: float,
    market_check_score: float,
    risk_penalty: float,
) -> float:
    """Compute a unified score for ranking signals."""
    score = (
        trigger_score * 0.25
        + propagation_score * 0.25
        + debate_score * 0.25
        + market_check_score * 0.25
        - risk_penalty
    )
    return round(max(0.0, min(1.0, score)), 4)
