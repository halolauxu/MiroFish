"""Risk budget rules."""

from __future__ import annotations

from typing import Any, Dict

from .common import alpha_family, is_defensive_action


_FAMILY_MULTIPLIERS = {
    "info_gap": 1.00,
    "continuation": 0.82,
    "source": 0.88,
}

_HOP_MULTIPLIERS = {
    0: 0.86,
    1: 1.00,
    2: 1.08,
    3: 1.16,
}


def compute_position_budget(signal: Dict[str, Any]) -> float:
    """Compute target position budget from signal quality and risk role."""
    confidence = float(signal.get("confidence", 0.0))
    score = float(signal.get("score", 0.0))
    market_check_score = float(signal.get("market_check_score", 0.0))
    graph_rank_score = float(signal.get("graph_rank_score", 0.0))
    risk_penalty = float(signal.get("risk_penalty", 0.0))
    hop = min(int(signal.get("hop", 0)), 3)

    base_weight = (
        0.02
        + score * 0.09
        + confidence * 0.05
        + market_check_score * 0.03
        + min(graph_rank_score, 1.0) * 0.02
    )

    family_mult = _FAMILY_MULTIPLIERS.get(alpha_family(signal), 0.78)
    hop_mult = _HOP_MULTIPLIERS.get(hop, 1.0)
    action_mult = 0.58 if is_defensive_action(signal) else 1.0
    reacted_mult = 0.90 if bool(signal.get("reacted_continuation", False)) else 1.0
    risk_mult = max(0.35, 1.0 - risk_penalty * 0.75)

    weight = base_weight * family_mult * hop_mult * action_mult * reacted_mult * risk_mult
    return round(max(0.0, min(0.14, weight)), 4)
