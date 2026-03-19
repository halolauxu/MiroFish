"""Build final market check objects."""

from __future__ import annotations

from typing import Any, Dict, List

from .crowding import assess_crowding
from .gap_risk import assess_gap_risk
from .liquidity import assess_liquidity
from .reaction import assess_reaction


def build_market_check(
    event: Dict[str, Any],
    reaction: Dict[str, Any],
    narratives: List[str],
    crowding_score: float,
) -> Dict[str, Any]:
    reaction_part = assess_reaction(reaction)
    liquidity_part = assess_liquidity(reaction)
    gap_risk_part = assess_gap_risk(reaction)
    crowding_part = assess_crowding(event, narratives, crowding_score)

    tradability_score = (
        reaction_part["reaction_score"] * 0.45
        + liquidity_part["liquidity_score"] * 0.25
        + (1.0 - crowding_part["crowding_score"]) * 0.20
        + (1.0 - gap_risk_part["gap_risk"]) * 0.10
    )
    tradable = tradability_score >= 0.35 and reaction_part["reaction_score"] > 0.15

    result = {
        "reaction": reaction,
        "reaction_score": reaction_part["reaction_score"],
        "liquidity_score": liquidity_part["liquidity_score"],
        "crowding_score": crowding_part["crowding_score"],
        "gap_risk": gap_risk_part["gap_risk"],
        "market_check_score": round(max(0.0, min(1.0, tradability_score)), 4),
        "tradable": tradable,
        "reaction_label": reaction_part["reaction_label"],
        "narrative_count": crowding_part["narrative_count"],
    }
    return result
