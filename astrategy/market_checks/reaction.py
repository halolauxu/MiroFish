"""Reaction checks."""

from __future__ import annotations

from typing import Any, Dict


def assess_reaction(reaction: Dict[str, Any]) -> Dict[str, Any]:
    """Assess whether the market has already priced in an event."""
    reacted = bool(reaction.get("reacted", False))
    ret = abs(float(reaction.get("return_pct", 0.0)))
    score = max(0.0, min(1.0, 0.85 - ret * 8.0))
    if reacted:
        score = min(score, 0.35)
    return {
        "reaction_score": round(score, 4),
        "reacted": reacted,
        "reaction_label": "已反应" if reacted else "未反应",
    }
