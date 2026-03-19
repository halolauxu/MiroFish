"""Crowding checks."""

from __future__ import annotations

from typing import Any, Dict, List


def assess_crowding(
    event: Dict[str, Any],
    narratives: List[str],
    crowding_score: float,
) -> Dict[str, Any]:
    """Return crowding diagnostics for a signal candidate."""
    event_type = event.get("event_type") or event.get("type") or "other"
    score = crowding_score
    if event_type in {"product_launch", "technology_breakthrough"}:
        score = min(1.0, score + 0.08)
    return {
        "crowding_score": round(max(0.0, min(1.0, score)), 4),
        "narrative_count": len(narratives),
    }
