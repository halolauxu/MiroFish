"""Narrative crowding estimation."""

from __future__ import annotations

from typing import Any, Dict, List


def estimate_crowding_score(
    event: Dict[str, Any],
    narratives: List[str],
) -> float:
    """Estimate theme crowding risk using event metadata and narrative breadth."""
    base = float(event.get("crowding_risk", 0.4))
    event_type = event.get("event_type") or event.get("type") or "other"
    if event_type in {"product_launch", "technology_breakthrough", "narrative_breakout"}:
        base += 0.1
    base += min(0.15, len(narratives) * 0.03)
    return round(max(0.0, min(1.0, base)), 4)
