"""Simple narrative lifecycle inference."""

from __future__ import annotations

from typing import Any, Dict, List


def infer_narrative_phase(
    event: Dict[str, Any],
    narratives: List[str],
    crowding_score: float,
) -> str:
    """Infer a coarse narrative phase."""
    if not narratives:
        return "无叙事"
    event_type = event.get("event_type") or event.get("type") or "other"
    if event_type in {"technology_breakthrough", "order_win", "policy_support"} and crowding_score < 0.45:
        return "萌芽期"
    if crowding_score < 0.6:
        return "扩散期"
    if crowding_score < 0.8:
        return "成熟期"
    return "衰退期"
