"""Build lightweight narrative relation edges from events."""

from __future__ import annotations

from typing import Any, Dict, List


def build_narrative_relations(event: Dict[str, Any], narratives: List[str]) -> List[Dict[str, Any]]:
    """Create narrative relation edge dicts for future EventGraph/NarrativeGraph ingestion."""
    stock_code = event.get("stock_code", "")
    event_id = event.get("event_id", "")
    available_at = event.get("available_at", "") or event.get("event_date", "")
    relations: List[Dict[str, Any]] = []
    for narrative in narratives:
        relations.append({
            "source": event_id or stock_code,
            "target": narrative,
            "relation": "RELATES_TO_NARRATIVE",
            "valid_from": available_at,
            "weight": 1.0,
        })
    return relations
