"""Shared helpers for portfolio construction."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List


_DEPLOYABLE_ACTIONS = {"open_long", "add_long", "rotate_into"}
_DEFENSIVE_ACTIONS = {"avoid", "trim_long", "close_long", "rotate_out"}


def parse_narrative_tags(raw_tags: Any) -> List[str]:
    """Normalize narrative tags into a clean list."""
    if raw_tags is None:
        return []
    if isinstance(raw_tags, list):
        items = raw_tags
    elif isinstance(raw_tags, str):
        text = raw_tags.strip()
        if not text or text.lower() == "nan":
            return []
        splitter = "," if "," in text else "，"
        items = [part.strip() for part in text.split(splitter)]
    else:
        return []
    return [str(item).strip() for item in items if str(item).strip()]


def primary_theme(signal: Dict[str, Any]) -> str:
    """Return the primary theme bucket for a signal."""
    tags = parse_narrative_tags(signal.get("narrative_tags"))
    if tags:
        return tags[0]
    event_type = str(signal.get("event_type", "")).strip()
    return event_type or "其他"


def all_themes(signal: Dict[str, Any]) -> List[str]:
    """Return all theme tags for a signal, falling back to event type."""
    tags = parse_narrative_tags(signal.get("narrative_tags"))
    return tags or [str(signal.get("event_type", "")).strip() or "其他"]


def alpha_family(signal: Dict[str, Any]) -> str:
    """Normalize alpha-family label."""
    family = str(signal.get("alpha_family", "")).strip()
    return family or "unknown"


def source_cluster(signal: Dict[str, Any]) -> str:
    """Return a lightweight cluster key for source-level concentration."""
    source_code = str(signal.get("source_code", "")).strip()
    if source_code:
        return source_code
    return str(signal.get("event_id", "")).strip() or "unknown"


def is_deployable_action(signal: Dict[str, Any]) -> bool:
    """Whether a signal belongs to the active long book."""
    return str(signal.get("action", "")).strip() in _DEPLOYABLE_ACTIONS


def is_defensive_action(signal: Dict[str, Any]) -> bool:
    """Whether a signal belongs to the defensive/rotation bucket."""
    return str(signal.get("action", "")).strip() in _DEFENSIVE_ACTIONS


def top_items(weight_map: Dict[str, float], limit: int = 3) -> List[Dict[str, Any]]:
    """Convert a weight map into a sorted summary list."""
    ranked = sorted(weight_map.items(), key=lambda item: item[1], reverse=True)
    return [
        {"name": key, "weight": round(value, 4)}
        for key, value in ranked[:limit]
        if value > 0
    ]
