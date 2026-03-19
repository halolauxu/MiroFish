"""
Schemas for the unified Event Master layer.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

_UTC = timezone.utc


def _iso_now() -> str:
    return datetime.now(tz=_UTC).isoformat()


@dataclass
class EventMasterRecord:
    """Unified event schema used by Event Master."""

    event_id: str
    event_type: str
    event_subtype: str
    title: str
    summary: str
    source: str
    entity_codes: List[str]
    entity_names: List[str] = field(default_factory=list)
    industry_codes: List[str] = field(default_factory=list)
    theme_tags: List[str] = field(default_factory=list)
    event_time: str = ""
    discover_time: str = ""
    available_at: str = ""
    severity: float = 0.0
    surprise_score: float = 0.0
    tradability_score: float = 0.0
    novelty_score: float = 0.0
    crowding_risk: float = 0.0
    confidence: float = 0.0
    impact_level: str = "medium"
    source_type: str = "news"
    raw_payload_ref: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: str = field(default_factory=_iso_now)
    updated_at: str = field(default_factory=_iso_now)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def make_event_master_record(**kwargs: Any) -> EventMasterRecord:
    """Convenience constructor with dataclass validation."""
    return EventMasterRecord(**kwargs)
