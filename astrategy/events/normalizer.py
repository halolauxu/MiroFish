"""
Normalization helpers between legacy historical events and Event Master.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List

from .schemas import EventMasterRecord

_HIGH_IMPACT_TYPES = {
    "earnings_surprise",
    "policy_support",
    "policy_risk",
    "supply_shortage",
    "supply_disruption",
    "technology_breakthrough",
    "product_launch",
    "scandal",
    "order_win",
}

_NEGATIVE_TYPES = {
    "scandal",
    "policy_risk",
    "management_change",
    "sentiment_reversal",
}

_SOURCE_TYPE_MAP = {
    "公开新闻": "news",
    "东方财富新闻(akshare)": "news",
}


def _to_iso_datetime(date_str: str) -> str:
    if not date_str:
        return ""
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.isoformat()
        except ValueError:
            continue
    return date_str


def _impact_to_severity(impact_level: str) -> float:
    mapping = {"low": 0.3, "medium": 0.55, "high": 0.8, "critical": 0.95}
    return mapping.get(str(impact_level or "").lower(), 0.5)


def legacy_event_to_master(event: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a legacy historical event record into Event Master schema."""
    event_type = event.get("event_type") or event.get("type") or "other"
    event_date = event.get("event_time") or event.get("event_date") or ""
    impact_level = event.get("impact_level", "medium")
    stock_code = event.get("stock_code", "")
    stock_name = event.get("stock_name", "")

    event_time_iso = _to_iso_datetime(event_date)
    discover_time = event.get("discover_time") or event_time_iso
    available_at = event.get("available_at") or discover_time or event_time_iso

    severity = event.get("severity")
    if severity is None:
        severity = _impact_to_severity(impact_level)

    surprise_score = event.get("surprise_score")
    if surprise_score is None:
        surprise_score = 0.75 if event_type in _HIGH_IMPACT_TYPES else 0.45

    tradability_score = event.get("tradability_score")
    if tradability_score is None:
        tradability_score = 0.7 if stock_code else 0.3

    novelty_score = event.get("novelty_score")
    if novelty_score is None:
        novelty_score = 0.6

    crowding_risk = event.get("crowding_risk")
    if crowding_risk is None:
        crowding_risk = 0.35 if event_type in _NEGATIVE_TYPES else 0.45

    confidence = event.get("confidence")
    if confidence is None:
        confidence = 0.7 if impact_level == "high" else 0.55

    record = EventMasterRecord(
        event_id=event.get("event_id", ""),
        event_type=event_type,
        event_subtype=event.get("event_subtype", event_type),
        title=event.get("title", ""),
        summary=event.get("summary", ""),
        source=event.get("source", "unknown"),
        entity_codes=[stock_code] if stock_code else [],
        entity_names=[stock_name] if stock_name else [],
        industry_codes=list(event.get("industry_codes", [])),
        theme_tags=list(event.get("theme_tags", [])),
        event_time=event_time_iso,
        discover_time=discover_time,
        available_at=available_at,
        severity=float(severity),
        surprise_score=float(surprise_score),
        tradability_score=float(tradability_score),
        novelty_score=float(novelty_score),
        crowding_risk=float(crowding_risk),
        confidence=float(confidence),
        impact_level=impact_level,
        source_type=event.get("source_type", _SOURCE_TYPE_MAP.get(event.get("source", ""), "news")),
        raw_payload_ref=event.get("raw_payload_ref", ""),
        metadata={
            "legacy_type": event.get("type", ""),
            "legacy_event_date": event.get("event_date", ""),
            "legacy_stock_code": stock_code,
            "legacy_stock_name": stock_name,
            **dict(event.get("metadata", {})),
        },
    )
    return record.to_dict()


def master_to_legacy_event(record: Dict[str, Any]) -> Dict[str, Any]:
    """Convert an Event Master record into the historical event shape."""
    entity_codes = list(record.get("entity_codes", []))
    entity_names = list(record.get("entity_names", []))
    event_time = record.get("event_time", "")
    event_date = event_time[:10] if isinstance(event_time, str) and len(event_time) >= 10 else event_time
    return {
        "event_id": record.get("event_id", ""),
        "title": record.get("title", ""),
        "type": record.get("event_type", record.get("event_subtype", "other")),
        "stock_code": entity_codes[0] if entity_codes else "",
        "stock_name": entity_names[0] if entity_names else "",
        "event_date": event_date,
        "summary": record.get("summary", ""),
        "impact_level": record.get("impact_level", "medium"),
        "source": record.get("source", "unknown"),
        "event_type": record.get("event_type", record.get("event_subtype", "other")),
        "event_subtype": record.get("event_subtype", record.get("event_type", "other")),
        "discover_time": record.get("discover_time", ""),
        "available_at": record.get("available_at", ""),
        "severity": record.get("severity", 0.0),
        "surprise_score": record.get("surprise_score", 0.0),
        "tradability_score": record.get("tradability_score", 0.0),
        "novelty_score": record.get("novelty_score", 0.0),
        "crowding_risk": record.get("crowding_risk", 0.0),
        "confidence": record.get("confidence", 0.0),
        "theme_tags": list(record.get("theme_tags", [])),
        "industry_codes": list(record.get("industry_codes", [])),
        "source_type": record.get("source_type", "news"),
        "raw_payload_ref": record.get("raw_payload_ref", ""),
        "metadata": dict(record.get("metadata", {})),
    }


def normalize_events(events: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize a sequence of mixed event records into legacy-compatible records."""
    normalized: List[Dict[str, Any]] = []
    for event in events:
        if "entity_codes" in event or "entity_names" in event or (
            "event_time" in event and "stock_code" not in event
        ):
            normalized.append(master_to_legacy_event(event))
        else:
            normalized.append(master_to_legacy_event(legacy_event_to_master(event)))
    return normalized
