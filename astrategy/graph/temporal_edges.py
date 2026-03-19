"""
Temporal helpers for local graph snapshots.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List


def normalize_temporal_value(value: str | None) -> str:
    """Normalize date/datetime strings for coarse chronological comparison."""
    if not value:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    if len(text) >= 10:
        return text[:10]
    return text


def is_record_active_as_of(record: Dict[str, Any], as_of: str | None) -> bool:
    """Check whether a node/edge/fact is active at the given date."""
    if not as_of:
        return True

    as_of_norm = normalize_temporal_value(as_of)
    if not as_of_norm:
        return True

    created_at = normalize_temporal_value(
        record.get("created_at") or record.get("valid_from") or record.get("date")
    )
    valid_from = normalize_temporal_value(record.get("valid_from"))
    valid_to = normalize_temporal_value(record.get("valid_to"))

    start = valid_from or created_at
    if start and start > as_of_norm:
        return False
    if valid_to and valid_to < as_of_norm:
        return False
    return True


def filter_records_as_of(records: List[Dict[str, Any]], as_of: str | None) -> List[Dict[str, Any]]:
    """Filter records based on their temporal validity."""
    if not as_of:
        return list(records)
    return [record for record in records if is_record_active_as_of(record, as_of)]
