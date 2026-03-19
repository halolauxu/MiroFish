"""
Event Master utilities for the strategy universe research pipeline.
"""

from .normalizer import legacy_event_to_master, master_to_legacy_event, normalize_events
from .registry import EventRegistry
from .schemas import EventMasterRecord, make_event_master_record

__all__ = [
    "EventMasterRecord",
    "EventRegistry",
    "legacy_event_to_master",
    "make_event_master_record",
    "master_to_legacy_event",
    "normalize_events",
]
