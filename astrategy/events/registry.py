"""
Storage helpers for Event Master datasets.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from .normalizer import legacy_event_to_master, master_to_legacy_event


class EventRegistry:
    """Persists normalized event datasets under ``astrategy/.data/event_master``."""

    def __init__(self, data_dir: str | Path | None = None) -> None:
        base = Path(data_dir) if data_dir is not None else (
            Path(__file__).resolve().parent.parent / ".data" / "event_master"
        )
        self.data_dir = base
        self.data_dir.mkdir(parents=True, exist_ok=True)

    @property
    def master_path(self) -> Path:
        return self.data_dir / "historical_event_master.json"

    def load_master(self, path: str | Path | None = None) -> List[Dict[str, Any]]:
        target = Path(path) if path is not None else self.master_path
        if not target.exists():
            return []
        return json.loads(target.read_text(encoding="utf-8"))

    def save_master(
        self,
        records: Iterable[Dict[str, Any]],
        path: str | Path | None = None,
    ) -> Path:
        target = Path(path) if path is not None else self.master_path
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = list(records)
        target.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return target

    def load_legacy_view(self, path: str | Path | None = None) -> List[Dict[str, Any]]:
        return [master_to_legacy_event(r) for r in self.load_master(path)]

    def convert_legacy_events(
        self,
        events: Iterable[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        return [legacy_event_to_master(event) for event in events]
