"""Narrative extraction from event text."""

from __future__ import annotations

from typing import Any, Dict, List

from .taxonomy import NARRATIVE_TAXONOMY


def extract_narratives(event: Dict[str, Any]) -> List[str]:
    """Extract narrative tags from title/summary/theme tags."""
    title = str(event.get("title", ""))
    summary = str(event.get("summary", ""))
    theme_tags = list(event.get("theme_tags", []))
    text = f"{title} {summary} {' '.join(theme_tags)}"

    matched: List[str] = []
    for narrative, keywords in NARRATIVE_TAXONOMY.items():
        if any(keyword in text for keyword in keywords):
            matched.append(narrative)
    return matched
