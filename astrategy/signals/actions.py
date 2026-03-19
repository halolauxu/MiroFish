"""Action mapping helpers."""

from __future__ import annotations


def resolve_action(direction: str, confidence: float, divergence: float, tradable: bool) -> str:
    """Map direction/confidence to an action label."""
    if not tradable:
        return "observe"
    if direction == "avoid":
        return "avoid"
    if direction == "long" and confidence >= 0.75 and divergence < 0.18:
        return "add_long"
    if direction == "long":
        return "open_long"
    return "observe"
