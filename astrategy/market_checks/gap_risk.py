"""Gap risk checks."""

from __future__ import annotations

from typing import Any, Dict


def assess_gap_risk(reaction: Dict[str, Any]) -> Dict[str, Any]:
    """Estimate overnight/gap risk from observed magnitude."""
    ret = abs(float(reaction.get("return_pct", 0.0) or 0.0))
    risk = min(1.0, ret * 10.0)
    return {
        "gap_risk": round(risk, 4),
    }
