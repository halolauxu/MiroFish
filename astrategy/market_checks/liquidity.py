"""Liquidity checks."""

from __future__ import annotations

from typing import Any, Dict


def assess_liquidity(reaction: Dict[str, Any]) -> Dict[str, Any]:
    """Crude liquidity assessment based on available price/volume hints."""
    entry_price = float(reaction.get("entry_price", 0.0) or 0.0)
    volume_change = float(reaction.get("volume_change_pct", 0.0) or 0.0)
    score = 0.6
    if entry_price <= 0:
        score -= 0.15
    if volume_change < -0.5:
        score -= 0.2
    elif volume_change > 0.5:
        score += 0.1
    return {
        "liquidity_score": round(max(0.0, min(1.0, score)), 4),
        "entry_price_known": entry_price > 0,
    }
