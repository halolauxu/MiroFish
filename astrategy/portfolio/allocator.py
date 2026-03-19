"""Portfolio allocator for unified signals."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List

from .common import (
    alpha_family,
    is_defensive_action,
    is_deployable_action,
    primary_theme,
    top_items,
)
from .constraints import apply_constraints
from .risk_budget import compute_position_budget
from .rotation import build_rotation_hints

_LONG_BOOK_CAP = 1.02
_POSITION_CAP = 0.16


def _safe_weight(signal: Dict[str, Any]) -> float:
    return float(signal.get("target_weight", 0.0))


def _share(positions: List[Dict[str, Any]], family: str) -> float:
    total_weight = sum(_safe_weight(item) for item in positions)
    if total_weight <= 1e-9:
        return 0.0
    family_weight = sum(
        _safe_weight(item)
        for item in positions
        if alpha_family(item) == family
    )
    return family_weight / total_weight


def _weighted_hop(positions: List[Dict[str, Any]]) -> float:
    total_weight = sum(_safe_weight(item) for item in positions)
    if total_weight <= 1e-9:
        return 0.0
    return sum(
        float(item.get("hop", 0.0)) * _safe_weight(item)
        for item in positions
    ) / total_weight


def _theme_weights(positions: List[Dict[str, Any]]) -> Dict[str, float]:
    theme_weights: Dict[str, float] = defaultdict(float)
    for item in positions:
        theme_weights[primary_theme(item)] += _safe_weight(item)
    return dict(theme_weights)


def _compute_dynamic_scale(
    positions: List[Dict[str, Any]],
    defensive_positions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Estimate dynamic book size from opportunity quality and crowding."""
    if not positions:
        return {
            "scale": 0.0,
            "avg_hop": 0.0,
            "info_gap_share": 0.0,
            "source_share": 0.0,
            "continuation_share": 0.0,
            "crowded_theme": "",
            "crowded_theme_ratio": 0.0,
        }

    theme_weights = _theme_weights(positions)
    crowded_theme = max(theme_weights, key=theme_weights.get)
    total_weight = sum(theme_weights.values())
    crowded_theme_ratio = (
        theme_weights.get(crowded_theme, 0.0) / max(total_weight, 1e-9)
    )

    avg_hop = _weighted_hop(positions)
    info_gap_share = _share(positions, "info_gap")
    source_share = _share(positions, "source")
    continuation_share = _share(positions, "continuation")
    defensive_weight = sum(_safe_weight(item) for item in defensive_positions)

    scale = 1.0
    scale += 0.18 * min(avg_hop, 2.5) / 2.5
    scale += 0.10 if len(positions) >= 2 else -0.05
    scale += 0.10 * min(defensive_weight, 0.12) / 0.12
    scale += 0.06 * min(info_gap_share, 0.5) / 0.5
    scale -= 0.14 * min(source_share, 0.8) / 0.8
    scale -= 0.08 * min(continuation_share, 0.7) / 0.7
    scale -= 0.08 * max(0.0, crowded_theme_ratio - 0.45) / 0.35
    scale = max(0.72, min(1.38, scale))

    return {
        "scale": round(scale, 4),
        "avg_hop": round(avg_hop, 4),
        "info_gap_share": round(info_gap_share, 4),
        "source_share": round(source_share, 4),
        "continuation_share": round(continuation_share, 4),
        "crowded_theme": crowded_theme,
        "crowded_theme_ratio": round(crowded_theme_ratio, 4),
    }


def _apply_dynamic_positioning(
    positions: List[Dict[str, Any]],
    defensive_positions: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Tilt long book toward higher-quality branches and scale total risk."""
    if not positions:
        return {"positions": positions, "dynamic_book": _compute_dynamic_scale([], defensive_positions)}

    dynamic_book = _compute_dynamic_scale(positions, defensive_positions)
    theme_weights = _theme_weights(positions)
    crowded_theme = dynamic_book.get("crowded_theme", "")
    defensive_themes = {primary_theme(item) for item in defensive_positions}

    current_total = sum(_safe_weight(item) for item in positions)
    target_total = min(_LONG_BOOK_CAP, current_total * float(dynamic_book.get("scale", 1.0)))

    weighted: List[tuple[Dict[str, Any], float]] = []
    for raw in positions:
        signal = dict(raw)
        tilt = 1.0
        family = alpha_family(signal)
        theme = primary_theme(signal)
        hop = int(signal.get("hop", 0))

        if family == "info_gap":
            tilt *= 1.08
        elif family == "source":
            tilt *= 0.92
        elif family == "continuation":
            tilt *= 0.97

        if hop >= 2:
            tilt *= 1.05
        if theme in defensive_themes:
            tilt *= 0.92
        if theme == crowded_theme and family != "info_gap":
            tilt *= 0.93
        if bool(signal.get("reacted_continuation", False)):
            tilt *= 0.96

        weighted.append((signal, _safe_weight(signal) * tilt))

    denom = sum(value for _, value in weighted)
    if denom <= 1e-9:
        return {"positions": positions, "dynamic_book": dynamic_book}

    adjusted: List[Dict[str, Any]] = []
    for signal, value in weighted:
        signal["target_weight"] = min(
            _POSITION_CAP,
            round(target_total * value / denom, 4),
        )
        adjusted.append(signal)

    adjusted_total = sum(_safe_weight(item) for item in adjusted)
    if adjusted_total > target_total + 1e-9:
        shrink = target_total / adjusted_total
        for item in adjusted:
            item["target_weight"] = round(_safe_weight(item) * shrink, 4)

    dynamic_book["target_total"] = round(
        sum(_safe_weight(item) for item in adjusted),
        4,
    )
    dynamic_book["theme_count"] = len(theme_weights)
    return {"positions": adjusted, "dynamic_book": dynamic_book}


def allocate_portfolio(signals: List[Dict[str, Any]], max_positions: int = 10) -> Dict[str, Any]:
    """Build a portfolio summary from ranked unified signals."""
    ranked = sorted(
        [dict(item) for item in signals],
        key=lambda item: (
            float(item.get("score", 0.0)),
            float(item.get("confidence", 0.0)),
            float(item.get("graph_rank_score", 0.0)),
        ),
        reverse=True,
    )

    long_candidates: List[Dict[str, Any]] = []
    defensive_candidates: List[Dict[str, Any]] = []

    for signal in ranked:
        signal["target_weight"] = compute_position_budget(signal)
        if signal["target_weight"] <= 0:
            continue
        if is_deployable_action(signal):
            long_candidates.append(signal)
        elif is_defensive_action(signal):
            defensive_candidates.append(signal)

    constrained = apply_constraints(
        long_candidates,
        defensive_candidates,
        max_positions=max_positions,
    )
    long_book = constrained["long_book"]
    defensive_book = constrained["defensive_book"]

    dynamic_result = _apply_dynamic_positioning(
        long_book["positions"],
        defensive_book["positions"],
    )
    positions = dynamic_result["positions"]
    defensive_positions = defensive_book["positions"]
    dynamic_book = dynamic_result["dynamic_book"]

    family_weights: Dict[str, float] = {}
    for bucket in (positions, defensive_positions):
        for signal in bucket:
            family = alpha_family(signal)
            family_weights[family] = family_weights.get(family, 0.0) + float(signal.get("target_weight", 0.0))

    long_theme_weights = _theme_weights(positions)
    long_event_weights: Dict[str, float] = defaultdict(float)
    for signal in positions:
        event_type = str(signal.get("event_type", "")).strip() or "其他"
        long_event_weights[event_type] += float(signal.get("target_weight", 0.0))

    theme_weights = dict(long_theme_weights)
    for key, value in defensive_book["theme_weights"].items():
        theme_weights[key] = theme_weights.get(key, 0.0) + value

    gross_long_weight = round(sum(_safe_weight(item) for item in positions), 4)
    defensive_weight = round(defensive_book["total_weight"], 4)
    total_weight = round(gross_long_weight + defensive_weight, 4)
    rotation_hints = build_rotation_hints(positions, defensive_positions)

    return {
        "positions": positions,
        "defensive_positions": defensive_positions,
        "gross_long_weight": gross_long_weight,
        "defensive_weight": defensive_weight,
        "total_weight": total_weight,
        "num_positions": len(positions),
        "num_defensive": len(defensive_positions),
        "rotation_hints": rotation_hints,
        "family_weights": top_items(family_weights, limit=4),
        "theme_weights": top_items(theme_weights, limit=4),
        "event_type_weights": top_items(long_event_weights, limit=4),
        "dynamic_book": dynamic_book,
        "constraint_stats": {
            "long_rejected": long_book["rejected_counts"],
            "defensive_rejected": defensive_book["rejected_counts"],
        },
    }
