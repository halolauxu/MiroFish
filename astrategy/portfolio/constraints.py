"""Portfolio constraints."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List

from .common import all_themes, alpha_family, primary_theme, source_cluster


def _apply_book_constraints(
    signals: List[Dict[str, Any]],
    *,
    max_single_weight: float,
    max_total_weight: float,
    max_positions: int,
    family_caps: Dict[str, float],
    theme_cap: float,
    source_cap: float,
    event_cap: float,
) -> Dict[str, Any]:
    """Apply concentration limits to one side of the book."""
    selected: List[Dict[str, Any]] = []
    family_weights = defaultdict(float)
    theme_weights = defaultdict(float)
    source_weights = defaultdict(float)
    event_weights = defaultdict(float)
    used_targets: set[str] = set()
    rejected = defaultdict(int)
    total_weight = 0.0

    ranked = sorted(
        signals,
        key=lambda item: (
            float(item.get("score", 0.0)),
            float(item.get("confidence", 0.0)),
            float(item.get("graph_rank_score", 0.0)),
        ),
        reverse=True,
    )

    for raw_signal in ranked:
        if len(selected) >= max_positions or total_weight >= max_total_weight - 1e-9:
            rejected["book_full"] += 1
            continue

        signal = dict(raw_signal)
        target_code = str(signal.get("target_code", "")).strip()
        if target_code and target_code in used_targets:
            rejected["duplicate_target"] += 1
            continue

        family = alpha_family(signal)
        event_type = str(signal.get("event_type", "")).strip() or "其他"
        source = source_cluster(signal)
        theme = primary_theme(signal)

        weight = min(float(signal.get("target_weight", 0.0)), max_single_weight)
        if weight <= 0:
            rejected["non_positive_weight"] += 1
            continue

        weight = min(weight, max_total_weight - total_weight)
        weight = min(weight, max(0.0, family_caps.get(family, 0.18) - family_weights[family]))
        weight = min(weight, max(0.0, theme_cap - theme_weights[theme]))
        weight = min(weight, max(0.0, source_cap - source_weights[source]))
        weight = min(weight, max(0.0, event_cap - event_weights[event_type]))

        if weight <= 0.01:
            rejected["cap_limited"] += 1
            continue

        signal["target_weight"] = round(weight, 4)
        signal["theme_bucket"] = theme
        signal["family_bucket"] = family
        signal["source_bucket"] = source
        signal["theme_tags"] = all_themes(signal)
        selected.append(signal)
        total_weight += weight
        family_weights[family] += weight
        theme_weights[theme] += weight
        source_weights[source] += weight
        event_weights[event_type] += weight
        if target_code:
            used_targets.add(target_code)

    return {
        "positions": selected,
        "total_weight": round(total_weight, 4),
        "family_weights": {k: round(v, 4) for k, v in family_weights.items()},
        "theme_weights": {k: round(v, 4) for k, v in theme_weights.items()},
        "source_weights": {k: round(v, 4) for k, v in source_weights.items()},
        "event_type_weights": {k: round(v, 4) for k, v in event_weights.items()},
        "rejected_counts": dict(rejected),
    }


def apply_constraints(
    long_signals: List[Dict[str, Any]],
    defensive_signals: List[Dict[str, Any]],
    *,
    max_single_weight: float = 0.12,
    max_long_weight: float = 0.95,
    max_defensive_weight: float = 0.32,
    max_positions: int = 10,
    max_defensive_positions: int = 6,
) -> Dict[str, Any]:
    """Apply portfolio concentration constraints to long and defensive books."""
    long_book = _apply_book_constraints(
        long_signals,
        max_single_weight=max_single_weight,
        max_total_weight=max_long_weight,
        max_positions=max_positions,
        family_caps={"info_gap": 0.34, "continuation": 0.42, "source": 0.26},
        theme_cap=0.28,
        source_cap=0.22,
        event_cap=0.32,
    )
    defensive_book = _apply_book_constraints(
        defensive_signals,
        max_single_weight=min(0.10, max_single_weight),
        max_total_weight=max_defensive_weight,
        max_positions=max_defensive_positions,
        family_caps={"info_gap": 0.10, "continuation": 0.22, "source": 0.18, "unknown": 0.12},
        theme_cap=0.18,
        source_cap=0.14,
        event_cap=0.20,
    )
    return {
        "long_book": long_book,
        "defensive_book": defensive_book,
    }
