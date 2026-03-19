"""Rotation hints."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List

from .common import alpha_family, primary_theme


def build_rotation_hints(
    long_positions: List[Dict[str, Any]],
    defensive_positions: List[Dict[str, Any]] | None = None,
) -> List[str]:
    """Generate actionable rotation hints from current portfolio shape."""
    defensive_positions = defensive_positions or []
    hints: List[str] = []

    by_source: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for signal in long_positions:
        by_source[str(signal.get("source_code", ""))].append(signal)

    for source_code, source_signals in by_source.items():
        source_leg = next((item for item in source_signals if int(item.get("hop", 0)) == 0), None)
        downstream = [
            item for item in source_signals
            if int(item.get("hop", 0)) >= 1 and item.get("action") in {"open_long", "add_long", "rotate_into"}
        ]
        if source_leg and downstream:
            best_downstream = max(downstream, key=lambda item: float(item.get("score", 0.0)))
            if float(best_downstream.get("score", 0.0)) >= float(source_leg.get("score", 0.0)) - 0.02:
                hints.append(
                    f"源头 {source_code} 已进入组合，可优先向下游 {best_downstream.get('target_code', '')} 轮动。"
                )

    theme_long = defaultdict(float)
    theme_defensive = defaultdict(float)
    for signal in long_positions:
        theme_long[primary_theme(signal)] += float(signal.get("target_weight", 0.0))
    for signal in defensive_positions:
        theme_defensive[primary_theme(signal)] += float(signal.get("target_weight", 0.0))

    crowded_themes = sorted(theme_long.items(), key=lambda item: item[1], reverse=True)
    if crowded_themes and crowded_themes[0][1] >= 0.24:
        hints.append(f"主题 {crowded_themes[0][0]} 权重偏高，后续增量仓位优先切到次级主题。")

    conflicting_themes = [
        theme for theme, long_weight in theme_long.items()
        if long_weight > 0.10 and theme_defensive.get(theme, 0.0) > 0.05
    ]
    for theme in conflicting_themes[:2]:
        hints.append(f"主题 {theme} 同时出现进攻和防守信号，适合边减源头边向低拥挤支线切换。")

    continuation_weight = sum(
        float(signal.get("target_weight", 0.0))
        for signal in long_positions
        if signal.get("alpha_family") == "continuation"
    )
    long_weight = sum(float(signal.get("target_weight", 0.0)) for signal in long_positions)
    source_weight = sum(
        float(signal.get("target_weight", 0.0))
        for signal in long_positions
        if alpha_family(signal) == "source"
    )
    deep_hop_weight = sum(
        float(signal.get("target_weight", 0.0))
        for signal in long_positions
        if int(signal.get("hop", 0)) >= 2
    )
    if long_weight > 0 and continuation_weight / long_weight >= 0.45:
        hints.append("延续型仓位占比较高，轮动时优先兑现高拥挤 continuation，补充 info_gap。")
    if long_weight > 0 and source_weight / long_weight >= 0.45 and deep_hop_weight / long_weight >= 0.18:
        hints.append("源头仓位偏重，可逐步把增量资金轮动到 2-3 跳高质量链路。")

    return hints[:4]
