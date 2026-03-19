"""Execution-plan formatting."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List


def _format_weight_items(title: str, items: List[Dict[str, Any]]) -> List[str]:
    if not items:
        return []
    names = " / ".join(f"{item.get('name', '')}:{item.get('weight', 0):.1%}" for item in items)
    return [f"- {title}: {names}"]


def build_execution_plan(portfolio: Dict[str, Any]) -> str:
    """Format a compact execution plan summary."""
    dynamic = portfolio.get("dynamic_book", {})
    lines = [
        "### 组合执行建议",
        "",
        f"- 长仓目标权重: {portfolio.get('gross_long_weight', 0):.1%}",
        f"- 防守/回避权重: {portfolio.get('defensive_weight', 0):.1%}",
        f"- 长仓持仓数: {portfolio.get('num_positions', 0)}",
        f"- 防守信号数: {portfolio.get('num_defensive', 0)}",
        f"- 动态仓位系数: {dynamic.get('scale', 0):.2f}",
    ]
    if dynamic:
        lines.append(
            f"- 动态画像: avg_hop={dynamic.get('avg_hop', 0):.2f}, "
            f"info_gap={dynamic.get('info_gap_share', 0):.1%}, "
            f"source={dynamic.get('source_share', 0):.1%}, "
            f"crowded={dynamic.get('crowded_theme', '')}:{dynamic.get('crowded_theme_ratio', 0):.1%}"
        )

    for row in _format_weight_items("Alpha 家族", portfolio.get("family_weights", [])):
        lines.append(row)
    for row in _format_weight_items("主题集中度", portfolio.get("theme_weights", [])):
        lines.append(row)
    for row in _format_weight_items("事件桶", portfolio.get("event_type_weights", [])):
        lines.append(row)

    for pos in portfolio.get("positions", [])[:5]:
        lines.append(
            f"- 长仓 {pos.get('target_code', '')}: {pos.get('action', '')}, "
            f"目标权重 {pos.get('target_weight', 0):.1%}, "
            f"alpha={pos.get('alpha_family', '')}, 分数 {pos.get('score', 0):.2f}"
        )
    for pos in portfolio.get("defensive_positions", [])[:3]:
        lines.append(
            f"- 防守 {pos.get('target_code', '')}: {pos.get('action', '')}, "
            f"参考权重 {pos.get('target_weight', 0):.1%}, "
            f"主题 {pos.get('theme_bucket', '')}"
        )
    for hint in portfolio.get("rotation_hints", []):
        lines.append(f"- {hint}")
    return "\n".join(lines)
