"""Signal explanation helpers."""

from __future__ import annotations

from typing import Any, Dict, List


def compose_reasoning(
    event: Dict[str, Any],
    target: Dict[str, Any],
    action: str,
    narratives: List[str],
    debate: Dict[str, Any],
    market_check: Dict[str, Any],
) -> str:
    """Compose a concise reasoning string for a unified signal."""
    narrative_str = ",".join(narratives) if narratives else "无"
    return (
        f"[五段式] 事件={event.get('title', '')[:36]} | 标的={target.get('name', '')}"
        f" | 动作={action} | 传播={target.get('hop', 0)}跳/{target.get('shock_weight', 0):.2f}"
        f" | 辩论={debate.get('consensus_direction', 'neutral')}/{debate.get('divergence', 0):.2f}"
        f" | 市场={market_check.get('reaction_label', '未知')}/{market_check.get('market_check_score', 0):.2f}"
        f" | 叙事={narrative_str}"
    )
