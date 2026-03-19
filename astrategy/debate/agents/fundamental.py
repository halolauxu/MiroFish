"""Fundamental debate agent."""

from __future__ import annotations

from typing import Any, Dict


class FundamentalAgent:
    name = "fundamental"

    def evaluate(self, event: Dict[str, Any], target: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        event_type = event.get("event_type") or event.get("type") or "other"
        base_scores = {
            "earnings_surprise": 0.8,
            "cooperation": 0.45,
            "order_win": 0.6,
            "technology_breakthrough": 0.55,
            "policy_support": 0.5,
            "scandal": -0.85,
            "policy_risk": -0.6,
            "management_change": -0.4,
        }
        score = base_scores.get(event_type, 0.0)
        score *= max(0.4, 1.0 - 0.12 * max(target.get("hop", 1) - 1, 0))
        direction = "bullish" if score > 0.1 else "bearish" if score < -0.1 else "neutral"
        return {
            "agent_name": self.name,
            "direction": direction,
            "score": round(score, 4),
            "confidence": min(0.9, 0.5 + abs(score) * 0.4),
            "reasoning": f"基于事件类型{event_type}和{target.get('hop', 1)}跳传导评估基本面兑现度。",
        }
