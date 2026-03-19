"""Risk debate agent."""

from __future__ import annotations

from typing import Any, Dict


class RiskAgent:
    name = "risk"

    def evaluate(self, event: Dict[str, Any], target: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        hop = int(target.get("hop", 1))
        crowding = float(context.get("crowding_score", 0.4))
        reaction_score = float(context.get("reaction_score", 0.7))
        score = -0.15 * max(hop - 1, 0) - 0.35 * crowding + 0.25 * reaction_score
        direction = "bullish" if score > 0.1 else "bearish" if score < -0.1 else "neutral"
        return {
            "agent_name": self.name,
            "direction": direction,
            "score": round(score, 4),
            "confidence": min(0.85, 0.45 + abs(score) * 0.35),
            "reasoning": f"结合跳数{hop}、拥挤度和市场反应评估风险收益比。",
        }
