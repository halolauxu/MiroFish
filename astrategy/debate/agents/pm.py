"""Portfolio manager debate agent."""

from __future__ import annotations

from typing import Any, Dict


class PMAgent:
    name = "pm"

    def evaluate(self, event: Dict[str, Any], target: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        shock_weight = float(target.get("shock_weight", 0.0))
        trigger_score = float(context.get("trigger_score", 0.0))
        score = shock_weight * 0.5 + trigger_score * 0.4 - float(context.get("crowding_score", 0.0)) * 0.2
        if event.get("event_type") in {"scandal", "policy_risk"}:
            score = -abs(score)
        direction = "bullish" if score > 0.1 else "bearish" if score < -0.1 else "neutral"
        return {
            "agent_name": self.name,
            "direction": direction,
            "score": round(score, 4),
            "confidence": min(0.9, 0.45 + abs(score) * 0.45),
            "reasoning": "从组合经理视角综合触发强度、传播强度与拥挤度进行最终裁决。",
        }
