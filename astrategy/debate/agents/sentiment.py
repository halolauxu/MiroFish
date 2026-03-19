"""Sentiment debate agent."""

from __future__ import annotations

from typing import Any, Dict


class SentimentAgent:
    name = "sentiment"

    def evaluate(self, event: Dict[str, Any], target: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        crowding = float(context.get("crowding_score", 0.4))
        event_type = event.get("event_type") or event.get("type") or "other"
        score = 0.35 - crowding * 0.4
        if event_type in {"scandal", "policy_risk", "sentiment_reversal"}:
            score -= 0.45
        direction = "bullish" if score > 0.1 else "bearish" if score < -0.1 else "neutral"
        return {
            "agent_name": self.name,
            "direction": direction,
            "score": round(score, 4),
            "confidence": min(0.85, 0.4 + abs(score) * 0.4),
            "reasoning": f"结合舆情拥挤度{crowding:.2f}和事件类型判断情绪延续或反转。",
        }
