"""Causality debate agent."""

from __future__ import annotations

from typing import Any, Dict


class CausalityAgent:
    name = "causality"

    def evaluate(self, event: Dict[str, Any], target: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        shock_weight = float(target.get("shock_weight", 0.0))
        relation_chain = " ".join(target.get("relation_chain", []))
        event_type = event.get("event_type") or event.get("type") or "other"
        score = shock_weight * 0.9
        if "COMPETES_WITH" in relation_chain and event_type in {"scandal", "policy_risk"}:
            score = abs(score)
        elif "COMPETES_WITH" in relation_chain:
            score = -abs(score) * 0.6
        elif event_type in {"scandal", "policy_risk", "management_change"}:
            score = -abs(score)
        direction = "bullish" if score > 0.1 else "bearish" if score < -0.1 else "neutral"
        return {
            "agent_name": self.name,
            "direction": direction,
            "score": round(score, 4),
            "confidence": min(0.9, 0.45 + abs(score) * 0.5),
            "reasoning": f"依据传播权重{shock_weight:.2f}和关系链判断事件是否可传导。",
        }
