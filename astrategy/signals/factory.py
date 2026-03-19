"""Unified signal factory."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .actions import resolve_action
from .explain import compose_reasoning
from .filters import should_emit_signal
from .ranker import compute_signal_score


class SignalFactory:
    """Build unified action-level signals from pipeline components."""

    def build(
        self,
        event: Dict[str, Any],
        target: Dict[str, Any],
        debate: Dict[str, Any],
        market_check: Dict[str, Any],
        narratives: List[str],
        allow_rejected: bool = False,
    ) -> Optional[Dict[str, Any]]:
        event_type = event.get("event_type") or event.get("type") or "other"
        if event_type in {"scandal", "policy_risk", "management_change"}:
            direction = "avoid"
        elif event_type in {"cooperation", "earnings_surprise", "order_win", "supply_shortage"}:
            direction = "long"
        else:
            direction = "avoid"

        graph_rank_score = float(
            target.get("graph_rank_score", target.get("graph_score", target.get("shock_weight", 0.0)))
        )
        path_quality = float(target.get("path_quality", target.get("shock_weight", 0.0)))
        specificity_score = float(target.get("specificity_score", 0.5))
        hop = min(int(target.get("hop", 1)), 3)
        propagation_score = min(
            1.0,
            graph_rank_score * 0.55
            + path_quality * 0.20
            + specificity_score * 0.10
            + 0.05 * hop,
        )
        debate_score = max(0.0, min(1.0, float(debate.get("conviction", 0.0)) * (1.0 - float(debate.get("divergence", 0.0)) * 0.5)))
        trigger_score = float(event.get("trigger_score", 0.0))
        market_check_score = float(market_check.get("market_check_score", 0.0))
        risk_penalty = float(market_check.get("crowding_score", 0.0)) * 0.1 + float(market_check.get("gap_risk", 0.0)) * 0.05
        confidence = max(
            0.0,
            min(
                0.95,
                0.25
                + trigger_score * 0.25
                + propagation_score * 0.20
                + debate_score * 0.20
                + market_check_score * 0.10,
            ),
        )
        action = resolve_action(
            direction=direction,
            confidence=confidence,
            divergence=float(debate.get("divergence", 0.0)),
            tradable=bool(market_check.get("tradable", False)),
        )
        score = compute_signal_score(
            trigger_score=trigger_score,
            propagation_score=propagation_score,
            debate_score=debate_score,
            market_check_score=market_check_score,
            risk_penalty=risk_penalty,
        )
        emittable = should_emit_signal(score, confidence, bool(market_check.get("tradable", False)))
        if not emittable and not allow_rejected:
            return None

        reasoning = compose_reasoning(event, target, action, narratives, debate, market_check)
        return {
            "emittable": emittable,
            "action": action,
            "signal_direction": direction,
            "score": score,
            "confidence": round(confidence, 4),
            "expected_return": round((score - 0.5) * 0.12, 4),
            "expected_holding_days": int(debate.get("expected_holding_days", 5) or 5),
            "trigger_strength": round(trigger_score, 4),
            "propagation_score": round(propagation_score, 4),
            "debate_score": round(debate_score, 4),
            "market_check_score": round(market_check_score, 4),
            "risk_penalty": round(risk_penalty, 4),
            "reasoning": reasoning,
        }
