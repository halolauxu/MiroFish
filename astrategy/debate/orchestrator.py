"""Structured debate orchestrator."""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional

from astrategy.debate.agents import (
    CausalityAgent,
    FundamentalAgent,
    PMAgent,
    RiskAgent,
    SentimentAgent,
)
from astrategy.debate.schemas import DebateResult, DebateVote


class DebateOrchestrator:
    """Aggregate structured agent votes into a calibrated debate result."""

    def __init__(self) -> None:
        self._agents = [
            FundamentalAgent(),
            CausalityAgent(),
            SentimentAgent(),
            RiskAgent(),
            PMAgent(),
        ]

    def run_rule_based(
        self,
        event: Dict[str, Any],
        target: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        context = dict(context or {})
        votes = [
            DebateVote(**agent.evaluate(event, target, context))
            for agent in self._agents
        ]
        return self._finalize(target.get("code", ""), votes, context).to_dict()

    def summarize_reactions(
        self,
        event: Dict[str, Any],
        target: Dict[str, Any],
        reactions: List[Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Convert raw S10 reactions into a structured debate result."""
        context = dict(context or {})
        votes: List[DebateVote] = []
        for reaction in reactions:
            score = float(reaction.get("sentiment_score", 0.0))
            direction = "bullish" if score > 0.1 else "bearish" if score < -0.1 else "neutral"
            votes.append(DebateVote(
                agent_name=str(reaction.get("archetype", "unknown")),
                direction=direction,
                score=score,
                confidence=min(0.95, 0.45 + abs(score) * 0.45),
                reasoning=str(reaction.get("reasoning", ""))[:120],
            ))

        if not votes:
            return self.run_rule_based(event, target, context)
        return self._finalize(target.get("code", ""), votes, context).to_dict()

    def _finalize(
        self,
        target_code: str,
        votes: List[DebateVote],
        context: Dict[str, Any],
    ) -> DebateResult:
        scores = [vote.score for vote in votes]
        mean_score = sum(scores) / len(scores) if scores else 0.0
        divergence = math.sqrt(sum((s - mean_score) ** 2 for s in scores) / len(scores)) if scores else 0.0
        conviction = max(0.0, min(1.0, abs(mean_score) * 1.2 + (1.0 - divergence) * 0.3))
        evidence_density = max(0.0, min(1.0, 0.35 + len(votes) * 0.08))
        if mean_score > 0.1:
            direction = "bullish"
        elif mean_score < -0.1:
            direction = "bearish"
        else:
            direction = "neutral"

        scenario_probs = {
            "bullish": round(max(0.0, min(1.0, 0.5 + mean_score / 2)), 4),
            "bearish": round(max(0.0, min(1.0, 0.5 - mean_score / 2)), 4),
        }
        scenario_probs["neutral"] = round(max(0.0, 1.0 - scenario_probs["bullish"] - scenario_probs["bearish"]), 4)
        invalidators = []
        if context.get("crowding_score", 0.0) > 0.75:
            invalidators.append("叙事过热导致兑现压力")
        if context.get("reaction_score", 1.0) < 0.35:
            invalidators.append("市场已大幅反应，信息差不足")
        if context.get("shock_weight", 0.0) < 0.15:
            invalidators.append("传播链过弱")

        summary = " | ".join(
            f"{vote.agent_name}:{vote.direction}({vote.score:+.2f})" for vote in votes
        )
        return DebateResult(
            target_code=target_code,
            consensus_direction=direction,
            conviction=round(conviction, 4),
            divergence=round(divergence, 4),
            evidence_density=round(evidence_density, 4),
            scenario_probs=scenario_probs,
            invalidators=invalidators,
            expected_holding_days=int(context.get("expected_holding_days", 5)),
            agent_votes=votes,
            debate_summary=summary,
        )
