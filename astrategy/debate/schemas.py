"""Data models for structured debate output."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


@dataclass
class DebateVote:
    agent_name: str
    direction: str
    score: float
    confidence: float
    reasoning: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DebateResult:
    target_code: str
    consensus_direction: str
    conviction: float
    divergence: float
    evidence_density: float
    scenario_probs: Dict[str, float]
    invalidators: List[str] = field(default_factory=list)
    expected_holding_days: int = 5
    agent_votes: List[DebateVote] = field(default_factory=list)
    debate_summary: str = ""

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["agent_votes"] = [vote.to_dict() for vote in self.agent_votes]
        return data
