"""
Shock Propagation Pipeline — 冲击传播链路
==========================================

核心理念：信息差 Alpha
    事件/舆情命中公司 A → 市场已给 A 定价 →
    图谱找到下游 B/C/D → 市场还没给 B/C/D 定价 →
    多 Agent 辩论 B/C/D 的影响方向和确定性 →
    分歧度高 → 轻仓；共识明确 → 重仓 →
    检查 B/C/D 近期是否已反应 → 未反应 = Alpha

Pipeline 步骤:
    1. 事件检测 (S01/S10)
    2. 图谱冲击传播 (topology.propagate_shock)
    3. Agent 辩论 (S10 多轮模拟 + 分歧度)
    4. 未反应检测 (价格检查)
    5. 信号生成

Usage:
    python -m astrategy.shock_pipeline [--stocks 600519,000858,...]
"""

from __future__ import annotations

import json
import logging
import math
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from astrategy.debate import DebateOrchestrator
from astrategy.events.normalizer import normalize_events
from astrategy.graph.local_store import LocalGraphStore
from astrategy.graph.topology import TopologyAnalyzer
from astrategy.market_checks import build_market_check
from astrategy.narratives import (
    build_narrative_relations,
    estimate_crowding_score,
    extract_narratives,
    infer_narrative_phase,
)
from astrategy.signals import SignalFactory
from astrategy.strategies.base import BaseStrategy, StrategySignal

logger = logging.getLogger("astrategy.shock_pipeline")

_CST = timezone(timedelta(hours=8))

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Shock propagation params
_MAX_HOPS = 3
_DECAY = 0.5
_PROPAGATION_RELATIONS = {
    "SUPPLIES_TO", "CUSTOMER_OF", "COOPERATES_WITH",
    "COMPETES_WITH",   # 竞争对手也受冲击（反向）
    "HOLDS_SHARES",    # 基金持仓链路（同一基金重仓的股票联动）
}

# Agent divergence thresholds
_HIGH_DIVERGENCE = 0.4    # std > 0.4 → agents strongly disagree
_LOW_DIVERGENCE = 0.15    # std < 0.15 → strong consensus

# Price reaction thresholds
_REACTION_THRESHOLD = 0.02   # |return_5d| > 2% = already reacted
_REACTION_LOOKBACK_DAYS = 5

# Signal generation thresholds
_MIN_SHOCK_WEIGHT = 0.1      # Ignore very weak propagation paths
_MIN_CONVICTION = 0.25       # Minimum conviction to generate signal
_MIN_TRIGGER_SCORE = 0.35

_ROUND2_ALLOWED_EVENT_TYPES = {
    "cooperation",
    "policy_risk",
    "order_win",
    "management_change",
    "ma",
    "price_adjustment",
    "earnings_surprise",
}

_REACTED_CONTINUATION_MIN_MARKET_SCORE = 0.38
_REACTED_CONTINUATION_RULES = {
    "cooperation": {1, 2, 3},
    "ma": {1, 2, 3},
    "order_win": {1, 2, 3},
    "policy_risk": {2, 3},
    "price_adjustment": {1, 2, 3},
}

_RELATION_EVENT_BIASES = {
    "cooperation": {
        "COOPERATES_WITH": 1.15,
        "SUPPLIES_TO": 1.05,
        "CUSTOMER_OF": 1.00,
        "COMPETES_WITH": 0.80,
        "HOLDS_SHARES": 0.35,
    },
    "order_win": {
        "SUPPLIES_TO": 1.20,
        "CUSTOMER_OF": 1.10,
        "COOPERATES_WITH": 1.05,
        "COMPETES_WITH": 0.75,
        "HOLDS_SHARES": 0.30,
    },
    "price_adjustment": {
        "COMPETES_WITH": 1.20,
        "SUPPLIES_TO": 0.95,
        "CUSTOMER_OF": 0.95,
        "COOPERATES_WITH": 0.90,
        "HOLDS_SHARES": 0.25,
    },
    "earnings_surprise": {
        "SUPPLIES_TO": 1.08,
        "CUSTOMER_OF": 1.05,
        "COOPERATES_WITH": 1.00,
        "COMPETES_WITH": 0.85,
        "HOLDS_SHARES": 0.30,
    },
    "ma": {
        "COOPERATES_WITH": 1.10,
        "SUPPLIES_TO": 1.00,
        "CUSTOMER_OF": 1.00,
        "COMPETES_WITH": 0.85,
        "HOLDS_SHARES": 0.45,
    },
    "management_change": {
        "SUPPLIES_TO": 1.00,
        "CUSTOMER_OF": 1.00,
        "COOPERATES_WITH": 0.95,
        "COMPETES_WITH": 0.90,
        "HOLDS_SHARES": 0.35,
    },
    "policy_risk": {
        "SUPPLIES_TO": 1.08,
        "CUSTOMER_OF": 1.05,
        "COOPERATES_WITH": 0.95,
        "COMPETES_WITH": 0.92,
        "HOLDS_SHARES": 0.28,
    },
}


# ---------------------------------------------------------------------------
# ShockPipeline
# ---------------------------------------------------------------------------

class ShockPipeline:
    """End-to-end shock propagation pipeline.

    Integrates graph topology, S10 agent simulation, and price reaction
    detection into a single causal chain.
    """

    def __init__(
        self,
        max_hops: int = _MAX_HOPS,
        decay: float = _DECAY,
        max_downstream: int = 15,
        research_profile: str = "round4_hybrid",
    ) -> None:
        self._max_hops = max_hops
        self._decay = decay
        self._max_downstream = max_downstream
        self._research_profile = research_profile
        self._debate = DebateOrchestrator()
        self._signal_factory = SignalFactory()

        # Load graph
        self._store = LocalGraphStore()
        self._graph_id = "supply_chain"
        self._graph_loaded = self._store.load(self._graph_id)
        if not self._graph_loaded:
            # Try alternative graph name
            self._graph_id = "astrategy"
            self._graph_loaded = self._store.load(self._graph_id)
        if self._graph_loaded:
            logger.info("Graph loaded for shock propagation")
        else:
            logger.warning("No graph data available — shock propagation disabled")

        self._edges: List[Dict] = []
        self._nodes: List[Dict] = []
        self._code_to_name: Dict[str, str] = {}
        self._name_to_code: Dict[str, str] = {}
        self._node_degree: Dict[str, int] = {}

        if self._graph_loaded:
            self._refresh_graph_snapshot()

    def _build_code_name_maps(self) -> None:
        """Build bidirectional code↔name lookup from graph nodes."""
        import re
        self._code_to_name.clear()
        self._name_to_code.clear()
        for n in self._nodes:
            name = n.get("name", "")
            attrs = n.get("attributes", {})
            code = attrs.get("code", "") or attrs.get("stock_code", "")
            display = attrs.get("display_name", "")
            summary = n.get("summary", "")

            # Extract real company name from summary (e.g. "贵州茅台(600519)")
            real_name = ""
            if summary:
                m = re.match(r"^(.+?)\(\d{6}\)", summary)
                if m:
                    real_name = m.group(1).strip()

            # Use best available name: real_name > display (unless "Unknown") > name
            best_name = real_name
            if not best_name and display and "Unknown" not in display:
                best_name = display
            if not best_name:
                best_name = name

            if code and name:
                self._code_to_name[code] = best_name
                self._name_to_code[name] = code
            # Many nodes use stock code as name
            if name and len(name) == 6 and name.isdigit():
                self._code_to_name[name] = best_name
                self._name_to_code[name] = name

    def _refresh_graph_snapshot(self, as_of: str | None = None) -> None:
        """Reload nodes/edges from the local graph at a given historical date."""
        if not self._graph_loaded:
            self._edges = []
            self._nodes = []
            self._code_to_name = {}
            self._name_to_code = {}
            self._node_degree = {}
            return

        self._edges = self._store.get_all_edges(self._graph_id, as_of=as_of)
        self._nodes = self._store.get_all_nodes(self._graph_id, as_of=as_of)
        self._build_code_name_maps()
        degree: Dict[str, int] = defaultdict(int)
        for edge in self._edges:
            src = edge.get("source_name") or edge.get("source", "")
            tgt = edge.get("target_name") or edge.get("target", "")
            if src:
                degree[src] += 1
            if tgt:
                degree[tgt] += 1
        self._node_degree = dict(degree)
        logger.info(
            "Graph snapshot%s: %d nodes, %d edges",
            f" @ {as_of}" if as_of else "",
            len(self._nodes),
            len(self._edges),
        )

    # ==================================================================
    # Step 1: Trigger
    # ==================================================================

    def detect_events(
        self,
        stock_codes: List[str],
        max_events: int = 5,
    ) -> List[Dict[str, Any]]:
        """Detect high-impact events using S01 + S10 event detection.

        Returns a list of event dicts, each with at least:
        {title, type, stock_code, stock_name, summary, key_data}
        """
        events: List[Dict] = []

        # --- S01: Supply chain event keywords ---
        try:
            from astrategy.strategies.s01_supply_chain import SupplyChainStrategy
            s01 = SupplyChainStrategy()
            s01_events = s01.detect_supply_events(stock_codes)
            for e in s01_events:
                e["source_strategy"] = "S01_supply_chain"
            events.extend(s01_events)
            logger.info("S01 detected %d supply chain events", len(s01_events))
        except Exception as exc:
            logger.warning("S01 event detection failed: %s", exc)

        # --- S10: High-impact event detection via LLM ---
        try:
            from astrategy.strategies.s10_sentiment_simulation import (
                SentimentSimulationStrategy,
            )
            s10 = SentimentSimulationStrategy(max_events_per_run=max_events)
            s10_events = s10.detect_simulation_worthy_events(stock_codes=stock_codes)
            for e in s10_events:
                e["source_strategy"] = "S10_sentiment"
            events.extend(s10_events)
            logger.info("S10 detected %d simulation-worthy events", len(s10_events))
        except Exception as exc:
            logger.warning("S10 event detection failed: %s", exc)

        # Deduplicate by stock_code + title similarity
        seen: Set[str] = set()
        unique: List[Dict] = []
        for e in events:
            key = f"{e.get('stock_code', '')}_{e.get('title', '')[:20]}"
            if key not in seen:
                seen.add(key)
                unique.append(e)

        logger.info("Total unique events detected: %d", len(unique))
        return unique[:max_events]

    def _normalize_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize event fields for internal pipeline usage."""
        event = dict(event)
        event_type = event.get("event_type") or event.get("type") or "other"
        event["event_type"] = event_type
        event["type"] = event_type
        event["discover_time"] = event.get("discover_time") or event.get("event_date", "")
        event["available_at"] = event.get("available_at") or event.get("discover_time", "")
        event["tradability_score"] = float(event.get("tradability_score", 0.7))
        event["severity"] = float(event.get("severity", 0.5))
        event["surprise_score"] = float(event.get("surprise_score", 0.5))
        event["confidence"] = float(event.get("confidence", 0.6))
        return event

    def _build_trigger(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Build trigger-stage metadata for an event."""
        normalized = self._normalize_event(event)
        narratives = extract_narratives(normalized)
        crowding_score = estimate_crowding_score(normalized, narratives)
        narrative_phase = infer_narrative_phase(normalized, narratives, crowding_score)
        trigger_score = (
            normalized.get("severity", 0.5) * 0.35
            + normalized.get("surprise_score", 0.5) * 0.35
            + normalized.get("tradability_score", 0.7) * 0.30
        )
        normalized["theme_tags"] = sorted(set(list(normalized.get("theme_tags", [])) + narratives))
        normalized["narrative_tags"] = narratives
        normalized["crowding_score"] = crowding_score
        normalized["narrative_phase"] = narrative_phase
        normalized["narrative_relations"] = build_narrative_relations(normalized, narratives)
        return {
            "event": normalized,
            "trigger_score": round(trigger_score, 4),
            "tradable": trigger_score >= _MIN_TRIGGER_SCORE,
            "trigger_reason": (
                f"severity={normalized.get('severity', 0):.2f}, "
                f"surprise={normalized.get('surprise_score', 0):.2f}, "
                f"tradability={normalized.get('tradability_score', 0):.2f}"
            ),
        }

    def _passes_research_profile(self, event_type: str, hop: int) -> bool:
        """Apply the second-round research corrections discovered in validation."""
        if self._research_profile not in {"round2_strict", "round4_hybrid"}:
            return True

        allowed_event_types = set(_ROUND2_ALLOWED_EVENT_TYPES)
        if self._research_profile == "round4_hybrid":
            allowed_event_types.add("buyback")

        if event_type not in allowed_event_types:
            return False

        if event_type == "cooperation":
            return True
        if event_type == "order_win":
            return True
        if event_type == "buyback":
            return hop >= 2
        if event_type == "policy_risk":
            return hop == 0 or hop >= 2
        if event_type in {"management_change", "ma", "price_adjustment"}:
            return hop >= 1
        if event_type == "earnings_surprise":
            return hop in {1, 3}
        return False

    def _allows_reacted_continuation(
        self,
        event_type: str,
        hop: int,
        market_check_score: float,
        enable_reacted_continuation: bool,
    ) -> bool:
        """Selective continuation branch for reacted downstream names."""
        if not enable_reacted_continuation:
            return False
        if self._research_profile != "round4_hybrid":
            return False
        allowed_hops = _REACTED_CONTINUATION_RULES.get(event_type)
        if not allowed_hops or hop not in allowed_hops:
            return False
        return market_check_score >= _REACTED_CONTINUATION_MIN_MARKET_SCORE

    def _event_relation_bias(self, event_type: str, relation_chain: List[str]) -> float:
        """Event-aware graph bias to suppress noisy generic edges."""
        if not relation_chain:
            return 1.0
        bias_map = _RELATION_EVENT_BIASES.get(event_type, {})
        if not bias_map:
            return 1.0
        values = [bias_map.get(rel, 0.7) for rel in relation_chain]
        return sum(values) / len(values)

    # ==================================================================
    # Step 2: Propagation
    # ==================================================================

    def propagate(
        self,
        source_code: str,
        event: Dict[str, Any],
        downstream_limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Find downstream companies affected by shock at *source_code*.

        Returns list of downstream targets sorted by shock_weight desc.
        Each entry: {code, name, shock_weight, hop, path, relation_chain}
        """
        if not self._graph_loaded:
            logger.warning("No graph — cannot propagate shock from %s", source_code)
            return []

        # Try both code and name as source
        source_name = self._code_to_name.get(source_code, source_code)
        shock_map = TopologyAnalyzer.propagate_shock(
            edges=self._edges,
            source=source_code,
            max_hops=self._max_hops,
            decay=self._decay,
            relation_types=_PROPAGATION_RELATIONS,
        )

        # Also try with display name if different
        if source_name != source_code:
            shock_map_alt = TopologyAnalyzer.propagate_shock(
                edges=self._edges,
                source=source_name,
                max_hops=self._max_hops,
                decay=self._decay,
                relation_types=_PROPAGATION_RELATIONS,
            )
            # Merge: take the higher shock weight
            for node, info in shock_map_alt.items():
                if node not in shock_map or info["shock_weight"] > shock_map[node]["shock_weight"]:
                    shock_map[node] = info

        # Convert to list with code resolution
        targets: List[Dict[str, Any]] = []
        event_type = event.get("event_type") or event.get("type") or ""
        for node_name, info in shock_map.items():
            if info["shock_weight"] < _MIN_SHOCK_WEIGHT:
                continue

            # Resolve to stock code
            code = self._name_to_code.get(node_name, "")
            if not code and len(node_name) == 6 and node_name.isdigit():
                code = node_name
            if not code:
                continue  # Skip non-stock nodes (industry labels etc.)

            # Skip if it's the source itself
            if code == source_code:
                continue

            display_name = self._code_to_name.get(code, node_name)
            degree = self._node_degree.get(code) or self._node_degree.get(node_name, 1)
            specificity_score = 1.0 / math.sqrt(max(1, degree))
            relation_bias = self._event_relation_bias(
                event_type,
                list(info.get("relation_chain", [])),
            )
            graph_score = float(info.get("graph_score", info["shock_weight"]))
            graph_rank_score = graph_score * relation_bias * (0.7 + 0.3 * specificity_score)
            targets.append({
                "code": code,
                "name": display_name,
                "shock_weight": info["shock_weight"],
                "hop": info["hop"],
                "path": info["path"],
                "relation_chain": info["relation_chain"],
                "path_quality": info.get("path_quality", info["shock_weight"]),
                "relation_score": info.get("relation_score", 0.0),
                "graph_score": round(graph_score, 6),
                "graph_rank_score": round(graph_rank_score, 6),
                "specificity_score": round(specificity_score, 6),
                "relation_bias": round(relation_bias, 6),
                "source_event": event.get("title", ""),
                "source_code": source_code,
            })

        # Rank by graph quality, not just raw reachability.
        targets.sort(
            key=lambda t: (
                t.get("graph_rank_score", 0.0),
                t.get("graph_score", 0.0),
                t["shock_weight"],
            ),
            reverse=True,
        )
        limit = self._max_downstream if downstream_limit is None else downstream_limit
        if limit > 0:
            targets = targets[:limit]

        logger.info(
            "Shock from %s → %d downstream targets (top: %s)",
            source_code, len(targets),
            ", ".join(
                f"{t['code']}({t.get('graph_rank_score', t['shock_weight']):.2f})"
                for t in targets[:5]
            ),
        )
        return targets

    def _build_propagation(
        self,
        event: Dict[str, Any],
        downstream_limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        source_code = event.get("stock_code", "")
        targets = self.propagate(source_code, event, downstream_limit=downstream_limit) if source_code else []
        return {
            "source_code": source_code,
            "targets": targets,
            "target_count": len(targets),
        }

    # ==================================================================
    # Step 3: Debate
    # ==================================================================

    def debate_impact(
        self,
        target: Dict[str, Any],
        event: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Run multi-agent debate on how shock impacts a downstream target.

        Injects graph propagation context into agent prompts so they reason
        about the supply-chain relationship, not just the raw event.

        Returns:
            {consensus_direction, consensus_sentiment, divergence,
             conviction, agent_reactions, debate_summary}
        """
        from astrategy.strategies.s10_sentiment_simulation import (
            SentimentSimulationStrategy,
            AGENT_ARCHETYPES,
            _INFLUENCE_WEIGHTS,
        )

        s10 = SentimentSimulationStrategy(max_events_per_run=1)

        # Build a synthetic event for the downstream target with graph context
        path_str = " → ".join(target["path"])
        rel_str = " → ".join(target["relation_chain"])
        graph_context = (
            f"【供应链冲击传播路径】\n"
            f"事件源头: {event.get('stock_name', event.get('stock_code', ''))} "
            f"({event.get('stock_code', '')})\n"
            f"传播路径: {path_str}\n"
            f"关系链: {rel_str}\n"
            f"冲击衰减权重: {target['shock_weight']:.2f} "
            f"(第{target['hop']}跳, 每跳衰减{self._decay*100:.0f}%)\n"
            f"原始事件: {event.get('title', '')}\n"
            f"事件摘要: {event.get('summary', '')}\n"
        )

        downstream_event = {
            "title": f"供应链冲击: {event.get('title', '')} → 影响{target['name']}",
            "type": event.get("type", "supply_shock"),
            "stock_code": target["code"],
            "stock_name": target["name"],
            "impact_level": "high" if target["shock_weight"] > 0.3 else "medium",
            "summary": (
                f"{event.get('summary', '')}。"
                f"该事件通过{rel_str}关系传导至{target['name']}。"
                f"传导权重{target['shock_weight']:.2f}。"
            ),
            "key_data": event.get("key_data", ""),
            "graph_context": graph_context,
        }
        # Inject graph context into summary so S10 agents see it
        downstream_event["summary"] = (
            downstream_event["summary"] + "\n" + graph_context
        )

        # Generate profiles and simulate
        try:
            profiles = s10.generate_agent_profiles(downstream_event, target["code"])
            if s10._use_multi_round:
                round_reactions = s10.simulate_multi_round_reactions(
                    downstream_event, profiles,
                )
                all_reactions = (
                    round_reactions.get("round1", [])
                    + round_reactions.get("round2", [])
                    + round_reactions.get("round3", [])
                )
            else:
                all_reactions = s10.simulate_reactions(downstream_event, profiles)
        except Exception as exc:
            logger.error("Agent debate failed for %s: %s", target["code"], exc)
            return self._debate.run_rule_based(
                event=event,
                target=target,
                context={
                    "trigger_score": event.get("trigger_score", 0.0),
                    "crowding_score": event.get("crowding_score", 0.4),
                    "shock_weight": target.get("shock_weight", 0.0),
                    "expected_holding_days": 5,
                },
            )

        result = self._debate.summarize_reactions(
            event=event,
            target=target,
            reactions=all_reactions,
            context={
                "trigger_score": event.get("trigger_score", 0.0),
                "crowding_score": event.get("crowding_score", 0.4),
                "shock_weight": target.get("shock_weight", 0.0),
                "expected_holding_days": 5,
            },
        )
        result["agent_reactions"] = all_reactions
        result["graph_context_used"] = True
        result["propagation_path"] = target["path"]
        result["relation_chain"] = target["relation_chain"]
        return result

    @staticmethod
    def _empty_debate_result() -> Dict[str, Any]:
        return {
            "consensus_direction": "neutral",
            "consensus_sentiment": 0.0,
            "divergence": 0.0,
            "conviction": 0.0,
            "agent_reactions": [],
            "debate_summary": "",
            "graph_context_used": False,
            "propagation_path": [],
            "relation_chain": [],
        }

    # ==================================================================
    # Step 4: Market Check
    # ==================================================================

    def check_price_reaction(
        self,
        stock_code: str,
        lookback_days: int = _REACTION_LOOKBACK_DAYS,
    ) -> Dict[str, Any]:
        """Check whether a stock has already reacted (priced in the shock).

        Returns {reacted: bool, return_pct: float, volume_change_pct: float}
        """
        try:
            from astrategy.data_collector.market_data import MarketDataCollector
            market = MarketDataCollector()
            end_dt = datetime.now(tz=_CST)
            start_dt = end_dt - timedelta(days=lookback_days + 10)  # buffer
            df = market.get_daily_quotes(
                stock_code,
                start_dt.strftime("%Y%m%d"),
                end_dt.strftime("%Y%m%d"),
            )
        except Exception as exc:
            logger.debug("Price data unavailable for %s: %s", stock_code, exc)
            return {"reacted": False, "return_pct": 0.0, "volume_change_pct": 0.0}

        if df is None or df.empty or len(df) < 2:
            return {"reacted": False, "return_pct": 0.0, "volume_change_pct": 0.0}

        # Use last N rows
        close_col = "收盘" if "收盘" in df.columns else df.columns[-2]
        vol_col = "成交量" if "成交量" in df.columns else None

        recent = df.tail(min(lookback_days + 1, len(df)))
        if len(recent) < 2:
            return {"reacted": False, "return_pct": 0.0, "volume_change_pct": 0.0}

        price_start = float(recent[close_col].iloc[0])
        price_end = float(recent[close_col].iloc[-1])

        if price_start == 0:
            return {"reacted": False, "return_pct": 0.0, "volume_change_pct": 0.0}

        return_pct = (price_end / price_start - 1.0)

        volume_change = 0.0
        if vol_col and vol_col in recent.columns:
            try:
                vol_recent = float(recent[vol_col].iloc[-1])
                vol_start = float(recent[vol_col].iloc[0])
                if vol_start > 0:
                    volume_change = (vol_recent / vol_start - 1.0)
            except (ValueError, TypeError):
                pass

        reacted = abs(return_pct) > _REACTION_THRESHOLD
        return {
            "reacted": reacted,
            "return_pct": round(return_pct, 4),
            "volume_change_pct": round(volume_change, 4),
        }

    def check_price_reaction_at_date(
        self,
        stock_code: str,
        event_date: str,
        forward_days: int = 5,
    ) -> Dict[str, Any]:
        """Check price reaction in *forward_days* after *event_date*.

        Used for historical backtesting: measure how the stock moved after
        the event occurred.

        Parameters
        ----------
        stock_code : stock code
        event_date : YYYY-MM-DD format
        forward_days : how many trading days forward to measure

        Returns
        -------
        {reacted, return_pct, volume_change_pct,
         return_1d, return_3d, return_5d, return_10d, return_20d}
        """
        try:
            from astrategy.data_collector.market_data import MarketDataCollector
            market = MarketDataCollector()
            evt_dt = datetime.strptime(event_date, "%Y-%m-%d")
            # Fetch from 5 days before event to 30 days after
            start_dt = evt_dt - timedelta(days=5)
            end_dt = evt_dt + timedelta(days=max(forward_days, 20) + 10)
            df = market.get_daily_quotes(
                stock_code,
                start_dt.strftime("%Y%m%d"),
                end_dt.strftime("%Y%m%d"),
            )
        except Exception as exc:
            logger.debug("Price data unavailable for %s at %s: %s",
                         stock_code, event_date, exc)
            return self._empty_price_result()

        if df is None or df.empty or len(df) < 3:
            return self._empty_price_result()

        close_col = "收盘" if "收盘" in df.columns else "close"
        open_col = "开盘" if "开盘" in df.columns else "open"
        high_col = "最高" if "最高" in df.columns else "high"
        low_col = "最低" if "最低" in df.columns else "low"
        vol_col = "成交量" if "成交量" in df.columns else None
        date_col = "日期" if "日期" in df.columns else "date"

        if close_col not in df.columns:
            return self._empty_price_result()

        # Find the row closest to event_date (event day or next trading day)
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col])
            evt_dt_pd = pd.to_datetime(event_date)
            after_event = df[df[date_col] >= evt_dt_pd]
            if after_event.empty:
                return self._empty_price_result()
            event_idx = after_event.index[0]
        else:
            # Fallback: use middle of data
            event_idx = len(df) // 3

        # ═══ T+1 入场逻辑 ═══
        # event_idx = 事件日(T+0)行。我们需要 T+1 开盘价入场。
        event_pos = df.index.get_loc(event_idx)
        t1_pos = event_pos + 1  # T+1 在 DataFrame 中的位置
        if t1_pos >= len(df):
            return self._empty_price_result()
        t1_idx = df.index[t1_pos]

        # 入场价 = T+1 开盘价 (可执行价格)
        if open_col in df.columns:
            entry_price = float(df.loc[t1_idx, open_col])
        else:
            entry_price = float(df.loc[t1_idx, close_col])
        if entry_price <= 0:
            return self._empty_price_result()

        # ═══ 涨跌停过滤 ═══
        # A股涨跌停: 普通股±10%, ST股±5%, 科创板/创业板±20%
        # 简化处理: 如果T+1开盘价=T+0收盘价*(1±limit)附近, 视为涨跌停
        t0_close = float(df.loc[event_idx, close_col])
        limit_pct = 0.10  # 默认10%
        if t0_close > 0:
            t1_open_chg = (entry_price / t0_close) - 1.0
            # 涨停: long方向买不进去
            # 跌停: avoid方向卖不出去
            # 标记但不过滤 — 留给上层决策
            hit_limit_up = t1_open_chg >= limit_pct * 0.98
            hit_limit_down = t1_open_chg <= -limit_pct * 0.98
        else:
            hit_limit_up = False
            hit_limit_down = False

        # ═══ Forward returns: 从 T+1 开盘入场后计算 ═══
        # return_Nd = T+N 收盘价 / T+1 开盘价 - 1
        after_t1_data = df.iloc[t1_pos:]  # 从 T+1 开始
        returns = {}
        for horizon in [1, 3, 5, 10, 20]:
            if len(after_t1_data) > horizon:
                exit_price = float(after_t1_data.iloc[horizon][close_col])
                returns[f"return_{horizon}d"] = round(
                    exit_price / entry_price - 1.0, 4
                )
            else:
                returns[f"return_{horizon}d"] = None

        main_return = returns.get(f"return_{forward_days}d", 0.0) or 0.0

        # Volume change
        volume_change = 0.0
        if vol_col and vol_col in df.columns and len(after_t1_data) > 1:
            try:
                vol_t1 = float(df.loc[t1_idx, vol_col])
                # Average volume in forward window
                fwd_vols = after_t1_data[vol_col].iloc[1:min(forward_days+1, len(after_t1_data))]
                if not fwd_vols.empty and vol_t1 > 0:
                    volume_change = float(fwd_vols.mean()) / vol_t1 - 1.0
            except (ValueError, TypeError):
                pass

        reacted = abs(main_return) > _REACTION_THRESHOLD
        result = {
            "reacted": reacted,
            "return_pct": round(main_return, 4),
            "volume_change_pct": round(volume_change, 4),
            "entry_price": entry_price,
            "t0_close": t0_close,
            "hit_limit_up": hit_limit_up,
            "hit_limit_down": hit_limit_down,
        }
        result.update(returns)
        return result

    @staticmethod
    def _empty_price_result() -> Dict[str, Any]:
        return {
            "reacted": False,
            "return_pct": 0.0,
            "volume_change_pct": 0.0,
            "entry_price": 0.0,
            "t0_close": 0.0,
            "hit_limit_up": False,
            "hit_limit_down": False,
            "return_1d": None,
            "return_3d": None,
            "return_5d": None,
            "return_10d": None,
            "return_20d": None,
        }

    def _build_market_check(
        self,
        event: Dict[str, Any],
        target_code: str,
        event_date: str = "",
        historical: bool = False,
        forward_days: int = 5,
    ) -> Dict[str, Any]:
        if historical and event_date:
            reaction = self.check_price_reaction_at_date(
                target_code,
                event_date,
                forward_days=forward_days,
            )
        else:
            reaction = self.check_price_reaction(target_code)

        narratives = list(event.get("narrative_tags", []))
        return build_market_check(
            event=event,
            reaction=reaction,
            narratives=narratives,
            crowding_score=float(event.get("crowding_score", 0.4)),
        )

    # ==================================================================
    # Step 5: Action
    # ==================================================================

    def _score_action(
        self,
        event: Dict[str, Any],
        target: Dict[str, Any],
        debate: Dict[str, Any],
        market_check: Dict[str, Any],
        allow_rejected: bool = False,
        enable_reacted_continuation: bool = True,
    ) -> Optional[Dict[str, Any]]:
        return self._build_shock_signal(
            event=event,
            target=target,
            debate=debate,
            reaction=market_check.get("reaction", {}),
            trigger_score=event.get("trigger_score", 0.0),
            market_check_score=market_check.get("market_check_score", 0.0),
            allow_rejected=allow_rejected,
            enable_reacted_continuation=enable_reacted_continuation,
        )

    def run(
        self,
        stock_codes: List[str],
        max_events: int = 5,
        skip_debate: bool = False,
        downstream_limit: Optional[int] = None,
        allow_rejected: bool = False,
        enable_reacted_continuation: bool = True,
    ) -> List[Dict[str, Any]]:
        """Execute the full shock propagation pipeline.

        Parameters
        ----------
        stock_codes :
            Universe of stock codes to monitor for events.
        max_events :
            Maximum events to process.
        skip_debate :
            If True, skip LLM agent debate (faster, for testing).

        Returns
        -------
        list[dict]
            Each dict is a complete shock signal:
            {source_event, source_code, target_code, target_name,
             shock_weight, hop, path, consensus_direction, divergence,
             conviction, reacted, return_5d, signal_direction, confidence}
        """
        t0 = time.time()
        logger.info("=" * 60)
        logger.info("Shock Pipeline — %d stocks", len(stock_codes))
        logger.info("=" * 60)

        # Step 1: Detect events
        events = self.detect_events(stock_codes, max_events=max_events)
        if not events:
            logger.info("No events detected — pipeline complete (0 signals)")
            return []

        self._refresh_graph_snapshot()

        all_signals: List[Dict[str, Any]] = []

        for event in events:
            trigger = self._build_trigger(event)
            if not trigger["tradable"]:
                logger.info("Event rejected at trigger stage: %s", trigger["trigger_reason"])
                continue

            event = trigger["event"]
            event["trigger_score"] = trigger["trigger_score"]
            source_code = event.get("stock_code", "")
            if not source_code:
                continue

            logger.info(
                "Processing event: [%s] %s (source: %s)",
                event.get("type", "?"), event.get("title", "?")[:50], source_code,
            )

            # Step 2: Propagate shock
            propagation = self._build_propagation(event, downstream_limit=downstream_limit)
            targets = propagation["targets"]
            if not targets:
                logger.info("No downstream targets for %s — skipping", source_code)
                continue

            # Step 3-5: For each downstream target
            for target in targets:
                if not self._passes_research_profile(event.get("event_type", ""), int(target.get("hop", 1))):
                    continue
                target_code = target["code"]

                # Step 3: Agent debate
                if skip_debate:
                    debate = self._debate.run_rule_based(
                        event=event,
                        target=target,
                        context={
                            "trigger_score": event.get("trigger_score", 0.0),
                            "crowding_score": event.get("crowding_score", 0.4),
                            "shock_weight": target.get("shock_weight", 0.0),
                            "expected_holding_days": 5,
                        },
                    )
                else:
                    debate = self.debate_impact(target, event)

                # Step 4: Check price reaction
                market_check = self._build_market_check(event, target_code)

                # Step 5: Generate signal
                signal = self._score_action(
                    event,
                    target,
                    debate,
                    market_check,
                    allow_rejected=allow_rejected,
                    enable_reacted_continuation=enable_reacted_continuation,
                )

                if signal is not None:
                    all_signals.append(signal)

        elapsed = time.time() - t0
        logger.info(
            "Shock Pipeline complete — %d signals from %d events in %.1fs",
            len(all_signals), len(events), elapsed,
        )
        return all_signals

    # ==================================================================
    # Historical Backtesting Mode
    # ==================================================================

    def run_historical(
        self,
        events: List[Dict[str, Any]],
        skip_debate: bool = False,
        forward_days: int = 5,
        downstream_limit: Optional[int] = None,
        include_source_signals: bool = True,
        allow_rejected: bool = False,
        enable_reacted_continuation: bool = True,
    ) -> List[Dict[str, Any]]:
        """Run the shock pipeline on historical events for backtesting.

        Unlike ``run()``, this skips live event detection and instead uses
        a pre-built list of historical events.  Price reactions are measured
        from the event date forward.

        Parameters
        ----------
        events :
            List of historical event dicts.  Each must have at minimum:
            {title, type, stock_code, stock_name, event_date, summary}
            ``event_date`` should be YYYY-MM-DD format.
        skip_debate :
            If True, skip LLM agent debate (faster).
        forward_days :
            Number of trading days after event to measure price reaction.

        Returns
        -------
        list[dict]
            Extended shock signals with historical return fields.
        """
        t0 = time.time()
        logger.info("=" * 60)
        logger.info("Shock Pipeline — HISTORICAL BACKTEST (%d events)", len(events))
        logger.info("=" * 60)

        all_signals: List[Dict[str, Any]] = []

        normalized_events = normalize_events(events)

        for event in normalized_events:
            trigger = self._build_trigger(event)
            event = trigger["event"]
            event["trigger_score"] = trigger["trigger_score"]

            source_code = event.get("stock_code", "")
            event_date = event.get("event_date", "")
            if not source_code or not event_date:
                continue

            self._refresh_graph_snapshot(as_of=event_date)

            logger.info(
                "Processing historical event: [%s] %s (source: %s, date: %s)",
                event.get("type", "?"),
                event.get("title", "?")[:50],
                source_code,
                event_date,
            )

            # Step 2: Propagate shock via graph
            propagation = self._build_propagation(event, downstream_limit=downstream_limit)
            targets = propagation["targets"]
            if not targets:
                logger.info("No downstream targets for %s", source_code)
                targets = []

            # Step 3-5: For each downstream target
            for target in targets:
                if not self._passes_research_profile(event.get("event_type", ""), int(target.get("hop", 1))):
                    continue
                target_code = target["code"]

                # Step 3: Agent debate (optional)
                if skip_debate:
                    debate = self._debate.run_rule_based(
                        event=event,
                        target=target,
                        context={
                            "trigger_score": event.get("trigger_score", 0.0),
                            "crowding_score": event.get("crowding_score", 0.4),
                            "shock_weight": target.get("shock_weight", 0.0),
                            "expected_holding_days": forward_days,
                        },
                    )
                else:
                    debate = self.debate_impact(target, event)

                # Step 4: Check price reaction at event date
                market_check = self._build_market_check(
                    event,
                    target_code,
                    event_date=event_date,
                    historical=True,
                    forward_days=forward_days,
                )

                # Step 5: Generate signal
                signal = self._score_action(
                    event,
                    target,
                    debate,
                    market_check,
                    allow_rejected=allow_rejected,
                    enable_reacted_continuation=enable_reacted_continuation,
                )

                if signal is not None:
                    # Add historical backtest fields
                    reaction = market_check["reaction"]
                    signal["event_date"] = event_date
                    signal["event_id"] = event.get("event_id", "")
                    signal["entry_price"] = reaction.get("entry_price", 0.0)
                    for h in [1, 3, 5, 10, 20]:
                        signal[f"fwd_return_{h}d"] = reaction.get(
                            f"return_{h}d"
                        )
                    all_signals.append(signal)

            # Also check the source stock itself (event origin)
            src_market_check = self._build_market_check(
                event,
                source_code,
                event_date=event_date,
                historical=True,
                forward_days=forward_days,
            )
            src_reaction = src_market_check["reaction"]
            if include_source_signals and self._passes_research_profile(event.get("event_type", ""), 0):
                source_signal = {
                    "source_event": event.get("title", ""),
                    "source_code": source_code,
                    "source_name": event.get("stock_name", ""),
                    "event_type": event.get("type", ""),
                    "target_code": source_code,
                    "target_name": event.get("stock_name", ""),
                    "shock_weight": 1.0,
                    "hop": 0,
                    "propagation_path": source_code,
                    "relation_chain": "SOURCE",
                    "consensus_direction": "bearish" if event.get("type") in (
                        "scandal", "policy_risk", "supply_shortage",
                    ) else "bullish",
                    "consensus_sentiment": 0.0,
                    "divergence": 0.0,
                    "conviction": 1.0,
                    "debate_summary": "事件源头",
                    "reacted": src_reaction.get("reacted", False),
                    "return_5d": src_reaction.get("return_pct", 0.0),
                    "volume_change_5d": src_reaction.get("volume_change_pct", 0.0),
                    "signal_direction": "avoid" if event.get("type") in (
                        "scandal", "policy_risk", "supply_shortage",
                    ) else "long",
                    "confidence": 0.5,
                    "score": round(max(0.0, min(1.0, event.get("trigger_score", 0.5) * 0.8)), 4),
                    "expected_return": 0.0,
                    "alpha_type": "事件源头",
                    "alpha_family": "source",
                    "reacted_continuation": False,
                    "position_hint": "N/A(源头)",
                    "graph_context_used": False,
                    "narrative_tags": list(event.get("narrative_tags", [])),
                    "narrative_phase": event.get("narrative_phase", ""),
                    "reasoning": f"[源头事件] {event.get('title', '')[:40]} | 相位={event.get('narrative_phase', '')}",
                    "trigger_score": event.get("trigger_score", 0.0),
                    "propagation_score": 1.0,
                    "graph_score": 1.0,
                    "graph_rank_score": 1.0,
                    "path_quality": 1.0,
                    "relation_score": 1.0,
                    "specificity_score": 1.0,
                    "market_check_score": src_market_check.get("market_check_score", 0.0),
                    "pipeline_stage": "action",
                    "action": "open_long" if event.get("type") not in (
                        "scandal", "policy_risk", "supply_shortage",
                    ) else "avoid",
                    "event_date": event_date,
                    "event_id": event.get("event_id", ""),
                    "entry_price": src_reaction.get("entry_price", 0.0),
                }
                for h in [1, 3, 5, 10, 20]:
                    source_signal[f"fwd_return_{h}d"] = src_reaction.get(
                        f"return_{h}d"
                    )
                all_signals.append(source_signal)

        elapsed = time.time() - t0
        logger.info(
            "Historical backtest complete — %d signals from %d events in %.1fs",
            len(all_signals), len(events), elapsed,
        )
        return all_signals

    def _build_shock_signal(
        self,
        event: Dict,
        target: Dict,
        debate: Dict,
        reaction: Dict,
        trigger_score: float = 0.0,
        market_check_score: float = 0.0,
        allow_rejected: bool = False,
        enable_reacted_continuation: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Build a final shock signal from pipeline components."""
        reacted = reaction.get("reacted", False)
        divergence = debate.get("divergence", 0.0)
        consensus = debate.get("consensus_direction", "neutral")
        scenario_probs = debate.get("scenario_probs", {})
        consensus_sentiment = (
            scenario_probs.get("bullish", 0.0) - scenario_probs.get("bearish", 0.0)
            if isinstance(scenario_probs, dict) else 0.0
        )

        # Core logic: Alpha = unreacted downstream
        event_type = event.get("type", "")
        hop = int(target.get("hop", 0))
        reacted_continuation = self._allows_reacted_continuation(
            event_type=event_type,
            hop=hop,
            market_check_score=market_check_score,
            enable_reacted_continuation=enable_reacted_continuation,
        )
        if reacted_continuation:
            alpha_type = "已反应(趋势延续)"
            alpha_family = "continuation"
        elif reacted:
            alpha_type = "已反应"
            alpha_family = "rejected_reacted"
        else:
            alpha_type = "未反应(信息差)"
            alpha_family = "info_gap"

        # ── Direction: 纯规则映射（基于Iteration 10回测验证）──
        # 不使用Agent辩论结果作为direction输入
        # A股方向映射表（回测验证）
        _AVOID_EVENTS = {"scandal", "policy_risk", "management_change",
                         "product_launch", "technology_breakthrough",
                         "price_adjustment", "buyback"}
        _LONG_EVENTS = {"cooperation", "earnings_surprise",
                        "supply_shortage", "order_win"}

        if event_type in _AVOID_EVENTS:
            direction = "avoid"
        elif event_type in _LONG_EVENTS:
            direction = "long"
        else:
            direction = "avoid"  # 默认保守

        # ── Confidence & action由统一 SignalFactory 计算 ──
        narratives = list(event.get("narrative_tags", []))
        market_check = {
            "reaction": reaction,
            "market_check_score": market_check_score,
            "crowding_score": float(event.get("crowding_score", 0.4)),
            "gap_risk": min(1.0, abs(float(reaction.get("return_pct", 0.0))) * 10.0),
            "tradable": market_check_score >= 0.35 and (not reacted or reacted_continuation),
            "reaction_label": "已反应" if reacted else "未反应",
        }
        unified = self._signal_factory.build(
            event=event,
            target=target,
            debate=debate,
            market_check=market_check,
            narratives=narratives,
            allow_rejected=allow_rejected,
        )
        if unified is None:
            return None
        action = unified["action"]
        confidence = unified["confidence"]

        # Divergence-based position sizing hint
        if divergence > _HIGH_DIVERGENCE:
            position_hint = "轻仓(高分歧)"
        elif divergence < _LOW_DIVERGENCE:
            position_hint = "重仓(强共识)"
        else:
            position_hint = "标准仓位"

        path_str = " → ".join(target.get("path", []))
        rel_str = " → ".join(target.get("relation_chain", []))

        return {
            # Event info
            "source_event": event.get("title", ""),
            "source_code": event.get("stock_code", ""),
            "source_name": event.get("stock_name", ""),
            "event_type": event.get("type", ""),
            # Target info
            "target_code": target["code"],
            "target_name": target["name"],
            # Propagation
            "shock_weight": target["shock_weight"],
            "hop": target["hop"],
            "propagation_path": path_str,
            "relation_chain": rel_str,
            "graph_score": round(float(target.get("graph_score", target["shock_weight"])), 4),
            "graph_rank_score": round(float(target.get("graph_rank_score", target.get("graph_score", target["shock_weight"]))), 4),
            "path_quality": round(float(target.get("path_quality", target["shock_weight"])), 4),
            "relation_score": round(float(target.get("relation_score", 0.0)), 4),
            "specificity_score": round(float(target.get("specificity_score", 0.0)), 4),
            # Debate (保留用于展示，不影响direction)
            "consensus_direction": consensus,
            "consensus_sentiment": consensus_sentiment,
            "divergence": divergence,
            "conviction": debate.get("conviction", 0.0),
            "debate_summary": debate.get("debate_summary", ""),
            "evidence_density": debate.get("evidence_density", 0.0),
            "scenario_probs": debate.get("scenario_probs", {}),
            "invalidators": debate.get("invalidators", []),
            # Price reaction
            "reacted": reacted,
            "return_5d": reaction.get("return_pct", 0.0),
            "volume_change_5d": reaction.get("volume_change_pct", 0.0),
            # 涨跌停标志 (T+1开盘触及涨跌停)
            "hit_limit_up": reaction.get("hit_limit_up", False),
            "hit_limit_down": reaction.get("hit_limit_down", False),
            # Signal
            "signal_direction": direction,
            "action": action,
            "emittable": bool(unified.get("emittable", True)),
            "confidence": round(confidence, 4),
            "score": unified["score"],
            "expected_return": unified["expected_return"],
            "expected_holding_days": unified["expected_holding_days"],
            "alpha_type": alpha_type,
            "alpha_family": alpha_family,
            "reacted_continuation": reacted_continuation,
            "position_hint": position_hint,
            "trigger_score": round(trigger_score, 4),
            "trigger_strength": unified["trigger_strength"],
            "propagation_score": unified["propagation_score"],
            "debate_score": unified["debate_score"],
            "market_check_score": round(market_check_score, 4),
            "risk_penalty": unified["risk_penalty"],
            "reasoning": unified["reasoning"],
            "narrative_tags": list(event.get("narrative_tags", [])),
            "narrative_phase": event.get("narrative_phase", ""),
            "pipeline_stage": "action",
            # Graph context
            "graph_context_used": debate.get("graph_context_used", False),
        }

    # ==================================================================
    # Convert to StrategySignal for compatibility
    # ==================================================================

    def to_strategy_signals(
        self,
        shock_signals: List[Dict[str, Any]],
    ) -> List[StrategySignal]:
        """Convert shock pipeline outputs to StrategySignal objects."""
        results: List[StrategySignal] = []
        for s in shock_signals:
            direction = s.get("signal_direction", "neutral")
            if direction == "neutral":
                continue

            confidence = s.get("confidence", 0.0)
            expected_return = s.get("expected_return", s.get("consensus_sentiment", 0.0) * 0.05)
            reasoning = s.get("reasoning") or (
                f"[冲击传播] {s.get('source_name', '')}({s.get('source_code', '')})"
                f" → {s.get('propagation_path', '')}"
                f" | 冲击权重:{s.get('shock_weight', 0):.2f}"
                f" | Agent共识:{s.get('consensus_direction', '')}"
                f" | 分歧度:{s.get('divergence', 0):.2f}"
                f" | {s.get('alpha_type', '')}"
                f" | {s.get('position_hint', '')}"
            )

            sig = StrategySignal(
                strategy_name="shock_propagation",
                stock_code=s["target_code"],
                stock_name=s.get("target_name", s["target_code"]),
                direction=direction,
                confidence=confidence,
                expected_return=round(expected_return, 4),
                holding_period_days=10,
                reasoning=reasoning,
                metadata=s,
            )
            results.append(sig)

        return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main():
    import argparse
    parser = argparse.ArgumentParser(description="冲击传播链路 — 信息差 Alpha")
    parser.add_argument(
        "--stocks", type=str, default="",
        help="逗号分隔的股票代码，留空=使用CSI-300前30",
    )
    parser.add_argument("--max-events", type=int, default=5)
    parser.add_argument(
        "--skip-debate", action="store_true",
        help="跳过LLM Agent辩论（快速测试）",
    )
    parser.add_argument(
        "--output-dir", type=str, default="",
        help="输出目录",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Resolve stock universe
    if args.stocks:
        stock_codes = [c.strip() for c in args.stocks.split(",") if c.strip()]
    else:
        try:
            import akshare as ak
            df = ak.index_stock_cons_csindex(symbol="000300")
            stock_codes = df["成分券代码"].head(30).tolist()
            logger.info("Using CSI-300 top 30 stocks")
        except Exception:
            stock_codes = [
                "600519", "000858", "601318", "600036", "000001",
                "601166", "600276", "000333", "002415", "600900",
            ]
            logger.info("Using hardcoded 10 stocks (fallback)")

    # Run pipeline
    pipeline = ShockPipeline()
    shock_signals = pipeline.run(
        stock_codes,
        max_events=args.max_events,
        skip_debate=args.skip_debate,
    )

    # Convert to StrategySignal
    strategy_signals = pipeline.to_strategy_signals(shock_signals)

    # Save results
    output_dir = Path(args.output_dir) if args.output_dir else (
        Path(__file__).resolve().parent / ".data" / "reports"
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(tz=_CST).strftime("%Y%m%d")

    # Save raw shock signals as JSON
    raw_path = output_dir / f"shock_signals_{date_str}.json"
    raw_path.write_text(
        json.dumps(shock_signals, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    # Print summary
    print()
    print("=" * 60)
    print("冲击传播链路 (Shock Pipeline) — 结果")
    print("=" * 60)
    print(f"  检测事件数: {len(set(s.get('source_event', '') for s in shock_signals))}")
    print(f"  下游冲击信号: {len(shock_signals)}")
    print(f"  有效信号(非neutral): {len(strategy_signals)}")

    unreacted = [s for s in shock_signals if not s.get("reacted", True)]
    print(f"  未反应(信息差): {len(unreacted)}")

    if shock_signals:
        print()
        print("  Top 信号:")
        sorted_sigs = sorted(shock_signals, key=lambda s: s.get("confidence", 0), reverse=True)
        for i, s in enumerate(sorted_sigs[:10]):
            print(
                f"    {i+1}. {s.get('target_name', '')}({s.get('target_code', '')}) "
                f"| 方向:{s.get('signal_direction', '')} "
                f"| 信心:{s.get('confidence', 0):.2f} "
                f"| 分歧:{s.get('divergence', 0):.2f} "
                f"| 冲击:{s.get('shock_weight', 0):.2f} "
                f"| {'⚡未反应' if not s.get('reacted') else '✓已反应'} "
                f"| 来源:{s.get('source_event', '')[:30]}"
            )

    print()
    print(f"  原始信号: {raw_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
