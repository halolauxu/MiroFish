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

from astrategy.graph.local_store import LocalGraphStore
from astrategy.graph.topology import TopologyAnalyzer
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
    ) -> None:
        self._max_hops = max_hops
        self._decay = decay
        self._max_downstream = max_downstream

        # Load graph
        self._store = LocalGraphStore()
        self._graph_loaded = self._store.load("supply_chain")
        if not self._graph_loaded:
            # Try alternative graph name
            self._graph_loaded = self._store.load("astrategy")
        if self._graph_loaded:
            logger.info("Graph loaded for shock propagation")
        else:
            logger.warning("No graph data available — shock propagation disabled")

        self._edges: List[Dict] = []
        self._nodes: List[Dict] = []
        self._code_to_name: Dict[str, str] = {}
        self._name_to_code: Dict[str, str] = {}

        if self._graph_loaded:
            graph_id = "supply_chain" if "supply_chain" in self._store._graphs else "astrategy"
            self._edges = self._store.get_all_edges(graph_id)
            self._nodes = self._store.get_all_nodes(graph_id)
            self._build_code_name_maps()
            logger.info(
                "Graph stats: %d nodes, %d edges",
                len(self._nodes), len(self._edges),
            )

    def _build_code_name_maps(self) -> None:
        """Build bidirectional code↔name lookup from graph nodes."""
        import re
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

    # ==================================================================
    # Step 1: Event Detection
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

    # ==================================================================
    # Step 2: Graph Shock Propagation
    # ==================================================================

    def propagate(
        self,
        source_code: str,
        event: Dict[str, Any],
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
            targets.append({
                "code": code,
                "name": display_name,
                "shock_weight": info["shock_weight"],
                "hop": info["hop"],
                "path": info["path"],
                "relation_chain": info["relation_chain"],
                "source_event": event.get("title", ""),
                "source_code": source_code,
            })

        # Sort by shock_weight descending, take top N
        targets.sort(key=lambda t: t["shock_weight"], reverse=True)
        targets = targets[:self._max_downstream]

        logger.info(
            "Shock from %s → %d downstream targets (top: %s)",
            source_code, len(targets),
            ", ".join(f"{t['code']}({t['shock_weight']:.2f})" for t in targets[:5]),
        )
        return targets

    # ==================================================================
    # Step 3: Agent Debate (multi-agent divergence)
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
            return self._empty_debate_result()

        # --- Compute divergence ---
        sentiments = [r.get("sentiment_score", 0.0) for r in all_reactions]
        if len(sentiments) >= 2:
            mean_s = sum(sentiments) / len(sentiments)
            variance = sum((s - mean_s) ** 2 for s in sentiments) / len(sentiments)
            divergence = math.sqrt(variance)
        else:
            divergence = 0.0

        # Weighted consensus (using market influence weights)
        weighted_sum = 0.0
        total_weight = 0.0
        for r in all_reactions:
            arch = r.get("archetype", "")
            w = _INFLUENCE_WEIGHTS.get(arch, 0.05)
            weighted_sum += r.get("sentiment_score", 0.0) * w
            total_weight += w

        consensus_sentiment = weighted_sum / total_weight if total_weight > 0 else 0.0

        # Conviction: inverse of divergence, scaled
        conviction = max(0.0, 1.0 - divergence * 2.0)

        # Consensus direction
        if consensus_sentiment > 0.1:
            consensus_direction = "bullish"
        elif consensus_sentiment < -0.1:
            consensus_direction = "bearish"
        else:
            consensus_direction = "neutral"

        # Build debate summary
        debate_lines = []
        for r in all_reactions:
            arch = r.get("archetype", "")
            action = r.get("action", "hold")
            sent = r.get("sentiment_score", 0.0)
            reason = r.get("reasoning", "")[:60]
            debate_lines.append(f"[{arch}] {action}({sent:+.2f}): {reason}")

        return {
            "consensus_direction": consensus_direction,
            "consensus_sentiment": round(consensus_sentiment, 4),
            "divergence": round(divergence, 4),
            "conviction": round(conviction, 4),
            "agent_reactions": all_reactions,
            "debate_summary": "\n".join(debate_lines),
            "graph_context_used": True,
            "propagation_path": target["path"],
            "relation_chain": target["relation_chain"],
        }

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
    # Step 4: Price Reaction Detection
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

    # ==================================================================
    # Step 5: Full Pipeline
    # ==================================================================

    def run(
        self,
        stock_codes: List[str],
        max_events: int = 5,
        skip_debate: bool = False,
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

        all_signals: List[Dict[str, Any]] = []

        for event in events:
            source_code = event.get("stock_code", "")
            if not source_code:
                continue

            logger.info(
                "Processing event: [%s] %s (source: %s)",
                event.get("type", "?"), event.get("title", "?")[:50], source_code,
            )

            # Step 2: Propagate shock
            targets = self.propagate(source_code, event)
            if not targets:
                logger.info("No downstream targets for %s — skipping", source_code)
                continue

            # Step 3 & 4: For each downstream target
            for target in targets:
                target_code = target["code"]

                # Step 3: Agent debate
                if skip_debate:
                    debate = self._empty_debate_result()
                else:
                    debate = self.debate_impact(target, event)

                # Step 4: Check price reaction
                reaction = self.check_price_reaction(target_code)

                # Step 5: Generate signal
                signal = self._build_shock_signal(
                    event=event,
                    target=target,
                    debate=debate,
                    reaction=reaction,
                )

                if signal is not None:
                    all_signals.append(signal)

        elapsed = time.time() - t0
        logger.info(
            "Shock Pipeline complete — %d signals from %d events in %.1fs",
            len(all_signals), len(events), elapsed,
        )
        return all_signals

    def _build_shock_signal(
        self,
        event: Dict,
        target: Dict,
        debate: Dict,
        reaction: Dict,
    ) -> Optional[Dict[str, Any]]:
        """Build a final shock signal from pipeline components."""
        reacted = reaction.get("reacted", False)
        conviction = debate.get("conviction", 0.0)
        divergence = debate.get("divergence", 0.0)
        consensus = debate.get("consensus_direction", "neutral")
        consensus_sentiment = debate.get("consensus_sentiment", 0.0)

        # Core logic: Alpha = unreacted downstream + agent consensus
        if reacted:
            alpha_type = "已反应"
            confidence_penalty = 0.5
        else:
            alpha_type = "未反应(信息差)"
            confidence_penalty = 1.0

        # Direction from agent debate
        if consensus == "bullish":
            direction = "long"
        elif consensus == "bearish":
            direction = "avoid"
        elif consensus == "neutral" and conviction == 0.0:
            # No debate ran (skip_debate mode) — infer from event type
            event_type = event.get("type", "")
            rel_chain = target.get("relation_chain", [])
            is_negative_event = event_type in (
                "scandal", "policy_risk", "supply_shortage", "production_cut",
            )
            has_compete = any("COMPETES" in r for r in rel_chain)

            if is_negative_event and has_compete:
                # Competitor in trouble → potential benefit
                direction = "long"
                conviction = 0.4
            elif is_negative_event:
                # Supply chain partner in trouble → negative impact
                direction = "avoid"
                conviction = 0.35
            else:
                direction = "long"
                conviction = 0.3
        else:
            direction = "neutral"

        # Skip neutral with low conviction
        if direction == "neutral" and conviction < _MIN_CONVICTION:
            return None

        # Confidence = shock_weight × conviction × reaction_penalty
        shock_w = target.get("shock_weight", 0.0)
        confidence = shock_w * conviction * confidence_penalty
        confidence = max(0.05, min(1.0, confidence))

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
            # Debate
            "consensus_direction": consensus,
            "consensus_sentiment": consensus_sentiment,
            "divergence": divergence,
            "conviction": conviction,
            "debate_summary": debate.get("debate_summary", ""),
            # Price reaction
            "reacted": reacted,
            "return_5d": reaction.get("return_pct", 0.0),
            "volume_change_5d": reaction.get("volume_change_pct", 0.0),
            # Signal
            "signal_direction": direction,
            "confidence": round(confidence, 4),
            "alpha_type": alpha_type,
            "position_hint": position_hint,
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
            consensus_sentiment = s.get("consensus_sentiment", 0.0)
            expected_return = consensus_sentiment * 0.05  # rough estimate

            reasoning = (
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
