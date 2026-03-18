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

        # Entry price = close on event day (or closest trading day)
        entry_price = float(df.loc[event_idx, close_col])
        if entry_price == 0:
            return self._empty_price_result()

        # Calculate returns at multiple horizons
        after_event_data = df.loc[event_idx:]
        returns = {}
        for horizon in [1, 3, 5, 10, 20]:
            if len(after_event_data) > horizon:
                exit_price = float(after_event_data.iloc[horizon][close_col])
                returns[f"return_{horizon}d"] = round(
                    exit_price / entry_price - 1.0, 4
                )
            else:
                returns[f"return_{horizon}d"] = None

        main_return = returns.get(f"return_{forward_days}d", 0.0) or 0.0

        # Volume change
        volume_change = 0.0
        if vol_col and vol_col in df.columns and len(after_event_data) > 1:
            try:
                vol_event = float(df.loc[event_idx, vol_col])
                # Average volume in forward window
                fwd_vols = after_event_data[vol_col].iloc[1:min(forward_days+1, len(after_event_data))]
                if not fwd_vols.empty and vol_event > 0:
                    volume_change = float(fwd_vols.mean()) / vol_event - 1.0
            except (ValueError, TypeError):
                pass

        reacted = abs(main_return) > _REACTION_THRESHOLD
        result = {
            "reacted": reacted,
            "return_pct": round(main_return, 4),
            "volume_change_pct": round(volume_change, 4),
            "entry_price": entry_price,
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
            "return_1d": None,
            "return_3d": None,
            "return_5d": None,
            "return_10d": None,
            "return_20d": None,
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

    # ==================================================================
    # Historical Backtesting Mode
    # ==================================================================

    def run_historical(
        self,
        events: List[Dict[str, Any]],
        skip_debate: bool = False,
        forward_days: int = 5,
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

        for event in events:
            source_code = event.get("stock_code", "")
            event_date = event.get("event_date", "")
            if not source_code or not event_date:
                continue

            logger.info(
                "Processing historical event: [%s] %s (source: %s, date: %s)",
                event.get("type", "?"),
                event.get("title", "?")[:50],
                source_code,
                event_date,
            )

            # Step 2: Propagate shock via graph
            targets = self.propagate(source_code, event)
            if not targets:
                logger.info("No downstream targets for %s", source_code)
                continue

            # Step 3 & 4: For each downstream target
            for target in targets:
                target_code = target["code"]

                # Step 3: Agent debate (optional)
                if skip_debate:
                    debate = self._empty_debate_result()
                else:
                    debate = self.debate_impact(target, event)

                # Step 4: Check price reaction at event date
                reaction = self.check_price_reaction_at_date(
                    target_code,
                    event_date,
                    forward_days=forward_days,
                )

                # Step 5: Generate signal
                signal = self._build_shock_signal(
                    event=event,
                    target=target,
                    debate=debate,
                    reaction=reaction,
                )

                if signal is not None:
                    # Add historical backtest fields
                    signal["event_date"] = event_date
                    signal["event_id"] = event.get("event_id", "")
                    signal["entry_price"] = reaction.get("entry_price", 0.0)
                    for h in [1, 3, 5, 10, 20]:
                        signal[f"fwd_return_{h}d"] = reaction.get(
                            f"return_{h}d"
                        )
                    all_signals.append(signal)

            # Also check the source stock itself (event origin)
            src_reaction = self.check_price_reaction_at_date(
                source_code, event_date, forward_days=forward_days,
            )
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
                "alpha_type": "事件源头",
                "position_hint": "N/A(源头)",
                "graph_context_used": False,
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
    ) -> Optional[Dict[str, Any]]:
        """Build a final shock signal from pipeline components."""
        reacted = reaction.get("reacted", False)
        divergence = debate.get("divergence", 0.0)
        consensus = debate.get("consensus_direction", "neutral")
        consensus_sentiment = debate.get("consensus_sentiment", 0.0)

        # Core logic: Alpha = unreacted downstream
        if reacted:
            alpha_type = "已反应"
        else:
            alpha_type = "未反应(信息差)"

        # ── Direction: 纯规则映射（基于Iteration 10回测验证）──
        # 不使用Agent辩论结果作为direction输入
        event_type = event.get("type", "")

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

        # ── Confidence: 基于可验证的硬指标 ──
        # 不使用 conviction（来自Agent辩论）
        hop = target.get("hop", 1)

        confidence = 0.3  # 基础分

        # 事件类型加成（基于回测胜率）
        _EVENT_TYPE_CONFIDENCE = {
            "scandal": 0.25,         # 回测胜率88.9%
            "policy_risk": 0.10,     # 57.1%
            "cooperation": 0.10,
            "earnings_surprise": 0.15,
            "supply_shortage": 0.10,
            "product_launch": 0.15,  # 利好出尽
            "technology_breakthrough": 0.10,
        }
        confidence += _EVENT_TYPE_CONFIDENCE.get(event_type, 0.05)

        # 跳数加成（回测显示下游信号优于源头）
        if hop >= 1:
            confidence += 0.10
        if hop >= 2:
            confidence += 0.05

        # 未反应加成
        if not reacted:
            confidence += 0.15

        confidence = max(0.1, min(0.9, confidence))

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
            # Debate (保留用于展示，不影响direction)
            "consensus_direction": consensus,
            "consensus_sentiment": consensus_sentiment,
            "divergence": divergence,
            "conviction": debate.get("conviction", 0.0),
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
