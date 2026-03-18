"""
Graph-Enhanced Multi-Factor Strategy (S07)
==========================================

Combines graph-based factors derived from the A-share knowledge graph
with traditional quantitative factors to produce composite stock rankings.

**Graph factors** (computed from Zep graph nodes/edges):
  1. supply_chain_centrality  -- PageRank on SUPPLIES_TO edges
  2. institution_concentration -- distinct institutions via HOLDS_SHARES
  3. concept_heat              -- sum of heat scores on related Concepts
  4. event_exposure            -- count of recent Event nodes via TRIGGERS
  5. industry_leadership       -- company market_cap / industry total market_cap
  6. peer_return_gap           -- avg(peer returns) - self return (catch-up)

**Traditional factors** (computed from akshare market/fundamental data):
  1. momentum_20d   -- 20-day price momentum
  2. reversal_5d    -- 5-day reversal (mean reversion signal)
  3. volatility_20d -- 20-day realised volatility
  4. turnover_rate  -- average daily turnover
  5. pe_percentile  -- PE ratio percentile within industry
  6. roe            -- latest return on equity

All factors are normalised to cross-sectional z-scores.  A weighted
composite score ranks the universe and produces long/short/neutral signals.

No LLM calls -- pure computation.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from astrategy.data_collector.fundamental import FundamentalCollector
from astrategy.data_collector.market_data import MarketDataCollector
from astrategy.strategies.base import BaseStrategy, StrategySignal

# Lazy import — TopologyAnalyzer doesn't need zep_cloud
try:
    from astrategy.graph.topology import TopologyAnalyzer
except ImportError:
    TopologyAnalyzer = None  # type: ignore[assignment,misc]

logger = logging.getLogger("astrategy.strategies.s07_graph_factors")

_CST = timezone(timedelta(hours=8))

# ---------------------------------------------------------------------------
# Default factor weights (graph + traditional)
# ---------------------------------------------------------------------------
DEFAULT_WEIGHTS: Dict[str, float] = {
    # Graph factors — weights are scaled dynamically by graph coverage ratio
    # (see compute_composite_score: if coverage < 50%, graph weight is reduced)
    # Weights optimised via coordinate descent (2026-03-17, quick mode, 30 stocks)
    "supply_chain_centrality": 1.5,
    "betweenness_centrality": 1.0,
    "institution_concentration": -1.0,  # contrarian: fewer institutions = less crowded
    "concept_heat": -1.5,              # contrarian: cooler concepts = less priced-in
    "event_exposure": 0.5,
    "industry_leadership": 0.8,
    "peer_return_gap": -2.0,           # outperformers, not catch-up
    # Traditional factors
    "momentum_20d": 1.0,
    "reversal_5d": 1.0,
    "volatility_20d": 3.0,            # high vol = momentum opportunity
    "turnover_rate": 1.0,
    "pe_percentile": -1.0,            # lower PE percentile preferred (value tilt)
    "roe": 1.0,
}

# Graph factor names (subject to coverage scaling)
_GRAPH_FACTOR_COLS = [
    "supply_chain_centrality",
    "betweenness_centrality",
    "institution_concentration",
    "concept_heat",
    "event_exposure",
    "industry_leadership",
    "peer_return_gap",
]


class GraphFactorsStrategy(BaseStrategy):
    """
    Graph-enhanced multi-factor strategy.

    Merges structural information from the knowledge graph with standard
    quantitative factors.  Ranks stocks by a composite z-score and emits
    long signals for top-ranked names and short signals for bottom-ranked.

    Parameters
    ----------
    weights : dict[str, float] | None
        Factor weight overrides.  Keys must match factor names listed in
        ``DEFAULT_WEIGHTS``.  Missing keys fall back to the default.
    top_n : int
        Number of top (long) and bottom (short) stocks to signal.
    holding_days : int
        Assumed holding period for emitted signals.
    lookback_days : int
        Calendar days of history to fetch for traditional factors.
    event_recency_days : int
        Only count Event nodes whose ``event_date`` falls within this many
        calendar days of the evaluation date.
    """

    def __init__(
        self,
        weights: Dict[str, float] | None = None,
        top_n: int = 8,
        holding_days: int = 20,
        lookback_days: int = 60,
        event_recency_days: int = 30,
        signal_dir=None,
    ) -> None:
        super().__init__(signal_dir=signal_dir)

        self._weights = {**DEFAULT_WEIGHTS}
        if weights:
            self._weights.update(weights)

        self._top_n = top_n
        self._holding_days = holding_days
        self._lookback_days = lookback_days
        self._event_recency_days = event_recency_days

        self._market = MarketDataCollector()
        self._fundamental = FundamentalCollector()

    # ── BaseStrategy interface ────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "graph_factors"

    def run(self, stock_codes: list[str] | None = None) -> list[StrategySignal]:
        """Run the multi-factor strategy across a universe of stocks.

        If *stock_codes* is ``None`` the strategy falls back to an empty
        list (the caller should provide the universe).

        The method:
        1. Fetches graph data from the configured Zep graph.
        2. Computes graph factors and traditional factors.
        3. Builds a composite score and emits signals.
        """
        if not stock_codes:
            logger.warning("No stock codes provided; returning empty signals.")
            return []

        end_date = datetime.now(tz=_CST).strftime("%Y%m%d")

        # -- Fetch graph data ---------------------------------------------------
        graph_nodes, graph_edges = self._fetch_graph_data()

        # -- Compute factor DataFrames ------------------------------------------
        gf = self.compute_graph_factors(graph_nodes, graph_edges, stock_codes)
        tf = self.compute_traditional_factors(stock_codes, end_date)

        # -- Composite score ----------------------------------------------------
        composite = self.compute_composite_score(gf, tf)

        # -- Generate signals ---------------------------------------------------
        signals = self._composite_to_signals(composite, stock_codes)
        return signals

    def run_single(self, stock_code: str) -> list[StrategySignal]:
        """Run the strategy for a single stock.

        Because factor z-scores require cross-sectional context this method
        fetches industry peers from the graph and scores the target stock
        within that peer group.
        """
        graph_nodes, graph_edges = self._fetch_graph_data()

        # Find peers via BELONGS_TO edges (same industry)
        peers = self._find_industry_peers(stock_code, graph_nodes, graph_edges)
        universe = list({stock_code} | set(peers))

        end_date = datetime.now(tz=_CST).strftime("%Y%m%d")
        gf = self.compute_graph_factors(graph_nodes, graph_edges, universe)
        tf = self.compute_traditional_factors(universe, end_date)
        composite = self.compute_composite_score(gf, tf)

        return self._composite_to_signals(composite, [stock_code])

    # =====================================================================
    # Graph factor computation
    # =====================================================================

    def compute_graph_factors(
        self,
        graph_nodes: List[Dict[str, Any]],
        graph_edges: List[Dict[str, Any]],
        stock_codes: list[str] | None = None,
    ) -> pd.DataFrame:
        """Compute all graph-based factors and return a DataFrame indexed by stock_code.

        Parameters
        ----------
        graph_nodes : list[dict]
            Node dicts as returned by ``GraphBuilder.get_all_nodes``.
        graph_edges : list[dict]
            Edge dicts as returned by ``GraphBuilder.get_all_edges``.
        stock_codes : list[str] | None
            If given, limit output to these codes.  Otherwise all Company
            nodes found in the graph are included.

        Returns
        -------
        pd.DataFrame
            Columns: supply_chain_centrality, institution_concentration,
            concept_heat, event_exposure, industry_leadership, peer_return_gap.
            Index: ``stock_code``.
        """
        # Build lookup maps
        code_to_name, name_to_code = self._build_code_name_maps(graph_nodes)
        all_codes = list(code_to_name.keys())
        if stock_codes:
            all_codes = [c for c in all_codes if c in set(stock_codes)]

        # If no graph data, return empty DataFrame with correct columns
        factor_cols = [
            "supply_chain_centrality",
            "betweenness_centrality",
            "institution_concentration",
            "concept_heat",
            "event_exposure",
            "industry_leadership",
            "peer_return_gap",
        ]
        if not all_codes:
            # Use stock_codes as index even if graph is empty
            idx = stock_codes or []
            df = pd.DataFrame(0.0, index=idx, columns=factor_cols)
            df.index.name = "stock_code"
            return df

        records: Dict[str, Dict[str, float]] = {c: {} for c in all_codes}

        # 1) supply_chain_centrality -- PageRank on SUPPLIES_TO sub-graph
        self._compute_supply_chain_centrality(
            graph_nodes, graph_edges, code_to_name, name_to_code, records,
        )

        # 1b) betweenness_centrality -- broker/connector role in full graph
        self._compute_betweenness_centrality(
            graph_nodes, graph_edges, code_to_name, records,
        )

        # 2) institution_concentration -- distinct HOLDS_SHARES sources
        self._compute_institution_concentration(
            graph_edges, name_to_code, records,
        )

        # 3) concept_heat -- sum of heat/hot_rank for linked Concepts
        self._compute_concept_heat(
            graph_nodes, graph_edges, name_to_code, records,
        )

        # 4) event_exposure -- count of recent TRIGGERS edges
        self._compute_event_exposure(
            graph_nodes, graph_edges, name_to_code, records,
        )

        # 5) industry_leadership -- market_cap share within industry
        self._compute_industry_leadership(
            graph_nodes, graph_edges, code_to_name, name_to_code, records,
        )

        # 6) peer_return_gap -- avg peer return - self return
        self._compute_peer_return_gap(
            graph_nodes, graph_edges, code_to_name, name_to_code, records,
        )

        # Assemble DataFrame
        rows = []
        for code in all_codes:
            row = {"stock_code": code}
            for col in factor_cols:
                row[col] = records[code].get(col, 0.0)
            rows.append(row)

        df = pd.DataFrame(rows).set_index("stock_code")

        # Fill codes present in stock_codes but absent from graph
        if stock_codes:
            missing = set(stock_codes) - set(df.index)
            if missing:
                missing_df = pd.DataFrame(
                    0.0, index=sorted(missing), columns=factor_cols,
                )
                missing_df.index.name = "stock_code"
                df = pd.concat([df, missing_df])

        return df

    # ── individual graph factor helpers ───────────────────────────────────

    def _compute_supply_chain_centrality(
        self,
        nodes: List[Dict],
        edges: List[Dict],
        code_to_name: Dict[str, str],
        name_to_code: Dict[str, str],
        records: Dict[str, Dict[str, float]],
    ) -> None:
        """PageRank on the SUPPLIES_TO sub-graph."""
        import re as _re
        _code_pat = _re.compile(r"^\d{6}$")
        # Only keep edges where both endpoints are 6-digit stock codes
        supply_edges = [
            e for e in edges
            if e.get("relation") == "SUPPLIES_TO"
            and _code_pat.match(e.get("source_name", ""))
            and _code_pat.match(e.get("target_name", ""))
        ]
        if not supply_edges:
            for code in records:
                records[code]["supply_chain_centrality"] = 0.0
            return

        # Filter nodes to those involved in supply edges
        involved_names: set[str] = set()
        for e in supply_edges:
            involved_names.add(e.get("source_name", ""))
            involved_names.add(e.get("target_name", ""))
        involved_names.discard("")
        sub_nodes = [n for n in nodes if n.get("name", "") in involved_names]

        pr = TopologyAnalyzer.pagerank(sub_nodes, supply_edges)

        for code in records:
            company_name = code_to_name.get(code, "")
            records[code]["supply_chain_centrality"] = pr.get(company_name, 0.0)

    def _compute_betweenness_centrality(
        self,
        nodes: List[Dict],
        edges: List[Dict],
        code_to_name: Dict[str, str],
        records: Dict[str, Dict[str, float]],
    ) -> None:
        """Betweenness centrality across ALL edge types (SUPPLIES_TO + COMPETES_WITH + COOPERATES_WITH)."""
        # Only use stock-code nodes (filter out Institution nodes etc.)
        import re as _re
        stock_nodes = [n for n in nodes if _re.match(r"^\d{6}$", str(n.get("name", "")))]
        stock_edges = [
            e for e in edges
            if e.get("relation") in ("SUPPLIES_TO", "COMPETES_WITH", "COOPERATES_WITH")
        ]
        if not stock_nodes or not stock_edges:
            for code in records:
                records[code]["betweenness_centrality"] = 0.0
            return

        bc = TopologyAnalyzer.betweenness_centrality(stock_nodes, stock_edges)
        for code in records:
            company_name = code_to_name.get(code, "")
            records[code]["betweenness_centrality"] = bc.get(company_name, 0.0)

    def _compute_institution_concentration(
        self,
        edges: List[Dict],
        name_to_code: Dict[str, str],
        records: Dict[str, Dict[str, float]],
    ) -> None:
        """Count distinct institutions holding each stock via HOLDS_SHARES.

        Uses two strategies:
        1. Count distinct institution sources per stock (original graph approach)
        2. Use edge weight as a proxy for fund-count when source is aggregate data
        """
        holds_edges = [e for e in edges if e.get("relation") == "HOLDS_SHARES"]
        counter: Dict[str, set] = defaultdict(set)
        weight_sum: Dict[str, float] = defaultdict(float)
        for e in holds_edges:
            target_name = e.get("target_name", "")
            target_id = e.get("target", "")
            source_name = e.get("source_name", "")
            # Try name_to_code first, then fall back to target_id directly
            code = name_to_code.get(target_name) or target_id
            if code and code in records:
                counter[code].add(source_name)
                weight_sum[code] += e.get("weight", 0.5)

        # If graph-based counting found nothing for most stocks, use API fallback
        codes_with_zero = [c for c in records if len(counter.get(c, set())) == 0]
        if len(codes_with_zero) > len(records) * 0.5:
            api_counts = self._fetch_fund_holder_counts()
            for code in codes_with_zero:
                if code in api_counts:
                    counter[code] = {f"api_fund_{i}" for i in range(int(api_counts[code]))}

        for code in records:
            records[code]["institution_concentration"] = float(len(counter.get(code, set())))

    @staticmethod
    def _fetch_fund_holder_counts() -> Dict[str, float]:
        """Fetch fund holder counts for all stocks from akshare (one API call)."""
        try:
            import akshare as ak
            df = ak.stock_report_fund_hold(symbol="基金持仓", date="20241231")
            code_col = next((c for c in df.columns if "代码" in c), None)
            count_col = next((c for c in df.columns if "基金家数" in c), None)
            if code_col and count_col:
                result = {}
                for _, row in df.iterrows():
                    code = str(row[code_col]).strip().zfill(6)
                    result[code] = float(row[count_col])
                logger.info("Fetched fund holder counts for %d stocks via API", len(result))
                return result
        except Exception as exc:
            logger.warning("Fund holder count API fallback failed: %s", exc)
        return {}

    def _compute_concept_heat(
        self,
        nodes: List[Dict],
        edges: List[Dict],
        name_to_code: Dict[str, str],
        records: Dict[str, Dict[str, float]],
    ) -> None:
        """Sum heat_score for Concept nodes connected via RELATES_TO_CONCEPT."""
        # Build concept name -> heat_score map
        concept_heat_map: Dict[str, float] = {}
        for n in nodes:
            labels = n.get("labels", [])
            if "Concept" in labels:
                name = n.get("name", "")
                attrs = n.get("attributes", {}) or {}
                # hot_rank: lower is hotter, so invert to a heat score
                hot_rank = attrs.get("hot_rank")
                if hot_rank is not None:
                    try:
                        rank_val = float(hot_rank)
                        concept_heat_map[name] = max(0.0, 100.0 - rank_val)
                    except (ValueError, TypeError):
                        concept_heat_map[name] = 1.0
                else:
                    # If no hot_rank, use a default heat of 1.0
                    concept_heat_map[name] = 1.0

        concept_edges = [e for e in edges if e.get("relation") == "RELATES_TO_CONCEPT"]
        heat_accum: Dict[str, float] = defaultdict(float)
        for e in concept_edges:
            source_name = e.get("source_name", "")
            target_name = e.get("target_name", "")
            code = name_to_code.get(source_name)
            if code and code in records:
                heat_accum[code] += concept_heat_map.get(target_name, 1.0)

        for code in records:
            records[code]["concept_heat"] = heat_accum.get(code, 0.0)

    def _compute_event_exposure(
        self,
        nodes: List[Dict],
        edges: List[Dict],
        name_to_code: Dict[str, str],
        records: Dict[str, Dict[str, float]],
    ) -> None:
        """Count recent Event nodes connected to each company via TRIGGERS."""
        cutoff = (datetime.now(tz=_CST) - timedelta(days=self._event_recency_days)).strftime("%Y%m%d")

        # Build set of recent event names
        recent_events: set[str] = set()
        for n in nodes:
            labels = n.get("labels", [])
            if "Event" in labels:
                attrs = n.get("attributes", {}) or {}
                event_date = str(attrs.get("event_date", ""))
                # If no date available, include it (conservative)
                if not event_date or event_date >= cutoff:
                    recent_events.add(n.get("name", ""))

        triggers_edges = [e for e in edges if e.get("relation") == "TRIGGERS"]
        event_count: Dict[str, int] = defaultdict(int)
        for e in triggers_edges:
            source_name = e.get("source_name", "")
            target_name = e.get("target_name", "")
            # Event TRIGGERS Company
            if source_name in recent_events:
                code = name_to_code.get(target_name)
                if code and code in records:
                    event_count[code] += 1

        for code in records:
            records[code]["event_exposure"] = float(event_count.get(code, 0))

    def _compute_industry_leadership(
        self,
        nodes: List[Dict],
        edges: List[Dict],
        code_to_name: Dict[str, str],
        name_to_code: Dict[str, str],
        records: Dict[str, Dict[str, float]],
    ) -> None:
        """Company market_cap / sum of industry peers market_cap.

        Falls back to akshare stock_individual_info_em when graph data
        lacks BELONGS_TO edges or market_cap attributes.
        """
        # Map company name -> market_cap from graph
        cap_map: Dict[str, float] = {}
        for n in nodes:
            labels = n.get("labels", [])
            if "Company" in labels:
                attrs = n.get("attributes", {}) or {}
                mc = attrs.get("market_cap")
                if mc is not None:
                    try:
                        cap_map[n.get("name", "")] = float(mc)
                    except (ValueError, TypeError):
                        pass

        # Map company name -> industry name via BELONGS_TO
        belongs_edges = [e for e in edges if e.get("relation") == "BELONGS_TO"]
        company_industry: Dict[str, str] = {}
        for e in belongs_edges:
            company_industry[e.get("source_name", "")] = e.get("target_name", "")

        # Also check node 'industry' attribute (fallback from graph construction)
        code_industry: Dict[str, str] = {}
        for n in nodes:
            labels = n.get("labels", [])
            if "Company" not in labels:
                continue
            attrs = n.get("attributes", {}) or {}
            code = str(attrs.get("code", "") or attrs.get("stock_code", ""))
            # Fall back to node name if it looks like a 6-digit stock code
            if not code:
                import re as _re
                node_name = n.get("name", "")
                if _re.match(r"^\d{6}$", node_name):
                    code = node_name
            industry = attrs.get("industry", "")
            if code and industry:
                code_industry[code] = industry

        # API fallback: for target stocks missing cap OR industry, fetch per-stock
        codes_needing_api = [
            c for c in records
            if (c not in code_industry and code_to_name.get(c, "") not in company_industry)
            or (code_to_name.get(c, "") not in cap_map and c not in cap_map)
        ]
        if codes_needing_api:
            self._fill_industry_cap_from_api(
                codes_needing_api, code_to_name, cap_map, code_industry,
            )

        # Unify: company_industry (name-keyed) + code_industry (code-keyed)
        for code, ind in code_industry.items():
            name = code_to_name.get(code, code)
            if name not in company_industry:
                company_industry[name] = ind

        # Group by industry
        industry_companies: Dict[str, List[str]] = defaultdict(list)
        for comp, ind in company_industry.items():
            industry_companies[ind].append(comp)

        for code in records:
            company_name = code_to_name.get(code, "")
            industry = company_industry.get(company_name, "")
            # Also check code-keyed
            if not industry:
                industry = code_industry.get(code, "")
            my_cap = cap_map.get(company_name, 0.0)
            if my_cap <= 0:
                my_cap = cap_map.get(code, 0.0)

            if not industry or my_cap <= 0:
                records[code]["industry_leadership"] = 0.0
                continue

            peers = industry_companies.get(industry, [])
            total_cap = sum(cap_map.get(p, 0.0) for p in peers)
            if total_cap > 0:
                records[code]["industry_leadership"] = my_cap / total_cap
            else:
                records[code]["industry_leadership"] = 0.0

    def _fill_industry_cap_from_api(
        self,
        codes: List[str],
        code_to_name: Dict[str, str],
        cap_map: Dict[str, float],
        code_industry: Dict[str, str],
    ) -> None:
        """Fetch market_cap and industry from stock_individual_info_em (API fallback)."""
        import akshare as ak
        for code in codes[:50]:  # limit to avoid rate-limiting
            try:
                df = ak.stock_individual_info_em(symbol=code)
                if df is None or df.empty:
                    continue
                info = dict(zip(df["item"], df["value"]))
                mc = info.get("总市值")
                industry = info.get("行业", "")
                name = code_to_name.get(code, code)
                if mc is not None:
                    cap_map[name] = float(mc)
                    cap_map[code] = float(mc)
                if industry:
                    code_industry[code] = industry
            except Exception:
                continue

    def _compute_peer_return_gap(
        self,
        nodes: List[Dict],
        edges: List[Dict],
        code_to_name: Dict[str, str],
        name_to_code: Dict[str, str],
        records: Dict[str, Dict[str, float]],
    ) -> None:
        """avg(peer returns) - self return.  Positive = lagging peers => catch-up potential."""
        # Get recent returns from PriceAction nodes or market data
        # First try graph PriceAction data
        return_map: Dict[str, float] = {}
        for n in nodes:
            labels = n.get("labels", [])
            if "PriceAction" in labels:
                attrs = n.get("attributes", {}) or {}
                sc = str(attrs.get("stock_code", ""))
                change = attrs.get("change_pct")
                if sc and change is not None:
                    try:
                        return_map[sc] = float(change)
                    except (ValueError, TypeError):
                        pass

        # If graph doesn't have enough return data, fetch from market data
        codes_needing_return = [c for c in records if c not in return_map]
        if codes_needing_return:
            end_dt = datetime.now(tz=_CST)
            start_dt = end_dt - timedelta(days=30)
            start_str = start_dt.strftime("%Y%m%d")
            end_str = end_dt.strftime("%Y%m%d")
            for code in codes_needing_return:
                try:
                    df = self._market.get_daily_quotes(code, start_str, end_str)
                    if not df.empty and len(df) >= 2:
                        first_close = df.iloc[0]["收盘"]
                        last_close = df.iloc[-1]["收盘"]
                        if first_close > 0:
                            return_map[code] = (last_close - first_close) / first_close * 100.0
                except Exception as exc:
                    logger.debug("Failed to fetch returns for %s: %s", code, exc)

        # Map company -> industry for peer grouping
        belongs_edges = [e for e in edges if e.get("relation") == "BELONGS_TO"]
        company_industry: Dict[str, str] = {}
        for e in belongs_edges:
            src_name = e.get("source_name", "")
            src_code = name_to_code.get(src_name)
            if src_code:
                company_industry[src_code] = e.get("target_name", "")

        # Group codes by industry
        industry_codes: Dict[str, List[str]] = defaultdict(list)
        for c, ind in company_industry.items():
            industry_codes[ind].append(c)

        for code in records:
            industry = company_industry.get(code, "")
            self_ret = return_map.get(code, 0.0)

            if not industry:
                records[code]["peer_return_gap"] = 0.0
                continue

            peers = [c for c in industry_codes.get(industry, []) if c != code]
            if not peers:
                records[code]["peer_return_gap"] = 0.0
                continue

            peer_rets = [return_map.get(p, 0.0) for p in peers]
            avg_peer = sum(peer_rets) / len(peer_rets)
            records[code]["peer_return_gap"] = avg_peer - self_ret

    # =====================================================================
    # Traditional factor computation
    # =====================================================================

    def compute_traditional_factors(
        self,
        codes: list[str],
        end_date: str,
    ) -> pd.DataFrame:
        """Compute traditional quantitative factors for a list of stocks.

        Parameters
        ----------
        codes : list[str]
            6-digit A-share stock codes.
        end_date : str
            End date in ``YYYYMMDD`` format.

        Returns
        -------
        pd.DataFrame
            Columns: momentum_20d, reversal_5d, volatility_20d,
            turnover_rate, pe_percentile, roe.  Index: ``stock_code``.
        """
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        start_dt = end_dt - timedelta(days=self._lookback_days)
        start_str = start_dt.strftime("%Y%m%d")

        factor_cols = [
            "momentum_20d",
            "reversal_5d",
            "volatility_20d",
            "turnover_rate",
            "pe_percentile",
            "roe",
        ]
        rows: List[Dict[str, Any]] = []

        # Collect per-stock info for industry PE percentile
        stock_pe: Dict[str, float] = {}
        stock_industry: Dict[str, str] = {}

        for code in codes:
            row: Dict[str, Any] = {"stock_code": code}

            # -- Fetch daily data -----------------------------------------------
            try:
                df = self._market.get_daily_quotes(code, start_str, end_date)
            except Exception as exc:
                logger.warning("Daily data fetch failed for %s: %s", code, exc)
                df = pd.DataFrame()

            if df.empty or len(df) < 5:
                for col in factor_cols:
                    row[col] = np.nan
                rows.append(row)
                continue

            closes = df["收盘"].astype(float).values

            # 1) momentum_20d: return over last 20 trading days
            if len(closes) >= 20:
                row["momentum_20d"] = (closes[-1] / closes[-20] - 1.0) * 100.0
            else:
                row["momentum_20d"] = (closes[-1] / closes[0] - 1.0) * 100.0

            # 2) reversal_5d: negative of 5-day return (mean reversion)
            if len(closes) >= 5:
                row["reversal_5d"] = -((closes[-1] / closes[-5] - 1.0) * 100.0)
            else:
                row["reversal_5d"] = 0.0

            # 3) volatility_20d: annualised std of daily returns (last 20 days)
            window = min(20, len(closes))
            recent_closes = closes[-window:]
            if len(recent_closes) >= 2:
                daily_rets = np.diff(recent_closes) / recent_closes[:-1]
                row["volatility_20d"] = float(np.std(daily_rets, ddof=1) * math.sqrt(252) * 100.0)
            else:
                row["volatility_20d"] = np.nan

            # 4) turnover_rate: average daily turnover (%)
            if "换手率" in df.columns:
                turnover_vals = df["换手率"].astype(float).tail(20)
                row["turnover_rate"] = float(turnover_vals.mean())
            else:
                row["turnover_rate"] = np.nan

            # 5) PE & ROE from fundamental data
            try:
                info = self._fundamental.get_stock_info(code)
            except Exception:
                info = {}

            pe_val = info.get("市盈率-动态")
            if pe_val is not None:
                try:
                    stock_pe[code] = float(pe_val)
                except (ValueError, TypeError):
                    pass

            industry = info.get("行业")
            if industry:
                stock_industry[code] = str(industry)

            # ROE from financial summary
            roe_val = np.nan
            try:
                fin = self._fundamental.get_financial_summary(code)
                if not fin.empty:
                    # Look for ROE column (净资产收益率)
                    roe_cols = [c for c in fin.columns if "净资产收益率" in str(c) or "ROE" in str(c).upper()]
                    if roe_cols:
                        roe_series = pd.to_numeric(fin[roe_cols[0]], errors="coerce")
                        valid = roe_series.dropna()
                        if not valid.empty:
                            roe_val = float(valid.iloc[0])
            except Exception as exc:
                logger.debug("ROE fetch failed for %s: %s", code, exc)

            row["roe"] = roe_val
            row["pe_percentile"] = np.nan  # computed cross-sectionally below
            rows.append(row)

        result = pd.DataFrame(rows).set_index("stock_code")

        # -- PE percentile within industry (cross-sectional) --------------------
        industry_groups: Dict[str, List[str]] = defaultdict(list)
        for code, ind in stock_industry.items():
            industry_groups[ind].append(code)

        for ind, group_codes in industry_groups.items():
            pe_vals = {c: stock_pe[c] for c in group_codes if c in stock_pe}
            if len(pe_vals) < 2:
                continue
            sorted_pes = sorted(pe_vals.values())
            n = len(sorted_pes)
            for c, pe in pe_vals.items():
                # Percentile rank within industry (0 = cheapest, 100 = most expensive)
                rank = sorted_pes.index(pe)
                result.at[c, "pe_percentile"] = rank / (n - 1) * 100.0

        return result

    # =====================================================================
    # Composite scoring
    # =====================================================================

    def compute_composite_score(
        self,
        graph_factors: pd.DataFrame,
        traditional_factors: pd.DataFrame,
    ) -> pd.DataFrame:
        """Merge graph and traditional factors, z-score normalise, and compute
        weighted composite score.

        Parameters
        ----------
        graph_factors : pd.DataFrame
            Output of ``compute_graph_factors``.
        traditional_factors : pd.DataFrame
            Output of ``compute_traditional_factors``.

        Returns
        -------
        pd.DataFrame
            All factor columns plus ``composite_score`` and ``rank``.
            Sorted by composite_score descending.  Index: ``stock_code``.
        """
        # Merge on index (stock_code)
        combined = graph_factors.join(traditional_factors, how="outer")

        # Z-score normalisation per column (cross-sectional)
        z_scored = pd.DataFrame(index=combined.index)
        for col in combined.columns:
            series = combined[col].astype(float)
            mean = series.mean()
            std = series.std()
            if std is not None and std > 1e-9:
                z_scored[col] = (series - mean) / std
            else:
                z_scored[col] = 0.0

        # Fill NaN z-scores with 0 (neutral)
        z_scored = z_scored.fillna(0.0)

        # Clip z-scores to [-3, 3] to prevent sparse factors from dominating.
        z_scored = z_scored.clip(-3.0, 3.0)

        # --- Dynamic graph weight scaling based on coverage ---
        # If graph covers <50% of stocks, scale graph factor weights down proportionally.
        # This prevents a 5%-coverage sparse graph from biasing signals with noise.
        n_total = len(combined)
        graph_cols = [c for c in z_scored.columns if c in _GRAPH_FACTOR_COLS]
        n_covered = 0
        if graph_cols:
            # Count stocks with at least one non-zero graph factor
            n_covered = int((combined[graph_cols].abs().sum(axis=1) > 1e-9).sum())
        coverage_ratio = n_covered / max(n_total, 1)
        # Scale: 0 coverage → 0.0 weight; 50% coverage → 1.0 weight; linear
        graph_scale = min(1.0, coverage_ratio / 0.50)
        if graph_scale < 1.0:
            logger.info(
                "Graph coverage %.1f%% (%d/%d stocks) — scaling graph factor weights by %.2f",
                coverage_ratio * 100, n_covered, n_total, graph_scale,
            )

        # --- Per-factor sparsity filter ---
        # Skip graph factors where <10% of stocks have non-zero raw values.
        # These produce misleading z-scores (handful of extreme values, rest zero).
        _COVERAGE_THRESHOLD = 0.10
        skip_factors: set = set()
        factor_coverage: dict = {}  # col -> nonzero_ratio (for diagnostics)
        for col in graph_cols:
            if col in combined.columns:
                nonzero_ratio = float((combined[col].abs() > 1e-9).mean())
                factor_coverage[col] = nonzero_ratio
                if nonzero_ratio < _COVERAGE_THRESHOLD:
                    skip_factors.add(col)
                    logger.info(
                        "Skipping sparse graph factor '%s' (%.1f%% non-zero, threshold %.0f%%)",
                        col, nonzero_ratio * 100, _COVERAGE_THRESHOLD * 100,
                    )
                else:
                    logger.info(
                        "Keeping graph factor '%s' (%.1f%% non-zero)",
                        col, nonzero_ratio * 100,
                    )

        # Store coverage info for external diagnostics (e.g. backtest reporting)
        self._last_factor_coverage = factor_coverage
        self._last_skip_factors = skip_factors
        self._last_z_scored = z_scored
        self._last_graph_scale = graph_scale
        self._last_coverage_ratio = coverage_ratio

        # Weighted composite
        composite = pd.Series(0.0, index=z_scored.index, dtype=float)
        total_abs_weight = 0.0
        for col in z_scored.columns:
            if col in skip_factors:
                continue
            w = self._weights.get(col, 0.0)
            if col in _GRAPH_FACTOR_COLS:
                w = w * graph_scale
            composite += z_scored[col] * w
            total_abs_weight += abs(w)

        if total_abs_weight > 0:
            composite /= total_abs_weight  # normalise so scale is comparable

        result = combined.copy()
        result["composite_score"] = composite
        result["rank"] = composite.rank(ascending=False, method="min").astype(int)
        result = result.sort_values("composite_score", ascending=False)

        return result

    # =====================================================================
    # Signal generation
    # =====================================================================

    def _composite_to_signals(
        self,
        composite: pd.DataFrame,
        target_codes: list[str],
    ) -> list[StrategySignal]:
        """Convert composite scores into StrategySignal objects.

        Top ``_top_n`` stocks receive *long* signals, bottom ``_top_n``
        receive *short* signals, and the rest are *neutral*.

        Only stocks present in *target_codes* are emitted (but the ranking
        is computed over the full composite universe for correct z-scores).
        """
        if composite.empty:
            return []

        n_stocks = len(composite)
        top_cutoff = min(self._top_n, n_stocks)

        # Determine thresholds from ranking
        sorted_scores = composite["composite_score"].sort_values(ascending=False)
        long_threshold = sorted_scores.iloc[min(top_cutoff - 1, n_stocks - 1)]
        short_threshold = sorted_scores.iloc[max(n_stocks - top_cutoff, 0)]

        # Fetch stock names for the target codes
        code_names = self._get_stock_names(target_codes)

        signals: list[StrategySignal] = []
        target_set = set(target_codes) if target_codes else set(composite.index)

        for code in composite.index:
            if code not in target_set:
                continue

            score = composite.at[code, "composite_score"]
            rank = int(composite.at[code, "rank"])

            if score >= long_threshold and top_cutoff < n_stocks:
                direction = "long"
            elif score <= short_threshold and top_cutoff < n_stocks:
                direction = "short"
            else:
                direction = "neutral"

            # Confidence: map composite z-score to [0, 1] using sigmoid-like transform
            confidence = min(1.0, max(0.0, 0.5 + 0.15 * score))

            # Expected return: rough estimate from historical factor return
            expected_return = score * 0.02  # 2% per unit z-score

            # Dynamic holding period: momentum-driven signals hold shorter,
            # value-driven signals hold longer (addresses T-20 vs T-40 inconsistency)
            momentum_score = composite.at[code, "momentum_20d"] if "momentum_20d" in composite.columns else 0.0
            reversal_score = composite.at[code, "reversal_5d"] if "reversal_5d" in composite.columns else 0.0
            if isinstance(momentum_score, float) and not math.isnan(momentum_score):
                mom_abs = abs(momentum_score)
            else:
                mom_abs = 0.0
            if isinstance(reversal_score, float) and not math.isnan(reversal_score):
                rev_abs = abs(reversal_score)
            else:
                rev_abs = 0.0

            # Strong momentum → shorter hold (10-15d), strong reversal → longer hold (20-30d)
            if mom_abs > rev_abs and mom_abs > 1.0:
                holding = max(10, min(15, self._holding_days - int(mom_abs * 2)))
            elif rev_abs > 1.0:
                holding = min(30, self._holding_days + int(rev_abs * 3))
            else:
                holding = self._holding_days

            # Build reasoning string
            factor_contributions = []
            for col in composite.columns:
                if col in ("composite_score", "rank"):
                    continue
                val = composite.at[code, col]
                if not (isinstance(val, float) and math.isnan(val)):
                    w = self._weights.get(col, 0.0)
                    if abs(w) > 0:
                        factor_contributions.append(f"{col}={val:.2f}(w={w:+.1f})")

            reasoning = (
                f"Composite rank {rank}/{n_stocks}, score={score:.3f}. "
                f"Factors: {', '.join(factor_contributions[:6])}"
            )

            # ----- Build detailed metadata for frontend factor table -----
            # Factor English-to-Chinese name mapping
            _FACTOR_CN: Dict[str, str] = {
                "supply_chain_centrality": "供应链中心性",
                "betweenness_centrality": "中介中心性",
                "institution_concentration": "机构持仓集中度",
                "concept_heat": "概念热度",
                "event_exposure": "事件敏感度",
                "industry_leadership": "行业领导力",
                "peer_return_gap": "同行收益差",
                "momentum_20d": "20日动量",
                "reversal_5d": "5日反转",
                "volatility_20d": "20日波动率",
                "turnover_rate": "换手率",
                "pe_percentile": "PE分位",
                "roe": "ROE",
            }

            # Retrieve cached z-scores and diagnostics from compute_composite_score
            z_scored = getattr(self, "_last_z_scored", None)
            skip_factors = getattr(self, "_last_skip_factors", set())
            graph_scale = getattr(self, "_last_graph_scale", 1.0)
            coverage_ratio = getattr(self, "_last_coverage_ratio", 0.0)

            factors_detail: Dict[str, Any] = {}
            for col, cn_name in _FACTOR_CN.items():
                if col in skip_factors:
                    factors_detail[cn_name] = None
                    continue
                # Get z-score value
                if z_scored is not None and col in z_scored.columns and code in z_scored.index:
                    z_val = z_scored.at[code, col]
                    if isinstance(z_val, float) and math.isnan(z_val):
                        z_val = 0.0
                else:
                    z_val = None
                # Get effective weight (graph factors are scaled)
                raw_w = self._weights.get(col, 0.0)
                eff_w = raw_w * graph_scale if col in _GRAPH_FACTOR_COLS else raw_w
                if z_val is not None:
                    factors_detail[cn_name] = {
                        "z_score": round(z_val, 4),
                        "weight": round(eff_w, 4),
                        "contribution": round(z_val * eff_w, 4),
                    }
                else:
                    factors_detail[cn_name] = None

            # Confidence breakdown
            rank_boost = 0.15 * score
            # Factor consistency: ratio of factors agreeing with signal direction
            pos_count = sum(
                1 for v in factors_detail.values()
                if v is not None and v["contribution"] > 0
            )
            neg_count = sum(
                1 for v in factors_detail.values()
                if v is not None and v["contribution"] < 0
            )
            total_factors = pos_count + neg_count
            if direction == "long":
                factor_consistency = pos_count / max(total_factors, 1)
            elif direction == "short":
                factor_consistency = neg_count / max(total_factors, 1)
            else:
                factor_consistency = 0.5

            metadata: Dict[str, Any] = {
                "rank": rank,
                "total_stocks": n_stocks,
                "composite_score": round(score, 4),
                "factors": factors_detail,
                "graph_coverage_ratio": round(coverage_ratio, 4),
                "confidence_breakdown": {
                    "base": 0.5,
                    "rank_boost": round(rank_boost, 4),
                    "factor_consistency": round(factor_consistency, 4),
                    "final": round(confidence, 4),
                },
            }

            signals.append(
                StrategySignal(
                    strategy_name=self.name,
                    stock_code=code,
                    stock_name=code_names.get(code, code),
                    direction=direction,
                    confidence=confidence,
                    expected_return=round(expected_return, 4),
                    holding_period_days=holding,
                    reasoning=reasoning,
                    metadata=metadata,
                )
            )

        return signals

    # =====================================================================
    # Internal helpers
    # =====================================================================

    def _fetch_graph_data(
        self,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Fetch nodes and edges from the local graph store.

        Returns empty lists if the graph is unavailable so that
        the strategy can still run on traditional factors alone.
        """
        try:
            from astrategy.graph.local_store import LocalGraphStore

            store = LocalGraphStore()
            graph_id = "supply_chain"
            if not store.load(graph_id):
                logger.warning("Local graph '%s' not found; no graph factors.", graph_id)
                return [], []
            nodes = store.get_all_nodes(graph_id)
            edges = store.get_all_edges(graph_id)
            logger.info(
                "Fetched %d nodes and %d edges from local graph '%s'",
                len(nodes), len(edges), graph_id,
            )
            return nodes, edges
        except Exception as exc:
            logger.warning(
                "Graph data unavailable (%s); proceeding with traditional factors only.",
                exc,
            )
            return [], []

    @staticmethod
    def _build_code_name_maps(
        nodes: List[Dict[str, Any]],
    ) -> Tuple[Dict[str, str], Dict[str, str]]:
        """Build bidirectional maps between stock_code and company node name.

        Returns
        -------
        (code_to_name, name_to_code)
        """
        import re as _re
        code_to_name: Dict[str, str] = {}
        name_to_code: Dict[str, str] = {}

        for n in nodes:
            labels = n.get("labels", [])
            if "Company" not in labels:
                continue
            attrs = n.get("attributes", {}) or {}
            name = n.get("name", "")
            # Support both "stock_code" and "code" attribute keys
            code = str(attrs.get("stock_code", "") or attrs.get("code", ""))
            # If still empty but node name itself is a 6-digit stock code, use it directly
            if not code and _re.match(r"^\d{6}$", name):
                code = name
            # Normalise code: strip exchange suffix (.SH / .SZ)
            if "." in code:
                code = code.split(".")[0]
            if code and name:
                code_to_name[code] = name
                name_to_code[name] = code

        return code_to_name, name_to_code

    def _find_industry_peers(
        self,
        stock_code: str,
        nodes: List[Dict[str, Any]],
        edges: List[Dict[str, Any]],
    ) -> List[str]:
        """Find industry peers of a stock via BELONGS_TO edges in the graph."""
        code_to_name, name_to_code = self._build_code_name_maps(nodes)
        company_name = code_to_name.get(stock_code, "")
        if not company_name:
            return []

        # Find industry for this company
        industry_name = ""
        for e in edges:
            if e.get("relation") == "BELONGS_TO" and e.get("source_name") == company_name:
                industry_name = e.get("target_name", "")
                break

        if not industry_name:
            return []

        # Find all companies in same industry
        peers: List[str] = []
        for e in edges:
            if e.get("relation") == "BELONGS_TO" and e.get("target_name") == industry_name:
                peer_name = e.get("source_name", "")
                peer_code = name_to_code.get(peer_name, "")
                if peer_code and peer_code != stock_code:
                    peers.append(peer_code)

        return peers

    def _get_stock_names(self, codes: list[str]) -> Dict[str, str]:
        """Fetch stock names, preferring local graph data to avoid slow network calls."""
        names: Dict[str, str] = {}

        # Priority 1: use graph node display_name (fast, no network)
        code_set = set(codes)
        try:
            from astrategy.graph.local_store import LocalGraphStore
            store = LocalGraphStore()
            if store.load("supply_chain"):
                for n in store.get_all_nodes("supply_chain"):
                    dn = n.get("display_name", "")
                    nm = n.get("name", "")
                    if dn and nm in code_set:
                        names[nm] = dn
        except Exception:
            pass

        # Priority 2: market API (may fail on SSL)
        missing = [c for c in codes if c not in names]
        if missing:
            try:
                rt = self._market.get_realtime_quotes(missing)
                if not rt.empty and "代码" in rt.columns and "名称" in rt.columns:
                    for _, row in rt.iterrows():
                        names[str(row["代码"])] = str(row["名称"])
            except Exception as exc:
                logger.debug("Failed to fetch stock names: %s", exc)

        # Fallback: use code as name
        for c in codes:
            if c not in names:
                names[c] = c

        return names
