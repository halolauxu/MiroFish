"""
Graph Enrichment Script
========================
Adds missing edge types and node attributes to the local supply_chain graph
to unlock S07's graph factors:

  1. BELONGS_TO edges   → industry_leadership factor
  2. market_cap attrs   → industry_leadership factor
  3. RELATES_TO_CONCEPT → concept_heat factor
  4. Event + TRIGGERS   → event_exposure factor

Usage:
    python -m astrategy.scripts.enrich_graph [--graph supply_chain]
"""

from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime, timedelta, timezone

import akshare as ak
import pandas as pd

from astrategy.graph.local_store import LocalGraphStore

logger = logging.getLogger("astrategy.scripts.enrich_graph")
_CST = timezone(timedelta(hours=8))


def _safe(fn, *args, **kwargs):
    """Call fn and return result, or None on error."""
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        logger.warning("API call failed: %s — %s", fn.__name__, str(exc)[:80])
        return None


# ── 1. Industry classification (BELONGS_TO) ────────────────────────────

def enrich_industry_classification(store: LocalGraphStore, graph_id: str) -> int:
    """Add BELONGS_TO edges from Company nodes to Industry nodes.

    Uses akshare stock_board_industry_cons_em to find which stocks belong
    to which industry. Also creates Industry nodes.
    """
    # Get all existing company codes from the graph
    nodes = store.get_all_nodes(graph_id)
    existing_codes = set()
    for n in nodes:
        attrs = n.get("attributes", {}) or {}
        code = str(attrs.get("code", "") or attrs.get("stock_code", ""))
        if len(code) == 6 and code.isdigit():
            existing_codes.add(code)

    # Also check if node name itself is a 6-digit code
    for n in nodes:
        name = n.get("name", "")
        if len(name) == 6 and name.isdigit():
            existing_codes.add(name)

    logger.info("Found %d stock codes in graph", len(existing_codes))

    # Get industry board list
    df_boards = _safe(ak.stock_board_industry_name_em)
    if df_boards is None or df_boards.empty:
        logger.warning("Cannot fetch industry board list, trying fallback...")
        # Fallback: use the industry attribute already on nodes
        return _enrich_industry_from_node_attrs(store, graph_id, nodes)

    added = 0
    industries_created = set()

    for _, row in df_boards.iterrows():
        industry_name = row.get("板块名称", "")
        if not industry_name:
            continue

        # Get constituents for this industry
        df_cons = _safe(ak.stock_board_industry_cons_em, symbol=industry_name)
        if df_cons is None or df_cons.empty:
            continue

        codes_in_board = set(df_cons["代码"].astype(str).tolist())
        matching = existing_codes & codes_in_board

        if not matching:
            continue

        # Create Industry node
        if industry_name not in industries_created:
            store.add_node(
                graph_id, industry_name,
                labels=["Industry"],
                summary=f"A股行业板块: {industry_name}",
            )
            industries_created.add(industry_name)

        # Add BELONGS_TO edges
        for code in matching:
            store.add_edge(
                graph_id,
                source=code,
                target=industry_name,
                relation="BELONGS_TO",
                fact=f"{code} belongs to {industry_name} industry",
            )
            added += 1

        if len(industries_created) % 10 == 0:
            logger.info("Processed %d industries, %d BELONGS_TO edges", len(industries_created), added)

        time.sleep(0.3)  # Rate limit

    logger.info("Added %d BELONGS_TO edges across %d industries", added, len(industries_created))
    return added


def _enrich_industry_from_node_attrs(
    store: LocalGraphStore, graph_id: str, nodes: list[dict],
) -> int:
    """Fallback: use the 'industry' attribute already on Company nodes."""
    added = 0
    industries_created: set[str] = set()

    for n in nodes:
        labels = n.get("labels", [])
        if "Company" not in labels:
            continue
        attrs = n.get("attributes", {}) or {}
        industry = attrs.get("industry", "")
        code = str(attrs.get("code", "") or attrs.get("stock_code", ""))
        name = n.get("name", "")

        if not industry or not (code or name):
            continue

        if industry not in industries_created:
            store.add_node(
                graph_id, industry,
                labels=["Industry"],
                summary=f"行业分类: {industry}",
            )
            industries_created.add(industry)

        source = code if len(code) == 6 else name
        store.add_edge(
            graph_id,
            source=source,
            target=industry,
            relation="BELONGS_TO",
            fact=f"{source} belongs to {industry}",
        )
        added += 1

    logger.info("Fallback: added %d BELONGS_TO edges from node attributes", added)
    return added


# ── 2. Market cap enrichment ──────────────────────────────────────────

def enrich_market_cap(
    store: LocalGraphStore, graph_id: str, max_stocks: int = 200,
) -> int:
    """Add market_cap + industry via stock_individual_info_em (per-stock API).

    Also creates BELONGS_TO edges for any new industry discovered.
    Limited to `max_stocks` to avoid API rate-limiting.
    """
    nodes = store.get_all_nodes(graph_id)
    code_to_node_name: dict[str, str] = {}

    for n in nodes:
        labels = n.get("labels", [])
        if "Company" not in labels:
            continue
        attrs = n.get("attributes", {}) or {}
        code = str(attrs.get("code", "") or attrs.get("stock_code", ""))
        if len(code) == 6 and code.isdigit():
            code_to_node_name[code] = n.get("name", "")

    # Prioritise stocks that already appear in edges (more important)
    edges = store.get_all_edges(graph_id)
    edge_codes: set[str] = set()
    for e in edges:
        for key in ("source_name", "target_name"):
            v = str(e.get(key, ""))
            if len(v) == 6 and v.isdigit():
                edge_codes.add(v)

    # Order: edge-connected stocks first, then rest
    priority = sorted(edge_codes & set(code_to_node_name.keys()))
    rest = sorted(set(code_to_node_name.keys()) - edge_codes)
    ordered_codes = (priority + rest)[:max_stocks]

    logger.info(
        "Will enrich market_cap for %d / %d stocks (%d priority)",
        len(ordered_codes), len(code_to_node_name), len(priority),
    )

    enriched = 0
    industries_created: set[str] = set()
    belongs_added = 0

    for i, code in enumerate(ordered_codes):
        node_name = code_to_node_name[code]
        df = _safe(ak.stock_individual_info_em, symbol=code)
        if df is None or df.empty:
            continue

        info = dict(zip(df["item"], df["value"]))
        mc = info.get("总市值")
        industry = info.get("行业", "")

        if mc is not None:
            try:
                mc_val = float(mc)
                store.add_node(
                    graph_id, node_name,
                    labels=["Company"],
                    market_cap=mc_val,
                )
                enriched += 1
            except (ValueError, TypeError):
                pass

        # Add BELONGS_TO if industry is present
        if industry:
            if industry not in industries_created:
                store.add_node(
                    graph_id, industry,
                    labels=["Industry"],
                    summary=f"行业分类: {industry}",
                )
                industries_created.add(industry)
            store.add_edge(
                graph_id,
                source=code,
                target=industry,
                relation="BELONGS_TO",
                fact=f"{code} belongs to {industry}",
            )
            belongs_added += 1

        if (i + 1) % 50 == 0:
            logger.info(
                "  Progress: %d/%d, enriched=%d, belongs_to=%d",
                i + 1, len(ordered_codes), enriched, belongs_added,
            )
        time.sleep(0.15)  # Rate limit

    logger.info(
        "Enriched market_cap for %d stocks, added %d BELONGS_TO edges",
        enriched, belongs_added,
    )
    return enriched + belongs_added


# ── 3. Concept nodes + RELATES_TO_CONCEPT edges ─────────────────────

def enrich_concepts(store: LocalGraphStore, graph_id: str) -> int:
    """Add Concept nodes and RELATES_TO_CONCEPT edges.

    Primary: use stock_board_concept_name_em bulk API.
    Fallback: use hardcoded top A-share concepts from S11 NARRATIVES.
    """
    nodes = store.get_all_nodes(graph_id)
    existing_codes = set()
    for n in nodes:
        attrs = n.get("attributes", {}) or {}
        code = str(attrs.get("code", "") or attrs.get("stock_code", ""))
        if len(code) == 6 and code.isdigit():
            existing_codes.add(code)
        name = n.get("name", "")
        if len(name) == 6 and name.isdigit():
            existing_codes.add(name)

    # Try bulk API first
    df_concepts = _safe(ak.stock_board_concept_name_em)
    if df_concepts is not None and not df_concepts.empty:
        return _enrich_concepts_from_api(store, graph_id, existing_codes, df_concepts)

    # Fallback: use S11 NARRATIVES definitions
    logger.info("Bulk concept API failed, using S11 NARRATIVES as concept fallback")
    return _enrich_concepts_from_narratives(store, graph_id, existing_codes)


def _enrich_concepts_from_api(
    store: LocalGraphStore, graph_id: str,
    existing_codes: set[str], df_concepts: pd.DataFrame,
) -> int:
    if "涨跌幅" in df_concepts.columns:
        df_concepts = df_concepts.sort_values("涨跌幅", ascending=False)

    top_concepts = df_concepts.head(30)
    added = 0

    for idx, (_, row) in enumerate(top_concepts.iterrows()):
        concept_name = row.get("板块名称", "")
        if not concept_name:
            continue

        df_cons = _safe(ak.stock_board_concept_cons_em, symbol=concept_name)
        if df_cons is None or df_cons.empty:
            continue

        codes_in_concept = set(df_cons["代码"].astype(str).tolist())
        matching = existing_codes & codes_in_concept
        if not matching:
            continue

        store.add_node(
            graph_id, concept_name,
            labels=["Concept"],
            summary=f"A股概念板块: {concept_name}",
            hot_rank=idx + 1,
        )

        for code in matching:
            store.add_edge(
                graph_id,
                source=code,
                target=concept_name,
                relation="RELATES_TO_CONCEPT",
                fact=f"{code} relates to concept {concept_name}",
            )
            added += 1

        if (idx + 1) % 5 == 0:
            logger.info("Processed %d / %d concepts, %d edges", idx + 1, len(top_concepts), added)
        time.sleep(0.3)

    logger.info("Added %d RELATES_TO_CONCEPT edges from %d concepts (API)", added, len(top_concepts))
    return added


def _enrich_concepts_from_narratives(
    store: LocalGraphStore, graph_id: str,
    existing_codes: set[str],
) -> int:
    """Use S11 NARRATIVES definitions to create concept nodes and edges."""
    from astrategy.strategies.s11_narrative_tracker import NARRATIVES

    added = 0
    for idx, (nname, ndata) in enumerate(NARRATIVES.items()):
        rep_stocks = ndata.get("representative_stocks", [])
        matching = [c for c in rep_stocks if c in existing_codes]
        if not matching:
            continue

        store.add_node(
            graph_id, nname,
            labels=["Concept"],
            summary=ndata.get("description", ""),
            hot_rank=idx + 1,
        )

        for code in matching:
            store.add_edge(
                graph_id,
                source=code,
                target=nname,
                relation="RELATES_TO_CONCEPT",
                fact=f"{code} relates to concept {nname}",
            )
            added += 1

    logger.info("Added %d RELATES_TO_CONCEPT edges from %d NARRATIVES (fallback)", added, len(NARRATIVES))
    return added


# ── 4. Event nodes + TRIGGERS edges ─────────────────────────────────

def enrich_events(store: LocalGraphStore, graph_id: str) -> int:
    """Add Event nodes from recent hot news and link to companies via TRIGGERS."""
    nodes = store.get_all_nodes(graph_id)
    code_to_names: dict[str, list[str]] = {}
    for n in nodes:
        labels = n.get("labels", [])
        if "Company" not in labels:
            continue
        attrs = n.get("attributes", {}) or {}
        code = str(attrs.get("code", "") or attrs.get("stock_code", ""))
        if len(code) == 6 and code.isdigit():
            if code not in code_to_names:
                code_to_names[code] = []
            code_to_names[code].append(n.get("name", ""))

    # Get hot topics / trending stocks
    df_hot = _safe(ak.stock_hot_rank_em)
    if df_hot is None or df_hot.empty:
        logger.warning("Cannot fetch hot stocks, skipping event enrichment")
        return 0

    # Focus on top 50 hot stocks that are in our graph
    hot_codes = df_hot["代码"].astype(str).head(50).tolist() if "代码" in df_hot.columns else []
    matching_hot = [c for c in hot_codes if c in code_to_names]

    added = 0
    today = datetime.now(tz=_CST).strftime("%Y%m%d")

    for code in matching_hot[:20]:
        # Get recent news for this stock
        try:
            df_news = ak.stock_news_em(symbol=code)
        except Exception:
            continue

        if df_news is None or df_news.empty:
            continue

        # Take top 3 most recent news as events
        for _, news_row in df_news.head(3).iterrows():
            title = str(news_row.get("新闻标题", ""))[:80]
            if not title:
                continue

            event_name = f"EVENT_{code}_{title[:20]}"

            store.add_node(
                graph_id, event_name,
                labels=["Event"],
                summary=title,
                event_date=today,
                stock_code=code,
            )

            # TRIGGERS edge: Event -> Company
            for company_name in code_to_names.get(code, [code]):
                store.add_edge(
                    graph_id,
                    source=event_name,
                    target=company_name,
                    relation="TRIGGERS",
                    fact=f"Event '{title}' triggers {company_name}",
                )
                added += 1

        time.sleep(0.2)

    logger.info("Added %d TRIGGERS edges from events for %d hot stocks", added, len(matching_hot))
    return added


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Enrich local graph with missing edge types")
    parser.add_argument("--graph", default="supply_chain", help="Graph name")
    parser.add_argument(
        "--skip", default="", help="Comma-separated steps to skip: industry,marketcap,concept,event"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    store = LocalGraphStore()
    graph_id = args.graph

    # Load existing graph
    store.load(graph_id)
    nodes_before = len(store.get_all_nodes(graph_id))
    edges_before = len(store.get_all_edges(graph_id))
    logger.info("Graph '%s' loaded: %d nodes, %d edges", graph_id, nodes_before, edges_before)

    skip = set(args.skip.split(",")) if args.skip else set()
    t0 = time.time()
    stats: dict[str, int] = {}

    # Step 1: Industry classification
    if "industry" not in skip:
        logger.info("=" * 50)
        logger.info("Step 1: Industry classification (BELONGS_TO)")
        stats["belongs_to"] = enrich_industry_classification(store, graph_id)

    # Step 2: Market cap
    if "marketcap" not in skip:
        logger.info("=" * 50)
        logger.info("Step 2: Market cap enrichment")
        stats["market_cap"] = enrich_market_cap(store, graph_id)

    # Step 3: Concept nodes
    if "concept" not in skip:
        logger.info("=" * 50)
        logger.info("Step 3: Concept nodes (RELATES_TO_CONCEPT)")
        stats["concept"] = enrich_concepts(store, graph_id)

    # Step 4: Event nodes
    if "event" not in skip:
        logger.info("=" * 50)
        logger.info("Step 4: Event nodes (TRIGGERS)")
        stats["event"] = enrich_events(store, graph_id)

    # Save
    store.save(graph_id)
    nodes_after = len(store.get_all_nodes(graph_id))
    edges_after = len(store.get_all_edges(graph_id))
    elapsed = time.time() - t0

    print()
    print("=" * 60)
    print(f"Graph Enrichment Complete — {elapsed:.0f}s")
    print(f"  Before: {nodes_before} nodes, {edges_before} edges")
    print(f"  After:  {nodes_after} nodes, {edges_after} edges")
    print(f"  Added:  {nodes_after - nodes_before} nodes, {edges_after - edges_before} edges")
    for k, v in stats.items():
        print(f"    {k}: {v} edges/attrs")
    print("=" * 60)


if __name__ == "__main__":
    main()
