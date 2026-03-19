"""Expand graph coverage for a target universe and persist audit-friendly manifests."""

from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List

from astrategy.graph.local_store import LocalGraphStore

from ..common import ensure_dir, graph_path, graph_root

logger = logging.getLogger("astrategy.datahub.graph.coverage")
_GRAPH_ID = "supply_chain"


def _normalize_date(value: str | None) -> str:
    if not value:
        return date.today().isoformat()
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text[:10]


def _safe(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        logger.warning("Graph API call failed: %s: %s", fn.__name__, str(exc)[:120])
        return None


def _graph_data_root() -> Path:
    return ensure_dir(graph_root())


def _daily_dir() -> Path:
    return ensure_dir(_graph_data_root() / "daily")


def _manifest_path() -> Path:
    return _graph_data_root() / "graph_manifest.json"


def _profile_cache_path() -> Path:
    return _graph_data_root() / "stock_profile_cache.json"


def _load_profile_cache() -> Dict[str, Dict[str, Any]]:
    path = _profile_cache_path()
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _save_profile_cache(cache: Dict[str, Dict[str, Any]]) -> Path:
    path = _profile_cache_path()
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _build_security_index(security_master: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {
        str(item.get("ticker", "")).zfill(6): item
        for item in security_master
        if str(item.get("ticker", "")).strip()
    }


def _universe_codes(universe_membership: Dict[str, Any], universe_id: str) -> List[str]:
    return sorted(
        {
            str(row.get("ticker", "")).zfill(6)
            for row in universe_membership.get("memberships", [])
            if row.get("universe_id") == universe_id and str(row.get("ticker", "")).strip()
        }
    )


def _membership_name_map(universe_membership: Dict[str, Any], universe_id: str) -> Dict[str, str]:
    return {
        str(row.get("ticker", "")).zfill(6): str(row.get("company_name", "")).strip()
        for row in universe_membership.get("memberships", [])
        if row.get("universe_id") == universe_id and str(row.get("ticker", "")).strip()
    }


def _stock_name(code: str, security_index: Dict[str, Dict[str, Any]], membership_names: Dict[str, str]) -> str:
    return (
        str(security_index.get(code, {}).get("company_name", "")).strip()
        or str(membership_names.get(code, "")).strip()
        or code
    )


def _load_store() -> LocalGraphStore:
    store = LocalGraphStore()
    if not store.load(_GRAPH_ID):
        store.create_graph(_GRAPH_ID)
    return store


def _node_map(store: LocalGraphStore) -> Dict[str, Dict[str, Any]]:
    return {
        str(node.get("name", "")): node
        for node in store.get_all_nodes(_GRAPH_ID)
        if str(node.get("name", "")).strip()
    }


def _edge_key(edge: Dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(edge.get("source_name") or edge.get("source") or "").strip(),
        str(edge.get("target_name") or edge.get("target") or "").strip(),
        str(edge.get("relation", "")).strip(),
    )


def _edge_keys(store: LocalGraphStore) -> set[tuple[str, str, str]]:
    return {_edge_key(edge) for edge in store.get_all_edges(_GRAPH_ID)}


def _upsert_company_node(
    store: LocalGraphStore,
    node_index: Dict[str, Dict[str, Any]],
    code: str,
    company_name: str,
    *,
    industry: str = "",
    as_of_date: str = "",
) -> bool:
    existing = node_index.get(code, {})
    existing_attrs = dict(existing.get("attributes", {}) or {})
    labels = sorted(set(existing.get("labels", []) or []) | {"Company"})
    summary = str(existing.get("summary", "")).strip() or f"{company_name}({code})"
    updated_attrs = {
        **existing_attrs,
        "code": code,
        "stock_code": code,
        "display_name": company_name,
    }
    if industry:
        updated_attrs["industry"] = industry
    changed = (
        not existing
        or str(existing_attrs.get("display_name", "")).strip() != company_name
        or (industry and str(existing_attrs.get("industry", "")).strip() != industry)
    )
    store.add_node(
        _GRAPH_ID,
        code,
        labels=labels,
        summary=summary,
        created_at=str(existing.get("created_at", "")).strip() or f"{as_of_date}T00:00:00",
        valid_from=str(existing.get("valid_from", "")).strip() or as_of_date,
        valid_to=str(existing.get("valid_to", "")).strip(),
        **updated_attrs,
    )
    node_index[code] = {
        "name": code,
        "labels": labels,
        "summary": summary,
        "attributes": updated_attrs,
        "created_at": str(existing.get("created_at", "")).strip() or f"{as_of_date}T00:00:00",
        "valid_from": str(existing.get("valid_from", "")).strip() or as_of_date,
        "valid_to": str(existing.get("valid_to", "")).strip(),
    }
    return changed


def _add_edge_once(
    store: LocalGraphStore,
    edge_keys: set[tuple[str, str, str]],
    source: str,
    target: str,
    relation: str,
    *,
    fact: str,
    weight: float = 1.0,
    confidence: float = 1.0,
    as_of_date: str = "",
    **attrs,
) -> bool:
    key = (source, target, relation)
    if key in edge_keys:
        return False
    store.add_edge(
        _GRAPH_ID,
        source=source,
        target=target,
        relation=relation,
        fact=fact,
        weight=weight,
        confidence=confidence,
        valid_from=as_of_date,
        created_at=f"{as_of_date}T00:00:00" if as_of_date else "",
        **attrs,
    )
    edge_keys.add(key)
    return True


def _resolve_industry_map(
    tickers: Iterable[str],
    security_index: Dict[str, Dict[str, Any]],
    *,
    refresh_graph: bool = False,
) -> Dict[str, str]:
    tickers = sorted(set(tickers))
    industry_map: Dict[str, str] = {
        code: str(security_index.get(code, {}).get("industry_l1", "")).strip()
        for code in tickers
        if str(security_index.get(code, {}).get("industry_l1", "")).strip()
    }

    try:
        import akshare as ak
    except Exception as exc:
        logger.warning("akshare unavailable for graph expansion: %s", exc)
        return industry_map

    df_boards = _safe(ak.stock_board_industry_name_em)
    if df_boards is not None and not df_boards.empty:
        ticker_set = set(tickers)
        for _, row in df_boards.iterrows():
            industry_name = str(row.get("板块名称", "")).strip()
            if not industry_name:
                continue
            df_cons = _safe(ak.stock_board_industry_cons_em, symbol=industry_name)
            if df_cons is None or df_cons.empty or "代码" not in df_cons.columns:
                continue
            matches = ticker_set & {str(code).zfill(6) for code in df_cons["代码"].tolist()}
            for code in matches:
                industry_map[code] = industry_name
            time.sleep(0.10)

    cache = _load_profile_cache()
    unresolved = [
        code for code in tickers
        if code not in industry_map or refresh_graph
    ]
    for code in unresolved:
        cached = cache.get(code, {})
        if cached and cached.get("industry") and not refresh_graph:
            industry_map.setdefault(code, str(cached.get("industry", "")).strip())
            continue
        df = _safe(ak.stock_individual_info_em, symbol=code)
        if df is None or df.empty or "item" not in df.columns or "value" not in df.columns:
            continue
        info = dict(zip(df["item"], df["value"]))
        industry = str(info.get("行业", "") or "").strip()
        if industry:
            industry_map[code] = industry
        cache[code] = {
            "industry": industry,
            "total_market_cap": str(info.get("总市值", "")).strip(),
            "fetched_at": date.today().isoformat(),
        }
        time.sleep(0.08)
    _save_profile_cache(cache)
    return industry_map


def _resolve_concept_map(
    tickers: Iterable[str],
    *,
    top_concepts: int = 40,
) -> Dict[str, List[str]]:
    try:
        import akshare as ak
    except Exception as exc:
        logger.warning("akshare unavailable for concept expansion: %s", exc)
        return {}

    ticker_set = set(tickers)
    df_concepts = _safe(ak.stock_board_concept_name_em)
    if df_concepts is None or df_concepts.empty:
        return {}
    if "涨跌幅" in df_concepts.columns:
        df_concepts = df_concepts.sort_values("涨跌幅", ascending=False)

    concept_map: Dict[str, List[str]] = {}
    for _, row in df_concepts.head(max(top_concepts, 0)).iterrows():
        concept_name = str(row.get("板块名称", "")).strip()
        if not concept_name:
            continue
        df_cons = _safe(ak.stock_board_concept_cons_em, symbol=concept_name)
        if df_cons is None or df_cons.empty or "代码" not in df_cons.columns:
            continue
        matches = sorted(ticker_set & {str(code).zfill(6) for code in df_cons["代码"].tolist()})
        if matches:
            concept_map[concept_name] = matches
        time.sleep(0.10)
    return concept_map


def build_graph_layer(
    security_master: List[Dict[str, Any]],
    universe_membership: Dict[str, Any],
    universe_id: str = "csi800",
    *,
    as_of_date: str | None = None,
    collect_graph: bool = False,
    top_concepts: int = 40,
    peer_limit: int = 3,
    refresh_graph: bool = False,
) -> Dict[str, Any]:
    """Expand the local graph for one universe and emit a manifest."""
    as_of_date = _normalize_date(as_of_date)
    store = _load_store()
    security_index = _build_security_index(security_master)
    tickers = _universe_codes(universe_membership, universe_id)
    membership_names = _membership_name_map(universe_membership, universe_id)
    node_index = _node_map(store)
    edge_keys = _edge_keys(store)
    nodes_before = len(node_index)
    edges_before = len(edge_keys)

    added_company_nodes = 0
    updated_company_nodes = 0
    for code in tickers:
        existed_before = code in node_index
        changed = _upsert_company_node(
            store,
            node_index,
            code,
            _stock_name(code, security_index, membership_names),
            industry=str(security_index.get(code, {}).get("industry_l1", "")).strip(),
            as_of_date=as_of_date,
        )
        if not existed_before:
            added_company_nodes += 1
        elif changed:
            updated_company_nodes += 1

    industry_map: Dict[str, str] = {}
    concept_map: Dict[str, List[str]] = {}
    belongs_to_added = 0
    competes_with_added = 0
    concept_edges_added = 0

    if collect_graph:
        industry_map = _resolve_industry_map(tickers, security_index, refresh_graph=refresh_graph)
        grouped_by_industry: Dict[str, List[str]] = defaultdict(list)
        for code, industry in industry_map.items():
            if industry:
                grouped_by_industry[industry].append(code)
                changed = _upsert_company_node(
                    store,
                    node_index,
                    code,
                    _stock_name(code, security_index, membership_names),
                    industry=industry,
                    as_of_date=as_of_date,
                )
                if changed:
                    updated_company_nodes += 1

        for industry, members in grouped_by_industry.items():
            store.add_node(
                _GRAPH_ID,
                industry,
                labels=["Industry"],
                summary=f"A股行业板块: {industry}",
                created_at=f"{as_of_date}T00:00:00",
                valid_from=as_of_date,
                industry_name=industry,
            )
            members = sorted(set(members))
            for code in members:
                if _add_edge_once(
                    store,
                    edge_keys,
                    code,
                    industry,
                    "BELONGS_TO",
                    fact=f"{code} belongs to industry {industry}",
                    weight=0.9,
                    confidence=0.95,
                    as_of_date=as_of_date,
                    industry=industry,
                ):
                    belongs_to_added += 1
            for idx, code in enumerate(members):
                for peer in members[idx + 1: idx + 1 + max(peer_limit, 0)]:
                    if _add_edge_once(
                        store,
                        edge_keys,
                        code,
                        peer,
                        "COMPETES_WITH",
                        fact=f"{code} and {peer} compete within {industry}",
                        weight=0.42,
                        confidence=0.78,
                        as_of_date=as_of_date,
                        shared_industry=industry,
                    ):
                        competes_with_added += 1

        concept_map = _resolve_concept_map(tickers, top_concepts=top_concepts)
        for concept_name, members in concept_map.items():
            store.add_node(
                _GRAPH_ID,
                concept_name,
                labels=["Concept"],
                summary=f"A股概念板块: {concept_name}",
                created_at=f"{as_of_date}T00:00:00",
                valid_from=as_of_date,
                concept_name=concept_name,
            )
            for code in members:
                if _add_edge_once(
                    store,
                    edge_keys,
                    code,
                    concept_name,
                    "RELATES_TO_CONCEPT",
                    fact=f"{code} belongs to concept {concept_name}",
                    weight=0.65,
                    confidence=0.82,
                    as_of_date=as_of_date,
                    concept=concept_name,
                ):
                    concept_edges_added += 1

    graph_file = store.save(_GRAPH_ID)
    final_nodes = store.get_all_nodes(_GRAPH_ID)
    final_edges = store.get_all_edges(_GRAPH_ID)
    final_edge_keys = _edge_keys(store)
    coverage_codes = {
        str(node.get("name", "")).strip()
        for node in final_nodes
        if str(node.get("name", "")).isdigit() and len(str(node.get("name", "")).strip()) == 6
    }
    edge_relation_count: Dict[str, int] = defaultdict(int)
    for edge in final_edges:
        source = str(edge.get("source_name") or edge.get("source") or "").strip()
        target = str(edge.get("target_name") or edge.get("target") or "").strip()
        if source in coverage_codes:
            edge_relation_count[source] += 1
        if target in coverage_codes:
            edge_relation_count[target] += 1

    rows = []
    for code in tickers:
        rows.append(
            {
                "ticker": code,
                "company_name": _stock_name(code, security_index, membership_names),
                "has_graph_node": code in coverage_codes,
                "graph_relation_count": int(edge_relation_count.get(code, 0)),
                "industry_l1": industry_map.get(code, str(security_index.get(code, {}).get("industry_l1", "")).strip()),
                "concept_count": sum(1 for members in concept_map.values() if code in members),
            }
        )

    payload = {
        "summary": {
            "universe_id": universe_id,
            "as_of_date": as_of_date,
            "requested_tickers": len(tickers),
            "graph_covered_tickers": sum(1 for row in rows if row["has_graph_node"]),
            "graph_connected_tickers": sum(1 for row in rows if row["graph_relation_count"] > 0),
            "added_company_nodes": added_company_nodes,
            "updated_company_nodes": updated_company_nodes,
            "industry_mapped_tickers": len([code for code, industry in industry_map.items() if industry]),
            "industry_nodes_added": len(set(industry_map.values()) - {""}),
            "concept_nodes_added": len(concept_map),
            "belongs_to_edges_added": belongs_to_added,
            "competes_with_edges_added": competes_with_added,
            "concept_edges_added": concept_edges_added,
            "nodes_before": nodes_before,
            "nodes_after": len(final_nodes),
            "edges_before": edges_before,
            "edges_after": len(final_edge_keys),
            "graph_path": str(graph_file),
        },
        "rows": rows,
    }
    logger.info(
        "Built graph layer for %s: covered=%d, connected=%d, edges_added=%d",
        universe_id,
        payload["summary"]["graph_covered_tickers"],
        payload["summary"]["graph_connected_tickers"],
        payload["summary"]["edges_after"] - payload["summary"]["edges_before"],
    )
    return payload


def save_graph_layer(payload: Dict[str, Any]) -> Dict[str, Path]:
    root = _graph_data_root()
    daily_path = _daily_dir() / f"{payload['summary']['as_of_date']}.json"
    manifest_path = _manifest_path()
    serialized = json.dumps(payload, ensure_ascii=False, indent=2)
    daily_path.write_text(serialized, encoding="utf-8")
    manifest_path.write_text(serialized, encoding="utf-8")
    logger.info("Saved graph layer: %s", manifest_path)
    return {"daily": daily_path, "manifest": manifest_path, "graph": graph_path()}
