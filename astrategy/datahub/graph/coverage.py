"""Expand graph coverage for a target universe and persist audit-friendly manifests."""

from __future__ import annotations

import json
import logging
import re
import time
import warnings
from collections import Counter, defaultdict
from datetime import date
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set, Tuple

from astrategy.graph.local_store import LocalGraphStore

from ..common import ensure_dir, graph_manifest_path, graph_path, graph_root, repo_relative_path

logger = logging.getLogger("astrategy.datahub.graph.coverage")
_GRAPH_ID = "supply_chain"
_GENERATED_RELATIONS = {"BELONGS_TO", "COMPETES_WITH", "RELATES_TO_CONCEPT"}
_GENERATED_LABELS = {"Industry", "Concept"}
_EVENT_TYPE_CONCEPTS = {
    "product_launch": "product_launch",
    "capacity_expansion": "capacity_expansion",
    "capital_raise": "capital_raise",
    "dividend": "dividend",
    "asset_sale": "asset_sale",
    "supply_shortage": "supply_shortage",
    "policy_risk": "policy_risk",
    "policy_support": "policy_support",
    "buyback": "buyback",
    "share_pledge": "share_pledge",
    "ma": "ma",
    "order_win": "order_win",
    "cooperation": "cooperation",
    "guarantee": "guarantee",
    "technology_breakthrough": "technology_breakthrough",
    "earnings_surprise": "earnings_surprise",
    "management_change": "management_change",
    "price_adjustment": "price_adjustment",
    "sentiment_reversal": "sentiment_reversal",
}
_SENTIMENT_CONCEPTS = {
    "bullish": "positive_sentiment",
    "bearish": "negative_sentiment",
}


def _normalize_date(value: str | None) -> str:
    if not value:
        return date.today().isoformat()
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text[:10]


def _safe(fn, *args, failures: List[Dict[str, Any]] | None = None, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        logger.warning("Graph API call failed: %s: %s", fn.__name__, str(exc)[:160])
        if failures is not None:
            failures.append({
                "api": fn.__name__,
                "error": str(exc)[:300],
            })
        return None


def _graph_data_root() -> Path:
    return ensure_dir(graph_root())


def _daily_dir() -> Path:
    return ensure_dir(_graph_data_root() / "daily")


def _manifest_path() -> Path:
    return graph_manifest_path()


def _profile_cache_path() -> Path:
    return _graph_data_root() / "stock_profile_cache.json"


def _concept_cache_path() -> Path:
    return _graph_data_root() / "concept_membership_cache.json"


def _load_json_dict(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_profile_cache() -> Dict[str, Dict[str, Any]]:
    payload = _load_json_dict(_profile_cache_path())
    return payload if isinstance(payload, dict) else {}


def _save_profile_cache(cache: Dict[str, Dict[str, Any]]) -> Path:
    path = _profile_cache_path()
    path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _load_concept_cache() -> Dict[str, List[str]]:
    payload = _load_json_dict(_concept_cache_path())
    concept_map: Dict[str, List[str]] = {}
    for concept, members in payload.items():
        if not isinstance(concept, str):
            continue
        if isinstance(members, list):
            codes = sorted({
                str(code).zfill(6)
                for code in members
                if str(code).strip().isdigit() and len(str(code).strip()) <= 6
            })
            if codes:
                concept_map[concept] = codes
    return concept_map


def _save_concept_cache(concept_map: Dict[str, List[str]]) -> Path:
    path = _concept_cache_path()
    path.write_text(json.dumps(concept_map, ensure_ascii=False, indent=2), encoding="utf-8")
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


def _reset_generated_subgraph(store: LocalGraphStore) -> None:
    graph = store._ensure(_GRAPH_ID)
    graph.edges = [
        edge
        for edge in graph.edges
        if str(edge.get("relation", "")).strip() not in _GENERATED_RELATIONS
    ]
    graph.facts = [
        fact
        for fact in graph.facts
        if str(fact.get("relation", "")).strip() not in _GENERATED_RELATIONS
    ]
    graph.nodes = {
        name: node
        for name, node in graph.nodes.items()
        if not (set(node.get("labels", []) or []) & _GENERATED_LABELS)
    }


def _load_store(*, refresh_graph: bool = False) -> LocalGraphStore:
    store = LocalGraphStore()
    if not store.load(_GRAPH_ID):
        store.create_graph(_GRAPH_ID)
    elif refresh_graph:
        _reset_generated_subgraph(store)
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


def _is_stock_code(value: str) -> bool:
    return value.isdigit() and len(value) == 6


def _node_attr(node_index: Dict[str, Dict[str, Any]], code: str, key: str) -> str:
    node = node_index.get(code, {})
    attrs = dict(node.get("attributes", {}) or {})
    value = str(attrs.get(key, "")).strip()
    if value:
        return value
    return str(node.get(key, "")).strip()


def _ths_headers(board_type: str) -> Dict[str, str]:
    import akshare as ak

    source_fn = ak.stock_board_industry_name_ths if board_type == "industry" else ak.stock_board_concept_name_ths
    js_code = source_fn.__globals__["py_mini_racer"].MiniRacer()
    js_content = source_fn.__globals__["_get_file_content_ths"]("ths.js")
    js_code.eval(js_content)
    v_code = js_code.call("v")
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": "https://q.10jqka.com.cn/",
        "Cookie": f"v={v_code}",
    }


def _ths_board_url(board_type: str, board_code: str, page: int) -> str:
    base = "thshy" if board_type == "industry" else "gn"
    suffix = f"/page/{page}/ajax/1/" if page > 1 else "/"
    return f"https://q.10jqka.com.cn/{base}/detail/code/{board_code}{suffix}"


def _ths_total_pages(html: str) -> int:
    match = re.search(r'page_info">\s*\d+\s*/\s*(\d+)\s*<', html)
    if match:
        return max(1, int(match.group(1)))
    pages = [int(value) for value in re.findall(r'class="changePage"[^>]*page="(\d+)"', html)]
    return max(pages or [1])


def _ths_table_codes(html: str) -> List[str]:
    import pandas as pd

    try:
        tables = pd.read_html(StringIO(html))
    except ValueError:
        return []
    for table in tables:
        if "代码" not in table.columns:
            continue
        return sorted(
            {
                str(value).split(".")[0].zfill(6)
                for value in table["代码"].tolist()
                if _is_stock_code(str(value).split(".")[0].zfill(6))
            }
        )
    return []


def _fetch_ths_board_members(
    board_type: str,
    board_name: str,
    board_code: str,
    ticker_set: Set[str],
    *,
    headers: Dict[str, str],
    failures: List[Dict[str, Any]] | None = None,
) -> tuple[List[str], Dict[str, int]]:
    import requests

    matches: Set[str] = set()
    request_count = 0
    failed_pages = 0
    page_count = 1

    session = requests.Session()
    page = 1
    while page <= page_count:
        url = _ths_board_url(board_type, board_code, page)
        request_count += 1
        try:
            response = session.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            html = response.text
        except Exception as exc:
            failed_pages += 1
            logger.warning("THS %s board fetch failed for %s page %s: %s", board_type, board_name, page, str(exc)[:160])
            if failures is not None:
                failures.append(
                    {
                        "api": f"ths_{board_type}_detail",
                        "board": board_name,
                        "page": page,
                        "error": str(exc)[:300],
                    }
                )
            page += 1
            continue

        if page == 1:
            page_count = _ths_total_pages(html)
        matches.update(ticker_set & set(_ths_table_codes(html)))
        time.sleep(0.03)
        page += 1
    return sorted(matches), {
        "request_count": request_count,
        "failed_pages": failed_pages,
        "page_count": page_count,
    }


def _ordered_ths_concept_boards(
    top_concepts: int,
    *,
    failures: List[Dict[str, Any]] | None = None,
) -> tuple[List[Tuple[str, str]], Dict[str, int]]:
    import pandas as pd

    diagnostics = defaultdict(int)
    if top_concepts <= 0:
        diagnostics["selected_board_count"] = 0
        return [], dict(diagnostics)

    try:
        import akshare as ak
    except Exception as exc:
        logger.warning("akshare unavailable for THS concept boards: %s", exc)
        diagnostics["akshare_unavailable"] += 1
        return [], dict(diagnostics)

    name_df = _safe(ak.stock_board_concept_name_ths, failures=failures)
    if name_df is None or name_df.empty:
        diagnostics["selected_board_count"] = 0
        return [], dict(diagnostics)

    name_to_code = {
        str(row.get("name", "")).strip(): str(row.get("code", "")).strip()
        for _, row in name_df.iterrows()
        if str(row.get("name", "")).strip() and str(row.get("code", "")).strip()
    }
    selected: List[Tuple[str, str]] = []
    selected_names: Set[str] = set()

    summary_df = _safe(ak.stock_board_concept_summary_ths, failures=failures)
    if summary_df is not None and not summary_df.empty and "概念名称" in summary_df.columns:
        if "日期" in summary_df.columns:
            summary_df = summary_df.assign(
                _sort_date=pd.to_datetime(summary_df["日期"], errors="coerce")
            )
        else:
            summary_df = summary_df.assign(_sort_date=pd.NaT)
        if "成分股数量" in summary_df.columns:
            summary_df = summary_df.assign(
                _component_count=pd.to_numeric(summary_df["成分股数量"], errors="coerce").fillna(0)
            )
        else:
            summary_df = summary_df.assign(_component_count=0)
        summary_df = summary_df.sort_values(
            by=["_sort_date", "_component_count", "概念名称"],
            ascending=[False, False, True],
        )
        for _, row in summary_df.iterrows():
            concept_name = str(row.get("概念名称", "")).strip()
            board_code = name_to_code.get(concept_name, "")
            if not concept_name or not board_code or concept_name in selected_names:
                continue
            selected.append((concept_name, board_code))
            selected_names.add(concept_name)
            diagnostics["selected_from_summary"] += 1
            if len(selected) >= top_concepts:
                break

    if len(selected) < top_concepts:
        for _, row in name_df.iterrows():
            concept_name = str(row.get("name", "")).strip()
            board_code = str(row.get("code", "")).strip()
            if not concept_name or not board_code or concept_name in selected_names:
                continue
            selected.append((concept_name, board_code))
            selected_names.add(concept_name)
            diagnostics["selected_from_name_map"] += 1
            if len(selected) >= top_concepts:
                break

    diagnostics["selected_board_count"] = len(selected)
    return selected, dict(diagnostics)


def _normalize_entity_codes(values: Iterable[Any], ticker_set: Set[str]) -> Set[str]:
    return {
        str(value).zfill(6)
        for value in values
        if _is_stock_code(str(value).zfill(6)) and str(value).zfill(6) in ticker_set
    }


def _resolve_live_topic_concepts(
    tickers: Iterable[str],
    *,
    event_payload: Dict[str, Any] | None = None,
    sentiment_payload: Dict[str, Any] | None = None,
    top_concepts: int = 40,
) -> tuple[Dict[str, List[str]], Dict[str, int]]:
    ticker_set = {str(code).zfill(6) for code in tickers if _is_stock_code(str(code).zfill(6))}
    concept_map: Dict[str, Set[str]] = defaultdict(set)
    diagnostics = defaultdict(int)
    live_mapped: Set[str] = set()

    if event_payload:
        for row in event_payload.get("rows", []):
            event_type = str(row.get("event_type", row.get("type", ""))).strip()
            entity_codes = _normalize_entity_codes(row.get("entity_codes", []), ticker_set)
            if not entity_codes:
                continue
            concept_name = _EVENT_TYPE_CONCEPTS.get(event_type, "")
            if concept_name:
                concept_map[concept_name].update(entity_codes)
                live_mapped.update(entity_codes)
                diagnostics["from_event_payload"] += len(entity_codes)
            for theme in row.get("theme_tags", []) or []:
                theme_name = str(theme).strip()
                if not theme_name:
                    continue
                concept_map[f"theme:{theme_name}"].update(entity_codes)
                live_mapped.update(entity_codes)
                diagnostics["from_event_theme_tags"] += len(entity_codes)

    if sentiment_payload:
        for row in sentiment_payload.get("rows", []):
            ticker = str(row.get("ticker", "")).zfill(6)
            if ticker not in ticker_set:
                continue
            label = str(row.get("sentiment_label", "")).strip()
            hot_rank = int(row.get("hot_rank", 0) or 0)
            attention_score = float(row.get("attention_score", 0.0) or 0.0)
            if label in _SENTIMENT_CONCEPTS:
                concept_map[_SENTIMENT_CONCEPTS[label]].add(ticker)
                live_mapped.add(ticker)
                diagnostics["from_sentiment_label"] += 1
            if hot_rank > 0:
                concept_map["market_attention"].add(ticker)
                live_mapped.add(ticker)
                diagnostics["from_hot_rank"] += 1
            if attention_score >= 0.55:
                concept_map["high_attention"].add(ticker)
                live_mapped.add(ticker)
                diagnostics["from_attention_score"] += 1

    ordered_items = sorted(
        concept_map.items(),
        key=lambda item: (-len(item[1]), item[0]),
    )
    if top_concepts > 0:
        ordered_items = ordered_items[:top_concepts]
    normalized = {
        concept: sorted(members)
        for concept, members in ordered_items
        if members
    }
    diagnostics["source"] = "live_event_sentiment_topics" if normalized else "none"
    diagnostics["live_mapped_tickers"] = len(
        {code for members in normalized.values() for code in members}
    )
    diagnostics["fallback_mapped_tickers"] = 0
    diagnostics["request_count"] = 0
    diagnostics["concept_count"] = len(normalized)
    return normalized, dict(diagnostics)


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


def _upsert_taxonomy_node(
    store: LocalGraphStore,
    node_index: Dict[str, Dict[str, Any]],
    node_name: str,
    label: str,
    summary: str,
    *,
    as_of_date: str = "",
    **attrs,
) -> None:
    existing = node_index.get(node_name, {})
    labels = sorted(set(existing.get("labels", []) or []) | {label})
    store.add_node(
        _GRAPH_ID,
        node_name,
        labels=labels,
        summary=str(existing.get("summary", "")).strip() or summary,
        created_at=str(existing.get("created_at", "")).strip() or f"{as_of_date}T00:00:00",
        valid_from=str(existing.get("valid_from", "")).strip() or as_of_date,
        valid_to=str(existing.get("valid_to", "")).strip(),
        **({**dict(existing.get("attributes", {}) or {}), **attrs}),
    )
    node_index[node_name] = {
        "name": node_name,
        "labels": labels,
        "summary": str(existing.get("summary", "")).strip() or summary,
        "attributes": {**dict(existing.get("attributes", {}) or {}), **attrs},
        "created_at": str(existing.get("created_at", "")).strip() or f"{as_of_date}T00:00:00",
        "valid_from": str(existing.get("valid_from", "")).strip() or as_of_date,
        "valid_to": str(existing.get("valid_to", "")).strip(),
    }


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


def _existing_industry_map(node_index: Dict[str, Dict[str, Any]], tickers: Iterable[str]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for code in tickers:
        industry = _node_attr(node_index, code, "industry")
        if industry:
            mapping[code] = industry
    return mapping


def _existing_concept_map(store: LocalGraphStore, tickers: Iterable[str]) -> Dict[str, List[str]]:
    ticker_set = set(tickers)
    concept_members: Dict[str, Set[str]] = defaultdict(set)
    for edge in store.get_all_edges(_GRAPH_ID):
        relation = str(edge.get("relation", "")).strip()
        if relation != "RELATES_TO_CONCEPT":
            continue
        source = str(edge.get("source_name") or edge.get("source") or "").strip()
        target = str(edge.get("target_name") or edge.get("target") or "").strip()
        if source in ticker_set and target:
            concept_members[target].add(source)
    return {
        concept: sorted(members)
        for concept, members in concept_members.items()
        if members
    }


def _resolve_industry_map(
    tickers: Iterable[str],
    security_index: Dict[str, Dict[str, Any]],
    node_index: Dict[str, Dict[str, Any]],
    *,
    refresh_graph: bool = False,
    failures: List[Dict[str, Any]] | None = None,
) -> tuple[Dict[str, str], Dict[str, int]]:
    tickers = sorted(set(tickers))
    industry_map: Dict[str, str] = {}
    diagnostics = defaultdict(int)
    live_mapped: Set[str] = set()
    fallback_mapped: Set[str] = set()
    cache = _load_profile_cache()

    try:
        import akshare as ak
    except Exception as exc:
        logger.warning("akshare unavailable for CNInfo industry mapping: %s", exc)
        diagnostics["akshare_unavailable"] += 1
    else:
        start_date = "20240101"
        end_date = _normalize_date(date.today().strftime("%Y%m%d")).replace("-", "")
        for code in tickers:
            diagnostics["request_count"] += 1
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", FutureWarning)
                    df = ak.stock_industry_change_cninfo(
                        symbol=code,
                        start_date=start_date,
                        end_date=end_date,
                    )
            except Exception as exc:
                logger.warning("CNInfo industry fetch failed for %s: %s", code, str(exc)[:160])
                if failures is not None:
                    failures.append(
                        {
                            "api": "stock_industry_change_cninfo",
                            "ticker": code,
                            "error": str(exc)[:300],
                        }
                    )
                continue
            if df is None or df.empty:
                continue
            df = df.sort_values("变更日期")
            latest = df.iloc[-1]
            industry = (
                str(latest.get("行业大类", "")).strip()
                or str(latest.get("行业中类", "")).strip()
                or str(latest.get("行业门类", "")).strip()
            )
            if not industry:
                continue
            industry_map[code] = industry
            live_mapped.add(code)
            diagnostics["from_cninfo_api"] += 1
            cache[code] = {
                **dict(cache.get(code, {}) or {}),
                "industry": industry,
                "fetched_at": date.today().isoformat(),
                "source": "cninfo_live",
            }
            time.sleep(0.02)

    unresolved = [code for code in tickers if code not in industry_map]
    for code in unresolved:
        industry = str(security_index.get(code, {}).get("industry_l1", "")).strip()
        if industry:
            industry_map[code] = industry
            fallback_mapped.add(code)
            diagnostics["from_security_master"] += 1

    if not refresh_graph:
        existing_map = _existing_industry_map(node_index, tickers)
        for code, industry in existing_map.items():
            if code not in industry_map and industry:
                industry_map[code] = industry
                fallback_mapped.add(code)
                diagnostics["from_local_graph"] += 1

        for code in tickers:
            cached = cache.get(code, {})
            industry = str(cached.get("industry", "")).strip()
            if code not in industry_map and industry:
                industry_map[code] = industry
                fallback_mapped.add(code)
                diagnostics["from_profile_cache"] += 1
    for code, industry in industry_map.items():
        cache[code] = {
            **dict(cache.get(code, {}) or {}),
            "industry": industry,
            "fetched_at": date.today().isoformat(),
            "source": "cninfo_live" if code in live_mapped else "fallback",
        }
    if cache:
        _save_profile_cache(cache)

    diagnostics["source"] = "cninfo_live" if live_mapped else "fallback"
    diagnostics["live_mapped_tickers"] = len(live_mapped)
    diagnostics["fallback_mapped_tickers"] = len(fallback_mapped)
    diagnostics["resolved_total"] = len([code for code, industry in industry_map.items() if industry])
    diagnostics["missing_after_resolution"] = len([code for code in tickers if code not in industry_map])
    return industry_map, dict(diagnostics)


def _resolve_concept_map(
    tickers: Iterable[str],
    existing_concepts: Dict[str, List[str]],
    *,
    top_concepts: int = 40,
    refresh_graph: bool = False,
    event_payload: Dict[str, Any] | None = None,
    sentiment_payload: Dict[str, Any] | None = None,
    failures: List[Dict[str, Any]] | None = None,
) -> tuple[Dict[str, List[str]], Dict[str, int]]:
    live_map, live_diag = _resolve_live_topic_concepts(
        tickers,
        event_payload=event_payload,
        sentiment_payload=sentiment_payload,
        top_concepts=top_concepts,
    )
    if live_map:
        _save_concept_cache(live_map)
        return live_map, live_diag

    ticker_set = set(tickers)
    diagnostics = defaultdict(int)
    concept_map: Dict[str, Set[str]] = defaultdict(set)
    fallback_mapped: Set[str] = set()

    if not refresh_graph:
        for concept_name, members in existing_concepts.items():
            matches = ticker_set & {str(code).zfill(6) for code in members}
            if matches:
                concept_map[concept_name].update(matches)
                fallback_mapped.update(matches)
                diagnostics["from_local_graph"] += len(matches)

        cached_map = _load_concept_cache()
        for concept_name, members in cached_map.items():
            matches = ticker_set & {str(code).zfill(6) for code in members}
            if matches:
                concept_map[concept_name].update(matches)
                fallback_mapped.update(matches)
                diagnostics["from_concept_cache"] += len(matches)

    normalized = {
        concept: sorted(members)
        for concept, members in sorted(
            concept_map.items(),
            key=lambda item: (-len(item[1]), item[0]),
        )[: max(top_concepts, 0) or None]
        if members
    }
    if normalized:
        _save_concept_cache(normalized)
    diagnostics["source"] = "cache" if normalized else "none"
    diagnostics["live_mapped_tickers"] = 0
    diagnostics["fallback_mapped_tickers"] = len(fallback_mapped)
    diagnostics["request_count"] = 0
    diagnostics["concept_count"] = len(normalized)
    return normalized, dict(diagnostics)


def _fallback_narrative_concepts(tickers: Iterable[str]) -> Dict[str, List[str]]:
    try:
        from astrategy.strategies.s11_narrative_tracker import NARRATIVES
    except Exception:
        return {}
    ticker_set = set(tickers)
    concept_map: Dict[str, List[str]] = {}
    for concept_name, narrative in NARRATIVES.items():
        members = [
            str(code).zfill(6)
            for code in narrative.get("representative_stocks", [])
            if str(code).zfill(6) in ticker_set
        ]
        if members:
            concept_map[concept_name] = sorted(set(members))
    return concept_map


def _code_neighbor_map(edges: Iterable[Dict[str, Any]]) -> Dict[str, Set[str]]:
    neighbors: Dict[str, Set[str]] = defaultdict(set)
    for edge in edges:
        source = str(edge.get("source_name") or edge.get("source") or "").strip()
        target = str(edge.get("target_name") or edge.get("target") or "").strip()
        if _is_stock_code(source) and target:
            neighbors[source].add(target)
        if _is_stock_code(target) and source:
            neighbors[target].add(source)
    return neighbors


def _concept_lookup(concept_map: Dict[str, List[str]]) -> Dict[str, Set[str]]:
    lookup: Dict[str, Set[str]] = defaultdict(set)
    for concept_name, members in concept_map.items():
        for code in members:
            lookup[code].add(concept_name)
    return lookup


def _relation_stats(final_edges: Iterable[Dict[str, Any]]) -> tuple[Dict[str, int], Dict[str, Counter]]:
    relation_count: Dict[str, int] = defaultdict(int)
    relation_types: Dict[str, Counter] = defaultdict(Counter)
    for edge in final_edges:
        relation = str(edge.get("relation", "")).strip()
        source = str(edge.get("source_name") or edge.get("source") or "").strip()
        target = str(edge.get("target_name") or edge.get("target") or "").strip()
        if _is_stock_code(source):
            relation_count[source] += 1
            relation_types[source][relation] += 1
        if _is_stock_code(target):
            relation_count[target] += 1
            relation_types[target][relation] += 1
    return dict(relation_count), relation_types


def _pair_key(code_a: str, code_b: str) -> tuple[str, str]:
    return tuple(sorted((code_a, code_b)))


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
    event_payload: Dict[str, Any] | None = None,
    sentiment_payload: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Expand the local graph for one universe and emit a manifest."""
    as_of_date = _normalize_date(as_of_date)
    failures: List[Dict[str, Any]] = []
    store = _load_store(refresh_graph=refresh_graph)
    security_index = _build_security_index(security_master)
    tickers = _universe_codes(universe_membership, universe_id)
    membership_names = _membership_name_map(universe_membership, universe_id)
    node_index = _node_map(store)
    edge_keys = _edge_keys(store)
    nodes_before = len(node_index)
    edges_before = len(edge_keys)
    existing_edges = list(store.get_all_edges(_GRAPH_ID))

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
    industry_diag: Dict[str, int] = {}
    concept_map: Dict[str, List[str]] = {}
    concept_diag: Dict[str, int] = {}
    belongs_to_added = 0
    competes_with_added = 0
    concept_edges_added = 0
    peer_pairs_added = 0
    fallback_counts = defaultdict(int)

    if collect_graph:
        industry_map, industry_diag = _resolve_industry_map(
            tickers,
            security_index,
            node_index,
            refresh_graph=refresh_graph,
            failures=failures,
        )
        existing_concepts = _existing_concept_map(store, tickers)
        concept_map, concept_diag = _resolve_concept_map(
            tickers,
            existing_concepts,
            top_concepts=top_concepts,
            refresh_graph=refresh_graph,
            event_payload=event_payload,
            sentiment_payload=sentiment_payload,
            failures=failures,
        )

        fallback_counts["industry"] += int(industry_diag.get("fallback_mapped_tickers", 0) or 0)
        fallback_counts["concept"] += int(concept_diag.get("fallback_mapped_tickers", 0) or 0)

        grouped_by_industry: Dict[str, List[str]] = defaultdict(list)
        for code, industry in industry_map.items():
            if not industry:
                continue
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

        code_to_concepts = _concept_lookup(concept_map)
        neighbor_map = _code_neighbor_map(existing_edges)

        for industry, members in grouped_by_industry.items():
            _upsert_taxonomy_node(
                store,
                node_index,
                industry,
                "Industry",
                f"A股行业板块: {industry}",
                as_of_date=as_of_date,
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
                    weight=0.92,
                    confidence=0.96,
                    as_of_date=as_of_date,
                    industry=industry,
                ):
                    belongs_to_added += 1

        peer_pair_keys: Set[Tuple[str, str]] = set()
        for industry, members in grouped_by_industry.items():
            members = sorted(set(members))
            for code in members:
                scored_candidates: List[tuple[float, str, Dict[str, Any]]] = []
                own_concepts = code_to_concepts.get(code, set())
                own_neighbors = neighbor_map.get(code, set())
                for peer in members:
                    if peer == code:
                        continue
                    pair_key = _pair_key(code, peer)
                    if pair_key in peer_pair_keys:
                        continue
                    shared_concepts = sorted(own_concepts & code_to_concepts.get(peer, set()))
                    shared_neighbors = sorted(own_neighbors & neighbor_map.get(peer, set()))
                    score = 1.0 + 0.55 * len(shared_concepts) + 0.25 * len(shared_neighbors)
                    if not shared_concepts and not shared_neighbors:
                        score -= 0.15
                    scored_candidates.append((
                        score,
                        peer,
                        {
                            "shared_concepts": shared_concepts[:5],
                            "shared_neighbors": shared_neighbors[:6],
                        },
                    ))
                scored_candidates.sort(key=lambda item: (-item[0], item[1]))
                for score, peer, evidence in scored_candidates[: max(peer_limit, 0)]:
                    source, target = _pair_key(code, peer)
                    if _add_edge_once(
                        store,
                        edge_keys,
                        source,
                        target,
                        "COMPETES_WITH",
                        fact=f"{source} and {target} compete within {industry}",
                        weight=round(min(0.95, 0.35 + 0.08 * score), 4),
                        confidence=round(min(0.95, 0.68 + 0.04 * score), 4),
                        as_of_date=as_of_date,
                        shared_industry=industry,
                        shared_concepts=evidence["shared_concepts"],
                        shared_neighbors=evidence["shared_neighbors"],
                    ):
                        competes_with_added += 1
                        peer_pairs_added += 1
                        peer_pair_keys.add((source, target))

        for concept_name, members in concept_map.items():
            _upsert_taxonomy_node(
                store,
                node_index,
                concept_name,
                "Concept",
                f"A股概念板块: {concept_name}",
                as_of_date=as_of_date,
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
                    weight=0.7,
                    confidence=0.84,
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
        if _is_stock_code(str(node.get("name", "")).strip())
    }
    relation_count, relation_types = _relation_stats(final_edges)
    code_to_concepts = _concept_lookup(concept_map) if concept_map else _concept_lookup(_existing_concept_map(store, tickers))

    rows = []
    for code in tickers:
        graph_relation_count = int(relation_count.get(code, 0))
        rows.append(
            {
                "ticker": code,
                "company_name": _stock_name(code, security_index, membership_names),
                "has_graph_node": code in coverage_codes,
                "graph_relation_count": graph_relation_count,
                "has_graph_edges": graph_relation_count > 0,
                "industry_l1": industry_map.get(code, str(security_index.get(code, {}).get("industry_l1", "")).strip()),
                "concept_count": len(code_to_concepts.get(code, set())),
                "peer_count": int(relation_types.get(code, Counter()).get("COMPETES_WITH", 0)),
                "top_relations": dict(relation_types.get(code, Counter()).most_common(5)),
            }
        )

    payload = {
        "summary": {
            "universe_id": universe_id,
            "as_of_date": as_of_date,
            "requested_tickers": len(tickers),
            "graph_covered_tickers": sum(1 for row in rows if row["has_graph_node"]),
            "graph_connected_tickers": sum(1 for row in rows if row["has_graph_edges"]),
            "added_company_nodes": added_company_nodes,
            "updated_company_nodes": updated_company_nodes,
            "industry_source": str(industry_diag.get("source", "none")),
            "concept_source": str(concept_diag.get("source", "none")),
            "industry_mapped_tickers": len([code for code, industry in industry_map.items() if industry]),
            "concept_mapped_tickers": len([code for code, concepts in code_to_concepts.items() if concepts]),
            "industry_live_mapped_tickers": int(industry_diag.get("live_mapped_tickers", 0) or 0),
            "concept_live_mapped_tickers": int(concept_diag.get("live_mapped_tickers", 0) or 0),
            "industry_fallback_mapped_tickers": int(industry_diag.get("fallback_mapped_tickers", 0) or 0),
            "concept_fallback_mapped_tickers": int(concept_diag.get("fallback_mapped_tickers", 0) or 0),
            "industry_nodes_added": len({industry for industry in industry_map.values() if industry}),
            "concept_nodes_added": len(concept_map),
            "belongs_to_edges_added": belongs_to_added,
            "competes_with_edges_added": competes_with_added,
            "peer_pairs_added": peer_pairs_added,
            "concept_edges_added": concept_edges_added,
            "industry_request_count": int(industry_diag.get("request_count", industry_diag.get("board_request_count", 0)) or 0),
            "concept_request_count": int(concept_diag.get("request_count", concept_diag.get("board_request_count", 0)) or 0),
            "industry_board_request_count": int(industry_diag.get("board_request_count", 0) or 0),
            "concept_board_request_count": int(concept_diag.get("board_request_count", 0) or 0),
            "nodes_before": nodes_before,
            "nodes_after": len(final_nodes),
            "edges_before": edges_before,
            "edges_after": len(final_edge_keys),
            "graph_path": repo_relative_path(graph_file),
            "graph_manifest_path": repo_relative_path(_manifest_path()),
            "api_failure_count": len(failures),
            "api_failures": failures[:20],
            "fallback_counts": dict(fallback_counts),
            "industry_resolution": industry_diag,
            "concept_resolution": concept_diag,
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
