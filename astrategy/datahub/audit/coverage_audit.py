"""Coverage auditing for the data-foundation layer."""

from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List

from astrategy.events.normalizer import normalize_events

from ..common import (
    audit_root,
    data_root,
    ensure_dir,
    event_master_path,
    filings_manifest_path,
    graph_manifest_path,
    graph_path,
    ingest_root,
    market_root,
    news_manifest_path,
    pool_event_master_path,
    sentiment_manifest_path,
    sentiment_root,
)

logger = logging.getLogger("astrategy.datahub.coverage_audit")


def _load_graph_stats() -> Dict[str, Dict[str, Any]]:
    manifest = graph_manifest_path()
    if manifest.exists():
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        stats: Dict[str, Dict[str, Any]] = {}
        for row in payload.get("rows", []):
            ticker = str(row.get("ticker", "")).strip().zfill(6)
            if not ticker or ticker == "000000":
                continue
            stats[ticker] = {
                "has_graph_node": bool(row.get("has_graph_node", False)),
                "graph_relation_count": int(row.get("graph_relation_count", 0) or 0),
                "has_graph_edges": bool(row.get("has_graph_edges", False)),
                "top_relations": dict(row.get("top_relations", {}) or {}),
            }
        if stats:
            return stats

    path = graph_path()
    if not path.exists():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    nodes = data.get("nodes", {})
    edges = data.get("edges", [])
    if not isinstance(nodes, dict):
        return {}

    stats: Dict[str, Dict[str, Any]] = {}
    for code in nodes:
        if isinstance(code, str) and code.isdigit() and len(code) == 6:
            stats[code] = {
                "has_graph_node": True,
                "graph_relation_count": 0,
                "relation_types": Counter(),
            }

    for edge in edges if isinstance(edges, list) else []:
        relation = str(edge.get("relation", "")).strip()
        for key in ("source_name", "target_name", "source", "target"):
            code = str(edge.get(key, "")).strip()
            if code in stats:
                stats[code]["graph_relation_count"] += 1
                stats[code]["relation_types"][relation] += 1

    normalized: Dict[str, Dict[str, Any]] = {}
    for code, info in stats.items():
        normalized[code] = {
            "has_graph_node": bool(info["has_graph_node"]),
            "graph_relation_count": int(info["graph_relation_count"]),
            "has_graph_edges": int(info["graph_relation_count"]) > 0,
            "top_relations": dict(info["relation_types"].most_common(5)),
        }
    return normalized


def _load_event_stats() -> Dict[str, Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for path in (event_master_path(), pool_event_master_path()):
        if path.exists():
            events.extend(normalize_events(json.loads(path.read_text(encoding="utf-8"))))
    if not events:
        return {}

    stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "event_count": 0,
        "available_at_complete": True,
        "event_types": Counter(),
    })
    for event in events:
        code = str(event.get("stock_code", "")).strip().zfill(6)
        if not code or code == "000000":
            continue
        stats[code]["event_count"] += 1
        stats[code]["event_types"][str(event.get("event_type", "") or event.get("type", "")).strip()] += 1
        if not str(event.get("available_at", "")).strip():
            stats[code]["available_at_complete"] = False

    normalized: Dict[str, Dict[str, Any]] = {}
    for code, info in stats.items():
        normalized[code] = {
            "event_count": info["event_count"],
            "available_at_complete": bool(info["available_at_complete"]),
            "top_event_types": dict(info["event_types"].most_common(5)),
        }
    return normalized


def _load_filings_stats() -> Dict[str, Dict[str, Any]]:
    path = filings_manifest_path()
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    stats: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"filing_count": 0})
    for row in payload.get("rows", []):
        ticker = str(row.get("ticker", "")).strip().zfill(6)
        if ticker and ticker != "000000":
            stats[ticker]["filing_count"] += 1
    return dict(stats)


def _load_news_stats() -> Dict[str, Dict[str, Any]]:
    path = news_manifest_path()
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    stats: Dict[str, Dict[str, Any]] = {}
    for row in payload.get("rows", []):
        ticker = str(row.get("ticker", "")).strip().zfill(6)
        if not ticker or ticker == "000000":
            continue
        stats[ticker] = {
            "news_count": int(row.get("news_count", 0) or 0),
            "target_date_news_count": int(row.get("target_date_news_count", 0) or 0),
        }
    return stats


def _load_sentiment_stats() -> Dict[str, Dict[str, Any]]:
    path = sentiment_manifest_path()
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    stats: Dict[str, Dict[str, Any]] = {}
    for row in payload.get("rows", []):
        ticker = str(row.get("ticker", "")).strip().zfill(6)
        if not ticker or ticker == "000000":
            continue
        stats[ticker] = {
            "sentiment_item_count": int(row.get("sentiment_item_count", 0) or 0),
            "target_date_sentiment_count": int(row.get("target_date_sentiment_count", 0) or 0),
            "avg_sentiment_score": float(row.get("avg_sentiment_score", 0.0) or 0.0),
            "latest_sentiment_score": float(row.get("latest_sentiment_score", 0.0) or 0.0),
            "attention_score": float(row.get("attention_score", 0.0) or 0.0),
            "hot_rank": int(row.get("hot_rank", 0) or 0),
            "sentiment_label": str(row.get("sentiment_label", "")).strip(),
        }
    return stats


def _load_manifest_payload(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _graph_layer_quality() -> Dict[str, Any]:
    payload = _load_manifest_payload(graph_manifest_path())
    summary = payload.get("summary", {})
    requested = int(summary.get("requested_tickers", 0) or 0)
    industry_source = str(summary.get("industry_source", "")).strip()
    concept_source = str(summary.get("concept_source", "")).strip()
    industry_mapped = int(summary.get("industry_live_mapped_tickers", summary.get("industry_mapped_tickers", 0)) or 0)
    concept_mapped = int(summary.get("concept_live_mapped_tickers", summary.get("concept_mapped_tickers", 0)) or 0)
    api_failure_count = int(summary.get("api_failure_count", 0) or 0)
    request_count = (
        int(summary.get("industry_request_count", summary.get("industry_board_request_count", 0)) or 0)
        + int(summary.get("concept_request_count", summary.get("concept_board_request_count", 0)) or 0)
    )
    reasons: List[str] = []
    if industry_source not in {"cninfo_live", "cninfo_cache"}:
        reasons.append("industry_source_not_authoritative")
    if concept_source not in {"ths_live", "live_event_sentiment_topics"}:
        reasons.append("concept_source_not_authoritative")
    failure_threshold = max(10, int(request_count * 0.05)) if request_count else 0
    if api_failure_count > failure_threshold:
        reasons.append("graph_api_failed")
    if requested and industry_mapped < max(1, int(requested * 0.75)):
        reasons.append("industry_live_mapping_insufficient")
    if requested and concept_mapped < max(1, int(requested * 0.20)):
        reasons.append("concept_live_mapping_insufficient")
    return {
        "status": "authoritative" if not reasons else "degraded",
        "reasons": reasons,
        "requested_tickers": requested,
        "industry_source": industry_source,
        "concept_source": concept_source,
        "industry_mapped_tickers": industry_mapped,
        "concept_mapped_tickers": concept_mapped,
        "api_failure_count": api_failure_count,
    }


def _sentiment_layer_quality() -> Dict[str, Any]:
    payload = _load_manifest_payload(sentiment_manifest_path())
    summary = payload.get("summary", {})
    requested = int(summary.get("requested_tickers", 0) or 0)
    with_sentiment = int(summary.get("with_sentiment", 0) or 0)
    with_hot_rank = int(summary.get("authoritative_hot_rank_count", summary.get("with_hot_rank", 0)) or 0)
    hot_topic_source = str(summary.get("hot_topic_source", "")).strip()
    hot_topic_authoritative = bool(summary.get("hot_topic_authoritative", False))
    hot_topic_api_failed = bool(summary.get("hot_topic_api_failed", False))
    reasons: List[str] = []
    if hot_topic_api_failed or not hot_topic_authoritative or hot_topic_source not in {"baidu_xueqiu_live", "baidu_xueqiu_cache"}:
        reasons.append("market_hot_topic_not_authoritative")
    hot_rank_floor = min(200, max(50, int(requested * 0.10))) if requested else 50
    if with_hot_rank < hot_rank_floor:
        reasons.append("hot_rank_coverage_insufficient")
    if requested and with_sentiment < max(1, int(requested * 0.80)):
        reasons.append("sentiment_coverage_insufficient")
    return {
        "status": "authoritative" if not reasons else "degraded",
        "reasons": reasons,
        "requested_tickers": requested,
        "with_sentiment": with_sentiment,
        "with_hot_rank": with_hot_rank,
        "hot_topic_source": hot_topic_source,
        "hot_topic_authoritative": hot_topic_authoritative,
        "hot_topic_api_failed": hot_topic_api_failed,
    }


def _load_price_codes() -> set[str]:
    return {code for code, info in _load_price_stats().items() if int(info.get("row_count", 0)) > 0}


def _load_price_stats() -> Dict[str, Dict[str, Any]]:
    manifest_path = market_root() / "price_manifest.json"
    if not manifest_path.exists():
        return {}

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows = payload.get("rows", [])
    price_stats: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        ticker = str(row.get("ticker", "")).strip().zfill(6)
        if not ticker or ticker == "000000":
            continue
        price_stats[ticker] = {
            "row_count": int(row.get("row_count", 0) or 0),
            "price_start": str(row.get("price_start", "") or ""),
            "price_end": str(row.get("price_end", "") or ""),
            "status": str(row.get("status", "") or ""),
        }
    return price_stats


def _coverage_score(row: Dict[str, Any]) -> float:
    return round(
        0.35 * float(row["has_prices"])
        + 0.20 * float(row["has_events"])
        + 0.10 * float(row["has_filings"])
        + 0.10 * float(row["has_news"])
        + 0.05 * float(row["has_sentiment"])
        + 0.15 * float(row["has_graph"])
        + 0.05 * float(row["has_available_at"]),
        4,
    )


def _strict_coverage_score(
    row: Dict[str, Any],
    *,
    graph_quality: Dict[str, Any],
    sentiment_quality: Dict[str, Any],
) -> float:
    graph_ok = graph_quality.get("status") == "authoritative"
    sentiment_ok = sentiment_quality.get("status") == "authoritative"
    return round(
        0.35 * float(row["has_prices"])
        + 0.20 * float(row["has_events"])
        + 0.10 * float(row["has_filings"])
        + 0.10 * float(row["has_news"])
        + 0.05 * float(row["has_sentiment"] and sentiment_ok)
        + 0.15 * float(row["has_graph"] and row["has_graph_edges"] and graph_ok)
        + 0.05 * float(row["has_available_at"]),
        4,
    )


def build_coverage_audit(
    security_master: List[Dict[str, Any]],
    universe_membership: Dict[str, Any],
    universe_id: str = "csi800",
    *,
    as_of_date: str | None = None,
) -> Dict[str, Any]:
    """Build a first-pass coverage audit for one universe."""
    graph_stats = _load_graph_stats()
    event_stats = _load_event_stats()
    price_stats = _load_price_stats()
    filing_stats = _load_filings_stats()
    news_stats = _load_news_stats()
    sentiment_stats = _load_sentiment_stats()
    graph_quality = _graph_layer_quality()
    sentiment_quality = _sentiment_layer_quality()
    price_codes = {code for code, info in price_stats.items() if int(info.get("row_count", 0)) > 0}

    universe_codes = [
        row["ticker"]
        for row in universe_membership.get("memberships", [])
        if row.get("universe_id") == universe_id
    ]
    membership_name_map = {
        str(row.get("ticker", "")).zfill(6): str(row.get("company_name", "")).strip()
        for row in universe_membership.get("memberships", [])
        if row.get("universe_id") == universe_id
    }
    security_index = {
        str(item["ticker"]).zfill(6): item
        for item in security_master
    }

    rows: List[Dict[str, Any]] = []
    for ticker in universe_codes:
        sec = security_index.get(ticker, {})
        event_info = event_stats.get(ticker, {})
        graph_info = graph_stats.get(ticker, {})
        sentiment_info = sentiment_stats.get(ticker, {})
        row = {
            "ticker": ticker,
            "company_name": sec.get("company_name") or membership_name_map.get(ticker) or ticker,
            "has_prices": ticker in price_codes,
            "price_row_count": int(price_stats.get(ticker, {}).get("row_count", 0)),
            "price_start": str(price_stats.get(ticker, {}).get("price_start", "")),
            "price_end": str(price_stats.get(ticker, {}).get("price_end", "")),
            "has_events": ticker in event_stats,
            "has_filings": int(filing_stats.get(ticker, {}).get("filing_count", 0)) > 0,
            "filing_count": int(filing_stats.get(ticker, {}).get("filing_count", 0)),
            "has_news": int(news_stats.get(ticker, {}).get("news_count", 0)) > 0,
            "news_count": int(news_stats.get(ticker, {}).get("news_count", 0)),
            "target_date_news_count": int(news_stats.get(ticker, {}).get("target_date_news_count", 0)),
            "has_sentiment": (
                int(sentiment_info.get("sentiment_item_count", 0)) > 0
                or int(sentiment_info.get("hot_rank", 0)) > 0
            ),
            "sentiment_item_count": int(sentiment_info.get("sentiment_item_count", 0)),
            "target_date_sentiment_count": int(sentiment_info.get("target_date_sentiment_count", 0)),
            "avg_sentiment_score": float(sentiment_info.get("avg_sentiment_score", 0.0)),
            "latest_sentiment_score": float(sentiment_info.get("latest_sentiment_score", 0.0)),
            "attention_score": float(sentiment_info.get("attention_score", 0.0)),
            "hot_rank": int(sentiment_info.get("hot_rank", 0)),
            "sentiment_label": str(sentiment_info.get("sentiment_label", "")).strip(),
            "has_graph": bool(graph_info.get("has_graph_node", False)),
            "graph_relation_count": int(graph_info.get("graph_relation_count", 0)),
            "has_graph_edges": bool(graph_info.get("has_graph_edges", int(graph_info.get("graph_relation_count", 0)) > 0)),
            "top_graph_relations": graph_info.get("top_relations", {}),
            "graph_layer_quality": graph_quality["status"],
            "graph_quality_reasons": graph_quality["reasons"],
            "sentiment_layer_quality": sentiment_quality["status"],
            "sentiment_quality_reasons": sentiment_quality["reasons"],
            "has_available_at": bool(event_info.get("available_at_complete", False)),
            "event_count": int(event_info.get("event_count", 0)),
            "top_event_types": event_info.get("top_event_types", {}),
        }
        row["coverage_score"] = _coverage_score(row)
        row["strict_coverage_score"] = _strict_coverage_score(
            row,
            graph_quality=graph_quality,
            sentiment_quality=sentiment_quality,
        )
        row["research_ready"] = bool(
            row["has_prices"]
            and row["price_row_count"] >= 20
            and row["has_events"]
            and row["has_graph"]
            and row["has_graph_edges"]
            and row["has_available_at"]
            and row["coverage_score"] >= 0.70
        )
        row["research_ready_strict"] = bool(
            row["research_ready"]
            and graph_quality["status"] == "authoritative"
            and sentiment_quality["status"] == "authoritative"
            and row["strict_coverage_score"] >= 0.70
        )
        rows.append(row)

    summary = {
        "universe_id": universe_id,
        "as_of_date": as_of_date or "",
        "universe_size": len(universe_codes),
        "has_prices": sum(1 for row in rows if row["has_prices"]),
        "price_ready": sum(1 for row in rows if row["price_row_count"] >= 20),
        "has_events": sum(1 for row in rows if row["has_events"]),
        "has_filings": sum(1 for row in rows if row["has_filings"]),
        "has_news": sum(1 for row in rows if row["has_news"]),
        "has_sentiment": sum(1 for row in rows if row["has_sentiment"]),
        "has_graph": sum(1 for row in rows if row["has_graph"]),
        "has_graph_edges": sum(1 for row in rows if row["has_graph_edges"]),
        "has_available_at": sum(1 for row in rows if row["has_available_at"]),
        "research_ready": sum(1 for row in rows if row["research_ready"]),
        "research_ready_strict": sum(1 for row in rows if row["research_ready_strict"]),
        "avg_coverage_score": round(
            sum(float(row["coverage_score"]) for row in rows) / max(len(rows), 1),
            4,
        ),
        "avg_strict_coverage_score": round(
            sum(float(row["strict_coverage_score"]) for row in rows) / max(len(rows), 1),
            4,
        ),
        "event_density_per_stock": round(
            sum(int(row["event_count"]) for row in rows) / max(len(rows), 1),
            4,
        ),
        "graph_layer_quality": graph_quality["status"],
        "graph_quality_reasons": graph_quality["reasons"],
        "sentiment_layer_quality": sentiment_quality["status"],
        "sentiment_quality_reasons": sentiment_quality["reasons"],
        "data_quality_status": (
            "authoritative"
            if graph_quality["status"] == "authoritative" and sentiment_quality["status"] == "authoritative"
            else "degraded"
        ),
    }

    top_event_gaps = sorted(
        [row for row in rows if not row["has_events"]],
        key=lambda row: row["ticker"],
    )[:20]

    audit = {
        "summary": summary,
        "rows": rows,
        "top_event_gaps": top_event_gaps,
    }
    logger.info(
        "Coverage audit built for %s: universe=%d, prices=%d, events=%d, graph=%d, ready=%d",
        universe_id,
        summary["universe_size"],
        summary["has_prices"],
        summary["has_events"],
        summary["has_graph"],
        summary["research_ready"],
    )
    return audit


def _format_markdown(audit: Dict[str, Any]) -> str:
    summary = audit["summary"]
    lines = [
        "# 底层数据覆盖率审计报告",
        "",
        f"- 股票池: {summary['universe_id']}",
        f"- 审计日期: {summary.get('as_of_date', '')}",
        f"- 股票数: {summary['universe_size']}",
        f"- 价格覆盖: {summary['has_prices']}",
        f"- 价格可回测(>=20 bars): {summary['price_ready']}",
        f"- 事件覆盖: {summary['has_events']}",
        f"- filings覆盖: {summary['has_filings']}",
        f"- news覆盖: {summary['has_news']}",
        f"- sentiment覆盖: {summary['has_sentiment']}",
        f"- 图谱覆盖: {summary['has_graph']}",
        f"- 图谱有边覆盖: {summary['has_graph_edges']}",
        f"- available_at 完整: {summary['has_available_at']}",
        f"- research_ready(宽松): {summary['research_ready']}",
        f"- research_ready(严格): {summary['research_ready_strict']}",
        f"- 平均覆盖分: {summary['avg_coverage_score']:.4f}",
        f"- 严格覆盖分: {summary['avg_strict_coverage_score']:.4f}",
        f"- 每股平均事件数: {summary['event_density_per_stock']:.4f}",
        f"- graph层质量: {summary['graph_layer_quality']}",
        f"- graph质量原因: {', '.join(summary.get('graph_quality_reasons', [])) or 'none'}",
        f"- sentiment层质量: {summary['sentiment_layer_quality']}",
        f"- sentiment质量原因: {', '.join(summary.get('sentiment_quality_reasons', [])) or 'none'}",
        f"- 总体数据质量: {summary['data_quality_status']}",
        "",
        "## Top Event Gaps",
        "",
        "| ticker | company | has_graph | coverage_score |",
        "|--------|---------|-----------|----------------|",
    ]
    for row in audit.get("top_event_gaps", []):
        lines.append(
            f"| {row['ticker']} | {row['company_name']} | "
            f"{'Y' if row['has_graph'] else 'N'} | {row['coverage_score']:.4f} |"
        )
    return "\n".join(lines)


def save_coverage_audit(audit: Dict[str, Any]) -> Dict[str, Path]:
    audit_dir = ensure_dir(audit_root())
    daily_dir = ensure_dir(audit_dir / "daily" / str(audit["summary"].get("universe_id", "unknown")))
    json_path = audit_dir / "coverage_audit.json"
    md_path = audit_dir / "coverage_audit.md"
    daily_json_path = daily_dir / f"{audit['summary'].get('as_of_date', 'latest')}.json"
    daily_md_path = daily_dir / f"{audit['summary'].get('as_of_date', 'latest')}.md"
    json_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(_format_markdown(audit), encoding="utf-8")
    daily_json_path.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding="utf-8")
    daily_md_path.write_text(_format_markdown(audit), encoding="utf-8")
    logger.info("Saved coverage audit: %s", json_path)
    return {
        "json": json_path,
        "markdown": md_path,
        "daily_json": daily_json_path,
        "daily_markdown": daily_md_path,
    }
