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
    graph_path,
    ingest_root,
    market_root,
    pool_event_master_path,
)

logger = logging.getLogger("astrategy.datahub.coverage_audit")


def _load_graph_codes() -> set[str]:
    path = graph_path()
    if not path.exists():
        return set()
    data = json.loads(path.read_text(encoding="utf-8"))
    nodes = data.get("nodes", {})
    if not isinstance(nodes, dict):
        return set()
    return {
        code for code in nodes
        if isinstance(code, str) and code.isdigit() and len(code) == 6
    }


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
    path = ingest_root() / "filings" / "filings_manifest.json"
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
    path = ingest_root() / "news" / "news_manifest.json"
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


def build_coverage_audit(
    security_master: List[Dict[str, Any]],
    universe_membership: Dict[str, Any],
    universe_id: str = "csi800",
    *,
    as_of_date: str | None = None,
) -> Dict[str, Any]:
    """Build a first-pass coverage audit for one universe."""
    graph_codes = _load_graph_codes()
    event_stats = _load_event_stats()
    price_stats = _load_price_stats()
    filing_stats = _load_filings_stats()
    news_stats = _load_news_stats()
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
            "has_sentiment": False,
            "has_graph": ticker in graph_codes,
            "has_available_at": bool(event_info.get("available_at_complete", False)),
            "event_count": int(event_info.get("event_count", 0)),
            "top_event_types": event_info.get("top_event_types", {}),
        }
        row["coverage_score"] = _coverage_score(row)
        row["research_ready"] = bool(
            row["has_prices"]
            and row["price_row_count"] >= 20
            and row["has_events"]
            and row["has_graph"]
            and row["has_available_at"]
            and row["coverage_score"] >= 0.70
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
        "has_available_at": sum(1 for row in rows if row["has_available_at"]),
        "research_ready": sum(1 for row in rows if row["research_ready"]),
        "avg_coverage_score": round(
            sum(float(row["coverage_score"]) for row in rows) / max(len(rows), 1),
            4,
        ),
        "event_density_per_stock": round(
            sum(int(row["event_count"]) for row in rows) / max(len(rows), 1),
            4,
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
        f"- 图谱覆盖: {summary['has_graph']}",
        f"- available_at 完整: {summary['has_available_at']}",
        f"- research_ready: {summary['research_ready']}",
        f"- 平均覆盖分: {summary['avg_coverage_score']:.4f}",
        f"- 每股平均事件数: {summary['event_density_per_stock']:.4f}",
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
