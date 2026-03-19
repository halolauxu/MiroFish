"""Incremental filing ingestion for a target universe."""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from astrategy.data_collector.announcement import AnnouncementCollector

from ..common import ensure_dir, ingest_root

logger = logging.getLogger("astrategy.datahub.ingest.filings")


def _filings_root() -> Path:
    return ensure_dir(ingest_root() / "filings")


def _daily_dir() -> Path:
    return ensure_dir(_filings_root() / "daily")


def _manifest_path() -> Path:
    return _filings_root() / "filings_manifest.json"


def _normalize_date(value: str | None) -> str:
    if not value:
        return date.today().isoformat()
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text[:10]


def _extract_ticker(row: Dict[str, Any]) -> str:
    for key in ("代码", "股票代码", "证券代码", "code", "symbol"):
        value = str(row.get(key, "")).strip()
        digits = "".join(ch for ch in value if ch.isdigit())
        if len(digits) >= 6:
            return digits[-6:]
    return ""


def _extract_title(row: Dict[str, Any]) -> str:
    for key in ("公告标题", "标题", "title"):
        value = str(row.get(key, "")).strip()
        if value:
            return value
    return ""


def _extract_name(row: Dict[str, Any]) -> str:
    for key in ("名称", "股票简称", "简称", "name"):
        value = str(row.get(key, "")).strip()
        if value:
            return value
    return ""


def build_filings_layer(
    universe_membership: Dict[str, Any],
    universe_id: str = "csi800",
    *,
    ingest_date: str | None = None,
    collect_filings: bool = False,
) -> Dict[str, Any]:
    """Fetch daily announcements and filter them to the target universe."""
    as_of_date = _normalize_date(ingest_date)
    universe_codes = {
        str(row.get("ticker", "")).zfill(6)
        for row in universe_membership.get("memberships", [])
        if row.get("universe_id") == universe_id
    }
    payload = {
        "summary": {
            "universe_id": universe_id,
            "ingest_date": as_of_date,
            "collect_filings": bool(collect_filings),
            "raw_announcements": 0,
            "important_announcements": 0,
            "matched_universe_announcements": 0,
            "matched_universe_tickers": 0,
        },
        "rows": [],
    }
    if not collect_filings:
        logger.info("Filings collection skipped for %s", universe_id)
        return payload

    collector = AnnouncementCollector()
    raw_rows = collector.get_daily_announcements(as_of_date.replace("-", ""))
    important_rows = collector.filter_important_announcements(raw_rows)

    rows: List[Dict[str, Any]] = []
    matched_codes: set[str] = set()
    for item in important_rows:
        ticker = _extract_ticker(item)
        if ticker not in universe_codes:
            continue
        matched_codes.add(ticker)
        rows.append(
            {
                "ticker": ticker,
                "company_name": _extract_name(item),
                "title": _extract_title(item),
                "announcement_type": str(item.get("公告类型", item.get("类型", ""))).strip(),
                "publish_date": _normalize_date(
                    str(item.get("公告日期", item.get("发布时间", as_of_date))).strip()
                ),
                "link": str(item.get("公告链接", item.get("链接", ""))).strip(),
                "source": "akshare.stock_notice_report",
                "raw": item,
            }
        )

    payload["summary"] = {
        "universe_id": universe_id,
        "ingest_date": as_of_date,
        "collect_filings": True,
        "raw_announcements": len(raw_rows),
        "important_announcements": len(important_rows),
        "matched_universe_announcements": len(rows),
        "matched_universe_tickers": len(matched_codes),
    }
    payload["rows"] = rows
    logger.info(
        "Built filings layer for %s: raw=%d, important=%d, matched=%d, tickers=%d",
        universe_id,
        len(raw_rows),
        len(important_rows),
        len(rows),
        len(matched_codes),
    )
    return payload


def save_filings_layer(payload: Dict[str, Any]) -> Dict[str, Path]:
    root = _filings_root()
    daily_path = _daily_dir() / f"{payload['summary']['ingest_date']}.json"
    manifest_path = _manifest_path()
    serialized = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    daily_path.write_text(serialized, encoding="utf-8")
    manifest_path.write_text(serialized, encoding="utf-8")
    logger.info("Saved filings layer: %s", daily_path)
    return {"daily": daily_path, "manifest": manifest_path}
