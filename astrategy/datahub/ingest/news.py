"""Incremental company-news ingestion for a target universe."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

from astrategy.data_collector.news import NewsCollector

from ..common import ensure_dir, ingest_root

logger = logging.getLogger("astrategy.datahub.ingest.news")


def _news_root() -> Path:
    return ensure_dir(ingest_root() / "news")


def _ticker_dir() -> Path:
    return ensure_dir(_news_root() / "by_ticker")


def _daily_dir() -> Path:
    return ensure_dir(_news_root() / "daily")


def _manifest_path() -> Path:
    return _news_root() / "news_manifest.json"


def _normalize_date(value: str | None) -> str:
    if not value:
        return date.today().isoformat()
    text = str(value).strip()
    if len(text) >= 10:
        return text[:10]
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text


def _load_existing_rows() -> Dict[str, Dict[str, Any]]:
    path = _manifest_path()
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        str(row.get("ticker", "")).zfill(6): row
        for row in payload.get("rows", [])
        if str(row.get("ticker", "")).strip()
    }


def _extract_publish_date(item: Dict[str, Any], fallback_date: str) -> str:
    for key in ("发布时间", "时间", "日期", "publish_time"):
        value = str(item.get(key, "")).strip()
        if value:
            return _normalize_date(value)
    return fallback_date


def build_news_layer(
    universe_membership: Dict[str, Any],
    universe_id: str = "csi800",
    *,
    ingest_date: str | None = None,
    collect_news: bool = False,
    news_sample_limit: int | None = None,
    max_news_per_stock: int = 5,
    refresh_news: bool = False,
) -> Dict[str, Any]:
    """Fetch recent company news for a target universe."""
    as_of_date = _normalize_date(ingest_date)
    tickers = sorted(
        {
            str(row.get("ticker", "")).zfill(6)
            for row in universe_membership.get("memberships", [])
            if row.get("universe_id") == universe_id
        }
    )
    if news_sample_limit is not None:
        tickers = tickers[: max(news_sample_limit, 0)]

    existing = _load_existing_rows()
    collector = NewsCollector()
    rows: List[Dict[str, Any]] = []

    for ticker in tickers:
        cached = existing.get(ticker)
        if cached and not refresh_news:
            rows.append({**cached, "from_cache": True})
            continue

        row = {
            "ticker": ticker,
            "news_count": 0,
            "target_date_news_count": 0,
            "latest_publish_date": "",
            "news_file": str(_ticker_dir() / f"{ticker}.json"),
            "status": "not_requested",
            "last_attempt_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "error": "",
            "from_cache": False,
        }
        if collect_news:
            try:
                items = collector.get_company_news(ticker, limit=max_news_per_stock)
                publish_dates = [_extract_publish_date(item, as_of_date) for item in items]
                for item, publish_date in zip(items, publish_dates):
                    item["_normalized_publish_date"] = publish_date
                (_ticker_dir() / f"{ticker}.json").write_text(
                    json.dumps(items, ensure_ascii=False, indent=2, default=str),
                    encoding="utf-8",
                )
                row.update(
                    {
                        "status": "fetched",
                        "news_count": len(items),
                        "target_date_news_count": sum(1 for value in publish_dates if value == as_of_date),
                        "latest_publish_date": max(publish_dates) if publish_dates else "",
                    }
                )
            except Exception as exc:
                row.update({"status": "failed", "error": str(exc)})
        rows.append(row)

    summary = {
        "universe_id": universe_id,
        "ingest_date": as_of_date,
        "collect_news": bool(collect_news),
        "requested_tickers": len(tickers),
        "fetched_tickers": sum(1 for row in rows if row["status"] == "fetched"),
        "with_news": sum(1 for row in rows if int(row.get("news_count", 0)) > 0),
        "target_date_news_tickers": sum(1 for row in rows if int(row.get("target_date_news_count", 0)) > 0),
        "cached_tickers": sum(1 for row in rows if row.get("from_cache")),
        "failed_tickers": sum(1 for row in rows if row["status"] == "failed"),
    }
    logger.info(
        "Built news layer for %s: requested=%d, with_news=%d, target_date=%d",
        universe_id,
        summary["requested_tickers"],
        summary["with_news"],
        summary["target_date_news_tickers"],
    )
    return {"summary": summary, "rows": rows}


def save_news_layer(payload: Dict[str, Any]) -> Dict[str, Path]:
    root = _news_root()
    daily_path = _daily_dir() / f"{payload['summary']['ingest_date']}.json"
    manifest_path = _manifest_path()
    serialized = json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    daily_path.write_text(serialized, encoding="utf-8")
    manifest_path.write_text(serialized, encoding="utf-8")
    logger.info("Saved news layer: %s", daily_path)
    return {"daily": daily_path, "manifest": manifest_path}
