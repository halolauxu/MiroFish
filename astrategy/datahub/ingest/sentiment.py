"""Universe-level sentiment ingestion built from filings, company news, and hot-topic data."""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List

from astrategy.data_collector.news import NewsCollector
from astrategy.strategies.s06_announcement_sentiment import AnnouncementSentimentStrategy

from ..common import ensure_dir, sentiment_root

logger = logging.getLogger("astrategy.datahub.ingest.sentiment")


def _normalize_date(value: str | None) -> str:
    if not value:
        return date.today().isoformat()
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text[:10]


def _ticker_dir() -> Path:
    return ensure_dir(sentiment_root() / "by_ticker")


def _daily_dir() -> Path:
    return ensure_dir(sentiment_root() / "daily")


def _manifest_path() -> Path:
    return sentiment_root() / "sentiment_manifest.json"


def _hot_topics_path() -> Path:
    return sentiment_root() / "hot_topics.json"


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


def _membership_name_map(universe_membership: Dict[str, Any], universe_id: str) -> Dict[str, str]:
    return {
        str(row.get("ticker", "")).zfill(6): str(row.get("company_name", "")).strip()
        for row in universe_membership.get("memberships", [])
        if row.get("universe_id") == universe_id and str(row.get("ticker", "")).strip()
    }


def _hot_topic_rank_map(items: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    mapping: Dict[str, Dict[str, Any]] = {}
    for idx, item in enumerate(items, start=1):
        raw_code = str(item.get("代码", item.get("股票代码", ""))).strip()
        code = "".join(ch for ch in raw_code if ch.isdigit())[-6:]
        if not code:
            continue
        mapping[code] = {
            "hot_rank": idx,
            "hot_name": str(item.get("股票名称", item.get("名称", item.get("股票简称", "")))).strip(),
            "hot_reason": str(item.get("相关信息", item.get("概念", item.get("板块名称", "")))).strip(),
        }
    return mapping


def _score_text(text: str) -> Dict[str, Any]:
    scorer = AnnouncementSentimentStrategy._rule_based_sentiment
    primary = scorer(text)
    return {
        "sentiment_score": float(primary.get("sentiment_score", 0.0) or 0.0),
        "category": str(primary.get("category", "")).strip(),
        "urgency": str(primary.get("urgency", "")).strip() or "medium",
        "key_phrases": list(primary.get("key_phrases", []) or []),
    }


def _sentiment_label(score: float) -> str:
    if score >= 0.2:
        return "bullish"
    if score <= -0.2:
        return "bearish"
    return "neutral"


def build_sentiment_layer(
    universe_membership: Dict[str, Any],
    filings_payload: Dict[str, Any],
    news_payload: Dict[str, Any],
    universe_id: str = "csi800",
    *,
    ingest_date: str | None = None,
    collect_sentiment: bool = False,
    refresh_sentiment: bool = False,
    hot_topic_limit: int = 100,
) -> Dict[str, Any]:
    """Analyze per-ticker sentiment using filings, company news, and market hot topics."""
    as_of_date = _normalize_date(ingest_date)
    tickers = sorted(
        {
            str(row.get("ticker", "")).zfill(6)
            for row in universe_membership.get("memberships", [])
            if row.get("universe_id") == universe_id and str(row.get("ticker", "")).strip()
        }
    )
    name_map = _membership_name_map(universe_membership, universe_id)
    existing = _load_existing_rows()
    hot_topics: List[Dict[str, Any]] = []
    if collect_sentiment:
        hot_topics = NewsCollector().get_market_hot_topics(limit=hot_topic_limit)
    hot_rank_map = _hot_topic_rank_map(hot_topics)

    filing_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
    for row in filings_payload.get("rows", []):
        ticker = str(row.get("ticker", "")).zfill(6)
        if ticker:
            filing_by_ticker.setdefault(ticker, []).append(row)

    news_manifest_map = {
        str(row.get("ticker", "")).zfill(6): row
        for row in news_payload.get("rows", [])
        if str(row.get("ticker", "")).strip()
    }

    rows: List[Dict[str, Any]] = []
    total_items = 0
    bullish = 0
    bearish = 0

    for ticker in tickers:
        cached = existing.get(ticker)
        hot_rank_info = hot_rank_map.get(ticker, {})
        if cached and not refresh_sentiment:
            row = {
                **cached,
                "hot_rank": int(hot_rank_info.get("hot_rank", cached.get("hot_rank", 0)) or 0),
                "hot_reason": str(hot_rank_info.get("hot_reason", cached.get("hot_reason", ""))).strip(),
                "from_cache": True,
            }
            rows.append(row)
            continue

        item_rows: List[Dict[str, Any]] = []

        for filing in filing_by_ticker.get(ticker, []):
            title = str(filing.get("title", "")).strip()
            if not title:
                continue
            scored = _score_text(title)
            item_rows.append(
                {
                    "source_type": "filing",
                    "title": title,
                    "publish_date": _normalize_date(filing.get("publish_date", as_of_date)),
                    "sentiment_score": scored["sentiment_score"],
                    "category": scored["category"] or str(filing.get("announcement_type", "")).strip(),
                    "urgency": scored["urgency"],
                }
            )

        news_row = news_manifest_map.get(ticker, {})
        news_file = Path(str(news_row.get("news_file", "")).strip())
        if news_file.exists():
            try:
                news_items = json.loads(news_file.read_text(encoding="utf-8"))
            except Exception:
                news_items = []
            for item in news_items:
                title = str(item.get("新闻标题", item.get("标题", item.get("title", "")))).strip()
                if not title:
                    continue
                scored = _score_text(title)
                publish_date = _normalize_date(item.get("_normalized_publish_date", as_of_date))
                item_rows.append(
                    {
                        "source_type": "news",
                        "title": title,
                        "publish_date": publish_date,
                        "sentiment_score": scored["sentiment_score"],
                        "category": scored["category"] or "新闻舆情",
                        "urgency": scored["urgency"],
                    }
                )

        item_rows.sort(key=lambda item: str(item.get("publish_date", "")))
        if item_rows:
            total_items += len(item_rows)
            avg_score = round(
                sum(float(item.get("sentiment_score", 0.0) or 0.0) for item in item_rows) / len(item_rows),
                4,
            )
            latest_score = float(item_rows[-1].get("sentiment_score", 0.0) or 0.0)
            bullish_count = sum(1 for item in item_rows if float(item.get("sentiment_score", 0.0) or 0.0) > 0.1)
            bearish_count = sum(1 for item in item_rows if float(item.get("sentiment_score", 0.0) or 0.0) < -0.1)
            target_date_count = sum(1 for item in item_rows if item.get("publish_date") == as_of_date)
            attention_score = round(
                min(
                    1.0,
                    0.45 * min(1.0, len(item_rows) / 6.0)
                    + 0.25 * min(1.0, abs(avg_score))
                    + 0.20 * (1.0 if target_date_count > 0 else 0.0)
                    + 0.10 * (1.0 if hot_rank_info else 0.0),
                ),
                4,
            )
            label = _sentiment_label(avg_score)
            if label == "bullish":
                bullish += 1
            elif label == "bearish":
                bearish += 1
        else:
            avg_score = 0.0
            latest_score = 0.0
            bullish_count = 0
            bearish_count = 0
            target_date_count = 0
            attention_score = round(0.15 if hot_rank_info else 0.0, 4)
            label = "neutral"

        detail_path = _ticker_dir() / f"{ticker}.json"
        detail_payload = {
            "ticker": ticker,
            "company_name": name_map.get(ticker, ticker),
            "ingest_date": as_of_date,
            "items": item_rows,
            "hot_rank": int(hot_rank_info.get("hot_rank", 0) or 0),
            "hot_reason": str(hot_rank_info.get("hot_reason", "")).strip(),
        }
        detail_path.write_text(json.dumps(detail_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        rows.append(
            {
                "ticker": ticker,
                "company_name": name_map.get(ticker, ticker),
                "sentiment_item_count": len(item_rows),
                "target_date_sentiment_count": target_date_count,
                "avg_sentiment_score": avg_score,
                "latest_sentiment_score": round(latest_score, 4),
                "bullish_item_count": bullish_count,
                "bearish_item_count": bearish_count,
                "sentiment_label": label,
                "attention_score": attention_score,
                "hot_rank": int(hot_rank_info.get("hot_rank", 0) or 0),
                "hot_reason": str(hot_rank_info.get("hot_reason", "")).strip(),
                "detail_file": str(detail_path),
                "status": "analyzed" if item_rows or hot_rank_info else "empty",
                "from_cache": False,
            }
        )

    summary = {
        "universe_id": universe_id,
        "ingest_date": as_of_date,
        "collect_sentiment": bool(collect_sentiment),
        "requested_tickers": len(tickers),
        "with_sentiment": sum(1 for row in rows if int(row.get("sentiment_item_count", 0)) > 0),
        "with_hot_rank": sum(1 for row in rows if int(row.get("hot_rank", 0)) > 0),
        "bullish_tickers": bullish,
        "bearish_tickers": bearish,
        "total_scored_items": total_items,
    }
    logger.info(
        "Built sentiment layer for %s: with_sentiment=%d, hot_rank=%d, items=%d",
        universe_id,
        summary["with_sentiment"],
        summary["with_hot_rank"],
        summary["total_scored_items"],
    )
    return {"summary": summary, "rows": rows, "hot_topics": hot_topics}


def save_sentiment_layer(payload: Dict[str, Any]) -> Dict[str, Path]:
    root = ensure_dir(sentiment_root())
    daily_path = _daily_dir() / f"{payload['summary']['ingest_date']}.json"
    manifest_path = _manifest_path()
    hot_topics_path = _hot_topics_path()
    serialized = json.dumps(payload, ensure_ascii=False, indent=2)
    daily_path.write_text(serialized, encoding="utf-8")
    manifest_path.write_text(serialized, encoding="utf-8")
    hot_topics_path.write_text(json.dumps(payload.get("hot_topics", []), ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Saved sentiment layer: %s", manifest_path)
    return {"daily": daily_path, "manifest": manifest_path, "hot_topics": hot_topics_path}
