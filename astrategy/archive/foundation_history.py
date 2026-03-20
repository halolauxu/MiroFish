#!/usr/bin/env python3
"""
Historical replay archive for data-foundation components.

Goals:
1. Freeze per-day foundation snapshots under an authoritative archive tree.
2. Rebuild historical as-of-date payloads from current local source stores.
3. Make gaps explicit instead of silently treating incomplete history as ready.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd

from astrategy.config import settings
from astrategy.datahub.common import (
    datahub_root,
    ensure_dir,
    graph_root,
    ingest_root,
    repo_relative_path,
    universe_root,
)
from astrategy.datahub.ingest.events import (
    _append_or_replace,
    _build_legacy_event,
    _infer_event_classification,
    _normalize_date as _normalize_event_date,
    _normalize_datetime as _normalize_event_datetime,
    _security_name_map,
    _stable_suffix,
)
from astrategy.datahub.ingest.filings import (
    _dedupe_rows as _dedupe_filing_rows,
    _normalize_date as _normalize_filing_date,
    _window_start_date as _filing_window_start_date,
)
from astrategy.datahub.ingest.sentiment import (
    _aggregate_sentiment,
    _build_item,
    _membership_name_map,
)

_COMPONENTS = ("filings", "news", "sentiment", "events", "graph")
_MIN_BACKTEST_SPAN_DAYS = 730


def _normalize_date(value: str | None) -> str:
    if not value:
        return ""
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text[:10]


def _to_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _span_days(first_date: str, last_date: str) -> int:
    if not first_date or not last_date:
        return 0
    start = _to_date(first_date)
    end = _to_date(last_date)
    if start > end:
        return 0
    return (end - start).days


def _render_table(headers: List[str], rows: Iterable[List[str]]) -> List[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def _business_dates(start_date: str, end_date: str) -> List[str]:
    if not start_date or not end_date:
        return []
    rng = pd.bdate_range(start=start_date, end=end_date)
    return [ts.strftime("%Y-%m-%d") for ts in rng]


def _safe_load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_security_master() -> List[Dict[str, Any]]:
    path = universe_root() / "security_master.json"
    payload = _safe_load_json(path)
    return payload if isinstance(payload, list) else []


def _load_universe_membership() -> Dict[str, Any]:
    path = universe_root() / "universe_membership.json"
    payload = _safe_load_json(path)
    if isinstance(payload, dict):
        return payload
    return {"summary": {}, "memberships": []}


def _universe_codes(universe_membership: Dict[str, Any], universe_id: str) -> List[str]:
    return sorted(
        {
            str(row.get("ticker", "")).zfill(6)
            for row in universe_membership.get("memberships", [])
            if row.get("universe_id") == universe_id and str(row.get("ticker", "")).strip()
        }
    )


def _filings_ticker_dir() -> Path:
    return ingest_root() / "filings" / "by_ticker"


def _news_ticker_dir() -> Path:
    return ingest_root() / "news" / "by_ticker"


def _graph_daily_dir() -> Path:
    return graph_root() / "daily"


def _filings_by_ticker(universe_codes: set[str]) -> Dict[str, List[Dict[str, Any]]]:
    rows_map: Dict[str, List[Dict[str, Any]]] = {}
    for path in sorted(_filings_ticker_dir().glob("*.json")):
        ticker = path.stem.strip().zfill(6)
        if universe_codes and ticker not in universe_codes:
            continue
        payload = _safe_load_json(path)
        if not isinstance(payload, list):
            continue
        rows = [
            row
            for row in payload
            if isinstance(row, dict) and str(row.get("publish_date", "")).strip()
        ]
        if rows:
            rows_map[ticker] = sorted(
                rows,
                key=lambda row: (
                    str(row.get("publish_date", "")),
                    str(row.get("title", "")),
                    str(row.get("link", "")),
                ),
            )
    return rows_map


def _news_date(item: Dict[str, Any]) -> str:
    for key in ("_normalized_publish_date", "发布时间", "时间", "日期"):
        raw = str(item.get(key, "")).strip()
        if raw:
            value = _normalize_date(raw)
            if value:
                return value
    return ""


def _news_by_ticker(universe_codes: set[str]) -> Dict[str, List[Dict[str, Any]]]:
    rows_map: Dict[str, List[Dict[str, Any]]] = {}
    for path in sorted(_news_ticker_dir().glob("*.json")):
        ticker = path.stem.strip().zfill(6)
        if universe_codes and ticker not in universe_codes:
            continue
        payload = _safe_load_json(path)
        if not isinstance(payload, list):
            continue
        rows: List[Dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            publish_date = _news_date(item)
            if not publish_date:
                continue
            normalized = dict(item)
            normalized["_normalized_publish_date"] = publish_date
            rows.append(normalized)
        if rows:
            rows_map[ticker] = sorted(
                rows,
                key=lambda item: (
                    str(item.get("_normalized_publish_date", "")),
                    str(item.get("新闻标题", item.get("标题", item.get("title", "")))),
                ),
            )
    return rows_map


def _available_dates_from_rows(
    rows_by_ticker: Dict[str, List[Dict[str, Any]]],
    key_fn,
) -> List[str]:
    dates = {
        key_fn(row)
        for rows in rows_by_ticker.values()
        for row in rows
        if key_fn(row)
    }
    return sorted(dates)


class FoundationHistoryBuilder:
    def __init__(self, base_dir: Path | str | None = None) -> None:
        self.base_dir = Path(base_dir) if base_dir else settings.storage._base
        self.archive_root = ensure_dir(self.base_dir / "authoritative_archive" / "foundation")
        self.audit_root = ensure_dir(self.base_dir / "authoritative_archive" / "audit")
        self.report_root = ensure_dir(self.base_dir / "reports")
        self.security_master = _load_security_master()
        self.universe_membership = _load_universe_membership()

    def _component_dir(self, component: str) -> Path:
        return ensure_dir(self.archive_root / component)

    def _snapshot_path(self, component: str, as_of_date: str) -> Path:
        date_tag = as_of_date.replace("-", "")
        return self._component_dir(component) / f"{date_tag}.json"

    def archive_snapshot(
        self,
        *,
        component: str,
        as_of_date: str,
        payload: Dict[str, Any],
        universe_id: str,
        quality_status: str,
        authoritative: bool,
        run_context: str,
        source_paths: Sequence[str] | None = None,
    ) -> Dict[str, Any]:
        if component not in _COMPONENTS:
            return {"component": component, "status": "skipped", "reason": "unsupported_component"}

        normalized_date = _normalize_date(as_of_date)
        snapshot = {
            "component": component,
            "as_of_date": normalized_date,
            "universe_id": universe_id,
            "quality_status": quality_status,
            "authoritative": bool(authoritative),
            "run_context": run_context,
            "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "source_paths": list(source_paths or []),
            "payload": payload,
        }
        path = self._snapshot_path(component, normalized_date)
        path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        return {"component": component, "status": "archived", "snapshot_path": str(path)}

    def archive_current_payloads(
        self,
        *,
        universe_id: str,
        as_of_date: str,
        filings_payload: Dict[str, Any] | None = None,
        news_payload: Dict[str, Any] | None = None,
        sentiment_payload: Dict[str, Any] | None = None,
        event_payload: Dict[str, Any] | None = None,
        graph_payload: Dict[str, Any] | None = None,
        source_paths: Dict[str, Sequence[str]] | None = None,
        run_context: str = "bootstrap",
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        path_map = source_paths or {}
        payload_map = {
            "filings": (filings_payload, "authoritative_current", True),
            "news": (news_payload, "live_cache_current", False),
            "sentiment": (sentiment_payload, "derived_current", False),
            "events": (event_payload, "derived_current", False),
            "graph": (graph_payload, "state_snapshot_current", False),
        }
        for component, (payload, quality_status, authoritative) in payload_map.items():
            if payload is None:
                continue
            results.append(
                self.archive_snapshot(
                    component=component,
                    as_of_date=as_of_date,
                    payload=payload,
                    universe_id=universe_id,
                    quality_status=quality_status,
                    authoritative=authoritative,
                    run_context=run_context,
                    source_paths=path_map.get(component, []),
                )
            )
        return results

    def _build_filings_snapshot(
        self,
        *,
        universe_id: str,
        as_of_date: str,
        tickers: List[str],
        name_map: Dict[str, str],
        filings_map: Dict[str, List[Dict[str, Any]]],
        lookback_days: int,
    ) -> Dict[str, Any]:
        start_date = _filing_window_start_date(as_of_date, lookback_days)
        rows: List[Dict[str, Any]] = []
        ticker_rows: List[Dict[str, Any]] = []
        for ticker in tickers:
            items = [
                row
                for row in filings_map.get(ticker, [])
                if start_date <= str(row.get("publish_date", "")) <= as_of_date
            ]
            target_count = sum(1 for row in items if str(row.get("publish_date", "")) == as_of_date)
            latest_publish_date = max((str(row.get("publish_date", "")) for row in items), default="")
            ticker_rows.append(
                {
                    "ticker": ticker,
                    "company_name": name_map.get(ticker, ticker),
                    "filing_count": len(items),
                    "target_date_filing_count": target_count,
                    "latest_publish_date": latest_publish_date,
                    "filing_file": repo_relative_path(_filings_ticker_dir() / f"{ticker}.json"),
                    "status": "historical_replay" if items else "empty",
                    "error": "",
                    "from_cache": True,
                }
            )
            rows.extend(items)

        deduped_rows = sorted(
            _dedupe_filing_rows(rows),
            key=lambda item: (
                str(item.get("publish_date", "")),
                str(item.get("ticker", "")),
                str(item.get("title", "")),
            ),
            reverse=True,
        )
        return {
            "summary": {
                "universe_id": universe_id,
                "ingest_date": as_of_date,
                "snapshot_mode": "historical_replay",
                "source": "cninfo.disclosure_report",
                "source_quality": "authoritative",
                "history_quality": "authoritative_local_replay",
                "lookback_days": lookback_days,
                "window_start_date": start_date,
                "requested_tickers": len(tickers),
                "important_announcements": len(deduped_rows),
                "matched_universe_announcements": len(deduped_rows),
                "matched_universe_tickers": sum(1 for row in ticker_rows if int(row.get("filing_count", 0)) > 0),
                "target_date_announcements": sum(1 for row in deduped_rows if str(row.get("publish_date", "")) == as_of_date),
                "target_date_tickers": sum(1 for row in ticker_rows if int(row.get("target_date_filing_count", 0)) > 0),
                "cached_tickers": len(tickers),
            },
            "rows": deduped_rows,
            "tickers": ticker_rows,
        }

    def _build_news_snapshot(
        self,
        *,
        universe_id: str,
        as_of_date: str,
        tickers: List[str],
        name_map: Dict[str, str],
        news_map: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        items: List[Dict[str, Any]] = []
        rows: List[Dict[str, Any]] = []
        for ticker in tickers:
            ticker_items = [
                {
                    **item,
                    "ticker": ticker,
                    "company_name": name_map.get(ticker, ticker),
                }
                for item in news_map.get(ticker, [])
                if str(item.get("_normalized_publish_date", "")) <= as_of_date
            ]
            target_count = sum(
                1 for item in ticker_items if str(item.get("_normalized_publish_date", "")) == as_of_date
            )
            latest_publish_date = max(
                (str(item.get("_normalized_publish_date", "")) for item in ticker_items),
                default="",
            )
            rows.append(
                {
                    "ticker": ticker,
                    "company_name": name_map.get(ticker, ticker),
                    "news_count": len(ticker_items),
                    "target_date_news_count": target_count,
                    "latest_publish_date": latest_publish_date,
                    "news_file": repo_relative_path(_news_ticker_dir() / f"{ticker}.json"),
                    "status": "historical_replay" if ticker_items else "empty",
                    "error": "",
                    "from_cache": True,
                }
            )
            items.extend(ticker_items)

        ordered_items = sorted(
            items,
            key=lambda item: (
                str(item.get("_normalized_publish_date", "")),
                str(item.get("ticker", "")),
                str(item.get("新闻标题", item.get("标题", item.get("title", "")))),
            ),
        )
        return {
            "summary": {
                "universe_id": universe_id,
                "ingest_date": as_of_date,
                "snapshot_mode": "historical_replay",
                "source_quality": "bounded_recent_cache",
                "history_quality": "local_cache_replay",
                "requested_tickers": len(tickers),
                "with_news": sum(1 for row in rows if int(row.get("news_count", 0)) > 0),
                "target_date_news_tickers": sum(1 for row in rows if int(row.get("target_date_news_count", 0)) > 0),
                "total_cached_items": len(ordered_items),
                "cached_tickers": len(tickers),
            },
            "rows": rows,
            "items": ordered_items,
        }

    def _build_sentiment_snapshot(
        self,
        *,
        universe_id: str,
        as_of_date: str,
        tickers: List[str],
        name_map: Dict[str, str],
        filings_snapshot: Dict[str, Any],
        news_snapshot: Dict[str, Any],
        hot_rank_top_n: int,
    ) -> Dict[str, Any]:
        filings_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        for row in filings_snapshot.get("rows", []):
            ticker = str(row.get("ticker", "")).zfill(6)
            if ticker:
                filings_by_ticker.setdefault(ticker, []).append(row)

        news_by_ticker: Dict[str, List[Dict[str, Any]]] = {}
        for item in news_snapshot.get("items", []):
            ticker = str(item.get("ticker", "")).zfill(6)
            if ticker:
                news_by_ticker.setdefault(ticker, []).append(item)

        rows: List[Dict[str, Any]] = []
        bullish = 0
        bearish = 0
        total_items = 0

        for ticker in tickers:
            item_rows: List[Dict[str, Any]] = []
            for filing in filings_by_ticker.get(ticker, []):
                title = str(filing.get("title", "")).strip()
                if not title:
                    continue
                item_rows.append(
                    _build_item(
                        source_type="filing",
                        title=title,
                        publish_time=filing.get("publish_time", filing.get("publish_date", as_of_date)),
                        fallback_date=_normalize_filing_date(str(filing.get("publish_date", as_of_date))),
                        summary=str(filing.get("announcement_type", "")).strip(),
                        category_hint=str(filing.get("announcement_type", "")).strip(),
                    )
                )
            for item in news_by_ticker.get(ticker, []):
                title = str(item.get("新闻标题", item.get("标题", item.get("title", "")))).strip()
                if not title:
                    continue
                summary = str(item.get("新闻内容", item.get("摘要", title))).strip()
                item_rows.append(
                    _build_item(
                        source_type="news",
                        title=title,
                        publish_time=item.get("发布时间", item.get("时间", item.get("_normalized_publish_date", as_of_date))),
                        fallback_date=str(item.get("_normalized_publish_date", as_of_date)),
                        summary=summary,
                        category_hint=str(item.get("文章来源", item.get("source", ""))).strip(),
                    )
                )
            aggregate = _aggregate_sentiment(item_rows, as_of_date, hot_rank=0)
            if aggregate["sentiment_label"] == "bullish":
                bullish += 1
            elif aggregate["sentiment_label"] == "bearish":
                bearish += 1
            total_items += int(aggregate["sentiment_item_count"])
            rows.append(
                {
                    "ticker": ticker,
                    "company_name": name_map.get(ticker, ticker),
                    **aggregate,
                    "hot_rank": 0,
                    "hot_reason": "",
                    "detail_file": "",
                    "status": "historical_replay" if aggregate["sentiment_item_count"] > 0 else "empty",
                    "from_cache": True,
                }
            )

        _ = hot_rank_top_n  # reserved for future historical hot-topic support
        return {
            "summary": {
                "universe_id": universe_id,
                "ingest_date": as_of_date,
                "snapshot_mode": "historical_replay",
                "source_quality": "derived_from_archived_foundation",
                "history_quality": "partial_no_hot_topic_history",
                "requested_tickers": len(tickers),
                "with_sentiment": sum(1 for row in rows if int(row.get("sentiment_item_count", 0)) > 0),
                "with_hot_rank": 0,
                "authoritative_hot_rank_count": 0,
                "bullish_tickers": bullish,
                "bearish_tickers": bearish,
                "total_scored_items": total_items,
                "hot_topic_source": "historical_unavailable",
                "hot_topic_authoritative": False,
                "hot_topic_live_sources": [],
                "hot_topic_failed_sources": ["historical_backfill_not_available"],
                "hot_topic_api_failed": False,
                "api_failure_count": 0,
            },
            "rows": rows,
            "hot_topics": [],
        }

    def _build_event_snapshot(
        self,
        *,
        universe_id: str,
        as_of_date: str,
        security_name_map: Dict[str, str],
        filings_snapshot: Dict[str, Any],
        news_snapshot: Dict[str, Any],
        sentiment_snapshot: Dict[str, Any],
        existing_master: List[Dict[str, Any]],
        existing_ids: set[str],
        existing_index: Dict[str, int],
    ) -> Dict[str, Any]:
        incremental: List[Dict[str, Any]] = []
        filing_event_count = 0
        news_event_count = 0
        sentiment_event_count = 0
        skipped_filing_other = 0
        skipped_news_other = 0

        for row in filings_snapshot.get("rows", []):
            ticker = str(row.get("ticker", "")).zfill(6)
            title = str(row.get("title", "")).strip()
            publish_date = _normalize_event_date(str(row.get("publish_date", "")))
            if not title or publish_date != as_of_date:
                continue
            announcement_type = str(row.get("announcement_type", "")).strip()
            publish_time = _normalize_event_datetime(row.get("publish_time", publish_date), publish_date)
            classification = _infer_event_classification(
                title,
                "filing",
                summary=announcement_type,
                announcement_type=announcement_type,
            )
            if classification["event_type"] == "other":
                skipped_filing_other += 1
                continue
            event_id = f"filing:{publish_date}:{ticker}:{_stable_suffix(title)}"
            legacy = _build_legacy_event(
                event_id=event_id,
                title=title,
                event_type=classification["event_type"],
                event_subtype=classification["event_subtype"],
                ticker=ticker,
                stock_name=str(row.get("company_name", "")).strip() or security_name_map.get(ticker, ticker),
                event_date=publish_time[:10],
                discover_time=publish_time,
                available_at=publish_time,
                summary=announcement_type or title,
                source="announcement_incremental",
                source_type="filing",
                raw_payload_ref=str(row.get("link", "")).strip(),
                metadata={
                    "ingest_date": as_of_date,
                    "announcement_type": announcement_type,
                    "matched_keywords": classification["matched_keywords"],
                },
            )
            status = _append_or_replace(incremental, existing_master, existing_ids, existing_index, legacy)
            if status == "inserted":
                filing_event_count += 1

        for item in news_snapshot.get("items", []):
            ticker = str(item.get("ticker", "")).zfill(6)
            title = str(item.get("新闻标题", item.get("标题", item.get("title", "")))).strip()
            publish_date = str(item.get("_normalized_publish_date", ""))
            if not title or publish_date != as_of_date:
                continue
            summary = str(item.get("新闻内容", item.get("摘要", title))).strip()[:400]
            publish_time = _normalize_event_datetime(
                item.get("发布时间", item.get("时间", item.get("_normalized_publish_date", as_of_date))),
                publish_date,
            )
            classification = _infer_event_classification(title, "news", summary=summary)
            if classification["event_type"] == "other":
                skipped_news_other += 1
                continue
            event_id = f"news:{publish_date}:{ticker}:{_stable_suffix(title)}"
            legacy = _build_legacy_event(
                event_id=event_id,
                title=title,
                event_type=classification["event_type"],
                event_subtype=classification["event_subtype"],
                ticker=ticker,
                stock_name=str(item.get("company_name", "")).strip() or security_name_map.get(ticker, ticker),
                event_date=publish_date,
                discover_time=publish_time,
                available_at=publish_time,
                summary=summary,
                source="news_incremental",
                source_type="news",
                raw_payload_ref=str(item.get("新闻链接", item.get("链接", ""))).strip(),
                metadata={
                    "ingest_date": as_of_date,
                    "publisher": item.get("文章来源", item.get("source", "")),
                    "matched_keywords": classification["matched_keywords"],
                },
            )
            status = _append_or_replace(incremental, existing_master, existing_ids, existing_index, legacy)
            if status == "inserted":
                news_event_count += 1

        for row in sentiment_snapshot.get("rows", []):
            ticker = str(row.get("ticker", "")).zfill(6)
            attention_score = float(row.get("attention_score", 0.0) or 0.0)
            avg_score = float(row.get("avg_sentiment_score", 0.0) or 0.0)
            target_date_count = int(row.get("target_date_sentiment_count", 0) or 0)
            if attention_score < 0.55 or abs(avg_score) < 0.25 or target_date_count <= 0:
                continue
            publish_time = f"{as_of_date}T00:00:00"
            title = f"{row.get('company_name', ticker)} 舆情热度变化"
            label = str(row.get("sentiment_label", "neutral")).strip() or "neutral"
            summary = (
                f"平均情绪分 {avg_score:+.2f}，关注度 {attention_score:.2f}，"
                f"热度排名 -，标签 {label}"
            )
            event_id = f"sentiment:{as_of_date}:{ticker}:{_stable_suffix(summary)}"
            legacy = _build_legacy_event(
                event_id=event_id,
                title=title,
                event_type="sentiment_reversal",
                event_subtype="sentiment_reversal.aggregate",
                ticker=ticker,
                stock_name=str(row.get("company_name", "")).strip() or security_name_map.get(ticker, ticker),
                event_date=as_of_date,
                discover_time=publish_time,
                available_at=publish_time,
                summary=summary,
                source="sentiment_incremental",
                source_type="sentiment",
                raw_payload_ref="",
                metadata={
                    "ingest_date": as_of_date,
                    "attention_score": attention_score,
                    "avg_sentiment_score": avg_score,
                    "hot_rank": 0,
                    "sentiment_label": label,
                },
            )
            status = _append_or_replace(incremental, existing_master, existing_ids, existing_index, legacy)
            if status == "inserted":
                sentiment_event_count += 1

        return {
            "summary": {
                "ingest_date": as_of_date,
                "snapshot_mode": "historical_replay",
                "history_quality": "derived_from_local_foundation",
                "incremental_event_count": len(incremental),
                "event_master_count": len(existing_master),
                "created_event_count": filing_event_count + news_event_count + sentiment_event_count,
                "filing_event_count": filing_event_count,
                "news_event_count": news_event_count,
                "sentiment_event_count": sentiment_event_count,
                "skipped_filing_other": skipped_filing_other,
                "skipped_news_other": skipped_news_other,
            },
            "rows": incremental,
        }

    def _graph_snapshot_for_date(self, as_of_date: str) -> Optional[Dict[str, Any]]:
        for candidate in (f"{as_of_date}.json", f"{as_of_date.replace('-', '')}.json"):
            payload = _safe_load_json(_graph_daily_dir() / candidate)
            if isinstance(payload, dict):
                return payload
        return None

    def backfill_local_history(
        self,
        *,
        universe_id: str = "csi800",
        components: Sequence[str] | None = None,
        start_date: str = "",
        end_date: str = "",
        filing_lookback_days: int = 30,
        sentiment_hot_rank_top_n: int = 200,
    ) -> Dict[str, Any]:
        selected = [component for component in (components or _COMPONENTS) if component in _COMPONENTS]
        tickers = _universe_codes(self.universe_membership, universe_id)
        universe_codes = set(tickers)
        name_map = _membership_name_map(self.universe_membership, universe_id)
        security_name_map = _security_name_map(self.security_master)
        filings_map = _filings_by_ticker(universe_codes)
        news_map = _news_by_ticker(universe_codes)

        filing_dates = _available_dates_from_rows(filings_map, lambda row: _normalize_filing_date(str(row.get("publish_date", ""))))
        news_dates = _available_dates_from_rows(news_map, lambda row: str(row.get("_normalized_publish_date", "")))
        graph_dates: List[str] = []
        for path in sorted(_graph_daily_dir().glob("*.json")):
            stem = path.stem.strip()
            if len(stem) == 10 and stem[4] == "-" and stem[7] == "-":
                graph_dates.append(stem)
            elif len(stem) == 8 and stem.isdigit():
                graph_dates.append(f"{stem[:4]}-{stem[4:6]}-{stem[6:8]}")
        foundation_dates = sorted(set(filing_dates) | set(news_dates) | set(graph_dates))
        if not foundation_dates:
            audit = self.build_audit()
            json_path, report_path = self.write_audit(audit)
            return {"results": [], "audit_json": str(json_path), "audit_report": str(report_path)}

        filing_first = filing_dates[0] if filing_dates else ""
        filing_last = filing_dates[-1] if filing_dates else ""
        news_first = news_dates[0] if news_dates else ""
        news_last = news_dates[-1] if news_dates else ""
        graph_date_set = set(graph_dates)
        derived_foundation_dates = sorted(set(filing_dates) | set(news_dates))
        derived_first = derived_foundation_dates[0] if derived_foundation_dates else ""
        derived_last = derived_foundation_dates[-1] if derived_foundation_dates else ""

        effective_start = _normalize_date(start_date) if start_date else foundation_dates[0]
        effective_end = _normalize_date(end_date) if end_date else foundation_dates[-1]
        daily_dates = [
            value
            for value in _business_dates(effective_start, effective_end)
            if value >= foundation_dates[0] and value <= foundation_dates[-1]
        ]

        results: List[Dict[str, Any]] = []
        event_master: List[Dict[str, Any]] = []
        event_ids: set[str] = set()
        event_index: Dict[str, int] = {}

        for as_of_date in daily_dates:
            filings_snapshot: Optional[Dict[str, Any]] = None
            news_snapshot: Optional[Dict[str, Any]] = None
            sentiment_snapshot: Optional[Dict[str, Any]] = None

            if "filings" in selected and filing_first and filing_first <= as_of_date <= filing_last:
                filings_snapshot = self._build_filings_snapshot(
                    universe_id=universe_id,
                    as_of_date=as_of_date,
                    tickers=tickers,
                    name_map=name_map,
                    filings_map=filings_map,
                    lookback_days=filing_lookback_days,
                )
                results.append(
                    self.archive_snapshot(
                        component="filings",
                        as_of_date=as_of_date,
                        payload=filings_snapshot,
                        universe_id=universe_id,
                        quality_status="authoritative_local_replay",
                        authoritative=True,
                        run_context="historical_backfill",
                        source_paths=[repo_relative_path(_filings_ticker_dir())],
                    )
                )

            if "news" in selected and news_first and news_first <= as_of_date <= news_last:
                news_snapshot = self._build_news_snapshot(
                    universe_id=universe_id,
                    as_of_date=as_of_date,
                    tickers=tickers,
                    name_map=name_map,
                    news_map=news_map,
                )
                results.append(
                    self.archive_snapshot(
                        component="news",
                        as_of_date=as_of_date,
                        payload=news_snapshot,
                        universe_id=universe_id,
                        quality_status="local_cache_replay",
                        authoritative=False,
                        run_context="historical_backfill",
                        source_paths=[repo_relative_path(_news_ticker_dir())],
                    )
                )

            if "sentiment" in selected and derived_first and derived_first <= as_of_date <= derived_last:
                if filings_snapshot is None:
                    filings_snapshot = self._build_filings_snapshot(
                        universe_id=universe_id,
                        as_of_date=as_of_date,
                        tickers=tickers,
                        name_map=name_map,
                        filings_map=filings_map,
                        lookback_days=filing_lookback_days,
                    )
                if news_snapshot is None:
                    news_snapshot = self._build_news_snapshot(
                        universe_id=universe_id,
                        as_of_date=as_of_date,
                        tickers=tickers,
                        name_map=name_map,
                        news_map=news_map,
                    )
                sentiment_snapshot = self._build_sentiment_snapshot(
                    universe_id=universe_id,
                    as_of_date=as_of_date,
                    tickers=tickers,
                    name_map=name_map,
                    filings_snapshot=filings_snapshot,
                    news_snapshot=news_snapshot,
                    hot_rank_top_n=sentiment_hot_rank_top_n,
                )
                results.append(
                    self.archive_snapshot(
                        component="sentiment",
                        as_of_date=as_of_date,
                        payload=sentiment_snapshot,
                        universe_id=universe_id,
                            quality_status="derived_no_hot_topic_history",
                            authoritative=False,
                            run_context="historical_backfill",
                            source_paths=[
                                repo_relative_path(self._snapshot_path("filings", as_of_date)),
                                repo_relative_path(self._snapshot_path("news", as_of_date)),
                        ],
                    )
                )

            if "events" in selected and derived_first and derived_first <= as_of_date <= derived_last:
                if filings_snapshot is None:
                    filings_snapshot = self._build_filings_snapshot(
                        universe_id=universe_id,
                        as_of_date=as_of_date,
                        tickers=tickers,
                        name_map=name_map,
                        filings_map=filings_map,
                        lookback_days=filing_lookback_days,
                    )
                if news_snapshot is None:
                    news_snapshot = self._build_news_snapshot(
                        universe_id=universe_id,
                        as_of_date=as_of_date,
                        tickers=tickers,
                        name_map=name_map,
                        news_map=news_map,
                    )
                if sentiment_snapshot is None:
                    sentiment_snapshot = self._build_sentiment_snapshot(
                        universe_id=universe_id,
                        as_of_date=as_of_date,
                        tickers=tickers,
                        name_map=name_map,
                        filings_snapshot=filings_snapshot,
                        news_snapshot=news_snapshot,
                        hot_rank_top_n=sentiment_hot_rank_top_n,
                    )
                event_snapshot = self._build_event_snapshot(
                    universe_id=universe_id,
                    as_of_date=as_of_date,
                    security_name_map=security_name_map,
                    filings_snapshot=filings_snapshot,
                    news_snapshot=news_snapshot,
                    sentiment_snapshot=sentiment_snapshot,
                    existing_master=event_master,
                    existing_ids=event_ids,
                    existing_index=event_index,
                )
                results.append(
                    self.archive_snapshot(
                        component="events",
                        as_of_date=as_of_date,
                        payload=event_snapshot,
                        universe_id=universe_id,
                            quality_status="derived_from_local_foundation",
                            authoritative=False,
                            run_context="historical_backfill",
                            source_paths=[
                                repo_relative_path(self._snapshot_path("filings", as_of_date)),
                                repo_relative_path(self._snapshot_path("news", as_of_date)),
                                repo_relative_path(self._snapshot_path("sentiment", as_of_date)),
                        ],
                    )
                )

            if "graph" in selected and as_of_date in graph_date_set:
                graph_snapshot = self._graph_snapshot_for_date(as_of_date)
                if graph_snapshot is not None:
                    results.append(
                        self.archive_snapshot(
                            component="graph",
                            as_of_date=as_of_date,
                            payload=graph_snapshot,
                            universe_id=universe_id,
                            quality_status="state_snapshot_only",
                            authoritative=False,
                            run_context="historical_backfill",
                            source_paths=[repo_relative_path(_graph_daily_dir() / f"{as_of_date.replace('-', '')}.json")],
                        )
                    )

        audit = self.build_audit(universe_id=universe_id)
        json_path, report_path = self.write_audit(audit)
        return {
            "results": results,
            "audit_json": str(json_path),
            "audit_report": str(report_path),
        }

    def build_audit(self, *, universe_id: str = "") -> Dict[str, Any]:
        components: Dict[str, Dict[str, Any]] = {}
        for component in _COMPONENTS:
            component_dir = self._component_dir(component)
            snapshots: List[Dict[str, Any]] = []
            for path in sorted(component_dir.glob("*.json")):
                payload = _safe_load_json(path)
                if isinstance(payload, dict):
                    snapshots.append(payload)
            dates = sorted(
                str(item.get("as_of_date", "")).strip()
                for item in snapshots
                if str(item.get("as_of_date", "")).strip()
            )
            first_date = dates[0] if dates else ""
            last_date = dates[-1] if dates else ""
            quality_statuses = sorted(
                {
                    str(item.get("quality_status", "")).strip()
                    for item in snapshots
                    if str(item.get("quality_status", "")).strip()
                }
            )
            authoritative_count = sum(1 for item in snapshots if bool(item.get("authoritative")))
            span_two_year_ready = bool(
                first_date
                and last_date
                and first_date <= (_to_date(last_date) - timedelta(days=_MIN_BACKTEST_SPAN_DAYS - 1)).isoformat()
            )
            authoritative_two_year_ready = bool(
                span_two_year_ready and snapshots and authoritative_count == len(snapshots)
            )
            components[component] = {
                "component": component,
                "snapshot_count": len(snapshots),
                "authoritative_snapshot_count": authoritative_count,
                "first_date": first_date,
                "last_date": last_date,
                "span_days": _span_days(first_date, last_date),
                "quality_statuses": quality_statuses,
                "span_two_year_ready": span_two_year_ready,
                "authoritative_two_year_ready": authoritative_two_year_ready,
            }

        return {
            "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "archive_root": str(self.archive_root),
            "universe_id": universe_id,
            "components": components,
        }

    def _render_audit_report(self, audit: Dict[str, Any]) -> str:
        rows: List[List[str]] = []
        for component, item in audit["components"].items():
            rows.append(
                [
                    component,
                    str(item.get("snapshot_count", 0)),
                    str(item.get("authoritative_snapshot_count", 0)),
                    item.get("first_date", "") or "N/A",
                    item.get("last_date", "") or "N/A",
                    str(item.get("span_days", 0)),
                    ",".join(item.get("quality_statuses", [])) or "N/A",
                    "YES" if item.get("span_two_year_ready") else "NO",
                    "YES" if item.get("authoritative_two_year_ready") else "NO",
                ]
            )
        lines = [
            "# Foundation Archive Audit",
            "",
            f"**Generated At**: {audit['generated_at']}",
            f"**Archive Root**: `{audit['archive_root']}`",
            f"**Universe**: {audit.get('universe_id', '') or 'unspecified'}",
            "",
            "## Component Summary",
            "",
        ]
        lines.extend(
            _render_table(
                [
                    "组件",
                    "Snapshots",
                    "Authoritative",
                    "首日",
                    "末日",
                    "SpanDays",
                    "Quality",
                    "Span2Y",
                    "Authoritative2Y",
                ],
                rows or [["N/A", "0", "0", "N/A", "N/A", "0", "N/A", "NO", "NO"]],
            )
        )
        return "\n".join(lines)

    def write_audit(self, audit: Dict[str, Any] | None = None) -> tuple[Path, Path]:
        payload = audit or self.build_audit()
        json_path = self.audit_root / "foundation_archive_audit.json"
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tag = datetime.utcnow().strftime("%Y%m%d")
        report_path = self.report_root / f"foundation_archive_audit_{tag}.md"
        report_path.write_text(self._render_audit_report(payload), encoding="utf-8")
        return json_path, report_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Archive and backfill data-foundation historical snapshots")
    parser.add_argument("--backfill-local", action="store_true", help="Replay local foundation stores into as-of-date archive snapshots")
    parser.add_argument("--audit-only", action="store_true", help="Only rebuild archive audit")
    parser.add_argument("--components", default="filings,news,sentiment,events,graph", help="Comma-separated components to process")
    parser.add_argument("--start-date", default="", help="Optional start date YYYY-MM-DD")
    parser.add_argument("--end-date", default="", help="Optional end date YYYY-MM-DD")
    parser.add_argument("--universe", default="csi800", help="Universe id, defaults to csi800")
    parser.add_argument("--filing-lookback-days", type=int, default=30, help="Rolling lookback for historical filings snapshots")
    args = parser.parse_args()

    components = [item.strip() for item in args.components.split(",") if item.strip()]
    builder = FoundationHistoryBuilder()

    if args.audit_only:
        audit = builder.build_audit(universe_id=args.universe)
        json_path, report_path = builder.write_audit(audit)
        print(f"AUDIT_JSON={json_path}")
        print(f"AUDIT_REPORT={report_path}")
        return

    result = builder.backfill_local_history(
        universe_id=args.universe,
        components=components,
        start_date=args.start_date,
        end_date=args.end_date,
        filing_lookback_days=args.filing_lookback_days,
    )
    print(f"AUDIT_JSON={result['audit_json']}")
    print(f"AUDIT_REPORT={result['audit_report']}")
    archived = [item for item in result["results"] if item.get("status") == "archived"]
    print(f"ARCHIVED={len(archived)}")


if __name__ == "__main__":
    main()
