"""Incremental filing ingestion for a target universe."""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from astrategy.data_collector.announcement import AnnouncementCollector

from ..common import ensure_dir, filings_manifest_path, ingest_root, repo_relative_path

logger = logging.getLogger("astrategy.datahub.ingest.filings")

_DEFAULT_FILING_LOOKBACK_DAYS = 30
_DEFAULT_CNINFO_WORKERS = 2

_ANNOUNCEMENT_TYPE_RULES: List[tuple[str, tuple[str, ...]]] = [
    ("业绩预告", ("业绩预告", "业绩快报", "预增", "预减", "预亏", "扭亏")),
    ("权益分派", ("分红", "派息", "权益分派", "送股", "转增")),
    ("股权变动", ("减持", "增持", "回购", "股份变动", "股权变动")),
    ("股权激励", ("股权激励", "限制性股票", "股票期权", "员工持股")),
    ("增发融资", ("定增", "向特定对象发行", "非公开发行", "募资", "融资")),
    ("债券融资", ("可转债", "公司债", "中期票据", "短融", "永续债")),
    ("重大合同", ("中标", "合同", "订单", "框架协议", "战略合作")),
    ("产品进展", ("新品", "新产品", "发布", "量产", "投产", "注册证", "获批")),
    ("产能建设", ("扩产", "扩建", "产能", "开工", "项目建设", "项目投产")),
    ("风险事项", ("立案", "处罚", "问询", "诉讼", "仲裁", "风险提示", "退市")),
]


def _filings_root() -> Path:
    return ensure_dir(ingest_root() / "filings")


def _ticker_dir() -> Path:
    return ensure_dir(_filings_root() / "by_ticker")


def _daily_dir() -> Path:
    return ensure_dir(_filings_root() / "daily")


def _manifest_path() -> Path:
    return filings_manifest_path()


def _normalize_date(value: str | None) -> str:
    if not value:
        return date.today().isoformat()
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text[:10]


def _window_start_date(as_of_date: str, lookback_days: int) -> str:
    anchor = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    start = anchor - timedelta(days=max(lookback_days - 1, 0))
    return start.isoformat()


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


def _extract_publish_time(row: Dict[str, Any], fallback_date: str) -> str:
    for key in ("发布时间", "公告时间", "时间", "publish_time"):
        value = str(row.get(key, "")).strip()
        if value:
            return value
    return fallback_date


def _extract_link(row: Dict[str, Any]) -> str:
    for key in ("公告链接", "链接", "网址", "link", "url"):
        value = str(row.get(key, "")).strip()
        if value:
            return value
    return ""


def _infer_announcement_type(title: str) -> str:
    text = str(title or "").strip()
    for category, keywords in _ANNOUNCEMENT_TYPE_RULES:
        if any(keyword in text for keyword in keywords):
            return category
    return ""


def _stable_key(row: Dict[str, Any]) -> str:
    return "|".join(
        [
            str(row.get("ticker", "")).strip(),
            str(row.get("publish_date", "")).strip(),
            str(row.get("title", "")).strip(),
            str(row.get("link", "")).strip(),
        ]
    )


def _normalize_filing_row(
    row: Dict[str, Any],
    *,
    fallback_ticker: str = "",
    fallback_name: str = "",
    source: str,
) -> Dict[str, Any]:
    ticker = _extract_ticker(row) or fallback_ticker
    title = _extract_title(row)
    if not ticker or not title:
        return {}
    publish_date = _normalize_date(
        str(row.get("公告日期", row.get("公告时间", row.get("发布时间", "")))).strip()
    )
    company_name = _extract_name(row) or fallback_name
    return {
        "ticker": ticker,
        "company_name": company_name,
        "title": title,
        "announcement_type": str(row.get("公告类型", row.get("类型", ""))).strip()
        or _infer_announcement_type(title),
        "publish_date": publish_date,
        "publish_time": _extract_publish_time(row, publish_date),
        "link": _extract_link(row),
        "source": source,
        "raw": row,
    }


def _dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: set[str] = set()
    deduped: List[Dict[str, Any]] = []
    for row in sorted(
        rows,
        key=lambda item: (
            str(item.get("ticker", "")),
            str(item.get("publish_date", "")),
            str(item.get("title", "")),
            str(item.get("link", "")),
        ),
    ):
        key = _stable_key(row)
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def _filter_window_rows(rows: List[Dict[str, Any]], start_date: str, end_date: str) -> List[Dict[str, Any]]:
    return [
        row
        for row in rows
        if start_date <= str(row.get("publish_date", "")).strip() <= end_date
    ]


def _ticker_cache_path(ticker: str) -> Path:
    return _ticker_dir() / f"{ticker}.json"


def _load_cached_ticker_rows(ticker: str, *, start_date: str, end_date: str) -> List[Dict[str, Any]] | None:
    path = _ticker_cache_path(ticker)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    items = payload if isinstance(payload, list) else payload.get("rows", [])
    if not isinstance(items, list):
        return None
    rows = [row for row in items if isinstance(row, dict)]
    return _filter_window_rows(rows, start_date, end_date)


def _collect_ticker_filings(
    collector: AnnouncementCollector,
    *,
    ticker: str,
    company_name: str,
    start_date: str,
    end_date: str,
    refresh_filings: bool,
) -> Dict[str, Any]:
    cached_rows = _load_cached_ticker_rows(ticker, start_date=start_date, end_date=end_date)
    path = _ticker_cache_path(ticker)
    if cached_rows is not None and not refresh_filings:
        return {
            "ticker": ticker,
            "company_name": company_name,
            "status": "cached",
            "from_cache": True,
            "raw_count": len(cached_rows),
            "rows": cached_rows,
            "filing_file": repo_relative_path(path),
            "error": "",
        }

    raw_rows = collector.get_company_announcements_cninfo(
        ticker,
        start=start_date,
        end=end_date,
        market="沪深京",
    )
    important_rows = collector.filter_important_announcements(raw_rows, title_key="公告标题")
    normalized_rows = _dedupe_rows(
        [
            normalized
            for item in important_rows
            if (normalized := _normalize_filing_row(
                item,
                fallback_ticker=ticker,
                fallback_name=company_name,
                source="cninfo.disclosure_report",
            ))
        ]
    )
    path.write_text(
        json.dumps(normalized_rows, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return {
        "ticker": ticker,
        "company_name": company_name,
        "status": "fetched",
        "from_cache": False,
        "raw_count": len(raw_rows),
        "rows": normalized_rows,
        "filing_file": repo_relative_path(path),
        "error": "",
    }


def build_filings_layer(
    universe_membership: Dict[str, Any],
    universe_id: str = "csi800",
    *,
    ingest_date: str | None = None,
    collect_filings: bool = False,
    filing_lookback_days: int = _DEFAULT_FILING_LOOKBACK_DAYS,
    refresh_filings: bool = False,
    cninfo_worker_count: int = _DEFAULT_CNINFO_WORKERS,
) -> Dict[str, Any]:
    """Fetch authoritative CNInfo announcements for the target universe."""
    as_of_date = _normalize_date(ingest_date)
    if not collect_filings and _manifest_path().exists():
        try:
            payload = json.loads(_manifest_path().read_text(encoding="utf-8"))
            if isinstance(payload, dict) and payload.get("rows") is not None:
                return payload
        except Exception:
            pass

    lookback_days = max(1, int(filing_lookback_days or _DEFAULT_FILING_LOOKBACK_DAYS))
    start_date = _window_start_date(as_of_date, lookback_days)
    membership_rows = [
        row
        for row in universe_membership.get("memberships", [])
        if row.get("universe_id") == universe_id
    ]
    ticker_name_map = {
        str(row.get("ticker", "")).zfill(6): str(row.get("company_name", "")).strip()
        for row in membership_rows
        if str(row.get("ticker", "")).strip()
    }
    tickers = sorted(ticker_name_map)

    payload = {
        "summary": {
            "universe_id": universe_id,
            "ingest_date": as_of_date,
            "collect_filings": bool(collect_filings),
            "source": "cninfo.disclosure_report",
            "source_quality": "authoritative",
            "lookback_days": lookback_days,
            "window_start_date": start_date,
            "requested_tickers": len(tickers),
            "raw_announcements": 0,
            "important_announcements": 0,
            "matched_universe_announcements": 0,
            "matched_universe_tickers": 0,
            "target_date_announcements": 0,
            "target_date_tickers": 0,
            "fetched_tickers": 0,
            "cached_tickers": 0,
            "failed_tickers": 0,
            "api_failure_count": 0,
        },
        "rows": [],
        "tickers": [],
    }
    if not collect_filings:
        logger.info("Filings collection skipped for %s", universe_id)
        return payload

    collector = AnnouncementCollector()
    ticker_payloads: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, int(cninfo_worker_count or 1))) as executor:
        futures = {
            executor.submit(
                _collect_ticker_filings,
                collector,
                ticker=ticker,
                company_name=ticker_name_map.get(ticker, ""),
                start_date=start_date,
                end_date=as_of_date,
                refresh_filings=refresh_filings,
            ): ticker
            for ticker in tickers
        }
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {
                    "ticker": ticker,
                    "company_name": ticker_name_map.get(ticker, ""),
                    "status": "failed",
                    "from_cache": False,
                    "raw_count": 0,
                    "rows": [],
                    "filing_file": repo_relative_path(_ticker_cache_path(ticker)),
                    "error": str(exc),
                }
            ticker_payloads.append(result)

    rows: List[Dict[str, Any]] = []
    ticker_rows: List[Dict[str, Any]] = []
    for result in sorted(ticker_payloads, key=lambda item: str(item.get("ticker", ""))):
        filing_rows = _filter_window_rows(list(result.get("rows", []) or []), start_date, as_of_date)
        target_date_count = sum(1 for row in filing_rows if str(row.get("publish_date", "")) == as_of_date)
        latest_publish_date = max((str(row.get("publish_date", "")) for row in filing_rows), default="")
        ticker_rows.append(
            {
                "ticker": result["ticker"],
                "company_name": result["company_name"],
                "filing_count": len(filing_rows),
                "target_date_filing_count": target_date_count,
                "latest_publish_date": latest_publish_date,
                "filing_file": result["filing_file"],
                "status": result["status"],
                "last_attempt_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                "error": result["error"],
                "from_cache": bool(result["from_cache"]),
            }
        )
        rows.extend(filing_rows)

    rows = sorted(
        _dedupe_rows(rows),
        key=lambda item: (
            str(item.get("publish_date", "")),
            str(item.get("ticker", "")),
            str(item.get("title", "")),
        ),
        reverse=True,
    )

    payload["summary"] = {
        "universe_id": universe_id,
        "ingest_date": as_of_date,
        "collect_filings": True,
        "source": "cninfo.disclosure_report",
        "source_quality": "authoritative",
        "lookback_days": lookback_days,
        "window_start_date": start_date,
        "requested_tickers": len(tickers),
        "raw_announcements": sum(int(result.get("raw_count", 0) or 0) for result in ticker_payloads),
        "important_announcements": len(rows),
        "matched_universe_announcements": len(rows),
        "matched_universe_tickers": sum(1 for row in ticker_rows if int(row.get("filing_count", 0)) > 0),
        "target_date_announcements": sum(1 for row in rows if str(row.get("publish_date", "")) == as_of_date),
        "target_date_tickers": sum(1 for row in ticker_rows if int(row.get("target_date_filing_count", 0)) > 0),
        "fetched_tickers": sum(1 for row in ticker_rows if row["status"] == "fetched"),
        "cached_tickers": sum(1 for row in ticker_rows if bool(row.get("from_cache"))),
        "failed_tickers": sum(1 for row in ticker_rows if row["status"] == "failed"),
        "api_failure_count": sum(1 for row in ticker_rows if row["status"] == "failed"),
    }
    payload["rows"] = rows
    payload["tickers"] = ticker_rows
    logger.info(
        "Built filings layer for %s: tickers=%d, with_filings=%d, target_date=%d, rows=%d",
        universe_id,
        payload["summary"]["requested_tickers"],
        payload["summary"]["matched_universe_tickers"],
        payload["summary"]["target_date_tickers"],
        len(rows),
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
    return {"daily": daily_path, "manifest": manifest_path, "by_ticker": _ticker_dir()}
