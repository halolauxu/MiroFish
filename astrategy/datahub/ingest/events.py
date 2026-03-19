"""Build incremental event master records from filings and news ingestion."""

from __future__ import annotations

import json
import logging
import hashlib
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from astrategy.events.normalizer import legacy_event_to_master

from ..common import ensure_dir, ingest_root, pool_event_master_path

logger = logging.getLogger("astrategy.datahub.ingest.events")


def _events_root() -> Path:
    return ensure_dir(ingest_root() / "events")


def _daily_dir() -> Path:
    return ensure_dir(_events_root() / "daily")


def _normalize_date(value: str | None) -> str:
    if not value:
        return date.today().isoformat()
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return text[:10]


def _infer_event_type(title: str, source_type: str) -> str:
    text = str(title or "")
    rules = [
        ("buyback", ("回购", "增持", "员工持股")),
        ("earnings_surprise", ("业绩预告", "业绩快报", "业绩预增", "业绩预减", "业绩预亏", "年报", "季报")),
        ("order_win", ("中标", "重大合同", "订单")),
        ("cooperation", ("合作", "签署协议", "框架协议")),
        ("ma", ("收购", "并购", "重组", "资产注入")),
        ("management_change", ("辞职", "离任", "换届", "董事长", "总经理")),
        ("price_adjustment", ("涨价", "提价", "调价")),
        ("policy_risk", ("处罚", "立案", "风险提示", "退市")),
        ("policy_support", ("补贴", "政策支持", "专项资金")),
        ("technology_breakthrough", ("发布", "新品", "突破", "投产")),
    ]
    for event_type, keywords in rules:
        if any(keyword in text for keyword in keywords):
            return event_type
    return "other" if source_type == "news" else "other"


def _impact_level(event_type: str) -> str:
    if event_type in {"policy_risk", "ma", "earnings_surprise", "order_win"}:
        return "high"
    if event_type in {"cooperation", "buyback", "technology_breakthrough", "price_adjustment"}:
        return "medium"
    return "low"


def _load_existing_event_master() -> List[Dict[str, Any]]:
    path = pool_event_master_path()
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    return list(data) if isinstance(data, list) else []


def _security_name_map(security_master: List[Dict[str, Any]]) -> Dict[str, str]:
    return {
        str(item.get("ticker", "")).zfill(6): str(item.get("company_name", "")).strip()
        for item in security_master
        if str(item.get("ticker", "")).strip()
    }


def _stable_suffix(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]


def build_incremental_event_layer(
    security_master: List[Dict[str, Any]],
    filings_payload: Dict[str, Any],
    news_payload: Dict[str, Any],
    *,
    ingest_date: str | None = None,
) -> Dict[str, Any]:
    """Transform incremental filings and news into Event Master records."""
    as_of_date = _normalize_date(ingest_date)
    name_map = _security_name_map(security_master)
    existing = _load_existing_event_master()
    existing_ids = {str(item.get("event_id", "")).strip() for item in existing}

    incremental: List[Dict[str, Any]] = []

    for row in filings_payload.get("rows", []):
        ticker = str(row.get("ticker", "")).zfill(6)
        title = str(row.get("title", "")).strip()
        publish_date = _normalize_date(row.get("publish_date", as_of_date))
        event_type = _infer_event_type(title, "filing")
        event_id = f"filing:{publish_date}:{ticker}:{_stable_suffix(title)}"
        if event_id in existing_ids:
            continue
        legacy = {
            "event_id": event_id,
            "title": title,
            "type": event_type,
            "event_type": event_type,
            "stock_code": ticker,
            "stock_name": row.get("company_name") or name_map.get(ticker, ticker),
            "event_date": publish_date,
            "discover_time": f"{publish_date}T00:00:00",
            "available_at": f"{publish_date}T00:00:00",
            "summary": str(row.get("announcement_type", "")).strip() or title,
            "impact_level": _impact_level(event_type),
            "source": "announcement_incremental",
            "source_type": "filing",
            "raw_payload_ref": str(row.get("link", "")).strip(),
            "metadata": {
                "ingest_date": as_of_date,
                "announcement_type": row.get("announcement_type", ""),
            },
        }
        incremental.append(legacy_event_to_master(legacy))
        existing_ids.add(event_id)

    for row in news_payload.get("rows", []):
        ticker = str(row.get("ticker", "")).zfill(6)
        news_file = Path(str(row.get("news_file", "")).strip())
        if not news_file.exists():
            continue
        items = json.loads(news_file.read_text(encoding="utf-8"))
        for item in items:
            title = str(item.get("新闻标题", item.get("标题", item.get("title", "")))).strip()
            if not title:
                continue
            publish_date = _normalize_date(item.get("_normalized_publish_date", as_of_date))
            event_type = _infer_event_type(title, "news")
            if event_type == "other":
                continue
            event_id = f"news:{publish_date}:{ticker}:{_stable_suffix(title)}"
            if event_id in existing_ids:
                continue
            legacy = {
                "event_id": event_id,
                "title": title,
                "type": event_type,
                "event_type": event_type,
                "stock_code": ticker,
                "stock_name": name_map.get(ticker, ticker),
                "event_date": publish_date,
                "discover_time": item.get("发布时间", item.get("时间", f"{publish_date}T00:00:00")),
                "available_at": item.get("发布时间", item.get("时间", f"{publish_date}T00:00:00")),
                "summary": str(item.get("新闻内容", item.get("摘要", title))).strip()[:400],
                "impact_level": _impact_level(event_type),
                "source": "news_incremental",
                "source_type": "news",
                "raw_payload_ref": str(item.get("新闻链接", item.get("链接", ""))).strip(),
                "metadata": {
                    "ingest_date": as_of_date,
                    "publisher": item.get("文章来源", item.get("source", "")),
                },
            }
            incremental.append(legacy_event_to_master(legacy))
            existing_ids.add(event_id)

    payload = {
        "summary": {
            "ingest_date": as_of_date,
            "incremental_event_count": len(incremental),
            "total_event_count": len(existing) + len(incremental),
        },
        "rows": incremental,
        "event_master": existing + incremental,
    }
    logger.info(
        "Built incremental event layer: incremental=%d, total=%d",
        payload["summary"]["incremental_event_count"],
        payload["summary"]["total_event_count"],
    )
    return payload


def save_incremental_event_layer(payload: Dict[str, Any]) -> Dict[str, Path]:
    root = _events_root()
    daily_path = _daily_dir() / f"{payload['summary']['ingest_date']}.json"
    latest_path = root / "event_master.json"
    manifest_path = root / "event_manifest.json"
    daily_path.write_text(json.dumps(payload["rows"], ensure_ascii=False, indent=2), encoding="utf-8")
    latest_path.write_text(json.dumps(payload["event_master"], ensure_ascii=False, indent=2), encoding="utf-8")
    manifest_path.write_text(json.dumps(payload["summary"], ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("Saved incremental event layer: %s", latest_path)
    return {"daily": daily_path, "event_master": latest_path, "manifest": manifest_path}
