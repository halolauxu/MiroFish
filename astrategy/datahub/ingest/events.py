"""Build incremental event master records from filings and news ingestion."""

from __future__ import annotations

import json
import logging
import hashlib
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List

from astrategy.events.normalizer import legacy_event_to_master

from ..common import ensure_dir, ingest_root, pool_event_master_path

logger = logging.getLogger("astrategy.datahub.ingest.events")

_EVENT_RULES: List[tuple[str, tuple[str, ...]]] = [
    ("policy_risk", ("立案", "处罚", "问询函", "监管函", "风险提示", "退市", "退市风险", "诉讼", "仲裁", "违约", "失信", "爆雷", "计提减值")),
    ("policy_support", ("政策支持", "专项资金", "政府补助", "补贴", "税收优惠", "入选试点", "豁免", "摘帽", "撤销风险警示")),
    ("earnings_surprise", ("业绩预增", "业绩预减", "业绩预亏", "业绩预告", "业绩快报", "扭亏", "净利润", "营收", "年报", "中报", "季报")),
    ("order_win", ("中标", "订单", "重大合同", "采购合同", "框架订单", "签订合同")),
    ("cooperation", ("战略合作", "合作", "签署协议", "框架协议", "联合开发", "合作框架")),
    ("ma", ("收购", "并购", "重组", "资产注入", "股权收购", "重大资产出售", "借壳")),
    ("buyback", ("回购", "增持", "员工持股", "股权激励")),
    ("management_change", ("辞职", "离任", "换届", "董事长", "总经理", "董事会改选", "高管变动", "董秘", "cfo")),
    ("price_adjustment", ("涨价", "提价", "调价", "上调价格", "提价函", "报价上调")),
    ("technology_breakthrough", ("技术突破", "突破", "新品", "新产品", "发布", "量产", "投产", "获批", "注册证", "认证通过")),
    ("product_launch", ("上市", "发售", "上线", "新机", "首发", "发布会")),
    ("capacity_expansion", ("扩产", "扩建", "项目建设", "开工建设", "新产能", "产线", "投建", "项目投产")),
    ("capital_raise", ("定增", "非公开发行", "募资", "融资", "可转债", "配股", "发行股份")),
    ("dividend", ("分红", "派息", "送股", "转增", "特别分红")),
    ("supply_shortage", ("停产", "减产", "限产", "供应紧张", "供应短缺", "停工", "停机检修")),
    ("asset_sale", ("出售资产", "资产剥离", "挂牌转让", "转让股权")),
]

_THEME_RULES: List[tuple[str, tuple[str, ...]]] = [
    ("ai", ("人工智能", "ai", "大模型", "算力")),
    ("semiconductor", ("芯片", "半导体", "封测", "晶圆")),
    ("new_energy", ("新能源", "锂电", "储能", "光伏", "风电", "氢能")),
    ("auto", ("汽车", "智驾", "整车", "电池")),
    ("robotics", ("机器人", "自动化", "工业母机")),
    ("medicine", ("医药", "创新药", "器械", "医疗")),
    ("military", ("军工", "航空航天", "船舶")),
    ("consumer", ("白酒", "消费", "食品饮料", "家电")),
]


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


def _infer_event_type(
    title: str,
    source_type: str,
    *,
    summary: str = "",
    announcement_type: str = "",
) -> str:
    text = " ".join(
        value.strip().lower()
        for value in (str(title or ""), str(summary or ""), str(announcement_type or ""))
        if value and str(value).strip()
    )
    for event_type, keywords in _EVENT_RULES:
        if any(keyword.lower() in text for keyword in keywords):
            return event_type
    if source_type == "news" and any(keyword in text for keyword in ("热议", "爆发", "催化", "风口", "受益")):
        return "sentiment_reversal"
    return "other"


def _impact_level(event_type: str) -> str:
    if event_type in {
        "policy_risk",
        "ma",
        "earnings_surprise",
        "order_win",
        "supply_shortage",
        "technology_breakthrough",
    }:
        return "high"
    if event_type in {
        "cooperation",
        "buyback",
        "price_adjustment",
        "capacity_expansion",
        "capital_raise",
        "product_launch",
        "policy_support",
    }:
        return "medium"
    return "low"


def _infer_theme_tags(*texts: str) -> List[str]:
    text = " ".join(str(value or "").lower() for value in texts)
    tags = [
        tag
        for tag, keywords in _THEME_RULES
        if any(keyword.lower() in text for keyword in keywords)
    ]
    return tags[:5]


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


def _build_legacy_event(
    *,
    event_id: str,
    title: str,
    event_type: str,
    ticker: str,
    stock_name: str,
    event_date: str,
    discover_time: str,
    available_at: str,
    summary: str,
    source: str,
    source_type: str,
    raw_payload_ref: str,
    metadata: Dict[str, Any],
) -> Dict[str, Any]:
    theme_tags = _infer_theme_tags(title, summary, str(metadata.get("announcement_type", "")))
    return {
        "event_id": event_id,
        "title": title,
        "type": event_type,
        "event_type": event_type,
        "stock_code": ticker,
        "stock_name": stock_name,
        "event_date": event_date,
        "discover_time": discover_time,
        "available_at": available_at,
        "summary": summary,
        "impact_level": _impact_level(event_type),
        "source": source,
        "source_type": source_type,
        "theme_tags": theme_tags,
        "raw_payload_ref": raw_payload_ref,
        "metadata": metadata,
    }


def _append_if_new(
    incremental: List[Dict[str, Any]],
    existing_ids: set[str],
    legacy: Dict[str, Any],
) -> None:
    event_id = str(legacy.get("event_id", "")).strip()
    if not event_id or event_id in existing_ids:
        return
    incremental.append(legacy_event_to_master(legacy))
    existing_ids.add(event_id)


def build_incremental_event_layer(
    security_master: List[Dict[str, Any]],
    filings_payload: Dict[str, Any],
    news_payload: Dict[str, Any],
    *,
    sentiment_payload: Dict[str, Any] | None = None,
    ingest_date: str | None = None,
) -> Dict[str, Any]:
    """Transform filings, news, and sentiment summaries into Event Master records."""
    as_of_date = _normalize_date(ingest_date)
    name_map = _security_name_map(security_master)
    existing = _load_existing_event_master()
    existing_ids = {str(item.get("event_id", "")).strip() for item in existing}

    incremental: List[Dict[str, Any]] = []
    filing_event_count = 0
    news_event_count = 0
    sentiment_event_count = 0

    for row in filings_payload.get("rows", []):
        ticker = str(row.get("ticker", "")).zfill(6)
        title = str(row.get("title", "")).strip()
        if not title:
            continue
        announcement_type = str(row.get("announcement_type", "")).strip()
        publish_date = _normalize_date(row.get("publish_date", as_of_date))
        event_type = _infer_event_type(
            title,
            "filing",
            summary=announcement_type,
            announcement_type=announcement_type,
        )
        event_id = f"filing:{publish_date}:{ticker}:{_stable_suffix(title)}"
        before = len(existing_ids)
        legacy = _build_legacy_event(
            event_id=event_id,
            title=title,
            event_type=event_type,
            ticker=ticker,
            stock_name=row.get("company_name") or name_map.get(ticker, ticker),
            event_date=publish_date,
            discover_time=f"{publish_date}T00:00:00",
            available_at=f"{publish_date}T00:00:00",
            summary=announcement_type or title,
            source="announcement_incremental",
            source_type="filing",
            raw_payload_ref=str(row.get("link", "")).strip(),
            metadata={
                "ingest_date": as_of_date,
                "announcement_type": announcement_type,
            },
        )
        _append_if_new(incremental, existing_ids, legacy)
        if len(existing_ids) > before:
            filing_event_count += 1

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
            summary = str(item.get("新闻内容", item.get("摘要", title))).strip()[:400]
            publish_date = _normalize_date(item.get("_normalized_publish_date", as_of_date))
            event_type = _infer_event_type(title, "news", summary=summary)
            if event_type == "other":
                continue
            event_id = f"news:{publish_date}:{ticker}:{_stable_suffix(title)}"
            before = len(existing_ids)
            legacy = _build_legacy_event(
                event_id=event_id,
                title=title,
                event_type=event_type,
                ticker=ticker,
                stock_name=name_map.get(ticker, ticker),
                event_date=publish_date,
                discover_time=item.get("发布时间", item.get("时间", f"{publish_date}T00:00:00")),
                available_at=item.get("发布时间", item.get("时间", f"{publish_date}T00:00:00")),
                summary=summary,
                source="news_incremental",
                source_type="news",
                raw_payload_ref=str(item.get("新闻链接", item.get("链接", ""))).strip(),
                metadata={
                    "ingest_date": as_of_date,
                    "publisher": item.get("文章来源", item.get("source", "")),
                },
            )
            _append_if_new(incremental, existing_ids, legacy)
            if len(existing_ids) > before:
                news_event_count += 1

    for row in (sentiment_payload or {}).get("rows", []):
        ticker = str(row.get("ticker", "")).zfill(6)
        if not ticker or ticker == "000000":
            continue
        attention_score = float(row.get("attention_score", 0.0) or 0.0)
        avg_score = float(row.get("avg_sentiment_score", 0.0) or 0.0)
        hot_rank = int(row.get("hot_rank", 0) or 0)
        target_date_count = int(row.get("target_date_sentiment_count", 0) or 0)
        if attention_score < 0.55:
            continue
        if abs(avg_score) < 0.25 and hot_rank == 0:
            continue
        if target_date_count <= 0 and hot_rank == 0:
            continue
        publish_date = as_of_date
        title = f"{row.get('company_name', ticker)} 舆情热度变化"
        label = str(row.get("sentiment_label", "neutral")).strip() or "neutral"
        summary = (
            f"平均情绪分 {avg_score:+.2f}，关注度 {attention_score:.2f}，"
            f"热度排名 {hot_rank or '-'}，标签 {label}"
        )
        event_id = f"sentiment:{publish_date}:{ticker}:{_stable_suffix(summary)}"
        before = len(existing_ids)
        legacy = _build_legacy_event(
            event_id=event_id,
            title=title,
            event_type="sentiment_reversal",
            ticker=ticker,
            stock_name=str(row.get("company_name", "")).strip() or name_map.get(ticker, ticker),
            event_date=publish_date,
            discover_time=f"{publish_date}T00:00:00",
            available_at=f"{publish_date}T00:00:00",
            summary=summary,
            source="sentiment_incremental",
            source_type="sentiment",
            raw_payload_ref=str(row.get("detail_file", "")).strip(),
            metadata={
                "ingest_date": as_of_date,
                "attention_score": attention_score,
                "avg_sentiment_score": avg_score,
                "hot_rank": hot_rank,
                "sentiment_label": label,
            },
        )
        _append_if_new(incremental, existing_ids, legacy)
        if len(existing_ids) > before:
            sentiment_event_count += 1

    payload = {
        "summary": {
            "ingest_date": as_of_date,
            "incremental_event_count": len(incremental),
            "total_event_count": len(existing) + len(incremental),
            "filing_event_count": filing_event_count,
            "news_event_count": news_event_count,
            "sentiment_event_count": sentiment_event_count,
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
