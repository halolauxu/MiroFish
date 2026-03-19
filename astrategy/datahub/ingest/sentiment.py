"""Universe-level sentiment ingestion built from filings, company news, and hot-topic data."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from astrategy.data_collector.news import NewsCollector
from astrategy.strategies.s06_announcement_sentiment import AnnouncementSentimentStrategy

from ..common import (
    ensure_dir,
    repo_relative_path,
    resolve_repo_path,
    sentiment_manifest_path,
    sentiment_root,
)

logger = logging.getLogger("astrategy.datahub.ingest.sentiment")
_AUTHORITATIVE_HOT_TOPIC_FAMILIES = {"baidu_xueqiu"}

_POSITIVE_KEYWORDS = {
    "中标": 0.55,
    "订单": 0.35,
    "重大合同": 0.55,
    "超预期": 0.45,
    "扭亏": 0.45,
    "增长": 0.20,
    "上调": 0.18,
    "提价": 0.30,
    "回购": 0.30,
    "增持": 0.28,
    "分红": 0.26,
    "产品发布": 0.28,
    "新品": 0.22,
    "量产": 0.22,
    "扩产": 0.24,
    "投产": 0.24,
    "政策支持": 0.28,
    "补贴": 0.22,
    "补助": 0.22,
    "首发": 0.18,
}

_NEGATIVE_KEYWORDS = {
    "立案": -0.60,
    "处罚": -0.55,
    "问询": -0.30,
    "风险提示": -0.42,
    "退市": -0.70,
    "诉讼": -0.32,
    "仲裁": -0.25,
    "减持": -0.28,
    "亏损": -0.34,
    "下降": -0.20,
    "停产": -0.42,
    "减产": -0.32,
    "供应短缺": -0.28,
    "供应紧张": -0.22,
    "违约": -0.52,
    "爆雷": -0.70,
    "失信": -0.55,
}

_CATEGORY_KEYWORDS = [
    ("业绩公告", ("业绩", "利润", "营收", "预增", "预亏", "快报")),
    ("股权变动", ("增持", "减持", "回购", "员工持股", "股权激励")),
    ("资产重组", ("收购", "并购", "重组", "资产出售", "转让股权")),
    ("重大合同", ("合同", "订单", "中标", "框架协议")),
    ("产品进展", ("新品", "新产品", "发布", "量产", "投产", "获批")),
    ("政策影响", ("政策", "补贴", "补助", "税收优惠", "监管", "处罚")),
]


def _normalize_date(value: str | None) -> str:
    if not value:
        return date.today().isoformat()
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    if len(text) >= 10:
        return text[:10]
    return text


def _normalize_datetime(value: Any, fallback_date: str) -> str:
    text = str(value or "").strip()
    if not text:
        return f"{fallback_date}T00:00:00"
    normalized = text.replace("/", "-").replace("T", " ")
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y%m%d%H%M%S",
        "%Y%m%d%H%M",
        "%Y%m%d",
    ):
        try:
            dt = datetime.strptime(normalized, fmt)
            if fmt.endswith("%d"):
                return dt.strftime("%Y-%m-%dT00:00:00")
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    if len(text) >= 19 and text[10] in {" ", "T"}:
        return text[:19].replace(" ", "T")
    if len(text) >= 10:
        return f"{text[:10]}T00:00:00"
    return f"{fallback_date}T00:00:00"


def _ticker_dir() -> Path:
    return ensure_dir(sentiment_root() / "by_ticker")


def _daily_dir() -> Path:
    return ensure_dir(sentiment_root() / "daily")


def _manifest_path() -> Path:
    return sentiment_manifest_path()


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


def _resolve_news_file(path_value: str, ticker: str) -> Path:
    fallback = ensure_dir(sentiment_root().parent / "news" / "by_ticker") / f"{ticker}.json"
    return resolve_repo_path(path_value, fallback=fallback)


def _load_hot_topics_cache() -> List[Dict[str, Any]]:
    path = _hot_topics_path()
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    return payload if isinstance(payload, list) else []


def _parse_name_code_text(value: Any) -> tuple[str, str]:
    text = str(value or "").strip()
    if not text:
        return "", ""
    digits = "".join(ch for ch in text if ch.isdigit())
    code = digits[-6:] if len(digits) >= 6 else ""
    if code:
        idx = text.find(code)
        if idx >= 0:
            name = (text[:idx] + text[idx + len(code) :]).strip(" -_/()")
            return name or text, code
    return text, ""


def _infer_hot_topic_meta(items: List[Dict[str, Any]], *, source_mode: str) -> Dict[str, Any]:
    source_family = "none"
    live_sources: List[str] = []
    failed_sources: List[str] = []
    for item in items:
        family = str(item.get("_hot_source_family", item.get("_hot_source", ""))).strip()
        if family and family != "none":
            source_family = family
        if not live_sources:
            live_sources = [str(source).strip() for source in item.get("_hot_live_sources", []) if str(source).strip()]
        if not failed_sources:
            failed_sources = [str(source).strip() for source in item.get("_hot_failed_sources", []) if str(source).strip()]
    authoritative = source_family in _AUTHORITATIVE_HOT_TOPIC_FAMILIES
    return {
        "source_family": source_family,
        "source_mode": source_mode,
        "source_label": f"{source_family}_{source_mode}" if source_family != "none" else "none",
        "live_sources": live_sources,
        "failed_sources": failed_sources,
        "authoritative": authoritative,
    }


def _membership_name_map(universe_membership: Dict[str, Any], universe_id: str) -> Dict[str, str]:
    return {
        str(row.get("ticker", "")).zfill(6): str(row.get("company_name", "")).strip()
        for row in universe_membership.get("memberships", [])
        if row.get("universe_id") == universe_id and str(row.get("ticker", "")).strip()
    }


def _name_to_ticker_map(name_map: Dict[str, str]) -> Dict[str, str]:
    return {
        str(name).strip(): ticker
        for ticker, name in name_map.items()
        if str(name).strip()
    }


def _hot_topic_rank_map(items: Iterable[Dict[str, Any]], name_map: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    name_to_ticker = _name_to_ticker_map(name_map)
    mapping: Dict[str, Dict[str, Any]] = {}
    for idx, item in enumerate(items, start=1):
        raw_code = str(item.get("代码", item.get("股票代码", item.get("证券代码", "")))).strip()
        code = "".join(ch for ch in raw_code if ch.isdigit())[-6:]
        hot_name = str(item.get("股票简称", item.get("股票名称", item.get("名称", item.get("股票简称", ""))))).strip()
        if not hot_name:
            hot_name, parsed_code = _parse_name_code_text(item.get("名称/代码", ""))
            if not code:
                code = parsed_code
        if not code:
            possible_name = hot_name.strip()
            code = name_to_ticker.get(possible_name, "")
        if not code:
            continue
        mapping[code] = {
            "hot_rank": int(item.get("热度排名", idx) or idx),
            "hot_name": hot_name,
            "hot_reason": str(item.get("相关信息", item.get("热度来源", item.get("概念", item.get("板块名称", ""))))).strip(),
            "hot_source_family": str(item.get("_hot_source_family", item.get("_hot_source", ""))).strip(),
            "hot_raw": item,
        }
    return mapping


def _category_from_text(text: str) -> str:
    lowered = str(text or "").lower()
    for category, keywords in _CATEGORY_KEYWORDS:
        if any(keyword.lower() in lowered for keyword in keywords):
            return category
    return "其他"


def _score_text(text: str, *, source_type: str = "", hot_rank: int = 0) -> Dict[str, Any]:
    scorer = AnnouncementSentimentStrategy._rule_based_sentiment
    primary = scorer(text)
    lowered = str(text or "").lower()
    adjustment = 0.0
    matched_keywords: List[str] = []
    for keyword, delta in _POSITIVE_KEYWORDS.items():
        if keyword.lower() in lowered:
            adjustment += delta
            matched_keywords.append(keyword)
    for keyword, delta in _NEGATIVE_KEYWORDS.items():
        if keyword.lower() in lowered:
            adjustment += delta
            matched_keywords.append(keyword)

    if source_type == "hot_topic" and hot_rank > 0:
        adjustment += max(0.0, 0.28 - min(hot_rank, 100) * 0.002)

    base_score = float(primary.get("sentiment_score", 0.0) or 0.0)
    score = max(-1.0, min(1.0, round(base_score + adjustment, 4)))
    urgency = str(primary.get("urgency", "")).strip() or "medium"
    if hot_rank and hot_rank <= 20:
        urgency = "high"
    category = str(primary.get("category", "")).strip()
    if not category or category == "其他":
        category = _category_from_text(text)
    return {
        "sentiment_score": score,
        "category": category,
        "urgency": urgency,
        "key_phrases": list(primary.get("key_phrases", []) or []) + matched_keywords[:4],
    }


def _sentiment_label(score: float) -> str:
    if score >= 0.18:
        return "bullish"
    if score <= -0.18:
        return "bearish"
    return "neutral"


def _source_weight(source_type: str) -> float:
    return {
        "filing": 1.15,
        "news": 1.0,
        "hot_topic": 1.25,
    }.get(source_type, 1.0)


def _build_item(
    *,
    source_type: str,
    title: str,
    publish_time: Any,
    fallback_date: str,
    summary: str = "",
    category_hint: str = "",
    hot_rank: int = 0,
) -> Dict[str, Any]:
    available_at = _normalize_datetime(publish_time, fallback_date)
    publish_date = available_at[:10]
    scored = _score_text(" ".join(part for part in (title, summary, category_hint) if part), source_type=source_type, hot_rank=hot_rank)
    return {
        "source_type": source_type,
        "title": title,
        "summary": summary[:240],
        "publish_date": publish_date,
        "available_at": available_at,
        "sentiment_score": scored["sentiment_score"],
        "category": scored["category"] or category_hint,
        "urgency": scored["urgency"],
        "weight": _source_weight(source_type),
        "hot_rank": hot_rank,
    }


def _aggregate_sentiment(item_rows: List[Dict[str, Any]], as_of_date: str, hot_rank: int) -> Dict[str, Any]:
    if not item_rows:
        return {
            "sentiment_item_count": 0,
            "target_date_sentiment_count": 0,
            "avg_sentiment_score": 0.0,
            "latest_sentiment_score": 0.0,
            "attention_score": round(0.12 if hot_rank else 0.0, 4),
            "sentiment_label": "neutral",
            "bullish_item_count": 0,
            "bearish_item_count": 0,
        }

    ordered = sorted(item_rows, key=lambda item: str(item.get("available_at", "")))
    weighted_sum = 0.0
    total_weight = 0.0
    for item in ordered:
        weight = float(item.get("weight", 1.0) or 1.0)
        weighted_sum += float(item.get("sentiment_score", 0.0) or 0.0) * weight
        total_weight += weight
    avg_score = round(weighted_sum / max(total_weight, 1e-6), 4)
    latest_score = round(float(ordered[-1].get("sentiment_score", 0.0) or 0.0), 4)
    bullish_count = sum(1 for item in ordered if float(item.get("sentiment_score", 0.0) or 0.0) > 0.12)
    bearish_count = sum(1 for item in ordered if float(item.get("sentiment_score", 0.0) or 0.0) < -0.12)
    target_date_count = sum(1 for item in ordered if item.get("publish_date") == as_of_date)
    source_diversity = len({str(item.get("source_type", "")).strip() for item in ordered if str(item.get("source_type", "")).strip()})
    recency_boost = 1.0 if target_date_count > 0 else 0.0
    hot_rank_boost = 1.0 if hot_rank > 0 else 0.0
    intensity = max(abs(avg_score), abs(latest_score))
    attention_score = round(
        min(
            1.0,
            0.35 * min(1.0, len(ordered) / 8.0)
            + 0.25 * min(1.0, intensity)
            + 0.20 * recency_boost
            + 0.10 * hot_rank_boost
            + 0.10 * min(1.0, source_diversity / 3.0),
        ),
        4,
    )
    label = _sentiment_label(avg_score if abs(avg_score) >= abs(latest_score) * 0.75 else latest_score)
    return {
        "sentiment_item_count": len(ordered),
        "target_date_sentiment_count": target_date_count,
        "avg_sentiment_score": avg_score,
        "latest_sentiment_score": latest_score,
        "attention_score": attention_score,
        "sentiment_label": label,
        "bullish_item_count": bullish_count,
        "bearish_item_count": bearish_count,
    }


def _apply_internal_hot_rank(rows: List[Dict[str, Any]], *, top_n: int = 200) -> None:
    scored = [
        row for row in rows
        if float(row.get("attention_score", 0.0) or 0.0) > 0
    ]
    scored.sort(
        key=lambda row: (
            -float(row.get("attention_score", 0.0) or 0.0),
            -int(row.get("target_date_sentiment_count", 0) or 0),
            -int(row.get("sentiment_item_count", 0) or 0),
            -abs(float(row.get("latest_sentiment_score", 0.0) or 0.0)),
            str(row.get("ticker", "")),
        )
    )
    rank_map = {
        str(row.get("ticker", "")).zfill(6): idx
        for idx, row in enumerate(scored[: max(top_n, 0)], start=1)
    }
    for row in rows:
        ticker = str(row.get("ticker", "")).zfill(6)
        rank = rank_map.get(ticker, 0)
        row["hot_rank"] = rank
        if rank > 0:
            reason = (
                f"internal_attention_rank: score={float(row.get('attention_score', 0.0) or 0.0):.2f}, "
                f"items={int(row.get('sentiment_item_count', 0) or 0)}"
            )
            row["hot_reason"] = reason
            detail_path = resolve_repo_path(
                str(row.get("detail_file", "")).strip(),
                fallback=_ticker_dir() / f"{ticker}.json",
            )
            if detail_path.exists():
                try:
                    payload = json.loads(detail_path.read_text(encoding="utf-8"))
                except Exception:
                    payload = {}
                payload["hot_rank"] = rank
                payload["hot_reason"] = reason
                payload["hot_topic_source"] = "internal_rank"
                detail_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


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
    hot_topic_meta = {
        "source_family": "none",
        "source_mode": "none",
        "source_label": "none",
        "live_sources": [],
        "failed_sources": [],
        "authoritative": False,
    }
    hot_topic_source = "none"
    hot_topic_api_failed = False
    if collect_sentiment:
        collector = NewsCollector()
        try:
            hot_topics = collector.get_market_hot_topics(limit=hot_topic_limit)
            hot_topic_meta = dict(collector.last_hot_topic_meta or hot_topic_meta)
            hot_topic_source = str(hot_topic_meta.get("source_label", "")).strip() or ("live" if hot_topics else "unavailable")
            hot_topic_api_failed = not bool(hot_topics)
        except Exception as exc:
            logger.warning("Hot topic fetch failed: %s", exc)
            hot_topics = []
            hot_topic_source = "unavailable"
            hot_topic_api_failed = True
    if not hot_topics:
        hot_topics = _load_hot_topics_cache()
        if hot_topics:
            hot_topic_meta = _infer_hot_topic_meta(hot_topics, source_mode="cache")
            hot_topic_source = hot_topic_meta["source_label"]
            hot_topic_api_failed = False
        elif collect_sentiment:
            hot_topic_source = "unavailable"
            hot_topic_api_failed = True
    hot_rank_map = _hot_topic_rank_map(hot_topics, name_map)

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
    api_failures = 0

    for ticker in tickers:
        cached = existing.get(ticker)
        hot_rank_info = hot_rank_map.get(ticker, {})
        if cached and not refresh_sentiment:
            detail_path = resolve_repo_path(
                str(cached.get("detail_file", "")).strip(),
                fallback=_ticker_dir() / f"{ticker}.json",
            )
            row = {
                **cached,
                "detail_file": repo_relative_path(detail_path),
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
            item_rows.append(
                _build_item(
                    source_type="filing",
                    title=title,
                    publish_time=filing.get("publish_time", filing.get("publish_date", as_of_date)),
                    fallback_date=_normalize_date(filing.get("publish_date", as_of_date)),
                    summary=str(filing.get("announcement_type", "")).strip(),
                    category_hint=str(filing.get("announcement_type", "")).strip(),
                )
            )

        news_row = news_manifest_map.get(ticker, {})
        news_file = _resolve_news_file(str(news_row.get("news_file", "")).strip(), ticker)
        if news_file.exists():
            try:
                news_items = json.loads(news_file.read_text(encoding="utf-8"))
            except Exception:
                news_items = []
                api_failures += 1
            for item in news_items:
                title = str(item.get("新闻标题", item.get("标题", item.get("title", "")))).strip()
                if not title:
                    continue
                summary = str(item.get("新闻内容", item.get("摘要", title))).strip()
                item_rows.append(
                    _build_item(
                        source_type="news",
                        title=title,
                        publish_time=item.get("发布时间", item.get("时间", item.get("_normalized_publish_date", as_of_date))),
                        fallback_date=_normalize_date(item.get("_normalized_publish_date", as_of_date)),
                        summary=summary,
                        category_hint=str(item.get("文章来源", item.get("source", ""))).strip(),
                    )
                )

        if hot_rank_info:
            item_rows.append(
                _build_item(
                    source_type="hot_topic",
                    title=str(hot_rank_info.get("hot_name", "")).strip() or f"{name_map.get(ticker, ticker)} 热榜上榜",
                    publish_time=as_of_date,
                    fallback_date=as_of_date,
                    summary=str(hot_rank_info.get("hot_reason", "")).strip(),
                    category_hint="市场热度",
                    hot_rank=int(hot_rank_info.get("hot_rank", 0) or 0),
                )
            )

        aggregate = _aggregate_sentiment(item_rows, as_of_date, int(hot_rank_info.get("hot_rank", 0) or 0))
        label = aggregate["sentiment_label"]
        if label == "bullish":
            bullish += 1
        elif label == "bearish":
            bearish += 1
        total_items += int(aggregate["sentiment_item_count"])

        detail_path = _ticker_dir() / f"{ticker}.json"
        ordered_items = sorted(item_rows, key=lambda item: str(item.get("available_at", "")))
        detail_payload = {
            "ticker": ticker,
            "company_name": name_map.get(ticker, ticker),
            "ingest_date": as_of_date,
            "items": ordered_items,
            "hot_rank": int(hot_rank_info.get("hot_rank", 0) or 0),
            "hot_reason": str(hot_rank_info.get("hot_reason", "")).strip(),
            "hot_topic_source": hot_topic_source,
        }
        detail_path.write_text(json.dumps(detail_payload, ensure_ascii=False, indent=2), encoding="utf-8")

        rows.append(
            {
                "ticker": ticker,
                "company_name": name_map.get(ticker, ticker),
                **aggregate,
                "hot_rank": int(hot_rank_info.get("hot_rank", 0) or 0),
                "hot_reason": str(hot_rank_info.get("hot_reason", "")).strip(),
                "detail_file": repo_relative_path(detail_path),
                "status": "analyzed" if aggregate["sentiment_item_count"] > 0 else "empty",
                "from_cache": False,
            }
        )

    if not any(int(row.get("hot_rank", 0) or 0) > 0 for row in rows) and not collect_sentiment:
        _apply_internal_hot_rank(rows, top_n=hot_topic_limit)
        hot_topic_source = "internal_rank"
        hot_topic_meta = {
            "source_family": "internal_rank",
            "source_mode": "derived",
            "source_label": "internal_rank",
            "live_sources": [],
            "failed_sources": [],
            "authoritative": False,
        }

    summary = {
        "universe_id": universe_id,
        "ingest_date": as_of_date,
        "collect_sentiment": bool(collect_sentiment),
        "requested_tickers": len(tickers),
        "with_sentiment": sum(1 for row in rows if int(row.get("sentiment_item_count", 0)) > 0),
        "with_hot_rank": sum(1 for row in rows if int(row.get("hot_rank", 0)) > 0),
        "authoritative_hot_rank_count": (
            sum(1 for row in rows if int(row.get("hot_rank", 0)) > 0)
            if hot_topic_meta.get("authoritative")
            else 0
        ),
        "bullish_tickers": bullish,
        "bearish_tickers": bearish,
        "total_scored_items": total_items,
        "hot_topic_source": hot_topic_source,
        "hot_topic_authoritative": bool(hot_topic_meta.get("authoritative", False)),
        "hot_topic_live_sources": list(hot_topic_meta.get("live_sources", [])),
        "hot_topic_failed_sources": list(hot_topic_meta.get("failed_sources", [])),
        "hot_topic_api_failed": hot_topic_api_failed,
        "api_failure_count": api_failures + int(hot_topic_api_failed),
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
