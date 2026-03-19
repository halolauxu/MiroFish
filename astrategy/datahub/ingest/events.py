"""Build incremental event master records from filings and news ingestion."""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List

from astrategy.events.normalizer import legacy_event_to_master

from ..common import ensure_dir, ingest_root, pool_event_master_path, resolve_repo_path

logger = logging.getLogger("astrategy.datahub.ingest.events")

_EVENT_RULES: List[Dict[str, Any]] = [
    {
        "event_type": "policy_risk",
        "event_subtype": "policy_risk.regulatory_penalty",
        "keywords": ("立案", "处罚", "证监会立案", "行政处罚", "监管处罚", "违法违规"),
    },
    {
        "event_type": "policy_risk",
        "event_subtype": "policy_risk.inquiry",
        "keywords": ("问询函", "监管函", "关注函", "交易所问询", "风险提示"),
    },
    {
        "event_type": "policy_risk",
        "event_subtype": "policy_risk.litigation",
        "keywords": ("诉讼", "仲裁", "司法冻结", "资产冻结", "查封", "执行裁定"),
    },
    {
        "event_type": "policy_risk",
        "event_subtype": "policy_risk.abnormal_trading",
        "keywords": ("股票交易异常波动", "异常波动公告", "交易风险提示"),
    },
    {
        "event_type": "policy_risk",
        "event_subtype": "policy_risk.delisting",
        "keywords": ("退市", "退市风险", "*st", "终止上市", "暂停上市"),
    },
    {
        "event_type": "policy_support",
        "event_subtype": "policy_support.subsidy",
        "keywords": ("政府补助", "补贴", "专项资金", "奖励资金", "财政补助"),
    },
    {
        "event_type": "policy_support",
        "event_subtype": "policy_support.tax_incentive",
        "keywords": ("税收优惠", "税收减免", "增值税返还", "所得税优惠"),
    },
    {
        "event_type": "policy_support",
        "event_subtype": "policy_support.pilot_program",
        "keywords": ("入选试点", "示范项目", "白名单", "豁免", "摘帽", "撤销风险警示"),
    },
    {
        "event_type": "earnings_surprise",
        "event_subtype": "earnings_surprise.guidance",
        "keywords": ("业绩预增", "业绩预减", "业绩预亏", "业绩预告", "业绩快报", "扭亏", "超预期"),
    },
    {
        "event_type": "earnings_surprise",
        "event_subtype": "earnings_surprise.report",
        "keywords": ("年报", "年度报告", "年度报告摘要", "中报", "季报", "一季报", "半年报", "净利润", "营收"),
    },
    {
        "event_type": "earnings_surprise",
        "event_subtype": "earnings_surprise.impairment",
        "keywords": ("计提资产减值准备", "资产减值准备", "信用减值损失"),
    },
    {
        "event_type": "order_win",
        "event_subtype": "order_win.bid",
        "keywords": ("中标", "预中标", "中标候选人", "重大合同", "采购合同", "订单"),
    },
    {
        "event_type": "cooperation",
        "event_subtype": "cooperation.strategic",
        "keywords": ("战略合作", "签署协议", "框架协议", "联合开发", "合作框架", "共建", "共同投资", "产业基金"),
    },
    {
        "event_type": "cooperation",
        "event_subtype": "cooperation.investment",
        "keywords": ("对外投资", "设立子公司", "增资子公司", "增资扩股"),
    },
    {
        "event_type": "cooperation",
        "event_subtype": "cooperation.related_party",
        "keywords": ("日常关联交易", "关联交易预计", "重大关联交易"),
    },
    {
        "event_type": "ma",
        "event_subtype": "ma.acquisition",
        "keywords": ("收购", "并购", "股权收购", "借壳", "资产注入"),
    },
    {
        "event_type": "ma",
        "event_subtype": "ma.restructuring",
        "keywords": ("重组", "重大资产重组", "吸收合并"),
    },
    {
        "event_type": "buyback",
        "event_subtype": "buyback.shareholder_action",
        "keywords": ("回购", "增持", "员工持股", "股权激励", "回购注销", "限制性股票注销", "股份注销"),
    },
    {
        "event_type": "share_reduction",
        "event_subtype": "share_reduction.disposal",
        "keywords": ("减持", "减持计划", "减持股份", "减持预披露", "清仓减持"),
    },
    {
        "event_type": "share_pledge",
        "event_subtype": "share_pledge.collateral_change",
        "keywords": ("股份质押", "股票质押", "解除质押", "补充质押", "质押及解除质押", "质押变动"),
    },
    {
        "event_type": "management_change",
        "event_subtype": "management_change.executive",
        "keywords": ("辞职", "离任", "换届", "董事长", "总经理", "高管变动", "董秘", "cfo", "聘任", "任命", "任职资格核准"),
    },
    {
        "event_type": "guarantee",
        "event_subtype": "guarantee.external_support",
        "keywords": ("担保", "反担保", "担保进展"),
    },
    {
        "event_type": "price_adjustment",
        "event_subtype": "price_adjustment.raise",
        "keywords": ("涨价", "提价", "调价", "上调价格", "提价函", "报价上调"),
    },
    {
        "event_type": "technology_breakthrough",
        "event_subtype": "technology_breakthrough.approval",
        "keywords": ("获批", "注册证", "认证通过", "临床", "技术突破", "技术进展"),
    },
    {
        "event_type": "technology_breakthrough",
        "event_subtype": "technology_breakthrough.mass_production",
        "keywords": ("量产", "试生产", "投产", "首台套", "突破"),
    },
    {
        "event_type": "product_launch",
        "event_subtype": "product_launch.new_release",
        "keywords": ("新品", "新产品", "发布", "发售", "上线", "首发", "发布会", "上市销售"),
    },
    {
        "event_type": "capacity_expansion",
        "event_subtype": "capacity_expansion.construction",
        "keywords": ("扩产", "扩建", "项目建设", "开工建设", "新产能", "产线", "投建", "项目投产"),
    },
    {
        "event_type": "capital_raise",
        "event_subtype": "capital_raise.equity",
        "keywords": ("定增", "非公开发行", "募资", "融资", "配股", "发行股份", "再融资"),
    },
    {
        "event_type": "capital_raise",
        "event_subtype": "capital_raise.debt",
        "keywords": ("可转债", "公司债", "中期票据", "短融", "永续债"),
    },
    {
        "event_type": "capital_raise",
        "event_subtype": "capital_raise.redemption",
        "keywords": ("提前赎回", "赎回暨摘牌", "本息兑付", "优先股赎回", "回售结果", "停止交易"),
    },
    {
        "event_type": "capital_raise",
        "event_subtype": "capital_raise.credit_line",
        "keywords": ("综合授信", "银行授信", "授信额度", "授信协议"),
    },
    {
        "event_type": "dividend",
        "event_subtype": "dividend.cash",
        "keywords": ("分红", "派息", "特别分红", "现金红利", "利润分配预案", "利润分配方案", "权益分派实施", "权益分派方案"),
    },
    {
        "event_type": "dividend",
        "event_subtype": "dividend.stock",
        "keywords": ("送股", "转增", "高送转"),
    },
    {
        "event_type": "asset_sale",
        "event_subtype": "asset_sale.divestiture",
        "keywords": ("出售资产", "资产剥离", "挂牌转让", "转让股权", "出售子公司"),
    },
    {
        "event_type": "supply_shortage",
        "event_subtype": "supply_shortage.production_disruption",
        "keywords": ("停产", "减产", "限产", "停工", "停机检修", "供应短缺", "供应紧张"),
    },
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


def _infer_event_classification(
    title: str,
    source_type: str,
    *,
    summary: str = "",
    announcement_type: str = "",
) -> Dict[str, Any]:
    text = " ".join(
        value.strip().lower()
        for value in (str(title or ""), str(summary or ""), str(announcement_type or ""))
        if value and str(value).strip()
    )
    for rule in _EVENT_RULES:
        matches = [keyword for keyword in rule["keywords"] if keyword.lower() in text]
        if matches:
            return {
                "event_type": rule["event_type"],
                "event_subtype": rule["event_subtype"],
                "matched_keywords": matches[:6],
            }
    if source_type == "news" and any(keyword in text for keyword in ("热议", "爆发", "催化", "风口", "受益", "热榜")):
        return {
            "event_type": "sentiment_reversal",
            "event_subtype": "sentiment_reversal.hot_topic",
            "matched_keywords": ["市场热度"],
        }
    return {
        "event_type": "other",
        "event_subtype": "other",
        "matched_keywords": [],
    }


def _impact_level(event_type: str) -> str:
    if event_type in {
        "policy_risk",
        "ma",
        "earnings_surprise",
        "order_win",
        "share_reduction",
        "supply_shortage",
        "technology_breakthrough",
        "asset_sale",
    }:
        return "high"
    if event_type in {
        "cooperation",
        "buyback",
        "share_pledge",
        "management_change",
        "price_adjustment",
        "capacity_expansion",
        "capital_raise",
        "guarantee",
        "product_launch",
        "sentiment_reversal",
        "policy_support",
        "dividend",
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
    event_subtype: str,
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
        "event_subtype": event_subtype,
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


def _append_or_replace(
    incremental: List[Dict[str, Any]],
    existing_master: List[Dict[str, Any]],
    existing_ids: set[str],
    existing_index: Dict[str, int],
    legacy: Dict[str, Any],
) -> str:
    event_id = str(legacy.get("event_id", "")).strip()
    if not event_id:
        return "skipped"
    master_record = legacy_event_to_master(legacy)
    if event_id in existing_ids:
        existing_master[existing_index[event_id]] = master_record
        incremental.append(master_record)
        return "updated"
    existing_index[event_id] = len(existing_master)
    existing_master.append(master_record)
    incremental.append(master_record)
    existing_ids.add(event_id)
    return "inserted"


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
    existing_index = {
        str(item.get("event_id", "")).strip(): idx
        for idx, item in enumerate(existing)
        if str(item.get("event_id", "")).strip()
    }

    incremental: List[Dict[str, Any]] = []
    filing_event_count = 0
    news_event_count = 0
    sentiment_event_count = 0
    filing_event_update_count = 0
    news_event_update_count = 0
    sentiment_event_update_count = 0
    skipped_news_other = 0
    skipped_filing_other = 0

    for row in filings_payload.get("rows", []):
        ticker = str(row.get("ticker", "")).zfill(6)
        title = str(row.get("title", "")).strip()
        if not title:
            continue
        announcement_type = str(row.get("announcement_type", "")).strip()
        publish_date = _normalize_date(row.get("publish_date", as_of_date))
        publish_time = _normalize_datetime(row.get("publish_time", publish_date), publish_date)
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
            stock_name=row.get("company_name") or name_map.get(ticker, ticker),
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
        append_status = _append_or_replace(incremental, existing, existing_ids, existing_index, legacy)
        if append_status == "inserted":
            filing_event_count += 1
        elif append_status == "updated":
            filing_event_update_count += 1

    for row in news_payload.get("rows", []):
        ticker = str(row.get("ticker", "")).zfill(6)
        news_file = resolve_repo_path(
            str(row.get("news_file", "")).strip(),
            fallback=ingest_root() / "news" / "by_ticker" / f"{ticker}.json",
        )
        if not news_file.exists():
            continue
        items = json.loads(news_file.read_text(encoding="utf-8"))
        for item in items:
            title = str(item.get("新闻标题", item.get("标题", item.get("title", "")))).strip()
            if not title:
                continue
            summary = str(item.get("新闻内容", item.get("摘要", title))).strip()[:400]
            publish_time = _normalize_datetime(
                item.get("发布时间", item.get("时间", item.get("_normalized_publish_date", as_of_date))),
                _normalize_date(item.get("_normalized_publish_date", as_of_date)),
            )
            classification = _infer_event_classification(title, "news", summary=summary)
            if classification["event_type"] == "other":
                skipped_news_other += 1
                continue
            publish_date = publish_time[:10]
            event_id = f"news:{publish_date}:{ticker}:{_stable_suffix(title)}"
            legacy = _build_legacy_event(
                event_id=event_id,
                title=title,
                event_type=classification["event_type"],
                event_subtype=classification["event_subtype"],
                ticker=ticker,
                stock_name=name_map.get(ticker, ticker),
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
            append_status = _append_or_replace(incremental, existing, existing_ids, existing_index, legacy)
            if append_status == "inserted":
                news_event_count += 1
            elif append_status == "updated":
                news_event_update_count += 1

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
        publish_time = f"{publish_date}T00:00:00"
        title = f"{row.get('company_name', ticker)} 舆情热度变化"
        label = str(row.get("sentiment_label", "neutral")).strip() or "neutral"
        summary = (
            f"平均情绪分 {avg_score:+.2f}，关注度 {attention_score:.2f}，"
            f"热度排名 {hot_rank or '-'}，标签 {label}"
        )
        event_id = f"sentiment:{publish_date}:{ticker}:{_stable_suffix(summary)}"
        legacy = _build_legacy_event(
            event_id=event_id,
            title=title,
            event_type="sentiment_reversal",
            event_subtype="sentiment_reversal.aggregate",
            ticker=ticker,
            stock_name=str(row.get("company_name", "")).strip() or name_map.get(ticker, ticker),
            event_date=publish_date,
            discover_time=publish_time,
            available_at=publish_time,
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
        append_status = _append_or_replace(incremental, existing, existing_ids, existing_index, legacy)
        if append_status == "inserted":
            sentiment_event_count += 1
        elif append_status == "updated":
            sentiment_event_update_count += 1

    payload = {
        "summary": {
            "ingest_date": as_of_date,
            "incremental_event_count": len(incremental),
            "total_event_count": len(existing),
            "created_event_count": filing_event_count + news_event_count + sentiment_event_count,
            "updated_event_count": filing_event_update_count + news_event_update_count + sentiment_event_update_count,
            "filing_event_count": filing_event_count,
            "filing_event_update_count": filing_event_update_count,
            "news_event_count": news_event_count,
            "news_event_update_count": news_event_update_count,
            "sentiment_event_count": sentiment_event_count,
            "sentiment_event_update_count": sentiment_event_update_count,
            "skipped_filing_other": skipped_filing_other,
            "skipped_news_other": skipped_news_other,
        },
        "rows": incremental,
        "event_master": existing,
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
