"""
News Collector
===============
Fetches company news, industry news, and market hot topics from akshare.
"""

import logging
import re
import time
from datetime import datetime
from typing import Any, Optional

import akshare as ak
import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------
_cache: dict[str, tuple[float, object]] = {}
DEFAULT_TTL = 180  # 3 minutes – news moves fast


def _get_cache(key: str, ttl: int = DEFAULT_TTL) -> Optional[object]:
    if key in _cache:
        ts, value = _cache[key]
        if time.time() - ts < ttl:
            return value
        del _cache[key]
    return None


def _set_cache(key: str, value: object) -> None:
    _cache[key] = (time.time(), value)


def _retry(fn, *args, retries: int = 2, delay: float = 0.5, timeout: float = 8.0, **kwargs):
    """Retry wrapper with per-attempt timeout.

    Parameters
    ----------
    timeout : float
        Max seconds to wait per attempt (enforced via signal on Unix).
    """
    import signal as _signal
    import platform

    use_alarm = platform.system() != "Windows"

    class _Timeout(Exception):
        pass

    def _handler(signum, frame):
        raise _Timeout(f"{fn.__name__} timed out after {timeout}s")

    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        old_handler = None
        try:
            if use_alarm and timeout > 0:
                old_handler = _signal.signal(_signal.SIGALRM, _handler)
                _signal.alarm(int(timeout))
            result = fn(*args, **kwargs)
            if use_alarm:
                _signal.alarm(0)
            return result
        except Exception as exc:
            if use_alarm:
                _signal.alarm(0)
            if old_handler is not None:
                _signal.signal(_signal.SIGALRM, old_handler)
            last_exc = exc
            logger.warning(
                "Attempt %d/%d for %s failed: %s",
                attempt, retries, fn.__name__, str(exc)[:80],
            )
            if attempt < retries:
                time.sleep(delay * attempt)
    raise last_exc  # type: ignore[misc]


def _df_to_dicts(df: pd.DataFrame, limit: int = 0) -> list[dict]:
    if df is None or df.empty:
        return []
    if limit > 0:
        df = df.head(limit)
    return df.to_dict(orient="records")


def _normalize_stock_code(value: Any) -> str:
    text = str(value or "").strip().upper()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 6:
        return digits[-6:]
    return ""


def _normalize_stock_name(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    return text.strip(" -_/")


def _parse_baidu_name_code(value: Any) -> tuple[str, str]:
    raw = str(value or "").strip()
    if not raw:
        return "", ""
    match = re.search(r"([A-Za-z]{2})?(\d{6})", raw)
    if match:
        code = match.group(2)
        name = (raw[: match.start()] + raw[match.end() :]).strip(" -_/()")
        return _normalize_stock_name(name or raw), code
    return _normalize_stock_name(raw), ""


def _xueqiu_headers() -> dict[str, str]:
    return {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Host": "xueqiu.com",
        "Pragma": "no-cache",
        "Referer": "https://xueqiu.com/hq",
        "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="100", "Google Chrome";v="100"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }


def _infer_hot_topic_meta(items: list[dict], *, source_mode: str) -> dict[str, Any]:
    live_sources: list[str] = []
    failed_sources: list[str] = []
    source_family = "none"
    for item in items:
        if source_family == "none":
            source_family = str(item.get("_hot_source_family", "")).strip() or source_family
        if not live_sources:
            live_sources = [str(source).strip() for source in item.get("_hot_live_sources", []) if str(source).strip()]
        if not failed_sources:
            failed_sources = [str(source).strip() for source in item.get("_hot_failed_sources", []) if str(source).strip()]
    source_family = source_family or "none"
    authoritative = source_family == "baidu_xueqiu" and bool(live_sources or source_mode == "cache")
    return {
        "source_family": source_family,
        "source_mode": source_mode,
        "source_label": f"{source_family}_{source_mode}" if source_family != "none" else "none",
        "live_sources": live_sources,
        "failed_sources": failed_sources,
        "authoritative": authoritative,
    }


class NewsCollector:
    """Collect A-share news and hot topics via akshare."""

    def __init__(self) -> None:
        self.last_hot_topic_meta: dict[str, Any] = {
            "source_family": "none",
            "source_mode": "none",
            "source_label": "none",
            "live_sources": [],
            "failed_sources": [],
            "authoritative": False,
        }

    def _collect_baidu_hot_topics(self, limit: int) -> list[dict]:
        df = _retry(
            ak.stock_hot_search_baidu,
            symbol="A股",
            date=datetime.now().strftime("%Y%m%d"),
            time="今日",
        )
        records: list[dict] = []
        for rank, item in enumerate(_df_to_dicts(df, limit=max(limit, 0)), start=1):
            name, code = _parse_baidu_name_code(item.get("名称/代码", ""))
            records.append(
                {
                    "股票代码": code,
                    "股票简称": name,
                    "source": "baidu_search",
                    "source_rank": rank,
                    "metric_value": float(item.get("综合热度", 0) or 0),
                    "related_info": f"百度热搜#{rank}",
                    "raw": item,
                }
            )
        return records

    def _collect_xueqiu_hot_topics(self, *, source: str, order_by: str, limit: int) -> list[dict]:
        url = "https://xueqiu.com/service/v5/stock/screener/screen"
        response = requests.get(
            url,
            params={
                "category": "CN",
                "size": str(max(limit, 1)),
                "order": "desc",
                "order_by": order_by,
                "only_count": "0",
                "page": "1",
            },
            headers=_xueqiu_headers(),
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("data", {}).get("list", []) or []
        records: list[dict] = []
        source_label = {
            "xueqiu_follow": "雪球关注",
            "xueqiu_tweet": "雪球讨论",
            "xueqiu_deal": "雪球交易",
        }.get(source, source)
        metric_field = {
            "xueqiu_follow": "follow",
            "xueqiu_tweet": "tweet",
            "xueqiu_deal": "deal",
        }.get(source, "")
        for rank, item in enumerate(rows[: max(limit, 0)], start=1):
            records.append(
                {
                    "股票代码": _normalize_stock_code(item.get("symbol", "")),
                    "股票简称": _normalize_stock_name(item.get("name", "")),
                    "source": source,
                    "source_rank": rank,
                    "metric_value": float(item.get(metric_field, 0) or 0),
                    "related_info": f"{source_label}#{rank}",
                    "raw": item,
                }
            )
        return records

    def _aggregate_hot_topic_records(
        self,
        records: list[dict],
        *,
        limit: int,
        live_sources: list[str],
        failed_sources: list[str],
    ) -> list[dict]:
        if not records:
            return []

        source_weights = {
            "baidu_search": 1.25,
            "xueqiu_follow": 1.0,
            "xueqiu_tweet": 0.95,
            "xueqiu_deal": 0.85,
        }
        source_metric_max: dict[str, float] = {}
        for record in records:
            source = str(record.get("source", "")).strip()
            metric = float(record.get("metric_value", 0.0) or 0.0)
            source_metric_max[source] = max(source_metric_max.get(source, 0.0), metric)

        grouped: dict[str, dict[str, Any]] = {}
        code_to_key: dict[str, str] = {}
        name_to_key: dict[str, str] = {}
        for record in records:
            code = _normalize_stock_code(record.get("股票代码", ""))
            name = _normalize_stock_name(record.get("股票简称", ""))
            name_key = re.sub(r"\s+", "", name)
            if code and code in code_to_key:
                key = code_to_key[code]
            elif name_key and name_key in name_to_key:
                key = name_to_key[name_key]
            else:
                key = code or f"name:{name_key or len(grouped)}"
            if key not in grouped:
                grouped[key] = {
                    "股票代码": code,
                    "股票简称": name,
                    "score": 0.0,
                    "sources": [],
                    "source_ranks": {},
                    "related_infos": [],
                    "raw_sources": {},
                }
            entry = grouped[key]
            if code and not entry["股票代码"]:
                entry["股票代码"] = code
            if name and not entry["股票简称"]:
                entry["股票简称"] = name
            if code:
                code_to_key[code] = key
            if name_key:
                name_to_key[name_key] = key

            source = str(record.get("source", "")).strip()
            rank = int(record.get("source_rank", 0) or 0)
            metric_value = float(record.get("metric_value", 0.0) or 0.0)
            rank_score = max(0.0, 1.0 - max(rank - 1, 0) / max(limit, 1))
            metric_score = metric_value / max(source_metric_max.get(source, 0.0), 1e-9)
            contribution = source_weights.get(source, 1.0) * (0.75 * rank_score + 0.25 * metric_score)
            entry["score"] += contribution
            entry["source_ranks"][source] = rank
            if source not in entry["sources"]:
                entry["sources"].append(source)
            info = str(record.get("related_info", "")).strip()
            if info:
                entry["related_infos"].append(info)
            entry["raw_sources"][source] = record.get("raw", {})

        aggregated = []
        for item in grouped.values():
            if not item["股票简称"] and not item["股票代码"]:
                continue
            aggregated.append(
                {
                    "股票代码": item["股票代码"],
                    "股票简称": item["股票简称"],
                    "综合热度": round(item["score"], 6),
                    "热度来源": ",".join(sorted(item["sources"])),
                    "相关信息": "; ".join(item["related_infos"][:6]),
                    "_hot_source": "baidu_xueqiu",
                    "_hot_source_family": "baidu_xueqiu",
                    "_hot_live_sources": live_sources,
                    "_hot_failed_sources": failed_sources,
                    "_source_ranks": item["source_ranks"],
                    "_source_count": len(item["sources"]),
                    "_raw_sources": item["raw_sources"],
                }
            )
        aggregated.sort(
            key=lambda row: (
                -float(row.get("综合热度", 0.0) or 0.0),
                -int(row.get("_source_count", 0) or 0),
                str(row.get("股票代码", "")),
                str(row.get("股票简称", "")),
            )
        )
        for rank, item in enumerate(aggregated[: max(limit, 0)], start=1):
            item["热度排名"] = rank
        return aggregated[: max(limit, 0)]

    # ------------------------------------------------------------------
    # Company news (个股新闻)
    # ------------------------------------------------------------------
    def get_company_news(self, code: str, limit: int = 20) -> list[dict]:
        """Return recent news articles for a single stock.

        Uses ``ak.stock_news_em`` (东方财富-个股新闻) which accepts a stock
        code and returns a DataFrame of news headlines, sources, and links.

        Parameters
        ----------
        code : str
            6-digit stock code, e.g. ``"000001"``.
        limit : int
            Maximum number of articles to return.

        Returns
        -------
        list[dict]
            Each dict may contain: 新闻标题, 新闻内容, 发布时间, 文章来源,
            新闻链接, etc.
        """
        cache_key = f"company_news_{code}"
        cached = _get_cache(cache_key)
        if cached is not None:
            return cached[:limit] if limit else cached  # type: ignore[return-value]

        try:
            df = _retry(ak.stock_news_em, symbol=code)
            result = _df_to_dicts(df)
            _set_cache(cache_key, result)
            return result[:limit]
        except Exception as exc:
            logger.error("get_company_news(%s) failed: %s", code, exc)
            return []

    # ------------------------------------------------------------------
    # Industry news (行业新闻)
    # ------------------------------------------------------------------
    def get_industry_news(self, industry: str, limit: int = 20) -> list[dict]:
        """Return recent news for an industry sector.

        This uses the industry board (板块) approach: first look up the
        constituent stocks of the industry board, then aggregate news from
        top constituents.  Alternatively it tries
        ``ak.stock_board_industry_info_em`` for board-level news.

        Parameters
        ----------
        industry : str
            Industry name in Chinese, e.g. ``"银行"``, ``"光伏"``.
        limit : int
            Maximum number of articles to return.

        Returns
        -------
        list[dict]
        """
        cache_key = f"industry_news_{industry}"
        cached = _get_cache(cache_key)
        if cached is not None:
            return cached[:limit] if limit else cached  # type: ignore[return-value]

        results: list[dict] = []

        # Strategy 1: Try to get news from the industry board directly
        try:
            # Get constituents of the industry board
            df_board = _retry(
                ak.stock_board_industry_cons_em,
                symbol=industry,
            )
            if df_board is not None and not df_board.empty:
                # Pick top 5 stocks by market cap / first 5 rows
                codes = df_board["代码"].head(5).tolist()
                for stock_code in codes:
                    try:
                        news_df = _retry(ak.stock_news_em, symbol=stock_code)
                        stock_news = _df_to_dicts(news_df, limit=5)
                        for item in stock_news:
                            item["_industry"] = industry
                            item["_stock_code"] = stock_code
                        results.extend(stock_news)
                    except Exception:
                        continue
        except Exception as exc:
            logger.warning("Industry board lookup for '%s' failed: %s", industry, exc)

        # Sort by time if available, then trim
        for item in results:
            # Normalise time field for sorting
            t = item.get("发布时间", item.get("时间", ""))
            item["_sort_time"] = str(t)
        results.sort(key=lambda x: x.get("_sort_time", ""), reverse=True)

        # Clean up internal keys
        for item in results:
            item.pop("_sort_time", None)

        results = results[:limit]
        _set_cache(cache_key, results)
        return results

    # ------------------------------------------------------------------
    # Market hot topics (市场热点)
    # ------------------------------------------------------------------
    def get_market_hot_topics(self, limit: int = 30) -> list[dict]:
        """Return current market hot topics / trending stocks.

        Uses authoritative market-attention sources:
        - 百度股市通热搜
        - 雪球关注排行
        - 雪球讨论排行
        - 雪球交易分享排行

        Parameters
        ----------
        limit : int
            Maximum number of entries.

        Returns
        -------
        list[dict]
            Each dict includes stock name / concept name and popularity info.
        """
        cache_key = "market_hot_topics"
        cached = _get_cache(cache_key, ttl=120)
        if cached is not None:
            self.last_hot_topic_meta = _infer_hot_topic_meta(cached, source_mode="cache")  # type: ignore[arg-type]
            return cached[:limit] if limit else cached  # type: ignore[return-value]

        normalized_limit = max(limit, 0)
        fetch_limit = min(max(normalized_limit, 120), 200)
        records: list[dict] = []
        live_sources: list[str] = []
        failed_sources: list[str] = []

        source_jobs = [
            ("baidu_search", self._collect_baidu_hot_topics, {"limit": fetch_limit}),
            ("xueqiu_follow", self._collect_xueqiu_hot_topics, {"source": "xueqiu_follow", "order_by": "follow", "limit": fetch_limit}),
            ("xueqiu_tweet", self._collect_xueqiu_hot_topics, {"source": "xueqiu_tweet", "order_by": "tweet", "limit": fetch_limit}),
            ("xueqiu_deal", self._collect_xueqiu_hot_topics, {"source": "xueqiu_deal", "order_by": "deal", "limit": fetch_limit}),
        ]

        for source_name, fn, kwargs in source_jobs:
            try:
                source_records = fn(**kwargs)
                if source_records:
                    records.extend(source_records)
                    live_sources.append(source_name)
                else:
                    failed_sources.append(source_name)
            except Exception as exc:
                failed_sources.append(source_name)
                logger.warning("%s hot-topic source failed: %s", source_name, exc)

        aggregated = self._aggregate_hot_topic_records(
            records,
            limit=normalized_limit,
            live_sources=live_sources,
            failed_sources=failed_sources,
        )
        self.last_hot_topic_meta = {
            "source_family": "baidu_xueqiu" if aggregated else "none",
            "source_mode": "live" if aggregated else "none",
            "source_label": "baidu_xueqiu_live" if aggregated else "none",
            "live_sources": live_sources,
            "failed_sources": failed_sources,
            "authoritative": bool(aggregated and live_sources),
        }
        if aggregated:
            _set_cache(cache_key, aggregated)
            return aggregated
        logger.error("All authoritative hot-topic sources failed")
        return []
