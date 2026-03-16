"""
News Collector
===============
Fetches company news, industry news, and market hot topics from akshare.
"""

import logging
import time
from typing import Optional

import akshare as ak
import pandas as pd

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


def _retry(fn, *args, retries: int = 2, delay: float = 0.5, **kwargs):
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "Attempt %d/%d for %s failed: %s",
                attempt, retries, fn.__name__, exc,
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


class NewsCollector:
    """Collect A-share news and hot topics via akshare."""

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

        Combines data from:
        - ``ak.stock_hot_rank_em()`` – East Money popularity ranking
        - Falls back to ``ak.stock_board_concept_name_em()`` for concept
          boards if the hot-rank API is unavailable.

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
            return cached[:limit] if limit else cached  # type: ignore[return-value]

        # Strategy 1: hot rank
        try:
            df = _retry(ak.stock_hot_rank_em)
            result = _df_to_dicts(df, limit=limit)
            _set_cache(cache_key, result)
            return result
        except Exception as exc:
            logger.warning("stock_hot_rank_em failed: %s", exc)

        # Strategy 2: concept boards (trending sectors)
        try:
            df = _retry(ak.stock_board_concept_name_em)
            if not df.empty:
                # Sort by 涨跌幅 descending to get hot concepts
                if "涨跌幅" in df.columns:
                    df = df.sort_values("涨跌幅", ascending=False)
                result = _df_to_dicts(df, limit=limit)
                _set_cache(cache_key, result)
                return result
        except Exception as exc:
            logger.warning("stock_board_concept_name_em failed: %s", exc)

        # Strategy 3: stock_hot_keyword_em
        try:
            df = _retry(ak.stock_hot_keyword_em, symbol="SZ000001")
            result = _df_to_dicts(df, limit=limit)
            _set_cache(cache_key, result)
            return result
        except Exception as exc:
            logger.error("All hot topic strategies failed: %s", exc)
            return []
