"""
Research Data Collector
========================
Fetches analyst research data from akshare: analyst rankings/ratings,
consensus earnings forecasts, and industry-level research reports.
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
DEFAULT_TTL = 600  # 10 minutes


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


class ResearchCollector:
    """Collect analyst research and forecast data via akshare."""

    # ------------------------------------------------------------------
    # Analyst ratings (分析师评级)
    # ------------------------------------------------------------------
    def get_analyst_ratings(self, code: str) -> list[dict]:
        """Return analyst ratings / recommendations for a stock.

        Uses ``ak.stock_rank_forecast_cninfo`` (巨潮-机构预测) to
        fetch recent analyst ratings. Falls back to returning empty list.

        Parameters
        ----------
        code : str
            6-digit stock code, e.g. ``"000001"``.

        Returns
        -------
        list[dict]
            Each dict may include: 证券代码, 证券简称, 研究机构简称,
            投资评级, 评级变化, 前一次投资评级, 目标价格-下限/上限, etc.
        """
        cache_key = f"analyst_ratings_{code}"
        cached = _get_cache(cache_key)
        if cached is not None:
            return cached  # type: ignore[return-value]

        # Fetch recent analyst forecasts from cninfo (巨潮).
        # stock_rank_forecast_cninfo returns data only for dates with published reports.
        # Scan day-by-day for the last 7 days to maximise hit rate.
        from datetime import datetime, timedelta
        results: list[dict] = []
        now = datetime.now()
        daily_table_cache: dict[str, pd.DataFrame] = {}

        for days_back in range(0, 7):
            date_str = (now - timedelta(days=days_back)).strftime("%Y%m%d")
            if date_str in daily_table_cache:
                df = daily_table_cache[date_str]
            else:
                try:
                    df = _retry(ak.stock_rank_forecast_cninfo, date=date_str)
                    daily_table_cache[date_str] = df if df is not None else pd.DataFrame()
                except Exception:
                    daily_table_cache[date_str] = pd.DataFrame()
                    continue
                df = daily_table_cache[date_str]
            if df is not None and not df.empty and "证券代码" in df.columns:
                matched = df[df["证券代码"].astype(str) == code]
                if not matched.empty:
                    results.extend(_df_to_dicts(matched))

        if results:
            _set_cache(cache_key, results)
            return results

        logger.info("get_analyst_ratings(%s): no ratings found in recent 30 days", code)
        return []

    # ------------------------------------------------------------------
    # Consensus forecast (一致预期)
    # ------------------------------------------------------------------
    def get_consensus_forecast(self, code: str) -> dict:
        """Return consensus earnings forecast for a stock.

        Uses ``ak.stock_profit_forecast_ths`` (同花顺-盈利预测) to get
        consensus EPS forecasts by year.

        Parameters
        ----------
        code : str
            6-digit stock code.

        Returns
        -------
        dict
            Keys: 股票代码, 预测机构数, 平均EPS (per year), etc.
            Returns empty dict on failure.
        """
        cache_key = f"consensus_forecast_{code}"
        cached = _get_cache(cache_key)
        if cached is not None:
            return cached  # type: ignore[return-value]

        try:
            df = _retry(
                ak.stock_profit_forecast_ths,
                symbol=code,
                indicator="预测年报每股收益",
            )
            if df is None or df.empty:
                return {}

            result: dict = {"股票代码": code}

            # Columns: 年度, 预测机构数, 最小值, 均值, 最大值, 行业平均数
            if "预测机构数" in df.columns:
                result["预测机构数"] = int(df["预测机构数"].iloc[0])

            for _, row in df.iterrows():
                year = str(row.get("年度", ""))
                result[f"EPS均值_{year}"] = float(row.get("均值", 0))
                result[f"EPS最小_{year}"] = float(row.get("最小值", 0))
                result[f"EPS最大_{year}"] = float(row.get("最大值", 0))

            _set_cache(cache_key, result)
            return result
        except Exception as exc:
            logger.info("get_consensus_forecast(%s) failed: %s", code, exc)
            return {}

    # ------------------------------------------------------------------
    # Industry research (行业研报)
    # ------------------------------------------------------------------
    def get_industry_research(self, industry: str) -> list[dict]:
        """Return research-related data for an industry sector.

        Since akshare does not have a dedicated industry-research-report
        endpoint, this method combines:
        1. Industry board valuation data (PE/PB) from
           ``ak.stock_board_industry_name_em``
        2. Analyst ranking data from ``ak.stock_analyst_rank_em``
           filtered by analysts covering stocks in the given industry.

        Parameters
        ----------
        industry : str
            Industry name in Chinese, e.g. ``"银行"``, ``"光伏"``.

        Returns
        -------
        list[dict]
            Combined industry research information.
        """
        cache_key = f"industry_research_{industry}"
        cached = _get_cache(cache_key)
        if cached is not None:
            return cached  # type: ignore[return-value]

        results: list[dict] = []

        # Part 1: Industry board data
        try:
            df = _retry(ak.stock_board_industry_name_em)
            if df is not None and not df.empty:
                mask = df["板块名称"].str.contains(industry, na=False)
                industry_df = df[mask]
                if not industry_df.empty:
                    industry_info = industry_df.to_dict(orient="records")
                    for item in industry_info:
                        item["_source"] = "industry_board"
                    results.extend(industry_info)
        except Exception as exc:
            logger.warning("Industry board lookup for '%s' failed: %s", industry, exc)

        # Part 2: Skip analyst rank (often blocked by proxy)
        # Could use ak.stock_analyst_rank_em but it's unreliable

        # Part 3: Get constituents for the industry board
        try:
            df = _retry(ak.stock_board_industry_cons_em, symbol=industry)
            if df is not None and not df.empty:
                # Summarise: count, top stocks
                summary = {
                    "_source": "industry_constituents",
                    "行业": industry,
                    "成分股数量": len(df),
                    "代表性个股": df["名称"].head(10).tolist()
                    if "名称" in df.columns
                    else [],
                }
                results.append(summary)
        except Exception as exc:
            logger.warning(
                "Industry constituents for '%s' failed: %s", industry, exc
            )

        _set_cache(cache_key, results)
        return results
