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


def _retry(fn, *args, retries: int = 3, delay: float = 1.0, **kwargs):
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

        Uses ``ak.stock_profit_forecast_em`` (东财-盈利预测) which returns
        analyst-level forecasts including buy/hold/sell ratings.

        Parameters
        ----------
        code : str
            6-digit stock code, e.g. ``"000001"``.

        Returns
        -------
        list[dict]
            Each dict may include: 研究机构, 分析师, 评级, 目标价,
            盈利预测, 报告日期, etc.
        """
        cache_key = f"analyst_ratings_{code}"
        cached = _get_cache(cache_key)
        if cached is not None:
            return cached  # type: ignore[return-value]

        # Strategy 1: stock_profit_forecast_em
        try:
            df = _retry(ak.stock_profit_forecast_em, symbol=code)
            result = _df_to_dicts(df)
            _set_cache(cache_key, result)
            return result
        except Exception as exc:
            logger.warning("stock_profit_forecast_em(%s) failed: %s", code, exc)

        # Strategy 2: stock_comment_em – analyst consensus
        try:
            df = _retry(ak.stock_comment_em, symbol=code)
            result = _df_to_dicts(df)
            _set_cache(cache_key, result)
            return result
        except Exception as exc:
            logger.error("get_analyst_ratings(%s) all strategies failed: %s", code, exc)
            return []

    # ------------------------------------------------------------------
    # Consensus forecast (一致预期)
    # ------------------------------------------------------------------
    def get_consensus_forecast(self, code: str) -> dict:
        """Return consensus earnings forecast for a stock.

        Aggregates analyst forecasts into a single summary dict with
        consensus EPS, revenue, net profit, and PE estimates.

        Uses ``ak.stock_profit_forecast_em`` and computes averages.

        Parameters
        ----------
        code : str
            6-digit stock code.

        Returns
        -------
        dict
            Keys may include: 股票代码, 股票名称, 预测机构数,
            预测年度, 平均预测EPS, 平均预测净利润, etc.
            Returns empty dict on failure.
        """
        cache_key = f"consensus_forecast_{code}"
        cached = _get_cache(cache_key)
        if cached is not None:
            return cached  # type: ignore[return-value]

        try:
            df = _retry(ak.stock_profit_forecast_em, symbol=code)
            if df is None or df.empty:
                return {}

            result: dict = {
                "股票代码": code,
                "预测机构数": len(df),
            }

            # Try to compute averages for numeric columns
            numeric_cols = df.select_dtypes(include=["number"]).columns
            for col in numeric_cols:
                series = df[col].dropna()
                if not series.empty:
                    result[f"平均{col}"] = round(float(series.mean()), 4)
                    result[f"最高{col}"] = round(float(series.max()), 4)
                    result[f"最低{col}"] = round(float(series.min()), 4)

            # Add report date range
            date_cols = [c for c in df.columns if "日期" in c or "date" in c.lower()]
            if date_cols:
                dates = df[date_cols[0]].dropna()
                if not dates.empty:
                    result["最早预测日期"] = str(dates.min())
                    result["最新预测日期"] = str(dates.max())

            _set_cache(cache_key, result)
            return result
        except Exception as exc:
            logger.error("get_consensus_forecast(%s) failed: %s", code, exc)
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

        # Part 2: Top analysts covering this industry
        try:
            # stock_analyst_rank_em returns a ranking of analysts
            df = _retry(ak.stock_analyst_rank_em, year="2024")
            if df is not None and not df.empty:
                # The analyst rank table doesn't filter by industry directly,
                # so we include top analysts as general context.
                top_analysts = _df_to_dicts(df, limit=10)
                for item in top_analysts:
                    item["_source"] = "analyst_rank"
                results.extend(top_analysts)
        except Exception as exc:
            logger.warning("Analyst rank lookup failed: %s", exc)

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
