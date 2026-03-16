"""
Fundamental Data Collector
===========================
Fetches company-level fundamental data from akshare: financial summaries,
top-10 holders, fund holdings, industry PE, and basic stock info.
"""

import logging
import time
from typing import Optional

import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Simple in-memory TTL cache
# ---------------------------------------------------------------------------
_cache: dict[str, tuple[float, object]] = {}
DEFAULT_TTL = 600  # 10 minutes – fundamental data is slow-moving


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


# ---------------------------------------------------------------------------
# Cached stock code -> name lookup (shared across methods)
# ---------------------------------------------------------------------------
_stock_name_map: dict[str, str] = {}


def _ensure_stock_name_map() -> dict[str, str]:
    """Load stock code -> name map from Sina API (cached)."""
    global _stock_name_map
    if _stock_name_map:
        return _stock_name_map
    cached = _get_cache("stock_name_map", ttl=3600)
    if cached is not None:
        _stock_name_map = cached  # type: ignore[assignment]
        return _stock_name_map
    try:
        df = ak.stock_info_a_code_name()
        if df is not None and not df.empty:
            code_col, name_col = df.columns[0], df.columns[1]
            _stock_name_map = dict(zip(
                df[code_col].astype(str), df[name_col].astype(str)
            ))
            _set_cache("stock_name_map", _stock_name_map)
    except Exception as exc:
        logger.warning("Failed to load stock name map: %s", exc)
    return _stock_name_map


class FundamentalCollector:
    """Collect A-share fundamental data via akshare."""

    # ------------------------------------------------------------------
    # Financial summary (财务摘要)
    # ------------------------------------------------------------------
    def get_financial_summary(self, code: str) -> pd.DataFrame:
        """Return financial summary data (财务摘要) for a stock.

        Uses Sina ``stock_financial_report_sina`` (利润表) as the primary
        source.  Falls back to an empty DataFrame if unavailable.

        Parameters
        ----------
        code : str
            6-digit A-share stock code, e.g. ``"000001"``.

        Returns
        -------
        pd.DataFrame
            Multi-row frame with one row per reporting period.
        """
        cache_key = f"financial_summary_{code}"
        cached = _get_cache(cache_key, ttl=3600)
        if cached is not None:
            return cached  # type: ignore[return-value]

        try:
            df = _retry(
                ak.stock_financial_report_sina,
                stock=code,
                symbol="利润表",
            )
            _set_cache(cache_key, df)
            return df
        except Exception as exc:
            logger.error("get_financial_summary(%s) failed: %s", code, exc)
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Top-10 shareholders (十大股东)
    # ------------------------------------------------------------------
    def get_top10_holders(self, code: str) -> pd.DataFrame:
        """Return top-10 shareholders for the latest reporting period.

        Uses Sina ``stock_main_stock_holder`` which returns all historical
        top-10 shareholder records.  We take the most recent 10 rows
        (i.e. the latest reporting period).

        Parameters
        ----------
        code : str
            6-digit stock code.

        Returns
        -------
        pd.DataFrame
            Columns: 编号, 股东名称, 持股数量, 持股比例, 股本性质,
            截至日期, 公告日期, ...
        """
        cache_key = f"top10_holders_{code}"
        cached = _get_cache(cache_key, ttl=3600)
        if cached is not None:
            return cached  # type: ignore[return-value]

        try:
            df = _retry(ak.stock_main_stock_holder, stock=code)
            if df is not None and not df.empty:
                # Get latest reporting period only (first 10 rows typically)
                if "截至日期" in df.columns:
                    latest_date = df["截至日期"].iloc[0]
                    df = df[df["截至日期"] == latest_date].reset_index(drop=True)
                else:
                    df = df.head(10)
            _set_cache(cache_key, df)
            return df
        except Exception as exc:
            logger.error("get_top10_holders(%s) failed: %s", code, exc)
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Fund holdings (基金持仓)
    # ------------------------------------------------------------------
    def get_fund_holdings(self, code: str) -> pd.DataFrame:
        """Return mutual fund holdings for a stock.

        Uses ``ak.stock_fund_stock_holder`` which returns fund holding
        details from Sina/THS.

        Parameters
        ----------
        code : str
            6-digit stock code.

        Returns
        -------
        pd.DataFrame
            Columns: 基金名称, 基金代码, 持仓数量, 占流通股比例, 持股市值,
            占净值比例, 截止日期
        """
        cache_key = f"fund_holdings_{code}"
        cached = _get_cache(cache_key, ttl=3600)
        if cached is not None:
            return cached  # type: ignore[return-value]

        try:
            df = _retry(ak.stock_fund_stock_holder, symbol=code)
            _set_cache(cache_key, df)
            return df
        except Exception as exc:
            logger.error("get_fund_holdings(%s) failed: %s", code, exc)
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Industry PE (行业PE)
    # ------------------------------------------------------------------
    def get_industry_pe(self, industry: str = "") -> pd.DataFrame:
        """Return industry-level PE/PB valuation data.

        Attempts ``ak.stock_board_industry_name_em``; returns an empty
        DataFrame if the East Money API is blocked (common behind proxies).

        Parameters
        ----------
        industry : str
            Industry name in Chinese, e.g. ``"银行"``.  If empty, return all.

        Returns
        -------
        pd.DataFrame
        """
        cache_key = f"industry_pe_{industry}"
        cached = _get_cache(cache_key, ttl=600)
        if cached is not None:
            return cached  # type: ignore[return-value]

        try:
            df = _retry(ak.stock_board_industry_name_em)
            if industry and not df.empty:
                mask = df["板块名称"].str.contains(industry, na=False)
                df = df[mask].reset_index(drop=True)
            _set_cache(cache_key, df)
            return df
        except Exception as exc:
            logger.info("get_industry_pe(%s) unavailable (East Money blocked): %s", industry, exc)
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Basic stock info (个股基本信息)
    # ------------------------------------------------------------------
    def get_stock_info(self, code: str) -> dict:
        """Return basic information for a stock as a dict.

        Uses the Sina-based ``stock_info_a_code_name`` lookup table to
        resolve stock code to name.  Returns a minimal dict since the
        full East Money individual-info API is blocked.

        Parameters
        ----------
        code : str
            6-digit stock code.

        Returns
        -------
        dict
            Keys: 股票代码, 股票简称.
        """
        cache_key = f"stock_info_{code}"
        cached = _get_cache(cache_key, ttl=3600)
        if cached is not None:
            return cached  # type: ignore[return-value]

        name_map = _ensure_stock_name_map()
        name = name_map.get(code, code)
        info = {"股票代码": code, "股票简称": name}
        _set_cache(cache_key, info)
        return info
