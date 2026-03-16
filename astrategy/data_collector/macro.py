"""
Macro Data Collector
=====================
Fetches Chinese macroeconomic indicators from akshare: PMI, CPI, GDP,
monetary supply (M2 / social financing), and LPR interest rates.
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
DEFAULT_TTL = 3600  # 1 hour – macro data updates infrequently


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


class MacroCollector:
    """Collect Chinese macroeconomic data via akshare."""

    # ------------------------------------------------------------------
    # PMI (采购经理指数)
    # ------------------------------------------------------------------
    def get_pmi(self, months: int = 12) -> pd.DataFrame:
        """Return official manufacturing PMI data.

        Uses ``ak.macro_china_pmi_yearly`` which returns the official
        NBS manufacturing PMI time series from Jin-10 data centre.

        Parameters
        ----------
        months : int
            Number of most recent months to return.

        Returns
        -------
        pd.DataFrame
            Columns typically: 日期/月份, 制造业PMI, etc.
        """
        cache_key = "pmi"
        cached = _get_cache(cache_key)
        if cached is not None:
            df = cached  # type: ignore[assignment]
            return df.tail(months).reset_index(drop=True) if not df.empty else df

        try:
            df = _retry(ak.macro_china_pmi_yearly)
            _set_cache(cache_key, df)
            if not df.empty:
                return df.tail(months).reset_index(drop=True)
            return df
        except Exception as exc:
            logger.error("get_pmi failed: %s", exc)
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # CPI (居民消费价格指数)
    # ------------------------------------------------------------------
    def get_cpi(self, months: int = 12) -> pd.DataFrame:
        """Return Chinese CPI data.

        Uses ``ak.macro_china_cpi_yearly`` which returns CPI year-over-year
        data from the NBS.

        Parameters
        ----------
        months : int
            Number of most recent months to return.

        Returns
        -------
        pd.DataFrame
            Columns: 日期, 全国-当月, 全国-同比增长, etc.
        """
        cache_key = "cpi"
        cached = _get_cache(cache_key)
        if cached is not None:
            df = cached  # type: ignore[assignment]
            return df.tail(months).reset_index(drop=True) if not df.empty else df

        try:
            df = _retry(ak.macro_china_cpi_yearly)
            _set_cache(cache_key, df)
            if not df.empty:
                return df.tail(months).reset_index(drop=True)
            return df
        except Exception as exc:
            logger.error("get_cpi failed: %s", exc)
            # Fallback: try monthly variant
            try:
                df = _retry(ak.macro_china_cpi_monthly)
                _set_cache(cache_key, df)
                return df.tail(months).reset_index(drop=True)
            except Exception as exc2:
                logger.error("get_cpi fallback also failed: %s", exc2)
                return pd.DataFrame()

    # ------------------------------------------------------------------
    # Monetary data – M2 / Social Financing (M2/社融)
    # ------------------------------------------------------------------
    def get_monetary_data(self) -> pd.DataFrame:
        """Return M2 money supply data.

        Uses ``ak.macro_china_m2_yearly`` which returns M2 year-over-year
        growth rate from the PBOC.

        Returns
        -------
        pd.DataFrame
            Columns: 日期/月份, M2数量(亿元), M2同比增长(%), etc.
        """
        cache_key = "monetary"
        cached = _get_cache(cache_key)
        if cached is not None:
            return cached  # type: ignore[return-value]

        try:
            df = _retry(ak.macro_china_m2_yearly)
            _set_cache(cache_key, df)
            return df
        except Exception as exc:
            logger.error("get_monetary_data (M2) failed: %s", exc)
            # Fallback: social financing data
            try:
                df = _retry(ak.macro_china_shrzgm)
                _set_cache(cache_key, df)
                return df
            except Exception as exc2:
                logger.error("get_monetary_data fallback also failed: %s", exc2)
                return pd.DataFrame()

    # ------------------------------------------------------------------
    # GDP (国内生产总值)
    # ------------------------------------------------------------------
    def get_gdp(self) -> pd.DataFrame:
        """Return Chinese GDP data.

        Uses ``ak.macro_china_gdp_yearly`` which returns quarterly GDP data
        from the NBS.

        Returns
        -------
        pd.DataFrame
            Columns: 季度, 国内生产总值-绝对值(亿元),
            国内生产总值-同比增长(%), etc.
        """
        cache_key = "gdp"
        cached = _get_cache(cache_key)
        if cached is not None:
            return cached  # type: ignore[return-value]

        try:
            df = _retry(ak.macro_china_gdp_yearly)
            _set_cache(cache_key, df)
            return df
        except Exception as exc:
            logger.error("get_gdp failed: %s", exc)
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # LPR (贷款市场报价利率)
    # ------------------------------------------------------------------
    def get_lpr_history(self) -> pd.DataFrame:
        """Return Loan Prime Rate (LPR) history.

        Uses ``ak.macro_china_lpr`` which returns the LPR time series
        including 1-year and 5-year rates.

        Returns
        -------
        pd.DataFrame
            Columns: TRADE_DATE, LPR1Y, LPR5Y, RATE_1, RATE_2, etc.
        """
        cache_key = "lpr"
        cached = _get_cache(cache_key)
        if cached is not None:
            return cached  # type: ignore[return-value]

        try:
            df = _retry(ak.macro_china_lpr)
            _set_cache(cache_key, df)
            return df
        except Exception as exc:
            logger.error("get_lpr_history failed: %s", exc)
            return pd.DataFrame()
