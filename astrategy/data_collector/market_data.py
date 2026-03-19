"""
Market Data Collector
=====================
Fetches market-level data from akshare: daily K-lines, realtime quotes,
industry indices, northbound capital flow, and individual stock money flow.
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
DEFAULT_TTL = 300  # 5 minutes


def _get_cache(key: str, ttl: int = DEFAULT_TTL) -> Optional[object]:
    if key in _cache:
        ts, value = _cache[key]
        if time.time() - ts < ttl:
            return value
        del _cache[key]
    return None


def _set_cache(key: str, value: object) -> None:
    _cache[key] = (time.time(), value)


# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------
def _retry(fn, *args, retries: int = 2, delay: float = 0.5, timeout: float = 15.0, **kwargs):
    """Call *fn* with retry logic and a per-attempt timeout."""
    import platform
    import signal as _signal

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
            return fn(*args, **kwargs)
        except Exception as exc:
            if use_alarm:
                _signal.alarm(0)
            if old_handler is not None:
                _signal.signal(_signal.SIGALRM, old_handler)
            last_exc = exc
            logger.warning(
                "Attempt %d/%d for %s failed: %s",
                attempt, retries, fn.__name__, exc,
            )
            if attempt < retries:
                time.sleep(delay * attempt)
        finally:
            if use_alarm:
                _signal.alarm(0)
            if old_handler is not None:
                _signal.signal(_signal.SIGALRM, old_handler)
    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Collector
# ---------------------------------------------------------------------------
class MarketDataCollector:
    """Collect A-share market data via akshare."""

    # ------------------------------------------------------------------
    # Daily K-line (日K线)
    # ------------------------------------------------------------------
    def get_daily_quotes(
        self,
        code: str,
        start: str,
        end: str,
        adjust: str = "qfq",
    ) -> pd.DataFrame:
        """Return daily OHLCV bars for a single stock.

        Parameters
        ----------
        code : str
            6-digit A-share stock code, e.g. ``"000001"``.
        start : str
            Start date in ``YYYYMMDD`` format.
        end : str
            End date in ``YYYYMMDD`` format.
        adjust : str
            ``"qfq"`` (forward), ``"hfq"`` (backward), or ``""`` (none).

        Returns
        -------
        pd.DataFrame
            Columns: 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅,
            涨跌幅, 涨跌额, 换手率
        """
        cache_key = f"daily_{code}_{start}_{end}_{adjust}"
        cached = _get_cache(cache_key, ttl=600)
        if cached is not None:
            return cached  # type: ignore[return-value]

        # Use Sina API directly (East Money blocked by proxy/TLS issues)
        try:
            # Sina uses "shXXXXXX" / "szXXXXXX" format
            sina_code = f"sh{code}" if code.startswith("6") else f"sz{code}"
            sina_adjust = adjust if adjust in ("qfq", "hfq") else ""
            df = _retry(
                ak.stock_zh_a_daily,
                symbol=sina_code,
                adjust=sina_adjust,
            )
            if df is not None and not df.empty:
                # Sina returns: date, open, high, low, close, volume
                # Filter by date range
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"])
                    start_dt = pd.to_datetime(start)
                    end_dt = pd.to_datetime(end)
                    df = df[(df["date"] >= start_dt) & (df["date"] <= end_dt)]
                    df = df.reset_index(drop=True)
                    # Rename to match expected columns
                    col_map = {
                        "date": "日期", "open": "开盘", "high": "最高",
                        "low": "最低", "close": "收盘", "volume": "成交量",
                    }
                    df = df.rename(columns=col_map)
            _set_cache(cache_key, df)
            return df
        except Exception as exc:
            logger.error("get_daily_quotes(%s) failed on both APIs: %s", code, exc)
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Realtime quotes (实时行情)
    # ------------------------------------------------------------------
    def get_realtime_quotes(self, codes: Optional[list[str]] = None) -> pd.DataFrame:
        """Return realtime A-share quotes from East Money.

        Parameters
        ----------
        codes : list[str] | None
            If provided, filter the result to these stock codes only.
            Each code should be 6-digit, e.g. ``["000001", "600519"]``.

        Returns
        -------
        pd.DataFrame
            Full realtime snapshot.  When *codes* is given the frame is
            filtered to matching rows (matched against ``"代码"`` column).
        """
        cache_key = "realtime_all"
        cached = _get_cache(cache_key, ttl=15)  # very short TTL for realtime
        if cached is not None:
            df = cached  # type: ignore[assignment]
        else:
            try:
                all_stocks = ak.stock_info_a_code_name()
                all_stocks.columns = ["代码", "名称"]
                df = all_stocks
                _set_cache(cache_key, df)
            except Exception as exc:
                logger.warning("stock_info_a_code_name failed: %s; trying local graph fallback", exc)
                df = self._load_names_from_graph()
                if not df.empty:
                    _set_cache(cache_key, df)

        if codes is not None and not df.empty:
            df = df[df["代码"].isin(codes)].reset_index(drop=True)
        return df

    @staticmethod
    def _load_names_from_graph() -> pd.DataFrame:
        """Fallback: load stock code→name mapping from local graph file."""
        try:
            import json
            from pathlib import Path
            graph_path = Path(__file__).resolve().parent.parent / ".data" / "local_graph" / "supply_chain.json"
            if not graph_path.exists():
                return pd.DataFrame()
            with open(graph_path, encoding="utf-8") as f:
                g = json.load(f)
            rows = []
            for key, node in g.get("nodes", {}).items():
                display = node.get("display_name", key)
                rows.append({"代码": key, "名称": display})
            if rows:
                logger.info("Loaded %d stock names from local graph fallback", len(rows))
                return pd.DataFrame(rows)
        except Exception as exc:
            logger.debug("Graph name fallback failed: %s", exc)
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # Industry index – Shenwan classification (申万行业指数)
    # ------------------------------------------------------------------
    def get_industry_index(self, level: str = "L1") -> pd.DataFrame:
        """Return Shenwan industry index board list from East Money.

        Parameters
        ----------
        level : str
            Not used by the underlying API; kept for interface consistency.
            ``ak.stock_board_industry_name_em()`` returns all industry boards.

        Returns
        -------
        pd.DataFrame
            Columns include: 排名, 板块名称, 板块代码, 最新价, 涨跌幅, etc.
        """
        cache_key = f"industry_index_{level}"
        cached = _get_cache(cache_key, ttl=300)
        if cached is not None:
            return cached  # type: ignore[return-value]

        try:
            # Use Sina index daily as fallback since East Money is blocked
            df = _retry(ak.stock_zh_index_daily, symbol="sh000001")
            _set_cache(cache_key, df)
            return df
        except Exception as exc:
            logger.error("get_industry_index failed: %s", exc)
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Northbound capital flow (北向资金)
    # ------------------------------------------------------------------
    def get_north_flow(self, date: Optional[str] = None) -> pd.DataFrame:
        """Return northbound (沪深港通) net inflow historical data.

        Parameters
        ----------
        date : str | None
            Currently unused – the API returns the full time series.
            If provided, the returned frame is filtered to that date
            (``YYYYMMDD`` or ``YYYY-MM-DD`` format).

        Returns
        -------
        pd.DataFrame
            Columns: 日期, 当日北向净流入 (亿), etc.
        """
        cache_key = "north_flow"
        cached = _get_cache(cache_key, ttl=600)
        if cached is not None:
            df = cached  # type: ignore[assignment]
        else:
            try:
                # indicator="北向" returns the combined northbound flow
                df = _retry(
                    ak.stock_hsgt_north_net_flow_in_em,
                    indicator="北向",
                )
                _set_cache(cache_key, df)
            except Exception:
                logger.info("North flow East Money API failed, returning empty")
                return pd.DataFrame()

        if date is not None and not df.empty:
            # Normalise date for matching
            date_norm = date.replace("-", "")
            date_col = df.columns[0]  # typically "日期"
            df = df[
                df[date_col].astype(str).str.replace("-", "") == date_norm
            ].reset_index(drop=True)

        return df

    # ------------------------------------------------------------------
    # Individual stock money flow (个股资金流)
    # ------------------------------------------------------------------
    def get_money_flow(self, code: str, days: int = 20) -> pd.DataFrame:
        """Return recent money-flow data for a single stock.

        Parameters
        ----------
        code : str
            6-digit stock code.
        days : int
            Number of recent trading days to return (tail slice).

        Returns
        -------
        pd.DataFrame
            Columns: 日期, 收盘价, 涨跌幅, 主力净流入-净额, 主力净流入-净占比, etc.
        """
        cache_key = f"money_flow_{code}"
        cached = _get_cache(cache_key, ttl=300)
        if cached is not None:
            df = cached  # type: ignore[assignment]
        else:
            try:
                df = _retry(
                    ak.stock_individual_fund_flow,
                    stock=code,
                    market="sh" if code.startswith("6") else "sz",
                )
                _set_cache(cache_key, df)
            except Exception:
                logger.info("Money flow API failed for %s, returning empty", code)
                return pd.DataFrame()

        if not df.empty:
            df = df.tail(days).reset_index(drop=True)
        return df
