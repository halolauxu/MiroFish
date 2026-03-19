"""
Announcement Collector
=======================
Fetches A-share company announcements (公告) from akshare and provides
keyword-based importance filtering to separate material disclosures from
routine filings.
"""

import contextlib
import io
import logging
import time
from datetime import datetime
from typing import Optional

import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------
_cache: dict[str, tuple[float, object]] = {}
DEFAULT_TTL = 300


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
# Importance keyword lists
# ---------------------------------------------------------------------------
# Keywords that indicate a potentially material announcement
IMPORTANT_KEYWORDS: list[str] = [
    "业绩预告", "业绩快报", "业绩预增", "业绩预减", "业绩预亏",
    "重大合同", "中标", "涨价", "提价", "调价",
    "收购", "并购", "重组", "资产注入", "借壳",
    "增持", "回购", "股权激励", "员工持股",
    "减持", "解禁", "限售股",
    "停牌", "复牌",
    "分红", "送股", "转增", "配股",
    "立案", "处罚", "违规", "风险提示",
    "战略合作", "签署协议", "框架协议",
    "定增", "非公开发行", "可转债",
    "高送转", "特别分红",
    "退市", "摘帽", "撤销风险警示",
]

# Keywords that mark routine / boilerplate announcements
ROUTINE_KEYWORDS: list[str] = [
    "董事会决议", "监事会决议", "股东大会决议",
    "章程修正案", "议事规则",
    "独立董事意见", "独立董事声明",
    "内部控制", "审计报告",
    "法律意见书",
    "关于召开", "会议通知",
    "补选", "换届",
    "例行",
]


def _df_to_dicts(df: pd.DataFrame) -> list[dict]:
    """Convert a DataFrame to a list of dicts, handling empty frames."""
    if df is None or df.empty:
        return []
    return df.to_dict(orient="records")


class AnnouncementCollector:
    """Collect A-share announcements via akshare."""

    # ------------------------------------------------------------------
    # Daily announcements (某日全部公告)
    # ------------------------------------------------------------------
    def get_daily_announcements(self, date: str) -> list[dict]:
        """Return all announcements published on a given date.

        Parameters
        ----------
        date : str
            Date string, e.g. ``"20240315"`` or ``"2024-03-15"``.

        Returns
        -------
        list[dict]
            Each dict contains keys such as: 代码, 名称, 公告标题, 公告类型,
            公告日期, 公告链接, etc.
        """
        date_norm = date.replace("-", "")
        cache_key = f"daily_ann_{date_norm}"
        cached = _get_cache(cache_key, ttl=600)
        if cached is not None:
            return cached  # type: ignore[return-value]

        try:
            # stock_notice_report returns announcements for a given date
            df = _retry(ak.stock_notice_report, date=date_norm)
            result = _df_to_dicts(df)
            _set_cache(cache_key, result)
            return result
        except Exception as exc:
            logger.error("get_daily_announcements(%s) failed: %s", date, exc)
            # Fallback: try fetching via stock_gsrl_gsdt_em (公司公告)
            try:
                df = _retry(
                    ak.stock_gsrl_gsdt_em,
                    date=date_norm,
                )
                result = _df_to_dicts(df)
                _set_cache(cache_key, result)
                return result
            except Exception as exc2:
                logger.error(
                    "get_daily_announcements fallback also failed: %s", exc2
                )
                return []

    # ------------------------------------------------------------------
    # Company announcements (公司公告)
    # ------------------------------------------------------------------
    def get_company_announcements(
        self,
        code: str,
        start: str,
        end: str,
    ) -> list[dict]:
        """Return announcements for a specific company within a date range.

        Parameters
        ----------
        code : str
            6-digit stock code.
        start : str
            Start date (``YYYYMMDD`` or ``YYYY-MM-DD``).
        end : str
            End date (``YYYYMMDD`` or ``YYYY-MM-DD``).

        Returns
        -------
        list[dict]
        """
        start_norm = start.replace("-", "")
        end_norm = end.replace("-", "")
        cache_key = f"company_ann_{code}_{start_norm}_{end_norm}"
        cached = _get_cache(cache_key, ttl=600)
        if cached is not None:
            return cached  # type: ignore[return-value]

        try:
            # Use stock_notice_report and filter by code
            df = _retry(ak.stock_notice_report, date=end_norm)
            if df is not None and not df.empty:
                # Try to filter by stock code column
                code_col = None
                for col in df.columns:
                    if "代码" in col or "code" in col.lower():
                        code_col = col
                        break
                if code_col:
                    df = df[df[code_col].astype(str).str.contains(code, na=False)]

                # Filter by date range if date column exists
                date_col = None
                for col in df.columns:
                    if "日期" in col or "date" in col.lower() or "时间" in col:
                        date_col = col
                        break
                if date_col:
                    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
                    start_dt = pd.to_datetime(start_norm, format="%Y%m%d")
                    end_dt = pd.to_datetime(end_norm, format="%Y%m%d")
                    df = df[
                        (df[date_col] >= start_dt) & (df[date_col] <= end_dt)
                    ]

            result = _df_to_dicts(df)
            _set_cache(cache_key, result)
            return result
        except Exception as exc:
            logger.error(
                "get_company_announcements(%s, %s, %s) failed: %s",
                code, start, end, exc,
            )
            return []

    # ------------------------------------------------------------------
    # Company announcements via CNInfo (官方公告查询)
    # ------------------------------------------------------------------
    def get_company_announcements_cninfo(
        self,
        code: str,
        start: str,
        end: str,
        market: str = "沪深京",
    ) -> list[dict]:
        """Return authoritative CNInfo announcements for one company.

        The upstream akshare helper is backed by CNInfo's official
        disclosure search endpoint.  It occasionally raises ``KeyError``
        when the query returns no rows, so we normalize that to an empty
        list instead of treating it as a hard failure.
        """

        start_norm = start.replace("-", "")
        end_norm = end.replace("-", "")
        code_norm = str(code).strip().zfill(6)
        cache_key = f"company_ann_cninfo_{code_norm}_{start_norm}_{end_norm}_{market}"
        cached = _get_cache(cache_key, ttl=3600)
        if cached is not None:
            return cached  # type: ignore[return-value]

        last_exc: Optional[Exception] = None
        for attempt in range(1, 3):
            try:
                # CNInfo becomes unstable under bursty request patterns.
                time.sleep(0.12)
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    df = ak.stock_zh_a_disclosure_report_cninfo(
                        symbol=code_norm,
                        market=market,
                        start_date=start_norm,
                        end_date=end_norm,
                    )
                result = _df_to_dicts(df)
                _set_cache(cache_key, result)
                return result
            except KeyError as exc:
                logger.info(
                    "get_company_announcements_cninfo(%s, %s, %s) empty result: %s",
                    code_norm, start_norm, end_norm, exc,
                )
                _set_cache(cache_key, [])
                return []
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "CNInfo disclosure fetch attempt %d/2 failed for %s: %s",
                    attempt, code_norm, exc,
                )
                if attempt < 2:
                    time.sleep(0.8 * attempt)

        logger.error(
            "get_company_announcements_cninfo(%s, %s, %s) failed: %s",
            code_norm, start_norm, end_norm, last_exc,
        )
        raise last_exc  # type: ignore[misc]

    # ------------------------------------------------------------------
    # Importance filtering (过滤例行公告)
    # ------------------------------------------------------------------
    def filter_important_announcements(
        self,
        announcements: list[dict],
        title_key: str = "公告标题",
    ) -> list[dict]:
        """Filter a list of announcement dicts to keep only important ones.

        Strategy:
        1. If the title contains any IMPORTANT_KEYWORDS -> keep.
        2. If the title contains only ROUTINE_KEYWORDS -> discard.
        3. Otherwise -> keep (err on the side of inclusion).

        Parameters
        ----------
        announcements : list[dict]
            Raw announcement dicts (as returned by ``get_daily_announcements``
            or ``get_company_announcements``).
        title_key : str
            The dict key that holds the announcement title.  Defaults to
            ``"公告标题"``.  The method will also try ``"标题"`` and
            ``"title"`` if the specified key is missing.

        Returns
        -------
        list[dict]
            Filtered list containing only material / important announcements.
        """
        important: list[dict] = []

        for ann in announcements:
            # Resolve the title
            title = ann.get(title_key, "")
            if not title:
                title = ann.get("标题", ann.get("title", ""))
            if not title:
                # Cannot determine importance without a title – include it
                important.append(ann)
                continue

            # Check for important keywords first
            has_important = any(kw in title for kw in IMPORTANT_KEYWORDS)
            if has_important:
                important.append(ann)
                continue

            # Check if it is purely routine
            is_routine = any(kw in title for kw in ROUTINE_KEYWORDS)
            if is_routine:
                continue  # skip routine

            # Neither clearly important nor clearly routine -> include
            important.append(ann)

        return important
