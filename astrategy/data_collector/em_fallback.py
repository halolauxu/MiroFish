"""
East Money API Fallback Utilities
==================================
East Money (东方财富) APIs are blocked by system proxy (127.0.0.1:7890).
This module provides timeout-wrapped calls and Sina-based fallbacks for
common operations: industry boards, stock name lookup, constituents, etc.
"""

import logging
import signal
import threading
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)

# Timeout for East Money API calls (seconds)
EM_TIMEOUT = 8

# ---------------------------------------------------------------------------
# Representative stocks per Shenwan L1 industry (for Sina-based sector returns)
# ---------------------------------------------------------------------------
# Each entry: industry_name -> list of (code, name) representative stocks
INDUSTRY_REPRESENTATIVES: Dict[str, List[Tuple[str, str]]] = {
    "银行": [("601398", "工商银行"), ("600036", "招商银行"), ("601166", "兴业银行")],
    "非银金融": [("601318", "中国平安"), ("600030", "中信证券"), ("601688", "华泰证券")],
    "食品饮料": [("600519", "贵州茅台"), ("000858", "五粮液"), ("000568", "泸州老窖")],
    "医药生物": [("600276", "恒瑞医药"), ("300760", "迈瑞医疗"), ("603259", "药明康德")],
    "电子": [("002415", "海康威视"), ("603501", "韦尔股份"), ("002371", "北方华创")],
    "电力设备": [("300750", "宁德时代"), ("601012", "隆基绿能"), ("300274", "阳光电源")],
    "汽车": [("002594", "比亚迪"), ("600104", "上汽集团"), ("601127", "赛力斯")],
    "家用电器": [("000333", "美的集团"), ("000651", "格力电器"), ("600690", "海尔智家")],
    "计算机": [("002230", "科大讯飞"), ("300496", "中科创达"), ("688111", "金山办公")],
    "通信": [("000063", "中兴通讯"), ("600050", "中国联通"), ("601728", "中国电信")],
    "传媒": [("300413", "芒果超媒"), ("002602", "世纪华通"), ("603444", "吉比特")],
    "机械设备": [("601100", "恒立液压"), ("300124", "汇川技术"), ("002008", "大族激光")],
    "基础化工": [("600309", "万华化学"), ("002601", "龙蟒佰利"), ("600989", "宝丰能源")],
    "有色金属": [("601899", "紫金矿业"), ("002460", "赣锋锂业"), ("600362", "江西铜业")],
    "钢铁": [("600019", "宝钢股份"), ("000709", "河钢股份"), ("600010", "包钢股份")],
    "煤炭": [("601088", "中国神华"), ("600188", "兖矿能源"), ("601898", "中煤能源")],
    "石油石化": [("600028", "中国石化"), ("601857", "中国石油"), ("600346", "恒力石化")],
    "公用事业": [("600900", "长江电力"), ("600886", "国投电力"), ("003816", "中国广核")],
    "交通运输": [("601006", "大秦铁路"), ("600029", "南方航空"), ("601111", "中国国航")],
    "房地产": [("001979", "招商蛇口"), ("000002", "万科A"), ("600048", "保利发展")],
    "建筑装饰": [("601668", "中国建筑"), ("601186", "中国铁建"), ("601390", "中国中铁")],
    "建筑材料": [("600585", "海螺水泥"), ("002271", "东方雨虹"), ("601615", "明泰铝业")],
    "农林牧渔": [("002714", "牧原股份"), ("300498", "温氏股份"), ("600438", "通威股份")],
    "纺织服饰": [("002563", "森马服饰"), ("603116", "红蜻蜓"), ("002029", "七匹狼")],
    "轻工制造": [("603833", "欧派家居"), ("603816", "顾家家居"), ("002831", "裕同科技")],
    "商贸零售": [("601888", "中国中免"), ("002024", "苏宁易购"), ("600827", "百联股份")],
    "社会服务": [("300144", "宋城演艺"), ("002841", "视源股份"), ("603136", "天目湖")],
    "国防军工": [("600760", "中航沈飞"), ("601989", "中国重工"), ("600893", "航发动力")],
    "环保": [("603568", "伟明环保"), ("000967", "盈峰环境"), ("300070", "碧水源")],
    "美容护理": [("300529", "健帆生物"), ("300957", "贝泰妮"), ("605136", "丽人丽妆")],
    "综合": [("600848", "上海临港"), ("000042", "中洲控股"), ("600663", "陆家嘴")],
}


# ---------------------------------------------------------------------------
# Stock code → name mapping (Sina-based, cached)
# ---------------------------------------------------------------------------
_stock_name_map: Optional[Dict[str, str]] = None


def get_stock_name_map() -> Dict[str, str]:
    """Return a {code: name} dict from Sina (cached globally)."""
    global _stock_name_map
    if _stock_name_map is not None:
        return _stock_name_map
    try:
        df = ak.stock_info_a_code_name()
        if df is not None and not df.empty:
            df.columns = ["code", "name"]
            _stock_name_map = dict(zip(df["code"].astype(str), df["name"].astype(str)))
            return _stock_name_map
    except Exception as exc:
        logger.warning("stock_info_a_code_name failed: %s", exc)
    _stock_name_map = {}
    return _stock_name_map


def lookup_stock_name(stock_code: str) -> Optional[str]:
    """Look up stock name by code using Sina API (no East Money)."""
    name_map = get_stock_name_map()
    return name_map.get(stock_code)


# ---------------------------------------------------------------------------
# Stock → industry mapping (predefined, no API needed)
# ---------------------------------------------------------------------------
_stock_to_industry: Optional[Dict[str, str]] = None


def _build_stock_to_industry() -> Dict[str, str]:
    """Build reverse mapping: stock_code -> industry from INDUSTRY_REPRESENTATIVES."""
    global _stock_to_industry
    if _stock_to_industry is not None:
        return _stock_to_industry
    mapping = {}
    for industry, stocks in INDUSTRY_REPRESENTATIVES.items():
        for code, name in stocks:
            mapping[code] = industry
    _stock_to_industry = mapping
    return mapping


def lookup_stock_industry(stock_code: str) -> Optional[str]:
    """Find which industry a stock belongs to (using predefined mapping)."""
    mapping = _build_stock_to_industry()
    return mapping.get(stock_code)


# ---------------------------------------------------------------------------
# Timeout wrapper for East Money calls
# ---------------------------------------------------------------------------

def call_with_timeout(fn, *args, timeout: int = EM_TIMEOUT, **kwargs):
    """Call fn with a timeout. Returns None if timeout or error."""
    result = [None]
    error = [None]

    def target():
        try:
            result[0] = fn(*args, **kwargs)
        except Exception as exc:
            error[0] = exc

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout=timeout)

    if thread.is_alive():
        logger.info("Timeout (%ds) calling %s, using fallback", timeout, fn.__name__)
        return None

    if error[0] is not None:
        logger.debug("Error calling %s: %s", fn.__name__, error[0])
        return None

    return result[0]


def get_industry_constituents_safe(industry_name: str) -> List[Tuple[str, str]]:
    """Get industry constituents with timeout, falling back to predefined stocks."""
    # Try East Money with timeout
    df = call_with_timeout(ak.stock_board_industry_cons_em, symbol=industry_name)
    if df is not None and not df.empty:
        code_col = "代码" if "代码" in df.columns else df.columns[0]
        name_col = "名称" if "名称" in df.columns else df.columns[1]
        return list(zip(
            df[code_col].astype(str).tolist(),
            df[name_col].astype(str).tolist(),
        ))

    # Fallback: predefined representatives
    reps = INDUSTRY_REPRESENTATIVES.get(industry_name, [])
    if reps:
        return reps

    logger.debug("No constituents for industry: %s", industry_name)
    return []


def scan_industry_returns_sina(lookback_days: int = 20) -> pd.DataFrame:
    """Compute industry sector returns using representative stocks via Sina API.

    Returns a DataFrame with columns:
        板块名称, return_5d, return_10d, return_20d,
        relative_strength, volume_change_pct, momentum_rank
    """
    from datetime import datetime, timedelta

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=lookback_days + 30)).strftime("%Y%m%d")

    records = []

    for industry, stocks in INDUSTRY_REPRESENTATIVES.items():
        industry_returns_5d = []
        industry_returns_10d = []
        industry_returns_20d = []
        vol_changes = []

        for code, name in stocks[:3]:  # Use top 3 reps per industry
            try:
                sina_code = f"sh{code}" if code.startswith("6") else f"sz{code}"
                df = ak.stock_zh_a_daily(symbol=sina_code, adjust="qfq")
                if df is None or df.empty or len(df) < 6:
                    continue

                # Filter by date range
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"])
                    df = df[df["date"] >= pd.to_datetime(start_date)]
                    df = df.reset_index(drop=True)

                if len(df) < 6:
                    continue

                closes = df["close"].astype(float).tolist()
                cur = closes[-1]

                if len(closes) >= 6:
                    industry_returns_5d.append((cur / closes[-6] - 1) * 100)
                if len(closes) >= 11:
                    industry_returns_10d.append((cur / closes[-11] - 1) * 100)
                if len(closes) >= 21:
                    industry_returns_20d.append((cur / closes[-21] - 1) * 100)

                # Volume change
                if "volume" in df.columns and len(df) >= 10:
                    vols = df["volume"].astype(float).tolist()
                    recent = sum(vols[-5:]) / 5
                    prior = sum(vols[-10:-5]) / 5
                    if prior > 0:
                        vol_changes.append((recent / prior - 1) * 100)

            except Exception as exc:
                logger.debug("Failed to fetch %s (%s): %s", code, name, exc)
                continue

        # Average across representatives
        ret5 = round(sum(industry_returns_5d) / len(industry_returns_5d), 2) if industry_returns_5d else 0.0
        ret10 = round(sum(industry_returns_10d) / len(industry_returns_10d), 2) if industry_returns_10d else 0.0
        ret20 = round(sum(industry_returns_20d) / len(industry_returns_20d), 2) if industry_returns_20d else 0.0
        vol_chg = round(sum(vol_changes) / len(vol_changes), 2) if vol_changes else 0.0

        records.append({
            "板块名称": industry,
            "板块代码": "",
            "最新价": None,
            "涨跌幅": ret5,  # Use 5d return as proxy for intraday
            "return_5d": ret5,
            "return_10d": ret10,
            "return_20d": ret20,
            "volume_change_pct": vol_chg,
        })

    df = pd.DataFrame(records)
    if df.empty:
        return df

    # Relative strength = weighted combo
    df["relative_strength"] = (
        df["return_5d"] * 0.5 + df["return_10d"] * 0.3 + df["return_20d"] * 0.2
    ).round(2)

    df["momentum_rank"] = df["relative_strength"].rank(ascending=False, method="min").astype(int)
    df = df.sort_values("momentum_rank").reset_index(drop=True)

    return df
