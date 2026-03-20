#!/usr/bin/env python3
"""
Fetch and materialize local daily price caches for specific codes.

Use this for one-off补数场景，例如:
  - 补 benchmark/index: 000300
  - 补单个缺失股票: 000800
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from astrategy.data_collector.market_data import MarketDataCollector
from astrategy.datahub.common import ensure_dir, market_root
from astrategy.datahub.market.prices import _load_existing_price_meta, save_price_layer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("fetch_price_cache")

INDEX_CODE_TO_SYMBOL = {
    "000300": "sh000300",
}


def _daily_dir() -> Path:
    return ensure_dir(market_root() / "daily")


def _normalize_stock_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df["日期"])
    out["open"] = pd.to_numeric(df["开盘"], errors="coerce")
    out["high"] = pd.to_numeric(df["最高"], errors="coerce")
    out["low"] = pd.to_numeric(df["最低"], errors="coerce")
    out["close"] = pd.to_numeric(df["收盘"], errors="coerce")
    out["volume"] = pd.to_numeric(df.get("成交量", 0.0), errors="coerce").fillna(0.0)
    return out.dropna(subset=["date", "close"]).reset_index(drop=True)


def _normalize_index_frame(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    out = pd.DataFrame()
    out["date"] = pd.to_datetime(df["date"])
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)
    out = out[(out["date"] >= start_dt) & (out["date"] <= end_dt)].copy()
    if out.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    out["open"] = pd.to_numeric(df.loc[out.index, "open"], errors="coerce")
    out["high"] = pd.to_numeric(df.loc[out.index, "high"], errors="coerce")
    out["low"] = pd.to_numeric(df.loc[out.index, "low"], errors="coerce")
    out["close"] = pd.to_numeric(df.loc[out.index, "close"], errors="coerce")
    volume_col = "volume" if "volume" in df.columns else None
    if volume_col:
        out["volume"] = pd.to_numeric(df.loc[out.index, volume_col], errors="coerce").fillna(0.0)
    else:
        out["volume"] = 0.0
    return out.dropna(subset=["date", "close"]).reset_index(drop=True)


def _records_from_frame(df: pd.DataFrame) -> List[Dict[str, float | str]]:
    rows: List[Dict[str, float | str]] = []
    for _, row in df.iterrows():
        rows.append(
            {
                "trade_date": row["date"].strftime("%Y-%m-%d"),
                "open": float(row.get("open", 0.0) or 0.0),
                "high": float(row.get("high", 0.0) or 0.0),
                "low": float(row.get("low", 0.0) or 0.0),
                "close": float(row.get("close", 0.0) or 0.0),
                "volume": float(row.get("volume", 0.0) or 0.0),
            }
        )
    return rows


def _date_bounds(df: pd.DataFrame) -> Tuple[str, str]:
    if df is None or df.empty or "date" not in df.columns:
        return "", ""
    ordered = pd.to_datetime(df["date"], errors="coerce").dropna().sort_values()
    if ordered.empty:
        return "", ""
    return ordered.iloc[0].strftime("%Y-%m-%d"), ordered.iloc[-1].strftime("%Y-%m-%d")


def _covers_requested_range(df: pd.DataFrame, start: str, end: str) -> bool:
    first, last = _date_bounds(df)
    return bool(first and last and first <= start <= end <= last)


def _load_universe_codes(universe_id: str) -> List[str]:
    membership_path = market_root().parent / "universe" / "universe_membership.json"
    if not membership_path.exists():
        raise FileNotFoundError(f"Universe membership not found: {membership_path}")
    payload = json.loads(membership_path.read_text(encoding="utf-8"))
    codes = {
        str(row.get("ticker", "")).strip().zfill(6)
        for row in payload.get("memberships", [])
        if str(row.get("universe_id", "")).strip() == universe_id and str(row.get("ticker", "")).strip()
    }
    return sorted(code for code in codes if code != "000000")


def _iter_codes(args: argparse.Namespace) -> List[str]:
    if args.codes:
        return [str(code).strip().zfill(6) for code in args.codes]
    if args.universe:
        codes = _load_universe_codes(args.universe)
        if args.limit is not None:
            codes = codes[: max(int(args.limit), 0)]
        return codes
    raise ValueError("Provide codes or --universe")


def _existing_coverage(code: str, start: str, end: str) -> Tuple[bool, int, str, str]:
    collector = MarketDataCollector()
    df = collector._load_local_daily_quotes(code, start.replace("-", ""), end.replace("-", ""))
    first, last = _date_bounds(df)
    return _covers_requested_range(df, start, end), len(df), first, last


def _fetch_index_quotes(code: str, start: str, end: str) -> Tuple[pd.DataFrame, str]:
    symbol = INDEX_CODE_TO_SYMBOL.get(code)
    if not symbol:
        return pd.DataFrame(), "unsupported_index"

    collector = MarketDataCollector()
    local_df = collector._load_local_daily_quotes(code, start.replace("-", ""), end.replace("-", ""))
    local_norm = _normalize_stock_frame(local_df)
    if _covers_requested_range(local_norm, start, end):
        return local_norm, "local_cache"

    import akshare as ak

    df = ak.stock_zh_index_daily(symbol=symbol)
    return _normalize_index_frame(df, start, end), "akshare_index"


def _fetch_stock_quotes(code: str, start: str, end: str) -> Tuple[pd.DataFrame, str]:
    collector = MarketDataCollector()
    local_df = collector._load_local_daily_quotes(code, start.replace("-", ""), end.replace("-", ""))
    local_complete = _covers_requested_range(_normalize_stock_frame(local_df), start, end)
    df = collector.get_daily_quotes(
        code,
        start.replace("-", ""),
        end.replace("-", ""),
    )
    normalized = _normalize_stock_frame(df)
    if local_complete and _covers_requested_range(normalized, start, end):
        return normalized, "local_cache"
    if _covers_requested_range(normalized, start, end):
        return normalized, "akshare_stock"
    if not normalized.empty:
        return normalized, "partial_local_cache"
    return normalized, "empty"


def _fetch_code(code: str, start: str, end: str) -> Tuple[pd.DataFrame, str]:
    code = str(code).strip().zfill(6)
    if code in INDEX_CODE_TO_SYMBOL:
        return _fetch_index_quotes(code, start, end)
    return _fetch_stock_quotes(code, start, end)


def _refresh_manifest() -> Path:
    rows_map = _load_existing_price_meta()
    rows = [rows_map[code] for code in sorted(rows_map.keys())]
    payload = {
        "summary": {
            "requested_tickers": len(rows),
            "with_prices": sum(1 for row in rows if int(row.get("row_count", 0) or 0) > 0),
            "fetched": sum(1 for row in rows if row.get("status") == "fetched"),
            "cached": sum(1 for row in rows if row.get("status") in {"cached", "existing_file"}),
            "empty": sum(1 for row in rows if row.get("status") == "empty"),
            "failed": sum(1 for row in rows if row.get("status") == "failed"),
        },
        "rows": rows,
    }
    return save_price_layer(payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch local price cache for specific codes")
    parser.add_argument("codes", nargs="*", help="One or more 6-digit stock/index codes")
    parser.add_argument("--universe", default="", help="Universe id from universe_membership.json, e.g. csi800")
    parser.add_argument(
        "--start-date",
        default="",
        help="Start date in YYYY-MM-DD",
    )
    parser.add_argument(
        "--end-date",
        default=date.today().isoformat(),
        help="End date in YYYY-MM-DD",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=730,
        help="If --start-date is omitted, derive start_date as end_date - lookback_days + 1",
    )
    parser.add_argument(
        "--skip-covered",
        action="store_true",
        help="Skip codes whose local cache already covers the requested range",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional cap when using --universe",
    )
    args = parser.parse_args()

    end_dt = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    start_date = args.start_date or (end_dt - timedelta(days=max(args.lookback_days - 1, 0))).isoformat()
    codes = _iter_codes(args)
    output_dir = _daily_dir()
    updated: List[Dict[str, str | int]] = []

    for raw_code in codes:
        code = str(raw_code).strip().zfill(6)
        path = output_dir / f"{code}.json"
        if args.skip_covered:
            covered, local_rows, local_first, local_last = _existing_coverage(code, start_date, args.end_date)
            if covered:
                logger.info(
                    "Skip %s: existing local cache already covers %s -> %s (%s rows, %s -> %s)",
                    code,
                    start_date,
                    args.end_date,
                    local_rows,
                    local_first or "N/A",
                    local_last or "N/A",
                )
                updated.append({"code": code, "status": "skipped", "source": "covered_local_cache", "rows": local_rows})
                continue
        try:
            df, source = _fetch_code(code, start_date, args.end_date)
            records = _records_from_frame(df)
            if not records:
                logger.warning("No rows returned for %s", code)
                updated.append({"code": code, "status": "empty", "source": source, "rows": 0})
                continue

            path.write_text(
                json.dumps(records, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            coverage_ok = _covers_requested_range(df, start_date, args.end_date)
            status = "ok" if coverage_ok else "partial"
            logger.info(
                "Wrote %s rows for %s via %s -> %s (covered=%s)",
                len(records),
                code,
                source,
                path,
                coverage_ok,
            )
            updated.append({"code": code, "status": status, "source": source, "rows": len(records)})
        except Exception as exc:
            logger.exception("Failed to fetch %s: %s", code, exc)
            updated.append({"code": code, "status": "failed", "source": "error", "rows": 0})

    manifest_path = _refresh_manifest()

    print()
    print("=" * 72)
    print("Local price cache update")
    print("=" * 72)
    print(f"Date range: {start_date} -> {args.end_date}")
    print(f"Codes processed: {len(codes)}")
    for row in updated:
        print(
            f"{row['code']}: status={row['status']}, source={row['source']}, rows={row['rows']}"
        )
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
