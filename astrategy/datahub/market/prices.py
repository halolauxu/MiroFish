"""Price-layer materialization and audit helpers."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List

from astrategy.data_collector.market_data import MarketDataCollector

from ..common import ensure_dir, market_root

logger = logging.getLogger("astrategy.datahub.prices")


def _price_dir() -> Path:
    return ensure_dir(market_root() / "daily")


def _manifest_path() -> Path:
    return market_root() / "price_manifest.json"


def _normalize_date(value: str) -> str:
    if not value:
        return ""
    return str(value).replace("-", "")[:8]


def _records_from_frame(df: Any) -> List[Dict[str, Any]]:
    if df is None or getattr(df, "empty", True):
        return []

    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        trade_date = row.get("日期", "")
        if hasattr(trade_date, "strftime"):
            trade_date = trade_date.strftime("%Y-%m-%d")
        rows.append(
            {
                "trade_date": str(trade_date),
                "open": float(row.get("开盘", 0.0) or 0.0),
                "high": float(row.get("最高", 0.0) or 0.0),
                "low": float(row.get("最低", 0.0) or 0.0),
                "close": float(row.get("收盘", 0.0) or 0.0),
                "volume": float(row.get("成交量", 0.0) or 0.0),
            }
        )
    return rows


def _load_existing_price_meta() -> Dict[str, Dict[str, Any]]:
    manifest = _manifest_path()
    if manifest.exists():
        data = json.loads(manifest.read_text(encoding="utf-8"))
        return {
            str(item.get("ticker", "")).zfill(6): item
            for item in data.get("rows", [])
            if str(item.get("ticker", "")).strip()
        }

    rows: Dict[str, Dict[str, Any]] = {}
    for path in _price_dir().glob("*.json"):
        ticker = path.stem.strip()
        if not (ticker.isdigit() and len(ticker) == 6):
            continue
        records = json.loads(path.read_text(encoding="utf-8"))
        rows[ticker] = {
            "ticker": ticker,
            "status": "existing_file",
            "row_count": len(records) if isinstance(records, list) else 0,
            "price_file": str(path),
            "price_start": records[0]["trade_date"] if records else "",
            "price_end": records[-1]["trade_date"] if records else "",
            "last_attempt_at": "",
            "error": "",
        }
    return rows


def _has_usable_cache(row: Dict[str, Any]) -> bool:
    price_file = Path(str(row.get("price_file", "")).strip())
    return int(row.get("row_count", 0) or 0) > 0 and price_file.exists()


def build_price_layer(
    universe_membership: Dict[str, Any],
    universe_id: str = "csi800",
    *,
    collect_prices: bool = False,
    lookback_days: int = 365,
    sample_limit: int | None = None,
    end_date: str | None = None,
    refresh: bool = False,
) -> Dict[str, Any]:
    """Build a price manifest and optionally collect daily bars."""
    all_codes = [
        str(row.get("ticker", "")).zfill(6)
        for row in universe_membership.get("memberships", [])
        if row.get("universe_id") == universe_id
    ]
    tickers = sorted({code for code in all_codes if code and code != "000000"})
    if sample_limit is not None:
        tickers = tickers[: max(sample_limit, 0)]

    existing_meta = _load_existing_price_meta()
    collector = MarketDataCollector()

    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else date.today()
    start_dt = end_dt - timedelta(days=max(lookback_days - 1, 0))
    start_key = start_dt.strftime("%Y%m%d")
    end_key = end_dt.strftime("%Y%m%d")

    rows: List[Dict[str, Any]] = []
    for ticker in tickers:
        cached = existing_meta.get(ticker)
        if cached and not refresh and _has_usable_cache(cached):
            rows.append(
                {
                    **cached,
                    "from_cache": True,
                }
            )
            continue

        row = {
            "ticker": ticker,
            "status": "not_requested",
            "row_count": 0,
            "price_file": str(_price_dir() / f"{ticker}.json"),
            "price_start": "",
            "price_end": "",
            "last_attempt_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "error": "",
            "from_cache": False,
        }

        if collect_prices:
            try:
                df = collector.get_daily_quotes(ticker, start_key, end_key)
                records = _records_from_frame(df)
                if records:
                    output = _price_dir() / f"{ticker}.json"
                    output.write_text(
                        json.dumps(records, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                    row.update(
                        {
                            "status": "fetched",
                            "row_count": len(records),
                            "price_start": str(records[0]["trade_date"]),
                            "price_end": str(records[-1]["trade_date"]),
                        }
                    )
                else:
                    row.update(
                        {
                            "status": "empty",
                            "error": "no_rows_returned",
                        }
                    )
            except Exception as exc:
                row.update(
                    {
                        "status": "failed",
                        "error": str(exc),
                    }
                )
        rows.append(row)

    summary = {
        "universe_id": universe_id,
        "requested_tickers": len(tickers),
        "collect_prices": bool(collect_prices),
        "lookback_days": int(lookback_days),
        "start_date": start_dt.isoformat(),
        "end_date": end_dt.isoformat(),
        "fetched": sum(1 for row in rows if row["status"] == "fetched" and not row.get("from_cache")),
        "cached": sum(1 for row in rows if row.get("from_cache")),
        "empty": sum(1 for row in rows if row["status"] == "empty"),
        "failed": sum(1 for row in rows if row["status"] == "failed"),
        "with_prices": sum(1 for row in rows if int(row.get("row_count", 0)) > 0),
    }
    logger.info(
        "Built price layer for %s: requested=%d, with_prices=%d, fetched=%d, cached=%d",
        universe_id,
        summary["requested_tickers"],
        summary["with_prices"],
        summary["fetched"],
        summary["cached"],
    )
    return {
        "summary": summary,
        "rows": rows,
    }


def save_price_layer(payload: Dict[str, Any]) -> Path:
    root = ensure_dir(market_root())
    output = root / "price_manifest.json"
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("Saved price manifest: %s", output)
    return output
