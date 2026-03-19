"""Bootstrap the first-stage data foundation outputs."""

from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict

from .audit import build_coverage_audit, save_coverage_audit
from .ingest import (
    build_filings_layer,
    build_incremental_event_layer,
    build_news_layer,
    save_filings_layer,
    save_incremental_event_layer,
    save_news_layer,
)
from .market import build_price_layer, save_price_layer
from .universe import (
    build_security_master,
    build_universe_membership,
    build_universe_snapshots,
    save_security_master,
    save_universe_membership,
    save_universe_snapshots,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("astrategy.datahub.bootstrap")


def run_data_foundation(
    universe_id: str = "csi800",
    *,
    collect_prices: bool = False,
    price_lookback_days: int = 365,
    price_sample_limit: int | None = None,
    refresh_prices: bool = False,
    ingest_date: str | None = None,
    collect_filings: bool = False,
    collect_news: bool = False,
    news_sample_limit: int | None = None,
    max_news_per_stock: int = 5,
    refresh_news: bool = False,
) -> Dict[str, Any]:
    """Run the data-foundation bootstrap workflow."""
    as_of_date = ingest_date or date.today().isoformat()
    security_master = build_security_master()
    security_path = save_security_master(security_master)

    universe_membership = build_universe_membership(security_master)
    universe_path = save_universe_membership(universe_membership)
    snapshots = build_universe_snapshots(security_master, universe_membership)
    snapshot_paths = save_universe_snapshots(snapshots)

    price_layer = build_price_layer(
        universe_membership,
        universe_id=universe_id,
        collect_prices=collect_prices,
        lookback_days=price_lookback_days,
        sample_limit=price_sample_limit,
        refresh=refresh_prices,
    )
    price_path = save_price_layer(price_layer)

    filings_layer = build_filings_layer(
        universe_membership,
        universe_id=universe_id,
        ingest_date=as_of_date,
        collect_filings=collect_filings,
    )
    filings_paths = save_filings_layer(filings_layer)

    news_layer = build_news_layer(
        universe_membership,
        universe_id=universe_id,
        ingest_date=as_of_date,
        collect_news=collect_news,
        news_sample_limit=news_sample_limit,
        max_news_per_stock=max_news_per_stock,
        refresh_news=refresh_news,
    )
    news_paths = save_news_layer(news_layer)

    incremental_events = build_incremental_event_layer(
        security_master,
        filings_layer,
        news_layer,
        ingest_date=as_of_date,
    )
    event_paths = save_incremental_event_layer(incremental_events)

    coverage_audit = build_coverage_audit(
        security_master,
        universe_membership,
        universe_id=universe_id,
        as_of_date=as_of_date,
    )
    audit_paths = save_coverage_audit(coverage_audit)

    return {
        "security_master_path": str(security_path),
        "universe_membership_path": str(universe_path),
        "universe_snapshot_paths": {k: str(v) for k, v in snapshot_paths.items()},
        "price_manifest_path": str(price_path),
        "filings_paths": {k: str(v) for k, v in filings_paths.items()},
        "news_paths": {k: str(v) for k, v in news_paths.items()},
        "incremental_event_paths": {k: str(v) for k, v in event_paths.items()},
        "coverage_audit_json": str(audit_paths["json"]),
        "coverage_audit_markdown": str(audit_paths["markdown"]),
        "coverage_audit_daily_json": str(audit_paths["daily_json"]),
        "price_summary": price_layer["summary"],
        "filings_summary": filings_layer["summary"],
        "news_summary": news_layer["summary"],
        "incremental_event_summary": incremental_events["summary"],
        "coverage_summary": coverage_audit["summary"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run first-stage data foundation bootstrap")
    parser.add_argument("--universe", type=str, default="csi800", help="Target universe id")
    parser.add_argument(
        "--collect-prices",
        action="store_true",
        help="Attempt to collect daily price bars for the target universe",
    )
    parser.add_argument(
        "--price-lookback-days",
        type=int,
        default=365,
        help="Lookback window for daily price collection",
    )
    parser.add_argument(
        "--price-sample-limit",
        type=int,
        default=None,
        help="Optional cap on the number of tickers to process for price collection",
    )
    parser.add_argument(
        "--refresh-prices",
        action="store_true",
        help="Refresh price files even when cached files already exist",
    )
    parser.add_argument(
        "--ingest-date",
        type=str,
        default=None,
        help="As-of date for daily ingestion and audit outputs (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--collect-filings",
        action="store_true",
        help="Collect and filter daily filings for the target universe",
    )
    parser.add_argument(
        "--collect-news",
        action="store_true",
        help="Collect company-news snapshots for the target universe",
    )
    parser.add_argument(
        "--news-sample-limit",
        type=int,
        default=None,
        help="Optional cap on the number of tickers to process for news ingestion",
    )
    parser.add_argument(
        "--max-news-per-stock",
        type=int,
        default=5,
        help="Maximum news items to store per stock during one run",
    )
    parser.add_argument(
        "--refresh-news",
        action="store_true",
        help="Refresh news files even when cached files already exist",
    )
    args = parser.parse_args()

    result = run_data_foundation(
        universe_id=args.universe,
        collect_prices=args.collect_prices,
        price_lookback_days=args.price_lookback_days,
        price_sample_limit=args.price_sample_limit,
        refresh_prices=args.refresh_prices,
        ingest_date=args.ingest_date,
        collect_filings=args.collect_filings,
        collect_news=args.collect_news,
        news_sample_limit=args.news_sample_limit,
        max_news_per_stock=args.max_news_per_stock,
        refresh_news=args.refresh_news,
    )
    logger.info("Security master: %s", result["security_master_path"])
    logger.info("Universe membership: %s", result["universe_membership_path"])
    logger.info("Price manifest: %s", result["price_manifest_path"])
    logger.info("Daily coverage audit: %s", result["coverage_audit_daily_json"])
    logger.info("Coverage audit: %s", result["coverage_audit_json"])


if __name__ == "__main__":
    main()
