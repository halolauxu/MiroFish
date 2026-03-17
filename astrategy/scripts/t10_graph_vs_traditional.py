"""
T10: S07 Graph Factor vs Traditional Factor Comparison
======================================================
Runs S07 twice on the same stock universe:
  A) graph_weight=0  (traditional factors only)
  B) graph_weight=normal (graph + traditional)

Outputs a comparison table showing whether graph factors add alpha.
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

# Ensure astrategy is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from astrategy.strategies.s07_graph_factors import (
    DEFAULT_WEIGHTS,
    GraphFactorsStrategy,
    _GRAPH_FACTOR_COLS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("t10_comparison")

_CST = timezone(timedelta(hours=8))


def run_comparison(stock_codes: list[str] | None = None, top_n: int = 10):
    """Run the comparison and print results."""

    if not stock_codes:
        # Use a representative subset (30 stocks from different sectors)
        try:
            import akshare as ak
            df = ak.index_stock_cons_csindex(symbol="000300")
            stock_codes = df["成分券代码"].head(30).tolist()
            logger.info("Using CSI-300 top 30 stocks")
        except Exception:
            stock_codes = [
                "600519", "000858", "601318", "600036", "000001",
                "601166", "600276", "000333", "002415", "600900",
                "601888", "300750", "002594", "600809", "000568",
                "601012", "600031", "002142", "600585", "601398",
                "000661", "601669", "600030", "002304", "601225",
                "000725", "002460", "300274", "600019", "601688",
            ]
            logger.info("Using hardcoded 30 stocks")

    end_date = datetime.now(tz=_CST).strftime("%Y%m%d")

    # ── Version A: graph weights = 0 ─────────────────────────────
    logger.info("=" * 60)
    logger.info("Version A: Traditional factors ONLY (graph weights = 0)")
    logger.info("=" * 60)

    zero_graph_weights = {**DEFAULT_WEIGHTS}
    for col in _GRAPH_FACTOR_COLS:
        zero_graph_weights[col] = 0.0

    strategy_a = GraphFactorsStrategy(weights=zero_graph_weights, top_n=top_n)
    signals_a = strategy_a.run(stock_codes)

    # ── Version B: normal graph weights ──────────────────────────
    logger.info("=" * 60)
    logger.info("Version B: Graph + Traditional (default weights)")
    logger.info("=" * 60)

    strategy_b = GraphFactorsStrategy(top_n=top_n)
    signals_b = strategy_b.run(stock_codes)

    # ── Build comparison ─────────────────────────────────────────
    def signals_to_df(signals, label):
        rows = []
        for s in signals:
            rows.append({
                "stock_code": s.stock_code,
                "stock_name": s.stock_name,
                "direction": s.direction,
                "confidence": s.confidence,
                "expected_return": s.expected_return,
                "composite_score": s.metadata.get("composite_score", 0),
                "rank": s.metadata.get("rank", 0),
            })
        df = pd.DataFrame(rows)
        df["version"] = label
        return df

    df_a = signals_to_df(signals_a, "A_traditional")
    df_b = signals_to_df(signals_b, "B_graph+trad")

    # ── Print comparison table ───────────────────────────────────
    print("\n" + "=" * 80)
    print("T10: S07 Graph Factor vs Traditional Factor Comparison")
    print("=" * 80)

    print(f"\nUniverse: {len(stock_codes)} stocks, top_n={top_n}")
    print(f"Date: {end_date}")

    print(f"\n--- Version A (Traditional Only): {len(signals_a)} signals ---")
    long_a = [s for s in signals_a if s.direction == "long"]
    short_a = [s for s in signals_a if s.direction == "short"]
    print(f"  Long:  {len(long_a)}, Short: {len(short_a)}")
    if long_a:
        print(f"  Top longs: {', '.join(f'{s.stock_name}({s.stock_code})' for s in long_a[:5])}")
    if short_a:
        print(f"  Top shorts: {', '.join(f'{s.stock_name}({s.stock_code})' for s in short_a[:5])}")

    print(f"\n--- Version B (Graph + Traditional): {len(signals_b)} signals ---")
    long_b = [s for s in signals_b if s.direction == "long"]
    short_b = [s for s in signals_b if s.direction == "short"]
    print(f"  Long:  {len(long_b)}, Short: {len(short_b)}")
    if long_b:
        print(f"  Top longs: {', '.join(f'{s.stock_name}({s.stock_code})' for s in long_b[:5])}")
    if short_b:
        print(f"  Top shorts: {', '.join(f'{s.stock_name}({s.stock_code})' for s in short_b[:5])}")

    # ── Signal overlap ───────────────────────────────────────────
    long_codes_a = {s.stock_code for s in long_a}
    long_codes_b = {s.stock_code for s in long_b}
    short_codes_a = {s.stock_code for s in short_a}
    short_codes_b = {s.stock_code for s in short_b}

    long_overlap = long_codes_a & long_codes_b
    short_overlap = short_codes_a & short_codes_b

    print(f"\n--- Signal Overlap ---")
    print(f"  Long overlap:  {len(long_overlap)}/{max(len(long_codes_a), len(long_codes_b))}")
    print(f"  Short overlap: {len(short_overlap)}/{max(len(short_codes_a), len(short_codes_b))}")

    long_only_b = long_codes_b - long_codes_a
    short_only_b = short_codes_b - short_codes_a
    if long_only_b:
        names_b = {s.stock_code: s.stock_name for s in long_b}
        print(f"  Graph-added longs:  {', '.join(f'{names_b.get(c,c)}({c})' for c in long_only_b)}")
    if short_only_b:
        names_b = {s.stock_code: s.stock_name for s in short_b}
        print(f"  Graph-added shorts: {', '.join(f'{names_b.get(c,c)}({c})' for c in short_only_b)}")

    # ── Rank comparison ──────────────────────────────────────────
    print(f"\n--- Rank Comparison (top 10) ---")
    merged = df_a[["stock_code", "stock_name", "composite_score", "rank"]].rename(
        columns={"composite_score": "score_A", "rank": "rank_A"}
    ).merge(
        df_b[["stock_code", "composite_score", "rank"]].rename(
            columns={"composite_score": "score_B", "rank": "rank_B"}
        ),
        on="stock_code",
        how="outer",
    ).fillna(0)
    merged["rank_change"] = merged["rank_A"] - merged["rank_B"]
    merged = merged.sort_values("rank_B")
    print(merged.head(15).to_string(index=False))

    # ── Graph factor contribution ────────────────────────────────
    print(f"\n--- Graph Factor Values (Version B, top 10 by rank) ---")
    graph_data = []
    for s in sorted(signals_b, key=lambda x: x.metadata.get("rank", 999))[:10]:
        row = {"code": s.stock_code, "name": s.stock_name}
        for col in _GRAPH_FACTOR_COLS:
            row[col] = s.metadata.get(col, 0.0)
        graph_data.append(row)

    if graph_data:
        gdf = pd.DataFrame(graph_data)
        # Show only non-zero columns
        non_zero_cols = ["code", "name"]
        for col in _GRAPH_FACTOR_COLS:
            if col in gdf.columns and gdf[col].abs().sum() > 1e-9:
                non_zero_cols.append(col)
        if len(non_zero_cols) > 2:
            print(gdf[non_zero_cols].to_string(index=False))
        else:
            print("  All graph factors are zero (graph data insufficient)")

    # ── Save comparison report ───────────────────────────────────
    report_dir = Path(__file__).resolve().parent.parent / ".data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "t10_graph_vs_traditional.md"

    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"# T10: S07 Graph vs Traditional Comparison\n\n")
        f.write(f"Date: {end_date}\n")
        f.write(f"Universe: {len(stock_codes)} stocks\n\n")
        f.write(f"## Signals\n")
        f.write(f"| Version | Long | Short | Neutral |\n")
        f.write(f"|---------|------|-------|--------|\n")
        f.write(f"| A (Traditional) | {len(long_a)} | {len(short_a)} | {len(signals_a)-len(long_a)-len(short_a)} |\n")
        f.write(f"| B (Graph+Trad) | {len(long_b)} | {len(short_b)} | {len(signals_b)-len(long_b)-len(short_b)} |\n\n")
        f.write(f"## Overlap\n")
        f.write(f"- Long: {len(long_overlap)}/{max(len(long_codes_a), len(long_codes_b))}\n")
        f.write(f"- Short: {len(short_overlap)}/{max(len(short_codes_a), len(short_codes_b))}\n\n")
        f.write(f"## Rank Comparison\n")
        f.write(merged.head(15).to_markdown(index=False))
        f.write("\n")

    print(f"\nReport saved to: {report_path}")
    print("=" * 80)


if __name__ == "__main__":
    run_comparison()
