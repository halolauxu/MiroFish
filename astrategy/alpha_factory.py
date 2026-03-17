"""
Alpha Factory — Minimum Viable Loop
====================================
Dispatches the three alpha production lines and consolidates signals
into a single CSV + Markdown report.

Alpha Lines:
  A: Knowledge Graph Factors (S07)
  B: Supply Chain Shock Propagation (S01, with S03 merged)
  C: Multi-Agent Narrative Intelligence (S10, with S06+S11 merged)

Usage:
    python -m astrategy.alpha_factory [--stocks 600519,000858,...] [--top-n 10]
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from astrategy.strategies.base import StrategySignal

logger = logging.getLogger("astrategy.alpha_factory")

_CST = timezone(timedelta(hours=8))


def run_line_a(stock_codes: list[str], top_n: int = 10) -> list[StrategySignal]:
    """Line A: S07 Graph-Enhanced Multi-Factor."""
    from astrategy.strategies.s07_graph_factors import GraphFactorsStrategy
    strategy = GraphFactorsStrategy(top_n=top_n)
    return strategy.run(stock_codes)


def run_line_b(stock_codes: list[str]) -> list[StrategySignal]:
    """Line B: S01 Supply Chain Shock (with S03 policy events merged)."""
    from astrategy.strategies.s01_supply_chain import SupplyChainStrategy
    strategy = SupplyChainStrategy()
    return strategy.run(stock_codes)


def run_line_c(stock_codes: list[str]) -> list[StrategySignal]:
    """Line C: S10 Sentiment Simulation (with S06+S11 merged)."""
    from astrategy.strategies.s10_sentiment_simulation import SentimentSimulationStrategy
    strategy = SentimentSimulationStrategy()
    signals: list[StrategySignal] = []
    for code in stock_codes[:10]:  # limit LLM calls
        try:
            sigs = strategy.run_single(code)
            signals.extend(sigs)
        except Exception as exc:
            logger.warning("Line C failed for %s: %s", code, str(exc)[:80])
    return signals


def consolidate(
    signals_a: list[StrategySignal],
    signals_b: list[StrategySignal],
    signals_c: list[StrategySignal],
) -> list[dict]:
    """Merge signals from all three lines into a flat list of dicts."""
    rows: list[dict] = []

    def _to_row(sig: StrategySignal, line: str) -> dict:
        return {
            "alpha_line": line,
            "strategy": sig.strategy_name,
            "stock_code": sig.stock_code,
            "stock_name": sig.stock_name,
            "direction": sig.direction,
            "confidence": round(sig.confidence, 4),
            "expected_return": round(sig.expected_return, 4),
            "holding_days": sig.holding_period_days,
            "reasoning": sig.reasoning[:120],
        }

    for s in signals_a:
        rows.append(_to_row(s, "A_graph_factors"))
    for s in signals_b:
        rows.append(_to_row(s, "B_supply_chain"))
    for s in signals_c:
        rows.append(_to_row(s, "C_narrative"))

    return rows


def save_csv(rows: list[dict], path: Path) -> None:
    """Write consolidated signals to CSV."""
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    logger.info("CSV saved to %s (%d rows)", path, len(rows))


def save_report(
    rows: list[dict],
    path: Path,
    elapsed: float,
    counts: dict[str, int],
) -> None:
    """Write Markdown summary report."""
    path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(tz=_CST).strftime("%Y-%m-%d %H:%M")

    lines = [
        f"# Alpha Factory Report — {now}",
        "",
        f"Runtime: {elapsed:.0f}s",
        "",
        "## Signal Summary",
        "",
        "| Alpha Line | Signals | Long | Short | Neutral |",
        "|------------|---------|------|-------|---------|",
    ]

    for line_name in ["A_graph_factors", "B_supply_chain", "C_narrative"]:
        line_rows = [r for r in rows if r["alpha_line"] == line_name]
        total = len(line_rows)
        longs = sum(1 for r in line_rows if r["direction"] == "long")
        shorts = sum(1 for r in line_rows if r["direction"] == "short")
        neutrals = total - longs - shorts
        lines.append(f"| {line_name} | {total} | {longs} | {shorts} | {neutrals} |")

    lines.append(f"| **Total** | **{len(rows)}** | | | |")
    lines.append("")

    # Top long signals across all lines
    longs = sorted(
        [r for r in rows if r["direction"] == "long"],
        key=lambda r: r["confidence"],
        reverse=True,
    )
    if longs:
        lines.append("## Top Long Signals")
        lines.append("")
        lines.append("| Line | Code | Name | Confidence | E[R] | Reasoning |")
        lines.append("|------|------|------|------------|------|-----------|")
        for r in longs[:15]:
            lines.append(
                f"| {r['alpha_line']} | {r['stock_code']} | {r['stock_name']} | "
                f"{r['confidence']:.2f} | {r['expected_return']:+.2%} | "
                f"{r['reasoning'][:60]} |"
            )
        lines.append("")

    # Top short signals
    shorts = sorted(
        [r for r in rows if r["direction"] == "short"],
        key=lambda r: r["confidence"],
        reverse=True,
    )
    if shorts:
        lines.append("## Top Short Signals")
        lines.append("")
        lines.append("| Line | Code | Name | Confidence | E[R] | Reasoning |")
        lines.append("|------|------|------|------------|------|-----------|")
        for r in shorts[:10]:
            lines.append(
                f"| {r['alpha_line']} | {r['stock_code']} | {r['stock_name']} | "
                f"{r['confidence']:.2f} | {r['expected_return']:+.2%} | "
                f"{r['reasoning'][:60]} |"
            )
        lines.append("")

    # Cross-line consensus (stocks appearing in 2+ lines)
    from collections import Counter
    code_counts = Counter(r["stock_code"] for r in rows if r["direction"] in ("long", "short"))
    consensus = [(code, cnt) for code, cnt in code_counts.items() if cnt >= 2]
    if consensus:
        lines.append("## Cross-Line Consensus (2+ lines agree)")
        lines.append("")
        for code, cnt in sorted(consensus, key=lambda x: -x[1]):
            matching = [r for r in rows if r["stock_code"] == code]
            dirs = set(r["direction"] for r in matching)
            src_lines = ", ".join(set(r["alpha_line"] for r in matching))
            name = matching[0]["stock_name"]
            lines.append(f"- **{name}**({code}): {cnt} signals from {src_lines}, direction={dirs}")
        lines.append("")

    report = "\n".join(lines)
    path.write_text(report, encoding="utf-8")
    logger.info("Report saved to %s", path)


def main():
    parser = argparse.ArgumentParser(description="Alpha Factory — 三线合一信号生产")
    parser.add_argument(
        "--stocks", type=str, default="",
        help="逗号分隔的股票代码，留空=使用CSI-300前30",
    )
    parser.add_argument("--top-n", type=int, default=10, help="S07 top/bottom N")
    parser.add_argument(
        "--output-dir", type=str, default="",
        help="输出目录，默认 .data/reports/",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # Resolve stock universe
    if args.stocks:
        stock_codes = [c.strip() for c in args.stocks.split(",") if c.strip()]
    else:
        try:
            import akshare as ak
            df = ak.index_stock_cons_csindex(symbol="000300")
            stock_codes = df["成分券代码"].head(30).tolist()
            logger.info("Using CSI-300 top 30 stocks")
        except Exception:
            stock_codes = [
                "600519", "000858", "601318", "600036", "000001",
                "601166", "600276", "000333", "002415", "600900",
            ]
            logger.info("Using hardcoded 10 stocks (fallback)")

    output_dir = Path(args.output_dir) if args.output_dir else (
        Path(__file__).resolve().parent / ".data" / "reports"
    )
    date_str = datetime.now(tz=_CST).strftime("%Y%m%d")

    logger.info("=" * 60)
    logger.info("Alpha Factory — %d stocks", len(stock_codes))
    logger.info("=" * 60)

    t0 = time.time()

    # ── Run all three lines ──────────────────────────────────────
    logger.info("Line A: Graph Factors (S07) ...")
    signals_a = run_line_a(stock_codes, top_n=args.top_n)
    logger.info("Line A: %d signals", len(signals_a))

    logger.info("Line B: Supply Chain Shock (S01) ...")
    signals_b = run_line_b(stock_codes)
    logger.info("Line B: %d signals", len(signals_b))

    logger.info("Line C: Narrative Intelligence (S10) ...")
    signals_c = run_line_c(stock_codes)
    logger.info("Line C: %d signals", len(signals_c))

    elapsed = time.time() - t0

    # ── Consolidate ──────────────────────────────────────────────
    rows = consolidate(signals_a, signals_b, signals_c)

    counts = {
        "A": len(signals_a),
        "B": len(signals_b),
        "C": len(signals_c),
        "total": len(rows),
    }

    # ── Save outputs ─────────────────────────────────────────────
    csv_path = output_dir / f"alpha_factory_{date_str}.csv"
    report_path = output_dir / f"alpha_factory_{date_str}.md"

    save_csv(rows, csv_path)
    save_report(rows, report_path, elapsed, counts)

    # Print summary
    print()
    print("=" * 60)
    print(f"Alpha Factory Complete — {elapsed:.0f}s")
    print(f"  Line A (Graph Factors):     {counts['A']} signals")
    print(f"  Line B (Supply Chain):      {counts['B']} signals")
    print(f"  Line C (Narrative):         {counts['C']} signals")
    print(f"  Total:                      {counts['total']} signals")
    print(f"  CSV:    {csv_path}")
    print(f"  Report: {report_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
