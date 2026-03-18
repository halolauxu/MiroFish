"""
Alpha Factory — 冲击传播链路（主线收敛版）
================================================

Architecture (v2 → v3 收敛):
    ONLY: Shock Pipeline (图谱传播 + 规则映射 + 信息差检测)
        事件 → 图谱找下游 → 规则映射方向 → 检查是否已反应 → Alpha

S07/S10 已移除。系统只跑冲击链路。

Usage:
    python -m astrategy.alpha_factory [--stocks 600519,000858,...] [--max-events 5]
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from astrategy.strategies.base import StrategySignal

logger = logging.getLogger("astrategy.alpha_factory")

_CST = timezone(timedelta(hours=8))


# ---------------------------------------------------------------------------
# Shock pipeline runner
# ---------------------------------------------------------------------------


def run_shock_pipeline(
    stock_codes: list[str],
    max_events: int = 5,
    skip_debate: bool = False,
) -> list[StrategySignal]:
    """PRIMARY: Shock Propagation Pipeline (图谱 + 规则映射 + 信息差)."""
    from astrategy.shock_pipeline import ShockPipeline
    pipeline = ShockPipeline()
    shock_signals = pipeline.run(
        stock_codes,
        max_events=max_events,
        skip_debate=skip_debate,
    )
    return pipeline.to_strategy_signals(shock_signals)


# ---------------------------------------------------------------------------
# Consolidation
# ---------------------------------------------------------------------------


def consolidate(
    shock_signals: list[StrategySignal],
) -> list[dict]:
    """Convert shock signals into a flat list of dicts."""
    rows: list[dict] = []

    def _to_row(sig: StrategySignal) -> dict:
        meta = sig.metadata or {}
        return {
            "alpha_line": "SHOCK_PRIMARY",
            "strategy": sig.strategy_name,
            "stock_code": sig.stock_code,
            "stock_name": sig.stock_name,
            "direction": sig.direction,
            "confidence": round(sig.confidence, 4),
            "expected_return": round(sig.expected_return, 4),
            "holding_days": sig.holding_period_days,
            "divergence": meta.get("divergence", ""),
            "shock_weight": meta.get("shock_weight", ""),
            "alpha_type": meta.get("alpha_type", ""),
            "cross_validated": False,
            "reasoning": sig.reasoning[:200],
        }

    for s in shock_signals:
        rows.append(_to_row(s))

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
        f"# Alpha Factory Report (v3 冲击链路收敛) — {now}",
        "",
        f"Runtime: {elapsed:.0f}s",
        "",
        "## Architecture",
        "- **ONLY**: 冲击传播链路 (事件→图谱传播→规则映射→信息差检测)",
        "",
        "## Signal Summary",
        "",
        "| Alpha Line | Signals | Long | Avoid |",
        "|------------|---------|------|-------|",
    ]

    total = len(rows)
    longs = sum(1 for r in rows if r["direction"] == "long")
    avoids = sum(1 for r in rows if r["direction"] in ("short", "avoid"))
    lines.append(f"| SHOCK_PRIMARY | {total} | {longs} | {avoids} |")
    lines.append("")

    # Shock pipeline signals
    if rows:
        lines.append("## 冲击传播信号")
        lines.append("")
        lines.append("| Code | Name | Direction | Confidence | Shock | Alpha Type | Reasoning |")
        lines.append("|------|------|-----------|------------|-------|------------|-----------|")
        for r in sorted(rows, key=lambda r: r["confidence"], reverse=True)[:20]:
            lines.append(
                f"| {r['stock_code']} | {r['stock_name']} | {r['direction']} | "
                f"{r['confidence']:.2f} | {r.get('shock_weight', '')} | "
                f"{r.get('alpha_type', '')} | "
                f"{r['reasoning'][:80]} |"
            )
        lines.append("")

    report = "\n".join(lines)
    path.write_text(report, encoding="utf-8")
    logger.info("Report saved to %s", path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Alpha Factory v3 — 冲击传播链路（收敛版）",
    )
    parser.add_argument(
        "--stocks", type=str, default="",
        help="逗号分隔的股票代码，留空=使用CSI-300前30",
    )
    parser.add_argument("--max-events", type=int, default=5, help="冲击链路最大事件数")
    parser.add_argument(
        "--skip-debate", action="store_true",
        help="跳过Agent辩论（快速测试模式）",
    )
    parser.add_argument(
        "--shock-only", action="store_true", default=True,
        help="只跑冲击链路（默认行为，保留兼容性）",
    )
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
    logger.info("Alpha Factory v3 — %d stocks (shock_only=True)", len(stock_codes))
    logger.info("=" * 60)

    t0 = time.time()

    # ── ONLY: Shock Pipeline ─────────────────────────────────────
    logger.info("Running Shock Propagation Pipeline ...")
    shock_signals = run_shock_pipeline(
        stock_codes,
        max_events=args.max_events,
        skip_debate=args.skip_debate,
    )
    logger.info("Shock Pipeline: %d signals", len(shock_signals))

    elapsed = time.time() - t0

    # ── Consolidate & Save ────────────────────────────────────────
    rows = consolidate(shock_signals)

    counts = {
        "SHOCK": len(shock_signals),
        "total": len(rows),
    }

    csv_path = output_dir / f"alpha_factory_{date_str}.csv"
    report_path = output_dir / f"alpha_factory_{date_str}.md"

    save_csv(rows, csv_path)
    save_report(rows, report_path, elapsed, counts)

    # Print summary
    print()
    print("=" * 60)
    print(f"Alpha Factory v3 (冲击链路收敛) — {elapsed:.0f}s")
    print(f"  Shock Pipeline:  {counts['SHOCK']} signals")

    unreacted = sum(
        1 for r in rows
        if r.get("alpha_type") == "未反应(信息差)"
    )
    if unreacted:
        print(f"  信息差Alpha:     {unreacted} signals")

    print(f"  CSV:    {csv_path}")
    print(f"  Report: {report_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
