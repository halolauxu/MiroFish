"""
Alpha Factory — 冲击传播链路 + 三线合一信号生产
================================================

Architecture (v2):
    PRIMARY: Shock Pipeline (图谱传播 + Agent辩论 + 信息差检测)
        事件 → 图谱找下游 → Agent辩论影响 → 检查是否已反应 → Alpha
    SECONDARY: S07 Graph Factors (传统多因子辅助)
    TERTIARY:  S10 Direct Sentiment (直接舆情模拟)

The Shock Pipeline is the MAIN alpha source.  S07 and S10 provide
supplementary signals that are used for cross-validation and conviction
boosting when they agree with the shock pipeline.

Usage:
    python -m astrategy.alpha_factory [--stocks 600519,000858,...] [--top-n 10]
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
# Individual line runners
# ---------------------------------------------------------------------------


def run_shock_pipeline(
    stock_codes: list[str],
    max_events: int = 5,
    skip_debate: bool = False,
) -> list[StrategySignal]:
    """PRIMARY: Shock Propagation Pipeline (图谱 + Agent辩论 + 信息差)."""
    from astrategy.shock_pipeline import ShockPipeline
    pipeline = ShockPipeline()
    shock_signals = pipeline.run(
        stock_codes,
        max_events=max_events,
        skip_debate=skip_debate,
    )
    return pipeline.to_strategy_signals(shock_signals)


def run_line_a(stock_codes: list[str], top_n: int = 10) -> list[StrategySignal]:
    """SECONDARY: S07 Graph-Enhanced Multi-Factor."""
    from astrategy.strategies.s07_graph_factors import GraphFactorsStrategy
    strategy = GraphFactorsStrategy(top_n=top_n)
    return strategy.run(stock_codes)


def run_line_c(stock_codes: list[str]) -> list[StrategySignal]:
    """TERTIARY: S10 Direct Sentiment Simulation."""
    from astrategy.strategies.s10_sentiment_simulation import SentimentSimulationStrategy
    strategy = SentimentSimulationStrategy()
    signals: list[StrategySignal] = []
    for code in stock_codes[:10]:  # limit LLM calls
        try:
            sigs = strategy.run_single(code)
            signals.extend(sigs)
        except Exception as exc:
            logger.warning("S10 failed for %s: %s", code, str(exc)[:80])
    return signals


# ---------------------------------------------------------------------------
# Cross-line consensus boosting
# ---------------------------------------------------------------------------


def boost_consensus(
    primary: list[StrategySignal],
    secondary: list[StrategySignal],
    tertiary: list[StrategySignal],
) -> list[StrategySignal]:
    """Boost confidence of primary signals when secondary/tertiary agree.

    If S07 or S10 independently agree with a shock pipeline signal on
    the same stock and direction, boost the shock signal's confidence by
    0.1 per agreeing line (max +0.2).
    """
    # Build lookup: stock_code -> direction for secondary/tertiary
    sec_directions: Dict[str, str] = {}
    for s in secondary:
        if s.direction in ("long", "avoid"):
            sec_directions[s.stock_code] = s.direction

    ter_directions: Dict[str, str] = {}
    for s in tertiary:
        if s.direction in ("long", "avoid"):
            ter_directions[s.stock_code] = s.direction

    boosted: list[StrategySignal] = []
    for sig in primary:
        boost = 0.0
        cross_sources: list[str] = []
        if sec_directions.get(sig.stock_code) == sig.direction:
            boost += 0.1
            cross_sources.append("S07图谱因子")
        if ter_directions.get(sig.stock_code) == sig.direction:
            boost += 0.1
            cross_sources.append("S10舆情模拟")

        if boost > 0:
            sig.confidence = min(1.0, sig.confidence + boost)
            sig.reasoning += f" [交叉验证+{boost:.1f}: {','.join(cross_sources)}]"
            sig.metadata["cross_validated"] = True
            sig.metadata["cross_sources"] = cross_sources
            logger.info(
                "Cross-validated: %s(%s) +%.1f from %s",
                sig.stock_name, sig.stock_code, boost, ",".join(cross_sources),
            )

        boosted.append(sig)

    return boosted


# ---------------------------------------------------------------------------
# Consolidation
# ---------------------------------------------------------------------------


def consolidate(
    shock_signals: list[StrategySignal],
    factor_signals: list[StrategySignal],
    sentiment_signals: list[StrategySignal],
) -> list[dict]:
    """Merge signals from all lines into a flat list of dicts."""
    rows: list[dict] = []

    def _to_row(sig: StrategySignal, line: str) -> dict:
        meta = sig.metadata or {}
        return {
            "alpha_line": line,
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
            "cross_validated": meta.get("cross_validated", False),
            "reasoning": sig.reasoning[:200],
        }

    for s in shock_signals:
        rows.append(_to_row(s, "SHOCK_PRIMARY"))
    for s in factor_signals:
        rows.append(_to_row(s, "FACTOR_SECONDARY"))
    for s in sentiment_signals:
        rows.append(_to_row(s, "SENTIMENT_TERTIARY"))

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
        f"# Alpha Factory Report (v2 冲击链路) — {now}",
        "",
        f"Runtime: {elapsed:.0f}s",
        "",
        "## Architecture",
        "- **PRIMARY**: 冲击传播链路 (事件→图谱传播→Agent辩论→信息差检测)",
        "- **SECONDARY**: S07 图谱多因子 (传统因子辅助)",
        "- **TERTIARY**: S10 舆情模拟 (直接情绪信号)",
        "",
        "## Signal Summary",
        "",
        "| Alpha Line | Signals | Long | Avoid | Neutral | Cross-Validated |",
        "|------------|---------|------|-------|---------|-----------------|",
    ]

    for line_name in ["SHOCK_PRIMARY", "FACTOR_SECONDARY", "SENTIMENT_TERTIARY"]:
        line_rows = [r for r in rows if r["alpha_line"] == line_name]
        total = len(line_rows)
        longs = sum(1 for r in line_rows if r["direction"] == "long")
        avoids = sum(1 for r in line_rows if r["direction"] in ("short", "avoid"))
        neutrals = total - longs - avoids
        cross = sum(1 for r in line_rows if r.get("cross_validated"))
        lines.append(
            f"| {line_name} | {total} | {longs} | {avoids} | {neutrals} | {cross} |"
        )

    lines.append(f"| **Total** | **{len(rows)}** | | | | |")
    lines.append("")

    # Shock pipeline signals (primary — most important)
    shock_rows = [r for r in rows if r["alpha_line"] == "SHOCK_PRIMARY"]
    if shock_rows:
        lines.append("## 🔥 冲击传播信号 (Primary)")
        lines.append("")
        lines.append("| Code | Name | Direction | Confidence | Shock | Divergence | Alpha Type | Reasoning |")
        lines.append("|------|------|-----------|------------|-------|------------|------------|-----------|")
        for r in sorted(shock_rows, key=lambda r: r["confidence"], reverse=True)[:20]:
            lines.append(
                f"| {r['stock_code']} | {r['stock_name']} | {r['direction']} | "
                f"{r['confidence']:.2f} | {r.get('shock_weight', '')} | "
                f"{r.get('divergence', '')} | {r.get('alpha_type', '')} | "
                f"{r['reasoning'][:80]} |"
            )
        lines.append("")

    # Cross-line consensus
    code_counts = Counter(r["stock_code"] for r in rows if r["direction"] in ("long", "avoid"))
    consensus = [(code, cnt) for code, cnt in code_counts.items() if cnt >= 2]
    if consensus:
        lines.append("## 🎯 Cross-Line Consensus (2+ lines agree)")
        lines.append("")
        for code, cnt in sorted(consensus, key=lambda x: -x[1]):
            matching = [r for r in rows if r["stock_code"] == code]
            dirs = set(r["direction"] for r in matching)
            src_lines = ", ".join(set(r["alpha_line"] for r in matching))
            name = matching[0]["stock_name"]
            lines.append(
                f"- **{name}**({code}): {cnt} signals from {src_lines}, direction={dirs}"
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
        description="Alpha Factory v2 — 冲击传播链路 + 三线合一",
    )
    parser.add_argument(
        "--stocks", type=str, default="",
        help="逗号分隔的股票代码，留空=使用CSI-300前30",
    )
    parser.add_argument("--top-n", type=int, default=10, help="S07 top/bottom N")
    parser.add_argument("--max-events", type=int, default=5, help="冲击链路最大事件数")
    parser.add_argument(
        "--skip-debate", action="store_true",
        help="跳过Agent辩论（快速测试模式）",
    )
    parser.add_argument(
        "--shock-only", action="store_true",
        help="只跑冲击链路（跳过S07和S10）",
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
    logger.info("Alpha Factory v2 — %d stocks (shock_only=%s)", len(stock_codes), args.shock_only)
    logger.info("=" * 60)

    t0 = time.time()

    # ── PRIMARY: Shock Pipeline ───────────────────────────────────
    logger.info("PRIMARY: Shock Propagation Pipeline ...")
    shock_signals = run_shock_pipeline(
        stock_codes,
        max_events=args.max_events,
        skip_debate=args.skip_debate,
    )
    logger.info("PRIMARY: %d shock signals", len(shock_signals))

    # ── SECONDARY: S07 (optional) ────────────────────────────────
    factor_signals: list[StrategySignal] = []
    if not args.shock_only:
        logger.info("SECONDARY: S07 Graph Factors ...")
        try:
            factor_signals = run_line_a(stock_codes, top_n=args.top_n)
            logger.info("SECONDARY: %d factor signals", len(factor_signals))
        except Exception as exc:
            logger.warning("SECONDARY (S07) failed: %s", exc)

    # ── TERTIARY: S10 (optional) ─────────────────────────────────
    sentiment_signals: list[StrategySignal] = []
    if not args.shock_only:
        logger.info("TERTIARY: S10 Sentiment Simulation ...")
        try:
            sentiment_signals = run_line_c(stock_codes)
            logger.info("TERTIARY: %d sentiment signals", len(sentiment_signals))
        except Exception as exc:
            logger.warning("TERTIARY (S10) failed: %s", exc)

    # ── Cross-validation boosting ────────────────────────────────
    if factor_signals or sentiment_signals:
        shock_signals = boost_consensus(shock_signals, factor_signals, sentiment_signals)

    elapsed = time.time() - t0

    # ── Consolidate & Save ────────────────────────────────────────
    rows = consolidate(shock_signals, factor_signals, sentiment_signals)

    counts = {
        "SHOCK": len(shock_signals),
        "FACTOR": len(factor_signals),
        "SENTIMENT": len(sentiment_signals),
        "total": len(rows),
    }

    csv_path = output_dir / f"alpha_factory_{date_str}.csv"
    report_path = output_dir / f"alpha_factory_{date_str}.md"

    save_csv(rows, csv_path)
    save_report(rows, report_path, elapsed, counts)

    # Print summary
    print()
    print("=" * 60)
    print(f"Alpha Factory v2 Complete — {elapsed:.0f}s")
    print(f"  PRIMARY  (Shock Pipeline):  {counts['SHOCK']} signals")
    print(f"  SECONDARY (S07 Factors):    {counts['FACTOR']} signals")
    print(f"  TERTIARY  (S10 Sentiment):  {counts['SENTIMENT']} signals")
    print(f"  Total:                      {counts['total']} signals")

    cross_count = sum(1 for r in rows if r.get("cross_validated"))
    if cross_count:
        print(f"  Cross-validated:            {cross_count} signals")

    unreacted = sum(
        1 for r in rows
        if r.get("alpha_type") == "未反应(信息差)"
    )
    if unreacted:
        print(f"  ⚡ 信息差Alpha:              {unreacted} signals")

    print(f"  CSV:    {csv_path}")
    print(f"  Report: {report_path}")
    print("=" * 60)


if __name__ == "__main__":
    main()
