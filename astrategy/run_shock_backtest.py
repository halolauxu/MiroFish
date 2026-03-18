#!/usr/bin/env python3
"""
冲击传播链路 — 历史事件回测
============================
用历史事件数据库验证信息差 Alpha 假设：
  "事件冲击沿图谱传播，跳数越多的下游公司反应越慢，信息差越大"

回测流程:
  1. 加载 historical_events.json
  2. 对每个事件运行冲击传播（图谱 + 可选 Agent 辩论）
  3. 用事件日期后的真实行情计算前向收益
  4. 按 hop 分组统计，验证 alpha 衰减假设

用法:
    python astrategy/run_shock_backtest.py                  # 快速（无辩论）
    python astrategy/run_shock_backtest.py --with-debate     # 含Agent辩论
    python astrategy/run_shock_backtest.py --events 3        # 只跑前3个事件
"""

import argparse
import json
import logging
import math
import os
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("shock_backtest")

# Transaction cost (A-share round trip)
_TC = 0.003


def load_events(path: str, max_events: int = 0) -> List[Dict]:
    """Load historical events from JSON."""
    with open(path, "r", encoding="utf-8") as f:
        events = json.load(f)
    if max_events > 0:
        events = events[:max_events]
    logger.info("Loaded %d historical events", len(events))
    return events


def run_backtest(
    events: List[Dict],
    skip_debate: bool = True,
    forward_days: int = 5,
) -> List[Dict]:
    """Run shock pipeline on historical events."""
    from astrategy.shock_pipeline import ShockPipeline

    pipeline = ShockPipeline()
    signals = pipeline.run_historical(
        events=events,
        skip_debate=skip_debate,
        forward_days=forward_days,
    )
    return signals


def analyze_signals(signals: List[Dict]) -> Dict[str, Any]:
    """Analyze backtest signals and compute performance metrics."""
    if not signals:
        return {"error": "No signals generated"}

    # --- Overall stats ---
    total = len(signals)
    with_returns = [s for s in signals if s.get("fwd_return_5d") is not None]
    n_valid = len(with_returns)

    # --- By hop group ---
    hop_groups: Dict[int, List[Dict]] = {}
    for s in with_returns:
        hop = s.get("hop", -1)
        hop_groups.setdefault(hop, []).append(s)

    hop_stats = {}
    for hop in sorted(hop_groups.keys()):
        group = hop_groups[hop]
        returns_5d = [s["fwd_return_5d"] for s in group if s["fwd_return_5d"] is not None]
        returns_10d = [s["fwd_return_10d"] for s in group if s.get("fwd_return_10d") is not None]
        returns_20d = [s["fwd_return_20d"] for s in group if s.get("fwd_return_20d") is not None]

        # Directional accuracy: did model predict correct direction?
        hits = []
        for s in group:
            ret = s.get("fwd_return_5d", 0.0)
            direction = s.get("signal_direction", "neutral")
            if direction == "long":
                hits.append(1 if ret > 0 else 0)
            elif direction == "avoid":
                hits.append(1 if ret < 0 else 0)
            # neutral: skip

        hop_stats[hop] = {
            "count": len(group),
            "avg_return_5d": _safe_mean(returns_5d),
            "avg_return_10d": _safe_mean(returns_10d),
            "avg_return_20d": _safe_mean(returns_20d),
            "avg_abs_return_5d": _safe_mean([abs(r) for r in returns_5d]) if returns_5d else 0,
            "hit_rate": _safe_mean(hits) if hits else None,
            "reacted_pct": _safe_mean([1 if s.get("reacted") else 0 for s in group]),
        }

    # --- By event type ---
    type_groups: Dict[str, List[Dict]] = {}
    for s in with_returns:
        etype = s.get("event_type", "unknown")
        type_groups.setdefault(etype, []).append(s)

    type_stats = {}
    for etype, group in type_groups.items():
        returns_5d = [s["fwd_return_5d"] for s in group if s["fwd_return_5d"] is not None]
        type_stats[etype] = {
            "count": len(group),
            "avg_return_5d": _safe_mean(returns_5d),
            "avg_abs_return_5d": _safe_mean([abs(r) for r in returns_5d]) if returns_5d else 0,
        }

    # --- Information gap alpha: unreacted downstream vs reacted ---
    unreacted = [s for s in with_returns if not s.get("reacted") and s.get("hop", 0) > 0]
    reacted = [s for s in with_returns if s.get("reacted") and s.get("hop", 0) > 0]

    unreacted_returns = [s["fwd_return_5d"] for s in unreacted if s["fwd_return_5d"] is not None]
    reacted_returns = [s["fwd_return_5d"] for s in reacted if s["fwd_return_5d"] is not None]

    # Direction-adjusted returns (long = positive, avoid = negative is correct)
    def direction_adjusted(sig_list):
        adjusted = []
        for s in sig_list:
            ret = s.get("fwd_return_5d", 0)
            if ret is None:
                continue
            direction = s.get("signal_direction", "neutral")
            if direction == "long":
                adjusted.append(ret - _TC)
            elif direction == "avoid":
                adjusted.append(-ret - _TC)
        return adjusted

    alpha_unreacted = direction_adjusted(unreacted)
    alpha_reacted = direction_adjusted(reacted)
    alpha_all = direction_adjusted(with_returns)

    # Sharpe calculation
    def _sharpe(returns_list):
        if len(returns_list) < 2:
            return 0.0
        avg = statistics.mean(returns_list)
        std = statistics.stdev(returns_list)
        if std < 1e-9:
            return 0.0
        periods = 252.0 / 10  # ~25 round trips per year (10 day hold)
        return (avg * periods) / (std * math.sqrt(periods))

    return {
        "total_signals": total,
        "valid_signals": n_valid,
        "hop_stats": hop_stats,
        "type_stats": type_stats,
        "unreacted_count": len(unreacted),
        "reacted_count": len(reacted),
        "alpha_unreacted": {
            "count": len(alpha_unreacted),
            "avg": _safe_mean(alpha_unreacted),
            "sharpe": _sharpe(alpha_unreacted),
            "hit_rate": _safe_mean([1 if r > 0 else 0 for r in alpha_unreacted]) if alpha_unreacted else None,
        },
        "alpha_reacted": {
            "count": len(alpha_reacted),
            "avg": _safe_mean(alpha_reacted),
            "sharpe": _sharpe(alpha_reacted),
            "hit_rate": _safe_mean([1 if r > 0 else 0 for r in alpha_reacted]) if alpha_reacted else None,
        },
        "alpha_all": {
            "count": len(alpha_all),
            "avg": _safe_mean(alpha_all),
            "sharpe": _sharpe(alpha_all),
            "hit_rate": _safe_mean([1 if r > 0 else 0 for r in alpha_all]) if alpha_all else None,
        },
    }


def _safe_mean(values):
    if not values:
        return 0.0
    return round(statistics.mean(values), 4)


def generate_report(
    signals: List[Dict],
    analysis: Dict,
    events: List[Dict],
    elapsed: float,
) -> str:
    """Generate markdown backtest report."""
    lines = [
        "# 冲击传播链路 — 历史事件回测报告",
        "",
        f"**日期**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"**事件数**: {len(events)}",
        f"**总信号数**: {analysis['total_signals']}",
        f"**有效信号数(有行情)**: {analysis['valid_signals']}",
        f"**耗时**: {elapsed:.1f}s",
        "",
    ]

    # Overall Alpha
    lines.extend([
        "## Alpha 汇总",
        "",
        "| 指标 | 全部信号 | 未反应(信息差) | 已反应 |",
        "|------|---------|--------------|--------|",
    ])
    for metric in ["count", "avg", "sharpe", "hit_rate"]:
        label = {"count": "信号数", "avg": "方向调整收益", "sharpe": "Sharpe(年化)", "hit_rate": "胜率"}[metric]
        vals = []
        for key in ["alpha_all", "alpha_unreacted", "alpha_reacted"]:
            v = analysis.get(key, {}).get(metric)
            if v is None:
                vals.append("N/A")
            elif metric == "hit_rate":
                vals.append(f"{v:.1%}")
            elif metric == "count":
                vals.append(str(v))
            elif metric == "sharpe":
                vals.append(f"{v:.2f}")
            else:
                vals.append(f"{v:.4f}")
        lines.append(f"| {label} | {vals[0]} | {vals[1]} | {vals[2]} |")

    # By hop
    lines.extend([
        "",
        "## 按传播跳数分析",
        "",
        "| Hop | 信号数 | 平均5日收益 | 平均10日收益 | 平均20日收益 | |5日收益| | 已反应% | 胜率 |",
        "|-----|--------|-----------|------------|------------|----------|--------|------|",
    ])
    for hop in sorted(analysis.get("hop_stats", {}).keys()):
        s = analysis["hop_stats"][hop]
        hop_label = "源头" if hop == 0 else f"{hop}跳"
        hr = f"{s['hit_rate']:.1%}" if s.get("hit_rate") is not None else "N/A"
        lines.append(
            f"| {hop_label} | {s['count']} "
            f"| {s['avg_return_5d']:.4f} "
            f"| {s['avg_return_10d']:.4f} "
            f"| {s['avg_return_20d']:.4f} "
            f"| {s['avg_abs_return_5d']:.4f} "
            f"| {s['reacted_pct']:.1%} "
            f"| {hr} |"
        )

    # By event type
    lines.extend([
        "",
        "## 按事件类型分析",
        "",
        "| 事件类型 | 信号数 | 平均5日收益 | |5日收益| |",
        "|---------|--------|-----------|----------|",
    ])
    for etype, s in sorted(analysis.get("type_stats", {}).items()):
        lines.append(
            f"| {etype} | {s['count']} "
            f"| {s['avg_return_5d']:.4f} "
            f"| {s['avg_abs_return_5d']:.4f} |"
        )

    # Per-signal detail
    lines.extend([
        "",
        "## 信号明细",
        "",
        "| 事件ID | 事件 | 目标 | Hop | 方向 | 冲击权重 | 已反应 | 5D | 10D | 20D |",
        "|--------|------|------|-----|------|---------|--------|-----|-----|-----|",
    ])
    for s in sorted(signals, key=lambda x: (x.get("event_id", ""), x.get("hop", 99))):
        r5 = s.get("fwd_return_5d")
        r10 = s.get("fwd_return_10d")
        r20 = s.get("fwd_return_20d")
        lines.append(
            f"| {s.get('event_id', '')[:6]} "
            f"| {s.get('source_event', '')[:15]} "
            f"| {s.get('target_name', '')}({s.get('target_code', '')}) "
            f"| {s.get('hop', '')} "
            f"| {s.get('signal_direction', '')} "
            f"| {s.get('shock_weight', 0):.2f} "
            f"| {'Y' if s.get('reacted') else 'N'} "
            f"| {r5:+.2%}" if r5 is not None else "| N/A",
        )
        # Append 10D and 20D on same line
        if isinstance(lines[-1], str) and lines[-1].startswith("|"):
            r10_str = f"{r10:+.2%}" if r10 is not None else "N/A"
            r20_str = f"{r20:+.2%}" if r20 is not None else "N/A"
            lines[-1] += f" | {r10_str} | {r20_str} |"

    # Conclusion
    lines.extend([
        "",
        "## 核心结论",
        "",
    ])

    alpha_unreacted = analysis.get("alpha_unreacted", {})
    alpha_reacted = analysis.get("alpha_reacted", {})
    alpha_all = analysis.get("alpha_all", {})

    if alpha_unreacted.get("avg", 0) > alpha_reacted.get("avg", 0):
        lines.append("1. **信息差 Alpha 假设成立**: 未反应下游信号的方向调整收益"
                     f"({alpha_unreacted.get('avg', 0):.4f}) > "
                     f"已反应信号({alpha_reacted.get('avg', 0):.4f})")
    else:
        lines.append("1. **信息差 Alpha 假设待验证**: 未反应/已反应信号收益差异不显著")

    hop_stats = analysis.get("hop_stats", {})
    if 0 in hop_stats and 2 in hop_stats:
        src_abs = hop_stats[0].get("avg_abs_return_5d", 0)
        hop2_abs = hop_stats[2].get("avg_abs_return_5d", 0)
        lines.append(f"2. **传播衰减**: 源头平均|收益|={src_abs:.4f}, "
                     f"2跳平均|收益|={hop2_abs:.4f}, "
                     f"衰减比={hop2_abs/src_abs:.2f}" if src_abs > 0 else
                     f"2. 源头数据不足")

    if alpha_all.get("sharpe", 0) > 0:
        lines.append(f"3. **整体 Sharpe = {alpha_all.get('sharpe', 0):.2f}** "
                     f"(含TC 0.3%)")
    else:
        lines.append(f"3. 整体 Sharpe = {alpha_all.get('sharpe', 0):.2f} (需改进)")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="冲击传播链路 — 历史事件回测")
    parser.add_argument("--events", type=int, default=0, help="限制事件数(0=全部)")
    parser.add_argument("--with-debate", action="store_true", help="包含Agent辩论(慢)")
    parser.add_argument("--forward-days", type=int, default=5, help="前向观察天数")
    parser.add_argument(
        "--events-file", type=str, default="",
        help="事件文件路径(默认=.data/historical_events.json)",
    )
    args = parser.parse_args()

    # Resolve paths
    data_dir = Path(__file__).resolve().parent / ".data"
    events_path = args.events_file or str(data_dir / "historical_events.json")
    report_dir = data_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    # Load events
    events = load_events(events_path, args.events)

    # Run backtest
    t0 = time.time()
    skip_debate = not args.with_debate
    signals = run_backtest(
        events,
        skip_debate=skip_debate,
        forward_days=args.forward_days,
    )
    elapsed = time.time() - t0

    # Analyze
    analysis = analyze_signals(signals)

    # Generate report
    report = generate_report(signals, analysis, events, elapsed)

    # Save
    date_str = datetime.now().strftime("%Y%m%d")
    report_path = report_dir / f"shock_backtest_{date_str}.md"
    report_path.write_text(report, encoding="utf-8")

    signals_path = report_dir / f"shock_backtest_signals_{date_str}.json"
    signals_path.write_text(
        json.dumps(signals, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    # Console summary
    print()
    print("=" * 70)
    print("冲击传播链路 — 历史事件回测结果")
    print("=" * 70)
    print(f"  事件数: {len(events)}")
    print(f"  总信号: {analysis['total_signals']}")
    print(f"  有效信号: {analysis['valid_signals']}")
    print(f"  耗时: {elapsed:.1f}s")
    print()
    print("  Alpha 对比:")
    for key, label in [("alpha_all", "全部"), ("alpha_unreacted", "未反应"), ("alpha_reacted", "已反应")]:
        a = analysis.get(key, {})
        hr = f"{a.get('hit_rate', 0):.1%}" if a.get("hit_rate") is not None else "N/A"
        print(
            f"    {label}: 方向调整收益={a.get('avg', 0):+.4f} "
            f"Sharpe={a.get('sharpe', 0):.2f} "
            f"胜率={hr} "
            f"(n={a.get('count', 0)})"
        )

    print()
    print("  按Hop分析:")
    for hop in sorted(analysis.get("hop_stats", {}).keys()):
        s = analysis["hop_stats"][hop]
        label = "源头" if hop == 0 else f"{hop}跳"
        hr = f"{s['hit_rate']:.1%}" if s.get("hit_rate") is not None else "N/A"
        print(
            f"    {label}: 5D={s['avg_return_5d']:+.4f} "
            f"10D={s['avg_return_10d']:+.4f} "
            f"20D={s['avg_return_20d']:+.4f} "
            f"|5D|={s['avg_abs_return_5d']:.4f} "
            f"已反应={s['reacted_pct']:.0%} "
            f"胜率={hr} (n={s['count']})"
        )

    print()
    print(f"  报告: {report_path}")
    print(f"  信号: {signals_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
