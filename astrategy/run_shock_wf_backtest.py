#!/usr/bin/env python3
"""
冲击传播链路 — 严格 Walk-Forward 回测
==========================================
严格验证信号质量：
  - IS/OOS 时间切分（按事件日期排序，前70%训练/后30%测试）
  - 方向调整 PnL（long: +return, avoid: -return）
  - 多持仓期（5D/10D/20D）
  - 市场基准对比（沪深300超额收益）
  - 信号分层分析（hop/event_type/reacted）
  - IC/IR 计算
  - 涨跌停与交易成本处理

用法:
    python astrategy/run_shock_wf_backtest.py
    python astrategy/run_shock_wf_backtest.py --with-debate
    python astrategy/run_shock_wf_backtest.py --is-ratio 0.6
"""

import argparse
import json
import logging
import math
import os
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("shock_wf_backtest")

# ── Constants ──────────────────────────────────────────────
_TC = 0.003           # A股往返交易成本
_LIMIT_UP = 0.10      # 涨停板（主板10%）
_LIMIT_DN = -0.10     # 跌停板
_BENCHMARK = "000300"  # 沪深300


# ── Market Data Helper ─────────────────────────────────────

def _fetch_prices(code: str, start: str, end: str) -> pd.DataFrame:
    """Fetch daily prices. Returns DataFrame with date/close/volume cols."""
    from astrategy.data_collector.market_data import MarketDataCollector
    market = MarketDataCollector()
    df = market.get_daily_quotes(code, start, end)
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "close", "volume"])
    out = pd.DataFrame()
    date_col = "日期" if "日期" in df.columns else "date"
    close_col = "收盘" if "收盘" in df.columns else "close"
    vol_col = "成交量" if "成交量" in df.columns else "volume"
    out["date"] = pd.to_datetime(df[date_col])
    out["close"] = df[close_col].astype(float)
    if vol_col in df.columns:
        out["volume"] = df[vol_col].astype(float)
    else:
        out["volume"] = 0.0
    return out.reset_index(drop=True)


_benchmark_cache: Optional[pd.DataFrame] = None

def _load_benchmark() -> pd.DataFrame:
    """Load CSI300 index data via akshare (Sina source)."""
    global _benchmark_cache
    if _benchmark_cache is not None:
        return _benchmark_cache
    try:
        import akshare as ak
        df = ak.stock_zh_index_daily(symbol="sh000300")
        if df is not None and not df.empty:
            df["date"] = pd.to_datetime(df["date"])
            _benchmark_cache = df
            return df
    except Exception as exc:
        logger.warning("Failed to load CSI300 benchmark: %s", exc)
    return pd.DataFrame(columns=["date", "close"])


def _get_benchmark_return(event_date: str, horizon_days: int) -> Optional[float]:
    """Get benchmark (CSI300) return from event_date forward."""
    try:
        df = _load_benchmark()
        if df.empty:
            return None
        evt_pd = pd.to_datetime(event_date)
        after = df[df["date"] >= evt_pd].reset_index(drop=True)
        if after.empty or len(after) <= horizon_days:
            return None
        entry = float(after.iloc[0]["close"])
        exit_ = float(after.iloc[min(horizon_days, len(after) - 1)]["close"])
        if entry == 0:
            return None
        return round(exit_ / entry - 1.0, 4)
    except Exception:
        return None


# ── Signal Quality Metrics ─────────────────────────────────

def direction_adjusted_return(signal: Dict, horizon: str = "fwd_return_5d") -> Optional[float]:
    """Compute direction-adjusted return (long=+, avoid=-)."""
    raw = signal.get(horizon)
    if raw is None:
        return None
    direction = signal.get("signal_direction", "neutral")
    if direction == "long":
        return raw - _TC
    elif direction == "avoid":
        return -raw - _TC
    return None


def compute_sharpe(returns: List[float], holding_days: int = 10) -> float:
    """Annualized Sharpe ratio."""
    if len(returns) < 2:
        return 0.0
    avg = statistics.mean(returns)
    std = statistics.stdev(returns)
    if std < 1e-9:
        return 0.0 if avg <= 0 else 99.0
    periods = 252.0 / max(holding_days, 1)
    return round((avg * periods) / (std * math.sqrt(periods)), 2)


def compute_ic(signals: List[Dict], horizon: str = "fwd_return_5d") -> float:
    """Spearman rank IC between confidence and actual return."""
    pairs = [
        (s.get("confidence", 0), s.get(horizon, 0))
        for s in signals
        if s.get(horizon) is not None and s.get("confidence") is not None
    ]
    if len(pairs) < 5:
        return 0.0
    try:
        df = pd.DataFrame(pairs, columns=["conf", "ret"])
        return round(float(df["conf"].corr(df["ret"], method="spearman")), 4)
    except Exception:
        return 0.0


def compute_max_drawdown(returns: List[float]) -> float:
    """Sequential max drawdown."""
    if not returns:
        return 0.0
    cum = 1.0
    peak = 1.0
    max_dd = 0.0
    for r in returns:
        cum *= (1.0 + r)
        peak = max(peak, cum)
        dd = (peak - cum) / peak
        max_dd = max(max_dd, dd)
    return round(max_dd, 4)


def compute_metrics_bundle(signals: List[Dict], horizon: str = "fwd_return_5d",
                           holding_days: int = 5) -> Dict[str, Any]:
    """Full metrics for a set of signals."""
    adj_returns = [direction_adjusted_return(s, horizon) for s in signals]
    adj_returns = [r for r in adj_returns if r is not None]

    raw_returns = [s.get(horizon) for s in signals if s.get(horizon) is not None]

    hits = []
    for s in signals:
        r = s.get(horizon)
        if r is None:
            continue
        d = s.get("signal_direction", "neutral")
        if d == "long":
            hits.append(1 if r > 0 else 0)
        elif d == "avoid":
            hits.append(1 if r < 0 else 0)

    n = len(adj_returns)
    if n == 0:
        return {"n": 0, "sharpe": 0, "hit_rate": 0, "avg_adj_return": 0,
                "avg_raw_return": 0, "max_dd": 0, "ic": 0, "profit_factor": 0}

    wins = [r for r in adj_returns if r > 0]
    losses = [r for r in adj_returns if r < 0]
    pf = sum(wins) / max(abs(sum(losses)), 1e-9) if losses else (99.0 if wins else 0.0)

    return {
        "n": n,
        "sharpe": compute_sharpe(adj_returns, holding_days),
        "hit_rate": round(sum(hits) / len(hits), 4) if hits else 0.0,
        "avg_adj_return": round(statistics.mean(adj_returns), 4),
        "avg_raw_return": round(statistics.mean(raw_returns), 4) if raw_returns else 0.0,
        "max_dd": compute_max_drawdown(adj_returns),
        "ic": compute_ic(signals, horizon),
        "profit_factor": round(min(pf, 99.0), 2),
    }


# ── Walk-Forward Engine ─────────────────────────────────────

def run_walk_forward(
    events: List[Dict],
    is_ratio: float = 0.7,
    skip_debate: bool = True,
    forward_days: int = 5,
) -> Dict[str, Any]:
    """Run Walk-Forward backtest.

    1. Sort events by date
    2. Split into IS (first is_ratio) and OOS (remainder)
    3. Run shock pipeline on both sets
    4. Compute metrics separately and compare
    """
    from astrategy.shock_pipeline import ShockPipeline

    # Sort by date
    events_sorted = sorted(events, key=lambda e: e.get("event_date", ""))
    n_total = len(events_sorted)
    n_is = max(1, int(n_total * is_ratio))

    is_events = events_sorted[:n_is]
    oos_events = events_sorted[n_is:]

    logger.info("Walk-Forward split: IS=%d events, OOS=%d events", len(is_events), len(oos_events))
    logger.info("IS date range: %s ~ %s",
                is_events[0].get("event_date", "?"),
                is_events[-1].get("event_date", "?"))
    if oos_events:
        logger.info("OOS date range: %s ~ %s",
                     oos_events[0].get("event_date", "?"),
                     oos_events[-1].get("event_date", "?"))

    pipeline = ShockPipeline()

    # Run IS
    t0 = time.time()
    is_signals = pipeline.run_historical(
        events=is_events, skip_debate=skip_debate, forward_days=forward_days,
    )
    is_time = time.time() - t0

    # Run OOS
    t1 = time.time()
    oos_signals = pipeline.run_historical(
        events=oos_events, skip_debate=skip_debate, forward_days=forward_days,
    ) if oos_events else []
    oos_time = time.time() - t1

    all_signals = is_signals + oos_signals

    # Add benchmark returns
    _enrich_with_benchmark(all_signals)

    return {
        "is_events": is_events,
        "oos_events": oos_events,
        "is_signals": is_signals,
        "oos_signals": oos_signals,
        "all_signals": all_signals,
        "is_time": is_time,
        "oos_time": oos_time,
    }


def _enrich_with_benchmark(signals: List[Dict]) -> None:
    """Add benchmark return to each signal for excess return calculation."""
    cache: Dict[Tuple[str, int], Optional[float]] = {}
    for s in signals:
        ed = s.get("event_date", "")
        if not ed:
            continue
        for h in [5, 10, 20]:
            key = (ed, h)
            if key not in cache:
                cache[key] = _get_benchmark_return(ed, h)
            bm = cache[key]
            s[f"benchmark_{h}d"] = bm
            raw = s.get(f"fwd_return_{h}d")
            if raw is not None and bm is not None:
                s[f"excess_{h}d"] = round(raw - bm, 4)
            else:
                s[f"excess_{h}d"] = None


# ── Analysis Functions ──────────────────────────────────────

def analyze_by_slice(signals: List[Dict], slice_key: str,
                     horizon: str = "fwd_return_5d") -> Dict[str, Dict]:
    """Group signals by a key and compute metrics per group."""
    groups: Dict[str, List[Dict]] = defaultdict(list)
    for s in signals:
        val = s.get(slice_key, "unknown")
        if isinstance(val, bool):
            val = "True" if val else "False"
        groups[str(val)].append(s)

    result = {}
    for key in sorted(groups.keys()):
        result[key] = compute_metrics_bundle(groups[key], horizon)
    return result


def generate_wf_report(
    wf: Dict,
    analysis: Dict,
    elapsed: float,
) -> str:
    """Generate comprehensive Walk-Forward report."""
    is_signals = wf["is_signals"]
    oos_signals = wf["oos_signals"]
    all_signals = wf["all_signals"]

    lines = [
        "# 冲击传播链路 — Walk-Forward 严格回测报告",
        "",
        f"**日期**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"**事件数**: IS={len(wf['is_events'])}, OOS={len(wf['oos_events'])}",
        f"**信号数**: IS={len(is_signals)}, OOS={len(oos_signals)}, ALL={len(all_signals)}",
        f"**耗时**: {elapsed:.1f}s",
        "",
    ]

    # IS vs OOS comparison
    lines.extend([
        "## IS vs OOS 对比",
        "",
        "| 指标 | IS (训练集) | OOS (测试集) | 全量 |",
        "|------|-----------|------------|------|",
    ])

    for horizon_label, horizon, hd in [("5D", "fwd_return_5d", 5),
                                        ("10D", "fwd_return_10d", 10),
                                        ("20D", "fwd_return_20d", 20)]:
        is_m = compute_metrics_bundle(is_signals, horizon, hd)
        oos_m = compute_metrics_bundle(oos_signals, horizon, hd) if oos_signals else {"n": 0, "sharpe": 0, "hit_rate": 0, "avg_adj_return": 0}
        all_m = compute_metrics_bundle(all_signals, horizon, hd)

        lines.append(f"| **{horizon_label} Sharpe** | {is_m['sharpe']:.2f} | {oos_m['sharpe']:.2f} | {all_m['sharpe']:.2f} |")
        lines.append(f"| {horizon_label} 胜率 | {is_m['hit_rate']:.1%} | {oos_m['hit_rate']:.1%} | {all_m['hit_rate']:.1%} |")
        lines.append(f"| {horizon_label} 方向调整收益 | {is_m['avg_adj_return']:+.4f} | {oos_m['avg_adj_return']:+.4f} | {all_m['avg_adj_return']:+.4f} |")
        lines.append(f"| {horizon_label} IC | {is_m['ic']:.4f} | {oos_m['ic']:.4f} | {all_m['ic']:.4f} |")
        lines.append(f"| {horizon_label} MaxDD | {is_m['max_dd']:.4f} | {oos_m['max_dd']:.4f} | {all_m['max_dd']:.4f} |")
        lines.append(f"| {horizon_label} 盈亏比 | {is_m['profit_factor']:.2f} | {oos_m['profit_factor']:.2f} | {all_m['profit_factor']:.2f} |")
        lines.append(f"| {horizon_label} n | {is_m['n']} | {oos_m['n']} | {all_m['n']} |")
        lines.append("| | | | |")

    # By hop
    lines.extend([
        "",
        "## 按 Hop 分层 (全量, 5D)",
        "",
    ])
    hop_analysis = analyze_by_slice(all_signals, "hop")
    lines.append("| Hop | n | Sharpe | 胜率 | 调整收益 | IC | MaxDD |")
    lines.append("|-----|---|--------|------|---------|-----|-------|")
    for hop, m in hop_analysis.items():
        lines.append(f"| {hop} | {m['n']} | {m['sharpe']:.2f} | {m['hit_rate']:.1%} | {m['avg_adj_return']:+.4f} | {m['ic']:.4f} | {m['max_dd']:.4f} |")

    # By event type
    lines.extend([
        "",
        "## 按事件类型分层 (全量, 5D)",
        "",
    ])
    type_analysis = analyze_by_slice(all_signals, "event_type")
    lines.append("| 事件类型 | n | Sharpe | 胜率 | 调整收益 |")
    lines.append("|---------|---|--------|------|---------|")
    for etype, m in type_analysis.items():
        lines.append(f"| {etype} | {m['n']} | {m['sharpe']:.2f} | {m['hit_rate']:.1%} | {m['avg_adj_return']:+.4f} |")

    # By reacted
    lines.extend([
        "",
        "## 信息差分析: 未反应 vs 已反应 (全量, 5D)",
        "",
    ])
    react_analysis = analyze_by_slice(all_signals, "reacted")
    lines.append("| 状态 | n | Sharpe | 胜率 | 调整收益 | IC |")
    lines.append("|------|---|--------|------|---------|-----|")
    for key, m in react_analysis.items():
        label = "已反应" if key == "True" else "未反应"
        lines.append(f"| {label} | {m['n']} | {m['sharpe']:.2f} | {m['hit_rate']:.1%} | {m['avg_adj_return']:+.4f} | {m['ic']:.4f} |")

    # Excess returns (vs benchmark)
    lines.extend([
        "",
        "## 超额收益 (vs 沪深300)",
        "",
    ])
    excess_5d = [s.get("excess_5d") for s in all_signals if s.get("excess_5d") is not None]
    excess_10d = [s.get("excess_10d") for s in all_signals if s.get("excess_10d") is not None]
    if excess_5d:
        lines.append(f"- 5D 平均超额: {statistics.mean(excess_5d):+.4f} (n={len(excess_5d)})")
    if excess_10d:
        lines.append(f"- 10D 平均超额: {statistics.mean(excess_10d):+.4f} (n={len(excess_10d)})")

    # Downstream only (hop > 0)
    downstream = [s for s in all_signals if s.get("hop", 0) > 0]
    if downstream:
        lines.extend([
            "",
            "## 仅下游信号 (hop > 0)",
            "",
        ])
        for hl, hz, hd in [("5D", "fwd_return_5d", 5), ("10D", "fwd_return_10d", 10)]:
            dm = compute_metrics_bundle(downstream, hz, hd)
            lines.append(f"- {hl}: Sharpe={dm['sharpe']:.2f}, 胜率={dm['hit_rate']:.1%}, n={dm['n']}, 调整收益={dm['avg_adj_return']:+.4f}")

    # Conclusion
    lines.extend([
        "",
        "## 核心结论",
        "",
    ])

    all_5d = compute_metrics_bundle(all_signals, "fwd_return_5d", 5)
    oos_5d = compute_metrics_bundle(oos_signals, "fwd_return_5d", 5) if oos_signals else all_5d

    if oos_5d["sharpe"] > 0 and oos_5d["hit_rate"] > 0.5:
        lines.append(f"1. **OOS 验证通过**: Sharpe={oos_5d['sharpe']:.2f}, 胜率={oos_5d['hit_rate']:.1%}")
    else:
        lines.append(f"1. **OOS 验证未通过**: Sharpe={oos_5d['sharpe']:.2f}, 胜率={oos_5d['hit_rate']:.1%}")

    if all_5d["ic"] > 0.05:
        lines.append(f"2. **IC 正相关**: {all_5d['ic']:.4f} — 信心度有预测力")
    elif all_5d["ic"] < -0.05:
        lines.append(f"2. **IC 负相关**: {all_5d['ic']:.4f} — 信心度需反转")
    else:
        lines.append(f"2. IC 接近零: {all_5d['ic']:.4f} — 信心度无区分度")

    downstream_5d = compute_metrics_bundle(downstream, "fwd_return_5d", 5) if downstream else None
    if downstream_5d and downstream_5d["sharpe"] > all_5d["sharpe"]:
        lines.append(f"3. **下游信号优于全量**: 下游 Sharpe={downstream_5d['sharpe']:.2f} > 全量 {all_5d['sharpe']:.2f}")

    return "\n".join(lines)


# ── Main ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="冲击传播链路 — Walk-Forward 严格回测")
    parser.add_argument("--events-file", type=str, default="")
    parser.add_argument("--is-ratio", type=float, default=0.7, help="IS 占比")
    parser.add_argument("--with-debate", action="store_true")
    parser.add_argument("--forward-days", type=int, default=5)
    parser.add_argument("--events", type=int, default=0, help="限制事件数")
    args = parser.parse_args()

    data_dir = Path(__file__).resolve().parent / ".data"
    events_path = args.events_file or str(data_dir / "historical_events.json")
    report_dir = data_dir / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    # Load events
    with open(events_path, "r", encoding="utf-8") as f:
        events = json.load(f)
    if args.events > 0:
        events = events[:args.events]
    logger.info("Loaded %d historical events", len(events))

    # Run Walk-Forward
    t0 = time.time()
    wf = run_walk_forward(
        events,
        is_ratio=args.is_ratio,
        skip_debate=not args.with_debate,
        forward_days=args.forward_days,
    )
    elapsed = time.time() - t0

    # Analysis
    analysis = {
        "all_5d": compute_metrics_bundle(wf["all_signals"], "fwd_return_5d", 5),
        "all_10d": compute_metrics_bundle(wf["all_signals"], "fwd_return_10d", 10),
        "is_5d": compute_metrics_bundle(wf["is_signals"], "fwd_return_5d", 5),
        "oos_5d": compute_metrics_bundle(wf["oos_signals"], "fwd_return_5d", 5) if wf["oos_signals"] else {},
    }

    # Report
    report = generate_wf_report(wf, analysis, elapsed)
    date_str = datetime.now().strftime("%Y%m%d")
    report_path = report_dir / f"shock_wf_backtest_{date_str}.md"
    report_path.write_text(report, encoding="utf-8")

    signals_path = report_dir / f"shock_wf_signals_{date_str}.json"
    signals_path.write_text(
        json.dumps(wf["all_signals"], ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )

    # Console
    print()
    print("=" * 70)
    print("冲击传播链路 — Walk-Forward 严格回测结果")
    print("=" * 70)
    print(f"  事件数: IS={len(wf['is_events'])}, OOS={len(wf['oos_events'])}")
    print(f"  信号数: IS={len(wf['is_signals'])}, OOS={len(wf['oos_signals'])}")
    print(f"  耗时: {elapsed:.1f}s")
    print()
    print("  === 5D 信号质量 ===")
    for label, key in [("IS", "is_5d"), ("OOS", "oos_5d"), ("ALL", "all_5d")]:
        m = analysis.get(key, {})
        if not m:
            print(f"    {label}: (无数据)")
            continue
        print(
            f"    {label}: Sharpe={m.get('sharpe', 0):.2f} "
            f"胜率={m.get('hit_rate', 0):.1%} "
            f"调整收益={m.get('avg_adj_return', 0):+.4f} "
            f"IC={m.get('ic', 0):.4f} "
            f"MaxDD={m.get('max_dd', 0):.4f} "
            f"盈亏比={m.get('profit_factor', 0):.2f} "
            f"(n={m.get('n', 0)})"
        )

    # Hop breakdown
    print()
    print("  === 按 Hop (全量, 5D) ===")
    hop_slices = analyze_by_slice(wf["all_signals"], "hop")
    for hop, m in hop_slices.items():
        print(
            f"    Hop {hop}: Sharpe={m['sharpe']:.2f} "
            f"胜率={m['hit_rate']:.1%} "
            f"n={m['n']}"
        )

    # Downstream only
    downstream = [s for s in wf["all_signals"] if s.get("hop", 0) > 0]
    if downstream:
        dm = compute_metrics_bundle(downstream, "fwd_return_5d", 5)
        print()
        print(f"  === 仅下游 (hop>0) 5D ===")
        print(
            f"    Sharpe={dm['sharpe']:.2f} "
            f"胜率={dm['hit_rate']:.1%} "
            f"调整收益={dm['avg_adj_return']:+.4f} "
            f"n={dm['n']}"
        )

    print()
    print(f"  报告: {report_path}")
    print(f"  信号: {signals_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
