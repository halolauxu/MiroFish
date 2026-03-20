#!/usr/bin/env python3
"""
Shock bucket study for walk-forward signal outputs.

Focus areas from the 2026-03-20 handoff:
  - 源头 vs 下游
  - policy_risk
  - ma
  - cooperation / buyback / management_change
"""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List

import pandas as pd

_TC = 0.003
_FOCUS_TYPES = [
    "policy_risk",
    "ma",
    "cooperation",
    "buyback",
    "management_change",
]


def _default_signals_file() -> Path:
    report_dir = Path(__file__).resolve().parent.parent / ".data" / "reports"
    candidates = sorted(report_dir.glob("shock_wf_signals_*.json"))
    if candidates:
        return candidates[-1]
    backtest_candidates = sorted(report_dir.glob("shock_backtest_signals_*.json"))
    if backtest_candidates:
        return backtest_candidates[-1]
    raise FileNotFoundError("No shock signal files found under astrategy/.data/reports")


def _load_signals(path: Path) -> List[Dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("all_signals", "signals", "results", "records", "items"):
            if isinstance(payload.get(key), list):
                return payload[key]
    raise ValueError(f"Unsupported signals payload: {path}")


def _holding_days(horizon: str) -> int:
    match = re.search(r"_(\d+)d$", horizon)
    return int(match.group(1)) if match else 5


def _direction_adjusted_return(signal: Dict[str, Any], horizon: str) -> float | None:
    raw = signal.get(horizon)
    if raw is None:
        return None
    direction = signal.get("signal_direction", "neutral")
    if direction == "long":
        return float(raw) - _TC
    if direction == "avoid":
        return -float(raw) - _TC
    return None


def _compute_ic(rows: List[Dict[str, Any]], horizon: str) -> float:
    pairs = [
        (row.get("confidence"), row.get(horizon))
        for row in rows
        if row.get("confidence") is not None and row.get(horizon) is not None
    ]
    if len(pairs) < 5:
        return 0.0
    frame = pd.DataFrame(pairs, columns=["confidence", "return"])
    value = frame["confidence"].corr(frame["return"], method="spearman")
    if value is None or math.isnan(float(value)):
        return 0.0
    return round(float(value), 4)


def _compute_sharpe(returns: List[float], holding_days: int) -> float:
    if len(returns) < 2:
        return 0.0
    std = statistics.stdev(returns)
    if std < 1e-9:
        return 0.0 if statistics.mean(returns) <= 0 else 99.0
    periods = 252.0 / max(holding_days, 1)
    return round((statistics.mean(returns) * periods) / (std * math.sqrt(periods)), 2)


def _bucket_metrics(rows: List[Dict[str, Any]], horizon: str) -> Dict[str, Any]:
    valid = [row for row in rows if row.get(horizon) is not None]
    holding_days = _holding_days(horizon)
    adjusted = [_direction_adjusted_return(row, horizon) for row in valid]
    adjusted = [value for value in adjusted if value is not None]
    raw_returns = [float(row[horizon]) for row in valid if row.get(horizon) is not None]

    hits: List[int] = []
    for row in valid:
        raw = row.get(horizon)
        if raw is None:
            continue
        direction = row.get("signal_direction", "neutral")
        if direction == "long":
            hits.append(1 if float(raw) > 0 else 0)
        elif direction == "avoid":
            hits.append(1 if float(raw) < 0 else 0)

    return {
        "n": len(valid),
        "sharpe": _compute_sharpe(adjusted, holding_days),
        "hit_rate": round(sum(hits) / len(hits), 4) if hits else 0.0,
        "avg_adj_return": round(statistics.mean(adjusted), 4) if adjusted else 0.0,
        "avg_raw_return": round(statistics.mean(raw_returns), 4) if raw_returns else 0.0,
        "ic": _compute_ic(valid, horizon),
        "reacted_pct": round(sum(1 for row in valid if row.get("reacted")) / len(valid), 4) if valid else 0.0,
        "long_pct": round(sum(1 for row in valid if row.get("signal_direction") == "long") / len(valid), 4) if valid else 0.0,
        "hop0_pct": round(sum(1 for row in valid if row.get("hop") == 0) / len(valid), 4) if valid else 0.0,
    }


def _label_source(row: Dict[str, Any]) -> str:
    code = str(row.get("source_code", "")).strip()
    name = str(row.get("source_name", "")).strip()
    if code and name:
        return f"{name}({code})"
    return name or code or "unknown"


def _label_chain(row: Dict[str, Any]) -> str:
    text = str(row.get("relation_chain", "")).strip()
    return text or "SOURCE"


def _top_groups(
    rows: List[Dict[str, Any]],
    key_fn: Callable[[Dict[str, Any]], str],
    horizon: str,
    *,
    top_n: int = 6,
) -> List[Dict[str, Any]]:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[key_fn(row)].append(row)

    ranked = sorted(groups.items(), key=lambda item: (-len(item[1]), item[0]))
    output: List[Dict[str, Any]] = []
    for label, group_rows in ranked[:top_n]:
        metrics = _bucket_metrics(group_rows, horizon)
        output.append({"label": label, **metrics})
    return output


def _format_bucket_table(rows: Iterable[Dict[str, Any]]) -> List[str]:
    lines = [
        "| Bucket | n | Sharpe | 胜率 | 调整收益 | Raw收益 | IC | 已反应% | Long% | Hop0% |",
        "|--------|---|--------|------|---------|--------|----|--------|-------|-------|",
    ]
    for row in rows:
        lines.append(
            f"| {row['label']} | {row['n']} | {row['sharpe']:.2f} | {row['hit_rate']:.1%} | "
            f"{row['avg_adj_return']:+.4f} | {row['avg_raw_return']:+.4f} | {row['ic']:.4f} | "
            f"{row['reacted_pct']:.1%} | {row['long_pct']:.1%} | {row['hop0_pct']:.1%} |"
        )
    return lines


def _format_top_table(rows: Iterable[Dict[str, Any]], title: str) -> List[str]:
    lines = [
        title,
        "",
        "| 组别 | n | Sharpe | 胜率 | 调整收益 | 已反应% | Hop0% |",
        "|------|---|--------|------|---------|--------|-------|",
    ]
    for row in rows:
        lines.append(
            f"| {row['label']} | {row['n']} | {row['sharpe']:.2f} | {row['hit_rate']:.1%} | "
            f"{row['avg_adj_return']:+.4f} | {row['reacted_pct']:.1%} | {row['hop0_pct']:.1%} |"
        )
    return lines


def _build_findings(summary_rows: Dict[str, Dict[str, Any]], type_sections: Dict[str, Dict[str, Any]]) -> List[str]:
    findings: List[str] = []
    hop0 = summary_rows["源头 hop=0"]
    downstream = summary_rows["下游 hop>0"]
    if hop0["avg_adj_return"] > downstream["avg_adj_return"] + 0.015:
        findings.append(
            f"`hop=0` 明显强于下游传播，5D 调整收益 {hop0['avg_adj_return']:+.4f} vs {downstream['avg_adj_return']:+.4f}，当前 alpha 仍集中在源头事件。"
        )

    policy = type_sections["policy_risk"]["slices"]
    if policy["已反应"]["avg_adj_return"] > policy["未反应"]["avg_adj_return"] + 0.04:
        findings.append(
            f"`policy_risk` 的优势主要来自已反应后的延续，已反应桶 {policy['已反应']['avg_adj_return']:+.4f}，未反应桶 {policy['未反应']['avg_adj_return']:+.4f}。"
        )

    ma_sources = type_sections["ma"]["top_sources"]
    if len(ma_sources) >= 2 and abs(ma_sources[0]["avg_adj_return"] - ma_sources[1]["avg_adj_return"]) >= 0.015:
        findings.append(
            f"`ma` 存在明显源事件分化，头部两个 source 调整收益分别为 {ma_sources[0]['avg_adj_return']:+.4f} / {ma_sources[1]['avg_adj_return']:+.4f}，更适合按 source 精细分桶。"
        )

    for event_type in ("cooperation", "buyback", "management_change"):
        overall = type_sections[event_type]["slices"]["全量"]
        if overall["sharpe"] < -1.0 and overall["n"] >= 8:
            findings.append(
                f"`{event_type}` 当前样本明显偏弱，Sharpe={overall['sharpe']:.2f}、胜率={overall['hit_rate']:.1%}，应优先收紧或暂停该桶。"
            )

    return findings


def run_study(
    signals: List[Dict[str, Any]],
    *,
    horizon: str = "fwd_return_5d",
    top_n: int = 6,
) -> Dict[str, Any]:
    valid = [row for row in signals if row.get(horizon) is not None]

    summary_specs = [
        ("全量", lambda row: True),
        ("源头 hop=0", lambda row: row.get("hop") == 0),
        ("下游 hop>0", lambda row: (row.get("hop") or 0) > 0),
        ("policy_risk", lambda row: row.get("event_type") == "policy_risk"),
        ("ma", lambda row: row.get("event_type") == "ma"),
        ("cooperation", lambda row: row.get("event_type") == "cooperation"),
        ("buyback", lambda row: row.get("event_type") == "buyback"),
        ("management_change", lambda row: row.get("event_type") == "management_change"),
    ]

    summary_rows: Dict[str, Dict[str, Any]] = {}
    for label, predicate in summary_specs:
        metrics = _bucket_metrics([row for row in valid if predicate(row)], horizon)
        summary_rows[label] = {"label": label, **metrics}

    type_sections: Dict[str, Dict[str, Any]] = {}
    for event_type in _FOCUS_TYPES:
        rows = [row for row in valid if row.get("event_type") == event_type]
        slices = {
            "全量": _bucket_metrics(rows, horizon),
            "源头 hop=0": _bucket_metrics([row for row in rows if row.get("hop") == 0], horizon),
            "下游 hop>0": _bucket_metrics([row for row in rows if (row.get("hop") or 0) > 0], horizon),
            "已反应": _bucket_metrics([row for row in rows if row.get("reacted")], horizon),
            "未反应": _bucket_metrics([row for row in rows if not row.get("reacted")], horizon),
        }
        type_sections[event_type] = {
            "slices": slices,
            "top_sources": _top_groups(rows, _label_source, horizon, top_n=top_n),
            "top_chains": _top_groups(rows, _label_chain, horizon, top_n=top_n),
        }

    findings = _build_findings(summary_rows, type_sections)
    return {
        "horizon": horizon,
        "total_signals": len(signals),
        "valid_signals": len(valid),
        "summary_rows": summary_rows,
        "type_sections": type_sections,
        "findings": findings,
    }


def _render_report(study: Dict[str, Any], *, signals_file: Path) -> str:
    lines = [
        "# 冲击传播链路 — 分桶研究",
        "",
        f"**日期**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"**信号文件**: `{signals_file}`",
        f"**总信号数**: {study['total_signals']}",
        f"**有效样本({study['horizon']})**: {study['valid_signals']}",
        "",
        "## 核心分桶",
        "",
    ]
    summary_rows = [study["summary_rows"][label] for label in [
        "全量",
        "源头 hop=0",
        "下游 hop>0",
        "policy_risk",
        "ma",
        "cooperation",
        "buyback",
        "management_change",
    ]]
    lines.extend(_format_bucket_table(summary_rows))

    if study["findings"]:
        lines.extend([
            "",
            "## 关键发现",
            "",
        ])
        for idx, finding in enumerate(study["findings"], start=1):
            lines.append(f"{idx}. {finding}")

    for event_type in _FOCUS_TYPES:
        section = study["type_sections"][event_type]
        lines.extend([
            "",
            f"## {event_type}",
            "",
        ])
        slice_rows = [{"label": label, **metrics} for label, metrics in section["slices"].items()]
        lines.extend(_format_bucket_table(slice_rows))
        lines.extend(["", *(_format_top_table(section["top_sources"], "### Top Sources"))])
        lines.extend(["", *(_format_top_table(section["top_chains"], "### Top Relation Chains"))])

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run shock bucket study from signal outputs")
    parser.add_argument("--signals-file", type=str, default="", help="Path to shock signal json")
    parser.add_argument("--horizon", type=str, default="fwd_return_5d")
    parser.add_argument("--top-n", type=int, default=6)
    parser.add_argument("--tag", type=str, default="", help="Optional output tag")
    args = parser.parse_args()

    signals_file = Path(args.signals_file) if args.signals_file else _default_signals_file()
    signals = _load_signals(signals_file)
    study = run_study(signals, horizon=args.horizon, top_n=args.top_n)

    report_dir = Path(__file__).resolve().parent.parent / ".data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    tag = args.tag
    if not tag:
        match = re.search(r"(\d{8})", signals_file.stem)
        tag = match.group(1) if match else datetime.now().strftime("%Y%m%d")

    report_path = report_dir / f"shock_bucket_study_{tag}.md"
    json_path = report_dir / f"shock_bucket_study_{tag}.json"

    report_path.write_text(_render_report(study, signals_file=signals_file), encoding="utf-8")
    json_path.write_text(json.dumps(study, ensure_ascii=False, indent=2), encoding="utf-8")

    print()
    print("=" * 72)
    print("Shock bucket study")
    print("=" * 72)
    print(f"Signals file: {signals_file}")
    print(f"Valid signals: {study['valid_signals']}")
    print(f"Report: {report_path}")
    print(f"JSON:   {json_path}")


if __name__ == "__main__":
    main()
