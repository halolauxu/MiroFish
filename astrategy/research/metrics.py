"""
评价指标模块
============

计算回测评价指标，包括：
- 基础指标：胜率、平均收益、Sharpe、MaxDrawdown、Calmar、盈亏比
- 高级指标：IC、RankIC、覆盖率、换手率估计
- 分组分析：按跳数、事件类型、是否反应分组
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


def compute_metrics(
    returns: pd.Series,
    holding_days: int = 5,
    confidence: Optional[pd.Series] = None,
    hops: Optional[pd.Series] = None,
    event_types: Optional[pd.Series] = None,
    reacted: Optional[pd.Series] = None,
) -> Dict[str, Any]:
    """计算完整评价指标。

    Parameters
    ----------
    returns : pd.Series
        方向调整后的净收益序列（已扣TC）。
    holding_days : int
        持有期天数，用于年化 Sharpe。
    confidence : pd.Series, optional
        信心度序列，用于计算 IC/RankIC。
    hops : pd.Series, optional
        跳数序列，用于按跳数分组分析。
    event_types : pd.Series, optional
        事件类型序列，用于按事件类型分组分析。
    reacted : pd.Series, optional
        是否已反应序列，用于按反应状态分组。

    Returns
    -------
    dict
        完整评价指标字典。
    """
    returns = returns.dropna()
    n = len(returns)

    if n == 0:
        return _empty_metrics()

    # ── 基础指标 ──
    win_mask = returns > 0
    loss_mask = returns < 0
    wins = returns[win_mask]
    losses = returns[loss_mask]

    total_signals = n
    win_rate = float(win_mask.sum() / n) if n > 0 else 0.0
    avg_return = float(returns.mean())

    # Sharpe (年化)
    sharpe = _compute_sharpe(returns, holding_days)

    # MaxDrawdown
    max_drawdown = _compute_max_drawdown(returns)

    # Calmar ratio = 年化收益 / MaxDrawdown
    ann_factor = 252.0 / max(holding_days, 1)
    ann_return = avg_return * ann_factor
    calmar = abs(ann_return / max_drawdown) if max_drawdown < -1e-9 else 0.0

    # 盈亏比 (Profit Factor)
    total_wins = float(wins.sum()) if len(wins) > 0 else 0.0
    total_losses = float(abs(losses.sum())) if len(losses) > 0 else 0.0
    profit_factor = (
        total_wins / total_losses if total_losses > 1e-9 else (99.0 if total_wins > 0 else 0.0)
    )

    avg_win = float(wins.mean()) if len(wins) > 0 else 0.0
    avg_loss = float(losses.mean()) if len(losses) > 0 else 0.0

    # ── IC / RankIC ──
    ic = 0.0
    rank_ic = 0.0
    if confidence is not None:
        confidence = confidence.reindex(returns.index).dropna()
        common_idx = returns.index.intersection(confidence.index)
        if len(common_idx) >= 5:
            r = returns.loc[common_idx]
            c = confidence.loc[common_idx]
            ic = float(r.corr(c))
            rank_ic = float(r.corr(c, method="spearman"))
            if math.isnan(ic):
                ic = 0.0
            if math.isnan(rank_ic):
                rank_ic = 0.0

    # ── 覆盖率 & 换手率估计 ──
    coverage = 1.0  # 信号覆盖率 = 有效信号数/总事件数 (需外部提供总事件数才有意义)
    turnover = 1.0 / max(holding_days, 1)  # 简单估计：每个持有周期换手一次

    result = {
        "total_signals": total_signals,
        "win_rate": round(win_rate, 4),
        "avg_return": round(avg_return, 6),
        "sharpe": round(sharpe, 2),
        "max_drawdown": round(max_drawdown, 4),
        "calmar": round(calmar, 2),
        "profit_factor": round(min(profit_factor, 99.0), 2),
        "avg_win": round(avg_win, 6),
        "avg_loss": round(avg_loss, 6),
        "ic": round(ic, 4),
        "rank_ic": round(rank_ic, 4),
        "coverage": round(coverage, 4),
        "turnover": round(turnover, 4),
    }

    # ── 按跳数分组 ──
    if hops is not None:
        result["by_hop"] = _group_metrics(returns, hops, holding_days, confidence)

    # ── 按事件类型分组 ──
    if event_types is not None:
        result["by_event_type"] = _group_metrics(returns, event_types, holding_days, confidence)

    # ── 按是否反应分组 ──
    if reacted is not None:
        result["by_reacted"] = _group_metrics(returns, reacted, holding_days, confidence)

    return result


def _compute_sharpe(returns: pd.Series, holding_days: int) -> float:
    """计算年化 Sharpe ratio。"""
    if len(returns) < 2:
        return 0.0
    mean_return = float(returns.mean())
    std_return = float(returns.std())
    if std_return < 1e-9:
        return 0.0 if mean_return <= 0 else 99.0
    ann_factor = np.sqrt(252.0 / max(holding_days, 1))
    return (mean_return / std_return) * ann_factor


def _compute_max_drawdown(returns: pd.Series) -> float:
    """计算最大回撤（返回负值）。"""
    if len(returns) == 0:
        return 0.0
    cumulative = (1 + returns).cumprod()
    running_max = cumulative.cummax()
    drawdown = (cumulative - running_max) / running_max
    return float(drawdown.min())


def _group_metrics(
    returns: pd.Series,
    groups: pd.Series,
    holding_days: int,
    confidence: Optional[pd.Series] = None,
) -> Dict[str, Dict[str, Any]]:
    """按分组键计算各组指标。"""
    groups = groups.reindex(returns.index)
    result = {}
    for key, idx in returns.groupby(groups).groups.items():
        sub_returns = returns.loc[idx]
        sub_conf = confidence.loc[idx] if confidence is not None else None
        key_str = str(key)
        if len(sub_returns) == 0:
            result[key_str] = _empty_metrics()
            continue

        n = len(sub_returns)
        win_rate = float((sub_returns > 0).sum() / n)
        avg_ret = float(sub_returns.mean())
        sharpe = _compute_sharpe(sub_returns, holding_days)
        max_dd = _compute_max_drawdown(sub_returns)

        ic = 0.0
        if sub_conf is not None and len(sub_conf.dropna()) >= 5:
            common = sub_returns.index.intersection(sub_conf.dropna().index)
            if len(common) >= 5:
                ic_val = sub_returns.loc[common].corr(sub_conf.loc[common])
                ic = float(ic_val) if not math.isnan(ic_val) else 0.0

        result[key_str] = {
            "total_signals": n,
            "win_rate": round(win_rate, 4),
            "avg_return": round(avg_ret, 6),
            "sharpe": round(sharpe, 2),
            "max_drawdown": round(max_dd, 4),
            "ic": round(ic, 4),
        }

    return result


def _empty_metrics() -> Dict[str, Any]:
    """返回空指标字典。"""
    return {
        "total_signals": 0,
        "win_rate": 0.0,
        "avg_return": 0.0,
        "sharpe": 0.0,
        "max_drawdown": 0.0,
        "calmar": 0.0,
        "profit_factor": 0.0,
        "avg_win": 0.0,
        "avg_loss": 0.0,
        "ic": 0.0,
        "rank_ic": 0.0,
        "coverage": 0.0,
        "turnover": 0.0,
    }


def format_metrics_table(metrics: Dict[str, Any], title: str = "") -> str:
    """将指标字典格式化为 Markdown 表格。"""
    lines = []
    if title:
        lines.append(f"### {title}")
        lines.append("")

    lines.append("| 指标 | 值 |")
    lines.append("|------|-----|")

    _LABELS = {
        "total_signals": "信号数",
        "win_rate": "胜率",
        "avg_return": "平均收益",
        "sharpe": "Sharpe(年化)",
        "max_drawdown": "最大回撤",
        "calmar": "Calmar",
        "profit_factor": "盈亏比",
        "avg_win": "平均盈利",
        "avg_loss": "平均亏损",
        "ic": "IC",
        "rank_ic": "RankIC",
        "coverage": "覆盖率",
        "turnover": "换手率",
    }

    for key, label in _LABELS.items():
        val = metrics.get(key)
        if val is None:
            continue
        if key == "win_rate":
            lines.append(f"| {label} | {val:.1%} |")
        elif key == "total_signals":
            lines.append(f"| {label} | {val} |")
        elif key in ("sharpe", "calmar", "profit_factor"):
            lines.append(f"| {label} | {val:.2f} |")
        elif key == "max_drawdown":
            lines.append(f"| {label} | {val:.2%} |")
        else:
            lines.append(f"| {label} | {val:.4f} |")

    return "\n".join(lines)
