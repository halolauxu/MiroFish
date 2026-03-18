"""
Walk-Forward 滚动验证
=====================

将事件按日期排序，分为 K 个窗口：
  每个窗口用前 N% 训练（确定方向映射规则），后 (100-N)% 测试。

准入标准：
  - OOS Sharpe >= 0.8
  - OOS 胜率 >= 55%
  - OOS 信号数 >= 50
  - OOS MaxDD <= 15%
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

import pandas as pd

from astrategy.research.backtest_engine import ShockBacktestEngine
from astrategy.research.metrics import compute_metrics

logger = logging.getLogger("astrategy.research.walk_forward")


class WalkForwardValidator:
    """Walk-Forward 滚动验证器。

    将事件按日期排序，分为 K 个窗口：
    每个窗口用前 train_ratio 训练，剩余测试。
    """

    def __init__(
        self,
        n_splits: int = 3,
        train_ratio: float = 0.7,
    ) -> None:
        self.n_splits = n_splits
        self.train_ratio = train_ratio

    def run(
        self,
        events: List[Dict[str, Any]],
        engine: ShockBacktestEngine,
        skip_debate: bool = True,
    ) -> Dict[str, Any]:
        """运行 Walk-Forward 验证。

        Parameters
        ----------
        events : list[dict]
            历史事件列表（须含 event_date）。
        engine : ShockBacktestEngine
            回测引擎实例。
        skip_debate : bool
            是否跳过 Agent 辩论。

        Returns
        -------
        dict
            包含各窗口指标、OOS 聚合指标、是否通过准入标准。
        """
        # 按日期排序
        events_sorted = sorted(events, key=lambda e: e.get("event_date", ""))
        n_total = len(events_sorted)

        if n_total < 4:
            logger.warning("事件数太少(%d), 无法进行 Walk-Forward 验证", n_total)
            return {
                "splits": [],
                "oos_aggregate": {},
                "passed": False,
                "pass_criteria": {},
                "error": "事件数不足",
            }

        # 构建窗口
        splits_result = []
        all_oos_dfs = []

        if self.n_splits == 1:
            # 单窗口模式：简单 IS/OOS 切分
            n_train = max(1, int(n_total * self.train_ratio))
            windows = [(0, n_train, n_train, n_total)]
        else:
            # 多窗口滚动：扩展窗口（expanding window）
            # 每个窗口的测试集不重叠
            test_size = max(1, n_total // self.n_splits)
            windows = []
            for i in range(self.n_splits):
                test_start = i * test_size
                test_end = min((i + 1) * test_size, n_total)
                if i == self.n_splits - 1:
                    test_end = n_total  # 最后一个窗口取完

                # 训练集 = 测试集之前的所有事件
                # 如果是第一个窗口，按 train_ratio 切分
                if i == 0:
                    train_end = max(1, int(test_end * self.train_ratio))
                    actual_test_start = train_end
                    windows.append((0, train_end, actual_test_start, test_end))
                else:
                    # 训练集 = 0 到 test_start
                    windows.append((0, test_start, test_start, test_end))

        for split_id, (train_start, train_end, test_start, test_end) in enumerate(windows):
            train_events = events_sorted[train_start:train_end]
            test_events = events_sorted[test_start:test_end]

            if not test_events:
                continue

            logger.info(
                "Split %d: 训练 %d 事件 (%s~%s), 测试 %d 事件 (%s~%s)",
                split_id,
                len(train_events),
                train_events[0].get("event_date", "?") if train_events else "?",
                train_events[-1].get("event_date", "?") if train_events else "?",
                len(test_events),
                test_events[0].get("event_date", "?"),
                test_events[-1].get("event_date", "?"),
            )

            # 运行训练集回测
            train_df = engine.run(train_events, skip_debate=skip_debate)
            train_metrics = engine.evaluate(train_df) if not train_df.empty else {}

            # 运行测试集回测
            test_df = engine.run(test_events, skip_debate=skip_debate)
            test_metrics = engine.evaluate(test_df) if not test_df.empty else {}

            if not test_df.empty:
                all_oos_dfs.append(test_df)

            splits_result.append({
                "split_id": split_id,
                "train_events": len(train_events),
                "test_events": len(test_events),
                "train_date_range": (
                    train_events[0].get("event_date", "") if train_events else "",
                    train_events[-1].get("event_date", "") if train_events else "",
                ),
                "test_date_range": (
                    test_events[0].get("event_date", ""),
                    test_events[-1].get("event_date", ""),
                ),
                "train_metrics": train_metrics,
                "test_metrics": test_metrics,
            })

        # 聚合所有 OOS 窗口
        if all_oos_dfs:
            oos_combined = pd.concat(all_oos_dfs, ignore_index=True)
            oos_aggregate = engine.evaluate(oos_combined)
        else:
            oos_combined = pd.DataFrame()
            oos_aggregate = {}

        # 准入标准判断
        oos_sharpe = oos_aggregate.get("sharpe", 0.0)
        oos_win_rate = oos_aggregate.get("win_rate", 0.0)
        oos_signals = oos_aggregate.get("total_signals", 0)
        oos_max_dd = abs(oos_aggregate.get("max_drawdown", 0.0))

        pass_criteria = {
            "sharpe_ge_0.8": oos_sharpe >= 0.8,
            "win_rate_ge_55": oos_win_rate >= 0.55,
            "signals_ge_50": oos_signals >= 50,
            "max_dd_le_15": oos_max_dd <= 0.15,
        }
        passed = all(pass_criteria.values())

        logger.info(
            "Walk-Forward 验证%s: Sharpe=%.2f, 胜率=%.1f%%, 信号=%d, MaxDD=%.1f%%",
            "通过" if passed else "未通过",
            oos_sharpe, oos_win_rate * 100, oos_signals, oos_max_dd * 100,
        )

        return {
            "splits": splits_result,
            "oos_aggregate": oos_aggregate,
            "passed": passed,
            "pass_criteria": pass_criteria,
        }

    def format_report(self, result: Dict[str, Any]) -> str:
        """将 Walk-Forward 结果格式化为 Markdown 报告。"""
        lines = [
            "## Walk-Forward 验证结果",
            "",
        ]

        # 准入标准
        passed = result.get("passed", False)
        lines.append(f"**准入判定**: {'PASS' if passed else 'FAIL'}")
        lines.append("")

        criteria = result.get("pass_criteria", {})
        lines.append("| 准入标准 | 结果 |")
        lines.append("|---------|------|")
        _LABELS = {
            "sharpe_ge_0.8": "OOS Sharpe >= 0.8",
            "win_rate_ge_55": "OOS 胜率 >= 55%",
            "signals_ge_50": "OOS 信号数 >= 50",
            "max_dd_le_15": "OOS MaxDD <= 15%",
        }
        for key, label in _LABELS.items():
            val = criteria.get(key, False)
            lines.append(f"| {label} | {'PASS' if val else 'FAIL'} |")

        # 各窗口详情
        lines.extend(["", "### 各窗口指标", ""])
        lines.append("| Split | 训练事件 | 测试事件 | Train Sharpe | Test Sharpe | Test 胜率 | Test 信号数 |")
        lines.append("|-------|---------|---------|-------------|-------------|----------|-----------|")

        for split in result.get("splits", []):
            tm = split.get("train_metrics", {})
            tsm = split.get("test_metrics", {})
            lines.append(
                f"| {split['split_id']} "
                f"| {split['train_events']} "
                f"| {split['test_events']} "
                f"| {tm.get('sharpe', 0):.2f} "
                f"| {tsm.get('sharpe', 0):.2f} "
                f"| {tsm.get('win_rate', 0):.1%} "
                f"| {tsm.get('total_signals', 0)} |"
            )

        # OOS 聚合
        oos = result.get("oos_aggregate", {})
        if oos:
            lines.extend(["", "### OOS 聚合指标", ""])
            lines.append(f"- Sharpe: {oos.get('sharpe', 0):.2f}")
            lines.append(f"- 胜率: {oos.get('win_rate', 0):.1%}")
            lines.append(f"- 平均收益: {oos.get('avg_return', 0):+.4f}")
            lines.append(f"- MaxDD: {oos.get('max_drawdown', 0):.2%}")
            lines.append(f"- 盈亏比: {oos.get('profit_factor', 0):.2f}")
            lines.append(f"- IC: {oos.get('ic', 0):.4f}")
            lines.append(f"- 信号数: {oos.get('total_signals', 0)}")

        return "\n".join(lines)
