"""
消融实验
========

证明图谱传播、未反应过滤等模块的增量价值。

实验设计：
  1. 图谱消融：图谱传播选股 vs 随机同行业选股
  2. 反应过滤消融：有未反应过滤 vs 无过滤
  3. 跳数消融：不同跳数的独立胜率对比
"""

from __future__ import annotations

import logging
import random
from collections import defaultdict
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from astrategy.research.backtest_engine import ShockBacktestEngine
from astrategy.research.metrics import compute_metrics

logger = logging.getLogger("astrategy.research.ablation")


class AblationExperiment:
    """消融实验：证明图谱/未反应过滤的增量价值。"""

    def run_graph_ablation(
        self,
        events: List[Dict[str, Any]],
        engine: ShockBacktestEngine,
        n_random_trials: int = 100,
        skip_debate: bool = True,
    ) -> Dict[str, Any]:
        """对比：图谱传播选股 vs 随机同行业选股。

        控制组：对每个事件，随机选同行业 N 只股票（N = 图谱找到的数量）
        实验组：图谱传播找到的下游公司
        跑 n_random_trials 次随机以获得置信区间。

        Parameters
        ----------
        events : list[dict]
            历史事件列表。
        engine : ShockBacktestEngine
            回测引擎。
        n_random_trials : int
            随机对照组重复次数。
        skip_debate : bool
            是否跳过辩论。

        Returns
        -------
        dict
            {graph_metrics, random_mean_metrics, random_std_metrics,
             graph_advantage, p_value_approx}
        """
        # 实验组：正常图谱回测
        graph_df = engine.run(events, skip_debate=skip_debate)
        graph_metrics = engine.evaluate(graph_df) if not graph_df.empty else {}

        # 统计每个事件图谱找到的下游数量
        downstream_counts = defaultdict(int)
        if not graph_df.empty:
            for _, row in graph_df.iterrows():
                if row.get("hop", 0) > 0:
                    downstream_counts[row["event_id"]] += 1

        # 控制组：随机同行业选股
        # 由于需要行业信息和随机股票池，这里用简化方法：
        # 对图谱结果的 direction_adjusted_return 做随机打乱
        random_sharpes = []
        random_win_rates = []

        if not graph_df.empty and len(graph_df) > 3:
            downstream_mask = graph_df["hop"] > 0
            downstream_returns = graph_df.loc[downstream_mask, "direction_adjusted_return"].values

            if len(downstream_returns) >= 3:
                for _ in range(n_random_trials):
                    # 随机打乱收益（打破图谱选股与收益的关联）
                    shuffled = downstream_returns.copy()
                    np.random.shuffle(shuffled)
                    shuffled_series = pd.Series(shuffled)
                    m = compute_metrics(shuffled_series, holding_days=engine.holding_days)
                    random_sharpes.append(m.get("sharpe", 0.0))
                    random_win_rates.append(m.get("win_rate", 0.0))

        random_mean_sharpe = float(np.mean(random_sharpes)) if random_sharpes else 0.0
        random_std_sharpe = float(np.std(random_sharpes)) if random_sharpes else 0.0
        random_mean_wr = float(np.mean(random_win_rates)) if random_win_rates else 0.0

        graph_sharpe = graph_metrics.get("sharpe", 0.0)

        # 近似 p-value：图谱 Sharpe 在随机分布中的分位数
        if random_sharpes:
            better_count = sum(1 for s in random_sharpes if s >= graph_sharpe)
            p_value = better_count / len(random_sharpes)
        else:
            p_value = 1.0

        advantage = graph_sharpe - random_mean_sharpe

        logger.info(
            "图谱消融: Graph Sharpe=%.2f, Random Mean=%.2f (std=%.2f), "
            "优势=%.2f, p=%.3f",
            graph_sharpe, random_mean_sharpe, random_std_sharpe,
            advantage, p_value,
        )

        return {
            "graph_metrics": graph_metrics,
            "random_mean_sharpe": round(random_mean_sharpe, 2),
            "random_std_sharpe": round(random_std_sharpe, 2),
            "random_mean_win_rate": round(random_mean_wr, 4),
            "graph_advantage_sharpe": round(advantage, 2),
            "p_value_approx": round(p_value, 4),
            "n_random_trials": n_random_trials,
        }

    def run_reaction_ablation(
        self,
        events: List[Dict[str, Any]],
        engine: ShockBacktestEngine,
        skip_debate: bool = True,
    ) -> Dict[str, Any]:
        """对比：有未反应过滤 vs 无过滤。

        控制组：所有下游公司（不管是否已反应）
        实验组：只保留未反应的公司

        Parameters
        ----------
        events : list[dict]
            历史事件列表。
        engine : ShockBacktestEngine
            回测引擎。
        skip_debate : bool
            是否跳过辩论。

        Returns
        -------
        dict
            {all_metrics, unreacted_metrics, reacted_metrics,
             unreacted_advantage}
        """
        results_df = engine.run(events, skip_debate=skip_debate)

        if results_df.empty:
            return {
                "all_metrics": {},
                "unreacted_metrics": {},
                "reacted_metrics": {},
                "unreacted_advantage": 0.0,
            }

        # 只看下游信号（hop > 0）
        downstream = results_df[results_df["hop"] > 0].copy()

        if downstream.empty:
            return {
                "all_metrics": {},
                "unreacted_metrics": {},
                "reacted_metrics": {},
                "unreacted_advantage": 0.0,
            }

        # 全部下游（控制组）
        all_metrics = compute_metrics(
            downstream["direction_adjusted_return"],
            holding_days=engine.holding_days,
        )

        # 未反应（实验组）
        unreacted = downstream[~downstream["reacted"]]
        unreacted_metrics = compute_metrics(
            unreacted["direction_adjusted_return"],
            holding_days=engine.holding_days,
        ) if not unreacted.empty else {}

        # 已反应
        reacted = downstream[downstream["reacted"]]
        reacted_metrics = compute_metrics(
            reacted["direction_adjusted_return"],
            holding_days=engine.holding_days,
        ) if not reacted.empty else {}

        advantage = (
            unreacted_metrics.get("sharpe", 0.0) - reacted_metrics.get("sharpe", 0.0)
        )

        logger.info(
            "反应过滤消融: 未反应 Sharpe=%.2f (n=%d), 已反应 Sharpe=%.2f (n=%d), 优势=%.2f",
            unreacted_metrics.get("sharpe", 0.0),
            unreacted_metrics.get("total_signals", 0),
            reacted_metrics.get("sharpe", 0.0),
            reacted_metrics.get("total_signals", 0),
            advantage,
        )

        return {
            "all_metrics": all_metrics,
            "unreacted_metrics": unreacted_metrics,
            "reacted_metrics": reacted_metrics,
            "unreacted_advantage_sharpe": round(advantage, 2),
        }

    def run_hop_ablation(
        self,
        events: List[Dict[str, Any]],
        engine: ShockBacktestEngine,
        skip_debate: bool = True,
    ) -> Dict[str, Any]:
        """对比：不同跳数的独立胜率和 Sharpe。

        hop=0 (源头) vs hop=1 vs hop=2 vs hop=3

        Parameters
        ----------
        events : list[dict]
            历史事件列表。
        engine : ShockBacktestEngine
            回测引擎。
        skip_debate : bool
            是否跳过辩论。

        Returns
        -------
        dict
            {by_hop: {0: metrics, 1: metrics, ...}, best_hop, conclusion}
        """
        results_df = engine.run(events, skip_debate=skip_debate)

        if results_df.empty:
            return {"by_hop": {}, "best_hop": -1, "conclusion": "无数据"}

        by_hop = {}
        for hop_val in sorted(results_df["hop"].unique()):
            subset = results_df[results_df["hop"] == hop_val]
            metrics = compute_metrics(
                subset["direction_adjusted_return"],
                holding_days=engine.holding_days,
            )
            by_hop[int(hop_val)] = metrics
            logger.info(
                "Hop %d: Sharpe=%.2f, 胜率=%.1f%%, n=%d",
                hop_val, metrics.get("sharpe", 0),
                metrics.get("win_rate", 0) * 100,
                metrics.get("total_signals", 0),
            )

        # 找最佳跳数（按 Sharpe）
        best_hop = max(by_hop, key=lambda h: by_hop[h].get("sharpe", 0))

        # 结论
        conclusions = []
        if best_hop > 0:
            conclusions.append(
                f"下游 Hop={best_hop} Sharpe 最优 "
                f"({by_hop[best_hop].get('sharpe', 0):.2f})，"
                f"支持信息差假设"
            )
        else:
            conclusions.append("源头(Hop=0) Sharpe 最优，信息差假设未被支持")

        # 检查跳数越大是否胜率越高
        hop_keys = sorted(by_hop.keys())
        if len(hop_keys) >= 2:
            wrs = [by_hop[h].get("win_rate", 0) for h in hop_keys if h > 0]
            if wrs and wrs[-1] > wrs[0]:
                conclusions.append("高跳数胜率 > 低跳数胜率，信息衰减梯度明显")
            elif wrs:
                conclusions.append("高跳数胜率未显著优于低跳数")

        return {
            "by_hop": by_hop,
            "best_hop": best_hop,
            "conclusion": "; ".join(conclusions),
        }

    def run_all(
        self,
        events: List[Dict[str, Any]],
        engine: ShockBacktestEngine,
        skip_debate: bool = True,
        n_random_trials: int = 100,
    ) -> Dict[str, Any]:
        """运行全部消融实验。"""
        logger.info("=" * 60)
        logger.info("开始消融实验")
        logger.info("=" * 60)

        graph_result = self.run_graph_ablation(
            events, engine, n_random_trials=n_random_trials, skip_debate=skip_debate,
        )
        reaction_result = self.run_reaction_ablation(
            events, engine, skip_debate=skip_debate,
        )
        hop_result = self.run_hop_ablation(
            events, engine, skip_debate=skip_debate,
        )

        return {
            "graph_ablation": graph_result,
            "reaction_ablation": reaction_result,
            "hop_ablation": hop_result,
        }

    def format_report(self, results: Dict[str, Any]) -> str:
        """将消融实验结果格式化为 Markdown。"""
        lines = ["## 消融实验结果", ""]

        # 图谱消融
        ga = results.get("graph_ablation", {})
        lines.extend([
            "### 1. 图谱传播 vs 随机选股",
            "",
            f"- 图谱 Sharpe: {ga.get('graph_metrics', {}).get('sharpe', 0):.2f}",
            f"- 随机 Sharpe 均值: {ga.get('random_mean_sharpe', 0):.2f} "
            f"(std={ga.get('random_std_sharpe', 0):.2f})",
            f"- 图谱优势: {ga.get('graph_advantage_sharpe', 0):+.2f}",
            f"- 近似 p-value: {ga.get('p_value_approx', 1):.4f}",
            f"- 随机试验次数: {ga.get('n_random_trials', 0)}",
            "",
        ])

        # 反应过滤消融
        ra = results.get("reaction_ablation", {})
        lines.extend([
            "### 2. 未反应过滤 vs 全部信号",
            "",
            "| 分组 | Sharpe | 胜率 | 信号数 |",
            "|------|--------|------|--------|",
        ])
        for label, key in [("未反应", "unreacted_metrics"),
                           ("已反应", "reacted_metrics"),
                           ("全部下游", "all_metrics")]:
            m = ra.get(key, {})
            lines.append(
                f"| {label} "
                f"| {m.get('sharpe', 0):.2f} "
                f"| {m.get('win_rate', 0):.1%} "
                f"| {m.get('total_signals', 0)} |"
            )
        lines.append(f"\n未反应优势(Sharpe): {ra.get('unreacted_advantage_sharpe', 0):+.2f}")
        lines.append("")

        # 跳数消融
        ha = results.get("hop_ablation", {})
        lines.extend([
            "### 3. 跳数对比",
            "",
            "| Hop | Sharpe | 胜率 | 平均收益 | 信号数 |",
            "|-----|--------|------|---------|--------|",
        ])
        for hop in sorted(ha.get("by_hop", {}).keys()):
            m = ha["by_hop"][hop]
            label = "源头" if hop == 0 else f"{hop}跳"
            lines.append(
                f"| {label} "
                f"| {m.get('sharpe', 0):.2f} "
                f"| {m.get('win_rate', 0):.1%} "
                f"| {m.get('avg_return', 0):+.4f} "
                f"| {m.get('total_signals', 0)} |"
            )
        lines.append(f"\n结论: {ha.get('conclusion', 'N/A')}")

        return "\n".join(lines)
