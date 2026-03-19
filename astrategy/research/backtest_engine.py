"""
冲击链路统一回测引擎
====================

严格 point-in-time 回测：
  - 事件日期后 T+1 开盘入场
  - T+N 收盘出场（N = 持有期）
  - 0.4% 往返成本（0.3% TC + 0.1% 滑点）

Usage:
    from astrategy.research.backtest_engine import ShockBacktestEngine
    engine = ShockBacktestEngine(cost_bps=40, holding_days=5)
    results = engine.run(events)
    metrics = engine.evaluate(results)
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from astrategy.events.normalizer import normalize_events
from astrategy.research.metrics import compute_metrics

logger = logging.getLogger("astrategy.research.backtest_engine")


class ShockBacktestEngine:
    """冲击链路统一回测引擎。

    严格 point-in-time：
      - 事件日期后 T+1 开盘入场
      - T+N 收盘出场（N = 持有期）
      - 默认 0.4% 往返成本（0.3% TC + 0.1% 滑点）
    """

    def __init__(
        self,
        cost_bps: int = 40,
        holding_days: int = 5,
    ) -> None:
        self.cost = cost_bps / 10000  # 0.004
        self.holding_days = holding_days
        self._pipeline = None  # lazy init

    @property
    def pipeline(self):
        """延迟初始化 ShockPipeline（避免导入时加载图谱）。"""
        if self._pipeline is None:
            from astrategy.shock_pipeline import ShockPipeline
            self._pipeline = ShockPipeline()
        return self._pipeline

    def run(
        self,
        events: List[Dict[str, Any]],
        skip_debate: bool = True,
        downstream_limit: Optional[int] = None,
        include_source_signals: bool = True,
        allow_rejected: bool = False,
        enable_reacted_continuation: bool = True,
    ) -> pd.DataFrame:
        """对历史事件运行回测，返回信号+收益 DataFrame。

        Parameters
        ----------
        events : list[dict]
            历史事件列表，每个事件至少包含：
            {title, type, stock_code, stock_name, event_date, summary}
        skip_debate : bool
            是否跳过 LLM Agent 辩论（默认 True，加速回测）。

        Returns
        -------
        pd.DataFrame
            列: event_id, event_date, event_type, source_code, target_code,
                hop, shock_weight, direction, reacted,
                entry_price, exit_price, gross_return, net_return,
                direction_adjusted_return, confidence
        """
        # 调用 pipeline.run_historical 获取原始信号
        raw_signals = self.pipeline.run_historical(
            events=events,
            skip_debate=skip_debate,
            forward_days=self.holding_days,
            downstream_limit=downstream_limit,
            include_source_signals=include_source_signals,
            allow_rejected=allow_rejected,
            enable_reacted_continuation=enable_reacted_continuation,
        )

        if not raw_signals:
            logger.warning("回测未产生任何信号")
            return pd.DataFrame()

        # 转换为 DataFrame
        records = []
        for sig in raw_signals:
            raw_return = sig.get(f"fwd_return_{self.holding_days}d")
            if raw_return is None:
                raw_return = sig.get("return_5d") or sig.get("fwd_return_5d")
            if raw_return is None:
                continue

            direction = sig.get("signal_direction", "neutral")
            if direction == "neutral":
                continue

            # ═══ 涨跌停过滤 ═══
            # long方向: T+1涨停开盘→买不进去→跳过
            # avoid方向: T+1跌停开盘→卖不出去→跳过
            hit_up = sig.get("hit_limit_up", False)
            hit_down = sig.get("hit_limit_down", False)
            if direction == "long" and hit_up:
                continue  # 涨停买不进
            if direction == "avoid" and hit_down:
                continue  # 跌停卖不出

            # 方向调整收益
            if direction == "avoid":
                adj_return = -raw_return - self.cost
            elif direction == "long":
                adj_return = raw_return - self.cost
            else:
                continue

            entry_price = sig.get("entry_price", 0.0)
            # 估算 exit_price
            exit_price = entry_price * (1 + raw_return) if entry_price > 0 else 0.0

            records.append({
                "event_id": sig.get("event_id", ""),
                "event_date": sig.get("event_date", ""),
                "event_type": sig.get("event_type", ""),
                "source_code": sig.get("source_code", ""),
                "source_name": sig.get("source_name", ""),
                "target_code": sig.get("target_code", ""),
                "target_name": sig.get("target_name", ""),
                "hop": sig.get("hop", 0),
                "shock_weight": sig.get("shock_weight", 0.0),
                "graph_score": sig.get("graph_score", 0.0),
                "graph_rank_score": sig.get("graph_rank_score", 0.0),
                "path_quality": sig.get("path_quality", 0.0),
                "relation_score": sig.get("relation_score", 0.0),
                "specificity_score": sig.get("specificity_score", 0.0),
                "direction": direction,
                "emittable": bool(sig.get("emittable", True)),
                "reacted": sig.get("reacted", False),
                "entry_price": entry_price,
                "exit_price": round(exit_price, 2),
                "gross_return": round(raw_return, 6),
                "net_return": round(raw_return - self.cost if direction == "long"
                                    else -raw_return - self.cost, 6),
                "direction_adjusted_return": round(adj_return, 6),
                "confidence": sig.get("confidence", 0.0),
                "score": sig.get("score", 0.0),
                "action": sig.get("action", ""),
                "alpha_type": sig.get("alpha_type", ""),
                "alpha_family": sig.get("alpha_family", ""),
                "relation_chain": sig.get("relation_chain", ""),
                "reacted_continuation": bool(sig.get("reacted_continuation", False)),
                "consensus_direction": sig.get("consensus_direction", ""),
                "divergence": sig.get("divergence", 0.0),
                "hit_limit_up": sig.get("hit_limit_up", False),
                "hit_limit_down": sig.get("hit_limit_down", False),
                "trigger_score": sig.get("trigger_score", 0.0),
                "propagation_score": sig.get("propagation_score", 0.0),
                "debate_score": sig.get("debate_score", 0.0),
                "market_check_score": sig.get("market_check_score", 0.0),
                "risk_penalty": sig.get("risk_penalty", 0.0),
                "narrative_phase": sig.get("narrative_phase", ""),
                "narrative_tags": ",".join(sig.get("narrative_tags", []))
                if isinstance(sig.get("narrative_tags"), list) else sig.get("narrative_tags", ""),
                # 多周期收益（供分析用）
                "fwd_return_1d": sig.get("fwd_return_1d"),
                "fwd_return_3d": sig.get("fwd_return_3d"),
                "fwd_return_5d": sig.get("fwd_return_5d"),
                "fwd_return_10d": sig.get("fwd_return_10d"),
                "fwd_return_20d": sig.get("fwd_return_20d"),
            })

        df = pd.DataFrame(records)
        logger.info(
            "回测完成: %d 个有效信号（来自 %d 个事件）",
            len(df), len(events),
        )
        return df

    def evaluate(self, results_df: pd.DataFrame) -> Dict[str, Any]:
        """计算回测评价指标。

        Parameters
        ----------
        results_df : pd.DataFrame
            run() 方法返回的 DataFrame。

        Returns
        -------
        dict
            完整评价指标字典（含分组分析）。
        """
        if results_df.empty:
            return {"error": "空结果，无法评价"}

        returns = results_df["direction_adjusted_return"]
        confidence = results_df["confidence"] if "confidence" in results_df.columns else None
        hops = results_df["hop"] if "hop" in results_df.columns else None
        event_types = results_df["event_type"] if "event_type" in results_df.columns else None
        reacted_col = results_df["reacted"].astype(str) if "reacted" in results_df.columns else None

        metrics = compute_metrics(
            returns=returns,
            holding_days=self.holding_days,
            confidence=confidence,
            hops=hops,
            event_types=event_types,
            reacted=reacted_col,
        )

        # 补充额外统计
        metrics["cost_bps"] = int(self.cost * 10000)
        metrics["holding_days"] = self.holding_days
        metrics["n_events"] = results_df["event_id"].nunique()

        if "event_id" in results_df.columns and len(results_df) > 0:
            metrics["coverage"] = round(
                results_df["event_id"].nunique() / max(results_df["event_id"].count(), 1),
                4,
            )
        if "event_type" in results_df.columns and not results_df["event_type"].empty:
            type_dist = results_df["event_type"].value_counts(normalize=True)
            breadth = float(-(type_dist * np.log(type_dist + 1e-12)).sum())
            metrics["breadth"] = round(breadth, 4)

        if "confidence" in results_df.columns and "direction_adjusted_return" in results_df.columns:
            try:
                bins = pd.qcut(results_df["confidence"], q=min(4, len(results_df)), duplicates="drop")
                calibration = (
                    results_df.assign(conf_bin=bins)
                    .groupby("conf_bin")["direction_adjusted_return"]
                    .mean()
                    .tolist()
                )
                metrics["confidence_calibration"] = [round(float(v), 6) for v in calibration]
                if calibration:
                    metrics["confidence_spread"] = round(float(max(calibration) - min(calibration)), 6)
            except Exception:
                metrics["confidence_calibration"] = []

        return metrics

    def load_events(
        self,
        path: Optional[str] = None,
        max_events: int = 0,
    ) -> List[Dict[str, Any]]:
        """加载历史事件数据。

        Parameters
        ----------
        path : str, optional
            事件文件路径，默认为 .data/historical_events.json。
        max_events : int
            限制事件数量，0 表示全部。

        Returns
        -------
        list[dict]
        """
        if path is None:
            path = str(
                Path(__file__).resolve().parent.parent / ".data" / "historical_events.json"
            )

        with open(path, "r", encoding="utf-8") as f:
            events = json.load(f)

        if max_events > 0:
            events = events[:max_events]

        events = normalize_events(events)

        logger.info("加载了 %d 个历史事件 (from %s)", len(events), path)
        return events
