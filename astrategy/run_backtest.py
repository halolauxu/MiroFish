#!/usr/bin/env python3
"""
全策略严格回测系统 v2
======================
对 S01-S10 全部策略执行严格回测：
- 中证800全部成分股（CSI800 = 沪深300 + 中证500）
- 多日期滚动回测（回看 N 个历史窗口验证信号有效性）
- 使用真实历史行情数据（新浪API）
- 接入 DeepSeek LLM（含 cache + cost_tracker）
- 策略间横向对比 + 结论评级

用法:
    python astrategy/run_backtest.py                    # 全量回测
    python astrategy/run_backtest.py --quick            # 快速模式（30只样本股）
    python astrategy/run_backtest.py --strategies S02   # 单策略
    python astrategy/run_backtest.py --no-llm           # 跳过LLM策略
"""
import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# 确保 astrategy 可导入
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from astrategy.backtest.evaluator import Evaluator
from astrategy.data_collector.market_data import MarketDataCollector
from astrategy.strategies.base import BaseStrategy, StrategySignal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backtest")

# ---------------------------------------------------------------------------
# 股票池定义
# ---------------------------------------------------------------------------

def fetch_csi800_codes() -> list[str]:
    """动态获取中证800最新成分股列表（CSI800 = 沪深300 + 中证500）。"""
    try:
        import akshare as ak
        df = ak.index_stock_cons_csindex(symbol="000906")
        codes = df["成分券代码"].astype(str).tolist()
        logger.info("从中证指数获取中证800成分股: %d 只", len(codes))
        return codes
    except Exception as exc:
        logger.warning("获取中证800成分股失败: %s, 使用预置列表", exc)
        return list(CSI800_FALLBACK)

# 中证800预置列表（API失败时回退，包含沪深300+中证500代表性股票）
CSI800_FALLBACK: list[str] = [
    "000001", "000002", "000063", "000100", "000157", "000166", "000301",
    "000333", "000338", "000408", "000425", "000538", "000568", "000596",
    "000625", "000651", "000661", "000725", "000768", "000858", "000876",
    "000895", "000963", "000977", "000999", "001979", "002001", "002027",
    "002049", "002142", "002179", "002230", "002252", "002304", "002371",
    "002384", "002415", "002459", "002460", "002475", "002493", "002594",
    "002601", "002714", "002916", "003816", "300014", "300015", "300059",
    "300122", "300124", "300274", "300347", "300408", "300413", "300433",
    "300498", "300628", "300750", "300759", "300760", "300782", "300896",
    "300999", "600000", "600009", "600010", "600011", "600015", "600016",
    "600018", "600019", "600023", "600025", "600027", "600028", "600029",
    "600030", "600031", "600036", "600048", "600050", "600061", "600085",
    "600104", "600111", "600150", "600176", "600183", "600188", "600196",
    "600276", "600309", "600346", "600362", "600406", "600436", "600438",
    "600460", "600489", "600519", "600570", "600585", "600588", "600660",
    "600690", "600760", "600809", "600886", "600887", "600893", "600900",
    "600905", "600918", "600919", "600938", "600941", "600989", "600999",
    "601006", "601009", "601012", "601018", "601021", "601058", "601066",
    "601088", "601100", "601111", "601117", "601127", "601138", "601166",
    "601186", "601211", "601225", "601236", "601288", "601318", "601319",
    "601328", "601336", "601360", "601390", "601398", "601600", "601601",
    "601618", "601628", "601633", "601668", "601669", "601688", "601689",
    "601698", "601728", "601766", "601788", "601800", "601816", "601818",
    "601838", "601857", "601868", "601872", "601877", "601878", "601881",
    "601888", "601898", "601899", "601901", "601919", "601939", "601985",
    "601988", "601998", "603019", "603259", "603260", "603288", "603369",
    "603501", "603799", "603986", "603993", "688008", "688009", "688012",
    "688036", "688041", "688111", "688169", "688187", "688256", "688303",
    "688396", "688981",
    # 中证500代表性股票（CSI800 = CSI300 + CSI500，以下为CSI500补充）
    "002050", "002074", "002120", "002124", "002179", "002183", "002236",
    "002241", "002273", "002286", "002340", "002352", "002396", "002410",
    "002456", "002508", "002541", "002558", "002600", "002624", "002648",
    "002673", "002707", "002739", "002756", "002841", "002867", "002920",
    "003035", "003816",
    "300003", "300012", "300017", "300033", "300070", "300102", "300142",
    "300144", "300168", "300207", "300223", "300251", "300316", "300336",
    "300363", "300373", "300383", "300416", "300454", "300474", "300529",
    "300579", "300601", "300661", "300676", "300699", "300724", "300748",
    "300763", "300769", "300787", "300832", "300888", "300908", "300919",
    "600060", "600079", "600100", "600141", "600153", "600160", "600163",
    "600166", "600172", "600219", "600237", "600256", "600271", "600315",
    "600323", "600352", "600358", "600373", "600395", "600416", "600426",
    "600458", "600476", "600481", "600507", "600516", "600535", "600546",
    "600547", "600557", "600566", "600572", "600582", "600596", "600606",
    "600637", "600643", "600658", "600673", "600674", "600685", "600694",
    "600697", "600704", "600710", "600718", "600724", "600737", "600741",
    "600748", "600754", "600762", "600765", "600768", "600771", "600779",
    "600783", "600789", "600793", "600797", "600808", "600811", "600815",
    "600820", "600825", "600833", "600836", "600839", "600845", "600848",
    "600853", "600858", "600862", "600865", "600868",
]

# 代表性样本（30只，覆盖主要行业，用于 --quick 模式）
QUICK_SAMPLE: list[str] = [
    # 金融
    "601318", "600036", "601166", "600030",
    # 消费
    "600519", "000858", "000568", "600887",
    # 科技
    "002415", "300750", "002594", "002230",
    # 医药
    "600276", "300760", "603259",
    # 制造/电力
    "000333", "600900", "601012", "000651",
    # 周期
    "601888", "002714", "600309", "601088",
    # 基建/地产
    "601668", "000002", "601390",
    # 通信/军工
    "000063", "600760", "601728",
    # 环保/新材料
    "600585",
]


# ---------------------------------------------------------------------------
# 真实行情价格获取器
# ---------------------------------------------------------------------------
_market = MarketDataCollector()


def real_price_fetcher(code: str, start: str, end: str) -> pd.DataFrame:
    """从新浪API获取真实历史行情用于回测评估。"""
    df = _market.get_daily_quotes(code, start, end)
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "close"])
    result = pd.DataFrame()
    if "日期" in df.columns:
        result["date"] = df["日期"]
    elif "date" in df.columns:
        result["date"] = df["date"]
    else:
        result["date"] = range(len(df))
    if "收盘" in df.columns:
        result["close"] = df["收盘"].astype(float)
    elif "close" in df.columns:
        result["close"] = df["close"].astype(float)
    else:
        return pd.DataFrame(columns=["date", "close"])
    return result


# ---------------------------------------------------------------------------
# 策略工厂
# ---------------------------------------------------------------------------

STRATEGY_NAMES: dict[str, str] = {
    "S01": "供应链传导",
    "S02": "机构持仓博弈",
    "S03": "事件传播",
    "S04": "盈利超预期",
    "S05": "分析师分歧",
    "S06": "公告情绪",
    "S07": "图因子",
    "S08": "行业轮动",
    "S09": "景气传导",
    "S10": "情绪模拟",
    "S11": "叙事追踪",
}


def create_strategy(strategy_id: str) -> Optional[BaseStrategy]:
    """根据策略ID创建策略实例。"""
    try:
        if strategy_id == "S01":
            from astrategy.strategies.s01_supply_chain import SupplyChainStrategy
            return SupplyChainStrategy()
        elif strategy_id == "S02":
            from astrategy.strategies.s02_institution import InstitutionStrategy
            return InstitutionStrategy()
        elif strategy_id == "S03":
            from astrategy.strategies.s03_event_propagation import EventPropagationStrategy
            return EventPropagationStrategy()
        elif strategy_id == "S04":
            from astrategy.strategies.s04_earnings_surprise import EarningsSurpriseStrategy
            return EarningsSurpriseStrategy()
        elif strategy_id == "S05":
            from astrategy.strategies.s05_analyst_divergence import AnalystDivergenceStrategy
            return AnalystDivergenceStrategy()
        elif strategy_id == "S06":
            from astrategy.strategies.s06_announcement_sentiment import AnnouncementSentimentStrategy
            return AnnouncementSentimentStrategy()
        elif strategy_id == "S07":
            from astrategy.strategies.s07_graph_factors import GraphFactorsStrategy
            return GraphFactorsStrategy()
        elif strategy_id == "S08":
            from astrategy.strategies.s08_sector_rotation import SectorRotationStrategy
            return SectorRotationStrategy()
        elif strategy_id == "S09":
            from astrategy.strategies.s09_prosperity_transmission import ProsperityTransmissionStrategy
            return ProsperityTransmissionStrategy()
        elif strategy_id == "S10":
            from astrategy.strategies.s10_sentiment_simulation import SentimentSimulationStrategy
            return SentimentSimulationStrategy()
        elif strategy_id == "S11":
            from astrategy.strategies.s11_narrative_tracker import NarrativeTrackerStrategy
            return NarrativeTrackerStrategy()
        else:
            logger.error("Unknown strategy: %s", strategy_id)
            return None
    except Exception as e:
        logger.error("Failed to create strategy %s: %s", strategy_id, e)
        return None


STRATEGIES_NO_LLM = ["S02", "S05", "S07"]
STRATEGIES_WITH_LLM = ["S04", "S06", "S08", "S09", "S10", "S11"]
STRATEGIES_NEED_GRAPH = ["S01", "S03"]
ALL_STRATEGY_IDS = [f"S{i:02d}" for i in range(1, 12)]


# ---------------------------------------------------------------------------
# 滚动回测评估器
# ---------------------------------------------------------------------------

class RollingEvaluator:
    """对策略信号进行多窗口滚动回测。

    原理：
    - 策略用当前数据生成信号（方向 + 置信度 + 持仓天数）
    - 滚动评估：将信号"回放"到过去 N 个历史窗口
      例如信号建议持有20天：检查 T-60→T-40、T-40→T-20、T-20→T 三个窗口的实际收益
    - 如果信号在过去多个窗口都正确，说明策略有持续预测力
    """

    def __init__(self, windows: list[int] = None):
        """
        Parameters
        ----------
        windows : list[int]
            回看窗口（交易日数），每个窗口代表一个历史评估起点。
            默认: [20, 40, 60] 即检查过去1个月、2个月、3个月的表现。
        """
        self.windows = windows or [20, 40, 60]

    def evaluate_signal_rolling(
        self, signal: StrategySignal, hold_days: int = None
    ) -> list[dict]:
        """对单个信号在多个历史窗口上评估。

        Returns
        -------
        list[dict]
            每个窗口的评估结果。
        """
        hold = hold_days or signal.holding_period_days or 20
        results = []

        for lookback in self.windows:
            # 计算评估窗口的起止日期
            eval_start = datetime.now() - timedelta(days=lookback + hold + 5)
            eval_entry = datetime.now() - timedelta(days=lookback)
            eval_exit = eval_entry + timedelta(days=hold + 5)

            start_str = eval_start.strftime("%Y%m%d")
            end_str = eval_exit.strftime("%Y%m%d")

            prices = real_price_fetcher(signal.stock_code, start_str, end_str)
            if prices.empty or len(prices) < 5:
                continue

            # 找到最接近 eval_entry 的价格
            prices["date"] = pd.to_datetime(prices["date"])
            entry_dt = pd.to_datetime(eval_entry)

            # 找 entry 点
            before_entry = prices[prices["date"] <= entry_dt]
            if before_entry.empty:
                continue
            entry_price = float(before_entry.iloc[-1]["close"])
            entry_idx = before_entry.index[-1]

            # 找 exit 点（entry + hold_days 个交易日）
            after_entry = prices[prices.index > entry_idx]
            if len(after_entry) < min(hold, 3):
                continue
            exit_idx = min(hold, len(after_entry) - 1)
            exit_price = float(after_entry.iloc[exit_idx]["close"])

            actual_return = (exit_price - entry_price) / entry_price

            # 扣除交易成本 (A股往返: 佣金0.025%×2 + 印花税0.05% + 滑点0.05%×2 ≈ 0.3%)
            _ROUND_TRIP_COST = 0.003
            actual_return = actual_return - _ROUND_TRIP_COST

            # 判断方向正确性
            if signal.direction == "long":
                hit = actual_return > 0
            elif signal.direction == "avoid":
                hit = actual_return < 0
            else:
                hit = abs(actual_return) < 0.02

            results.append({
                "stock_code": signal.stock_code,
                "stock_name": signal.stock_name,
                "direction": signal.direction,
                "confidence": signal.confidence,
                "lookback_days": lookback,
                "hold_days": hold,
                "entry_price": round(entry_price, 2),
                "exit_price": round(exit_price, 2),
                "actual_return": round(actual_return, 4),
                "hit": hit,
                "expected_return": signal.expected_return,
            })

        return results

    def evaluate_batch_rolling(
        self, signals: list[StrategySignal]
    ) -> pd.DataFrame:
        """批量滚动评估。"""
        all_results = []
        for sig in signals:
            try:
                results = self.evaluate_signal_rolling(sig)
                all_results.extend(results)
            except Exception as exc:
                logger.debug("Rolling eval failed for %s: %s", sig.stock_code, exc)
        return pd.DataFrame(all_results) if all_results else pd.DataFrame()

    @staticmethod
    def compute_rolling_metrics(df: pd.DataFrame) -> dict:
        """从滚动评估结果计算汇总指标。"""
        if df.empty or "actual_return" not in df.columns:
            return {
                "hit_rate": 0.0, "avg_return": 0.0, "sharpe_ratio": 0.0,
                "max_drawdown": 0.0, "profit_factor": 0.0, "total_evals": 0,
                "windows_tested": 0, "consistency": 0.0,
            }

        valid = df.dropna(subset=["actual_return"])
        if valid.empty:
            return {
                "hit_rate": 0.0, "avg_return": 0.0, "sharpe_ratio": 0.0,
                "max_drawdown": 0.0, "profit_factor": 0.0, "total_evals": 0,
                "windows_tested": 0, "consistency": 0.0,
            }

        returns = valid["actual_return"].tolist()
        hits = valid["hit"].tolist()

        hit_rate = sum(1 for h in hits if h) / len(hits) if hits else 0
        avg_return = sum(returns) / len(returns) if returns else 0

        # Sharpe
        import math, statistics
        if len(returns) > 1:
            ret_std = statistics.stdev(returns)
            sharpe = (avg_return * 12.6) / max(ret_std * math.sqrt(12.6), 1e-9)
        else:
            sharpe = 0.0

        # Max drawdown
        cumulative = 1.0
        peak = 1.0
        max_dd = 0.0
        for r in returns:
            cumulative *= (1 + r)
            peak = max(peak, cumulative)
            dd = (peak - cumulative) / peak
            max_dd = max(max_dd, dd)

        # Profit factor
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r < 0]
        pf = sum(wins) / max(abs(sum(losses)), 1e-9) if losses else (
            float("inf") if wins else 0.0
        )

        # 一致性：每个窗口的胜率
        window_hit_rates = []
        for w in valid["lookback_days"].unique():
            w_df = valid[valid["lookback_days"] == w]
            w_hits = w_df["hit"].tolist()
            if w_hits:
                window_hit_rates.append(sum(1 for h in w_hits if h) / len(w_hits))
        consistency = min(window_hit_rates) if window_hit_rates else 0.0

        return {
            "hit_rate": round(hit_rate, 4),
            "avg_return": round(avg_return, 6),
            "sharpe_ratio": round(sharpe, 4),
            "max_drawdown": round(max_dd, 6),
            "profit_factor": round(min(pf, 99.99), 4),
            "total_evals": len(valid),
            "windows_tested": len(valid["lookback_days"].unique()),
            "consistency": round(consistency, 4),
        }


# ---------------------------------------------------------------------------
# 回测引擎
# ---------------------------------------------------------------------------

class BacktestEngine:
    """多策略严格回测引擎（含滚动评估）。"""

    def __init__(self, stock_codes: list[str], rolling_evaluator: RollingEvaluator):
        self.stock_codes = stock_codes
        self.rolling = rolling_evaluator
        self.signals: dict[str, list[StrategySignal]] = {}
        self.rolling_results: dict[str, pd.DataFrame] = {}
        self.rolling_metrics: dict[str, dict] = {}
        self.errors: dict[str, str] = {}
        self.timings: dict[str, float] = {}

    def run_strategy(self, strategy_id: str) -> tuple[list[StrategySignal], Optional[str]]:
        """运行单个策略。"""
        logger.info("=" * 60)
        logger.info("Running %s (%s) ...", strategy_id, STRATEGY_NAMES.get(strategy_id, ""))
        logger.info("=" * 60)

        strategy = create_strategy(strategy_id)
        if strategy is None:
            return [], f"策略创建失败"

        t0 = time.time()
        try:
            # S03 needs graph_id parameter
            if strategy_id == "S03":
                signals = strategy.run(self.stock_codes, graph_id="supply_chain")
            else:
                signals = strategy.run(self.stock_codes)
            elapsed = time.time() - t0
            self.timings[strategy_id] = elapsed

            if signals:
                strategy.save_signals(signals)
                logger.info("[%s] %d signals in %.1fs", strategy_id, len(signals), elapsed)
                for sig in signals[:3]:
                    logger.info(
                        "  %s %s %s conf=%.3f exp=%.2f%%",
                        sig.stock_code, sig.stock_name, sig.direction,
                        sig.confidence, sig.expected_return * 100,
                    )
                if len(signals) > 3:
                    logger.info("  ... +%d more", len(signals) - 3)
            else:
                logger.warning("[%s] 0 signals (%.1fs)", strategy_id, elapsed)
            return signals, None

        except Exception as e:
            elapsed = time.time() - t0
            self.timings[strategy_id] = elapsed
            error_msg = f"{type(e).__name__}: {str(e)[:200]}"
            logger.error("[%s] FAIL (%.1fs): %s", strategy_id, elapsed, error_msg)
            return [], error_msg

    def evaluate_rolling(self, strategy_id: str, signals: list[StrategySignal]):
        """滚动回测评估。"""
        directional = [s for s in signals if s.direction in ("long", "avoid")]
        if not directional:
            logger.info("[%s] 无方向性信号可评估", strategy_id)
            return

        logger.info("[%s] 滚动回测 %d 个方向性信号 × %d 个窗口 ...",
                     strategy_id, len(directional), len(self.rolling.windows))

        df = self.rolling.evaluate_batch_rolling(directional)
        self.rolling_results[strategy_id] = df

        if not df.empty:
            metrics = RollingEvaluator.compute_rolling_metrics(df)
            self.rolling_metrics[strategy_id] = metrics
            logger.info(
                "[%s] 胜率=%.1f%% | 平均收益=%.2f%% | 夏普=%.2f | 盈亏比=%.2f | 一致性=%.1f%% | 评估数=%d",
                strategy_id,
                metrics["hit_rate"] * 100,
                metrics["avg_return"] * 100,
                metrics["sharpe_ratio"],
                metrics["profit_factor"],
                metrics["consistency"] * 100,
                metrics["total_evals"],
            )

    def run_all(self, strategy_ids: list[str]) -> None:
        """运行并评估全部策略。"""
        for sid in strategy_ids:
            signals, error = self.run_strategy(sid)
            if error:
                self.errors[sid] = error
                self.signals[sid] = []
            else:
                self.signals[sid] = signals
                self.evaluate_rolling(sid, signals)

    def _rate_strategy(self, metrics: dict) -> str:
        """给策略打评级。"""
        hr = metrics.get("hit_rate", 0)
        sharpe = metrics.get("sharpe_ratio", 0)
        consistency = metrics.get("consistency", 0)
        pf = metrics.get("profit_factor", 0)

        score = 0
        if hr >= 0.6:
            score += 2
        elif hr >= 0.5:
            score += 1

        if sharpe >= 1.5:
            score += 2
        elif sharpe >= 0.5:
            score += 1

        if consistency >= 0.5:
            score += 1

        if pf >= 2.0:
            score += 1
        elif pf >= 1.0:
            score += 0.5

        if score >= 5:
            return "A (强烈推荐)"
        elif score >= 3.5:
            return "B (推荐)"
        elif score >= 2:
            return "C (一般)"
        elif score >= 1:
            return "D (较弱)"
        else:
            return "F (无效)"

    def generate_report(self) -> str:
        """生成完整的策略对比回测报告 + 结论。"""
        lines = [
            "# AStrategy 全策略严格回测报告",
            "",
            f"**回测日期**: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            f"**股票池**: {len(self.stock_codes)} 只股票（中证800）",
            f"**回测窗口**: {self.rolling.windows} 个交易日（多窗口滚动验证）",
            f"**信号方向**: long=买入, avoid=回避, neutral=观望（A股无做空）",
            "",
            "---",
            "",
        ]

        # 1. 运行概览
        lines.append("## 1. 策略运行概览")
        lines.append("")
        lines.append("| 策略 | 名称 | 状态 | 信号数 | 耗时 | 备注 |")
        lines.append("|------|------|------|--------|------|------|")

        for sid in sorted(self.signals.keys() | self.errors.keys()):
            name = STRATEGY_NAMES.get(sid, "")
            elapsed = self.timings.get(sid, 0)
            if sid in self.errors:
                lines.append(f"| {sid} | {name} | FAIL | 0 | {elapsed:.0f}s | {self.errors[sid][:40]} |")
            else:
                sigs = self.signals.get(sid, [])
                directional = len([s for s in sigs if s.direction in ("long", "avoid")])
                lines.append(f"| {sid} | {name} | PASS | {len(sigs)} ({directional}个方向性) | {elapsed:.0f}s | |")

        lines.append("")

        # 2. 信号方向分布
        lines.append("## 2. 信号方向分布")
        lines.append("")
        lines.append("| 策略 | 买入(Long) | 回避(Avoid) | 观望(Neutral) | 总计 |")
        lines.append("|------|-----------|-------------|---------------|------|")

        for sid in sorted(self.signals.keys()):
            sigs = self.signals[sid]
            longs = len([s for s in sigs if s.direction == "long"])
            avoids = len([s for s in sigs if s.direction == "avoid"])
            neutrals = len([s for s in sigs if s.direction == "neutral"])
            lines.append(f"| {sid} | {longs} | {avoids} | {neutrals} | {len(sigs)} |")

        lines.append("")

        # 3. 滚动回测绩效对比（核心）
        lines.append("## 3. 滚动回测绩效对比（核心指标）")
        lines.append("")
        lines.append("> 每个信号在过去多个历史窗口上验证，检验策略的**持续预测能力**。")
        lines.append("")

        if self.rolling_metrics:
            sorted_metrics = sorted(
                self.rolling_metrics.items(),
                key=lambda x: x[1].get("sharpe_ratio", 0),
                reverse=True,
            )

            lines.append("| 策略 | 胜率 | 平均收益 | 夏普比率 | 最大回撤 | 盈亏比 | 一致性 | 评估数 | 评级 |")
            lines.append("|------|------|----------|----------|----------|--------|--------|--------|------|")

            for sid, m in sorted_metrics:
                rating = self._rate_strategy(m)
                lines.append(
                    f"| {sid} ({STRATEGY_NAMES.get(sid, '')}) "
                    f"| {m['hit_rate']:.1%} "
                    f"| {m['avg_return']:.2%} "
                    f"| {m['sharpe_ratio']:.2f} "
                    f"| {m['max_drawdown']:.2%} "
                    f"| {m['profit_factor']:.2f} "
                    f"| {m['consistency']:.1%} "
                    f"| {m['total_evals']} "
                    f"| {rating} |"
                )
        else:
            lines.append("*无有效回测数据*")

        lines.append("")

        # 4. 各窗口详细表现
        lines.append("## 4. 各窗口详细表现")
        lines.append("")

        for sid, df in self.rolling_results.items():
            if df.empty:
                continue
            lines.append(f"### {sid} ({STRATEGY_NAMES.get(sid, '')})")
            lines.append("")
            lines.append("| 回看窗口 | 胜率 | 平均收益 | 样本数 |")
            lines.append("|----------|------|----------|--------|")

            for w in sorted(df["lookback_days"].unique()):
                w_df = df[df["lookback_days"] == w]
                w_hits = w_df["hit"].sum() / len(w_df) if len(w_df) > 0 else 0
                w_avg = w_df["actual_return"].mean() if len(w_df) > 0 else 0
                lines.append(f"| T-{w}日 | {w_hits:.1%} | {w_avg:.2%} | {len(w_df)} |")

            lines.append("")

        # 5. 高置信度信号 Top 20
        lines.append("## 5. 高置信度信号 (Top 20)")
        lines.append("")
        all_sigs = []
        for sid, sigs in self.signals.items():
            for s in sigs:
                if s.direction in ("long", "avoid"):
                    all_sigs.append((sid, s))

        all_sigs.sort(key=lambda x: x[1].confidence, reverse=True)

        if all_sigs:
            lines.append("| 策略 | 股票 | 方向 | 置信度 | 预期收益 | 理由 |")
            lines.append("|------|------|------|--------|----------|------|")
            for sid, sig in all_sigs[:20]:
                dir_label = "买入" if sig.direction == "long" else "回避"
                lines.append(
                    f"| {sid} | {sig.stock_code} {sig.stock_name} "
                    f"| {dir_label} | {sig.confidence:.3f} "
                    f"| {sig.expected_return:+.2%} "
                    f"| {sig.reasoning[:50]}... |"
                )
        lines.append("")

        # 6. LLM 成本
        lines.append("## 6. LLM 成本统计")
        lines.append("")
        try:
            from astrategy.llm import get_cost_tracker
            tracker = get_cost_tracker()
            daily = tracker.get_daily_cost()
            lines.append(f"- 总调用次数: {daily['call_count']}")
            lines.append(f"- 输入 Token: {daily['total_input_tokens']:,}")
            lines.append(f"- 输出 Token: {daily['total_output_tokens']:,}")
            lines.append(f"- 预估成本: ${daily['total_cost_usd']:.4f}")
        except Exception:
            lines.append("*LLM成本数据不可用*")
        lines.append("")

        # 7. 运行统计
        total_time = sum(self.timings.values())
        lines.append("## 7. 运行统计")
        lines.append("")
        lines.append(f"- 总耗时: {total_time:.0f}s ({total_time/60:.1f}min)")
        lines.append(f"- 成功策略: {len(self.signals) - len(self.errors)}/{len(self.signals) + len(self.errors)}")
        lines.append(f"- 总信号数: {sum(len(s) for s in self.signals.values())}")
        lines.append("")

        # 8. 总结论
        lines.append("## 8. 回测结论")
        lines.append("")

        if self.rolling_metrics:
            best = max(self.rolling_metrics.items(), key=lambda x: x[1].get("sharpe_ratio", 0))
            best_sid, best_m = best

            lines.append(f"### 最佳策略: {best_sid} ({STRATEGY_NAMES.get(best_sid, '')})")
            lines.append(f"- 评级: {self._rate_strategy(best_m)}")
            lines.append(f"- 胜率: {best_m['hit_rate']:.1%}")
            lines.append(f"- 年化夏普: {best_m['sharpe_ratio']:.2f}")
            lines.append(f"- 盈亏比: {best_m['profit_factor']:.2f}")
            lines.append("")

            lines.append("### 策略分级")
            lines.append("")
            for sid, m in sorted(self.rolling_metrics.items(), key=lambda x: x[1].get("sharpe_ratio", 0), reverse=True):
                rating = self._rate_strategy(m)
                conclusion = ""
                if m["hit_rate"] >= 0.55 and m["sharpe_ratio"] > 0:
                    conclusion = "有一定预测力"
                elif m["hit_rate"] < 0.45:
                    conclusion = "方向判断能力弱，需优化"
                elif m["total_evals"] < 5:
                    conclusion = "样本不足，需扩大回测"
                else:
                    conclusion = "表现一般"
                lines.append(f"- **{sid}** ({STRATEGY_NAMES.get(sid, '')}): {rating} — {conclusion}")

            lines.append("")
            lines.append("### 综合建议")
            lines.append("")

            good = [sid for sid, m in self.rolling_metrics.items() if m["hit_rate"] >= 0.55]
            weak = [sid for sid, m in self.rolling_metrics.items() if m["hit_rate"] < 0.45]

            if good:
                lines.append(f"- 推荐使用: {', '.join(good)}")
            if weak:
                lines.append(f"- 需要优化: {', '.join(weak)}")

            no_signal = [sid for sid in self.signals if sid not in self.rolling_metrics and len(self.signals[sid]) == 0]
            if no_signal:
                lines.append(f"- 未产生信号: {', '.join(no_signal)}（可能是数据覆盖不足）")

            failed = list(self.errors.keys())
            if failed:
                lines.append(f"- 运行失败: {', '.join(failed)}")

        else:
            lines.append("无有效回测数据，无法给出结论。")

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(description="AStrategy 全策略严格回测系统 v2")
    parser.add_argument(
        "--strategies", type=str, default="",
        help="逗号分隔的策略ID，如 S02,S07,S08。留空=运行全部",
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="快速模式：使用30只样本股",
    )
    parser.add_argument(
        "--no-llm", action="store_true",
        help="跳过需要LLM的策略",
    )
    parser.add_argument(
        "--no-graph", action="store_true",
        help="跳过需要图谱的策略",
    )
    parser.add_argument(
        "--windows", type=str, default="20,40,60",
        help="滚动回测窗口（交易日），如 20,40,60",
    )
    parser.add_argument(
        "--output", type=str, default="",
        help="报告输出路径",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # 确定股票池
    if args.quick:
        stock_codes = QUICK_SAMPLE
    else:
        stock_codes = fetch_csi800_codes()
    logger.info("股票池: %d 只股票", len(stock_codes))

    # 确定策略列表
    if args.strategies:
        strategy_ids = [s.strip().upper() for s in args.strategies.split(",")]
    else:
        strategy_ids = list(ALL_STRATEGY_IDS)

    if args.no_llm:
        strategy_ids = [s for s in strategy_ids if s not in STRATEGIES_WITH_LLM]
        logger.info("跳过LLM策略，只运行: %s", strategy_ids)
    if args.no_graph:
        # Check if local graph exists before skipping
        from pathlib import Path as _P
        local_graph_path = _P(__file__).parent / ".data" / "local_graph" / "supply_chain.json"
        if local_graph_path.exists():
            logger.info("发现本地图谱，S01/S03 将使用本地图谱运行")
        else:
            strategy_ids = [s for s in strategy_ids if s not in STRATEGIES_NEED_GRAPH]
            logger.info("跳过图谱策略（无本地图谱）: %s", strategy_ids)

    logger.info("待运行策略: %s", strategy_ids)

    # 解析滚动窗口
    windows = [int(w.strip()) for w in args.windows.split(",")]
    logger.info("滚动回测窗口: %s 个交易日", windows)

    # 创建滚动评估器
    rolling = RollingEvaluator(windows=windows)

    # 运行回测
    engine = BacktestEngine(stock_codes=stock_codes, rolling_evaluator=rolling)
    engine.run_all(strategy_ids)

    # 生成报告
    report = engine.generate_report()

    # 输出
    output_path = args.output or str(
        Path(__file__).resolve().parent / ".data" / "reports" / "backtest_report.md"
    )
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(report, encoding="utf-8")
    logger.info("")
    logger.info("=" * 60)
    logger.info("回测报告已保存: %s", output_path)
    logger.info("=" * 60)

    print("\n" + report)


if __name__ == "__main__":
    main()
