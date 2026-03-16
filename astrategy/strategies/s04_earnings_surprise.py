"""
Earnings Surprise Strategy (S04)
=================================

When a company's actual earnings significantly differ from analyst consensus
and the stock price has not yet fully reacted, there is an exploitable
opportunity.  This strategy:

1. Screens a universe of stocks for material earnings surprises (|surprise| > 5%).
2. Checks whether the post-earnings price action already reflects the surprise.
3. For unreacted surprises, calls the LLM to assess earnings quality and
   sustainability.
4. Generates long signals for positive sustainable surprises and short signals
   for negative structural surprises.

Typical holding period: 20-60 trading days.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from astrategy.config import settings
from astrategy.data_collector.fundamental import FundamentalCollector
from astrategy.data_collector.market_data import MarketDataCollector
from astrategy.data_collector.research import ResearchCollector
from astrategy.llm.client import LLMClient
from astrategy.strategies.base import BaseStrategy, StrategySignal

logger = logging.getLogger("astrategy.strategies.s04_earnings_surprise")

_CST = timezone(timedelta(hours=8))

# Default earnings surprise threshold (absolute percentage)
DEFAULT_SURPRISE_THRESHOLD = 5.0

# Post-earnings observation window (trading days)
POST_EARNINGS_WINDOW = 10

# Prompt template path (optional file; inline fallback used if missing)
_PROMPT_TEMPLATE_PATH = (
    Path(__file__).resolve().parent.parent
    / "prompt_templates"
    / "earnings_analysis.txt"
)

# ---------------------------------------------------------------------------
# Inline prompt fallback
# ---------------------------------------------------------------------------
_EARNINGS_ANALYSIS_PROMPT = """\
你是一位资深A股财报分析师。请根据以下财务和市场数据，深入分析该公司的业绩情况。

## 基本信息
股票代码: {stock_code}
股票名称: {stock_name}

## 财务数据
- 营业收入: {revenue}
- 净利润: {net_profit}
- 营收同比增长: {revenue_yoy}%
- 净利润同比增长: {profit_yoy}%
- 毛利率: {gross_margin}%
- 毛利率变动: {gross_margin_change} 个百分点

## 业绩预期差
- 实际每股收益(EPS): {actual_eps}
- 分析师一致预期EPS: {consensus_eps}
- 业绩预期差: {surprise_pct}%
- 预测机构数: {forecast_count}

## 财报发布后市场反应
- 财报后累计涨跌幅: {post_earnings_return}%
- 观察窗口: 最近{observation_window}个交易日

请以JSON格式输出分析结果，包含以下字段:
{{
  "earnings_quality": "high/medium/low",
  "quality_reasoning": "业绩质量判断理由（收入驱动 vs 一次性因素 vs 成本节约等）",
  "sustainability": "high/medium/low",
  "sustainability_reasoning": "趋势可持续性分析",
  "risk_factors": ["风险因素1", "风险因素2"],
  "outlook_change": "improve/stable/deteriorate",
  "outlook_reasoning": "基本面展望变化分析",
  "confidence": 0.0-1.0,
  "recommended_action": "long/short/neutral",
  "expected_return_pct": 预期收益率百分比（数字）,
  "holding_period_days": 建议持仓天数（20-60）,
  "summary": "一句话总结"
}}
"""


class EarningsSurpriseStrategy(BaseStrategy):
    """Earnings surprise strategy combining quantitative screening with LLM analysis.

    Parameters
    ----------
    surprise_threshold : float
        Minimum absolute surprise percentage to qualify as a candidate (default 5%).
    holding_days : int
        Default holding period for signals (default 40 days).
    signal_dir : Path | str | None
        Override for the signal persistence directory.
    """

    def __init__(
        self,
        surprise_threshold: float = DEFAULT_SURPRISE_THRESHOLD,
        holding_days: int = 40,
        signal_dir: Path | str | None = None,
    ) -> None:
        super().__init__(signal_dir=signal_dir)
        self._surprise_threshold = surprise_threshold
        self._holding_days = holding_days

        self._fundamental = FundamentalCollector()
        self._market = MarketDataCollector()
        self._research = ResearchCollector()
        self._llm = LLMClient()
        self._llm.set_strategy(self.name)

    # ── BaseStrategy interface ────────────────────────────────────────

    @property
    def name(self) -> str:
        return "earnings_surprise"

    # ==================================================================
    # 1. get_earnings_data
    # ==================================================================

    def get_earnings_data(self, stock_code: str) -> dict:
        """Fetch latest financial summary and consensus forecast, then compute
        the earnings surprise percentage.

        Returns a dict with keys:
            stock_code, stock_name,
            revenue, net_profit, revenue_yoy, profit_yoy,
            gross_margin, gross_margin_change,
            actual_eps, consensus_eps, surprise_pct,
            forecast_count, post_earnings_return,
            has_data (bool)
        """
        result: Dict[str, Any] = {
            "stock_code": stock_code,
            "stock_name": stock_code,
            "revenue": None,
            "net_profit": None,
            "revenue_yoy": None,
            "profit_yoy": None,
            "gross_margin": None,
            "gross_margin_change": None,
            "actual_eps": None,
            "consensus_eps": None,
            "surprise_pct": None,
            "forecast_count": 0,
            "post_earnings_return": None,
            "has_data": False,
        }

        # ── Stock name ───────────────────────────────────────────────
        info = self._fundamental.get_stock_info(stock_code)
        stock_name = str(info.get("股票简称", stock_code))
        result["stock_name"] = stock_name

        # ── Financial summary ────────────────────────────────────────
        fin = self._fundamental.get_financial_summary(stock_code)
        if fin is None or fin.empty:
            logger.warning("No financial summary for %s", stock_code)
            return result

        actual_eps = self._extract_metric(fin, ["基本每股收益", "每股收益", "EPS"])
        revenue = self._extract_metric(fin, ["营业总收入", "营业收入", "revenue"])
        net_profit = self._extract_metric(fin, ["净利润", "归母净利润", "net_profit"])
        revenue_yoy = self._extract_metric(
            fin, ["营业总收入同比增长率", "营收同比增长", "营收同比", "revenue_yoy"]
        )
        profit_yoy = self._extract_metric(
            fin, ["净利润同比增长率", "净利润同比", "profit_yoy"]
        )
        gross_margin = self._extract_metric(
            fin, ["销售毛利率", "毛利率", "gross_margin"]
        )

        result["revenue"] = revenue
        result["net_profit"] = net_profit
        result["revenue_yoy"] = revenue_yoy
        result["profit_yoy"] = profit_yoy
        result["gross_margin"] = gross_margin
        result["actual_eps"] = actual_eps

        # Gross margin change: diff between latest two periods
        gross_margin_change = self._compute_metric_change(
            fin, ["销售毛利率", "毛利率", "gross_margin"]
        )
        result["gross_margin_change"] = gross_margin_change

        # ── Consensus forecast ───────────────────────────────────────
        consensus = self._research.get_consensus_forecast(stock_code)
        consensus_eps = self._extract_consensus_eps(consensus)
        result["consensus_eps"] = consensus_eps
        result["forecast_count"] = consensus.get("预测机构数", 0)

        # ── Surprise calculation ─────────────────────────────────────
        if actual_eps is not None and consensus_eps is not None and abs(consensus_eps) > 1e-6:
            surprise_pct = (actual_eps - consensus_eps) / abs(consensus_eps) * 100.0
            result["surprise_pct"] = round(surprise_pct, 2)
        elif actual_eps is not None and profit_yoy is not None:
            # Fallback: use YoY profit growth as a rough proxy for surprise
            # if consensus data is unavailable
            result["surprise_pct"] = round(profit_yoy, 2)
            logger.debug(
                "No consensus EPS for %s; using profit_yoy (%.1f%%) as surprise proxy",
                stock_code, profit_yoy,
            )

        # ── Post-earnings price reaction ─────────────────────────────
        post_ret = self._compute_post_earnings_return(stock_code)
        result["post_earnings_return"] = post_ret

        result["has_data"] = result["surprise_pct"] is not None
        return result

    # ==================================================================
    # 2. screen_candidates
    # ==================================================================

    def screen_candidates(self, stock_codes: list[str]) -> list[dict]:
        """Batch screen stocks for earnings surprises.

        Returns only those where:
        - |surprise_pct| > threshold
        - Price reaction is small relative to the surprise direction
          (i.e. the surprise has not yet been fully priced in)
        """
        candidates: list[dict] = []

        for code in stock_codes:
            try:
                data = self.get_earnings_data(code)
            except Exception as exc:
                logger.warning("Failed to get earnings data for %s: %s", code, exc)
                continue

            if not data.get("has_data"):
                continue

            surprise = data["surprise_pct"]
            if surprise is None or abs(surprise) < self._surprise_threshold:
                continue

            # Check if the price has already reacted in the surprise direction
            post_ret = data.get("post_earnings_return")
            if post_ret is not None:
                # If surprise is positive and price already up significantly,
                # or surprise is negative and price already down significantly,
                # the market has reacted -- skip.
                reaction_ratio = self._compute_reaction_ratio(surprise, post_ret)
                if reaction_ratio > 0.8:
                    logger.debug(
                        "Skipping %s: price already reacted (surprise=%.1f%%, "
                        "post_return=%.1f%%, reaction_ratio=%.2f)",
                        code, surprise, post_ret, reaction_ratio,
                    )
                    continue

            data["reaction_ratio"] = (
                self._compute_reaction_ratio(surprise, post_ret)
                if post_ret is not None
                else 0.0
            )
            candidates.append(data)

        # Sort by absolute surprise descending
        candidates.sort(key=lambda d: abs(d.get("surprise_pct", 0)), reverse=True)

        logger.info(
            "Screened %d stocks, found %d candidates with |surprise| > %.1f%%",
            len(stock_codes), len(candidates), self._surprise_threshold,
        )
        return candidates

    # ==================================================================
    # 3. llm_deep_analysis
    # ==================================================================

    def llm_deep_analysis(
        self,
        stock_code: str,
        earnings_data: dict,
    ) -> dict:
        """Call the LLM to assess earnings quality, sustainability, and outlook.

        Returns the parsed JSON analysis dict, or a rule-based fallback on failure.
        """
        # ── Load prompt template ─────────────────────────────────────
        try:
            template = _PROMPT_TEMPLATE_PATH.read_text(encoding="utf-8")
        except FileNotFoundError:
            template = _EARNINGS_ANALYSIS_PROMPT

        prompt = template.format(
            stock_code=stock_code,
            stock_name=earnings_data.get("stock_name", stock_code),
            revenue=self._fmt_value(earnings_data.get("revenue"), "元"),
            net_profit=self._fmt_value(earnings_data.get("net_profit"), "元"),
            revenue_yoy=self._fmt_pct(earnings_data.get("revenue_yoy")),
            profit_yoy=self._fmt_pct(earnings_data.get("profit_yoy")),
            gross_margin=self._fmt_pct(earnings_data.get("gross_margin")),
            gross_margin_change=self._fmt_pct(earnings_data.get("gross_margin_change")),
            actual_eps=self._fmt_value(earnings_data.get("actual_eps")),
            consensus_eps=self._fmt_value(earnings_data.get("consensus_eps")),
            surprise_pct=self._fmt_pct(earnings_data.get("surprise_pct")),
            forecast_count=earnings_data.get("forecast_count", 0),
            post_earnings_return=self._fmt_pct(earnings_data.get("post_earnings_return")),
            observation_window=POST_EARNINGS_WINDOW,
        )

        messages = [
            {
                "role": "system",
                "content": (
                    "你是一位资深A股财报分析师，擅长判断业绩预期差的质量和可持续性。"
                    "请严格按JSON格式输出分析结果。"
                ),
            },
            {"role": "user", "content": prompt},
        ]

        try:
            result = self._llm.chat_json(messages=messages, max_tokens=2048)
            return result
        except Exception as exc:
            logger.error("LLM earnings analysis failed for %s: %s", stock_code, exc)
            return self._build_fallback_analysis(earnings_data)

    # ==================================================================
    # 4. compute_signal
    # ==================================================================

    def compute_signal(
        self,
        stock_code: str,
        earnings: dict,
        analysis: dict,
    ) -> StrategySignal:
        """Combine quantitative earnings data with LLM analysis to produce
        a StrategySignal.

        Direction logic:
        - long  if positive surprise + sustainable quality
        - short if negative surprise + structural (not one-time)
        - neutral otherwise

        Confidence is derived from surprise magnitude, LLM quality assessment,
        and forecast institution count.
        """
        surprise = earnings.get("surprise_pct", 0.0) or 0.0
        quality = analysis.get("earnings_quality", "medium")
        sustainability = analysis.get("sustainability", "medium")
        llm_action = analysis.get("recommended_action", "neutral")
        llm_confidence = analysis.get("confidence", 0.5)
        llm_expected_return = analysis.get("expected_return_pct", None)
        llm_holding = analysis.get("holding_period_days", self._holding_days)

        # ── Direction ────────────────────────────────────────────────
        if surprise > 0 and sustainability in ("high", "medium") and quality != "low":
            direction = "long"
        elif surprise < 0 and sustainability in ("high", "medium") and quality != "low":
            direction = "short"
        else:
            direction = "neutral"

        # If LLM explicitly disagrees, give it some weight
        if llm_action in ("long", "short", "neutral") and llm_action != direction:
            # LLM override only when confidence is high
            if llm_confidence >= 0.7:
                direction = llm_action

        # ── Confidence ───────────────────────────────────────────────
        # Base confidence from surprise magnitude (sigmoid-like mapping)
        abs_surprise = abs(surprise)
        magnitude_conf = min(1.0, abs_surprise / 30.0)  # 30% surprise -> 1.0

        # Quality multiplier
        quality_mult = {"high": 1.2, "medium": 1.0, "low": 0.6}.get(quality, 1.0)

        # Sustainability multiplier
        sustain_mult = {"high": 1.2, "medium": 1.0, "low": 0.7}.get(sustainability, 1.0)

        # Forecast coverage bonus (more analysts = more reliable consensus)
        forecast_count = earnings.get("forecast_count", 0) or 0
        coverage_bonus = min(0.1, forecast_count * 0.01)

        confidence = magnitude_conf * quality_mult * sustain_mult + coverage_bonus
        # Blend with LLM confidence
        confidence = 0.6 * confidence + 0.4 * llm_confidence
        confidence = max(0.0, min(1.0, confidence))

        # ── Expected return ──────────────────────────────────────────
        if llm_expected_return is not None:
            try:
                expected_return = float(llm_expected_return) / 100.0
            except (ValueError, TypeError):
                expected_return = self._estimate_return(surprise, quality)
        else:
            expected_return = self._estimate_return(surprise, quality)

        # ── Holding period ───────────────────────────────────────────
        holding_days = llm_holding if 20 <= llm_holding <= 60 else self._holding_days

        # ── Reasoning ────────────────────────────────────────────────
        summary = analysis.get("summary", "")
        outlook = analysis.get("outlook_reasoning", "")
        risk_factors = analysis.get("risk_factors", [])

        reasoning_parts = []
        if summary:
            reasoning_parts.append(summary)
        reasoning_parts.append(
            f"Surprise={surprise:+.1f}%, quality={quality}, sustainability={sustainability}"
        )
        if outlook:
            reasoning_parts.append(f"Outlook: {outlook}")
        if risk_factors:
            reasoning_parts.append(f"Risks: {', '.join(risk_factors[:3])}")

        reasoning = "; ".join(reasoning_parts)

        # ── Metadata ─────────────────────────────────────────────────
        metadata: Dict[str, Any] = {
            "surprise_pct": earnings.get("surprise_pct"),
            "actual_eps": earnings.get("actual_eps"),
            "consensus_eps": earnings.get("consensus_eps"),
            "revenue_yoy": earnings.get("revenue_yoy"),
            "profit_yoy": earnings.get("profit_yoy"),
            "gross_margin_change": earnings.get("gross_margin_change"),
            "earnings_quality": quality,
            "post_earnings_return": earnings.get("post_earnings_return"),
            "llm_analysis": analysis.get("summary", ""),
        }

        return StrategySignal(
            strategy_name=self.name,
            stock_code=stock_code,
            stock_name=earnings.get("stock_name", stock_code),
            direction=direction,
            confidence=round(confidence, 2),
            expected_return=round(expected_return, 4),
            holding_period_days=holding_days,
            reasoning=reasoning,
            metadata=metadata,
        )

    # ==================================================================
    # 5. run
    # ==================================================================

    def run(self, stock_codes: list[str] | None = None) -> list[StrategySignal]:
        """Full earnings surprise pipeline.

        1. Screen all candidates (cheap, no LLM).
        2. Deep analyse only candidates with significant surprise (expensive, uses LLM).
        3. Generate signals.

        Parameters
        ----------
        stock_codes:
            Universe of stock codes to screen.  If ``None``, returns empty.

        Returns
        -------
        list[StrategySignal]
        """
        if not stock_codes:
            logger.warning("[%s] No stock codes provided; returning empty signals.", self.name)
            return []

        logger.info("[%s] Starting earnings surprise screening for %d stocks ...", self.name, len(stock_codes))

        # Step 1: Screen candidates (no LLM)
        candidates = self.screen_candidates(stock_codes)
        if not candidates:
            logger.info("[%s] No earnings surprise candidates found.", self.name)
            return []

        logger.info("[%s] Found %d candidates; running LLM deep analysis ...", self.name, len(candidates))

        # Step 2 & 3: Deep analyse + generate signals
        # Limit to avoid excessive LLM calls
        max_candidates = getattr(settings.strategy, "max_stocks_per_run", 20)
        candidates = candidates[:max_candidates]

        signals: list[StrategySignal] = []
        for candidate in candidates:
            code = candidate["stock_code"]
            try:
                analysis = self.llm_deep_analysis(code, candidate)
                signal = self.compute_signal(code, candidate, analysis)
                signals.append(signal)
            except Exception as exc:
                logger.error("[%s] Failed to analyse %s: %s", self.name, code, exc)

        logger.info("[%s] Generated %d signals.", self.name, len(signals))
        return signals

    # ==================================================================
    # 6. run_single
    # ==================================================================

    def run_single(self, stock_code: str) -> list[StrategySignal]:
        """Run the earnings surprise strategy for a single stock.

        Unlike ``run()``, this always performs the LLM deep analysis regardless
        of the surprise threshold (though confidence will be lower for small
        surprises).
        """
        logger.info("[%s] Running single-stock analysis for %s ...", self.name, stock_code)

        earnings = self.get_earnings_data(stock_code)
        if not earnings.get("has_data"):
            logger.warning("[%s] No earnings data available for %s", self.name, stock_code)
            stock_name = earnings.get("stock_name", stock_code)
            return [
                StrategySignal(
                    strategy_name=self.name,
                    stock_code=stock_code,
                    stock_name=stock_name,
                    direction="neutral",
                    confidence=0.1,
                    expected_return=0.0,
                    holding_period_days=self._holding_days,
                    reasoning="Insufficient earnings data to generate signal",
                    metadata={"earnings_quality": "low", "surprise_pct": None},
                )
            ]

        analysis = self.llm_deep_analysis(stock_code, earnings)
        signal = self.compute_signal(stock_code, earnings, analysis)
        return [signal]

    # ==================================================================
    # Internal helpers
    # ==================================================================

    @staticmethod
    def _extract_metric(
        df: pd.DataFrame,
        candidate_columns: list[str],
    ) -> Optional[float]:
        """Extract the first available numeric metric from a financial summary DataFrame.

        Searches for column names containing any of the candidate strings,
        then returns the latest non-NaN value.
        """
        for candidate in candidate_columns:
            matching_cols = [
                c for c in df.columns
                if candidate.lower() in str(c).lower()
            ]
            for col in matching_cols:
                series = pd.to_numeric(df[col], errors="coerce").dropna()
                if not series.empty:
                    return float(series.iloc[0])  # latest period first
        return None

    @staticmethod
    def _compute_metric_change(
        df: pd.DataFrame,
        candidate_columns: list[str],
    ) -> Optional[float]:
        """Compute the change (latest - prior) for a metric across two reporting periods."""
        for candidate in candidate_columns:
            matching_cols = [
                c for c in df.columns
                if candidate.lower() in str(c).lower()
            ]
            for col in matching_cols:
                series = pd.to_numeric(df[col], errors="coerce").dropna()
                if len(series) >= 2:
                    return round(float(series.iloc[0]) - float(series.iloc[1]), 2)
        return None

    @staticmethod
    def _extract_consensus_eps(consensus: dict) -> Optional[float]:
        """Extract a consensus EPS value from the forecast dict.

        Looks for averaged EPS-related keys produced by ResearchCollector.
        """
        eps_keys = [
            k for k in consensus
            if ("eps" in k.lower() or "每股收益" in k or "每股盈利" in k)
            and "平均" in k
        ]
        for key in eps_keys:
            try:
                return float(consensus[key])
            except (ValueError, TypeError):
                continue

        # Fallback: try to derive from average net profit and shares
        # (not always available)
        return None

    def _compute_post_earnings_return(self, stock_code: str) -> Optional[float]:
        """Compute the cumulative return over the POST_EARNINGS_WINDOW trading days
        ending at the most recent trading day.

        This approximates the post-announcement price reaction.
        """
        end_dt = datetime.now(tz=_CST)
        start_dt = end_dt - timedelta(days=POST_EARNINGS_WINDOW * 2)  # buffer for weekends
        start_str = start_dt.strftime("%Y%m%d")
        end_str = end_dt.strftime("%Y%m%d")

        try:
            df = self._market.get_daily_quotes(stock_code, start_str, end_str)
            if df is None or df.empty or len(df) < 2:
                return None

            # Use the last POST_EARNINGS_WINDOW trading days
            df = df.tail(POST_EARNINGS_WINDOW + 1)
            if len(df) < 2:
                return None

            closes = df["收盘"].astype(float).values
            ret = (closes[-1] / closes[0] - 1.0) * 100.0
            return round(ret, 2)
        except Exception as exc:
            logger.debug("Failed to compute post-earnings return for %s: %s", stock_code, exc)
            return None

    @staticmethod
    def _compute_reaction_ratio(surprise_pct: float, post_return: float) -> float:
        """Estimate how much of the surprise has been priced in.

        Returns a value in [0, 1+] where:
        - 0 means no reaction in the expected direction
        - 1 means the price has moved at least as much as a typical surprise response
        - >1 means over-reaction

        Uses a simple heuristic: historical studies suggest that a 10% earnings
        surprise typically leads to a 3-5% price move within 10 days.
        """
        if abs(surprise_pct) < 1e-6:
            return 1.0

        # Expected price impact: ~35% of surprise magnitude (empirical estimate)
        expected_impact = abs(surprise_pct) * 0.35

        if expected_impact < 1e-6:
            return 1.0

        # Check if post_return is in the same direction as the surprise
        same_direction = (surprise_pct > 0 and post_return > 0) or (
            surprise_pct < 0 and post_return < 0
        )

        if same_direction:
            return min(2.0, abs(post_return) / expected_impact)
        else:
            # Price moved opposite to surprise -- strong unreacted signal
            return 0.0

    @staticmethod
    def _estimate_return(surprise_pct: float, quality: str) -> float:
        """Estimate expected return from surprise magnitude and quality.

        Based on post-earnings-announcement drift (PEAD) literature:
        - Large surprises with high quality tend to drift 3-8% over 60 days
        - Low quality surprises mean-revert
        """
        base = abs(surprise_pct) * 0.003  # 0.3% expected return per 1% surprise

        quality_mult = {"high": 1.5, "medium": 1.0, "low": 0.3}.get(quality, 1.0)
        ret = base * quality_mult

        # Cap at 15%
        ret = min(ret, 0.15)

        # Sign matches surprise direction
        if surprise_pct < 0:
            ret = -ret

        return round(ret, 4)

    @staticmethod
    def _build_fallback_analysis(earnings: dict) -> dict:
        """Rule-based fallback analysis when LLM is unavailable."""
        surprise = earnings.get("surprise_pct", 0.0) or 0.0
        revenue_yoy = earnings.get("revenue_yoy")
        profit_yoy = earnings.get("profit_yoy")
        gross_margin_change = earnings.get("gross_margin_change")

        # Heuristic quality assessment
        quality = "medium"
        if revenue_yoy is not None and profit_yoy is not None:
            # Revenue-driven growth (both revenue and profit growing) -> higher quality
            if revenue_yoy > 10 and profit_yoy > 10:
                quality = "high"
            elif revenue_yoy < 0 and profit_yoy > 0:
                # Profit up but revenue down -> likely cost-cutting, lower quality
                quality = "low"

        sustainability = "medium"
        if gross_margin_change is not None:
            if gross_margin_change > 2:
                sustainability = "high"
            elif gross_margin_change < -2:
                sustainability = "low"

        if surprise > 0:
            action = "long" if quality != "low" else "neutral"
        elif surprise < 0:
            action = "short" if quality != "low" else "neutral"
        else:
            action = "neutral"

        return {
            "earnings_quality": quality,
            "quality_reasoning": "Rule-based assessment (LLM unavailable)",
            "sustainability": sustainability,
            "sustainability_reasoning": "Based on gross margin trend",
            "risk_factors": ["LLM analysis unavailable; rule-based fallback only"],
            "outlook_change": "stable",
            "outlook_reasoning": "Insufficient data for detailed outlook",
            "confidence": 0.4,
            "recommended_action": action,
            "expected_return_pct": round(abs(surprise) * 0.3, 1),
            "holding_period_days": 40,
            "summary": (
                f"{'正向' if surprise > 0 else '负向'}业绩预期差 {abs(surprise):.1f}%, "
                f"业绩质量{quality}, 可持续性{sustainability} (规则判断)"
            ),
        }

    @staticmethod
    def _fmt_value(val: Any, unit: str = "") -> str:
        """Format a numeric value for display in the prompt."""
        if val is None:
            return "N/A"
        try:
            v = float(val)
            if abs(v) >= 1e8:
                return f"{v / 1e8:.2f}亿{unit}"
            elif abs(v) >= 1e4:
                return f"{v / 1e4:.2f}万{unit}"
            else:
                return f"{v:.4f}{unit}"
        except (ValueError, TypeError):
            return str(val)

    @staticmethod
    def _fmt_pct(val: Any) -> str:
        """Format a percentage value for display."""
        if val is None:
            return "N/A"
        try:
            return f"{float(val):.2f}"
        except (ValueError, TypeError):
            return str(val)
