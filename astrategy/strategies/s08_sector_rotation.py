"""
Sector Rotation Strategy (行业轮动策略)
========================================
Analyses the macro environment, industry performance, and fund flows to
predict which sectors will outperform in the coming weeks.  Combines
quantitative momentum signals with LLM-based reasoning to generate
actionable sector-level signals that are then expanded to constituent
stocks.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import akshare as ak
import pandas as pd

from astrategy.config import settings
from astrategy.data_collector.em_fallback import (
    call_with_timeout,
    get_industry_constituents_safe,
    lookup_stock_industry,
    lookup_stock_name,
    scan_industry_returns_sina,
)
from astrategy.data_collector.macro import MacroCollector
from astrategy.data_collector.market_data import MarketDataCollector
from astrategy.llm import create_llm_client
from astrategy.strategies.base import BaseStrategy, StrategySignal

logger = logging.getLogger(__name__)

_CST = timezone(timedelta(hours=8))

# Prompt template path
_PROMPT_TEMPLATE_PATH = (
    Path(__file__).resolve().parent.parent / "prompt_templates" / "industry_rotation.txt"
)

# Shenwan L1 industry list (申万一级31行业) – used as fallback when the API
# doesn't return a clean set, and for mapping stock -> industry.
SW_L1_INDUSTRIES: list[str] = [
    "农林牧渔", "基础化工", "钢铁", "有色金属", "电子", "汽车",
    "家用电器", "食品饮料", "纺织服饰", "轻工制造", "医药生物",
    "公用事业", "交通运输", "房地产", "商贸零售", "社会服务",
    "银行", "非银金融", "综合", "建筑材料", "建筑装饰", "电力设备",
    "机械设备", "国防军工", "计算机", "传媒", "通信", "煤炭",
    "石油石化", "环保", "美容护理",
]


class SectorRotationStrategy(BaseStrategy):
    """Sector rotation strategy combining quant momentum with LLM analysis."""

    def __init__(self, signal_dir: Path | str | None = None) -> None:
        super().__init__(signal_dir=signal_dir)
        self._market = MarketDataCollector()
        self._macro = MacroCollector()
        self._llm = create_llm_client(strategy_name=self.name)

    # ── identity ──────────────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "sector_rotation"

    # ==================================================================
    # 1. scan_industry_performance
    # ==================================================================

    def scan_industry_performance(self, lookback_days: int = 20) -> pd.DataFrame:
        """Fetch all industry board data and compute momentum metrics.

        Uses Sina-based representative stocks per industry to avoid hanging
        on blocked East Money APIs.

        Returns a DataFrame with columns:
            板块名称, 板块代码, 最新价, 涨跌幅,
            return_5d, return_10d, return_20d,
            relative_strength, volume_change_pct, momentum_rank
        """
        return scan_industry_returns_sina(lookback_days=lookback_days)

    # ==================================================================
    # 2. analyze_fund_flows
    # ==================================================================

    def analyze_fund_flows(self, days: int = 20) -> pd.DataFrame:
        """Analyse northbound capital flow and detect acceleration trends.

        Returns a DataFrame with columns:
            日期, 当日净流入, flow_5d_ma, flow_10d_ma,
            flow_acceleration, flow_trend
        """
        df_north = self._market.get_north_flow()
        if df_north is None or df_north.empty:
            logger.warning("No northbound flow data available.")
            return pd.DataFrame()

        # Standardise column names – the API column names vary
        date_col = df_north.columns[0]
        value_col = None
        for c in df_north.columns:
            if "净流入" in str(c) or "净买" in str(c):
                value_col = c
                break
        if value_col is None:
            # Fallback: use second column
            value_col = df_north.columns[1] if len(df_north.columns) > 1 else df_north.columns[0]

        df = df_north[[date_col, value_col]].copy()
        df.columns = ["日期", "当日净流入"]
        df["当日净流入"] = pd.to_numeric(df["当日净流入"], errors="coerce")
        df = df.dropna(subset=["当日净流入"]).tail(days + 10).reset_index(drop=True)

        if len(df) < 5:
            return df

        df["flow_5d_ma"] = df["当日净流入"].rolling(5, min_periods=1).mean().round(2)
        df["flow_10d_ma"] = df["当日净流入"].rolling(10, min_periods=1).mean().round(2)

        # Acceleration: 5d MA change over last 5 days
        df["flow_acceleration"] = df["flow_5d_ma"].diff(5).round(2)

        # Simple trend label
        def _trend_label(row):
            if pd.isna(row["flow_acceleration"]):
                return "neutral"
            if row["flow_5d_ma"] > 0 and row["flow_acceleration"] > 0:
                return "accelerating_inflow"
            if row["flow_5d_ma"] > 0 and row["flow_acceleration"] <= 0:
                return "decelerating_inflow"
            if row["flow_5d_ma"] <= 0 and row["flow_acceleration"] < 0:
                return "accelerating_outflow"
            if row["flow_5d_ma"] <= 0 and row["flow_acceleration"] >= 0:
                return "decelerating_outflow"
            return "neutral"

        df["flow_trend"] = df.apply(_trend_label, axis=1)
        df = df.tail(days).reset_index(drop=True)
        return df

    # ==================================================================
    # 3. get_macro_context
    # ==================================================================

    def get_macro_context(self) -> dict:
        """Fetch PMI, CPI, and monetary data; determine the economic cycle phase.

        Returns a dict with keys:
            pmi_latest, pmi_trend, cpi_latest, cpi_trend,
            m2_growth, macro_phase, phase_description,
            raw_pmi, raw_cpi
        """
        result: Dict[str, Any] = {
            "pmi_latest": None,
            "pmi_trend": "unknown",
            "cpi_latest": None,
            "cpi_trend": "unknown",
            "m2_growth": None,
            "macro_phase": "unknown",
            "phase_description": "",
            "raw_pmi": "",
            "raw_cpi": "",
        }

        # ── PMI ───────────────────────────────────────────────────────
        df_pmi = self._macro.get_pmi(months=6)
        if df_pmi is not None and not df_pmi.empty:
            # Jin-10 format: columns = ['商品', '日期', '今值', '预测值', '前值']
            # Other formats may have: '制造业PMI', or similar
            pmi_col = None
            for c in df_pmi.columns:
                cstr = str(c)
                if cstr == "今值":
                    pmi_col = c
                    break
                if "制造业" in cstr or "pmi" in cstr.lower():
                    pmi_col = c
                    break
            if pmi_col is None:
                pmi_col = df_pmi.columns[2] if len(df_pmi.columns) > 2 else df_pmi.columns[-1]

            pmi_values = pd.to_numeric(df_pmi[pmi_col], errors="coerce").dropna().tolist()
            if pmi_values:
                result["pmi_latest"] = pmi_values[-1]
                if len(pmi_values) >= 3:
                    if pmi_values[-1] > pmi_values[-3]:
                        result["pmi_trend"] = "上行"
                    elif pmi_values[-1] < pmi_values[-3]:
                        result["pmi_trend"] = "下行"
                    else:
                        result["pmi_trend"] = "平稳"
            result["raw_pmi"] = df_pmi.tail(3).to_string(index=False)

        # ── CPI ───────────────────────────────────────────────────────
        df_cpi = self._macro.get_cpi(months=6)
        if df_cpi is not None and not df_cpi.empty:
            # Jin-10 format: columns = ['商品', '日期', '今值', '预测值', '前值']
            cpi_col = None
            for c in df_cpi.columns:
                cstr = str(c)
                if cstr == "今值":
                    cpi_col = c
                    break
                if "同比" in cstr or "增长" in cstr or "cpi" in cstr.lower():
                    cpi_col = c
                    break
            if cpi_col is None:
                cpi_col = df_cpi.columns[2] if len(df_cpi.columns) > 2 else df_cpi.columns[-1]

            cpi_values = pd.to_numeric(df_cpi[cpi_col], errors="coerce").dropna().tolist()
            if cpi_values:
                result["cpi_latest"] = cpi_values[-1]
                if len(cpi_values) >= 3:
                    if cpi_values[-1] > cpi_values[-3]:
                        result["cpi_trend"] = "上行"
                    elif cpi_values[-1] < cpi_values[-3]:
                        result["cpi_trend"] = "下行"
                    else:
                        result["cpi_trend"] = "平稳"
            result["raw_cpi"] = df_cpi.tail(3).to_string(index=False)

        # ── M2 ────────────────────────────────────────────────────────
        df_m2 = self._macro.get_monetary_data()
        if df_m2 is not None and not df_m2.empty:
            # Jin-10 format: columns = ['商品', '日期', '今值', '预测值', '前值']
            m2_col = None
            for c in df_m2.columns:
                cstr = str(c)
                if cstr == "今值":
                    m2_col = c
                    break
                if "同比" in cstr or "增长" in cstr or "m2" in cstr.lower():
                    m2_col = c
                    break
            if m2_col is not None:
                m2_vals = pd.to_numeric(df_m2[m2_col], errors="coerce").dropna().tolist()
                if m2_vals:
                    result["m2_growth"] = m2_vals[-1]

        # ── Determine macro phase ────────────────────────────────────
        pmi = result["pmi_latest"]
        cpi = result["cpi_latest"]

        if pmi is not None and cpi is not None:
            if pmi > 50 and cpi < 3:
                result["macro_phase"] = "recovery"
                result["phase_description"] = (
                    "复苏期：经济扩张（PMI>50）叠加低通胀（CPI<3%），"
                    "有利于成长股和顺周期行业。"
                )
            elif pmi > 50 and cpi >= 3:
                result["macro_phase"] = "expansion"
                result["phase_description"] = (
                    "过热期：经济强劲扩张（PMI>50）叠加通胀上行（CPI>=3%），"
                    "关注上游资源品和消费龙头。"
                )
            elif pmi <= 50 and cpi >= 3:
                result["macro_phase"] = "slowdown"
                result["phase_description"] = (
                    "滞胀期：经济放缓（PMI<=50）叠加高通胀（CPI>=3%），"
                    "防御为主，关注必需消费和公用事业。"
                )
            else:
                result["macro_phase"] = "contraction"
                result["phase_description"] = (
                    "衰退期：经济收缩（PMI<=50）叠加低通胀（CPI<3%），"
                    "关注政策宽松受益板块和高股息防御板块。"
                )
        elif pmi is not None:
            if pmi > 50:
                result["macro_phase"] = "recovery"
                result["phase_description"] = "PMI处于扩张区间（>50），经济整体向好。"
            else:
                result["macro_phase"] = "contraction"
                result["phase_description"] = "PMI处于收缩区间（<=50），经济承压。"

        return result

    # ==================================================================
    # 4. llm_rotation_analysis
    # ==================================================================

    def llm_rotation_analysis(
        self,
        industry_data: pd.DataFrame,
        macro_context: dict,
        fund_flows: pd.DataFrame,
    ) -> dict:
        """Call LLM with the industry_rotation prompt template.

        Returns the parsed JSON dict from the LLM, or a fallback dict
        if the LLM call fails.
        """
        # ── Build prompt inputs ──────────────────────────────────────
        # Top / bottom 5 industries
        top5 = industry_data.head(5) if len(industry_data) >= 5 else industry_data
        bottom5 = industry_data.tail(5) if len(industry_data) >= 5 else industry_data

        industry_perf_text = (
            "### 涨幅前五行业\n"
            + top5[
                ["板块名称", "return_5d", "return_10d", "return_20d", "relative_strength", "volume_change_pct"]
            ].to_string(index=False)
            + "\n\n### 涨幅后五行业\n"
            + bottom5[
                ["板块名称", "return_5d", "return_10d", "return_20d", "relative_strength", "volume_change_pct"]
            ].to_string(index=False)
        )

        macro_text = (
            f"PMI最新值: {macro_context.get('pmi_latest', 'N/A')}  趋势: {macro_context.get('pmi_trend', 'N/A')}\n"
            f"CPI最新值: {macro_context.get('cpi_latest', 'N/A')}  趋势: {macro_context.get('cpi_trend', 'N/A')}\n"
            f"M2同比增速: {macro_context.get('m2_growth', 'N/A')}\n"
            f"经济周期判断: {macro_context.get('macro_phase', 'N/A')} - {macro_context.get('phase_description', '')}\n"
        )
        if macro_context.get("raw_pmi"):
            macro_text += f"\n近期PMI数据:\n{macro_context['raw_pmi']}\n"
        if macro_context.get("raw_cpi"):
            macro_text += f"\n近期CPI数据:\n{macro_context['raw_cpi']}\n"

        # Fund flow summary
        if fund_flows is not None and not fund_flows.empty:
            latest_trend = fund_flows["flow_trend"].iloc[-1] if "flow_trend" in fund_flows.columns else "unknown"
            flow_summary = (
                f"最近北向资金流向趋势: {latest_trend}\n"
                + fund_flows.tail(5).to_string(index=False)
            )
        else:
            flow_summary = "北向资金数据暂不可用"

        # ── Load and fill prompt template ────────────────────────────
        try:
            template = _PROMPT_TEMPLATE_PATH.read_text(encoding="utf-8")
        except FileNotFoundError:
            logger.error("Prompt template not found at %s", _PROMPT_TEMPLATE_PATH)
            template = (
                "你是一位A股行业配置策略师。根据以下数据分析行业轮动信号。\n\n"
                "## 宏观经济数据\n{macro_data}\n\n"
                "## 各行业近期表现\n{industry_performance}\n\n"
                "## 重要政策/事件\n{policy_events}\n\n"
                "## 资金流向数据\n{fund_flow}\n\n"
                "请输出JSON格式分析结果。"
            )

        # Use manual replacement instead of str.format() because the
        # template contains JSON example braces that would conflict.
        prompt = template
        prompt = prompt.replace("{macro_data}", macro_text)
        prompt = prompt.replace("{industry_performance}", industry_perf_text)
        prompt = prompt.replace("{policy_events}", "暂无特别政策事件信息")
        prompt = prompt.replace("{fund_flow}", flow_summary)

        messages = [
            {"role": "system", "content": "你是一位资深的A股行业轮动策略分析师，擅长宏观周期分析与行业配置。"},
            {"role": "user", "content": prompt},
        ]

        # ── Call LLM ─────────────────────────────────────────────────
        try:
            result = self._llm.chat_json(messages=messages, max_tokens=4096)
            return result
        except Exception as exc:
            logger.error("LLM rotation analysis failed: %s", exc)
            # Return minimal fallback so the strategy can still produce signals
            return self._build_fallback_analysis(industry_data, macro_context)

    def _build_fallback_analysis(
        self, industry_data: pd.DataFrame, macro_context: dict
    ) -> dict:
        """Rule-based fallback when the LLM call fails."""
        top_names = industry_data.head(3)["板块名称"].tolist() if len(industry_data) >= 3 else []
        bottom_names = industry_data.tail(3)["板块名称"].tolist() if len(industry_data) >= 3 else []
        phase = macro_context.get("macro_phase", "unknown")

        recommended = []
        for name in top_names:
            recommended.append(
                {
                    "industry": name,
                    "action": "超配",
                    "weight_suggestion": 0.15,
                    "reasoning": "动量排名靠前，量价配合良好",
                    "expected_return_range": [0.0, 5.0],
                    "key_stocks": [],
                    "risk_factors": ["动量反转风险"],
                }
            )

        return {
            "economic_cycle": {
                "current_phase": phase,
                "transition_signal": "中期",
                "confidence": 0.4,
                "key_indicators": [],
            },
            "recommended_industries": recommended,
            "rotation_direction": {
                "from_sectors": bottom_names,
                "to_sectors": top_names,
                "style_shift": "无明显切换",
                "description": "基于动量排名的规则判断（LLM分析不可用）",
            },
            "catalysts": [],
            "timing": {
                "urgency": "中期（1-3月）",
                "optimal_entry_window": "N/A",
                "holding_period": "20个交易日",
            },
            "risk_assessment": {
                "overall_market_risk": "medium",
                "black_swan_scenarios": [],
                "hedging_suggestions": [],
            },
        }

    # ==================================================================
    # 5. generate_industry_signals
    # ==================================================================

    def generate_industry_signals(
        self,
        analysis: dict,
        industry_data: pd.DataFrame,
        macro_context: dict,
        fund_flows: pd.DataFrame,
    ) -> list[StrategySignal]:
        """Convert LLM analysis into StrategySignal objects.

        For recommended (超配) industries, fetches top constituents and
        generates long signals.  For avoid (低配) industries, generates
        short/neutral signals.
        """
        signals: list[StrategySignal] = []

        # Extract flow trend
        north_flow_trend = "unknown"
        if fund_flows is not None and not fund_flows.empty and "flow_trend" in fund_flows.columns:
            north_flow_trend = str(fund_flows["flow_trend"].iloc[-1])

        phase = macro_context.get("macro_phase", "unknown")

        # Determine holding period from LLM timing
        timing = analysis.get("timing", {})
        urgency = timing.get("urgency", "中期（1-3月）")
        if "立即" in urgency or "1-2周" in urgency:
            holding_days = 10
        else:
            holding_days = 20

        # Overall LLM reasoning
        rotation_dir = analysis.get("rotation_direction", {})
        llm_reasoning = rotation_dir.get("description", "")
        style_shift = rotation_dir.get("style_shift", "")
        if style_shift:
            llm_reasoning += f"  风格切换: {style_shift}"

        recommended_industries = analysis.get("recommended_industries", [])

        # Extract recommended / avoid industry names for metadata
        rec_names = [ind.get("industry", "") for ind in recommended_industries if ind.get("action") == "超配"]
        avoid_names = rotation_dir.get("from_sectors", [])

        # ── Generate long signals for recommended industries ─────────
        for ind_info in recommended_industries:
            industry_name = ind_info.get("industry", "")
            action = ind_info.get("action", "标配")

            if action == "低配":
                direction = "short"
                confidence_base = 0.3
            elif action == "标配":
                direction = "neutral"
                confidence_base = 0.4
            else:  # 超配
                direction = "long"
                confidence_base = 0.6

            # Adjust confidence from LLM cycle confidence
            cycle_conf = analysis.get("economic_cycle", {}).get("confidence", 0.5)
            confidence = min(1.0, confidence_base + cycle_conf * 0.2)

            # Expected return
            ret_range = ind_info.get("expected_return_range", [0.0, 5.0])
            expected_return = sum(ret_range) / 2 / 100 if ret_range else 0.03

            # Look up industry returns from scanned data
            ind_returns = {"return_5d": 0.0, "return_10d": 0.0, "return_20d": 0.0}
            if not industry_data.empty:
                match = industry_data[industry_data["板块名称"] == industry_name]
                if not match.empty:
                    row = match.iloc[0]
                    ind_returns["return_5d"] = float(row.get("return_5d", 0.0))
                    ind_returns["return_10d"] = float(row.get("return_10d", 0.0))
                    ind_returns["return_20d"] = float(row.get("return_20d", 0.0))

            base_reasoning = ind_info.get("reasoning", "") or f"{industry_name}: {action}"
            reasoning_parts = [base_reasoning]
            risk_factors = ind_info.get("risk_factors", [])
            if risk_factors:
                reasoning_parts.append(f"风险: {', '.join(risk_factors)}")

            # ── Rotation factor ranks from industry_data ────────────
            momentum_val = ind_returns["return_20d"]
            relative_strength_val = 0.0
            volume_change_val = 0.0
            momentum_rank = 0
            rs_rank = 0
            vol_rank = 0
            sector_rank = 0
            total_sectors = len(industry_data) if not industry_data.empty else 0

            if not industry_data.empty:
                match_row = industry_data[industry_data["板块名称"] == industry_name]
                if not match_row.empty:
                    row_data = match_row.iloc[0]
                    relative_strength_val = float(row_data.get("relative_strength", 0.0))
                    volume_change_val = float(row_data.get("volume_change_pct", 0.0))
                    # momentum_rank is pre-computed in industry_data
                    momentum_rank = int(row_data.get("momentum_rank", 0))
                    # Compute ranks for relative_strength and volume_change
                    rs_rank = int((industry_data["relative_strength"].rank(ascending=False)).loc[match_row.index[0]])
                    vol_rank = int((industry_data["volume_change_pct"].rank(ascending=False)).loc[match_row.index[0]])
                    sector_rank = momentum_rank  # overall rank by momentum

            # ── Confidence breakdown ──────────────────────────────────
            conf_base = 0.5
            momentum_boost = min(0.2, max(-0.2, momentum_val / 100.0)) if momentum_val else 0.0
            volume_boost = min(0.1, max(-0.1, volume_change_val / 200.0)) if volume_change_val else 0.0

            meta = {
                "strategy_display_name": "S09 行业轮动",
                "sector": industry_name,
                "rotation_factors": {
                    "动量(20日)": {"value": round(momentum_val, 4), "rank": momentum_rank},
                    "相对强度": {"value": round(relative_strength_val, 4), "rank": rs_rank},
                    "成交量变化": {"value": round(volume_change_val, 4), "rank": vol_rank},
                },
                "sector_rank": sector_rank,
                "total_sectors": total_sectors,
                "confidence_breakdown": {
                    "base": conf_base,
                    "momentum_boost": round(momentum_boost, 4),
                    "volume_boost": round(volume_boost, 4),
                    "final": round(confidence, 4),
                },
                # 保留原有关键字段供下游使用
                "industry": industry_name,
                "macro_phase": phase,
                "north_flow_trend": north_flow_trend,
                "llm_reasoning": llm_reasoning,
                "recommended_industries": rec_names,
                "avoid_industries": avoid_names,
                "action": action,
                "weight_suggestion": ind_info.get("weight_suggestion", 0.0),
            }

            # Fetch constituent stocks for the industry
            constituents = self._get_industry_constituents(industry_name)
            if not constituents:
                # Emit one signal at industry level with a placeholder code
                signals.append(
                    StrategySignal(
                        strategy_name=self.name,
                        stock_code=f"IND_{industry_name}",
                        stock_name=industry_name,
                        direction=direction,
                        confidence=round(confidence, 2),
                        expected_return=round(expected_return, 4),
                        holding_period_days=holding_days,
                        reasoning="; ".join(reasoning_parts),
                        metadata=meta,
                    )
                )
                continue

            # Key stocks highlighted by LLM
            key_stocks = set(ind_info.get("key_stocks", []))

            # Limit constituents to top N by market-cap or just first N
            max_stocks = settings.strategy.max_stocks_per_run // max(len(recommended_industries), 1)
            max_stocks = max(max_stocks, 3)
            selected = constituents[:max_stocks]

            for stock_code, stock_name in selected:
                # Boost confidence if the stock was explicitly named by LLM
                stock_conf = min(1.0, confidence + 0.1) if stock_name in key_stocks or stock_code in key_stocks else confidence

                signals.append(
                    StrategySignal(
                        strategy_name=self.name,
                        stock_code=stock_code,
                        stock_name=stock_name,
                        direction=direction,
                        confidence=round(stock_conf, 2),
                        expected_return=round(expected_return, 4),
                        holding_period_days=holding_days,
                        reasoning="; ".join(reasoning_parts),
                        metadata=meta,
                    )
                )

        return signals

    # ==================================================================
    # 6. run
    # ==================================================================

    def run(self, stock_codes: list[str] | None = None) -> list[StrategySignal]:
        """Full sector rotation pipeline: quant scan + LLM analysis.

        Parameters
        ----------
        stock_codes:
            Ignored for this strategy – it scans all industries.
            If provided, the output is filtered to those stock codes.

        Returns
        -------
        list[StrategySignal]
        """
        logger.info("[%s] Starting sector rotation analysis …", self.name)

        # Step 1: Quantitative industry scan
        logger.info("[%s] Scanning industry performance …", self.name)
        industry_data = self.scan_industry_performance(lookback_days=20)
        if industry_data.empty:
            logger.error("[%s] Industry scan returned no data; aborting.", self.name)
            return []

        # Step 2: Fund flow analysis
        logger.info("[%s] Analysing fund flows …", self.name)
        fund_flows = self.analyze_fund_flows(days=20)

        # Step 3: Macro context
        logger.info("[%s] Fetching macro context …", self.name)
        macro_context = self.get_macro_context()

        # Step 4: LLM rotation analysis
        logger.info("[%s] Running LLM rotation analysis …", self.name)
        analysis = self.llm_rotation_analysis(industry_data, macro_context, fund_flows)

        # Step 5: Generate signals
        logger.info("[%s] Generating signals …", self.name)
        signals = self.generate_industry_signals(analysis, industry_data, macro_context, fund_flows)

        # Optional filter
        if stock_codes:
            signals = [s for s in signals if s.stock_code in set(stock_codes)]

        logger.info("[%s] Generated %d signals.", self.name, len(signals))
        return signals

    # ==================================================================
    # 7. run_single
    # ==================================================================

    def run_single(self, stock_code: str) -> list[StrategySignal]:
        """Run sector rotation for the industry that *stock_code* belongs to.

        Looks up the stock's industry, then returns the industry-level
        signal applied to this specific stock.
        """
        industry_name = self._lookup_stock_industry(stock_code)
        stock_name = self._lookup_stock_name(stock_code)

        if not industry_name:
            logger.warning("[%s] Cannot determine industry for %s", self.name, stock_code)
            return []

        # Reuse full run but keep only signals matching the stock's industry
        all_signals = self.run()

        # Find signals for this industry
        industry_signals = [
            s for s in all_signals
            if s.metadata.get("industry") == industry_name
        ]

        if not industry_signals:
            # No signal was produced for this industry; return neutral
            return [
                StrategySignal(
                    strategy_name=self.name,
                    stock_code=stock_code,
                    stock_name=stock_name or stock_code,
                    direction="neutral",
                    confidence=0.3,
                    expected_return=0.0,
                    holding_period_days=20,
                    reasoning=f"所属行业 {industry_name} 未在推荐/回避列表中",
                    metadata={"strategy_display_name": "S09 行业轮动", "sector": industry_name, "industry": industry_name, "macro_phase": "unknown"},
                )
            ]

        # Check if this stock already has a signal
        for sig in industry_signals:
            if sig.stock_code == stock_code:
                return [sig]

        # Otherwise, clone the first industry signal for this stock
        ref = industry_signals[0]
        return [
            StrategySignal(
                strategy_name=self.name,
                stock_code=stock_code,
                stock_name=stock_name or stock_code,
                direction=ref.direction,
                confidence=ref.confidence,
                expected_return=ref.expected_return,
                holding_period_days=ref.holding_period_days,
                reasoning=ref.reasoning,
                metadata=ref.metadata,
            )
        ]

    # ==================================================================
    # Internal helpers
    # ==================================================================

    @staticmethod
    def _get_industry_constituents(industry_name: str) -> list[tuple[str, str]]:
        """Return a list of (stock_code, stock_name) for an industry board.

        Uses timeout-wrapped East Money call with predefined fallback.
        """
        return get_industry_constituents_safe(industry_name)

    @staticmethod
    def _lookup_stock_industry(stock_code: str) -> str | None:
        """Find which Shenwan L1 industry a stock belongs to."""
        return lookup_stock_industry(stock_code)

    @staticmethod
    def _lookup_stock_name(stock_code: str) -> str | None:
        """Get the display name for a stock code."""
        return lookup_stock_name(stock_code)
