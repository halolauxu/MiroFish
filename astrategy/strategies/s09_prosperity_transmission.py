"""
Prosperity Transmission Strategy (景气度传导策略)
===================================================
Upstream industry prosperity changes lead downstream by 3-6 months.
This strategy detects upstream turning points and predicts downstream
prosperity changes, then finds actionable stocks in industries that
are about to benefit (or deteriorate).

Core logic:
1. Collect macro data (PMI, CPI, PPI proxies) and build industry
   prosperity indicators.
2. Use LLM to assess each industry's prosperity score (0-100).
3. Track scores over 6-12 months to detect inflection points.
4. When an upstream industry turns, predict downstream impact with a
   3-6 month lead time.
5. Find constituent stocks in the downstream industries whose prices
   have not yet reflected the predicted change.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import akshare as ak
import pandas as pd

from astrategy.config import settings
from astrategy.data_collector.em_fallback import (
    call_with_timeout,
    get_industry_constituents_safe,
    lookup_stock_industry as _em_lookup_industry,
    lookup_stock_name as _em_lookup_name,
)
from astrategy.data_collector.macro import MacroCollector
from astrategy.data_collector.market_data import MarketDataCollector
from astrategy.llm import create_llm_client
from astrategy.strategies.base import BaseStrategy, StrategySignal

logger = logging.getLogger(__name__)

_CST = timezone(timedelta(hours=8))

# ---------------------------------------------------------------------------
# Industry chain definitions (产业链映射)
# ---------------------------------------------------------------------------

INDUSTRY_CHAINS: Dict[str, Dict[str, List[str]]] = {
    "锂电池": {
        "upstream": ["锂矿", "正极材料", "负极材料"],
        "downstream": ["新能源汽车", "储能"],
    },
    "半导体": {
        "upstream": ["硅片", "光刻胶", "设备"],
        "downstream": ["消费电子", "汽车电子"],
    },
    "钢铁": {
        "upstream": ["铁矿石", "焦煤"],
        "downstream": ["房地产", "基建", "汽车"],
    },
    "化工": {
        "upstream": ["原油", "天然气"],
        "downstream": ["纺织", "农业", "塑料制品"],
    },
    "光伏": {
        "upstream": ["多晶硅", "银浆", "玻璃"],
        "downstream": ["电站运营", "分布式光伏"],
    },
    "面板": {
        "upstream": ["偏光片", "驱动IC", "背光模组"],
        "downstream": ["电视", "笔记本", "车载显示"],
    },
    "造纸": {
        "upstream": ["木浆", "废纸"],
        "downstream": ["包装", "出版", "电商物流"],
    },
    "养殖": {
        "upstream": ["饲料", "兽药", "种业"],
        "downstream": ["屠宰加工", "肉制品", "餐饮"],
    },
}

# Map industry-chain keywords to Shenwan board names for constituent lookup
_INDUSTRY_TO_BOARD: Dict[str, str] = {
    "新能源汽车": "汽车零部件",
    "储能": "电池",
    "消费电子": "消费电子",
    "汽车电子": "汽车零部件",
    "房地产": "房地产开发",
    "基建": "建筑装饰",
    "汽车": "汽车整车",
    "纺织": "纺织服饰",
    "农业": "农林牧渔",
    "塑料制品": "橡胶",
    "电站运营": "电力",
    "分布式光伏": "电力设备",
    "电视": "家用电器",
    "笔记本": "消费电子",
    "车载显示": "消费电子",
    "包装": "包装印刷",
    "出版": "出版",
    "电商物流": "物流",
    "屠宰加工": "食品加工",
    "肉制品": "食品加工",
    "餐饮": "社会服务",
    # upstream keywords
    "锂矿": "有色金属",
    "正极材料": "电池",
    "负极材料": "电池",
    "硅片": "光伏设备",
    "光刻胶": "化学制品",
    "设备": "半导体",
    "铁矿石": "钢铁",
    "焦煤": "煤炭",
    "原油": "石油石化",
    "天然气": "石油石化",
    "多晶硅": "光伏设备",
    "银浆": "贵金属",
    "玻璃": "装修建材",
    "偏光片": "电子化学品",
    "驱动IC": "半导体",
    "背光模组": "光学光电子",
    "木浆": "造纸",
    "废纸": "造纸",
    "饲料": "饲料",
    "兽药": "农牧饲渔",
    "种业": "农林牧渔",
}

# ---------------------------------------------------------------------------
# In-memory prosperity score cache
# ---------------------------------------------------------------------------

_prosperity_cache: Dict[str, Tuple[float, dict]] = {}
_PROSPERITY_TTL = 86400  # 24 hours – monthly data doesn't change intra-day

# Prosperity score history (persisted per-session for turning-point detection)
_prosperity_history: Dict[str, List[dict]] = {}


class ProsperityTransmissionStrategy(BaseStrategy):
    """Detect upstream prosperity turning points and predict downstream impact."""

    def __init__(self, signal_dir: Path | str | None = None) -> None:
        super().__init__(signal_dir=signal_dir)
        self._macro = MacroCollector()
        self._market = MarketDataCollector()
        self._llm = create_llm_client(strategy_name=self.name)
        # Path for persisting prosperity history across runs
        self._history_path = (
            self._signal_dir / self.name / "_prosperity_history.json"
        )
        self._load_history()

    # ── identity ──────────────────────────────────────────────────────

    @property
    def name(self) -> str:
        return "prosperity_transmission"

    # ==================================================================
    # Persistence helpers for prosperity history
    # ==================================================================

    def _load_history(self) -> None:
        """Load prosperity history from disk if available."""
        global _prosperity_history
        if self._history_path.exists():
            try:
                data = json.loads(self._history_path.read_text(encoding="utf-8"))
                _prosperity_history.update(data)
                logger.info(
                    "Loaded prosperity history for %d industries",
                    len(_prosperity_history),
                )
            except Exception as exc:
                logger.warning("Failed to load prosperity history: %s", exc)

    def _save_history(self) -> None:
        """Persist prosperity history to disk."""
        self._history_path.parent.mkdir(parents=True, exist_ok=True)
        self._history_path.write_text(
            json.dumps(_prosperity_history, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    # ==================================================================
    # 1. collect_prosperity_data
    # ==================================================================

    def collect_prosperity_data(self) -> dict:
        """Fetch PMI, CPI, PPI-proxy data and build an industry prosperity context.

        Returns a dict with keys:
            pmi_data, cpi_data, ppi_proxy, monetary_data,
            pmi_latest, pmi_trend, cpi_latest, ppi_latest, raw_summary
        """
        result: Dict[str, Any] = {
            "pmi_data": None,
            "cpi_data": None,
            "ppi_proxy": None,
            "monetary_data": None,
            "pmi_latest": None,
            "pmi_trend": "unknown",
            "cpi_latest": None,
            "cpi_trend": "unknown",
            "ppi_latest": None,
            "ppi_trend": "unknown",
            "raw_summary": "",
        }

        # ── PMI ────────────────────────────────────────────────────
        df_pmi = self._macro.get_pmi(months=12)
        if df_pmi is not None and not df_pmi.empty:
            result["pmi_data"] = df_pmi
            pmi_col = _find_column(df_pmi, ["制造业", "pmi", "PMI"])
            if pmi_col:
                vals = pd.to_numeric(df_pmi[pmi_col], errors="coerce").dropna().tolist()
                if vals:
                    result["pmi_latest"] = vals[-1]
                    result["pmi_trend"] = _compute_trend(vals)

        # ── CPI ────────────────────────────────────────────────────
        df_cpi = self._macro.get_cpi(months=12)
        if df_cpi is not None and not df_cpi.empty:
            result["cpi_data"] = df_cpi
            cpi_col = _find_column(df_cpi, ["同比", "增长", "cpi", "CPI"])
            if cpi_col:
                vals = pd.to_numeric(df_cpi[cpi_col], errors="coerce").dropna().tolist()
                if vals:
                    result["cpi_latest"] = vals[-1]
                    result["cpi_trend"] = _compute_trend(vals)

        # ── PPI proxy (use CPI producer-side or monetary data) ────
        # akshare does not have a direct PPI API in all versions;
        # we approximate via the PMI sub-index for raw materials
        # prices or use CPI industrial data if available.
        try:
            df_ppi = ak.macro_china_ppi_yearly()
            if df_ppi is not None and not df_ppi.empty:
                result["ppi_proxy"] = df_ppi.tail(12).reset_index(drop=True)
                ppi_col = _find_column(df_ppi, ["同比", "增长", "ppi", "PPI", "工业"])
                if ppi_col:
                    vals = pd.to_numeric(df_ppi[ppi_col], errors="coerce").dropna().tolist()
                    if vals:
                        result["ppi_latest"] = vals[-1]
                        result["ppi_trend"] = _compute_trend(vals[-12:])
        except Exception as exc:
            logger.debug("PPI proxy fetch failed: %s", exc)

        # ── Monetary data ──────────────────────────────────────────
        df_m2 = self._macro.get_monetary_data()
        if df_m2 is not None and not df_m2.empty:
            result["monetary_data"] = df_m2.tail(6).reset_index(drop=True)

        # Build a concise raw summary for LLM prompts
        summary_parts: List[str] = []
        if result["pmi_latest"] is not None:
            summary_parts.append(
                f"PMI最新={result['pmi_latest']} 趋势={result['pmi_trend']}"
            )
        if result["cpi_latest"] is not None:
            summary_parts.append(
                f"CPI最新={result['cpi_latest']} 趋势={result['cpi_trend']}"
            )
        if result["ppi_latest"] is not None:
            summary_parts.append(
                f"PPI最新={result['ppi_latest']} 趋势={result['ppi_trend']}"
            )
        result["raw_summary"] = "; ".join(summary_parts)

        # Fetch industry-specific stock returns to differentiate prosperity scores
        result["industry_returns"] = {}
        try:
            from astrategy.data_collector.em_fallback import scan_industry_returns_sina
            ind_df = scan_industry_returns_sina(lookback_days=60)
            if ind_df is not None and not ind_df.empty:
                for _, row in ind_df.iterrows():
                    board = str(row.get("板块名称", ""))
                    result["industry_returns"][board] = {
                        "return_20d": row.get("return_20d"),
                        "return_10d": row.get("return_10d"),
                        "return_5d": row.get("return_5d"),
                    }
        except Exception as exc:
            logger.debug("Industry returns fetch failed: %s", exc)

        return result

    # ==================================================================
    # 2. compute_prosperity_score
    # ==================================================================

    def compute_prosperity_score(
        self, industry: str, macro_data: dict
    ) -> dict:
        """Use LLM to assess an industry's current prosperity.

        Returns:
            {
                "industry": str,
                "prosperity_score": float (0-100),
                "trend": str ("上行"/"下行"/"平稳"),
                "confidence": float (0-1),
                "reasoning": str,
                "timestamp": str (ISO),
            }
        """
        # Check cache
        cache_key = f"prosperity_{industry}"
        cached = _prosperity_cache.get(cache_key)
        if cached is not None:
            ts, value = cached
            if time.time() - ts < _PROSPERITY_TTL:
                return value

        # Build LLM prompt
        macro_summary = macro_data.get("raw_summary", "宏观数据不可用")

        # Include PPI detail if available
        ppi_detail = ""
        if macro_data.get("ppi_proxy") is not None:
            ppi_df = macro_data["ppi_proxy"]
            ppi_detail = f"\nPPI近期数据:\n{ppi_df.tail(6).to_string(index=False)}"

        pmi_detail = ""
        if macro_data.get("pmi_data") is not None:
            pmi_df = macro_data["pmi_data"]
            pmi_detail = f"\nPMI近期数据:\n{pmi_df.tail(6).to_string(index=False)}"

        # Look up industry-specific stock return as differentiated proxy
        board_name = _INDUSTRY_TO_BOARD.get(industry, "")
        industry_returns = macro_data.get("industry_returns", {})
        ind_return_info = industry_returns.get(board_name, {})

        # Compute a quantitative baseline score from industry returns
        # This ensures different industries get meaningfully different scores
        r5 = ind_return_info.get("return_5d")
        r20 = ind_return_info.get("return_20d")
        rs = ind_return_info.get("relative_strength")

        quant_baseline = None
        if rs is not None:
            # relative_strength range approx -10 to +15 → map to 30-80
            quant_baseline = max(25.0, min(80.0, 50.0 + float(rs) * 1.8))
        elif r20 is not None:
            quant_baseline = max(25.0, min(80.0, 50.0 + float(r20) * 0.8))

        ind_return_text = ""
        if ind_return_info:
            parts = []
            if r5 is not None:
                parts.append(f"近5日涨跌幅={r5:.1f}%")
            if r20 is not None:
                parts.append(f"近20日涨跌幅={r20:.1f}%")
            if rs is not None:
                parts.append(f"综合相对强度={rs:.1f}")
            if quant_baseline is not None:
                parts.append(f"量化基准分={quant_baseline:.0f}")
            ind_return_text = (
                f"\n## 【核心数据】{industry}（{board_name}板块）近期市场表现\n"
                + "；".join(parts)
                + f"\n注：量化基准分已根据市场表现计算，你的最终评分应以此为锚点（±10分浮动），"
                + f"不要脱离市场表现给出与基准分差距超过15分的评分。"
            )

        prompt = (
            f"你是一位产业链景气度分析专家。请评估「{industry}」行业当前的景气度。\n\n"
            f"{ind_return_text}\n\n"
            f"## 宏观背景数据（参考）\n{macro_summary}\n"
            f"注：以下宏观数据对所有行业相同，请优先参考上方行业专属数据。\n"
            f"{pmi_detail}\n{ppi_detail}\n\n"
            f"## 评分要求（严格执行）\n"
            f"1. 以量化基准分为锚点，结合行业基本面调整（±10分以内）\n"
            f"2. 评分范围0-100，50为荣枯线；各行业之间必须有明显差异\n"
            f"3. 判断趋势：近5日强于近20日 → 上行；近20日强于近5日 → 下行；否则平稳\n"
            f"4. 给出置信度0-1（有量化数据时置信度应≥0.65）\n\n"
            f"请以JSON格式返回：\n"
            f'{{"prosperity_score": 数值, "trend": "上行/下行/平稳", '
            f'"confidence": 数值, "reasoning": "分析理由（需引用具体数据）"}}'
        )

        messages = [
            {
                "role": "system",
                "content": (
                    "你是产业链景气度分析专家。根据宏观经济指标评估特定行业景气状况。"
                    "输出严格的JSON格式。"
                ),
            },
            {"role": "user", "content": prompt},
        ]

        now_iso = datetime.now(tz=_CST).isoformat()

        try:
            resp = self._llm.chat_json(messages=messages, max_tokens=1024)
            score = float(resp.get("prosperity_score", 50))
            score = max(0.0, min(100.0, score))
            trend = resp.get("trend", "平稳")
            if trend not in ("上行", "下行", "平稳"):
                trend = "平稳"
            confidence = float(resp.get("confidence", 0.5))
            confidence = max(0.0, min(1.0, confidence))
            reasoning = resp.get("reasoning", "")
        except Exception as exc:
            logger.warning(
                "LLM prosperity assessment for %s failed: %s", industry, exc
            )
            # Fallback based on PMI
            pmi = macro_data.get("pmi_latest")
            if pmi is not None:
                score = float(pmi)  # PMI is already 0-100 scale
            else:
                score = 50.0
            trend = macro_data.get("pmi_trend", "平稳")
            confidence = 0.3
            reasoning = "LLM评估失败，使用PMI作为近似值"

        result = {
            "industry": industry,
            "prosperity_score": round(score, 1),
            "trend": trend,
            "confidence": round(confidence, 2),
            "reasoning": reasoning,
            "timestamp": now_iso,
        }

        # Update cache
        _prosperity_cache[cache_key] = (time.time(), result)

        # Append to history
        if industry not in _prosperity_history:
            _prosperity_history[industry] = []
        _prosperity_history[industry].append(result)
        # Keep at most 24 months of history
        _prosperity_history[industry] = _prosperity_history[industry][-24:]

        return result

    # ==================================================================
    # 3. detect_turning_points
    # ==================================================================

    def detect_turning_points(
        self, industry: str, history: Optional[List[dict]] = None
    ) -> dict:
        """Detect prosperity inflection points over the recent 6-12 months.

        Turning point conditions (any of):
        - Prosperity score crosses the 50 boom/bust line
        - First derivative (month-over-month change) changes sign for 2+
          consecutive months
        - Score changes by >= 10 points over 3 months

        Returns:
            {
                "industry": str,
                "turning_point_detected": bool,
                "direction": str ("up" / "down" / "none"),
                "months_since_turn": int,
                "current_score": float,
                "previous_score": float,
                "score_change_3m": float,
            }
        """
        if history is None:
            history = _prosperity_history.get(industry, [])

        result = {
            "industry": industry,
            "turning_point_detected": False,
            "direction": "none",
            "months_since_turn": 0,
            "current_score": 50.0,
            "previous_score": 50.0,
            "score_change_3m": 0.0,
        }

        if len(history) < 2:
            if history:
                result["current_score"] = history[-1].get("prosperity_score", 50.0)
            return result

        scores = [h.get("prosperity_score", 50.0) for h in history]
        result["current_score"] = scores[-1]
        result["previous_score"] = scores[-2]

        # 3-month score change
        if len(scores) >= 4:
            result["score_change_3m"] = round(scores[-1] - scores[-4], 1)
        elif len(scores) >= 2:
            result["score_change_3m"] = round(scores[-1] - scores[0], 1)

        # Condition 1: Cross the 50-line
        if len(scores) >= 2:
            if scores[-2] < 50 <= scores[-1]:
                result["turning_point_detected"] = True
                result["direction"] = "up"
            elif scores[-2] >= 50 > scores[-1]:
                result["turning_point_detected"] = True
                result["direction"] = "down"

        # Condition 2: Derivative sign change for 2+ consecutive months
        if not result["turning_point_detected"] and len(scores) >= 3:
            deltas = [scores[i] - scores[i - 1] for i in range(1, len(scores))]
            if len(deltas) >= 2:
                # Check last 2 deltas have same sign, and previous 2 had opposite sign
                recent_direction = deltas[-1] + deltas[-2] if len(deltas) >= 2 else 0
                if len(deltas) >= 3:
                    prior_direction = deltas[-3]
                    if recent_direction > 0 and prior_direction < 0:
                        result["turning_point_detected"] = True
                        result["direction"] = "up"
                    elif recent_direction < 0 and prior_direction > 0:
                        result["turning_point_detected"] = True
                        result["direction"] = "down"

        # Condition 3: Large absolute change over 3 months
        if not result["turning_point_detected"] and abs(result["score_change_3m"]) >= 10:
            result["turning_point_detected"] = True
            result["direction"] = "up" if result["score_change_3m"] > 0 else "down"

        # Estimate months since turning point
        if result["turning_point_detected"]:
            result["months_since_turn"] = _estimate_months_since_turn(
                scores, result["direction"]
            )

        return result

    # ==================================================================
    # 4. predict_downstream_impact
    # ==================================================================

    def predict_downstream_impact(
        self,
        upstream_turn: dict,
        chain_name: str,
        chain: Dict[str, List[str]],
        macro_data: dict,
    ) -> List[dict]:
        """Predict downstream prosperity changes based on upstream turning point.

        Uses LLM to assess transmission strength and expected delay for
        each downstream industry.

        Returns a list of dicts, one per downstream industry:
            {
                "downstream_industry": str,
                "predicted_direction": str ("up" / "down"),
                "predicted_prosperity_change": float (-50 to +50),
                "transmission_delay_months": int (3-6),
                "transmission_strength": float (0-1),
                "reasoning": str,
            }
        """
        upstream_industry = upstream_turn["industry"]
        direction = upstream_turn["direction"]
        score = upstream_turn["current_score"]
        score_change = upstream_turn["score_change_3m"]
        downstream_industries = chain.get("downstream", [])

        if not downstream_industries:
            return []

        downstream_list = "、".join(downstream_industries)
        dir_cn = "上行" if direction == "up" else "下行"

        prompt = (
            f"你是产业链传导分析专家。上游行业「{upstream_industry}」出现景气度拐点。\n\n"
            f"## 上游状况\n"
            f"- 产业链: {chain_name}\n"
            f"- 当前景气度评分: {score}\n"
            f"- 近3月变化: {score_change:+.1f}\n"
            f"- 趋势方向: {dir_cn}\n"
            f"- 宏观背景: {macro_data.get('raw_summary', 'N/A')}\n\n"
            f"## 下游行业列表\n{downstream_list}\n\n"
            f"## 请分析\n"
            f"对每个下游行业，判断：\n"
            f"1. 传导方向（上游{dir_cn}对下游是利好还是利空）\n"
            f"2. 景气度预期变化幅度（-50到+50）\n"
            f"3. 传导时滞（3-6个月）\n"
            f"4. 传导强度（0-1）\n\n"
            f"请以JSON格式返回，包含 \"downstream_impacts\" 数组，每个元素包含：\n"
            f'{{"industry": "行业名", "predicted_direction": "up/down", '
            f'"predicted_change": 数值, "delay_months": 整数, '
            f'"strength": 数值, "reasoning": "理由"}}'
        )

        messages = [
            {
                "role": "system",
                "content": (
                    "你是产业链传导分析专家，擅长分析上下游景气度传导关系。"
                    "输出严格的JSON格式。"
                ),
            },
            {"role": "user", "content": prompt},
        ]

        try:
            resp = self._llm.chat_json(messages=messages, max_tokens=2048)
            impacts = resp.get("downstream_impacts", [])
        except Exception as exc:
            logger.warning("LLM downstream impact prediction failed: %s", exc)
            impacts = []

        # Normalize and fill defaults
        results: List[dict] = []
        processed_industries = set()

        for item in impacts:
            industry = item.get("industry", "")
            if not industry or industry in processed_industries:
                continue
            processed_industries.add(industry)

            pred_dir = item.get("predicted_direction", direction)
            if pred_dir not in ("up", "down"):
                pred_dir = direction

            results.append(
                {
                    "downstream_industry": industry,
                    "predicted_direction": pred_dir,
                    "predicted_prosperity_change": _clamp(
                        float(item.get("predicted_change", 0)), -50, 50
                    ),
                    "transmission_delay_months": _clamp(
                        int(item.get("delay_months", 4)), 1, 12
                    ),
                    "transmission_strength": _clamp(
                        float(item.get("strength", 0.5)), 0, 1
                    ),
                    "reasoning": item.get("reasoning", ""),
                }
            )

        # Ensure every downstream industry has an entry (rule-based fallback)
        for ds_ind in downstream_industries:
            if ds_ind not in processed_industries:
                default_change = 10.0 if direction == "up" else -10.0
                results.append(
                    {
                        "downstream_industry": ds_ind,
                        "predicted_direction": direction,
                        "predicted_prosperity_change": default_change,
                        "transmission_delay_months": 4,
                        "transmission_strength": 0.4,
                        "reasoning": "规则默认：上游转向通常3-6个月后传导至下游",
                    }
                )

        return results

    # ==================================================================
    # 5. find_actionable_stocks
    # ==================================================================

    def find_actionable_stocks(
        self, industry: str, predicted_direction: str
    ) -> List[Tuple[str, str, dict]]:
        """Get constituent stocks for a downstream industry and filter candidates.

        Returns a list of (stock_code, stock_name, stock_meta) tuples.
        stock_meta includes pe_percentile approximation and recent return.
        """
        # Map industry keyword to a Shenwan board name
        board_name = _INDUSTRY_TO_BOARD.get(industry)
        if not board_name:
            # Try using the industry name directly
            board_name = industry

        # Fetch constituents (with timeout fallback)
        constituents: List[Tuple[str, str]] = get_industry_constituents_safe(board_name)

        if not constituents:
            logger.info("No constituents found for industry: %s", industry)
            return []

        # Fetch realtime quotes for filtering
        codes = [c for c, _ in constituents[:50]]  # limit API call size
        df_quotes = self._market.get_realtime_quotes(codes=codes)

        results: List[Tuple[str, str, dict]] = []

        for code, name in constituents[:50]:
            meta: Dict[str, Any] = {"pe_percentile": 50.0, "recent_return_20d": 0.0}

            if df_quotes is not None and not df_quotes.empty:
                match = df_quotes[df_quotes["代码"] == code]
                if not match.empty:
                    row = match.iloc[0]
                    pe = pd.to_numeric(row.get("市盈率-动态", None), errors="coerce")
                    if pd.notna(pe):
                        # Rough PE percentile: compare against all returned stocks
                        all_pe = pd.to_numeric(
                            df_quotes["市盈率-动态"], errors="coerce"
                        ).dropna()
                        if len(all_pe) > 1 and pe > 0:
                            meta["pe_percentile"] = round(
                                (all_pe < pe).sum() / len(all_pe) * 100, 1
                            )

                    # 20-day return proxy: use 涨跌幅 as daily, approximate
                    change_pct = pd.to_numeric(
                        row.get("涨跌幅", 0), errors="coerce"
                    )
                    if pd.notna(change_pct):
                        meta["recent_return_20d"] = float(change_pct)

            # Filter: for "up" predictions, prefer stocks with lower PE
            # percentile (value not yet priced in) and lower recent returns
            if predicted_direction == "up":
                # Skip stocks with PE percentile > 90 (already expensive)
                if meta["pe_percentile"] > 90:
                    continue
            elif predicted_direction == "down":
                # For short signals, prefer stocks with high PE (overvalued)
                if meta["pe_percentile"] < 10:
                    continue

            results.append((code, name, meta))

        # Sort: for "up", prefer lower PE percentile; for "down", prefer higher
        if predicted_direction == "up":
            results.sort(key=lambda x: x[2].get("pe_percentile", 50))
        else:
            results.sort(key=lambda x: -x[2].get("pe_percentile", 50))

        # Return top candidates
        max_per_industry = settings.strategy.max_stocks_per_run // max(
            len(INDUSTRY_CHAINS), 1
        )
        max_per_industry = max(max_per_industry, 3)
        return results[:max_per_industry]

    # ==================================================================
    # 6. run
    # ==================================================================

    def run(self, stock_codes: Optional[List[str]] = None) -> List[StrategySignal]:
        """Full prosperity transmission pipeline.

        Scans all industry chains, detects upstream turning points,
        predicts downstream impact, and generates signals.

        Parameters
        ----------
        stock_codes:
            If provided, only return signals for these stock codes.
        """
        logger.info("[%s] Starting prosperity transmission analysis ...", self.name)

        # Step 1: Collect macro data
        logger.info("[%s] Collecting macro prosperity data ...", self.name)
        macro_data = self.collect_prosperity_data()

        signals: List[StrategySignal] = []

        # Step 2: Scan all industry chains
        for chain_name, chain in INDUSTRY_CHAINS.items():
            logger.info("[%s] Analysing chain: %s", self.name, chain_name)

            # 2a: Compute prosperity scores for upstream industries
            upstream_turns: List[dict] = []
            for upstream_ind in chain.get("upstream", []):
                logger.debug(
                    "[%s] Computing prosperity for upstream: %s",
                    self.name, upstream_ind,
                )
                score_data = self.compute_prosperity_score(upstream_ind, macro_data)

                # 2b: Detect turning points
                turning = self.detect_turning_points(upstream_ind)
                turning["prosperity_data"] = score_data

                if turning["turning_point_detected"]:
                    upstream_turns.append(turning)
                    logger.info(
                        "[%s] Turning point detected: %s direction=%s score=%.1f",
                        self.name,
                        upstream_ind,
                        turning["direction"],
                        turning["current_score"],
                    )

            if not upstream_turns:
                logger.debug(
                    "[%s] No upstream turning points in chain %s",
                    self.name, chain_name,
                )
                continue

            # Step 3: Predict downstream impact for each upstream turn
            for turn in upstream_turns:
                impacts = self.predict_downstream_impact(
                    turn, chain_name, chain, macro_data
                )

                # Step 4: Find actionable stocks
                for impact in impacts:
                    ds_industry = impact["downstream_industry"]
                    pred_direction = impact["predicted_direction"]

                    candidates = self.find_actionable_stocks(
                        ds_industry, pred_direction
                    )

                    if not candidates:
                        logger.debug(
                            "[%s] No candidates for downstream: %s",
                            self.name, ds_industry,
                        )
                        continue

                    # Step 5: Generate signals
                    signal_direction = (
                        "long" if pred_direction == "up" else "short"
                    )
                    # Base confidence from transmission strength * upstream confidence
                    upstream_conf = turn["prosperity_data"].get("confidence", 0.5)
                    base_confidence = (
                        impact["transmission_strength"] * upstream_conf
                    )
                    base_confidence = max(0.2, min(0.9, base_confidence))

                    # Holding period based on transmission delay
                    delay_months = impact["transmission_delay_months"]
                    holding_days = delay_months * 21  # ~21 trading days/month

                    # Expected return based on predicted change magnitude
                    pred_change = abs(impact["predicted_prosperity_change"])
                    expected_return = pred_change / 100 * 0.5  # rough mapping

                    for code, name, stock_meta in candidates:
                        meta = {
                            "industry_chain": f"{chain_name}产业链",
                            "upstream_industry": turn["industry"],
                            "upstream_prosperity": turn["current_score"],
                            "upstream_turning_point": True,
                            "downstream_industry": ds_industry,
                            "predicted_prosperity_change": impact[
                                "predicted_prosperity_change"
                            ],
                            "transmission_delay_months": delay_months,
                            "current_stock_pe_percentile": stock_meta.get(
                                "pe_percentile", 50.0
                            ),
                            "llm_reasoning": (
                                f"上游{turn['industry']}景气度"
                                f"{'上行' if turn['direction'] == 'up' else '下行'}"
                                f"拐点，预计{delay_months}个月后传导至"
                                f"下游{ds_industry}。"
                                f" {impact.get('reasoning', '')}"
                            ),
                        }

                        reasoning = (
                            f"{chain_name}产业链传导：上游{turn['industry']}"
                            f"景气度={turn['current_score']:.0f}，"
                            f"{'上行' if turn['direction'] == 'up' else '下行'}拐点"
                            f"（3月变化{turn['score_change_3m']:+.1f}），"
                            f"预计传导至下游{ds_industry}，"
                            f"延迟{delay_months}月，强度{impact['transmission_strength']:.0%}"
                        )

                        signals.append(
                            StrategySignal(
                                strategy_name=self.name,
                                stock_code=code,
                                stock_name=name,
                                direction=signal_direction,
                                confidence=round(base_confidence, 2),
                                expected_return=round(expected_return, 4),
                                holding_period_days=holding_days,
                                reasoning=reasoning,
                                metadata=meta,
                            )
                        )

        # Persist history for next run
        self._save_history()

        # Optional filter by stock codes
        if stock_codes:
            code_set = set(stock_codes)
            signals = [s for s in signals if s.stock_code in code_set]

        logger.info("[%s] Generated %d signals.", self.name, len(signals))
        return signals

    # ==================================================================
    # 7. run_single
    # ==================================================================

    def run_single(self, stock_code: str) -> List[StrategySignal]:
        """Run prosperity transmission analysis for a single stock.

        Determines which industry chain(s) the stock belongs to and
        returns relevant signals.
        """
        # Look up stock name
        stock_name = _lookup_stock_name(stock_code) or stock_code

        # Look up which industry board the stock belongs to
        stock_industry = _lookup_stock_industry(stock_code)

        if not stock_industry:
            logger.warning(
                "[%s] Cannot determine industry for %s", self.name, stock_code
            )
            return [
                StrategySignal(
                    strategy_name=self.name,
                    stock_code=stock_code,
                    stock_name=stock_name,
                    direction="neutral",
                    confidence=0.2,
                    expected_return=0.0,
                    holding_period_days=60,
                    reasoning="无法确定所属行业，无法进行景气度传导分析",
                    metadata={},
                )
            ]

        # Run full pipeline and filter to this stock
        all_signals = self.run(stock_codes=[stock_code])

        if all_signals:
            return all_signals

        # If the stock wasn't picked up by any chain, check if its industry
        # appears in any downstream list and provide a neutral assessment
        logger.info(
            "[%s] Stock %s (%s) not in any active transmission chain",
            self.name, stock_code, stock_industry,
        )

        # Collect macro data for a quick prosperity check
        macro_data = self.collect_prosperity_data()
        score_data = self.compute_prosperity_score(stock_industry, macro_data)

        return [
            StrategySignal(
                strategy_name=self.name,
                stock_code=stock_code,
                stock_name=stock_name,
                direction="neutral",
                confidence=0.3,
                expected_return=0.0,
                holding_period_days=60,
                reasoning=(
                    f"所属行业{stock_industry}景气度={score_data['prosperity_score']:.0f}，"
                    f"趋势{score_data['trend']}，但未检测到上游产业链明确拐点"
                ),
                metadata={
                    "downstream_industry": stock_industry,
                    "downstream_prosperity": score_data["prosperity_score"],
                    "downstream_trend": score_data["trend"],
                    "llm_reasoning": score_data.get("reasoning", ""),
                },
            )
        ]


# ===========================================================================
# Module-level helper functions
# ===========================================================================


def _find_column(df: pd.DataFrame, keywords: List[str]) -> Optional[str]:
    """Find the first column whose name contains any of the keywords."""
    for c in df.columns:
        cstr = str(c)
        for kw in keywords:
            if kw.lower() in cstr.lower():
                return c
    # Fallback: second column (first is usually date)
    if len(df.columns) > 1:
        return df.columns[1]
    return df.columns[0] if len(df.columns) > 0 else None


def _compute_trend(values: List[float]) -> str:
    """Determine trend direction from the last 3+ values."""
    if len(values) < 3:
        return "平稳"
    if values[-1] > values[-3]:
        return "上行"
    elif values[-1] < values[-3]:
        return "下行"
    return "平稳"


def _clamp(value: float, low: float, high: float) -> float:
    """Clamp a value to [low, high]."""
    return max(low, min(high, value))


def _estimate_months_since_turn(scores: List[float], direction: str) -> int:
    """Walk backwards through scores to estimate when the turn started."""
    if len(scores) < 2:
        return 0

    months = 0
    for i in range(len(scores) - 1, 0, -1):
        delta = scores[i] - scores[i - 1]
        if direction == "up" and delta <= 0:
            break
        if direction == "down" and delta >= 0:
            break
        months += 1

    return months


def _lookup_stock_name(stock_code: str) -> Optional[str]:
    """Get the display name for a stock code (Sina-based, no East Money)."""
    return _em_lookup_name(stock_code)


def _lookup_stock_industry(stock_code: str) -> Optional[str]:
    """Find which industry board a stock belongs to (predefined mapping)."""
    result = _em_lookup_industry(stock_code)
    if result is not None:
        return result
    # Original East Money fallback with timeout (kept for edge cases)
    try:
        df_boards = call_with_timeout(ak.stock_board_industry_name_em, timeout=8)
        if df_boards is None or df_boards.empty:
            return None
        for _, row in df_boards.head(50).iterrows():
            board_name = str(row.get("板块名称", ""))
            try:
                df_cons = call_with_timeout(
                    ak.stock_board_industry_cons_em, symbol=board_name, timeout=5
                )
                if df_cons is not None and not df_cons.empty:
                    code_col = (
                        "代码" if "代码" in df_cons.columns else df_cons.columns[0]
                    )
                    codes = df_cons[code_col].astype(str).tolist()
                    if stock_code in codes:
                        return board_name
            except Exception:
                continue
    except Exception as exc:
        logger.debug("_lookup_stock_industry failed: %s", exc)
    return None
