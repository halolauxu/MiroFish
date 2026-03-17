"""
Narrative Tracker Strategy (S11)
==================================
Tracks the lifecycle of investment narratives (投资叙事/主题) in A-share markets
and generates signals based on where each narrative stands in its diffusion curve.

Core idea
----------
Investment narratives follow a predictable lifecycle:
  萌芽期 (Inception, 0-25) → 扩散期 (Spreading, 25-50) →
  成熟期 (Mature, 50-75)   → 衰退期 (Fading, 75-100)

Alpha comes from:
1. **Early identification** (萌芽→扩散): buy leading stocks before consensus
2. **Momentum** (扩散期): ride the theme with sector leaders
3. **Avoidance** (成熟→衰退): avoid when narrative is priced-in or reversing

Unlike pure quant strategies, narrative alpha requires understanding the
*story* market participants are telling themselves, not just the numbers.
This is where MiroFish's language-based intelligence adds value.

Signals
-------
  - 萌芽期 (score 0-30): STRONG LONG for leading stocks, early alpha
  - 扩散期 (score 30-55): MODERATE LONG for sector leaders
  - 成熟期 (score 55-75): NEUTRAL (consensus priced-in)
  - 衰退期 (score 75-100): AVOID for pure-theme stocks

Typical holding period: 10-30 days (narrative-duration dependent).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from astrategy.data_collector.market_data import MarketDataCollector
from astrategy.data_collector.news import NewsCollector
from astrategy.data_collector.research import ResearchCollector
from astrategy.llm import create_llm_client
from astrategy.strategies.base import BaseStrategy, StrategySignal

logger = logging.getLogger("astrategy.strategies.s11_narrative_tracker")
_CST = timezone(timedelta(hours=8))

# ---------------------------------------------------------------------------
# Pre-defined investment narratives for A-share market (2025-2026)
# Each narrative: keywords (for news matching), representative stocks,
# description for LLM context.
# ---------------------------------------------------------------------------

NARRATIVES: Dict[str, Dict[str, Any]] = {
    "AI算力": {
        "description": "人工智能基础设施投资：GPU芯片、算力中心、光模块、液冷散热",
        "keywords": ["算力", "AI芯片", "英伟达", "GPU", "大模型", "光模块", "液冷", "智算"],
        "representative_stocks": ["688041", "300308", "002415", "603501", "300777"],
        "theme_type": "tech",
    },
    "人形机器人": {
        "description": "人形机器人及工业机器人：减速器、电机、传感器、整机",
        "keywords": ["人形机器人", "具身智能", "减速器", "关节", "特斯拉", "工业机器人"],
        "representative_stocks": ["300124", "002816", "688169", "300296", "002747"],
        "theme_type": "tech",
    },
    "低空经济": {
        "description": "无人机、eVTOL、低空基础设施、航空物流",
        "keywords": ["低空经济", "无人机", "eVTOL", "飞行汽车", "空中出行"],
        "representative_stocks": ["600038", "002179", "300581", "688005"],
        "theme_type": "tech",
    },
    "半导体国产替代": {
        "description": "芯片设计、制造设备、封测、EDA国产化",
        "keywords": ["半导体", "国产替代", "芯片", "光刻", "EDA", "先进封装", "存储芯片"],
        "representative_stocks": ["688012", "688981", "002049", "603501", "688008"],
        "theme_type": "tech",
    },
    "新能源汽车": {
        "description": "电动车整车、电池、充电桩、智能驾驶",
        "keywords": ["新能源汽车", "电动车", "充电桩", "智能驾驶", "固态电池", "碳酸锂"],
        "representative_stocks": ["002594", "300750", "600884", "002456", "300014"],
        "theme_type": "新能源",
    },
    "光伏储能": {
        "description": "光伏组件、逆变器、储能电池、绿氢",
        "keywords": ["光伏", "储能", "逆变器", "硅片", "绿氢", "钙钛矿", "风电"],
        "representative_stocks": ["601012", "300274", "688599", "002129", "300116"],
        "theme_type": "新能源",
    },
    "消费复苏": {
        "description": "白酒、餐饮、旅游、免税、可选消费回暖",
        "keywords": ["消费复苏", "白酒", "餐饮", "旅游", "免税", "出行", "内需"],
        "representative_stocks": ["600519", "000858", "000568", "601888", "600036"],
        "theme_type": "消费",
    },
    "创新药": {
        "description": "国内创新药企业、ADC抗体、GLP-1、肿瘤免疫",
        "keywords": ["创新药", "ADC", "GLP-1", "肿瘤", "CAR-T", "生物类似药", "出海"],
        "representative_stocks": ["603259", "688180", "300760", "600276", "688389"],
        "theme_type": "医药",
    },
    "军工国防": {
        "description": "航空发动机、导弹、雷达、船舶军工",
        "keywords": ["军工", "国防", "航发", "导弹", "军舰", "装备", "航空发动机"],
        "representative_stocks": ["600893", "600760", "002013", "600765", "688609"],
        "theme_type": "军工",
    },
    "国企改革": {
        "description": "央企整合、市值管理、国企分红提升",
        "keywords": ["国企改革", "央企整合", "市值管理", "分红", "混改", "国资"],
        "representative_stocks": ["601857", "600028", "601088", "601800", "601989"],
        "theme_type": "政策",
    },
    "数据要素": {
        "description": "数字经济、数据确权、数据交易所、数字政府",
        "keywords": ["数据要素", "数字经济", "数据确权", "数据交易", "数字中国"],
        "representative_stocks": ["300014", "688561", "002410", "600570", "688111"],
        "theme_type": "政策",
    },
    "高端制造": {
        "description": "工业母机、精密制造、航空航天零部件",
        "keywords": ["工业母机", "高端制造", "精密", "航空零部件", "数控机床"],
        "representative_stocks": ["601100", "002249", "300659", "688011", "002506"],
        "theme_type": "制造",
    },
    "大宗商品": {
        "description": "铜、铝、黄金、煤炭、石油，受益于全球通胀或供给收缩",
        "keywords": ["铜价", "黄金", "煤炭", "石油", "大宗商品", "资源股", "涨价"],
        "representative_stocks": ["601600", "601899", "600188", "601857", "600362"],
        "theme_type": "资源",
    },
    "出海主题": {
        "description": "产品/服务出海：家电、工程机械、新能源车出口",
        "keywords": ["出海", "海外收入", "东南亚", "一带一路", "出口", "全球化"],
        "representative_stocks": ["000333", "000425", "600690", "002594", "603596"],
        "theme_type": "出口",
    },
}

# ---------------------------------------------------------------------------
# LLM prompts
# ---------------------------------------------------------------------------

_NARRATIVE_SCORING_PROMPT = """\
你是A股投资叙事分析专家。请对以下投资叙事进行当前热度评分。

## 叙事名称
{narrative_name}

## 叙事描述
{narrative_description}

## 近期相关新闻/研报（最多20条）
{news_text}

## 评分标准
请对该叙事的当前扩散阶段打分（0-100）：
- 0-25  萌芽期：少量专业报告提及，主流媒体尚未关注，板块涨幅有限
- 25-50 扩散期：多家券商研报，行业媒体广泛报道，板块出现阶段性上涨
- 50-75 成熟期：主流财经媒体头条，大量散户关注，估值已明显提升
- 75-100 衰退期：叙事开始被质疑，板块分化，资金撤离

请以JSON格式输出：
{{
  "narrative_score": 0-100的整数,
  "phase": "萌芽期/扩散期/成熟期/衰退期",
  "evidence_strength": "weak/moderate/strong",
  "key_catalysts": ["近期核心催化剂1", "核心催化剂2"],
  "leading_stocks": ["最受益的2-3只股票代码"],
  "laggard_stocks": ["还没有充分反映叙事的股票代码，可为空"],
  "risk_factors": ["叙事面临的主要风险"],
  "phase_reasoning": "当前阶段判断的主要依据（50字内）"
}}
"""

_STOCK_NARRATIVE_FIT_PROMPT = """\
你是A股行业研究员。请评估以下股票与当前投资叙事的契合度。

叙事: {narrative_name}
叙事描述: {narrative_description}
叙事当前阶段: {phase}
叙事评分: {score}/100

股票列表（代码 + 名称）:
{stock_list}

请以JSON格式输出，包含每只股票的评估：
{{
  "assessments": [
    {{
      "stock_code": "代码",
      "fit_score": 0-10整数（10=核心受益，0=无关），
      "fit_reason": "契合原因（30字内）",
      "is_leading": true/false（是否已有充分反映）,
      "expected_change_pct": 预期涨跌幅（如10.0表示+10%，基于叙事阶段）
    }}
  ]
}}
"""

# ---------------------------------------------------------------------------
# Strategy implementation
# ---------------------------------------------------------------------------


class NarrativeTrackerStrategy(BaseStrategy):
    """Track investment narrative diffusion and generate phase-aware signals.

    Parameters
    ----------
    min_evidence_strength : str
        Minimum evidence strength ("weak"/"moderate"/"strong") to act on.
    narratives_per_run : int
        How many narratives to scan per run (ranked by recent news volume).
    holding_days : int
        Default holding period.
    """

    def __init__(
        self,
        min_evidence_strength: str = "moderate",
        narratives_per_run: int = 6,
        holding_days: int = 20,
        signal_dir: Path | str | None = None,
    ) -> None:
        super().__init__(signal_dir=signal_dir)
        self._min_strength = min_evidence_strength
        self._narratives_per_run = narratives_per_run
        self._holding_days = holding_days

        self._news = NewsCollector()
        self._research = ResearchCollector()
        self._market = MarketDataCollector()
        self._llm = create_llm_client(strategy_name=self.name)
        self._code_name_cache: Dict[str, str] = {}

    @property
    def name(self) -> str:
        return "narrative_tracker"

    # ── BaseStrategy interface ──────────────────────────────────────────

    def run(self, stock_codes: list[str] | None = None) -> list[StrategySignal]:
        """Full narrative tracking pipeline.

        1. Select most active narratives based on recent news volume.
        2. Score each narrative's phase.
        3. Match stocks to narratives and generate phase-appropriate signals.
        """
        logger.info("[%s] Starting narrative tracker ...", self.name)

        # 1. Select narratives to scan
        active_narratives = self._select_active_narratives()
        if not active_narratives:
            logger.info("[%s] No active narratives detected.", self.name)
            return []

        signals: list[StrategySignal] = []

        for narrative_name in active_narratives[: self._narratives_per_run]:
            try:
                narrative_signals = self._process_narrative(
                    narrative_name, stock_codes=stock_codes
                )
                signals.extend(narrative_signals)
            except Exception as exc:
                logger.error("[%s] Narrative '%s' failed: %s",
                             self.name, narrative_name, exc)

        logger.info("[%s] Generated %d signals across %d narratives.",
                    self.name, len(signals), len(active_narratives))
        return signals

    def run_single(self, stock_code: str) -> list[StrategySignal]:
        """Evaluate a single stock across all tracked narratives."""
        all_signals: list[StrategySignal] = []
        for narrative_name in list(NARRATIVES.keys())[: self._narratives_per_run]:
            try:
                sigs = self._process_narrative(narrative_name, stock_codes=[stock_code])
                all_signals.extend(sigs)
            except Exception as exc:
                logger.warning("[%s] Skipping narrative '%s': %s",
                               self.name, narrative_name, exc)
        return all_signals

    # ── narrative selection ─────────────────────────────────────────────

    def _select_active_narratives(self) -> list[str]:
        """Rank narratives by recent news volume to prioritise which to score.

        Uses keyword matching against market hot topics (fast, no LLM cost).
        """
        hot_news: list[dict] = []
        try:
            hot_news = self._news.get_market_hot_topics(limit=50)
        except Exception as exc:
            logger.warning("[%s] Hot topics fetch failed: %s", self.name, exc)

        # Score each narrative by keyword hits in hot news
        scores: dict[str, int] = {}
        for nname, ndata in NARRATIVES.items():
            keywords = ndata.get("keywords", [])
            count = 0
            for item in hot_news:
                text = json.dumps(item, ensure_ascii=False)
                count += sum(1 for kw in keywords if kw in text)
            scores[nname] = count

        # Sort by hit count; narratives with zero hits still included
        ranked = sorted(scores.keys(), key=lambda n: scores[n], reverse=True)
        logger.info(
            "[%s] Active narratives (top-3): %s",
            self.name,
            [(n, scores[n]) for n in ranked[:3]],
        )
        return ranked

    # ── narrative processing ────────────────────────────────────────────

    def _process_narrative(
        self, narrative_name: str, stock_codes: list[str] | None = None
    ) -> list[StrategySignal]:
        """Score one narrative and generate signals for relevant stocks."""
        ndata = NARRATIVES[narrative_name]

        # 1. Gather relevant news
        news_text = self._gather_narrative_news(narrative_name, ndata)

        # 2. Score narrative phase via LLM
        scoring = self._score_narrative(narrative_name, ndata, news_text)
        if not scoring:
            return []

        phase = scoring.get("phase", "成熟期")
        score = scoring.get("narrative_score", 50)
        evidence = scoring.get("evidence_strength", "weak")

        logger.info(
            "[%s] Narrative '%s': phase=%s score=%d evidence=%s",
            self.name, narrative_name, phase, score, evidence,
        )

        # Skip if evidence is too weak
        strength_rank = {"weak": 0, "moderate": 1, "strong": 2}
        if strength_rank.get(evidence, 0) < strength_rank.get(self._min_strength, 1):
            logger.info("[%s] Skipping '%s' — weak evidence", self.name, narrative_name)
            return []

        # 3. Determine target stocks
        candidate_codes = self._get_candidate_stocks(ndata, scoring, stock_codes)
        if not candidate_codes:
            return []

        # 4. Assess per-stock fit (only for non-mature phases)
        signals = self._generate_signals(
            narrative_name=narrative_name,
            ndata=ndata,
            scoring=scoring,
            phase=phase,
            score=score,
            candidate_codes=candidate_codes,
        )
        return signals

    def _gather_narrative_news(self, narrative_name: str, ndata: dict) -> str:
        """Collect news articles relevant to a narrative."""
        keywords = ndata.get("keywords", [])
        rep_stocks = ndata.get("representative_stocks", [])
        news_items: list[dict] = []

        # Company-specific news for representative stocks
        for code in rep_stocks[:3]:
            try:
                items = self._news.get_company_news(code, limit=5)
                news_items.extend(items)
            except Exception:
                pass

        # Industry news via keyword
        if keywords:
            try:
                kw_news = self._news.get_industry_news(keywords[0], limit=10)
                news_items.extend(kw_news)
            except Exception:
                pass

        # Format
        lines: list[str] = []
        seen: set[str] = set()
        for item in news_items[:20]:
            title = (
                item.get("新闻标题") or item.get("标题")
                or item.get("名称", "")
            )
            time_str = item.get("发布时间") or item.get("时间", "")
            source = item.get("文章来源") or item.get("来源", "")
            title_str = str(title)[:100]
            if title_str and title_str not in seen:
                seen.add(title_str)
                lines.append(f"- [{time_str}][{source}] {title_str}")

        return "\n".join(lines) if lines else "（暂无相关新闻）"

    def _score_narrative(
        self, narrative_name: str, ndata: dict, news_text: str
    ) -> Optional[dict]:
        """Call LLM to score the narrative's current phase."""
        prompt = _NARRATIVE_SCORING_PROMPT.format(
            narrative_name=narrative_name,
            narrative_description=ndata.get("description", ""),
            news_text=news_text,
        )
        messages = [
            {"role": "system", "content": "你是A股投资叙事分析专家。严格以JSON格式输出。"},
            {"role": "user", "content": prompt},
        ]
        try:
            return self._llm.chat_json(messages=messages, max_tokens=800)
        except Exception as exc:
            logger.error("[%s] Narrative scoring LLM failed: %s", self.name, exc)
            return None

    def _get_candidate_stocks(
        self,
        ndata: dict,
        scoring: dict,
        universe: list[str] | None,
    ) -> list[str]:
        """Combine representative + LLM-suggested stocks, optionally filtered by universe."""
        candidates: set[str] = set()
        candidates.update(ndata.get("representative_stocks", []))
        candidates.update(scoring.get("leading_stocks", []))
        candidates.update(scoring.get("laggard_stocks", []))

        # Filter to universe if provided
        if universe:
            candidates = candidates & set(universe)

        return list(candidates)

    def _generate_signals(
        self,
        narrative_name: str,
        ndata: dict,
        scoring: dict,
        phase: str,
        score: int,
        candidate_codes: list[str],
    ) -> list[StrategySignal]:
        """Generate signals based on narrative phase and per-stock fit."""
        signals: list[StrategySignal] = []

        # Phase-based direction & base confidence
        phase_config = {
            "萌芽期":  {"direction": "long",    "base_conf": 0.65, "exp_ret":  0.08},
            "扩散期":  {"direction": "long",    "base_conf": 0.55, "exp_ret":  0.05},
            "成熟期":  {"direction": "neutral", "base_conf": 0.40, "exp_ret":  0.01},
            "衰退期":  {"direction": "avoid",   "base_conf": 0.55, "exp_ret": -0.05},
        }
        cfg = phase_config.get(phase, phase_config["成熟期"])

        # For mature/avoid phases, signal all candidates
        # For early phases, prefer leading stocks from LLM
        leading = set(scoring.get("leading_stocks", []))
        laggard = set(scoring.get("laggard_stocks", []))

        for code in candidate_codes:
            # Adjust confidence and direction for individual stocks
            direction = cfg["direction"]
            confidence = cfg["base_conf"]
            expected_return = cfg["exp_ret"]
            stock_reason = ""

            # In 成熟期: avoid if pure-theme play, neutral otherwise
            if phase == "成熟期" and code in leading:
                direction = "avoid"
                confidence = 0.45
                expected_return = -0.02
                stock_reason = "成熟期领涨股，估值充分反映"

            # In 萌芽/扩散期: boost confidence for laggards (catch-up alpha)
            if phase in ("萌芽期", "扩散期") and code in laggard:
                confidence = min(0.80, confidence + 0.15)
                expected_return = min(0.12, expected_return + 0.04)
                stock_reason = "叙事扩散中的补涨标的"

            # Evidence-strength discount
            if scoring.get("evidence_strength") == "weak":
                confidence *= 0.7
            elif scoring.get("evidence_strength") == "strong":
                confidence = min(0.85, confidence * 1.2)

            confidence = max(0.15, min(0.85, confidence))

            # Get stock name (best-effort)
            if not self._code_name_cache:
                try:
                    df_names = self._market.get_realtime_quotes()
                    if not df_names.empty and "代码" in df_names.columns and "名称" in df_names.columns:
                        self._code_name_cache = dict(zip(df_names["代码"], df_names["名称"]))
                except Exception:
                    pass
            stock_name = self._code_name_cache.get(code, code)

            # Holding days based on phase
            hold_days_map = {
                "萌芽期": 30,
                "扩散期": 20,
                "成熟期": 5,
                "衰退期": 15,
            }
            holding = hold_days_map.get(phase, self._holding_days)

            # Catalysts text
            catalysts = "; ".join(scoring.get("key_catalysts", [])[:2])
            risks = "; ".join(scoring.get("risk_factors", [])[:1])

            reasoning = (
                f"[叙事追踪] {narrative_name} | {phase}(分值{score}) | "
                f"催化剂:{catalysts[:40]} | "
                + (f"风险:{risks[:30]} | " if risks else "")
                + (f"{stock_reason}" if stock_reason else "")
            )

            metadata: Dict[str, Any] = {
                "narrative_name": narrative_name,
                "narrative_phase": phase,
                "narrative_score": score,
                "evidence_strength": scoring.get("evidence_strength", "weak"),
                "phase_reasoning": scoring.get("phase_reasoning", ""),
                "key_catalysts": scoring.get("key_catalysts", []),
                "risk_factors": scoring.get("risk_factors", []),
                "is_leading_stock": code in leading,
                "is_laggard_stock": code in laggard,
                "theme_type": ndata.get("theme_type", ""),
            }

            signals.append(StrategySignal(
                strategy_name=self.name,
                stock_code=code,
                stock_name=stock_name,
                direction=direction,
                confidence=round(confidence, 2),
                expected_return=round(expected_return, 4),
                holding_period_days=holding,
                reasoning=reasoning,
                metadata=metadata,
            ))

        logger.info(
            "[%s] '%s' phase=%s → %d signals",
            self.name, narrative_name, phase, len(signals),
        )
        return signals

    # ── utility: get current narrative landscape ────────────────────────

    def scan_all_narratives(self) -> list[dict]:
        """Scan all defined narratives and return a scored landscape.

        Useful for dashboard / report generation. Does NOT generate signals.
        Returns a list of dicts with narrative_name, phase, score, evidence.
        """
        results: list[dict] = []
        for nname, ndata in NARRATIVES.items():
            try:
                news_text = self._gather_narrative_news(nname, ndata)
                scoring = self._score_narrative(nname, ndata, news_text)
                if scoring:
                    results.append({
                        "narrative_name": nname,
                        "theme_type": ndata.get("theme_type", ""),
                        "phase": scoring.get("phase", "未知"),
                        "score": scoring.get("narrative_score", 0),
                        "evidence_strength": scoring.get("evidence_strength", "weak"),
                        "phase_reasoning": scoring.get("phase_reasoning", ""),
                        "key_catalysts": scoring.get("key_catalysts", []),
                    })
            except Exception as exc:
                logger.warning("[%s] Scan failed for '%s': %s", self.name, nname, exc)

        results.sort(key=lambda x: x["score"], reverse=True)
        logger.info("[%s] Scanned %d narratives.", self.name, len(results))
        return results
