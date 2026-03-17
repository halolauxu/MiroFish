"""
Announcement Sentiment Change Strategy (S06)
==============================================
**DEPRECATED**: Announcement sentiment analysis has been merged into S10
(SentimentSimulationStrategy) as an event enrichment input. S10 calls
S06.collect_announcements(), analyze_sentiment(), and compute_sentiment_trajectory()
to detect inflection points and boost conviction. This file is kept for
reference and as a library of reusable methods.

Original description:
Track sentiment changes in a company's announcements over time.  When
sentiment shifts from negative to positive (or vice versa) before the
market reacts, generate trading signals.

Core idea
---------
1. Collect recent announcements (90 days) and filter for material ones.
2. Batch-analyse sentiment via LLM (token-efficient: multiple
   announcements per call).
3. Compute a rolling sentiment trajectory and detect inflection points.
4. Check whether the stock price already aligns with the sentiment trend;
   misalignment = opportunity.

Typical holding period: 15-30 trading days.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from astrategy.config import settings
from astrategy.data_collector.announcement import AnnouncementCollector
from astrategy.data_collector.market_data import MarketDataCollector
from astrategy.llm import create_llm_client
from astrategy.strategies.base import BaseStrategy, StrategySignal

logger = logging.getLogger("astrategy.strategies.s06_announcement_sentiment")

_CST = timezone(timedelta(hours=8))

# Prompt template on disk (optional; inline fallback below)
_PROMPT_TEMPLATE_PATH = (
    Path(__file__).resolve().parent.parent
    / "prompt_templates"
    / "sentiment_analysis.txt"
)

# Maximum announcements to send per LLM batch call
_BATCH_SIZE = 8

# ---------------------------------------------------------------------------
# Inline batch-sentiment prompt (used when the template file is absent or
# when we want to batch multiple announcements in a single call)
# ---------------------------------------------------------------------------
_BATCH_SENTIMENT_PROMPT = """\
你是一位专业的A股市场舆情分析师。请对以下{count}条公告进行情感分析。

公司: {stock_code} {stock_name}

{announcements_text}

请严格以JSON格式输出，包含一个 "results" 数组，每个元素对应一条公告:
{{
  "results": [
    {{
      "index": 0,
      "sentiment_score": 0.0到1.0之间的数值（-1.0极度消极，1.0极度积极）,
      "key_phrases": ["关键短语1", "关键短语2"],
      "category": "业绩公告/分红送转/股权变动/重大合同/资产重组/行政处罚/诉讼仲裁/高管变动/增减持/回购注销/政策法规/行业动态/日常运营/其他",
      "urgency": "high/medium/low"
    }}
  ]
}}

注意:
- sentiment_score 取值 -1.0 到 1.0
- 注意区分实质利好/利空与例行公告
- index 必须与公告序号一一对应
"""

# ---------------------------------------------------------------------------
# Sentiment cache (announcement content doesn't change)
# ---------------------------------------------------------------------------
_sentiment_cache: Dict[str, dict] = {}


def _ann_cache_key(ann: dict) -> str:
    """Deterministic cache key for an announcement dict."""
    title = ann.get("公告标题", ann.get("标题", ann.get("title", "")))
    date = str(ann.get("公告日期", ann.get("日期", ann.get("date", ""))))
    raw = f"{title}|{date}"
    return hashlib.md5(raw.encode()).hexdigest()


class AnnouncementSentimentStrategy(BaseStrategy):
    """Announcement sentiment change strategy.

    Parameters
    ----------
    lookback_days : int
        How many days of announcements to collect (default 90).
    window : int
        Rolling sentiment window size in number of announcements (default 5).
    holding_days : int
        Default holding period for generated signals (default 20).
    signal_dir : Path | str | None
        Override signal persistence directory.
    """

    def __init__(
        self,
        lookback_days: int = 90,
        window: int = 5,
        holding_days: int = 20,
        signal_dir: Path | str | None = None,
    ) -> None:
        super().__init__(signal_dir=signal_dir)
        self._lookback_days = lookback_days
        self._window = window
        self._holding_days = holding_days

        self._announcement = AnnouncementCollector()
        self._market = MarketDataCollector()
        self._llm = create_llm_client(strategy_name=self.name)

    # ── BaseStrategy interface ────────────────────────────────────────

    @property
    def name(self) -> str:
        return "announcement_sentiment"

    # ==================================================================
    # 1. collect_announcements
    # ==================================================================

    def collect_announcements(
        self, stock_code: str, days: int = 90,
    ) -> list[dict]:
        """Get company announcements, filter for important ones, sort chronologically.

        Parameters
        ----------
        stock_code : str
            6-digit A-share stock code.
        days : int
            Look-back window in calendar days.

        Returns
        -------
        list[dict]
            Important announcements sorted oldest-first.
        """
        end_dt = datetime.now(tz=_CST)
        start_dt = end_dt - timedelta(days=days)
        start_str = start_dt.strftime("%Y%m%d")
        end_str = end_dt.strftime("%Y%m%d")

        raw = self._announcement.get_company_announcements(stock_code, start_str, end_str)
        if not raw:
            logger.info("[%s] No announcements found for %s", self.name, stock_code)
            return []

        # Keep only material announcements
        important = self._announcement.filter_important_announcements(raw)
        logger.info(
            "[%s] %s: %d raw announcements -> %d important",
            self.name, stock_code, len(raw), len(important),
        )

        # Sort chronologically (oldest first)
        important = self._sort_chronologically(important)
        return important

    # ==================================================================
    # 2. analyze_sentiment (single announcement)
    # ==================================================================

    def analyze_sentiment(self, announcement: dict) -> dict:
        """Analyse a single announcement's sentiment via LLM (with caching).

        Returns
        -------
        dict
            Keys: sentiment_score, key_phrases, category, urgency, date, title
        """
        cache_key = _ann_cache_key(announcement)
        if cache_key in _sentiment_cache:
            return _sentiment_cache[cache_key]

        title = self._get_title(announcement)
        date_str = self._get_date_str(announcement)

        messages = [
            {
                "role": "system",
                "content": (
                    "你是一位专业的A股公告情感分析师。请严格以JSON格式输出。"
                ),
            },
            {
                "role": "user",
                "content": (
                    f"请分析以下A股公告的情感倾向:\n\n"
                    f"标题: {title}\n日期: {date_str}\n\n"
                    f"输出JSON: {{\"sentiment_score\": float(-1到1), "
                    f"\"key_phrases\": [str], "
                    f"\"category\": str, \"urgency\": \"high/medium/low\"}}"
                ),
            },
        ]

        try:
            result = self._llm.chat_json(messages=messages, max_tokens=512)
        except Exception as exc:
            logger.warning("Single sentiment analysis failed for '%s': %s", title, exc)
            result = self._rule_based_sentiment(title)

        result["date"] = date_str
        result["title"] = title
        _sentiment_cache[cache_key] = result
        return result

    # ==================================================================
    # 2b. batch sentiment analysis (token-efficient)
    # ==================================================================

    def _analyze_sentiment_batch(
        self,
        stock_code: str,
        stock_name: str,
        announcements: list[dict],
    ) -> list[dict]:
        """Analyse multiple announcements in batched LLM calls.

        Announcements already in the cache are skipped.  The remaining are
        grouped into batches of ``_BATCH_SIZE`` and sent together.

        Returns
        -------
        list[dict]
            One sentiment dict per input announcement (order preserved).
        """
        results: list[dict] = [{}] * len(announcements)
        uncached_indices: list[int] = []

        # Populate from cache
        for i, ann in enumerate(announcements):
            ck = _ann_cache_key(ann)
            if ck in _sentiment_cache:
                results[i] = _sentiment_cache[ck]
            else:
                uncached_indices.append(i)

        if not uncached_indices:
            return results

        # Process uncached in batches
        for batch_start in range(0, len(uncached_indices), _BATCH_SIZE):
            batch_idx = uncached_indices[batch_start: batch_start + _BATCH_SIZE]
            batch_anns = [announcements[i] for i in batch_idx]

            # Build announcements text block
            ann_lines: list[str] = []
            for seq, ann in enumerate(batch_anns):
                title = self._get_title(ann)
                date_str = self._get_date_str(ann)
                ann_lines.append(f"[{seq}] 日期: {date_str} | 标题: {title}")

            prompt = _BATCH_SENTIMENT_PROMPT.format(
                count=len(batch_anns),
                stock_code=stock_code,
                stock_name=stock_name,
                announcements_text="\n".join(ann_lines),
            )

            messages = [
                {
                    "role": "system",
                    "content": "你是专业的A股公告情感分析师。严格以JSON格式输出。",
                },
                {"role": "user", "content": prompt},
            ]

            try:
                response = self._llm.chat_json(messages=messages, max_tokens=1024)
                batch_results = response.get("results", [])
            except Exception as exc:
                logger.warning(
                    "Batch sentiment failed for %s (batch size %d): %s",
                    stock_code, len(batch_anns), exc,
                )
                # Fall back to rule-based for the whole batch
                batch_results = []
                for ann in batch_anns:
                    batch_results.append(
                        self._rule_based_sentiment(self._get_title(ann))
                    )

            # Map batch results back
            for seq, idx in enumerate(batch_idx):
                ann = announcements[idx]
                if seq < len(batch_results):
                    r = batch_results[seq]
                else:
                    r = self._rule_based_sentiment(self._get_title(ann))

                # Normalise
                r.setdefault("sentiment_score", 0.0)
                r.setdefault("key_phrases", [])
                r.setdefault("category", "其他")
                r.setdefault("urgency", "low")

                # Clamp score
                try:
                    r["sentiment_score"] = max(-1.0, min(1.0, float(r["sentiment_score"])))
                except (ValueError, TypeError):
                    r["sentiment_score"] = 0.0

                # Flatten key_phrases if they are dicts
                if r["key_phrases"] and isinstance(r["key_phrases"][0], dict):
                    r["key_phrases"] = [
                        kp.get("phrase", str(kp)) for kp in r["key_phrases"]
                    ]

                r["date"] = self._get_date_str(ann)
                r["title"] = self._get_title(ann)

                results[idx] = r
                _sentiment_cache[_ann_cache_key(ann)] = r

        return results

    # ==================================================================
    # 3. compute_sentiment_trajectory
    # ==================================================================

    def compute_sentiment_trajectory(
        self,
        stock_code: str,
        sentiments: list[dict],
    ) -> dict:
        """Compute rolling sentiment statistics and detect inflection points.

        Parameters
        ----------
        stock_code : str
            For logging/context.
        sentiments : list[dict]
            Chronologically ordered sentiment results.

        Returns
        -------
        dict
            current_sentiment, trend, inflection_detected, magnitude_of_change,
            rolling_averages, derivative
        """
        if not sentiments:
            return {
                "current_sentiment": 0.0,
                "trend": "stable",
                "inflection_detected": False,
                "magnitude_of_change": 0.0,
                "rolling_averages": [],
                "derivative": [],
            }

        scores = [s.get("sentiment_score", 0.0) for s in sentiments]

        # Rolling average (window = self._window)
        w = min(self._window, len(scores))
        rolling: list[float] = []
        for i in range(len(scores)):
            window_start = max(0, i - w + 1)
            window_slice = scores[window_start: i + 1]
            rolling.append(sum(window_slice) / len(window_slice))

        # Derivative (first differences of rolling averages)
        derivative: list[float] = []
        for i in range(1, len(rolling)):
            derivative.append(rolling[i] - rolling[i - 1])

        # Detect inflection: sign change in derivative
        inflection_detected = False
        if len(derivative) >= 2:
            last = derivative[-1]
            prev = derivative[-2]
            if last * prev < 0:  # sign change
                inflection_detected = True

        # Current sentiment (latest rolling average)
        current_sentiment = rolling[-1] if rolling else 0.0

        # Trend
        if len(rolling) >= 3:
            recent_slope = rolling[-1] - rolling[-min(3, len(rolling))]
            if recent_slope > 0.1:
                trend = "improving"
            elif recent_slope < -0.1:
                trend = "deteriorating"
            else:
                trend = "stable"
        else:
            trend = "stable"

        # Magnitude of change: difference between earliest and latest rolling
        magnitude = rolling[-1] - rolling[0] if len(rolling) >= 2 else 0.0

        return {
            "current_sentiment": round(current_sentiment, 4),
            "trend": trend,
            "inflection_detected": inflection_detected,
            "magnitude_of_change": round(magnitude, 4),
            "rolling_averages": [round(r, 4) for r in rolling],
            "derivative": [round(d, 4) for d in derivative],
        }

    # ==================================================================
    # 4. detect_language_shifts
    # ==================================================================

    def detect_language_shifts(self, sentiments: list[dict]) -> dict:
        """Track specific management-language shifts across announcements.

        Compares key phrases from the earliest third vs the latest third of
        the sentiment list.  Detects tonal migration such as:
        "稳健增长" -> "承压" -> "显著改善"

        Returns
        -------
        dict
            early_phrases, recent_phrases, tone_shift, shift_direction
        """
        if len(sentiments) < 2:
            return {
                "early_phrases": [],
                "recent_phrases": [],
                "tone_shift": False,
                "shift_direction": "none",
            }

        split = max(1, len(sentiments) // 3)
        early = sentiments[:split]
        recent = sentiments[-split:]

        early_phrases: list[str] = []
        recent_phrases: list[str] = []
        for s in early:
            early_phrases.extend(s.get("key_phrases", []))
        for s in recent:
            recent_phrases.extend(s.get("key_phrases", []))

        # Positive/negative keyword buckets
        positive_kw = {
            "增长", "改善", "突破", "创新高", "超预期", "稳健",
            "提升", "加速", "扩张", "利好", "显著改善", "大幅增长",
        }
        negative_kw = {
            "下降", "承压", "亏损", "风险", "减少", "下滑",
            "低于预期", "萎缩", "收缩", "恶化", "大幅下降", "不确定",
        }

        def _tone_score(phrases: list[str]) -> float:
            if not phrases:
                return 0.0
            pos = sum(1 for p in phrases if any(k in p for k in positive_kw))
            neg = sum(1 for p in phrases if any(k in p for k in negative_kw))
            total = pos + neg
            if total == 0:
                return 0.0
            return (pos - neg) / total

        early_tone = _tone_score(early_phrases)
        recent_tone = _tone_score(recent_phrases)
        delta = recent_tone - early_tone

        tone_shift = abs(delta) > 0.3
        if delta > 0.3:
            shift_direction = "negative_to_positive"
        elif delta < -0.3:
            shift_direction = "positive_to_negative"
        else:
            shift_direction = "none"

        return {
            "early_phrases": early_phrases[:10],
            "recent_phrases": recent_phrases[:10],
            "tone_shift": tone_shift,
            "shift_direction": shift_direction,
            "early_tone_score": round(early_tone, 3),
            "recent_tone_score": round(recent_tone, 3),
        }

    # ==================================================================
    # 5. check_price_alignment
    # ==================================================================

    def check_price_alignment(
        self, stock_code: str, sentiment_trend: str,
    ) -> dict:
        """Compare sentiment direction vs recent price direction.

        Misalignment (positive sentiment + falling price, or vice versa)
        represents a potential trading opportunity.

        Returns
        -------
        dict
            price_change_pct, price_direction, aligned, divergence_score
        """
        end_dt = datetime.now(tz=_CST)
        start_dt = end_dt - timedelta(days=30)
        start_str = start_dt.strftime("%Y%m%d")
        end_str = end_dt.strftime("%Y%m%d")

        try:
            df = self._market.get_daily_quotes(stock_code, start_str, end_str)
        except Exception as exc:
            logger.warning("Price data unavailable for %s: %s", stock_code, exc)
            return {
                "price_change_pct": None,
                "price_direction": "unknown",
                "aligned": True,
                "divergence_score": 0.0,
            }

        if df is None or df.empty or len(df) < 2:
            return {
                "price_change_pct": None,
                "price_direction": "unknown",
                "aligned": True,
                "divergence_score": 0.0,
            }

        closes = df["收盘"].astype(float).values
        price_change = (closes[-1] / closes[0] - 1.0) * 100.0

        if price_change > 2.0:
            price_direction = "up"
        elif price_change < -2.0:
            price_direction = "down"
        else:
            price_direction = "flat"

        # Determine alignment
        sentiment_positive = sentiment_trend == "improving"
        sentiment_negative = sentiment_trend == "deteriorating"

        if sentiment_positive and price_direction == "down":
            aligned = False
            divergence_score = abs(price_change) / 10.0  # normalise
        elif sentiment_negative and price_direction == "up":
            aligned = False
            divergence_score = abs(price_change) / 10.0
        else:
            aligned = True
            divergence_score = 0.0

        divergence_score = min(1.0, divergence_score)

        return {
            "price_change_pct": round(price_change, 2),
            "price_direction": price_direction,
            "aligned": aligned,
            "divergence_score": round(divergence_score, 4),
        }

    # ==================================================================
    # 6. run
    # ==================================================================

    def run(self, stock_codes: list[str] | None = None) -> list[StrategySignal]:
        """Full announcement-sentiment pipeline.

        1. Collect announcements (cheap).
        2. Pre-filter with rules (no LLM).
        3. Batch-analyse sentiment (LLM, token-efficient).
        4. Compute trajectory and check price alignment.
        5. Generate signals where inflection + misalignment detected.

        Parameters
        ----------
        stock_codes :
            Universe of stock codes.  Returns empty if ``None``.
        """
        if not stock_codes:
            logger.warning("[%s] No stock codes provided; returning empty.", self.name)
            return []

        logger.info(
            "[%s] Starting announcement sentiment analysis for %d stocks ...",
            self.name, len(stock_codes),
        )

        max_stocks = getattr(settings.strategy, "max_stocks_per_run", 20)
        stock_codes = stock_codes[:max_stocks]

        signals: list[StrategySignal] = []
        for code in stock_codes:
            try:
                stock_signals = self.run_single(code)
                signals.extend(stock_signals)
            except Exception as exc:
                logger.error("[%s] Failed for %s: %s", self.name, code, exc)

        logger.info("[%s] Generated %d signals total.", self.name, len(signals))
        return signals

    # ==================================================================
    # 7. run_single
    # ==================================================================

    def run_single(self, stock_code: str) -> list[StrategySignal]:
        """Run announcement sentiment analysis for a single stock.

        Returns
        -------
        list[StrategySignal]
            Zero or one signal.
        """
        logger.info("[%s] Analysing %s ...", self.name, stock_code)

        # 1. Collect announcements
        announcements = self.collect_announcements(stock_code, days=self._lookback_days)
        if not announcements:
            logger.info("[%s] No important announcements for %s", self.name, stock_code)
            return []

        # Resolve stock name from announcements
        stock_name = self._resolve_stock_name(announcements, stock_code)

        # 2. Batch-analyse sentiment (token-efficient)
        sentiments = self._analyze_sentiment_batch(stock_code, stock_name, announcements)

        # Filter out empty results (shouldn't happen, but be safe)
        sentiments = [s for s in sentiments if s]
        if not sentiments:
            return []

        # 3. Compute trajectory
        trajectory = self.compute_sentiment_trajectory(stock_code, sentiments)

        # 4. Detect language shifts
        language_shifts = self.detect_language_shifts(sentiments)

        # 5. Check price alignment
        alignment = self.check_price_alignment(stock_code, trajectory["trend"])

        # 6. Generate signal
        signal = self._build_signal(
            stock_code=stock_code,
            stock_name=stock_name,
            sentiments=sentiments,
            trajectory=trajectory,
            language_shifts=language_shifts,
            alignment=alignment,
        )

        if signal is None:
            return []
        return [signal]

    # ==================================================================
    # Signal construction
    # ==================================================================

    def _build_signal(
        self,
        stock_code: str,
        stock_name: str,
        sentiments: list[dict],
        trajectory: dict,
        language_shifts: dict,
        alignment: dict,
    ) -> Optional[StrategySignal]:
        """Combine analysis components into a trading signal.

        Generates a signal only when there is a sentiment inflection point
        or price-sentiment misalignment.
        """
        inflection = trajectory["inflection_detected"]
        misaligned = not alignment["aligned"]
        trend = trajectory["trend"]

        # Only generate a signal when there's actionable information
        if not inflection and not misaligned and trend == "stable":
            logger.debug(
                "[%s] %s: no inflection, no misalignment, stable -> skip",
                self.name, stock_code,
            )
            return None

        # Direction logic
        current_sentiment = trajectory["current_sentiment"]
        if trend == "improving" and (inflection or misaligned):
            direction = "long"
        elif trend == "deteriorating" and (inflection or misaligned):
            direction = "short"
        elif current_sentiment > 0.3:
            direction = "long"
        elif current_sentiment < -0.3:
            direction = "short"
        else:
            direction = "neutral"

        # Confidence
        base_confidence = min(1.0, abs(current_sentiment))

        inflection_bonus = 0.15 if inflection else 0.0
        misalignment_bonus = 0.10 * alignment["divergence_score"] if misaligned else 0.0
        language_bonus = 0.10 if language_shifts.get("tone_shift", False) else 0.0
        volume_bonus = min(0.10, len(sentiments) * 0.01)  # more data = more confident

        confidence = base_confidence + inflection_bonus + misalignment_bonus + language_bonus + volume_bonus
        confidence = max(0.05, min(1.0, confidence))

        # Expected return
        magnitude = abs(trajectory["magnitude_of_change"])
        divergence = alignment.get("divergence_score", 0.0)
        expected_return = min(0.15, (magnitude * 0.05 + divergence * 0.03))
        if direction == "short":
            expected_return = -expected_return

        # Collect key phrases from most recent sentiments
        recent_phrases: list[str] = []
        for s in sentiments[-3:]:
            recent_phrases.extend(s.get("key_phrases", []))
        recent_phrases = recent_phrases[:10]

        # Sentiment change: latest vs earliest
        first_score = sentiments[0].get("sentiment_score", 0.0)
        last_score = sentiments[-1].get("sentiment_score", 0.0)
        sentiment_change = last_score - first_score

        # Latest category
        latest_category = sentiments[-1].get("category", "其他") if sentiments else "其他"

        # Reasoning
        reasoning_parts = [
            f"情感趋势: {trend}",
            f"当前情感分: {current_sentiment:.2f}",
        ]
        if inflection:
            reasoning_parts.append("检测到情感拐点")
        if misaligned:
            price_dir = alignment.get("price_direction", "unknown")
            reasoning_parts.append(f"价格({price_dir})与情感({trend})背离")
        if language_shifts.get("tone_shift"):
            reasoning_parts.append(
                f"管理层措辞转变: {language_shifts['shift_direction']}"
            )
        reasoning_parts.append(f"分析公告数: {len(sentiments)}")

        reasoning = "; ".join(reasoning_parts)

        metadata: Dict[str, Any] = {
            "sentiment_score": round(current_sentiment, 4),
            "sentiment_change": round(sentiment_change, 4),
            "trend": trend,
            "inflection_detected": inflection,
            "key_phrases": recent_phrases,
            "price_sentiment_divergence": alignment.get("divergence_score", 0.0),
            "announcement_count": len(sentiments),
            "latest_category": latest_category,
        }

        return StrategySignal(
            strategy_name=self.name,
            stock_code=stock_code,
            stock_name=stock_name,
            direction=direction,
            confidence=round(confidence, 2),
            expected_return=round(expected_return, 4),
            holding_period_days=self._holding_days,
            reasoning=reasoning,
            metadata=metadata,
        )

    # ==================================================================
    # Internal helpers
    # ==================================================================

    @staticmethod
    def _get_title(ann: dict) -> str:
        return ann.get("公告标题", ann.get("标题", ann.get("title", "未知标题")))

    @staticmethod
    def _get_date_str(ann: dict) -> str:
        raw = ann.get("公告日期", ann.get("日期", ann.get("date", "")))
        if isinstance(raw, datetime):
            return raw.strftime("%Y-%m-%d")
        if hasattr(raw, "strftime"):
            return raw.strftime("%Y-%m-%d")
        return str(raw)

    @staticmethod
    def _resolve_stock_name(announcements: list[dict], fallback: str) -> str:
        for ann in announcements:
            name = ann.get("名称", ann.get("股票简称", ann.get("name", "")))
            if name:
                return str(name)
        return fallback

    @staticmethod
    def _sort_chronologically(announcements: list[dict]) -> list[dict]:
        """Sort announcements oldest-first by date."""
        def _parse_date(ann: dict) -> datetime:
            raw = ann.get("公告日期", ann.get("日期", ann.get("date", "")))
            if isinstance(raw, datetime):
                return raw
            if hasattr(raw, "to_pydatetime"):
                return raw.to_pydatetime()
            try:
                s = str(raw).replace("-", "")[:8]
                return datetime.strptime(s, "%Y%m%d")
            except (ValueError, TypeError):
                return datetime.min

        return sorted(announcements, key=_parse_date)

    @staticmethod
    def _rule_based_sentiment(title: str) -> dict:
        """Quick rule-based sentiment when LLM is unavailable."""
        positive_kw = [
            "业绩预增", "中标", "增持", "回购", "分红", "高送转",
            "战略合作", "重大合同", "涨价", "提价", "摘帽",
            "显著改善", "大幅增长", "超预期",
        ]
        negative_kw = [
            "业绩预减", "业绩预亏", "减持", "解禁", "立案",
            "处罚", "违规", "退市", "风险提示", "亏损",
            "下降", "承压", "下滑",
        ]

        pos = sum(1 for k in positive_kw if k in title)
        neg = sum(1 for k in negative_kw if k in title)

        if pos > neg:
            score = min(1.0, 0.3 + pos * 0.2)
        elif neg > pos:
            score = max(-1.0, -0.3 - neg * 0.2)
        else:
            score = 0.0

        category = "其他"
        if any(k in title for k in ("业绩", "利润", "营收")):
            category = "业绩公告"
        elif any(k in title for k in ("增持", "减持", "回购")):
            category = "股权变动"
        elif any(k in title for k in ("收购", "并购", "重组")):
            category = "资产重组"
        elif any(k in title for k in ("合同", "中标", "签署")):
            category = "重大合同"
        elif any(k in title for k in ("分红", "送股", "转增")):
            category = "分红送转"

        return {
            "sentiment_score": score,
            "key_phrases": [title[:20]],
            "category": category,
            "urgency": "medium",
        }
