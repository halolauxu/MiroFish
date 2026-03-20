#!/usr/bin/env python3
"""
Trading-desk control framework for the A-share strategy universe.

Goal:
    Connect the research stack into one top-level decision chain:

        regime
        -> mainline
        -> catalyst
        -> flow / expectation gap
        -> graph / sentiment filter
        -> execution mapping

Default mode is local-first: it loads the latest cached strategy signals and
research artifacts already present under ``astrategy/.data``. This keeps the
framework runnable even when live APIs or LLM calls are unavailable.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from astrategy.archive.authoritative_history import _MIN_BACKTEST_SPAN_DAYS


_CANDIDATE_STAGE_WEIGHTS = {
    "regime": 0.22,
    "mainline": 0.20,
    "catalyst": 0.34,
    "flow": 0.24,
}

_PHASE_MULTIPLIER = {
    "萌芽期": 0.85,
    "扩散期": 1.00,
    "成熟期": 0.20,
    "衰退期": -0.90,
}

_LINE_BLUEPRINTS: Dict[str, Dict[str, str]] = {
    "regime_rotation": {
        "label": "Regime 轮动",
        "stage": "regime",
        "role": "战场选择器",
        "monetization_path": "行业ETF/板块龙头轮动；收缩期优先高景气龙头，弱势行业做减仓与禁入。",
    },
    "mainline_narrative": {
        "label": "主线叙事",
        "stage": "mainline",
        "role": "进攻主引擎",
        "monetization_path": "围绕主线龙头与补涨梯队分层开仓；进入衰退阶段时切回减仓与观察池。",
    },
    "sentiment_catalyst": {
        "label": "情绪催化",
        "stage": "catalyst",
        "role": "短线催化器",
        "monetization_path": "事件触发单与观察池晋级；只有在主线或资金共振时才放大仓位，单独使用以试单为主。",
    },
    "event_continuation": {
        "label": "事件延续",
        "stage": "catalyst",
        "role": "防守/overlay",
        "monetization_path": "负向 continuation 优先货币化为减仓、回避、新仓过滤与对冲篮子；正向只做精选 source-event probe long。",
    },
    "flow_expectation": {
        "label": "资金/预期差",
        "stage": "flow_expectation",
        "role": "轮动副引擎",
        "monetization_path": "机构共振与预期差补涨轮动；顺主线做 catch-up，不逆着主线硬做左侧抄底。",
    },
    "graph_filter": {
        "label": "图谱过滤",
        "stage": "graph_sentiment_filter",
        "role": "过滤与加权器",
        "monetization_path": "只用于候选筛选、仓位加权和补涨线索发掘，不单独裸开仓。",
    },
}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _signed_direction(direction: str) -> float:
    if direction == "long":
        return 1.0
    if direction in {"avoid", "short"}:
        return -1.0
    return 0.0


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def _annualize_simple(avg_return: float, holding_days: float) -> float:
    return round(avg_return * 252.0 / max(float(holding_days or 0.0), 1.0), 4)


def _annualize_compound(avg_return: float, holding_days: float) -> float:
    periods = 252.0 / max(float(holding_days or 0.0), 1.0)
    try:
        return round((1.0 + avg_return) ** periods - 1.0, 4)
    except Exception:
        return 0.0


def _fmt_num(value: Any, digits: int = 2) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return "N/A"


def _fmt_pct(value: Any, digits: int = 2) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):+.{digits}%}"
    except Exception:
        return "N/A"


def _fmt_hit(value: Any, digits: int = 1) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{float(value):.{digits}%}"
    except Exception:
        return "N/A"


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def _data_root() -> Path:
    return _repo_root() / ".data"


def _signals_root() -> Path:
    return _data_root() / "signals"


def _reports_root() -> Path:
    return _data_root() / "reports"


def _latest_json(path: Path, pattern: str) -> Optional[Path]:
    files = sorted(path.glob(pattern))
    return files[-1] if files else None


def _iso_date(value: str | None) -> Optional[date]:
    text = str(value or "").strip()
    if not text:
        return None
    text = text[:10]
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except Exception:
        try:
            return datetime.strptime(text, "%Y%m%d").date()
        except Exception:
            return None


def _normalize_strategy_signal(raw: Dict[str, Any]) -> Dict[str, Any]:
    metadata = raw.get("metadata") or {}
    stock_code = str(raw.get("stock_code", "")).strip()
    if stock_code.isdigit() and len(stock_code) < 6:
        stock_code = stock_code.zfill(6)
    return {
        "strategy_name": str(raw.get("strategy_name", "")).strip(),
        "stock_code": stock_code,
        "stock_name": str(raw.get("stock_name", "")).strip() or stock_code,
        "direction": str(raw.get("direction", "neutral")).strip() or "neutral",
        "confidence": _safe_float(raw.get("confidence", 0.0)),
        "expected_return": _safe_float(raw.get("expected_return", 0.0)),
        "holding_period_days": _safe_int(raw.get("holding_period_days", 0)),
        "reasoning": str(raw.get("reasoning", "")).strip(),
        "metadata": metadata if isinstance(metadata, dict) else {},
        "raw": raw,
    }


def _normalize_shock_signal(raw: Dict[str, Any]) -> Dict[str, Any]:
    target_code = str(raw.get("target_code", "")).strip()
    if target_code.isdigit() and len(target_code) < 6:
        target_code = target_code.zfill(6)
    return {
        "strategy_name": "shock_pipeline",
        "stock_code": target_code,
        "stock_name": str(raw.get("target_name", "")).strip() or target_code,
        "direction": str(raw.get("signal_direction", "neutral")).strip() or "neutral",
        "confidence": _safe_float(raw.get("confidence", 0.0)),
        "expected_return": _safe_float(raw.get("expected_return", 0.0)),
        "holding_period_days": _safe_int(raw.get("expected_holding_days", 5), 5),
        "reasoning": str(raw.get("reasoning", "")).strip(),
        "metadata": {
            "event_type": str(raw.get("event_type", "")).strip(),
            "source_code": str(raw.get("source_code", "")).strip(),
            "source_name": str(raw.get("source_name", "")).strip(),
            "hop": _safe_int(raw.get("hop", 0)),
            "relation_chain": str(raw.get("relation_chain", "")).strip(),
            "shock_weight": _safe_float(raw.get("shock_weight", 0.0)),
            "reacted": bool(raw.get("reacted", False)),
            "source_event": str(raw.get("source_event", "")).strip(),
        },
        "raw": raw,
    }


def _event_prior_strength(metrics: Dict[str, Any]) -> float:
    sharpe = _safe_float(metrics.get("sharpe", 0.0))
    avg_adj = _safe_float(metrics.get("avg_adj_return", 0.0))
    score = 0.12 * sharpe + 7.5 * avg_adj
    return _clamp(score, -0.60, 0.60)


@dataclass
class CandidateProfile:
    stock_code: str
    stock_name: str
    industry: str = ""
    regime_score: float = 0.0
    mainline_score: float = 0.0
    catalyst_score: float = 0.0
    flow_score: float = 0.0
    filter_quality: float = 0.5
    catalyst_event_types: set[str] = field(default_factory=set)
    reasons: Dict[str, List[str]] = field(
        default_factory=lambda: defaultdict(list)  # type: ignore[arg-type]
    )

    def add_reason(self, stage: str, message: str) -> None:
        self.reasons[stage].append(message)

    @property
    def base_score(self) -> float:
        return (
            self.regime_score * _CANDIDATE_STAGE_WEIGHTS["regime"]
            + self.mainline_score * _CANDIDATE_STAGE_WEIGHTS["mainline"]
            + self.catalyst_score * _CANDIDATE_STAGE_WEIGHTS["catalyst"]
            + self.flow_score * _CANDIDATE_STAGE_WEIGHTS["flow"]
        )

    @property
    def final_score(self) -> float:
        multiplier = 0.80 + 0.40 * self.filter_quality
        return self.base_score * multiplier

    @property
    def conviction(self) -> float:
        return abs(self.final_score)


class TradingDeskFramework:
    """Local-first top-level trading desk orchestrator."""

    def __init__(self, as_of_date: str | None = None) -> None:
        self.repo_root = _repo_root()
        self.data_root = _data_root()
        self.signals_root = _signals_root()
        self.reports_root = _reports_root()
        self.as_of_date = _iso_date(as_of_date) or date.today()

        self.security_master = self._load_security_master()
        self.strategy_inputs = self._load_all_strategy_inputs()
        self.shock_inputs = self._load_latest_shock_inputs()
        self.bucket_priors = self._load_bucket_priors()
        self.recent_events = self._load_recent_events()
        self.authoritative_audit = self._load_authoritative_audit()

        self.candidates: Dict[str, CandidateProfile] = {}
        self._seed_candidates()

    def _load_security_master(self) -> Dict[str, Dict[str, Any]]:
        path = self.data_root / "datahub" / "universe" / "security_master.json"
        if not path.exists():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
        mapping: Dict[str, Dict[str, Any]] = {}
        for row in payload:
            code = str(row.get("ticker", "")).strip().zfill(6)
            if not code:
                continue
            mapping[code] = {
                "stock_name": str(row.get("company_name", "")).strip() or code,
                "industry": str(row.get("industry_l1", "")).strip(),
                "concept_tags": list(row.get("concept_tags", []) or []),
            }
        return mapping

    def _load_signal_snapshot(self, strategy_name: str) -> Dict[str, Any]:
        path = _latest_json(self.signals_root / strategy_name, "*.json")
        if path is None:
            return {"strategy_name": strategy_name, "path": "", "signals": []}
        payload = json.loads(path.read_text(encoding="utf-8"))
        signals = [_normalize_strategy_signal(item) for item in payload if isinstance(item, dict)]
        return {"strategy_name": strategy_name, "path": str(path), "signals": signals}

    def _load_all_strategy_inputs(self) -> Dict[str, Dict[str, Any]]:
        names = [
            "sector_rotation",
            "narrative_tracker",
            "institution_association",
            "graph_factors",
            "sentiment_simulation",
            "analyst_divergence",
            "earnings_surprise",
        ]
        return {name: self._load_signal_snapshot(name) for name in names}

    def _load_latest_shock_inputs(self) -> Dict[str, Any]:
        path = _latest_json(self.reports_root, "shock_signals_*.json")
        if path is None:
            return {"path": "", "signals": []}
        payload = json.loads(path.read_text(encoding="utf-8"))
        signals = [_normalize_shock_signal(item) for item in payload if isinstance(item, dict)]
        return {"path": str(path), "signals": signals}

    def _load_bucket_priors(self) -> Dict[str, Dict[str, Any]]:
        path = _latest_json(self.reports_root, "shock_bucket_study_*local_refresh.json")
        if path is None:
            path = _latest_json(self.reports_root, "shock_bucket_study_*.json")
        if path is None:
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
        priors: Dict[str, Dict[str, Any]] = {}
        for event_type, section in payload.get("type_sections", {}).items():
            slices = section.get("slices", {})
            all_metrics = slices.get("全量", {})
            priors[event_type] = {
                **all_metrics,
                "edge_strength": _event_prior_strength(all_metrics),
            }
        return priors

    def _load_recent_events(self, lookback_days: int = 7) -> Dict[str, List[Dict[str, Any]]]:
        path = self.data_root / "datahub" / "ingest" / "events" / "event_master.json"
        if not path.exists():
            return {}
        payload = json.loads(path.read_text(encoding="utf-8"))
        latest_dt: Optional[date] = None
        for item in payload:
            dt = _iso_date(item.get("event_time") or item.get("discover_time"))
            if dt and (latest_dt is None or dt > latest_dt):
                latest_dt = dt
        latest_dt = latest_dt or self.as_of_date
        start_dt = latest_dt - timedelta(days=lookback_days - 1)

        recent_by_code: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in payload:
            dt = _iso_date(item.get("event_time") or item.get("discover_time"))
            if dt is None or dt < start_dt or dt > latest_dt:
                continue
            event_type = str(item.get("event_type", "")).strip()
            title = str(item.get("title", "")).strip()
            summary = str(item.get("summary", "")).strip()
            for code in item.get("entity_codes", []) or []:
                norm_code = str(code).strip().zfill(6)
                if not norm_code:
                    continue
                recent_by_code[norm_code].append(
                    {
                        "event_type": event_type,
                        "title": title,
                        "summary": summary,
                        "event_date": dt.isoformat(),
                    }
                )
        return dict(recent_by_code)

    def _load_authoritative_audit(self) -> Dict[str, Any]:
        path = self.data_root / "authoritative_archive" / "audit" / "archive_audit.json"
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else {}
        except Exception:
            return {}

    def _seed_candidates(self) -> None:
        for snapshot in self.strategy_inputs.values():
            for signal in snapshot.get("signals", []):
                self._ensure_candidate(signal["stock_code"], signal["stock_name"])
        for signal in self.shock_inputs.get("signals", []):
            self._ensure_candidate(signal["stock_code"], signal["stock_name"])
        for code in self.recent_events.keys():
            self._ensure_candidate(code)

    def _ensure_candidate(self, stock_code: str, stock_name: str | None = None) -> CandidateProfile:
        code = str(stock_code).strip().zfill(6)
        if code not in self.candidates:
            sec = self.security_master.get(code, {})
            name = stock_name or sec.get("stock_name") or code
            industry = sec.get("industry", "")
            self.candidates[code] = CandidateProfile(
                stock_code=code,
                stock_name=str(name).strip() or code,
                industry=str(industry).strip(),
            )
        return self.candidates[code]

    def _best_directional_snapshot(self, strategy_name: str) -> Optional[Dict[str, Any]]:
        signal_dir = self.signals_root / strategy_name
        if not signal_dir.exists():
            return None

        best: Optional[Dict[str, Any]] = None
        for path in sorted(signal_dir.glob("*.json")):
            if path.name.startswith("_"):
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, list):
                continue
            directional = [
                item for item in payload
                if isinstance(item, dict) and str(item.get("direction", "")).strip() in {"long", "avoid"}
            ]
            if directional:
                best = {"path": str(path), "directional_payload": directional}
        return best

    def _load_strategy_signal_objects(self, strategy_name: str) -> Dict[str, Any]:
        snapshot = self._best_directional_snapshot(strategy_name)
        if snapshot is None:
            return {"path": "", "signals": []}

        from astrategy.strategies.base import StrategySignal

        signals: List[StrategySignal] = []
        for row in snapshot["directional_payload"]:
            try:
                signals.append(StrategySignal.from_dict(row))
            except Exception:
                continue
        return {"path": snapshot["path"], "signals": signals}

    def _line_status(self, sharpe: float, hit_rate: float, sample_size: int, role: str) -> str:
        if role == "过滤与加权器":
            return "作为过滤器使用"
        if sample_size < 5:
            return "样本过少，暂不放大"
        if sharpe >= 1.0 and hit_rate >= 0.55:
            return "可放大"
        if sharpe > 0:
            return "观察推进"
        return "暂缓/只保留过滤作用"

    def _rolling_proxy_metrics(
        self,
        strategy_names: List[str],
        line_id: str,
    ) -> Dict[str, Any]:
        from astrategy.run_backtest import RollingEvaluator

        blueprint = _LINE_BLUEPRINTS[line_id]
        windows = [20, 40, 60]
        rolling = RollingEvaluator(windows=windows)

        paths: List[str] = []
        signals = []
        per_strategy_counts: Dict[str, int] = {}
        for strategy_name in strategy_names:
            loaded = self._load_strategy_signal_objects(strategy_name)
            if loaded["signals"]:
                paths.append(loaded["path"])
                per_strategy_counts[strategy_name] = len(loaded["signals"])
                signals.extend(loaded["signals"])

        if not signals:
            return {
                "line_id": line_id,
                "label": blueprint["label"],
                "stage": blueprint["stage"],
                "validation_mode": "unavailable",
                "role": blueprint["role"],
                "monetization_path": blueprint["monetization_path"],
                "signal_sources": paths,
                "component_counts": per_strategy_counts,
                "signal_count": 0,
                "oos_evals": 0,
                "holding_days": 0.0,
                "wf_sharpe": 0.0,
                "is_sharpe": 0.0,
                "all_sharpe": 0.0,
                "hit_rate": 0.0,
                "avg_return": 0.0,
                "annualized_proxy_simple": 0.0,
                "annualized_proxy_compound": 0.0,
                "status": "暂无方向性样本",
                "notes": "当前落盘快照没有可验证的 long/avoid 信号。",
            }

        eval_df = rolling.evaluate_batch_rolling(signals)
        if eval_df.empty:
            return {
                "line_id": line_id,
                "label": blueprint["label"],
                "stage": blueprint["stage"],
                "validation_mode": "unavailable",
                "role": blueprint["role"],
                "monetization_path": blueprint["monetization_path"],
                "signal_sources": paths,
                "component_counts": per_strategy_counts,
                "signal_count": len(signals),
                "oos_evals": 0,
                "holding_days": round(statistics.mean(s.holding_period_days for s in signals), 2),
                "wf_sharpe": 0.0,
                "is_sharpe": 0.0,
                "all_sharpe": 0.0,
                "hit_rate": 0.0,
                "avg_return": 0.0,
                "annualized_proxy_simple": 0.0,
                "annualized_proxy_compound": 0.0,
                "status": "暂无可评估窗口",
                "notes": "滚动窗口没有形成有效价格评估样本。",
            }

        recent_window = min(windows)
        is_df = eval_df[eval_df["lookback_days"] != recent_window]
        oos_df = eval_df[eval_df["lookback_days"] == recent_window]

        is_metrics = RollingEvaluator.compute_rolling_metrics(is_df)
        oos_metrics = RollingEvaluator.compute_rolling_metrics(oos_df)
        all_metrics = RollingEvaluator.compute_rolling_metrics(eval_df)
        holding_days = statistics.mean(s.holding_period_days for s in signals)

        annual_simple = _annualize_simple(oos_metrics.get("avg_return", 0.0), holding_days)
        annual_compound = _annualize_compound(oos_metrics.get("avg_return", 0.0), holding_days)
        notes = []
        if oos_metrics.get("total_evals", 0) < 10:
            notes.append("OOS 样本较少，当前 Sharpe 主要用于方向排序。")
        if is_metrics.get("sharpe_ratio", 0.0) * oos_metrics.get("sharpe_ratio", 0.0) < 0:
            notes.append("IS/OOS 符号反转，说明这条线还不稳定。")

        return {
            "line_id": line_id,
            "label": blueprint["label"],
            "stage": blueprint["stage"],
            "validation_mode": "rolling_proxy",
            "role": blueprint["role"],
            "monetization_path": blueprint["monetization_path"],
            "signal_sources": paths,
            "component_counts": per_strategy_counts,
            "signal_count": len(signals),
            "oos_evals": int(oos_metrics.get("total_evals", 0)),
            "holding_days": round(holding_days, 2),
            "wf_sharpe": round(_safe_float(oos_metrics.get("sharpe_ratio", 0.0)), 4),
            "is_sharpe": round(_safe_float(is_metrics.get("sharpe_ratio", 0.0)), 4),
            "all_sharpe": round(_safe_float(all_metrics.get("sharpe_ratio", 0.0)), 4),
            "hit_rate": round(_safe_float(oos_metrics.get("hit_rate", 0.0)), 4),
            "avg_return": round(_safe_float(oos_metrics.get("avg_return", 0.0)), 4),
            "annualized_proxy_simple": annual_simple,
            "annualized_proxy_compound": annual_compound,
            "status": self._line_status(
                _safe_float(oos_metrics.get("sharpe_ratio", 0.0)),
                _safe_float(oos_metrics.get("hit_rate", 0.0)),
                _safe_int(oos_metrics.get("total_evals", 0)),
                blueprint["role"],
            ),
            "notes": " ".join(notes) if notes else "基于当前落盘信号的滚动 OOS 代理验证。",
        }

    def _latest_shock_artifact(self, pattern: str) -> Optional[Path]:
        preferred = sorted(self.reports_root.glob(pattern.replace("*", "*local_refresh*")))
        if preferred:
            return preferred[-1]
        candidates = sorted(self.reports_root.glob(pattern))
        return candidates[-1] if candidates else None

    def _shock_event_counts(self, report_path: Optional[Path]) -> tuple[int, int]:
        if report_path and report_path.exists():
            text = report_path.read_text(encoding="utf-8")
            match = re.search(r"\*\*事件数\*\*: IS=(\d+), OOS=(\d+)", text)
            if match:
                return int(match.group(1)), int(match.group(2))
        return 1213, 521

    def _strict_shock_metrics(self) -> Dict[str, Any]:
        blueprint = _LINE_BLUEPRINTS["event_continuation"]
        signals_path = self._latest_shock_artifact("shock_wf_signals_*.json")
        report_path = self._latest_shock_artifact("shock_wf_backtest_*.md")
        if signals_path is None:
            return {
                "line_id": "event_continuation",
                "label": blueprint["label"],
                "stage": blueprint["stage"],
                "validation_mode": "unavailable",
                "role": blueprint["role"],
                "monetization_path": blueprint["monetization_path"],
                "signal_sources": [],
                "component_counts": {},
                "signal_count": 0,
                "oos_evals": 0,
                "holding_days": 5.0,
                "wf_sharpe": 0.0,
                "is_sharpe": 0.0,
                "all_sharpe": 0.0,
                "hit_rate": 0.0,
                "avg_return": 0.0,
                "annualized_proxy_simple": 0.0,
                "annualized_proxy_compound": 0.0,
                "status": "缺少 shock WF 产物",
                "notes": "未发现可用的 shock WF 信号文件。",
            }

        from astrategy.events.normalizer import normalize_events
        from astrategy.run_shock_wf_backtest import compute_metrics_bundle

        signals = json.loads(signals_path.read_text(encoding="utf-8"))
        is_events, oos_events = self._shock_event_counts(report_path)
        total_events = is_events + oos_events

        event_path = self.data_root / "datahub" / "ingest" / "events" / "event_master.json"
        events = normalize_events(json.loads(event_path.read_text(encoding="utf-8"))) if event_path.exists() else []
        events = [
            event for event in events
            if event.get("stock_code") and event.get("event_date")
        ]
        events = sorted(events, key=lambda event: (event.get("event_date", ""), event.get("event_id", "")))
        events = events[:total_events]
        first_event_date = events[0].get("event_date", "") if events else ""
        last_event_date = events[-1].get("event_date", "") if events else ""
        history_span_days = 0
        if first_event_date and last_event_date:
            try:
                history_span_days = int(
                    (
                        datetime.strptime(last_event_date[:10], "%Y-%m-%d")
                        - datetime.strptime(first_event_date[:10], "%Y-%m-%d")
                    ).days
                )
            except Exception:
                history_span_days = 0
        is_ids = {event.get("event_id") for event in events[:is_events]}
        oos_ids = {event.get("event_id") for event in events[is_events:total_events]}

        is_signals = [signal for signal in signals if signal.get("event_id") in is_ids]
        oos_signals = [signal for signal in signals if signal.get("event_id") in oos_ids]

        is_metrics = compute_metrics_bundle(is_signals, "fwd_return_5d", 5)
        oos_metrics = compute_metrics_bundle(oos_signals, "fwd_return_5d", 5)
        all_metrics = compute_metrics_bundle(signals, "fwd_return_5d", 5)

        annual_simple = _annualize_simple(oos_metrics.get("avg_adj_return", 0.0), 5.0)
        annual_compound = _annualize_compound(oos_metrics.get("avg_adj_return", 0.0), 5.0)
        notes = []
        if history_span_days < _MIN_BACKTEST_SPAN_DAYS:
            notes.append(
                f"历史跨度只有 {history_span_days} 天，未达到两年门槛 ({_MIN_BACKTEST_SPAN_DAYS} 天)。"
            )
        if oos_metrics.get("n", 0) < 50:
            notes.append("OOS 有效样本仍偏少，但当前是仓库内最严格的事件线验证。")
        if oos_metrics.get("avg_adj_return", 0.0) > 0 and all_metrics.get("avg_raw_return", 0.0) < 0:
            notes.append("当前 alpha 主要来自方向调整后的 continuation，不是裸多头收益。")

        status = self._line_status(
            _safe_float(oos_metrics.get("sharpe", 0.0)),
            _safe_float(oos_metrics.get("hit_rate", 0.0)),
            _safe_int(oos_metrics.get("n", 0)),
            blueprint["role"],
        )
        if history_span_days < _MIN_BACKTEST_SPAN_DAYS:
            status = "历史不足2年，不出最终结论"

        return {
            "line_id": "event_continuation",
            "label": blueprint["label"],
            "stage": blueprint["stage"],
            "validation_mode": "strict_wf",
            "role": blueprint["role"],
            "monetization_path": blueprint["monetization_path"],
            "signal_sources": [str(signals_path), str(report_path) if report_path else ""],
            "component_counts": {"shock_signals": len(signals)},
            "signal_count": len(signals),
            "oos_evals": int(oos_metrics.get("n", 0)),
            "holding_days": 5.0,
            "history_span_days": history_span_days,
            "min_backtest_span_days": _MIN_BACKTEST_SPAN_DAYS,
            "wf_sharpe": round(_safe_float(oos_metrics.get("sharpe", 0.0)), 4),
            "is_sharpe": round(_safe_float(is_metrics.get("sharpe", 0.0)), 4),
            "all_sharpe": round(_safe_float(all_metrics.get("sharpe", 0.0)), 4),
            "hit_rate": round(_safe_float(oos_metrics.get("hit_rate", 0.0)), 4),
            "avg_return": round(_safe_float(oos_metrics.get("avg_adj_return", 0.0)), 4),
            "annualized_proxy_simple": annual_simple,
            "annualized_proxy_compound": annual_compound,
            "status": status,
            "notes": " ".join(notes) if notes else "基于 shock WF 严格 OOS 指标。",
        }

    def _authoritative_gap_metrics(
        self,
        strategy_names: List[str],
        line_id: str,
    ) -> Dict[str, Any]:
        blueprint = _LINE_BLUEPRINTS[line_id]
        audit_lines = self.authoritative_audit.get("lines", {}) if isinstance(self.authoritative_audit, dict) else {}
        audit_strategies = self.authoritative_audit.get("strategies", {}) if isinstance(self.authoritative_audit, dict) else {}
        line_audit = audit_lines.get(line_id) if isinstance(audit_lines, dict) else None
        if isinstance(line_audit, dict):
            component_notes = []
            total_signal_count = 0
            signal_sources: List[str] = []
            component_counts: Dict[str, int] = {}
            holding_days: List[int] = []
            for strategy_name in line_audit.get("strategies", []):
                strategy_audit = audit_strategies.get(strategy_name, {})
                if not isinstance(strategy_audit, dict):
                    continue
                component_counts[f"{strategy_name}_signal_dates"] = _safe_int(strategy_audit.get("signal_date_count", 0))
                component_counts[f"{strategy_name}_factor_dates"] = _safe_int(strategy_audit.get("factor_date_count", 0))
                total_signal_count += _safe_int(strategy_audit.get("total_signal_count", 0))
                note = (
                    f"{strategy_name}: signal_dates={strategy_audit.get('signal_date_count', 0)}, "
                    f"factor_dates={strategy_audit.get('factor_date_count', 0)}, "
                    f"strict_ready={strategy_audit.get('strict_wf_ready', False)}"
                )
                component_notes.append(note)
                strategy_dir = self.data_root / "authoritative_archive" / "signals" / strategy_name
                if strategy_dir.exists():
                    signal_sources.append(str(strategy_dir))
            return {
                "line_id": line_id,
                "label": blueprint["label"],
                "stage": blueprint["stage"],
                "validation_mode": "authoritative_missing",
                "role": blueprint["role"],
                "monetization_path": blueprint["monetization_path"],
                "signal_sources": signal_sources,
                "component_counts": component_counts,
                "signal_count": total_signal_count,
                "oos_evals": 0,
                "holding_days": holding_days,
                "wf_sharpe": None,
                "is_sharpe": None,
                "all_sharpe": None,
                "hit_rate": None,
                "avg_return": None,
                "annualized_proxy_simple": None,
                "annualized_proxy_compound": None,
                "status": "缺历史，不出结论",
                "notes": " | ".join(component_notes) if component_notes else "archive audit 显示缺少 strict WF 所需历史。",
            }

        signal_paths: List[str] = []
        directional_dates: set[str] = set()
        holding_days: set[int] = set()
        directional_signal_count = 0
        factor_dates: set[str] = set()
        component_counts: Dict[str, int] = {}

        for strategy_name in strategy_names:
            signal_dir = self.signals_root / strategy_name
            if signal_dir.exists():
                files = sorted(signal_dir.glob("*.json"))
                component_counts[f"{strategy_name}_signal_files"] = len([p for p in files if not p.name.startswith("_")])
                for path in files:
                    if path.name.startswith("_"):
                        continue
                    signal_paths.append(str(path))
                    try:
                        payload = json.loads(path.read_text(encoding="utf-8"))
                    except Exception:
                        continue
                    if not isinstance(payload, list):
                        continue
                    for row in payload:
                        if not isinstance(row, dict):
                            continue
                        direction = str(row.get("direction", "")).strip()
                        if direction not in {"long", "avoid"}:
                            continue
                        directional_signal_count += 1
                        signal_date = str(row.get("timestamp") or row.get("signal_date") or "")[:10]
                        if signal_date:
                            directional_dates.add(signal_date)
                        if row.get("holding_period_days") is not None:
                            holding_days.add(_safe_int(row.get("holding_period_days"), 0))

            factor_root = self.data_root / "factors"
            matched_dirs = []
            if factor_root.exists():
                matched_dirs = [
                    path for path in factor_root.iterdir()
                    if path.is_dir() and path.name.endswith(strategy_name)
                ]
            dates = {file_path.stem for factor_dir in matched_dirs for file_path in factor_dir.glob("*.parquet")}
            if dates:
                factor_dates.update(dates)
            component_counts[f"{strategy_name}_factor_dates"] = len(dates)

        notes = []
        notes.append(f"方向性信号日期只有 {len(directional_dates)} 个。")
        if holding_days:
            notes.append(f"持有期需求 {sorted(day for day in holding_days if day > 0)} 天。")
        if factor_dates:
            notes.append(f"authoritative 因子日期 {len(factor_dates)} 个。")
        else:
            notes.append("缺少可用于严格 WF 的 authoritative 因子时间序列。")

        return {
            "line_id": line_id,
            "label": blueprint["label"],
            "stage": blueprint["stage"],
            "validation_mode": "authoritative_missing",
            "role": blueprint["role"],
            "monetization_path": blueprint["monetization_path"],
            "signal_sources": signal_paths,
            "component_counts": component_counts,
            "signal_count": directional_signal_count,
            "oos_evals": 0,
            "holding_days": sorted(day for day in holding_days if day > 0),
            "wf_sharpe": None,
            "is_sharpe": None,
            "all_sharpe": None,
            "hit_rate": None,
            "avg_return": None,
            "annualized_proxy_simple": None,
            "annualized_proxy_compound": None,
            "status": "缺历史，不出结论",
            "notes": " ".join(notes),
        }

    def build_validation_stage(self) -> Dict[str, Any]:
        rows = [
            self._authoritative_gap_metrics(["sector_rotation"], "regime_rotation"),
            self._authoritative_gap_metrics(["narrative_tracker"], "mainline_narrative"),
            self._authoritative_gap_metrics(["sentiment_simulation"], "sentiment_catalyst"),
            self._strict_shock_metrics(),
            self._authoritative_gap_metrics(
                ["institution_association", "analyst_divergence", "earnings_surprise"],
                "flow_expectation",
            ),
            self._authoritative_gap_metrics(["graph_factors"], "graph_filter"),
        ]

        ranked = sorted(
            rows,
            key=lambda item: (_safe_float(item.get("wf_sharpe"), -999.0), item["label"]),
            reverse=True,
        )
        route_focus = [
            item["label"]
            for item in ranked
            if (
                item.get("validation_mode") == "strict_wf"
                and _safe_float(item.get("wf_sharpe"), 0.0) > 0
                and _safe_int(item.get("history_span_days", _MIN_BACKTEST_SPAN_DAYS), _MIN_BACKTEST_SPAN_DAYS)
                >= _MIN_BACKTEST_SPAN_DAYS
            )
        ][:3]
        deferred = [
            item["label"]
            for item in ranked
            if item.get("validation_mode") != "strict_wf"
        ][:5]
        return {
            "sub_lines": ranked,
            "route_focus": route_focus,
            "deferred_lines": deferred,
        }

    def build_regime_stage(self) -> Dict[str, Any]:
        signals = self.strategy_inputs["sector_rotation"]["signals"]
        macro_counter = Counter()
        industry_scores: Dict[str, float] = defaultdict(float)
        recommended = Counter()
        avoided = Counter()
        long_count = 0
        avoid_count = 0

        for signal in signals:
            profile = self._ensure_candidate(signal["stock_code"], signal["stock_name"])
            signed = _signed_direction(signal["direction"]) * signal["confidence"]
            profile.regime_score += signed * 0.90
            meta = signal["metadata"]
            industry = str(meta.get("industry", "")).strip() or profile.industry
            if industry:
                industry_scores[industry] += signed
                profile.add_reason("regime", f"{industry} 轮动信号 {signal['direction']} {signal['confidence']:.2f}")
            phase = str(meta.get("macro_phase", "unknown")).strip() or "unknown"
            macro_counter[phase] += 1
            for name in meta.get("recommended_industries", []) or []:
                recommended[str(name).strip()] += 1
            for name in meta.get("avoid_industries", []) or []:
                avoided[str(name).strip()] += 1
            if signal["direction"] == "long":
                long_count += 1
            elif signal["direction"] == "avoid":
                avoid_count += 1

        if macro_counter.get("contraction", 0) >= max(macro_counter.values() or [0]):
            risk_mode = "defensive_barbell"
        elif long_count > avoid_count:
            risk_mode = "offensive_rotation"
        elif avoid_count > long_count:
            risk_mode = "risk_reduction"
        else:
            risk_mode = "balanced"

        favored = [
            {"name": name, "score": round(score, 4)}
            for name, score in sorted(industry_scores.items(), key=lambda item: item[1], reverse=True)
            if score > 0
        ][:5]
        risky = [
            {"name": name, "score": round(score, 4)}
            for name, score in sorted(industry_scores.items(), key=lambda item: item[1])
            if score < 0
        ][:5]

        return {
            "source_path": self.strategy_inputs["sector_rotation"]["path"],
            "risk_mode": risk_mode,
            "macro_phases": dict(macro_counter),
            "favored_industries": favored,
            "avoid_industries": risky,
            "recommended_industries": [{"name": k, "count": v} for k, v in recommended.most_common(5)],
            "avoided_industries": [{"name": k, "count": v} for k, v in avoided.most_common(5)],
        }

    def build_mainline_stage(self) -> Dict[str, Any]:
        signals = self.strategy_inputs["narrative_tracker"]["signals"]
        theme_scores: Dict[str, float] = defaultdict(float)
        phase_counter = Counter()

        for signal in signals:
            profile = self._ensure_candidate(signal["stock_code"], signal["stock_name"])
            meta = signal["metadata"]
            theme = str(meta.get("narrative_name", "")).strip() or "unknown"
            phase = str(meta.get("narrative_phase", "")).strip() or "unknown"
            phase_counter[phase] += 1
            multiplier = _PHASE_MULTIPLIER.get(phase, 0.4)
            signed = _signed_direction(signal["direction"]) * signal["confidence"] * multiplier
            profile.mainline_score += signed
            profile.add_reason("mainline", f"{theme} {phase} {signal['direction']} {signal['confidence']:.2f}")
            theme_scores[theme] += signed

        top_themes = [
            {"name": name, "score": round(score, 4)}
            for name, score in sorted(theme_scores.items(), key=lambda item: item[1], reverse=True)
            if score > 0
        ][:6]
        weak_themes = [
            {"name": name, "score": round(score, 4)}
            for name, score in sorted(theme_scores.items(), key=lambda item: item[1])
            if score < 0
        ][:6]

        return {
            "source_path": self.strategy_inputs["narrative_tracker"]["path"],
            "phases": dict(phase_counter),
            "top_themes": top_themes,
            "weak_themes": weak_themes,
        }

    def build_catalyst_stage(self) -> Dict[str, Any]:
        catalyst_rows: List[Dict[str, Any]] = []

        for signal in self.strategy_inputs["sentiment_simulation"]["signals"]:
            profile = self._ensure_candidate(signal["stock_code"], signal["stock_name"])
            meta = signal["metadata"]
            event_type = str(meta.get("event_type", "")).strip()
            prior = self.bucket_priors.get(event_type, {})
            prior_strength = _safe_float(prior.get("edge_strength", 0.0))
            signed = _signed_direction(signal["direction"]) * signal["confidence"]
            strength = signed * (1.0 + max(prior_strength, 0.0) * 0.5)
            profile.catalyst_score += strength
            if event_type:
                profile.catalyst_event_types.add(event_type)
            title = str(meta.get("event_title", "")).strip() or signal["reasoning"][:48]
            profile.add_reason(
                "catalyst",
                f"S10 {event_type or 'event'} {signal['direction']} {signal['confidence']:.2f}: {title}",
            )
            catalyst_rows.append(
                {
                    "stock_code": profile.stock_code,
                    "stock_name": profile.stock_name,
                    "event_type": event_type or "unknown",
                    "direction": signal["direction"],
                    "confidence": round(signal["confidence"], 4),
                    "prior_strength": round(prior_strength, 4),
                    "title": title,
                }
            )

        for signal in self.shock_inputs["signals"]:
            profile = self._ensure_candidate(signal["stock_code"], signal["stock_name"])
            meta = signal["metadata"]
            event_type = str(meta.get("event_type", "")).strip()
            prior = self.bucket_priors.get(event_type, {})
            prior_strength = _safe_float(prior.get("edge_strength", 0.0))
            hop = _safe_int(meta.get("hop", 0))
            hop_mult = {0: 1.10, 1: 0.95, 2: 0.80, 3: 0.65}.get(hop, 0.70)
            signed = _signed_direction(signal["direction"]) * signal["confidence"] * hop_mult
            strength = signed * (1.0 + max(prior_strength, 0.0) * 0.6)
            profile.catalyst_score += strength
            if event_type:
                profile.catalyst_event_types.add(event_type)
            profile.add_reason(
                "catalyst",
                f"Shock {event_type or 'event'} hop={hop} {signal['direction']} {signal['confidence']:.2f}",
            )
            catalyst_rows.append(
                {
                    "stock_code": profile.stock_code,
                    "stock_name": profile.stock_name,
                    "event_type": event_type or "unknown",
                    "direction": signal["direction"],
                    "confidence": round(signal["confidence"], 4),
                    "prior_strength": round(prior_strength, 4),
                    "title": str(meta.get("source_event", "")).strip()[:60],
                }
            )

        hot_recent = []
        for code, events in self.recent_events.items():
            profile = self._ensure_candidate(code)
            top_types = Counter(event["event_type"] for event in events)
            hot_recent.append(
                {
                    "stock_code": code,
                    "stock_name": profile.stock_name,
                    "recent_events": len(events),
                    "top_event_type": top_types.most_common(1)[0][0] if top_types else "",
                }
            )
            if len(events) >= 2:
                profile.add_reason("catalyst", f"最近 {len(events)} 条事件进入观察池")

        catalyst_rows = sorted(
            catalyst_rows,
            key=lambda item: (abs(item["confidence"] * (1 + item["prior_strength"])), item["stock_code"]),
            reverse=True,
        )
        return {
            "source_paths": [
                self.strategy_inputs["sentiment_simulation"]["path"],
                self.shock_inputs["path"],
            ],
            "top_catalysts": catalyst_rows[:10],
            "recent_event_hotspots": sorted(hot_recent, key=lambda item: item["recent_events"], reverse=True)[:10],
        }

    def build_flow_stage(self) -> Dict[str, Any]:
        rows: List[Dict[str, Any]] = []

        for signal in self.strategy_inputs["institution_association"]["signals"]:
            profile = self._ensure_candidate(signal["stock_code"], signal["stock_name"])
            meta = signal["metadata"]
            gap = _safe_float(meta.get("catch_up_gap", 0.0))
            bonus = 1.0 + min(max(gap, 0.0), 0.12) / 0.12 * 0.2
            signed = _signed_direction(signal["direction"]) * signal["confidence"] * bonus
            profile.flow_score += signed
            profile.add_reason(
                "flow",
                f"机构共振 {signal['direction']} {signal['confidence']:.2f}, catch-up gap={gap:.2%}",
            )
            rows.append(
                {
                    "stock_code": profile.stock_code,
                    "stock_name": profile.stock_name,
                    "source": "institution_association",
                    "direction": signal["direction"],
                    "score": round(signed, 4),
                    "detail": f"gap={gap:.2%}, holders={meta.get('institution_count', 0)}",
                }
            )

        for signal in self.strategy_inputs["analyst_divergence"]["signals"]:
            profile = self._ensure_candidate(signal["stock_code"], signal["stock_name"])
            meta = signal["metadata"]
            divergence = _safe_float(meta.get("divergence_score", 0.0))
            if signal["direction"] != "neutral":
                signed = _signed_direction(signal["direction"]) * signal["confidence"] * 0.7
                profile.flow_score += signed
                profile.add_reason(
                    "flow",
                    f"分析师方向 {signal['direction']} {signal['confidence']:.2f}, divergence={divergence:.2f}",
                )
            elif divergence >= 0.40:
                profile.add_reason("flow", f"分析师分歧高 ({divergence:.2f})，适合进观察池")
            rows.append(
                {
                    "stock_code": profile.stock_code,
                    "stock_name": profile.stock_name,
                    "source": "analyst_divergence",
                    "direction": signal["direction"],
                    "score": round(_signed_direction(signal["direction"]) * signal["confidence"], 4),
                    "detail": f"divergence={divergence:.2f}",
                }
            )

        for signal in self.strategy_inputs["earnings_surprise"]["signals"]:
            profile = self._ensure_candidate(signal["stock_code"], signal["stock_name"])
            signed = _signed_direction(signal["direction"]) * signal["confidence"]
            profile.flow_score += signed
            profile.add_reason("flow", f"业绩预期差 {signal['direction']} {signal['confidence']:.2f}")
            rows.append(
                {
                    "stock_code": profile.stock_code,
                    "stock_name": profile.stock_name,
                    "source": "earnings_surprise",
                    "direction": signal["direction"],
                    "score": round(signed, 4),
                    "detail": "",
                }
            )

        rows = sorted(rows, key=lambda item: abs(item["score"]), reverse=True)
        return {
            "source_paths": [
                self.strategy_inputs["institution_association"]["path"],
                self.strategy_inputs["analyst_divergence"]["path"],
                self.strategy_inputs["earnings_surprise"]["path"],
            ],
            "top_flow_signals": rows[:12],
        }

    def build_filter_stage(self) -> Dict[str, Any]:
        graph_rank: Dict[str, float] = {}
        graph_source = self.strategy_inputs["graph_factors"]["signals"]

        for signal in graph_source:
            profile = self._ensure_candidate(signal["stock_code"], signal["stock_name"])
            meta = signal["metadata"]
            rank = _safe_int(meta.get("rank", 0))
            total = max(_safe_int(meta.get("total_stocks", 0)), 1)
            percentile = 1.0 - (rank - 1) / max(total - 1, 1) if rank > 0 else 0.5
            graph_rank[profile.stock_code] = percentile
            if percentile >= 0.75:
                profile.filter_quality += 0.14
                profile.add_reason("filter", f"图谱结构靠前 (rank {rank}/{total})")
            elif percentile <= 0.25:
                profile.filter_quality -= 0.08
                profile.add_reason("filter", f"图谱结构偏弱 (rank {rank}/{total})")

        rows: List[Dict[str, Any]] = []
        for profile in self.candidates.values():
            event_prior_strengths = [
                _safe_float(self.bucket_priors.get(event_type, {}).get("edge_strength", 0.0))
                for event_type in profile.catalyst_event_types
            ]
            if event_prior_strengths:
                avg_prior = sum(event_prior_strengths) / len(event_prior_strengths)
                if avg_prior >= 0.15:
                    profile.filter_quality += 0.12
                    profile.add_reason("filter", f"历史桶质量较强 ({avg_prior:+.2f})")
                elif avg_prior <= -0.10:
                    profile.filter_quality -= 0.12
                    profile.add_reason("filter", f"历史桶质量偏弱 ({avg_prior:+.2f})")
            else:
                avg_prior = 0.0

            stage_values = [
                profile.regime_score,
                profile.mainline_score,
                profile.catalyst_score,
                profile.flow_score,
            ]
            positives = sum(1 for value in stage_values if value > 0.08)
            negatives = sum(1 for value in stage_values if value < -0.08)
            if positives and negatives:
                profile.filter_quality -= 0.10
                profile.add_reason("filter", "多阶段方向冲突，降低优先级")
            elif max(positives, negatives) >= 2:
                profile.filter_quality += 0.06
                profile.add_reason("filter", "多阶段方向一致")

            profile.filter_quality = _clamp(profile.filter_quality, 0.05, 0.95)
            rows.append(
                {
                    "stock_code": profile.stock_code,
                    "stock_name": profile.stock_name,
                    "graph_percentile": round(graph_rank.get(profile.stock_code, 0.5), 4),
                    "bucket_support": round(avg_prior, 4),
                    "filter_quality": round(profile.filter_quality, 4),
                }
            )

        rows = sorted(rows, key=lambda item: item["filter_quality"], reverse=True)
        return {
            "source_paths": [self.strategy_inputs["graph_factors"]["path"]],
            "top_filter_support": rows[:12],
        }

    def build_execution_stage(self) -> Dict[str, Any]:
        longs: List[Dict[str, Any]] = []
        avoids: List[Dict[str, Any]] = []
        watchlist: List[Dict[str, Any]] = []

        for profile in self.candidates.values():
            score = profile.final_score
            thematic_support = profile.regime_score + profile.mainline_score
            catalyst_support = profile.catalyst_score
            flow_support = profile.flow_score
            route = ""
            action = ""

            strong_thematic_long = thematic_support >= 0.90 and score >= 0.22
            catalyst_long = catalyst_support >= 0.18 and flow_support >= 0.35 and score >= 0.20
            catchup_long = flow_support >= 0.60 and score >= 0.14
            catalyst_avoid = catalyst_support <= -0.16 and score <= -0.14
            cooling_avoid = (thematic_support <= -0.10 or flow_support <= -0.60) and score <= -0.12

            if strong_thematic_long:
                route = "主线趋势多头"
                action = "open_long"
            elif catalyst_long:
                if flow_support >= 0.60 and thematic_support < 0.25:
                    route = "预期差/补涨"
                    action = "rotate_into"
                else:
                    route = "事件驱动多头"
                    action = "probe_long"
            elif catchup_long:
                route = "预期差/补涨"
                action = "rotate_into"
            elif catalyst_avoid:
                route = "事件防守/回避"
                action = "avoid_or_hedge"
            elif cooling_avoid:
                route = "主题降温回避"
                action = "reduce_or_avoid"
            elif (
                abs(score) >= 0.10
                or abs(catalyst_support) >= 0.18
                or abs(flow_support) >= 0.55
                or abs(thematic_support) >= 0.90
            ):
                route = "观察池"
                action = "watchlist"
            else:
                continue

            row = {
                "stock_code": profile.stock_code,
                "stock_name": profile.stock_name,
                "industry": profile.industry,
                "route": route,
                "action": action,
                "final_score": round(score, 4),
                "regime": round(profile.regime_score, 4),
                "mainline": round(profile.mainline_score, 4),
                "catalyst": round(catalyst_support, 4),
                "flow": round(flow_support, 4),
                "filter_quality": round(profile.filter_quality, 4),
                "thesis": " | ".join(
                    (
                        profile.reasons["regime"][:1]
                        + profile.reasons["mainline"][:1]
                        + profile.reasons["catalyst"][:1]
                        + profile.reasons["flow"][:1]
                        + profile.reasons["filter"][:1]
                    )[:4]
                ),
            }
            if route in {"主线趋势多头", "事件驱动多头", "预期差/补涨"}:
                longs.append(row)
            elif route in {"事件防守/回避", "主题降温回避"}:
                avoids.append(row)
            else:
                watchlist.append(row)

        longs.sort(key=lambda item: item["final_score"], reverse=True)
        avoids.sort(key=lambda item: item["final_score"])
        watchlist.sort(key=lambda item: abs(item["final_score"]), reverse=True)

        route_counter = Counter(row["route"] for row in longs + avoids + watchlist)
        if any(row["route"] == "主线趋势多头" for row in longs):
            primary_line = "主线趋势多头 + selective catalyst"
        elif avoids and (not longs or abs(avoids[0]["final_score"]) > longs[0]["final_score"]):
            primary_line = "防守 + 事件 continuation overlay"
        elif longs:
            primary_line = "催化驱动 + 预期差轮动"
        else:
            primary_line = "观察池主导，等待主线确认"

        if any(row["route"] == "预期差/补涨" for row in longs):
            secondary_line = "机构/预期差补涨"
        elif watchlist:
            secondary_line = "观察池培育"
        else:
            secondary_line = "图谱扩展候选"

        return {
            "primary_line": primary_line,
            "secondary_line": secondary_line,
            "route_mix": dict(route_counter),
            "core_longs": longs[:8],
            "avoid_overlay": avoids[:8],
            "watchlist": watchlist[:8],
        }

    def run(self) -> Dict[str, Any]:
        regime = self.build_regime_stage()
        mainline = self.build_mainline_stage()
        catalyst = self.build_catalyst_stage()
        flow = self.build_flow_stage()
        filters = self.build_filter_stage()
        execution = self.build_execution_stage()
        validation = self.build_validation_stage()
        return {
            "as_of_date": self.as_of_date.isoformat(),
            "source_inputs": {
                name: snapshot.get("path", "")
                for name, snapshot in self.strategy_inputs.items()
                if snapshot.get("path")
            },
            "shock_source": self.shock_inputs.get("path", ""),
            "bucket_prior_count": len(self.bucket_priors),
            "execution_summary": {
                "primary_line": execution["primary_line"],
                "secondary_line": execution["secondary_line"],
                "route_mix": execution["route_mix"],
            },
            "validation_summary": {
                "route_focus": validation["route_focus"],
                "deferred_lines": validation["deferred_lines"],
            },
            "core_longs": execution["core_longs"],
            "avoid_overlay": execution["avoid_overlay"],
            "watchlist": execution["watchlist"],
            "sub_line_validation": validation["sub_lines"],
            "stages": {
                "regime": regime,
                "mainline": mainline,
                "catalyst": catalyst,
                "flow_expectation": flow,
                "graph_sentiment_filter": filters,
                "execution_mapping": execution,
                "line_validation": validation,
            },
        }


def _markdown_table(headers: List[str], rows: Iterable[List[str]]) -> List[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def _render_report(payload: Dict[str, Any]) -> str:
    stages = payload["stages"]
    regime = stages["regime"]
    mainline = stages["mainline"]
    catalyst = stages["catalyst"]
    flow = stages["flow_expectation"]
    filters = stages["graph_sentiment_filter"]
    execution = stages["execution_mapping"]
    validation = stages["line_validation"]

    lines = [
        "# A股交易员总控框架报告",
        "",
        f"**日期**: {payload['as_of_date']}",
        f"**Shock 校准源**: `{payload.get('shock_source', '')}`",
        f"**历史事件桶数**: {payload.get('bucket_prior_count', 0)}",
        "",
        "## Regime",
        "",
        f"- 风险模式: `{regime['risk_mode']}`",
        f"- 宏观相位分布: `{regime['macro_phases']}`",
        "",
    ]

    lines.extend(
        _markdown_table(
            ["Favored Industries", "Score"],
            [[item["name"], f"{item['score']:+.2f}"] for item in regime["favored_industries"]] or [["N/A", "0.00"]],
        )
    )
    lines.extend(["", "## Mainline", ""])
    lines.extend(
        _markdown_table(
            ["Top Themes", "Score"],
            [[item["name"], f"{item['score']:+.2f}"] for item in mainline["top_themes"]] or [["N/A", "0.00"]],
        )
    )

    lines.extend(["", "## Catalyst", ""])
    lines.extend(
        _markdown_table(
            ["Code", "Name", "Event", "Dir", "Conf", "Prior", "Title"],
            [
                [
                    item["stock_code"],
                    item["stock_name"],
                    item["event_type"],
                    item["direction"],
                    f"{item['confidence']:.2f}",
                    f"{item['prior_strength']:+.2f}",
                    item["title"][:28],
                ]
                for item in catalyst["top_catalysts"]
            ] or [["N/A", "", "", "", "0.00", "0.00", ""]],
        )
    )

    lines.extend(["", "## Flow / Expectation", ""])
    lines.extend(
        _markdown_table(
            ["Code", "Name", "Source", "Dir", "Score", "Detail"],
            [
                [
                    item["stock_code"],
                    item["stock_name"],
                    item["source"],
                    item["direction"],
                    f"{item['score']:+.2f}",
                    item["detail"],
                ]
                for item in flow["top_flow_signals"]
            ] or [["N/A", "", "", "", "0.00", ""]],
        )
    )

    lines.extend(["", "## Graph / Sentiment Filter", ""])
    lines.extend(
        _markdown_table(
            ["Code", "Name", "GraphPct", "Bucket", "Quality"],
            [
                [
                    item["stock_code"],
                    item["stock_name"],
                    f"{item['graph_percentile']:.2f}",
                    f"{item['bucket_support']:+.2f}",
                    f"{item['filter_quality']:.2f}",
                ]
                for item in filters["top_filter_support"]
            ] or [["N/A", "", "0.50", "0.00", "0.50"]],
        )
    )

    lines.extend([
        "",
        "## Execution Mapping",
        "",
        f"- Primary line: `{execution['primary_line']}`",
        f"- Secondary line: `{execution['secondary_line']}`",
        f"- Route mix: `{execution['route_mix']}`",
        "",
        "### Core Longs",
        "",
    ])
    lines.extend(
        _markdown_table(
            ["Code", "Name", "Route", "Score", "R", "M", "C", "F", "Q"],
            [
                [
                    item["stock_code"],
                    item["stock_name"],
                    item["route"],
                    f"{item['final_score']:+.2f}",
                    f"{item['regime']:+.2f}",
                    f"{item['mainline']:+.2f}",
                    f"{item['catalyst']:+.2f}",
                    f"{item['flow']:+.2f}",
                    f"{item['filter_quality']:.2f}",
                ]
                for item in execution["core_longs"]
            ] or [["N/A", "", "", "0.00", "0.00", "0.00", "0.00", "0.00", "0.50"]],
        )
    )
    lines.extend(["", "### Avoid Overlay", ""])
    lines.extend(
        _markdown_table(
            ["Code", "Name", "Route", "Score", "R", "M", "C", "F", "Q"],
            [
                [
                    item["stock_code"],
                    item["stock_name"],
                    item["route"],
                    f"{item['final_score']:+.2f}",
                    f"{item['regime']:+.2f}",
                    f"{item['mainline']:+.2f}",
                    f"{item['catalyst']:+.2f}",
                    f"{item['flow']:+.2f}",
                    f"{item['filter_quality']:.2f}",
                ]
                for item in execution["avoid_overlay"]
            ] or [["N/A", "", "", "0.00", "0.00", "0.00", "0.00", "0.00", "0.50"]],
        )
    )
    lines.extend(["", "### Watchlist", ""])
    lines.extend(
        _markdown_table(
            ["Code", "Name", "Route", "Score", "Thesis"],
            [
                [
                    item["stock_code"],
                    item["stock_name"],
                    item["route"],
                    f"{item['final_score']:+.2f}",
                    item["thesis"][:64],
                ]
                for item in execution["watchlist"]
            ] or [["N/A", "", "", "0.00", ""]],
        )
    )
    lines.extend([
        "",
        "## Validation Blueprint",
        "",
        f"- Focus lines: `{validation['route_focus']}`",
        f"- Deferred lines: `{validation['deferred_lines']}`",
        "",
    ])
    lines.extend(
        _markdown_table(
            ["子线", "模式", "WF/OOS Sharpe", "年化代理", "胜率", "样本", "角色", "状态"],
            [
                [
                    item["label"],
                    item["validation_mode"],
                    _fmt_num(item["wf_sharpe"], 2),
                    _fmt_pct(item["annualized_proxy_simple"], 2),
                    _fmt_hit(item["hit_rate"], 1),
                    str(item["oos_evals"]),
                    item["role"],
                    item["status"],
                ]
                for item in validation["sub_lines"]
            ] or [["N/A", "", "0.00", "0.00%", "0.0%", "0", "", ""]],
        )
    )
    lines.extend(["", "### Monetization Paths", ""])
    lines.extend(
        _markdown_table(
            ["子线", "赚钱路径", "备注"],
            [
                [
                    item["label"],
                    item["monetization_path"][:40],
                    item["notes"][:40],
                ]
                for item in validation["sub_lines"]
            ] or [["N/A", "", ""]],
        )
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the A-share trading-desk control framework")
    parser.add_argument("--as-of-date", type=str, default="", help="Override report date (YYYY-MM-DD)")
    parser.add_argument("--output-dir", type=str, default="", help="Report output directory")
    args = parser.parse_args()

    framework = TradingDeskFramework(as_of_date=args.as_of_date or None)
    payload = framework.run()

    output_dir = Path(args.output_dir) if args.output_dir else _reports_root()
    output_dir.mkdir(parents=True, exist_ok=True)
    tag = payload["as_of_date"].replace("-", "")

    report_path = output_dir / f"trading_desk_framework_{tag}.md"
    json_path = output_dir / f"trading_desk_framework_{tag}.json"

    report_path.write_text(_render_report(payload), encoding="utf-8")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print()
    print("=" * 72)
    print("Trading Desk Framework")
    print("=" * 72)
    print(f"Date:   {payload['as_of_date']}")
    print(f"Report: {report_path}")
    print(f"JSON:   {json_path}")


if __name__ == "__main__":
    main()
