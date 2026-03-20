#!/usr/bin/env python3
"""
Authoritative historical archive for desk-line research.

Purpose:
    1. Persist official strategy snapshots into a canonical archive.
    2. Materialize aligned factor snapshots for future strict WF.
    3. Audit historical continuity by line / strategy.

Important:
    This module is designed for authoritative daily/weekly research runs.
    It should be invoked explicitly from schedulers or manual backfill jobs,
    not from generic ``save_signals()`` calls that may come from experiments.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from astrategy.config import settings
from astrategy.factors.store import FactorStore
from astrategy.strategies.base import StrategySignal, _now_cst

_MIN_BACKTEST_SPAN_DAYS = 730
_MIN_SNAPSHOT_COUNT_BY_CADENCE = {
    "daily": 480,
    "weekly": 100,
}


@dataclass(frozen=True)
class StrategyArchiveSpec:
    strategy_name: str
    line_id: str
    line_label: str
    cadence: str
    min_periods_for_strict_wf: int
    description: str


STRATEGY_ARCHIVE_SPECS: Dict[str, StrategyArchiveSpec] = {
    "sector_rotation": StrategyArchiveSpec(
        strategy_name="sector_rotation",
        line_id="regime_rotation",
        line_label="Regime 轮动",
        cadence="weekly",
        min_periods_for_strict_wf=100,
        description="战场选择器，负责周频宏观/行业轮动快照。",
    ),
    "institution_association": StrategyArchiveSpec(
        strategy_name="institution_association",
        line_id="flow_expectation",
        line_label="资金/预期差",
        cadence="weekly",
        min_periods_for_strict_wf=100,
        description="机构共振与 catch-up gap 的周频快照。",
    ),
    "analyst_divergence": StrategyArchiveSpec(
        strategy_name="analyst_divergence",
        line_id="flow_expectation",
        line_label="资金/预期差",
        cadence="weekly",
        min_periods_for_strict_wf=100,
        description="卖方分歧与目标价偏离的周频快照。",
    ),
    "graph_factors": StrategyArchiveSpec(
        strategy_name="graph_factors",
        line_id="graph_filter",
        line_label="图谱过滤",
        cadence="daily",
        min_periods_for_strict_wf=480,
        description="图谱结构因子的日频快照。",
    ),
    "sentiment_simulation": StrategyArchiveSpec(
        strategy_name="sentiment_simulation",
        line_id="sentiment_catalyst",
        line_label="情绪催化",
        cadence="daily",
        min_periods_for_strict_wf=480,
        description="多轮情绪模拟与事件催化的日频快照。",
    ),
    "narrative_tracker": StrategyArchiveSpec(
        strategy_name="narrative_tracker",
        line_id="mainline_narrative",
        line_label="主线叙事",
        cadence="daily",
        min_periods_for_strict_wf=480,
        description="主线叙事相位与龙头/补涨映射的日频快照。",
    ),
    "earnings_surprise": StrategyArchiveSpec(
        strategy_name="earnings_surprise",
        line_id="flow_expectation",
        line_label="资金/预期差",
        cadence="daily",
        min_periods_for_strict_wf=480,
        description="财报预期差事件快照。",
    ),
}


def _archive_root() -> Path:
    root = settings.storage._base / "authoritative_archive"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _signal_archive_dir() -> Path:
    path = _archive_root() / "signals"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _audit_dir() -> Path:
    path = _archive_root() / "audit"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _report_dir() -> Path:
    path = settings.storage._base / "reports"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_json_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float, str)):
        return value
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return str(value)


def _flatten_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    flat: Dict[str, Any] = {}
    for key, value in (metadata or {}).items():
        flat[f"meta_{key}"] = _safe_json_scalar(value)
    return flat


def _as_date_str(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    if not text:
        return fallback
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10].replace("-", "")
    return text[:8]


def _as_iso_date(value: str) -> str:
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
    return value


def _business_periods(first_date: str, last_date: str, cadence: str) -> int:
    if not first_date or not last_date:
        return 0
    start = pd.Timestamp(_as_iso_date(first_date))
    end = pd.Timestamp(_as_iso_date(last_date))
    if start > end:
        return 0
    if cadence == "weekly":
        start_period = start.to_period("W-FRI")
        end_period = end.to_period("W-FRI")
        return len(pd.period_range(start=start_period, end=end_period, freq="W-FRI"))
    return len(pd.bdate_range(start=start, end=end))


def _span_days(first_date: str, last_date: str) -> int:
    if not first_date or not last_date:
        return 0
    start = pd.Timestamp(_as_iso_date(first_date))
    end = pd.Timestamp(_as_iso_date(last_date))
    if start > end:
        return 0
    return int((end - start).days)


def _render_table(headers: List[str], rows: Iterable[List[str]]) -> List[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


class AuthoritativeHistoryBuilder:
    """Build and audit authoritative desk-line archives."""

    def __init__(self, base_dir: Path | str | None = None) -> None:
        self.base_dir = Path(base_dir) if base_dir else settings.storage._base
        self.signal_root = settings.storage.signal_dir
        self.factor_store = FactorStore(base_dir=self.base_dir / "factors")
        self.archive_root = _archive_root()
        self.signal_archive_root = _signal_archive_dir()
        self.audit_root = _audit_dir()
        self.report_root = _report_dir()

    def _spec(self, strategy_name: str) -> Optional[StrategyArchiveSpec]:
        return STRATEGY_ARCHIVE_SPECS.get(strategy_name)

    def _normalize_records(self, signals: List[StrategySignal], as_of_date: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for signal in signals:
            raw = signal.to_dict()
            rows.append(
                {
                    **raw,
                    "as_of_date": as_of_date,
                    "signal_date": _as_iso_date(as_of_date),
                    "signed_direction": signal.signed_score,
                    "direction_score": 1 if signal.direction == "long" else (-1 if signal.direction == "avoid" else 0),
                }
            )
        return rows

    def _records_to_factor_frame(
        self,
        strategy_name: str,
        line_id: str,
        cadence: str,
        records: List[Dict[str, Any]],
        as_of_date: str,
    ) -> pd.DataFrame:
        rows: List[Dict[str, Any]] = []
        for idx, row in enumerate(records):
            metadata = row.get("metadata") or {}
            flat = {
                "archive_row_id": f"{strategy_name}:{as_of_date}:{idx}",
                "as_of_date": as_of_date,
                "line_id": line_id,
                "cadence": cadence,
                "stock_code": str(row.get("stock_code", "")).strip().zfill(6),
                "stock_name": str(row.get("stock_name", "")).strip(),
                "direction": str(row.get("direction", "neutral")).strip(),
                "signed_direction": float(row.get("signed_direction", 0.0) or 0.0),
                "confidence": float(row.get("confidence", 0.0) or 0.0),
                "expected_return": float(row.get("expected_return", 0.0) or 0.0),
                "holding_period_days": int(row.get("holding_period_days", 0) or 0),
                "reasoning": str(row.get("reasoning", "")).strip(),
                "timestamp": str(row.get("timestamp", "")).strip(),
                "expires_at": str(row.get("expires_at", "")).strip(),
                "metadata_json": json.dumps(metadata, ensure_ascii=False, sort_keys=True),
            }
            flat.update(_flatten_metadata(metadata))
            rows.append(flat)

        frame = pd.DataFrame(rows)
        if "stock_code" in frame.columns and frame["stock_code"].is_unique:
            frame = frame.set_index("stock_code")
        return frame

    def archive_signals(
        self,
        strategy_name: str,
        signals: List[StrategySignal],
        as_of_date: str,
        source_path: str = "",
        run_context: str = "manual",
    ) -> Dict[str, Any]:
        spec = self._spec(strategy_name)
        if spec is None:
            return {"strategy_name": strategy_name, "status": "skipped", "reason": "unmapped_strategy"}

        as_of_date = _as_date_str(as_of_date, fallback=_now_cst().strftime("%Y%m%d"))
        records = self._normalize_records(signals, as_of_date)

        strategy_dir = self.signal_archive_root / strategy_name
        strategy_dir.mkdir(parents=True, exist_ok=True)
        archive_path = strategy_dir / f"{as_of_date}.json"

        payload = {
            "strategy_name": strategy_name,
            "line_id": spec.line_id,
            "line_label": spec.line_label,
            "cadence": spec.cadence,
            "as_of_date": as_of_date,
            "source_path": source_path,
            "run_context": run_context,
            "authoritative": True,
            "generated_at": _now_cst().isoformat(),
            "signal_count": len(records),
            "directional_signal_count": sum(1 for row in records if row["direction"] in {"long", "avoid"}),
            "records": records,
        }
        archive_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        factor_frame = self._records_to_factor_frame(
            strategy_name=strategy_name,
            line_id=spec.line_id,
            cadence=spec.cadence,
            records=records,
            as_of_date=as_of_date,
        )
        factor_path = self.factor_store.save(strategy_name, as_of_date, factor_frame)
        return {
            "strategy_name": strategy_name,
            "line_id": spec.line_id,
            "status": "archived",
            "archive_path": str(archive_path),
            "factor_path": str(factor_path),
            "signal_count": len(records),
        }

    def archive_signal_file(self, strategy_name: str, signal_path: Path | str) -> Dict[str, Any]:
        path = Path(signal_path)
        if not path.exists():
            return {"strategy_name": strategy_name, "status": "missing", "reason": str(path)}

        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return {"strategy_name": strategy_name, "status": "skipped", "reason": "non_list_payload"}

        signals: List[StrategySignal] = []
        for row in payload:
            if not isinstance(row, dict):
                continue
            try:
                signals.append(StrategySignal.from_dict(row))
            except Exception:
                continue
        as_of_date = _as_date_str(path.stem)
        return self.archive_signals(
            strategy_name=strategy_name,
            signals=signals,
            as_of_date=as_of_date,
            source_path=str(path),
            run_context="backfill",
        )

    def backfill_existing(
        self,
        strategy_names: Optional[List[str]] = None,
        dates: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        selected = strategy_names or sorted(STRATEGY_ARCHIVE_SPECS.keys())
        date_filter = {str(item).strip() for item in (dates or []) if str(item).strip()}
        results: List[Dict[str, Any]] = []

        for strategy_name in selected:
            signal_dir = self.signal_root / strategy_name
            if not signal_dir.exists():
                results.append({"strategy_name": strategy_name, "status": "missing_signal_dir"})
                continue
            for path in sorted(signal_dir.glob("*.json")):
                if path.name.startswith("_"):
                    continue
                if date_filter and path.stem not in date_filter:
                    continue
                results.append(self.archive_signal_file(strategy_name, path))

        audit = self.build_audit()
        json_path, report_path = self.write_audit(audit)
        return {
            "results": results,
            "audit_json": str(json_path),
            "audit_report": str(report_path),
        }

    def _load_archived_snapshots(self, strategy_name: str) -> List[Dict[str, Any]]:
        strategy_dir = self.signal_archive_root / strategy_name
        if not strategy_dir.exists():
            return []
        snapshots = []
        for path in sorted(strategy_dir.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            snapshots.append(payload)
        return snapshots

    def build_audit(self) -> Dict[str, Any]:
        strategies: Dict[str, Dict[str, Any]] = {}
        lines: Dict[str, Dict[str, Any]] = {}

        for strategy_name, spec in STRATEGY_ARCHIVE_SPECS.items():
            snapshots = self._load_archived_snapshots(strategy_name)
            signal_dates = sorted(snapshot.get("as_of_date", "") for snapshot in snapshots if snapshot.get("as_of_date"))
            factor_dates = self.factor_store.list_dates(strategy_name)
            directional_dates = sorted(
                snapshot.get("as_of_date", "")
                for snapshot in snapshots
                if _safe_json_scalar(snapshot.get("directional_signal_count", 0)) not in {None, 0, "0"}
            )
            total_signals = sum(int(snapshot.get("signal_count", 0) or 0) for snapshot in snapshots)
            aligned_dates = sorted(set(signal_dates) & set(factor_dates))
            first_date = signal_dates[0] if signal_dates else ""
            last_date = signal_dates[-1] if signal_dates else ""
            expected_periods = _business_periods(first_date, last_date, spec.cadence)
            coverage_ratio = round(min(len(signal_dates) / expected_periods, 1.0), 4) if expected_periods else 0.0
            span_days = _span_days(first_date, last_date)
            min_snapshots = max(
                spec.min_periods_for_strict_wf,
                _MIN_SNAPSHOT_COUNT_BY_CADENCE.get(spec.cadence, spec.min_periods_for_strict_wf),
            )
            has_two_year_span = span_days >= _MIN_BACKTEST_SPAN_DAYS
            strict_ready = (
                has_two_year_span
                and len(aligned_dates) >= min_snapshots
                and len(signal_dates) >= min_snapshots
                and len(signal_dates) == len(factor_dates)
            )

            strategy_entry = {
                "strategy_name": strategy_name,
                "line_id": spec.line_id,
                "line_label": spec.line_label,
                "cadence": spec.cadence,
                "min_periods_for_strict_wf": spec.min_periods_for_strict_wf,
                "signal_dates": signal_dates,
                "factor_dates": factor_dates,
                "aligned_dates": aligned_dates,
                "signal_date_count": len(signal_dates),
                "factor_date_count": len(factor_dates),
                "directional_date_count": len(directional_dates),
                "total_signal_count": total_signals,
                "first_date": first_date,
                "last_date": last_date,
                "span_days": span_days,
                "expected_periods": expected_periods,
                "coverage_ratio": coverage_ratio,
                "min_backtest_span_days": _MIN_BACKTEST_SPAN_DAYS,
                "min_snapshot_count": min_snapshots,
                "two_year_minimum_ready": has_two_year_span and len(signal_dates) >= min_snapshots,
                "strict_wf_ready": strict_ready,
                "notes": spec.description,
            }
            strategies[strategy_name] = strategy_entry

            line_entry = lines.setdefault(
                spec.line_id,
                {
                    "line_id": spec.line_id,
                    "line_label": spec.line_label,
                    "cadence": spec.cadence,
                    "strategies": [],
                    "strict_wf_ready": True,
                    "signal_date_count": 0,
                    "factor_date_count": 0,
                },
            )
            line_entry["strategies"].append(strategy_name)
            line_entry["strict_wf_ready"] = line_entry["strict_wf_ready"] and strict_ready
            line_entry["signal_date_count"] += len(signal_dates)
            line_entry["factor_date_count"] += len(factor_dates)

        for line_id, entry in lines.items():
            strategy_entries = [strategies[name] for name in entry["strategies"]]
            first_dates = [item["first_date"] for item in strategy_entries if item["first_date"]]
            last_dates = [item["last_date"] for item in strategy_entries if item["last_date"]]
            entry["first_date"] = min(first_dates) if first_dates else ""
            entry["last_date"] = max(last_dates) if last_dates else ""
            entry["span_days"] = _span_days(entry["first_date"], entry["last_date"])
            entry["components_ready"] = {
                item["strategy_name"]: item["strict_wf_ready"]
                for item in strategy_entries
            }

        return {
            "generated_at": _now_cst().isoformat(),
            "archive_root": str(self.archive_root),
            "strategies": strategies,
            "lines": lines,
        }

    def _render_audit_report(self, audit: Dict[str, Any]) -> str:
        strategy_rows = []
        for strategy_name, item in sorted(audit["strategies"].items()):
            strategy_rows.append(
                [
                    strategy_name,
                    item["line_label"],
                    item["cadence"],
                    str(item["signal_date_count"]),
                    str(item["factor_date_count"]),
                    str(item["directional_date_count"]),
                    item["first_date"] or "N/A",
                    item["last_date"] or "N/A",
                    str(item.get("span_days", 0)),
                    f"{item['coverage_ratio']:.1%}",
                    "YES" if item["strict_wf_ready"] else "NO",
                ]
            )

        line_rows = []
        for line_id, item in sorted(audit["lines"].items()):
            line_rows.append(
                [
                    item["line_label"],
                    item["cadence"],
                    ",".join(item["strategies"]),
                    str(item["signal_date_count"]),
                    str(item["factor_date_count"]),
                    str(item.get("span_days", 0)),
                    "YES" if item["strict_wf_ready"] else "NO",
                ]
            )

        lines = [
            "# Authoritative Desk Archive Audit",
            "",
            f"**生成时间**: {audit['generated_at']}",
            f"**归档目录**: `{audit['archive_root']}`",
            "",
            "## Line Summary",
            "",
        ]
        lines.extend(
            _render_table(
                ["子线", "周期", "组件", "SignalDates", "FactorDates", "SpanDays", "StrictWFReady"],
                line_rows or [["N/A", "", "", "0", "0", "0", "NO"]],
            )
        )
        lines.extend(["", "## Strategy Summary", ""])
        lines.extend(
            _render_table(
                ["策略", "子线", "周期", "SignalDates", "FactorDates", "DirectionalDates", "首日", "末日", "SpanDays", "覆盖率", "StrictWFReady"],
                strategy_rows or [["N/A", "", "", "0", "0", "0", "N/A", "N/A", "0", "0.0%", "NO"]],
            )
        )
        return "\n".join(lines)

    def write_audit(self, audit: Optional[Dict[str, Any]] = None) -> tuple[Path, Path]:
        payload = audit or self.build_audit()
        json_path = self.audit_root / "archive_audit.json"
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        date_tag = _now_cst().strftime("%Y%m%d")
        report_path = self.report_root / f"authoritative_archive_audit_{date_tag}.md"
        report_path.write_text(self._render_audit_report(payload), encoding="utf-8")
        return json_path, report_path


def archive_authoritative_signals(
    strategy_name: str,
    signals: List[StrategySignal],
    as_of_date: str,
    source_path: str = "",
    run_context: str = "scheduler",
) -> Dict[str, Any]:
    builder = AuthoritativeHistoryBuilder()
    return builder.archive_signals(
        strategy_name=strategy_name,
        signals=signals,
        as_of_date=as_of_date,
        source_path=source_path,
        run_context=run_context,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build authoritative desk-line history archive")
    parser.add_argument("--backfill-existing", action="store_true", help="Backfill from existing .data/signals snapshots")
    parser.add_argument("--audit-only", action="store_true", help="Only rebuild audit from existing archive")
    parser.add_argument("--strategies", type=str, default="", help="Comma-separated strategy names")
    parser.add_argument("--dates", type=str, default="", help="Comma-separated YYYYMMDD dates to backfill")
    args = parser.parse_args()

    selected = [item.strip() for item in args.strategies.split(",") if item.strip()]
    dates = [item.strip() for item in args.dates.split(",") if item.strip()]

    builder = AuthoritativeHistoryBuilder()
    if args.audit_only:
        audit = builder.build_audit()
        json_path, report_path = builder.write_audit(audit)
        print(f"AUDIT_JSON={json_path}")
        print(f"AUDIT_REPORT={report_path}")
        return

    result = builder.backfill_existing(strategy_names=selected or None, dates=dates or None)
    print(f"AUDIT_JSON={result['audit_json']}")
    print(f"AUDIT_REPORT={result['audit_report']}")
    archived = [item for item in result["results"] if item.get("status") == "archived"]
    print(f"ARCHIVED={len(archived)}")


if __name__ == "__main__":
    main()
