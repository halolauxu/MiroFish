#!/usr/bin/env python3
"""
Audit authoritative source history against the two-year backfill requirement.

This module does not guess. It scans the local authoritative foundation
artifacts and answers three questions for each source:
1. What exact date span do we have locally?
2. Does that span satisfy the last-two-years requirement?
3. If not, is the blocker missing history, snapshot-only design, or both?
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Tuple

from astrategy.archive.authoritative_history import _MIN_BACKTEST_SPAN_DAYS
from astrategy.config import settings


def _safe_load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _coerce_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    if len(text) >= 8 and text[:8].isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:8]}"
    return ""


def _span_days(first_date: str, last_date: str) -> int:
    if not first_date or not last_date:
        return 0
    start = datetime.strptime(first_date, "%Y-%m-%d").date()
    end = datetime.strptime(last_date, "%Y-%m-%d").date()
    if start > end:
        return 0
    return (end - start).days


def _render_table(headers: List[str], rows: Iterable[List[str]]) -> List[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


class FoundationBackfillAuditor:
    def __init__(self, *, anchor_date: str | None = None, universe_id: str = "") -> None:
        self.base_dir = settings.storage._base
        self.audit_root = self.base_dir / "authoritative_archive" / "audit"
        self.report_root = self.base_dir / "reports"
        self.audit_root.mkdir(parents=True, exist_ok=True)
        self.report_root.mkdir(parents=True, exist_ok=True)
        self.anchor_date = date.fromisoformat(anchor_date) if anchor_date else date.today()
        self.required_start = self.anchor_date - timedelta(days=_MIN_BACKTEST_SPAN_DAYS - 1)
        self.required_end = self.anchor_date
        self.universe_id = str(universe_id or "").strip()

    def _load_universe_codes(self) -> set[str]:
        if not self.universe_id:
            return set()
        membership_path = self.base_dir / "datahub" / "universe" / "universe_membership.json"
        payload = _safe_load_json(membership_path) or {}
        rows = payload.get("memberships", []) if isinstance(payload, dict) else []
        return {
            str(row.get("ticker", "")).strip().zfill(6)
            for row in rows
            if isinstance(row, dict)
            and str(row.get("universe_id", "")).strip() == self.universe_id
            and str(row.get("ticker", "")).strip()
        }

    def _scan_date_values(
        self,
        paths: Iterable[Path],
        extractor: Callable[[Any], Iterable[str]],
    ) -> Dict[str, Any]:
        first_date = ""
        last_date = ""
        total_files = 0
        populated_files = 0
        total_items = 0

        for path in paths:
            total_files += 1
            payload = _safe_load_json(path)
            if payload is None:
                continue
            values = [item for item in extractor(payload) if item]
            normalized = sorted({_coerce_date(item) for item in values if _coerce_date(item)})
            if not normalized:
                continue
            populated_files += 1
            total_items += len(normalized)
            if not first_date or normalized[0] < first_date:
                first_date = normalized[0]
            if not last_date or normalized[-1] > last_date:
                last_date = normalized[-1]

        span_days = _span_days(first_date, last_date)
        has_two_year_span = bool(
            first_date
            and last_date
            and first_date <= self.required_start.isoformat()
            and last_date >= self.required_end.isoformat()
        )
        return {
            "file_count": total_files,
            "populated_file_count": populated_files,
            "date_value_count": total_items,
            "first_date": first_date,
            "last_date": last_date,
            "span_days": span_days,
            "two_year_ready": has_two_year_span,
        }

    def _price_audit(self) -> Dict[str, Any]:
        root = self.base_dir / "datahub" / "market" / "daily"
        files = sorted(root.glob("*.json"))
        universe_codes = self._load_universe_codes()
        if universe_codes:
            files = [path for path in files if path.stem in universe_codes]
        file_count = 0
        populated_files = 0
        ready_files = 0
        first_date = ""
        last_date = ""

        for path in files:
            file_count += 1
            payload = _safe_load_json(path)
            if not isinstance(payload, list) or not payload:
                continue
            dates = sorted(
                _coerce_date(row.get("trade_date"))
                for row in payload
                if isinstance(row, dict) and _coerce_date(row.get("trade_date"))
            )
            if not dates:
                continue
            populated_files += 1
            if dates[0] <= self.required_start.isoformat() and dates[-1] >= self.required_end.isoformat():
                ready_files += 1
            if not first_date or dates[0] < first_date:
                first_date = dates[0]
            if not last_date or dates[-1] > last_date:
                last_date = dates[-1]

        return {
            "component": "prices",
            "path": str(root),
            "file_count": file_count,
            "populated_file_count": populated_files,
            "two_year_ready_files": ready_files,
            "first_date": first_date,
            "last_date": last_date,
            "span_days": _span_days(first_date, last_date),
            "two_year_ready": ready_files > 0 and ready_files == populated_files,
            "notes": (
                f"{ready_files}/{populated_files} 个本地价格文件覆盖 "
                f"{self.required_start.isoformat()} -> {self.required_end.isoformat()}"
                if populated_files
                else "本地价格目录为空"
            ),
        }

    def _news_audit(self) -> Dict[str, Any]:
        root = self.base_dir / "datahub" / "ingest" / "news" / "by_ticker"
        summary = self._scan_date_values(
            root.glob("*.json"),
            lambda payload: (
                str(item.get("_normalized_publish_date") or item.get("发布时间") or "")
                for item in payload
                if isinstance(payload, list) and isinstance(item, dict)
            ),
        )
        return {
            "component": "news",
            "path": str(root),
            **summary,
            "notes": "公司新闻按 ticker 存档，当前是增量最近新闻而不是完整历史新闻库。",
        }

    def _filings_audit(self) -> Dict[str, Any]:
        root = self.base_dir / "datahub" / "ingest" / "filings" / "by_ticker"
        summary = self._scan_date_values(
            root.glob("*.json"),
            lambda payload: (
                str(item.get("publish_date") or item.get("publish_time") or "")
                for item in payload
                if isinstance(payload, list) and isinstance(item, dict)
            ),
        )
        return {
            "component": "filings",
            "path": str(root),
            **summary,
            "notes": "公告层当前默认是 rolling window，不是两年全量归档。",
        }

    def _sentiment_audit(self) -> Dict[str, Any]:
        root = self.base_dir / "datahub" / "ingest" / "sentiment" / "by_ticker"

        def _extract(payload: Any) -> Iterable[str]:
            if not isinstance(payload, dict):
                return []
            dates = [str(payload.get("ingest_date") or "")]
            dates.extend(
                str(item.get("publish_date") or item.get("available_at") or "")
                for item in payload.get("items", [])
                if isinstance(item, dict)
            )
            return dates

        summary = self._scan_date_values(root.glob("*.json"), _extract)
        return {
            "component": "sentiment",
            "path": str(root),
            **summary,
            "notes": "情绪层是 ingest 当天快照聚合，不是连续两年日历快照。",
        }

    def _events_audit(self) -> Dict[str, Any]:
        path = self.base_dir / "datahub" / "ingest" / "events" / "event_master.json"

        def _extract(payload: Any) -> Iterable[str]:
            if not isinstance(payload, list):
                return []
            values: List[str] = []
            for item in payload:
                if not isinstance(item, dict):
                    continue
                for key in ("available_at", "event_time", "discover_time"):
                    candidate = _coerce_date(item.get(key))
                    if candidate:
                        values.append(candidate)
                        break
                metadata = item.get("metadata") or {}
                if isinstance(metadata, dict):
                    fallback = _coerce_date(metadata.get("legacy_event_date") or metadata.get("ingest_date"))
                    if fallback:
                        values.append(fallback)
            return values

        summary = self._scan_date_values([path], _extract)
        return {
            "component": "events",
            "path": str(path),
            **summary,
            "notes": "事件主表来自当前增量事件汇总，历史跨度受上游公告/新闻窗口约束。",
        }

    def _graph_audit(self) -> Dict[str, Any]:
        path = self.base_dir / "datahub" / "graph" / "graph_manifest.json"
        payload = _safe_load_json(path) or {}
        as_of_date = _coerce_date((payload.get("summary") or {}).get("as_of_date"))
        return {
            "component": "graph",
            "path": str(path),
            "file_count": 1 if path.exists() else 0,
            "populated_file_count": 1 if as_of_date else 0,
            "date_value_count": 1 if as_of_date else 0,
            "first_date": as_of_date,
            "last_date": as_of_date,
            "span_days": 0,
            "two_year_ready": False,
            "notes": "图谱层当前是单日状态快照，不是两年连续历史图谱。",
        }

    def build_audit(self) -> Dict[str, Any]:
        components = [
            self._price_audit(),
            self._news_audit(),
            self._filings_audit(),
            self._sentiment_audit(),
            self._events_audit(),
            self._graph_audit(),
        ]
        return {
            "generated_at": datetime.now().isoformat(),
            "anchor_date": self.required_end.isoformat(),
            "required_start_date": self.required_start.isoformat(),
            "required_span_days": _MIN_BACKTEST_SPAN_DAYS,
            "universe_id": self.universe_id,
            "components": {item["component"]: item for item in components},
        }

    def _render_report(self, audit: Dict[str, Any]) -> str:
        rows: List[List[str]] = []
        for component_name, item in audit["components"].items():
            rows.append(
                [
                    component_name,
                    str(item.get("file_count", 0)),
                    str(item.get("populated_file_count", 0)),
                    item.get("first_date", "") or "N/A",
                    item.get("last_date", "") or "N/A",
                    str(item.get("span_days", 0)),
                    "YES" if item.get("two_year_ready") else "NO",
                    str(item.get("notes", "")),
                ]
            )

        lines = [
            "# Foundation Two-Year Backfill Audit",
            "",
            f"**Anchor Date**: {audit['anchor_date']}",
            f"**Required Start**: {audit['required_start_date']}",
            f"**Required Span**: {audit['required_span_days']} days",
            f"**Universe**: {audit.get('universe_id', '') or 'all_local_files'}",
            "",
            "## Component Summary",
            "",
        ]
        lines.extend(
            _render_table(
                ["组件", "Files", "Populated", "首日", "末日", "SpanDays", "TwoYearReady", "Notes"],
                rows,
            )
        )
        return "\n".join(lines)

    def write_audit(self, audit: Dict[str, Any] | None = None) -> Tuple[Path, Path]:
        payload = audit or self.build_audit()
        json_path = self.audit_root / "foundation_backfill_audit.json"
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

        date_tag = self.required_end.strftime("%Y%m%d")
        report_path = self.report_root / f"foundation_backfill_audit_{date_tag}.md"
        report_path.write_text(self._render_report(payload), encoding="utf-8")
        return json_path, report_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit authoritative source history for two-year backfill readiness")
    parser.add_argument("--anchor-date", default=None, help="Anchor date in YYYY-MM-DD; defaults to today")
    parser.add_argument("--universe", default="", help="Optional universe id, e.g. csi800")
    args = parser.parse_args()

    auditor = FoundationBackfillAuditor(anchor_date=args.anchor_date, universe_id=args.universe)
    audit = auditor.build_audit()
    json_path, report_path = auditor.write_audit(audit)
    print(f"AUDIT_JSON={json_path}")
    print(f"AUDIT_REPORT={report_path}")


if __name__ == "__main__":
    main()
