"""Universe membership registry builders."""

from __future__ import annotations

import json
import logging
import platform
import signal as _signal
from pathlib import Path
from typing import Any, Dict, Iterable, List

from ..common import datahub_root, ensure_dir

logger = logging.getLogger("astrategy.datahub.universe_registry")


def _resolve_csi800_codes() -> tuple[List[str], str, Dict[str, str]]:
    try:
        import akshare as ak

        use_alarm = platform.system() != "Windows"

        class _Timeout(Exception):
            pass

        def _handler(signum, frame):
            raise _Timeout("index_stock_cons_csindex timed out after 15s")

        old_handler = None
        if use_alarm:
            old_handler = _signal.signal(_signal.SIGALRM, _handler)
            _signal.alarm(15)
        try:
            df = ak.index_stock_cons_csindex(symbol="000906")
        finally:
            if use_alarm:
                _signal.alarm(0)
            if old_handler is not None:
                _signal.signal(_signal.SIGALRM, old_handler)

        codes = [str(code).zfill(6) for code in df["成分券代码"].tolist()]
        name_map = {
            str(code).zfill(6): str(name).strip()
            for code, name in zip(df["成分券代码"].tolist(), df["成分券名称"].tolist())
        }
        source = "live_or_fallback"
        if len(codes) < 500:
            source = "fallback_representative"
        return sorted(set(codes)), source, name_map
    except Exception as exc:
        logger.warning("Failed to resolve CSI800 codes: %s", exc)
        try:
            from astrategy.run_backtest import fetch_csi800_codes

            codes = [str(code).zfill(6) for code in fetch_csi800_codes()]
            source = "live_or_fallback"
            if len(codes) < 500:
                source = "fallback_representative"
            return sorted(set(codes)), source, {}
        except Exception as inner_exc:
            logger.warning("Failed to resolve CSI800 fallback codes: %s", inner_exc)
            return [], "unavailable", {}


def _membership_records(
    universe_id: str,
    tickers: Iterable[str],
    source: str,
    company_names: Dict[str, str] | None = None,
) -> List[Dict[str, Any]]:
    company_names = company_names or {}
    return [
        {
            "universe_id": universe_id,
            "ticker": str(ticker).zfill(6),
            "company_name": str(company_names.get(str(ticker).zfill(6), "")).strip(),
            "valid_from": "2026-03-19",
            "valid_to": "",
            "membership_source": source,
        }
        for ticker in sorted(set(tickers))
    ]


def build_universe_membership(
    security_master: List[Dict[str, Any]],
    production_candidates: Iterable[str] | None = None,
) -> Dict[str, Any]:
    """Build all known universe membership snapshots."""
    all_a_codes = [str(item["ticker"]).zfill(6) for item in security_master]
    csi800_codes, csi800_source, csi800_name_map = _resolve_csi800_codes()
    production_codes = [str(code).zfill(6) for code in (production_candidates or [])]

    memberships = (
        _membership_records("all_a", all_a_codes, "security_master")
        + _membership_records("csi800", csi800_codes, csi800_source, company_names=csi800_name_map)
        + _membership_records("production_candidate", production_codes, "coverage_audit")
    )

    summary = {
        "all_a_size": len(set(all_a_codes)),
        "csi800_size": len(set(csi800_codes)),
        "production_candidate_size": len(set(production_codes)),
        "csi800_source": csi800_source,
    }

    logger.info(
        "Built universe membership: all_a=%d, csi800=%d, production_candidate=%d",
        summary["all_a_size"],
        summary["csi800_size"],
        summary["production_candidate_size"],
    )
    return {
        "summary": summary,
        "memberships": memberships,
    }


def save_universe_membership(payload: Dict[str, Any]) -> Path:
    universe_dir = ensure_dir(datahub_root() / "universe")
    output = universe_dir / "universe_membership.json"
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("Saved universe membership: %s", output)
    return output
