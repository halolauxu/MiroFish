"""Universe snapshot builders."""

from __future__ import annotations

import json
import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from ..common import ensure_dir, universe_root

logger = logging.getLogger("astrategy.datahub.universe_snapshots")


def build_universe_snapshots(
    security_master: List[Dict[str, Any]],
    universe_membership: Dict[str, Any],
    *,
    as_of_date: str | None = None,
) -> Dict[str, Dict[str, Any]]:
    """Build point-in-time universe snapshots from the membership table."""
    as_of = as_of_date or date.today().isoformat()
    security_index = {
        str(item.get("ticker", "")).zfill(6): item
        for item in security_master
    }

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in universe_membership.get("memberships", []):
        universe_id = str(row.get("universe_id", "")).strip()
        ticker = str(row.get("ticker", "")).zfill(6)
        if not universe_id or not ticker or ticker == "000000":
            continue
        sec = security_index.get(ticker, {})
        grouped.setdefault(universe_id, []).append(
            {
                "ticker": ticker,
                "company_name": sec.get("company_name") or row.get("company_name") or ticker,
                "industry_l1": sec.get("industry_l1", ""),
                "status": sec.get("status", ""),
                "membership_source": row.get("membership_source", ""),
                "valid_from": row.get("valid_from", ""),
                "valid_to": row.get("valid_to", ""),
            }
        )

    snapshots: Dict[str, Dict[str, Any]] = {}
    for universe_id, constituents in grouped.items():
        snapshots[universe_id] = {
            "summary": {
                "universe_id": universe_id,
                "as_of_date": as_of,
                "constituent_count": len(constituents),
            },
            "constituents": sorted(constituents, key=lambda item: item["ticker"]),
        }
    logger.info("Built %d universe snapshots", len(snapshots))
    return snapshots


def save_universe_snapshots(payload: Dict[str, Dict[str, Any]]) -> Dict[str, Path]:
    snapshot_dir = ensure_dir(universe_root() / "snapshots")
    outputs: Dict[str, Path] = {}
    for universe_id, snapshot in payload.items():
        output = snapshot_dir / f"{universe_id}_snapshot.json"
        output.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        outputs[universe_id] = output
    logger.info("Saved %d universe snapshots", len(outputs))
    return outputs
