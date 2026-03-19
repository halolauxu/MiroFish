"""Build a lightweight security master for the research platform."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

from astrategy.data_collector.market_data import MarketDataCollector

from ..common import datahub_root, ensure_dir, graph_path

logger = logging.getLogger("astrategy.datahub.security_master")


def _load_graph_companies() -> Dict[str, Dict[str, Any]]:
    graph_file = graph_path()
    if not graph_file.exists():
        return {}

    data = json.loads(graph_file.read_text(encoding="utf-8"))
    nodes = data.get("nodes", {})
    companies: Dict[str, Dict[str, Any]] = {}

    if not isinstance(nodes, dict):
        return companies

    for code, node in nodes.items():
        if not (isinstance(code, str) and code.isdigit() and len(code) == 6):
            continue
        attributes = node.get("attributes", {}) if isinstance(node, dict) else {}
        companies[code] = {
            "ticker": code,
            "company_name": (
                str(node.get("display_name", "")).strip()
                or str(attributes.get("display_name", "")).strip()
                or code
            ),
            "industry_l1": str(attributes.get("industry", "") or "").strip(),
            "graph_summary": str(node.get("summary", "") or "").strip(),
            "graph_labels": list(node.get("labels", []) or []),
        }

    return companies


def _load_name_source() -> Dict[str, str]:
    collector = MarketDataCollector()
    df = collector.get_realtime_quotes()
    if df.empty:
        return {}
    code_col = "代码" if "代码" in df.columns else df.columns[0]
    name_col = "名称" if "名称" in df.columns else df.columns[min(1, len(df.columns) - 1)]
    return {
        str(row[code_col]).zfill(6): str(row[name_col]).strip()
        for _, row in df.iterrows()
        if str(row[code_col]).strip()
    }


def build_security_master() -> List[Dict[str, Any]]:
    """Build the current security master snapshot."""
    graph_companies = _load_graph_companies()
    name_map = _load_name_source()

    all_codes = sorted(set(graph_companies) | set(name_map))
    records: List[Dict[str, Any]] = []

    for code in all_codes:
        graph_info = graph_companies.get(code, {})
        company_name = name_map.get(code) or graph_info.get("company_name") or code
        records.append({
            "ticker": code,
            "company_name": company_name,
            "exchange": "A_SHARE",
            "list_date": "",
            "delist_date": "",
            "status": "active",
            "industry_l1": graph_info.get("industry_l1", ""),
            "industry_l2": "",
            "industry_l3": "",
            "concept_tags": [],
            "name_source": "market_data" if code in name_map else "graph",
            "graph_mapped": code in graph_companies,
        })

    logger.info("Built security master: %d securities", len(records))
    return records


def save_security_master(records: List[Dict[str, Any]]) -> Path:
    universe_dir = ensure_dir(datahub_root() / "universe")
    output = universe_dir / "security_master.json"
    output.write_text(
        json.dumps(records, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info("Saved security master: %s", output)
    return output
