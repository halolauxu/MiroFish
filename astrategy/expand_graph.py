"""
Graph Expansion Script — AStrategy
===================================
Expands the local supply_chain.json graph from 96 edges to 2000+ edges by:

1. HOLDS_SHARES edges  — from fund quarterly holdings (stock_report_fund_hold)
2. COMPETES_WITH edges — same-industry peers in CSI800
3. CUSTOMER_OF edges   — from annual report "前五大客户" via LLM batch extraction
4. SUPPLIES_TO (new)   — LLM batch generation for top-100 companies

Adds:
- Institution nodes (fund companies)
- Betweenness + PageRank inputs for S07
- HOLDS_SHARES edges for institution_concentration factor in S07

Run:
    python astrategy/expand_graph.py

Output: astrategy/.data/local_graph/supply_chain.json (in-place expansion)
"""

from __future__ import annotations

import json
import logging
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import akshare as ak
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [expand_graph] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("expand_graph")

GRAPH_PATH = Path(__file__).parent / ".data" / "local_graph" / "supply_chain.json"
_6DIGIT = re.compile(r"^\d{6}$")


# ── helpers ─────────────────────────────────────────────────────────────────

def _is_stock_code(s: str) -> bool:
    return bool(_6DIGIT.match(str(s).strip()))


def _retry(fn, retries: int = 2, delay: float = 2.0):
    for i in range(retries + 1):
        try:
            return fn()
        except Exception as exc:
            if i == retries:
                raise
            logger.warning("Retry %d/%d for %s: %s", i + 1, retries, fn, exc)
            time.sleep(delay)


def load_graph() -> dict:
    with open(GRAPH_PATH, encoding="utf-8") as f:
        return json.load(f)


def save_graph(data: dict) -> None:
    GRAPH_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info("Saved graph: %d nodes, %d edges", len(data["nodes"]), len(data["edges"]))


# ── Step 1: Load CSI800 stock universe + industry map ───────────────────────

def build_universe() -> dict[str, str]:
    """Return {code: display_name} for all A-share stocks."""
    logger.info("Fetching A-share name list...")
    try:
        df = _retry(ak.stock_info_a_code_name)
        df.columns = ["code", "name"]
        return dict(zip(df["code"].astype(str).str.zfill(6), df["name"]))
    except Exception as e:
        logger.error("Failed to fetch stock names: %s", e)
        return {}


def build_csi800() -> list[str]:
    """Return CSI800 constituent codes."""
    try:
        df = ak.index_stock_cons_weight_csindex(symbol="000906")
        col = [c for c in df.columns if "成分" in c or "code" in c.lower() or "股票" in c][0]
        codes = df[col].astype(str).str.zfill(6).tolist()
        logger.info("CSI800 loaded: %d stocks", len(codes))
        return codes
    except Exception as e:
        logger.warning("CSI800 failed (%s), using fallback from graph nodes", e)
        return []


def build_industry_map(name_map: dict[str, str]) -> dict[str, str]:
    """Try to get industry classification from AkShare. Returns {code: industry}."""
    logger.info("Fetching industry classification...")
    industry_map: dict[str, str] = {}
    # Try east-money industry boards (may fail due to network)
    try:
        df = ak.stock_board_industry_name_em()
        for _, row in df.iterrows():
            board_name = row.get("板块名称", "")
            try:
                cons = ak.stock_board_industry_cons_em(symbol=board_name)
                code_col = [c for c in cons.columns if "代码" in c or "code" in c.lower()][0]
                for code in cons[code_col].astype(str).str.zfill(6):
                    industry_map[code] = board_name
            except Exception:
                pass
            time.sleep(0.3)
        logger.info("Industry map: %d stocks classified", len(industry_map))
    except Exception as e:
        logger.warning("Industry board fetch failed: %s — using node summaries", e)
    return industry_map


# ── Step 2: HOLDS_SHARES edges from fund holdings ───────────────────────────

def add_holds_shares_edges(data: dict, name_map: dict[str, str]) -> int:
    """
    Add Institution nodes + HOLDS_SHARES edges from fund quarterly holdings.
    Uses the aggregate fund-hold table (5224 rows from 20241231).
    Returns number of new edges added.
    """
    logger.info("Fetching fund holdings...")
    try:
        df = _retry(lambda: ak.stock_report_fund_hold(symbol="基金持仓", date="20241231"))
    except Exception as e:
        logger.error("fund_hold failed: %s", e)
        return 0

    # Column mapping (may vary by AkShare version)
    code_col = next((c for c in df.columns if "股票代码" in c or "代码" in c), None)
    name_col = next((c for c in df.columns if "股票名称" in c or "名称" in c), None)
    fund_col = next((c for c in df.columns if "基金" in c and "代码" not in c and "数量" not in c), None)
    hold_col = next((c for c in df.columns if "持股数量" in c or "持股" in c), None)

    if code_col is None:
        logger.warning("Cannot find stock-code column in fund_hold. Cols: %s", list(df.columns))
        return 0

    logger.info("fund_hold columns: %s", list(df.columns))

    nodes = data["nodes"]
    edges = data["edges"]
    facts = data["facts"]
    existing_facts = {e.get("fact", "") for e in edges}
    new_edge_count = 0

    # Group by fund (institution) → list of (stock_code, stock_name)
    # If fund_col is not identified, we create one generic Institution per batch
    fund_holdings: dict[str, list[tuple[str, str, float]]] = defaultdict(list)

    for _, row in df.iterrows():
        try:
            code = str(row.get(code_col, "")).strip().zfill(6)
            if not _is_stock_code(code):
                continue
            sname = str(row.get(name_col, "")) if name_col else name_map.get(code, code)
            fund_name = str(row.get(fund_col, "未知基金")) if fund_col else "基金持仓汇总"
            # Clean up fund name (remove trailing spaces, brackets etc.)
            fund_name = fund_name.strip()
            if not fund_name or fund_name in ("nan", "None", ""):
                fund_name = "基金汇总"

            hold_pct = 0.0
            if hold_col:
                try:
                    hold_pct = float(str(row.get(hold_col, 0)).replace("%", "") or 0)
                except Exception:
                    hold_pct = 0.0

            fund_holdings[fund_name].append((code, sname, hold_pct))
        except Exception:
            continue

    logger.info("Parsed %d distinct funds", len(fund_holdings))

    # Add Institution nodes + HOLDS_SHARES edges
    # Limit to top 200 funds (by number of holdings) to avoid bloat
    top_funds = sorted(fund_holdings.items(), key=lambda x: len(x[1]), reverse=True)[:200]

    for fund_name, holdings in top_funds:
        # Add Institution node (keyed by fund name)
        inst_key = f"inst::{fund_name}"
        if inst_key not in nodes:
            nodes[inst_key] = {
                "name": inst_key,
                "labels": ["Institution"],
                "summary": f"机构投资者: {fund_name}",
                "attributes": {"type": "fund", "display_name": fund_name},
                "display_name": fund_name,
            }

        # Add HOLDS_SHARES edges for each stock this fund holds
        # Also add Company node if not present
        for code, sname, hold_pct in holdings:
            if code not in nodes:
                display = name_map.get(code, sname)
                nodes[code] = {
                    "name": code,
                    "labels": ["Company"],
                    "summary": f"{display}({code})",
                    "attributes": {"code": code},
                    "display_name": display,
                }

            fact_str = f"{fund_name}持有{code}股票"
            if fact_str in existing_facts:
                continue

            edge = {
                "source": inst_key,
                "target": code,
                "source_name": inst_key,
                "target_name": code,
                "source_display": fund_name,
                "target_display": name_map.get(code, sname),
                "relation": "HOLDS_SHARES",
                "fact": fact_str,
                "weight": min(1.0, hold_pct / 5.0) if hold_pct > 0 else 0.5,
            }
            edges.append(edge)
            facts.append({
                "source": inst_key, "target": code,
                "relation": "HOLDS_SHARES", "fact": fact_str, "weight": edge["weight"]
            })
            existing_facts.add(fact_str)
            new_edge_count += 1

    logger.info("Added %d HOLDS_SHARES edges from %d funds", new_edge_count, len(top_funds))
    return new_edge_count


# ── Step 3: COMPETES_WITH edges from same-industry peers ────────────────────

def add_competes_with_edges(
    data: dict,
    name_map: dict[str, str],
    csi800: list[str],
) -> int:
    """
    Add COMPETES_WITH edges for stocks in the same SW-industry within CSI800.
    Uses the industry information already embedded in node summaries.
    """
    logger.info("Building COMPETES_WITH edges from node industries...")

    nodes = data["nodes"]
    edges = data["edges"]
    facts = data["facts"]
    existing_facts = {e.get("fact", "") for e in edges}

    # Build industry → [codes] from existing nodes
    industry_to_codes: dict[str, list[str]] = defaultdict(list)
    csi800_set = set(csi800)

    for key, node in nodes.items():
        if not _is_stock_code(key):
            continue
        if csi800_set and key not in csi800_set:
            continue
        attrs = node.get("attributes", {}) or {}
        industry = attrs.get("industry", "")
        if not industry or industry.lower() in ("none", "nan", ""):
            # Try to extract from summary
            summary = node.get("summary", "")
            m = re.search(r"[，,]?\s*(\S+?)行业", summary)
            if m:
                industry = m.group(1) + "行业"
        if industry:
            industry_to_codes[industry].append(key)

    logger.info("Industries with CSI800 stocks: %d", len(industry_to_codes))

    new_edge_count = 0
    for industry, codes in industry_to_codes.items():
        if len(codes) < 2:
            continue
        # Take top 10 (arbitrary order) and add pairwise COMPETES_WITH
        top = codes[:10]
        for i, c1 in enumerate(top):
            for c2 in top[i + 1:]:
                fact_str = f"{c1}与{c2}同属{industry}，存在竞争关系"
                if fact_str in existing_facts:
                    continue
                d1 = name_map.get(c1, c1)
                d2 = name_map.get(c2, c2)
                edge = {
                    "source": c1, "target": c2,
                    "source_name": c1, "target_name": c2,
                    "source_display": d1, "target_display": d2,
                    "relation": "COMPETES_WITH",
                    "fact": fact_str,
                    "weight": 0.6,
                }
                edges.append(edge)
                facts.append({
                    "source": c1, "target": c2,
                    "relation": "COMPETES_WITH", "fact": fact_str, "weight": 0.6
                })
                existing_facts.add(fact_str)
                new_edge_count += 1

    logger.info("Added %d COMPETES_WITH edges", new_edge_count)
    return new_edge_count


# ── Step 4: LLM batch — extract supply chain for top companies ──────────────

_SUPPLY_CHAIN_PROMPT = """\
你是A股供应链专家。请根据以下公司列表，列出这些公司之间已知的、确实存在的供应链和客户关系。

公司列表（股票代码: 公司名称）：
{company_list}

要求：
1. 只列出上述列表中公司之间的关系（不要加入列表外的公司）
2. 只列出你有较高把握（>80%）的关系
3. 每行一个关系，格式：源公司代码|目标公司代码|关系类型|说明
4. 关系类型只能是：SUPPLIES_TO（供应）、CUSTOMER_OF（客户）、COOPERATES_WITH（合作）
5. 说明要简洁（10字以内）

示例输出：
002460|300750|SUPPLIES_TO|供应锂矿原材料
300750|002594|SUPPLIES_TO|供应动力电池
600019|002594|SUPPLIES_TO|供应汽车钢板

请输出关系列表（如果不确定，宁可少输出也不要错误）：
"""


def add_llm_supply_chain_edges(
    data: dict,
    name_map: dict[str, str],
    csi800: list[str],
    llm_client: Any,
    batch_size: int = 30,
    max_batches: int = 20,
) -> int:
    """
    Use LLM to extract supply chain relations from top CSI800 companies.
    Processes companies in industry-grouped batches for coherence.
    """
    logger.info("Starting LLM supply chain extraction...")

    nodes = data["nodes"]
    edges = data["edges"]
    facts = data["facts"]
    existing_facts = {e.get("fact", "") for e in edges}

    # Build industry-grouped company batches
    industry_to_codes: dict[str, list[str]] = defaultdict(list)
    csi800_set = set(csi800) if csi800 else None

    for code, name in name_map.items():
        if csi800_set and code not in csi800_set:
            continue
        node = nodes.get(code, {})
        attrs = node.get("attributes", {}) or {}
        industry = attrs.get("industry", "")
        if not industry:
            summary = node.get("summary", "")
            m = re.search(r"[，,]?\s*(\S+?)行业", summary)
            if m:
                industry = m.group(1)
        industry_to_codes[industry or "未分类"].append(code)

    # Sort industries by size, take top industries
    sorted_industries = sorted(industry_to_codes.items(), key=lambda x: -len(x[1]))

    new_edge_count = 0
    batches_done = 0

    for industry, codes in sorted_industries:
        if batches_done >= max_batches:
            break
        if len(codes) < 3:
            continue

        # Take top companies from this industry
        sample = codes[:batch_size]
        company_list = "\n".join(
            f"{c}: {name_map.get(c, c)}" for c in sample
        )

        prompt = _SUPPLY_CHAIN_PROMPT.format(company_list=company_list)
        try:
            response = llm_client.chat(
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
            )
        except Exception as exc:
            logger.warning("LLM call failed for industry %s: %s", industry, exc)
            batches_done += 1
            continue

        # Parse response
        sample_set = set(sample)
        for line in response.strip().split("\n"):
            line = line.strip()
            if "|" not in line:
                continue
            parts = line.split("|")
            if len(parts) < 4:
                continue
            src, tgt, rel, desc = parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()

            # Validate
            if not _is_stock_code(src) or not _is_stock_code(tgt):
                continue
            if rel not in ("SUPPLIES_TO", "CUSTOMER_OF", "COOPERATES_WITH"):
                continue
            if src not in sample_set or tgt not in sample_set:
                continue
            if src == tgt:
                continue

            src_name = name_map.get(src, src)
            tgt_name = name_map.get(tgt, tgt)
            fact_str = f"{src_name}{desc}{tgt_name}"
            if fact_str in existing_facts:
                continue

            # Ensure nodes exist
            for code in (src, tgt):
                if code not in nodes:
                    display = name_map.get(code, code)
                    nodes[code] = {
                        "name": code, "labels": ["Company"],
                        "summary": f"{display}({code})",
                        "attributes": {"code": code},
                        "display_name": display,
                    }

            edge = {
                "source": src, "target": tgt,
                "source_name": src, "target_name": tgt,
                "source_display": src_name, "target_display": tgt_name,
                "relation": rel, "fact": fact_str, "weight": 0.8,
            }
            edges.append(edge)
            facts.append({
                "source": src, "target": tgt,
                "relation": rel, "fact": fact_str, "weight": 0.8,
            })
            existing_facts.add(fact_str)
            new_edge_count += 1

        logger.info(
            "Industry '%s' (%d codes): +%d edges (total so far: %d)",
            industry, len(codes), new_edge_count, len(edges)
        )
        batches_done += 1
        time.sleep(0.5)  # Rate limit

    logger.info("LLM extraction done: +%d edges from %d batches", new_edge_count, batches_done)
    return new_edge_count


# ── Step 5: Fix non-stock node contamination ────────────────────────────────

def remove_non_stock_nodes(data: dict) -> int:
    """
    Remove nodes whose key is NOT a 6-digit stock code AND is NOT an Institution.
    E.g. '公用事业行业', '电子行业' etc. that pollute PageRank.
    """
    nodes = data["nodes"]
    to_remove = []
    for key, node in nodes.items():
        labels = node.get("labels", [])
        if "Institution" in labels:
            continue  # Keep institution nodes
        if not _is_stock_code(key):
            # Non-stock-code, non-institution node
            to_remove.append(key)

    for key in to_remove:
        del nodes[key]

    logger.info("Removed %d non-stock-code nodes (e.g. industry name nodes)", len(to_remove))
    return len(to_remove)


# ── Step 6: Fix node display names ──────────────────────────────────────────

def fix_node_display_names(data: dict, name_map: dict[str, str]) -> int:
    """Fix nodes that still show 'Unknown(XXXXXX)' as display_name."""
    nodes = data["nodes"]
    fixed = 0
    for key, node in nodes.items():
        if not _is_stock_code(key):
            continue
        current_display = node.get("display_name", "")
        if "Unknown" in str(current_display) or not current_display:
            real_name = name_map.get(key, "")
            if real_name:
                node["display_name"] = real_name
                attrs = node.setdefault("attributes", {})
                attrs["display_name"] = real_name
                fixed += 1
    logger.info("Fixed display names for %d nodes", fixed)
    return fixed


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("AStrategy Graph Expansion")
    logger.info("=" * 60)

    # Load existing graph
    data = load_graph()
    initial_edges = len(data["edges"])
    logger.info("Initial graph: %d nodes, %d edges", len(data["nodes"]), initial_edges)

    # Build universe
    name_map = build_universe()
    csi800 = build_csi800()

    if not csi800:
        # Fallback: use all CSI800 codes from existing graph nodes
        csi800 = [k for k in data["nodes"] if _is_stock_code(k)]
        logger.info("Using %d codes from existing graph as universe", len(csi800))

    # Step 1: Fix display names
    fix_node_display_names(data, name_map)

    # Step 2: Remove non-stock node contamination
    remove_non_stock_nodes(data)

    # Step 3: Add HOLDS_SHARES from fund holdings
    n_holds = add_holds_shares_edges(data, name_map)

    # Step 4: Add COMPETES_WITH from industry peers
    n_competes = add_competes_with_edges(data, name_map, csi800)

    # Step 5: LLM batch supply chain extraction
    try:
        import os; sys.path.insert(0, str(Path(__file__).parent.parent))
        os.chdir(Path(__file__).parent.parent)
        from astrategy.llm import create_llm_client
        llm = create_llm_client(strategy_name="graph_expansion")
        n_llm = add_llm_supply_chain_edges(data, name_map, csi800, llm,
                                           batch_size=25, max_batches=15)
    except Exception as exc:
        logger.warning("LLM supply chain extraction skipped: %s", exc)
        n_llm = 0

    # Save
    final_edges = len(data["edges"])
    save_graph(data)

    logger.info("=" * 60)
    logger.info("Graph Expansion Complete")
    logger.info("  Initial edges: %d", initial_edges)
    logger.info("  +HOLDS_SHARES: %d", n_holds)
    logger.info("  +COMPETES_WITH: %d", n_competes)
    logger.info("  +LLM SUPPLIES_TO/CUSTOMER_OF: %d", n_llm)
    logger.info("  Final edges: %d (%.1fx growth)", final_edges, final_edges / max(initial_edges, 1))
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
