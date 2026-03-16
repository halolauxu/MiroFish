"""
Build A-share knowledge graph in Zep Cloud.

Populates the graph with CSI-300 company data, industry classifications,
and supply chain / cooperation relationships for S01 and S03 strategies.

Usage:
    python astrategy/build_graph.py [--graph-id supply_chain]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import akshare as ak
import pandas as pd

from astrategy.graph.local_store import LocalGraphStore
from astrategy.data_collector.em_fallback import (
    INDUSTRY_REPRESENTATIVES,
    get_stock_name_map,
    lookup_stock_industry,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("build_graph")

# ── CSI-300 constituents ──────────────────────────────────────────

def fetch_csi300_codes() -> list[str]:
    """Fetch CSI-300 constituent stock codes."""
    try:
        df = ak.index_stock_cons_csindex(symbol="000300")
        codes = df["成分券代码"].tolist()
        logger.info("Fetched %d CSI-300 constituents from CSIndex", len(codes))
        return codes
    except Exception as e:
        logger.warning("Failed to fetch CSI-300 from CSIndex: %s", e)
        # Fallback: use representative stocks from em_fallback
        codes = []
        for stocks in INDUSTRY_REPRESENTATIVES.values():
            codes.extend(stocks)
        codes = list(set(codes))
        logger.info("Using %d fallback representative stocks", len(codes))
        return codes


# ── Supply chain relationships (domain knowledge) ────────────────

# Key supply chain relationships among CSI-300 companies
# Format: (upstream_code, downstream_code, relation_type, description)
SUPPLY_CHAIN_EDGES = [
    # 锂电池产业链
    ("002460", "300750", "SUPPLIES_TO", "赣锋锂业向宁德时代供应锂矿原材料"),
    ("300750", "002594", "SUPPLIES_TO", "宁德时代向比亚迪供应动力电池"),
    ("300750", "600418", "SUPPLIES_TO", "宁德时代向江淮汽车供应动力电池"),
    ("002460", "002594", "SUPPLIES_TO", "赣锋锂业向比亚迪供应锂原材料"),

    # 光伏产业链
    ("601012", "300274", "SUPPLIES_TO", "隆基绿能向阳光电源供应光伏组件"),
    ("600438", "601012", "SUPPLIES_TO", "通威股份向隆基绿能供应硅料"),

    # 半导体产业链
    ("688981", "002415", "SUPPLIES_TO", "中芯国际向海康威视供应芯片代工"),
    ("688981", "000725", "SUPPLIES_TO", "中芯国际向京东方供应驱动芯片"),

    # 白酒产业链 (竞争关系)
    ("600519", "000858", "COMPETES_WITH", "贵州茅台与五粮液同为高端白酒龙头"),
    ("600519", "000568", "COMPETES_WITH", "贵州茅台与泸州老窖同为浓香白酒"),
    ("000858", "000568", "COMPETES_WITH", "五粮液与泸州老窖同为浓香白酒"),

    # 银行业 (合作关系)
    ("601398", "601939", "COOPERATES_WITH", "工商银行与建设银行银团贷款合作"),
    ("600036", "601318", "COOPERATES_WITH", "招商银行与中国平安交叉销售合作"),

    # 保险与银行
    ("601318", "601628", "COMPETES_WITH", "中国平安与中国人寿保险业务竞争"),

    # 券商
    ("600030", "601211", "COMPETES_WITH", "中信证券与国泰君安头部券商竞争"),

    # 新能源汽车产业链
    ("002594", "002230", "COOPERATES_WITH", "比亚迪与科大讯飞智能驾驶合作"),

    # 钢铁/建筑产业链
    ("600019", "601390", "SUPPLIES_TO", "宝钢股份向中国中铁供应钢材"),
    ("600019", "601668", "SUPPLIES_TO", "宝钢股份向中国建筑供应钢材"),

    # 能源产业链
    ("601088", "600900", "COOPERATES_WITH", "中国神华与长江电力能源合作"),
    ("601857", "600028", "COOPERATES_WITH", "中国石油与中国石化油气管网共享"),

    # 通信产业链
    ("601728", "600050", "COOPERATES_WITH", "中国电信与中国联通5G共建共享"),
    ("000063", "601728", "SUPPLIES_TO", "中兴通讯向中国电信供应通信设备"),

    # 家电产业链
    ("000333", "000651", "COMPETES_WITH", "美的集团与格力电器空调业务竞争"),

    # 医药产业链
    ("600276", "000538", "COMPETES_WITH", "恒瑞医药与云南白药创新药竞争"),

    # 电力设备
    ("601877", "601669", "SUPPLIES_TO", "正泰电器向中国电建供应电气设备"),

    # 地产与建材
    ("000002", "600585", "COOPERATES_WITH", "万科与海螺水泥建材供应合作"),

    # 食品饮料
    ("600887", "603288", "COMPETES_WITH", "伊利股份与海天味业消费品龙头"),

    # 汽车零部件
    ("600741", "002594", "SUPPLIES_TO", "华域汽车向比亚迪供应汽车零部件"),
    ("600741", "600104", "SUPPLIES_TO", "华域汽车向上汽集团供应零部件"),

    # AI/科技产业链
    ("002230", "688111", "COOPERATES_WITH", "科大讯飞与金山办公AI办公合作"),

    # 军工产业链
    ("600760", "600893", "COOPERATES_WITH", "中航沈飞与航发动力军机配套"),
]

# ── Industry chain relationships ─────────────────────────────────

INDUSTRY_CHAIN = [
    # 上游 → 下游 行业传导链
    ("采掘", "有色金属", "SUPPLIES_TO", "矿产资源向有色金属冶炼供应"),
    ("有色金属", "电子", "SUPPLIES_TO", "有色金属向电子行业供应原材料"),
    ("有色金属", "电气设备", "SUPPLIES_TO", "铜铝等向电气设备行业供应"),
    ("化工", "农林牧渔", "SUPPLIES_TO", "化肥农药向农业供应"),
    ("化工", "纺织服装", "SUPPLIES_TO", "化纤原料向纺织行业供应"),
    ("钢铁", "建筑装饰", "SUPPLIES_TO", "钢材向建筑行业供应"),
    ("钢铁", "机械设备", "SUPPLIES_TO", "钢材向机械制造供应"),
    ("钢铁", "汽车", "SUPPLIES_TO", "钢材向汽车制造供应"),
    ("电子", "通信", "SUPPLIES_TO", "电子元器件向通信设备供应"),
    ("电子", "计算机", "SUPPLIES_TO", "芯片向计算机行业供应"),
    ("电气设备", "公用事业", "SUPPLIES_TO", "发电/输配电设备向电力行业供应"),
    ("汽车", "交通运输", "COOPERATES_WITH", "汽车制造与交通运输协同"),
    ("食品饮料", "商业贸易", "COOPERATES_WITH", "食品饮料与零售渠道合作"),
    ("医药生物", "商业贸易", "COOPERATES_WITH", "医药与医药流通合作"),
    ("银行", "非银金融", "COOPERATES_WITH", "银行与券商/保险业务协同"),
    ("房地产", "建筑材料", "COOPERATES_WITH", "地产与建材供需关联"),
    ("房地产", "家用电器", "COOPERATES_WITH", "地产与家电后周期关联"),
]


def build_company_data(codes: list[str]) -> list[dict]:
    """Build company entity data from stock codes."""
    name_map = get_stock_name_map()
    companies = []
    for code in codes:
        name = name_map.get(code, f"Unknown({code})")
        industry = lookup_stock_industry(code)
        companies.append({
            "code": code,
            "name": name,
            "industry": industry,
        })
    return companies


def build_industry_episodes() -> list[str]:
    """Build industry entity episodes."""
    episodes = []
    for industry, stocks in INDUSTRY_REPRESENTATIVES.items():
        names = get_stock_name_map()
        stock_names = [f"{s}({names.get(s, '?')})" for s in stocks]
        episode = (
            f"[行业] {industry} 是申万一级行业分类。"
            f"代表性成分股包括: {', '.join(stock_names)}。"
        )
        episodes.append(episode)
    return episodes


def build_supply_chain_episodes() -> list[str]:
    """Build supply chain relationship episodes."""
    names = get_stock_name_map()
    episodes = []

    for src, tgt, rel, desc in SUPPLY_CHAIN_EDGES:
        src_name = names.get(src, src)
        tgt_name = names.get(tgt, tgt)
        if rel == "SUPPLIES_TO":
            ep = f"{src_name}({src}) 是 {tgt_name}({tgt}) 的上游供应商。{desc}"
        elif rel == "COMPETES_WITH":
            ep = f"{src_name}({src}) 与 {tgt_name}({tgt}) 是竞争对手。{desc}"
        elif rel == "COOPERATES_WITH":
            ep = f"{src_name}({src}) 与 {tgt_name}({tgt}) 存在合作关系。{desc}"
        else:
            ep = f"{src_name}({src}) {rel} {tgt_name}({tgt})。{desc}"
        episodes.append(ep)

    return episodes


def build_industry_chain_episodes() -> list[str]:
    """Build industry chain relationship episodes."""
    episodes = []
    for src_ind, tgt_ind, rel, desc in INDUSTRY_CHAIN:
        if rel == "SUPPLIES_TO":
            ep = f"[产业链] {src_ind}行业是{tgt_ind}行业的上游。{desc}"
        else:
            ep = f"[产业链] {src_ind}行业与{tgt_ind}行业存在协同关系。{desc}"
        episodes.append(ep)
    return episodes


def main():
    parser = argparse.ArgumentParser(description="Build A-share knowledge graph")
    parser.add_argument("--graph-id", default="supply_chain",
                        help="Graph ID (Zep user_id), default: supply_chain")
    parser.add_argument("--skip-companies", action="store_true",
                        help="Skip company data ingestion")
    args = parser.parse_args()

    graph_id = args.graph_id
    store = LocalGraphStore()

    # ── Step 1: Create graph ──────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Step 1: Creating local graph '%s' ...", graph_id)
    store.create_graph(graph_id)

    # ── Step 2: Add company entities ──────────────────────────────
    if not args.skip_companies:
        logger.info("=" * 60)
        logger.info("Step 2: Adding company entities ...")
        codes = fetch_csi300_codes()
        companies = build_company_data(codes)
        store.add_companies(graph_id, companies)
        logger.info("Added %d companies.", len(companies))
    else:
        logger.info("Step 2: Skipped (--skip-companies)")

    # ── Step 3: Add industry episodes ─────────────────────────────
    logger.info("=" * 60)
    logger.info("Step 3: Adding industry classifications ...")
    industry_eps = build_industry_episodes()
    store.add_episodes(graph_id, industry_eps)
    logger.info("Added %d industry episodes.", len(industry_eps))

    # ── Step 4: Add supply chain relationships ────────────────────
    logger.info("=" * 60)
    logger.info("Step 4: Adding supply chain relationships ...")
    names = get_stock_name_map()
    edges = []
    for src, tgt, rel, desc in SUPPLY_CHAIN_EDGES:
        edges.append({
            "source": names.get(src, src),
            "target": names.get(tgt, tgt),
            "relation": rel,
            "description": desc,
        })
    store.add_relationships(graph_id, edges)
    # Also add as episodes for text search
    sc_eps = build_supply_chain_episodes()
    store.add_episodes(graph_id, sc_eps)
    logger.info("Added %d supply chain relationships.", len(edges))

    # ── Step 5: Add industry chain relationships ──────────────────
    logger.info("=" * 60)
    logger.info("Step 5: Adding industry chain relationships ...")
    ind_edges = []
    for src_ind, tgt_ind, rel, desc in INDUSTRY_CHAIN:
        ind_edges.append({
            "source": src_ind + "行业",
            "target": tgt_ind + "行业",
            "relation": rel,
            "description": desc,
        })
    store.add_relationships(graph_id, ind_edges)
    ic_eps = build_industry_chain_episodes()
    store.add_episodes(graph_id, ic_eps)
    logger.info("Added %d industry chain relationships.", len(ind_edges))

    # ── Step 6: Save & verify ─────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Step 6: Saving and verifying ...")
    save_path = store.save(graph_id)

    nodes = store.get_all_nodes(graph_id)
    edges = store.get_all_edges(graph_id)
    logger.info("Graph '%s': %d nodes, %d edges", graph_id, len(nodes), len(edges))

    # Sample searches
    for query in ["宁德时代 供应链", "比亚迪 上游", "白酒 竞争"]:
        results = store.search(graph_id, query, limit=3)
        logger.info("Search '%s': %d results", query, len(results))
        for r in results[:2]:
            logger.info("  [%.2f] %s → %s: %s",
                         r["score"], r["source"], r["target"],
                         r["fact"][:80] if r["fact"] else "")

    logger.info("=" * 60)
    logger.info("Graph construction complete!")
    logger.info("Saved to: %s", save_path)
    logger.info("Graph ID: %s", graph_id)
    logger.info("Nodes: %d, Edges: %d", len(nodes), len(edges))


if __name__ == "__main__":
    main()
