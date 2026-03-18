"""
AStrategy API Blueprint — A股量化策略系统 API

提供信号、事件、知识图谱、组合、回测、系统状态等接口。
所有数据从 astrategy/.data/ 目录读取真实文件。
"""

import json
import logging
import re
import traceback
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Blueprint, jsonify, request

logger = logging.getLogger("mirofish.api.astrategy")

astrategy_bp = Blueprint("astrategy", __name__, url_prefix="/api/astrategy")

# ── 路径常量 ──────────────────────────────────────────────────────

# 项目根目录 (MiroFish/)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_ASTRATEGY_DIR = _PROJECT_ROOT / "astrategy"
_DATA_DIR = _ASTRATEGY_DIR / ".data"
_SIGNALS_DIR = _DATA_DIR / "signals"
_REPORTS_DIR = _DATA_DIR / "reports"
_EVENTS_FILE = _DATA_DIR / "historical_events.json"
_GRAPH_FILE = _DATA_DIR / "local_graph" / "supply_chain.json"

# ── 策略名中文映射 ────────────────────────────────────────────────
_STRATEGY_NAMES_CN = {
    "sector_rotation": "行业轮动",
    "institution_association": "机构关联",
    "graph_factors": "图谱因子",
    "sentiment_simulation": "舆情模拟",
    "analyst_divergence": "分析师分歧",
    "narrative_tracker": "叙事追踪",
    "prosperity_transmission": "景气传导",
    "shock_propagation": "冲击链路",
    "shock_pipeline": "冲击链路",
}

# ── 工具函数 ──────────────────────────────────────────────────────

def _ok(data: Any, **kwargs) -> Tuple[dict, int]:
    """成功响应"""
    resp = {"success": True, "data": data}
    resp.update(kwargs)
    return jsonify(resp), 200


def _err(message: str, status: int = 400) -> Tuple[dict, int]:
    """错误响应"""
    return jsonify({"success": False, "error": message}), status


def _read_json(path: Path) -> Any:
    """安全读取 JSON 文件"""
    if not path.exists():
        raise FileNotFoundError(f"文件不存在: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_company_from_summary(summary: str) -> Tuple[str, str]:
    """从 summary 字段解析公司名和代码。
    格式: '贵州茅台(600519)' 或 '贵州茅台(600519), 白酒行业'
    """
    m = re.match(r"(.+?)\((\d{6})\)", summary or "")
    if m:
        return m.group(1).strip(), m.group(2)
    return "", ""


def _get_latest_date_in_dir(strategy_dir: Path) -> Optional[str]:
    """获取策略目录中最新的日期文件 (YYYYMMDD.json)"""
    date_files = sorted(
        [f for f in strategy_dir.glob("*.json") if re.match(r"^\d{8}\.json$", f.name)],
        key=lambda f: f.name,
        reverse=True,
    )
    return date_files[0].stem if date_files else None


def _enrich_signal_fields(sig: dict) -> None:
    """为信号补充前端需要的顶层字段 (source, divergence, alpha_type 等)"""
    strategy = sig.get("strategy_name", "")
    sig["source"] = strategy
    sig["source_cn"] = _STRATEGY_NAMES_CN.get(strategy, strategy)
    meta = sig.get("metadata", {}) or {}
    if "divergence" not in sig:
        sig["divergence"] = meta.get("divergence", None)
    if "alpha_type" not in sig:
        sig["alpha_type"] = meta.get("alpha_type", None)
    if "event_title" not in sig:
        sig["event_title"] = meta.get("event_title", None)
    if "display_name" not in sig:
        sig["display_name"] = sig.get("stock_name", "") or sig.get("stock_code", "")


def _load_strategy_signals(strategy_dir: Path, date_str: Optional[str] = None) -> List[dict]:
    """加载指定策略目录的信号，默认取最新日期"""
    if not strategy_dir.is_dir():
        return []
    target_date = date_str or _get_latest_date_in_dir(strategy_dir)
    if not target_date:
        return []
    signal_file = strategy_dir / f"{target_date}.json"
    if not signal_file.exists():
        return []
    try:
        data = _read_json(signal_file)
        signals = data if isinstance(data, list) else [data]
        # 注入策略来源和信号ID
        for i, sig in enumerate(signals):
            if "signal_id" not in sig:
                code = sig.get("stock_code", "")
                sname = sig.get("strategy_name", strategy_dir.name)
                sig["signal_id"] = f"{sname}_{target_date}_{code}_{i}"
            if "strategy_name" not in sig:
                sig["strategy_name"] = strategy_dir.name
            if "signal_date" not in sig:
                sig["signal_date"] = f"{target_date[:4]}-{target_date[4:6]}-{target_date[6:8]}"
            _enrich_signal_fields(sig)
        return signals
    except Exception as e:
        logger.warning("读取信号文件失败 %s: %s", signal_file, e)
        return []


def _load_all_latest_signals() -> List[dict]:
    """扫描所有策略子目录 + 冲击链路信号，加载最新信号"""
    all_signals: List[dict] = []
    if _SIGNALS_DIR.is_dir():
        for strategy_dir in sorted(_SIGNALS_DIR.iterdir()):
            if strategy_dir.is_dir():
                signals = _load_strategy_signals(strategy_dir)
                all_signals.extend(signals)

    # 同时加载冲击链路信号（核心 Alpha 来源）
    shock_signals = _load_shock_signals()
    for i, sig in enumerate(shock_signals):
        target_code = sig.get("target_code", "")
        source_code = sig.get("source_code", "")
        # 生成稳定的 signal_id
        if not sig.get("signal_id"):
            sig["signal_id"] = f"shock_{source_code}_{target_code}_{i}"
        # 强制设定前端需要的标准字段（不用 setdefault，因为值可能为 None）
        sig["strategy_name"] = "shock_propagation"
        sig["stock_code"] = target_code
        sig["stock_name"] = sig.get("target_name") or target_code
        sig["direction"] = sig.get("signal_direction") or "neutral"
        sig["confidence"] = sig.get("confidence") or 0.0
        sig["expected_return"] = sig.get("return_5d") or 0.0
        sig["holding_period_days"] = 10
        sig["divergence"] = sig.get("divergence") or 0.0
        sig["alpha_type"] = sig.get("alpha_type") or ""
        if not sig.get("reasoning"):
            sig["reasoning"] = (
                f"[冲击传播] {sig.get('source_name', '')}({source_code})"
                f" → {sig.get('propagation_path', '')}"
                f" | 冲击权重:{sig.get('shock_weight', 0):.2f}"
                f" | Agent共识:{sig.get('consensus_direction', '')}"
                f" | 分歧度:{sig.get('divergence', 0):.2f}"
                f" | {sig.get('alpha_type', '')}"
            )
        sig["signal_date"] = sig.get("event_date") or datetime.now().strftime("%Y-%m-%d")
        # 前端关键字段
        sig["source"] = "shock_propagation"
        sig["source_cn"] = "冲击链路"
        sig["display_name"] = sig.get("target_name") or target_code
        all_signals.append(sig)

    # ── 信号冲突检测与解决 ──────────────────────────────────────────
    _STRATEGY_WEIGHTS = {
        "shock_propagation": 3.0,
        "shock_pipeline": 3.0,
        "graph_factors": 2.0,
        "institution_association": 1.5,
        "institution": 1.5,
        "sentiment_simulation": 1.0,
        "sector_rotation": 1.0,
    }
    _DEFAULT_WEIGHT = 0.5

    # 按 stock_code 分组
    by_stock: Dict[str, List[dict]] = defaultdict(list)
    for sig in all_signals:
        code = sig.get("stock_code", "")
        if code:
            by_stock[code].append(sig)

    for code, sigs in by_stock.items():
        directions = {s.get("direction", "neutral") for s in sigs}
        # 只有同时存在 long 和 avoid 才算冲突
        has_conflict = "long" in directions and "avoid" in directions

        if not has_conflict:
            # 无冲突 — 标记即可
            for s in sigs:
                s["conflict"] = {
                    "has_conflict": False,
                    "final_direction": s.get("direction", "neutral"),
                    "vote_long": 0.0,
                    "vote_avoid": 0.0,
                    "strategies_agree": [],
                    "strategies_disagree": [],
                }
            continue

        # 有冲突 — 加权投票
        vote_long = 0.0
        vote_avoid = 0.0
        strats_long: List[str] = []
        strats_avoid: List[str] = []

        for s in sigs:
            sname = s.get("strategy_name", "")
            w = _STRATEGY_WEIGHTS.get(sname, _DEFAULT_WEIGHT)
            d = s.get("direction", "neutral")
            cn = _STRATEGY_NAMES_CN.get(sname, sname)
            if d == "long":
                vote_long += w
                strats_long.append(cn)
            elif d == "avoid":
                vote_avoid += w
                strats_avoid.append(cn)

        final_direction = "long" if vote_long >= vote_avoid else "avoid"
        agree = strats_long if final_direction == "long" else strats_avoid
        disagree = strats_avoid if final_direction == "long" else strats_long

        for s in sigs:
            s["conflict"] = {
                "has_conflict": True,
                "final_direction": final_direction,
                "vote_long": round(vote_long, 2),
                "vote_avoid": round(vote_avoid, 2),
                "strategies_agree": agree,
                "strategies_disagree": disagree,
            }

    # 按 confidence 降序排列
    all_signals.sort(key=lambda s: s.get("confidence", 0), reverse=True)
    return all_signals


def _load_shock_signals() -> List[dict]:
    """加载冲击链路信号（从 reports 目录）"""
    # 找最新的 shock_signals 或 shock_backtest_signals 文件
    candidates = sorted(
        [f for f in _REPORTS_DIR.glob("shock*signals*.json")],
        key=lambda f: f.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return []
    try:
        data = _read_json(candidates[0])
        return data if isinstance(data, list) else [data]
    except Exception as e:
        logger.warning("读取冲击信号失败: %s", e)
        return []


def _load_graph_data() -> dict:
    """加载知识图谱数据（带缓存）"""
    if not hasattr(_load_graph_data, "_cache"):
        _load_graph_data._cache = None
        _load_graph_data._mtime = 0

    if _GRAPH_FILE.exists():
        mtime = _GRAPH_FILE.stat().st_mtime
        if _load_graph_data._cache is None or mtime > _load_graph_data._mtime:
            _load_graph_data._cache = _read_json(_GRAPH_FILE)
            _load_graph_data._mtime = mtime

    return _load_graph_data._cache or {"nodes": {}, "edges": []}


# ══════════════════════════════════════════════════════════════════
#  信号 (Signals)
# ══════════════════════════════════════════════════════════════════

@astrategy_bp.route("/signals/active")
def signals_active():
    """返回所有策略最新日期的活跃信号，按 confidence 降序"""
    try:
        signals = _load_all_latest_signals()
        return _ok(signals, total=len(signals))
    except Exception as e:
        logger.error("获取活跃信号失败: %s\n%s", e, traceback.format_exc())
        return _err(f"获取活跃信号失败: {e}", 500)


@astrategy_bp.route("/signals")
def signals_list():
    """带过滤和分页的信号列表

    Query params:
      - strategy: 策略名过滤
      - direction: 方向过滤 (long/short/avoid)
      - min_confidence: 最低置信度
      - page: 页码 (从1开始)
      - limit: 每页条数 (默认20)
    """
    try:
        signals = _load_all_latest_signals()

        # 过滤
        strategy = request.args.get("strategy")
        direction = request.args.get("direction")
        min_conf = request.args.get("min_confidence", type=float)

        if strategy:
            signals = [s for s in signals if s.get("strategy_name") == strategy]
        if direction:
            signals = [s for s in signals if s.get("direction") == direction]
        if min_conf is not None:
            signals = [s for s in signals if s.get("confidence", 0) >= min_conf]

        total = len(signals)

        # 分页
        page = request.args.get("page", 1, type=int)
        limit = request.args.get("limit", 20, type=int)
        limit = min(limit, 200)  # 防止一次请求过大
        start = (page - 1) * limit
        paged = signals[start : start + limit]

        return _ok(paged, total=total, page=page, limit=limit,
                   total_pages=(total + limit - 1) // limit if limit else 1)
    except Exception as e:
        logger.error("获取信号列表失败: %s\n%s", e, traceback.format_exc())
        return _err(f"获取信号列表失败: {e}", 500)


@astrategy_bp.route("/signals/<signal_id>/trace")
def signal_trace(signal_id: str):
    """返回冲击链路信号的完整传播追踪。

    对于 shock pipeline 信号，返回:
    event → propagation_path → debate_summary → signal details
    """
    try:
        shock_signals = _load_shock_signals()

        # 在冲击信号中搜索匹配
        # signal_id 可能是 target_code 或 event_id_target_code
        matched = None
        for sig in shock_signals:
            target_code = sig.get("target_code", "")
            event_id = sig.get("event_id", "")
            # 支持多种匹配方式
            if signal_id in (
                target_code,
                f"{event_id}_{target_code}",
                sig.get("signal_id", ""),
            ):
                matched = sig
                break

        # 也在普通策略信号中查找
        if not matched:
            all_signals = _load_all_latest_signals()
            for sig in all_signals:
                if sig.get("signal_id") == signal_id:
                    matched = sig
                    break

        if not matched:
            return _err(f"未找到信号: {signal_id}", 404)

        # 构建 trace 数据 — 统一返回 event/propagation/debate/signal 四段结构
        is_shock = "source_event" in matched

        if is_shock:
            # 冲击链路信号: 从 shock signal 字段构建完整链路
            # 解析 propagation_path 为数组
            path_str = matched.get("propagation_path", "")
            path_codes = [c.strip() for c in path_str.split("→")] if path_str else []

            # 解析 relation_chain 为数组
            rel_str = matched.get("relation_chain", "")
            if rel_str:
                # relation_chain 可能是 "COMPETES_WITH" 或 "COMPETES_WITH → COOPERATES_WITH"
                if "→" in rel_str:
                    relations = [r.strip() for r in rel_str.split("→")]
                elif "," in rel_str:
                    relations = [r.strip() for r in rel_str.split(",")]
                else:
                    relations = [rel_str.strip()]
            else:
                relations = []

            # 解析 debate_summary 为 agents 列表
            debate_agents = []
            debate_text = matched.get("debate_summary", "") or ""
            if debate_text:
                # 尝试 JSON 格式
                try:
                    parsed = json.loads(debate_text) if debate_text.strip().startswith("[") else None
                    if isinstance(parsed, list):
                        debate_agents = parsed
                except (json.JSONDecodeError, TypeError):
                    pass

                # 纯文本格式: [游资/短线客] sell(-0.80): 理由...
                if not debate_agents and "[" in debate_text:
                    import re
                    for line in debate_text.split("\n"):
                        line = line.strip()
                        if not line:
                            continue
                        m = re.match(
                            r"\[(.+?)\]\s*(\w+)\(([-+]?\d*\.?\d+)\):\s*(.*)",
                            line,
                        )
                        if m:
                            archetype, action, sentiment, reasoning = m.groups()
                            debate_agents.append({
                                "archetype": archetype,
                                "action": action,
                                "sentiment": float(sentiment),
                                "reasoning": reasoning,
                            })

            trace = {
                "signal_id": signal_id,
                "event": {
                    "title": matched.get("source_event", ""),
                    "type": matched.get("event_type", ""),
                    "date": matched.get("event_date", ""),
                    "summary": matched.get("source_event", ""),
                    "stock_code": matched.get("source_code", ""),
                    "stock_name": matched.get("source_name", ""),
                    "event_id": matched.get("event_id", ""),
                },
                "propagation": {
                    "path": path_codes,
                    "relations": relations,
                    "shock_weight": matched.get("shock_weight"),
                    "hop": matched.get("hop"),
                    "decay": matched.get("shock_weight"),
                },
                "debate": {
                    "agents": debate_agents,
                    "divergence": matched.get("divergence"),
                    "consensus_direction": matched.get("consensus_direction"),
                    "consensus_sentiment": matched.get("consensus_sentiment"),
                    "conviction": matched.get("conviction"),
                    "debate_summary": debate_text,
                },
                "signal": {
                    "direction": matched.get("signal_direction", ""),
                    "confidence": matched.get("confidence"),
                    "expected_return": matched.get("return_5d") or 0.0,
                    "alpha_type": matched.get("alpha_type"),
                    "reacted": matched.get("reacted"),
                    "return_5d": matched.get("return_5d") or 0.0,
                    "target_code": matched.get("target_code"),
                    "target_name": matched.get("target_name"),
                    "position_hint": matched.get("position_hint"),
                    "reasoning": matched.get("reasoning", ""),
                    "forward_returns": {
                        "1d": matched.get("fwd_return_1d") or matched.get("return_5d") or 0.0,
                        "3d": matched.get("fwd_return_3d") or matched.get("return_5d") or 0.0,
                        "5d": matched.get("fwd_return_5d") or matched.get("return_5d") or 0.0,
                        "10d": matched.get("fwd_return_10d") or matched.get("return_5d") or 0.0,
                        "20d": matched.get("fwd_return_20d") or matched.get("return_5d") or 0.0,
                    },
                    "volume_change_5d": matched.get("volume_change_5d") or 0.0,
                },
            }
        else:
            # 普通策略信号: 构建简化 trace
            meta = matched.get("metadata", {}) or {}
            trace = {
                "signal_id": signal_id,
                "event": {
                    "title": meta.get("event_title") or meta.get("catalyst") or "",
                    "type": matched.get("strategy_name", "signal"),
                    "date": matched.get("signal_date", ""),
                    "summary": meta.get("event_title") or matched.get("reasoning", ""),
                    "stock_code": matched.get("stock_code", ""),
                    "stock_name": matched.get("stock_name", ""),
                    "strategy_display_name": meta.get("strategy_display_name", ""),
                },
                "propagation": None,
                "debate": None,
                "signal": {
                    "direction": matched.get("direction", ""),
                    "confidence": matched.get("confidence"),
                    "expected_return": matched.get("expected_return"),
                    "reasoning": matched.get("reasoning", ""),
                    "strategy_name": matched.get("strategy_name", ""),
                    "strategy_display_name": meta.get("strategy_display_name", ""),
                    # 完整传递 metadata 供前端渲染
                    "metadata": meta,
                    # 因子分解（S07）
                    "factors": meta.get("factors"),
                    # 同行对比（S02）
                    "peer_detail": meta.get("peer_detail"),
                    # 置信度分解（通用）
                    "confidence_breakdown": meta.get("confidence_breakdown"),
                    # 行业轮动因子（S08）
                    "rotation_factors": meta.get("rotation_factors"),
                    # 排名信息
                    "rank": meta.get("rank"),
                    "total_stocks": meta.get("total_stocks"),
                    "composite_score": meta.get("composite_score"),
                    "alpha_type": meta.get("alpha_type"),
                    "reacted": None,
                    "return_5d": None,
                    "forward_returns": None,
                },
            }

        return _ok(trace)
    except Exception as e:
        logger.error("获取信号追踪失败: %s\n%s", e, traceback.format_exc())
        return _err(f"获取信号追踪失败: {e}", 500)


# ══════════════════════════════════════════════════════════════════
#  事件 (Events)
# ══════════════════════════════════════════════════════════════════

@astrategy_bp.route("/events/history")
def events_history():
    """返回历史事件数据库"""
    try:
        events = _read_json(_EVENTS_FILE)
        return _ok(events, total=len(events))
    except FileNotFoundError:
        return _err("历史事件文件不存在", 404)
    except Exception as e:
        logger.error("获取历史事件失败: %s\n%s", e, traceback.format_exc())
        return _err(f"获取历史事件失败: {e}", 500)


@astrategy_bp.route("/events/live")
def events_live():
    """从最新信号文件中提取近期事件（去重）"""
    try:
        events_seen = set()
        live_events: List[dict] = []

        # 从冲击信号中提取事件 — 先按 event_id 分组收集受影响股票和信号ID
        shock_signals = _load_shock_signals()
        event_stocks: Dict[str, List[dict]] = defaultdict(list)
        event_signal_ids: Dict[str, List[str]] = defaultdict(list)
        for sig in shock_signals:
            eid = sig.get("event_id", "")
            if eid:
                event_stocks[eid].append({
                    "code": sig.get("target_code", ""),
                    "name": sig.get("target_name", ""),
                })
                sid = sig.get("signal_id", "")
                if sid:
                    event_signal_ids[eid].append(sid)

        for sig in shock_signals:
            event_id = sig.get("event_id", "")
            if event_id and event_id not in events_seen:
                events_seen.add(event_id)
                affected = event_stocks.get(event_id, [])
                sig_count = len(affected)
                # 根据影响股票数量和事件类型判断 impact_level
                etype = sig.get("event_type", "")
                if sig_count >= 5 or etype in ("scandal", "policy_risk"):
                    impact_level = "high"
                elif sig_count >= 2:
                    impact_level = "medium"
                else:
                    impact_level = "low"
                live_events.append({
                    "event_id": event_id,
                    "title": sig.get("source_event", ""),
                    "type": etype,
                    "stock_code": sig.get("source_code", ""),
                    "stock_name": sig.get("source_name", ""),
                    "event_date": sig.get("event_date", ""),
                    "signal_count": sig_count,
                    "signal_ids": event_signal_ids.get(event_id, []),
                    "source": "shock_pipeline",
                    "affected_stocks": affected,
                    "impact_level": impact_level,
                })

        # 从策略信号的 metadata 中提取事件标题（如有）
        all_signals = _load_all_latest_signals()
        for sig in all_signals:
            meta = sig.get("metadata", {})
            event_title = meta.get("event_title") or meta.get("catalyst") or ""
            if event_title and event_title not in events_seen:
                events_seen.add(event_title)
                live_events.append({
                    "event_id": f"live_{sig.get('stock_code', '')}",
                    "title": event_title,
                    "type": meta.get("event_type", "signal"),
                    "stock_code": sig.get("stock_code", ""),
                    "stock_name": sig.get("stock_name", ""),
                    "event_date": sig.get("signal_date", ""),
                    "signal_count": 1,
                    "signal_ids": [sig.get("signal_id", "")],
                    "source": sig.get("strategy_name", ""),
                    "affected_stocks": [{
                        "code": sig.get("stock_code", ""),
                        "name": sig.get("stock_name", ""),
                    }],
                    "impact_level": "low",
                })

        # 按日期降序
        live_events.sort(key=lambda e: e.get("event_date", ""), reverse=True)
        return _ok({
            "events": live_events,
            "data_source": {
                "source": "东方财富个股新闻(akshare stock_news_em) + 市场热点",
                "frequency": "每次触发时实时抓取",
                "last_scan": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "coverage": "CSI800 成分股",
            },
        }, total=len(live_events))
    except Exception as e:
        logger.error("获取实时事件失败: %s\n%s", e, traceback.format_exc())
        return _err(f"获取实时事件失败: {e}", 500)


# ══════════════════════════════════════════════════════════════════
#  知识图谱 (Graph)
# ══════════════════════════════════════════════════════════════════

@astrategy_bp.route("/graph/data")
def graph_data():
    """返回知识图谱节点和边。

    Query params:
      - relation_types: 逗号分隔的关系类型过滤 (如 SUPPLIES_TO,COMPETES_WITH)
      - limit: 最大返回节点数 (默认500)
      - search: 搜索关键词 (匹配公司名/代码)
    """
    try:
        raw = _load_graph_data()
        raw_nodes = raw.get("nodes", {})
        raw_edges = raw.get("edges", [])

        limit = request.args.get("limit", 500, type=int)
        limit = min(limit, 5000)
        relation_filter = request.args.get("relation_types", "")
        search_q = request.args.get("search", "").strip()

        # 关系类型过滤
        allowed_relations = set()
        if relation_filter:
            allowed_relations = {r.strip() for r in relation_filter.split(",")}

        # 是否包含机构节点 (默认过滤掉 inst:: 节点)
        include_inst = request.args.get("include_inst", "false").lower() == "true"

        # 构建节点列表
        nodes: List[dict] = []
        node_id_set = set()
        for node_key, node_data in raw_nodes.items():
            # 过滤机构节点 (inst::XX)
            if not include_inst and node_key.startswith("inst::"):
                continue

            name = node_data.get("display_name") or node_data.get("name", node_key)
            summary = node_data.get("summary", "")
            parsed_name, parsed_code = _parse_company_from_summary(summary)
            attrs = node_data.get("attributes", {})
            industry = attrs.get("industry") or node_data.get("industry", "")

            display_name = parsed_name or attrs.get("display_name", "") or name
            code = parsed_code or attrs.get("code", "") or node_key

            # 搜索过滤
            if search_q:
                searchable = f"{display_name} {code} {industry} {summary}"
                if search_q not in searchable:
                    continue

            node_id_set.add(node_key)
            nodes.append({
                "id": node_key,
                "name": display_name,
                "display_name": display_name,
                "code": code,
                "industry": industry,
                "labels": node_data.get("labels", []),
            })

        # 构建边列表
        edges: List[dict] = []
        for edge in raw_edges:
            src = edge.get("source") or edge.get("source_name", "")
            tgt = edge.get("target") or edge.get("target_name", "")
            rel = edge.get("relation", "RELATED_TO")

            # 过滤涉及机构节点的边
            if not include_inst and (src.startswith("inst::") or tgt.startswith("inst::")):
                continue

            if allowed_relations and rel not in allowed_relations:
                continue

            # 只保留两端都在 node_id_set 中的边
            if src not in node_id_set or tgt not in node_id_set:
                continue

            edges.append({
                "source": src,
                "target": tgt,
                "relation": rel,
                "weight": edge.get("weight", 1.0),
                "fact": edge.get("fact", ""),
                "source_display": edge.get("source_display", ""),
                "target_display": edge.get("target_display", ""),
            })

        # 如果有 limit，截取涉及最多边的节点
        if len(nodes) > limit:
            # 统计每个节点参与的边数
            edge_count: Counter = Counter()
            for e in edges:
                edge_count[e["source"]] += 1
                edge_count[e["target"]] += 1
            # 保留度数最高的节点
            top_nodes = {
                n["id"]
                for n in sorted(nodes, key=lambda n: edge_count.get(n["id"], 0), reverse=True)[
                    :limit
                ]
            }
            nodes = [n for n in nodes if n["id"] in top_nodes]
            edges = [
                e for e in edges if e["source"] in top_nodes and e["target"] in top_nodes
            ]

        return _ok(
            {"nodes": nodes, "edges": edges},
            node_count=len(nodes),
            edge_count=len(edges),
        )
    except FileNotFoundError:
        return _err("知识图谱文件不存在", 404)
    except Exception as e:
        logger.error("获取图谱数据失败: %s\n%s", e, traceback.format_exc())
        return _err(f"获取图谱数据失败: {e}", 500)


@astrategy_bp.route("/graph/node/<node_id>/neighbors")
def graph_node_neighbors(node_id: str):
    """返回指定节点的直接邻居"""
    try:
        raw = _load_graph_data()
        raw_nodes = raw.get("nodes", {})
        raw_edges = raw.get("edges", [])

        if node_id not in raw_nodes:
            return _err(f"节点不存在: {node_id}", 404)

        # 查找所有相邻边
        neighbor_ids = set()
        related_edges: List[dict] = []
        for edge in raw_edges:
            src = edge.get("source") or edge.get("source_name", "")
            tgt = edge.get("target") or edge.get("target_name", "")
            if src == node_id:
                neighbor_ids.add(tgt)
                related_edges.append(edge)
            elif tgt == node_id:
                neighbor_ids.add(src)
                related_edges.append(edge)

        # 建立邻居→关系映射
        neighbor_relations: Dict[str, str] = {}
        for edge in related_edges:
            src = edge.get("source") or edge.get("source_name", "")
            tgt = edge.get("target") or edge.get("target_name", "")
            rel = edge.get("relation", "UNKNOWN")
            if src == node_id:
                neighbor_relations.setdefault(tgt, rel)
            elif tgt == node_id:
                neighbor_relations.setdefault(src, rel)

        # 构建邻居节点信息
        neighbors: List[dict] = []
        for nid in neighbor_ids:
            nd = raw_nodes.get(nid, {})
            summary = nd.get("summary", "")
            parsed_name, parsed_code = _parse_company_from_summary(summary)
            attrs = nd.get("attributes", {})
            neighbors.append({
                "id": nid,
                "name": parsed_name or attrs.get("display_name", "") or nd.get("display_name", "") or nd.get("name", nid),
                "code": parsed_code or nid,
                "industry": attrs.get("industry") or nd.get("industry", ""),
                "relation": neighbor_relations.get(nid, "UNKNOWN"),
            })

        # 中心节点信息
        center_node = raw_nodes[node_id]
        center_summary = center_node.get("summary", "")
        cp_name, cp_code = _parse_company_from_summary(center_summary)
        c_attrs = center_node.get("attributes", {})

        return _ok({
            "center": {
                "id": node_id,
                "name": cp_name or c_attrs.get("display_name", "") or center_node.get("display_name", "") or node_id,
                "code": cp_code or node_id,
            },
            "neighbors": neighbors,
            "edges": [
                {
                    "source": e.get("source") or e.get("source_name", ""),
                    "target": e.get("target") or e.get("target_name", ""),
                    "relation": e.get("relation", ""),
                    "fact": e.get("fact", ""),
                    "weight": e.get("weight", 1.0),
                }
                for e in related_edges
            ],
        })
    except Exception as e:
        logger.error("获取节点邻居失败: %s\n%s", e, traceback.format_exc())
        return _err(f"获取节点邻居失败: {e}", 500)


@astrategy_bp.route("/graph/path")
def graph_path():
    """返回两个节点间的最短路径

    Query params:
      - from: 起始节点 ID
      - to: 目标节点 ID
    """
    try:
        from_id = request.args.get("from", "").strip()
        to_id = request.args.get("to", "").strip()
        if not from_id or not to_id:
            return _err("缺少参数: from 和 to 为必填项")

        raw = _load_graph_data()
        raw_nodes = raw.get("nodes", {})
        raw_edges = raw.get("edges", [])

        if from_id not in raw_nodes:
            return _err(f"起始节点不存在: {from_id}", 404)
        if to_id not in raw_nodes:
            return _err(f"目标节点不存在: {to_id}", 404)

        # BFS 最短路径
        from collections import deque

        adjacency: Dict[str, List[Tuple[str, dict]]] = defaultdict(list)
        for edge in raw_edges:
            src = edge.get("source") or edge.get("source_name", "")
            tgt = edge.get("target") or edge.get("target_name", "")
            adjacency[src].append((tgt, edge))
            adjacency[tgt].append((src, edge))

        visited = {from_id}
        queue = deque([(from_id, [from_id], [])])  # (current, path_nodes, path_edges)
        found_path = None
        found_edges = None

        while queue:
            current, path_nodes, path_edges = queue.popleft()
            if current == to_id:
                found_path = path_nodes
                found_edges = path_edges
                break
            for neighbor, edge in adjacency.get(current, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((
                        neighbor,
                        path_nodes + [neighbor],
                        path_edges + [edge],
                    ))

        if not found_path:
            return _ok({"path": [], "edges": [], "found": False, "hops": -1},
                       message="两个节点之间不存在连通路径")

        # 构建路径节点详情
        path_details = []
        for nid in found_path:
            nd = raw_nodes.get(nid, {})
            summary = nd.get("summary", "")
            pn, pc = _parse_company_from_summary(summary)
            attrs = nd.get("attributes", {})
            path_details.append({
                "id": nid,
                "name": pn or attrs.get("display_name", "") or nd.get("display_name", "") or nid,
                "code": pc or nid,
            })

        edge_details = [
            {
                "source": e.get("source") or e.get("source_name", ""),
                "target": e.get("target") or e.get("target_name", ""),
                "relation": e.get("relation", ""),
                "fact": e.get("fact", ""),
            }
            for e in found_edges
        ]

        return _ok({
            "path": path_details,
            "edges": edge_details,
            "found": True,
            "hops": len(found_path) - 1,
        })
    except Exception as e:
        logger.error("获取最短路径失败: %s\n%s", e, traceback.format_exc())
        return _err(f"获取最短路径失败: {e}", 500)


@astrategy_bp.route("/graph/stats")
def graph_stats():
    """返回图谱统计信息"""
    try:
        raw = _load_graph_data()
        raw_nodes = raw.get("nodes", {})
        raw_edges = raw.get("edges", [])

        # 关系类型统计
        relation_counts: Counter = Counter()
        for edge in raw_edges:
            relation_counts[edge.get("relation", "UNKNOWN")] += 1

        # 节点度数统计
        degree: Counter = Counter()
        for edge in raw_edges:
            src = edge.get("source") or edge.get("source_name", "")
            tgt = edge.get("target") or edge.get("target_name", "")
            degree[src] += 1
            degree[tgt] += 1

        top_nodes = degree.most_common(20)
        top_nodes_detail = []
        for nid, deg in top_nodes:
            nd = raw_nodes.get(nid, {})
            summary = nd.get("summary", "")
            pn, _ = _parse_company_from_summary(summary)
            attrs = nd.get("attributes", {})
            top_nodes_detail.append({
                "id": nid,
                "name": pn or attrs.get("display_name", "") or nd.get("display_name", "") or nid,
                "degree": deg,
            })

        return _ok({
            "node_count": len(raw_nodes),
            "edge_count": len(raw_edges),
            "relation_types": dict(relation_counts),
            "avg_degree": round(sum(degree.values()) / max(len(degree), 1), 2),
            "top_nodes": top_nodes_detail,
        })
    except Exception as e:
        logger.error("获取图谱统计失败: %s\n%s", e, traceback.format_exc())
        return _err(f"获取图谱统计失败: {e}", 500)


# ══════════════════════════════════════════════════════════════════
#  组合 (Portfolio)
# ══════════════════════════════════════════════════════════════════

@astrategy_bp.route("/portfolio/summary")
def portfolio_summary():
    """返回组合汇总（模拟数据，后续接入实盘）"""
    try:
        # 从最新信号推算持仓数量
        signals = _load_all_latest_signals()
        long_signals = [s for s in signals if s.get("direction") == "long"]
        avoid_signals = [s for s in signals if s.get("direction") in ("avoid", "short")]

        return _ok({
            "total_value": 1000000.0,
            "cash": 300000.0,
            "position_count": len(long_signals),
            "long_count": len(long_signals),
            "avoid_count": len(avoid_signals),
            "daily_return": 0.0,
            "weekly_return": 0.0,
            "total_return": 0.0,
            "sharpe": 0.0,
            "max_drawdown": 0.0,
            "note": "模拟数据，尚未接入实盘账户",
        })
    except Exception as e:
        logger.error("获取组合摘要失败: %s\n%s", e, traceback.format_exc())
        return _err(f"获取组合摘要失败: {e}", 500)


@astrategy_bp.route("/portfolio/positions")
def portfolio_positions():
    """从最新信号推导当前持仓（long + avoid 都展示）"""
    try:
        signals = _load_all_latest_signals()
        # 显示所有有方向性的信号（long 和 avoid）
        directional = [s for s in signals if s.get("direction") in ("long", "avoid")]

        # 去重：同一 stock_code 只保留 confidence 最高的
        seen: Dict[str, dict] = {}
        for sig in directional:
            code = sig.get("stock_code", "")
            if code not in seen or sig.get("confidence", 0) > seen[code].get("confidence", 0):
                seen[code] = sig

        positions = []
        source_cn_map = {
            "shock_propagation": "冲击链路",
            "graph_factors": "S07图谱因子",
            "sector_rotation": "行业轮动",
            "institution": "机构关联",
            "sentiment_simulation": "S10舆情",
        }

        for sig in seen.values():
            strategy = sig.get("strategy_name", "")
            positions.append({
                "stock_code": sig.get("stock_code", ""),
                "stock_name": sig.get("stock_name", ""),
                "direction": sig.get("direction", "long"),
                "confidence": sig.get("confidence", 0),
                "expected_return": sig.get("expected_return", 0),
                "holding_period_days": sig.get("holding_period_days", 0),
                "strategy": strategy,
                "strategy_cn": source_cn_map.get(strategy, strategy),
                "reasoning": sig.get("reasoning", ""),
                "entry_date": sig.get("signal_date", ""),
                "signal_id": sig.get("signal_id", ""),
                "alpha_type": sig.get("alpha_type", ""),
                "divergence": sig.get("divergence", 0),
            })

        positions.sort(key=lambda p: p["confidence"], reverse=True)
        return _ok(positions, total=len(positions))
    except Exception as e:
        logger.error("获取持仓失败: %s\n%s", e, traceback.format_exc())
        return _err(f"获取持仓失败: {e}", 500)


# ══════════════════════════════════════════════════════════════════
#  回测 (Backtest)
# ══════════════════════════════════════════════════════════════════

@astrategy_bp.route("/backtest/summary")
def backtest_summary():
    """从冲击回测报告中读取汇总指标"""
    try:
        # 读取最新的回测信号文件
        shock_signals = _load_shock_signals()

        if not shock_signals:
            return _ok({
                "sharpe": 0,
                "hit_rate": 0,
                "signal_count": 0,
                "message": "暂无回测数据",
            })

        total = len(shock_signals)
        # 计算胜率: direction-adjusted return > 0
        wins = 0
        returns_list = []
        for sig in shock_signals:
            direction = sig.get("signal_direction", "")
            ret_5d = sig.get("fwd_return_5d") or sig.get("return_5d")
            if ret_5d is None:
                continue
            # 方向调整: avoid → 期望跌 (收益取反), long → 期望涨
            if direction == "avoid":
                adj_ret = -ret_5d
            else:
                adj_ret = ret_5d
            returns_list.append(adj_ret)
            if adj_ret > 0:
                wins += 1

        valid = len(returns_list)
        hit_rate = wins / valid if valid else 0
        avg_return = sum(returns_list) / valid if valid else 0

        # 简易 Sharpe 估算 (年化)
        import statistics
        if len(returns_list) > 1:
            std = statistics.stdev(returns_list)
            sharpe = (avg_return / std) * (252 / 5) ** 0.5 if std > 0 else 0
        else:
            sharpe = 0

        # 按事件类型统计
        type_stats: Dict[str, dict] = defaultdict(
            lambda: {"count": 0, "wins": 0, "total_return": 0.0}
        )
        for sig in shock_signals:
            etype = sig.get("event_type", "unknown")
            direction = sig.get("signal_direction", "")
            ret_5d = sig.get("fwd_return_5d") or sig.get("return_5d")
            if ret_5d is None:
                continue
            adj_ret = -ret_5d if direction == "avoid" else ret_5d
            type_stats[etype]["count"] += 1
            type_stats[etype]["total_return"] += adj_ret
            if adj_ret > 0:
                type_stats[etype]["wins"] += 1

        by_type = {
            k: {
                "count": v["count"],
                "hit_rate": round(v["wins"] / v["count"], 4) if v["count"] else 0,
                "avg_return": round(v["total_return"] / v["count"], 4) if v["count"] else 0,
            }
            for k, v in type_stats.items()
        }

        # 按跳数统计
        hop_stats: Dict[int, dict] = defaultdict(
            lambda: {"count": 0, "wins": 0, "total_return": 0.0}
        )
        for sig in shock_signals:
            hop = sig.get("hop", -1)
            direction = sig.get("signal_direction", "")
            ret_5d = sig.get("fwd_return_5d") or sig.get("return_5d")
            if ret_5d is None:
                continue
            adj_ret = -ret_5d if direction == "avoid" else ret_5d
            hop_stats[hop]["count"] += 1
            hop_stats[hop]["total_return"] += adj_ret
            if adj_ret > 0:
                hop_stats[hop]["wins"] += 1

        by_hop = {
            str(k): {
                "count": v["count"],
                "hit_rate": round(v["wins"] / v["count"], 4) if v["count"] else 0,
                "avg_return": round(v["total_return"] / v["count"], 4) if v["count"] else 0,
            }
            for k, v in sorted(hop_stats.items())
        }

        return _ok({
            "sharpe": round(sharpe, 2),
            "hit_rate": round(hit_rate, 4),
            "total_signals": total,
            "signal_count": total,  # 兼容旧字段名
            "valid_signal_count": valid,
            "avg_return_5d": round(avg_return, 4),
            "by_event_type": by_type,
            "by_hop": by_hop,
        })
    except Exception as e:
        logger.error("获取回测摘要失败: %s\n%s", e, traceback.format_exc())
        return _err(f"获取回测摘要失败: {e}", 500)


@astrategy_bp.route("/backtest/strategies")
def backtest_strategies():
    """各策略信号统计对比"""
    try:
        if not _SIGNALS_DIR.is_dir():
            return _ok([])

        strategies: List[dict] = []
        for strategy_dir in sorted(_SIGNALS_DIR.iterdir()):
            if not strategy_dir.is_dir():
                continue

            name = strategy_dir.name
            # 找出所有日期文件
            date_files = sorted(
                [f for f in strategy_dir.glob("*.json") if re.match(r"^\d{8}\.json$", f.name)],
                key=lambda f: f.name,
            )
            if not date_files:
                continue

            # 加载最新信号
            latest_signals = _load_strategy_signals(strategy_dir)
            total_signals = len(latest_signals)

            # 方向分布
            dir_counts: Counter = Counter()
            conf_sum = 0.0
            for sig in latest_signals:
                dir_counts[sig.get("direction", "unknown")] += 1
                conf_sum += sig.get("confidence", 0)

            strategies.append({
                "strategy_name": name,
                "signal_count": total_signals,
                "date_count": len(date_files),
                "latest_date": date_files[-1].stem,
                "oldest_date": date_files[0].stem,
                "avg_confidence": round(conf_sum / total_signals, 4) if total_signals else 0,
                "direction_distribution": dict(dir_counts),
                "last_updated": datetime.fromtimestamp(
                    date_files[-1].stat().st_mtime
                ).isoformat(),
            })

        # 补充冲击链路策略
        shock_signals = _load_shock_signals()
        if shock_signals:
            dir_counts_shock: Counter = Counter()
            conf_sum_s = 0.0
            for sig in shock_signals:
                dir_counts_shock[sig.get("signal_direction", "unknown")] += 1
                conf_sum_s += sig.get("confidence", 0)
            strategies.append({
                "strategy_name": "shock_pipeline",
                "signal_count": len(shock_signals),
                "date_count": 1,
                "latest_date": shock_signals[0].get("event_date", ""),
                "oldest_date": shock_signals[-1].get("event_date", ""),
                "avg_confidence": round(conf_sum_s / len(shock_signals), 4),
                "direction_distribution": dict(dir_counts_shock),
                "last_updated": "",
            })

        return _ok(strategies, total=len(strategies))
    except Exception as e:
        logger.error("获取策略对比失败: %s\n%s", e, traceback.format_exc())
        return _err(f"获取策略对比失败: {e}", 500)


# ══════════════════════════════════════════════════════════════════
#  系统状态 (System)
# ══════════════════════════════════════════════════════════════════

@astrategy_bp.route("/system/status")
def system_status():
    """返回系统运行状态"""
    try:
        status: Dict[str, Any] = {
            "service": "AStrategy",
            "status": "running",
        }

        # 图谱状态
        if _GRAPH_FILE.exists():
            raw = _load_graph_data()
            status["graph"] = {
                "node_count": len(raw.get("nodes", {})),
                "edge_count": len(raw.get("edges", [])),
                "file_size_mb": round(_GRAPH_FILE.stat().st_size / 1024 / 1024, 2),
                "last_modified": datetime.fromtimestamp(
                    _GRAPH_FILE.stat().st_mtime
                ).isoformat(),
            }
        else:
            status["graph"] = {"node_count": 0, "edge_count": 0, "message": "图谱文件不存在"}

        # 信号状态
        signal_stats: Dict[str, Any] = {"strategies": {}}
        total_signals = 0
        if _SIGNALS_DIR.is_dir():
            for strategy_dir in sorted(_SIGNALS_DIR.iterdir()):
                if strategy_dir.is_dir():
                    latest_date = _get_latest_date_in_dir(strategy_dir)
                    signals = _load_strategy_signals(strategy_dir) if latest_date else []
                    signal_stats["strategies"][strategy_dir.name] = {
                        "latest_date": latest_date,
                        "signal_count": len(signals),
                    }
                    total_signals += len(signals)
        signal_stats["total_signal_count"] = total_signals
        status["signals"] = signal_stats

        # 事件状态
        if _EVENTS_FILE.exists():
            events = _read_json(_EVENTS_FILE)
            status["events"] = {
                "count": len(events) if isinstance(events, list) else 0,
                "last_modified": datetime.fromtimestamp(
                    _EVENTS_FILE.stat().st_mtime
                ).isoformat(),
            }

        # 回测报告状态
        if _REPORTS_DIR.is_dir():
            report_files = sorted(_REPORTS_DIR.iterdir(), key=lambda f: f.stat().st_mtime, reverse=True)
            status["reports"] = {
                "count": len(report_files),
                "latest": report_files[0].name if report_files else None,
                "last_modified": (
                    datetime.fromtimestamp(report_files[0].stat().st_mtime).isoformat()
                    if report_files
                    else None
                ),
            }

        # 冲击信号
        shock_signals = _load_shock_signals()
        status["shock_pipeline"] = {
            "signal_count": len(shock_signals),
            "event_count": len({s.get("event_id") for s in shock_signals if s.get("event_id")}),
        }

        return _ok(status)
    except Exception as e:
        logger.error("获取系统状态失败: %s\n%s", e, traceback.format_exc())
        return _err(f"获取系统状态失败: {e}", 500)
