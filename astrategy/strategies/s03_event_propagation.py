"""
Event Propagation Strategy (事件传播策略)
==========================================
Major policy/event propagates along industry chains with time delays.
First-order beneficiaries react quickly (limit up), second-order
beneficiaries react slowly -- buy second-order before they react.

Signal logic
------------
1. Scan recent news for major policy / regulatory events (national-level).
2. Use LLM to classify the event and identify affected industries.
3. Traverse the knowledge graph to find multi-order beneficiaries:
   - 1st order: directly impacted by the policy/industry.
   - 2nd order: supply-chain / cooperation partners of 1st-order stocks.
   - 3rd order: further graph neighbours.
4. Check post-event price reaction for each beneficiary.
5. For unreacted 2nd/3rd-order stocks, use LLM to assess propagation
   strength and expected delay.
6. Generate long signals for high-confidence unreacted beneficiaries.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from astrategy.data_collector.market_data import MarketDataCollector
from astrategy.data_collector.news import NewsCollector
from astrategy.graph.local_store import LocalGraphStore
from astrategy.graph.topology import TopologyAnalyzer
from astrategy.llm import create_llm_client
from astrategy.strategies.base import BaseStrategy, StrategySignal

logger = logging.getLogger(__name__)

_CST = timezone(timedelta(hours=8))

# ---------------------------------------------------------------------------
# Policy / event keyword sets
# ---------------------------------------------------------------------------

_MAJOR_EVENT_KEYWORDS = (
    "国务院",
    "发改委",
    "工信部",
    "政策",
    "规划",
    "补贴",
    "新规",
    "禁令",
    "制裁",
    "央行",
    "财政部",
    "商务部",
    "科技部",
    "国资委",
    "证监会",
    "银保监会",
    "住建部",
    "生态环境部",
    "战略",
    "专项",
    "行动计划",
    "指导意见",
    "实施方案",
    "十四五",
    "十五五",
)

# Reaction thresholds (absolute post-event return)
_FULLY_REACTED_THRESHOLD = 0.05    # > 5 % move
_PARTIALLY_REACTED_THRESHOLD = 0.02  # 2 - 5 % move
# Below 2 % is considered "unreacted"


# ---------------------------------------------------------------------------
# Strategy
# ---------------------------------------------------------------------------

class EventPropagationStrategy(BaseStrategy):
    """Detect unreacted second/third-order event beneficiaries."""

    # Tunable parameters
    HOLDING_PERIOD = 10             # expected holding period (trading days)
    REACTION_WINDOW_DAYS = 5        # calendar days to measure post-event price move
    CONFIDENCE_BASE = 0.45
    CONFIDENCE_LLM_SCALE = 0.40    # max additional confidence from LLM assessment
    MIN_CONFIDENCE = 0.50           # minimum confidence to emit signal
    MAX_EVENTS = 5                  # max concurrent events to process
    MAX_BENEFICIARIES_PER_LEVEL = 30

    def __init__(
        self,
        signal_dir: Path | str | None = None,
        graph_builder=None,  # kept for API compatibility, ignored
        llm_client: LLMClient | None = None,
    ) -> None:
        super().__init__(signal_dir)
        self._news = NewsCollector()
        self._market = MarketDataCollector()
        self._graph = graph_builder
        self._llm = llm_client
        # Lazy-init LLM and graph if not injected
        self._llm_initialised = llm_client is not None
        self._graph_initialised = graph_builder is not None

    def _ensure_llm(self):
        if self._llm is None:
            self._llm = create_llm_client(strategy_name=self.name)
            self._llm_initialised = True
        return self._llm

    def _ensure_graph(self):
        if self._graph is None:
            # Prefer local graph store (no Zep dependency)
            local = LocalGraphStore()
            if local.load("supply_chain"):
                logger.info("Using local graph store for S03")
                self._graph = local
            else:
                logger.warning("Local graph '%s' not found; S03 will produce no signals.", self._graph_id)
                self._graph = LocalGraphStore()  # empty store
            self._graph_initialised = True
        return self._graph

    # ---- identity --------------------------------------------------------

    @property
    def name(self) -> str:
        return "event_propagation"

    # ---- 1. detect_major_events ------------------------------------------

    def detect_major_events(self, date: str | None = None) -> list[dict]:
        """Scan news for major policy/event keywords and classify via LLM.

        Parameters
        ----------
        date : str | None
            Date label (``YYYYMMDD``).  Currently unused for filtering
            (news APIs return recent items); kept for interface consistency.

        Returns
        -------
        list[dict]
            Each dict: ``{event_title, event_type, affected_industries,
            impact_direction, significance_level, source_text}``
        """
        # Gather news from multiple sources
        raw_news: list[dict] = []

        # Market hot topics often surface policy events
        hot_topics = self._news.get_market_hot_topics(limit=50)
        for item in hot_topics:
            title = str(
                item.get("股票名称", item.get("板块名称", item.get("名称", "")))
            )
            content = str(item.get("新闻内容", item.get("最新动态", "")))
            raw_news.append({"title": title, "content": content, "raw": item})

        # Also scan a few major-industry boards for headline news
        for industry in ("新能源", "半导体", "医药", "人工智能", "军工"):
            try:
                ind_news = self._news.get_industry_news(industry, limit=10)
                for item in ind_news:
                    title = str(item.get("新闻标题", item.get("标题", "")))
                    content = str(item.get("新闻内容", ""))
                    raw_news.append({
                        "title": title,
                        "content": content,
                        "raw": item,
                    })
            except Exception as exc:
                logger.warning("Industry news scan for '%s' failed: %s", industry, exc)

        # Keyword pre-filter
        candidates: list[dict] = []
        seen_titles: set[str] = set()
        for item in raw_news:
            combined = item["title"] + " " + item["content"]
            if any(kw in combined for kw in _MAJOR_EVENT_KEYWORDS):
                # Deduplicate by title
                norm_title = item["title"].strip()
                if norm_title and norm_title not in seen_titles:
                    seen_titles.add(norm_title)
                    candidates.append(item)

        if not candidates:
            logger.info("No major events detected in current news scan.")
            return []

        # Trim to manageable batch for LLM classification
        candidates = candidates[: self.MAX_EVENTS * 3]

        # LLM classification
        events = self._classify_events_via_llm(candidates)
        return events[: self.MAX_EVENTS]

    def _classify_events_via_llm(self, candidates: list[dict]) -> list[dict]:
        """Use LLM to classify and filter candidate events."""
        llm = self._ensure_llm()

        news_block = ""
        for i, item in enumerate(candidates, 1):
            news_block += (
                f"[{i}] 标题: {item['title']}\n"
                f"    内容: {item['content'][:200]}\n\n"
            )

        prompt = f"""\
你是A股政策分析专家。以下是最近的新闻条目，请识别其中具有重大市场影响力的
国家级政策或行业级重大事件。

新闻列表:
{news_block}

请输出JSON，格式:
{{
  "events": [
    {{
      "news_index": <int>,
      "event_title": "<事件简短标题>",
      "event_type": "<policy|regulation|subsidy|sanction|industry_reform|macro|other>",
      "affected_industries": ["<行业1>", "<行业2>"],
      "impact_direction": "<positive|negative|mixed>",
      "significance_level": <1-5, 5为最高>
    }}
  ]
}}

筛选标准:
- 只保留国家级/部委级政策、重大行业变革、国际制裁等真正影响面广的事件
- significance_level >= 3 才保留
- 普通公司公告、个股涨跌不算重大事件
"""
        messages = [{"role": "user", "content": prompt}]

        try:
            result = llm.chat_json(messages, temperature=0.2)
        except Exception as exc:
            logger.error("LLM event classification failed: %s", exc)
            return []

        events: list[dict] = []
        for ev in result.get("events", []):
            sig_level = ev.get("significance_level", 0)
            if sig_level < 3:
                continue
            idx = ev.get("news_index", 0)
            source_text = ""
            if 1 <= idx <= len(candidates):
                source_text = candidates[idx - 1]["title"]
            events.append({
                "event_title": ev.get("event_title", ""),
                "event_type": ev.get("event_type", "other"),
                "affected_industries": ev.get("affected_industries", []),
                "impact_direction": ev.get("impact_direction", "mixed"),
                "significance_level": sig_level,
                "source_text": source_text,
            })

        logger.info("LLM classified %d significant events from %d candidates.",
                     len(events), len(candidates))
        return events

    # ---- 2. identify_beneficiaries ---------------------------------------

    def identify_beneficiaries(
        self,
        event: dict,
        graph_id: str,
    ) -> dict[int, list[dict]]:
        """Traverse the knowledge graph to find multi-order beneficiaries.

        Parameters
        ----------
        event : dict
            Event dict from ``detect_major_events``.
        graph_id : str
            Zep graph (user) ID to traverse.

        Returns
        -------
        dict[int, list[dict]]
            Mapping from propagation level (1, 2, 3) to lists of
            ``{stock_code, stock_name, relation_path}`` dicts.
        """
        graph = self._ensure_graph()

        affected_industries = event.get("affected_industries", [])
        if not affected_industries:
            return {}

        # Fetch graph data
        nodes = graph.get_all_nodes(graph_id)
        edges = graph.get_all_edges(graph_id)

        if not nodes or not edges:
            logger.warning("Graph '%s' is empty; cannot identify beneficiaries.", graph_id)
            return {}

        # Build lookup structures
        name_to_node: dict[str, dict] = {}
        for n in nodes:
            name_to_node[n.get("name", "")] = n

        # --- 1st order: find nodes matching affected industries ---
        first_order_nodes: list[str] = []
        for node in nodes:
            node_name = node.get("name", "")
            labels = node.get("labels", [])
            summary = node.get("summary", "")
            combined = f"{node_name} {' '.join(labels)} {summary}"
            for ind in affected_industries:
                if ind in combined:
                    first_order_nodes.append(node_name)
                    break

        # Also search the graph for each affected industry
        for ind in affected_industries:
            try:
                search_results = graph.search(graph_id, f"{ind} 公司 股票", limit=15)
                for sr in search_results:
                    for key in ("source", "target"):
                        name = sr.get(key, "")
                        if name and name not in first_order_nodes:
                            first_order_nodes.append(name)
            except Exception as exc:
                logger.warning("Graph search for industry '%s' failed: %s", ind, exc)

        first_order_nodes = list(set(first_order_nodes))[:self.MAX_BENEFICIARIES_PER_LEVEL]

        # --- 2nd order: neighbours of 1st-order nodes ---
        second_order_set: set[str] = set()
        first_order_set = set(first_order_nodes)
        for node_name in first_order_nodes:
            neighbours = TopologyAnalyzer.get_neighbors(edges, node_name, depth=1)
            for nb in neighbours:
                if nb not in first_order_set:
                    second_order_set.add(nb)

        second_order_nodes = sorted(second_order_set)[:self.MAX_BENEFICIARIES_PER_LEVEL]

        # --- 3rd order: neighbours of 2nd-order (excluding 1st and 2nd) ---
        third_order_set: set[str] = set()
        combined_12 = first_order_set | second_order_set
        for node_name in second_order_nodes:
            neighbours = TopologyAnalyzer.get_neighbors(edges, node_name, depth=1)
            for nb in neighbours:
                if nb not in combined_12:
                    third_order_set.add(nb)

        third_order_nodes = sorted(third_order_set)[:self.MAX_BENEFICIARIES_PER_LEVEL]

        # Build result dicts with stock info
        result: dict[int, list[dict]] = {1: [], 2: [], 3: []}

        for level, node_list in [
            (1, first_order_nodes),
            (2, second_order_nodes),
            (3, third_order_nodes),
        ]:
            for node_name in node_list:
                node_info = name_to_node.get(node_name, {})
                stock_code = self._extract_stock_code(node_info)
                if not stock_code:
                    # Try to extract from name (e.g. "贵州茅台(600519)")
                    stock_code = self._extract_code_from_name(node_name)

                # Build relation path
                if level == 1:
                    path = f"{event['event_title']} -> {', '.join(affected_industries)} -> {node_name}"
                else:
                    # Find shortest path from any 1st-order node
                    best_path: list[str] = []
                    for fo in first_order_nodes[:10]:
                        p = TopologyAnalyzer.shortest_path(edges, fo, node_name)
                        if p and (not best_path or len(p) < len(best_path)):
                            best_path = p
                    path_str = " -> ".join(best_path) if best_path else node_name
                    path = f"{event['event_title']} -> {path_str}"

                result[level].append({
                    "node_name": node_name,
                    "stock_code": stock_code or "",
                    "stock_name": node_name,
                    "labels": node_info.get("labels", []),
                    "relation_path": path,
                })

        for level in (1, 2, 3):
            logger.info(
                "Event '%s': %d %s-order beneficiaries identified.",
                event["event_title"],
                len(result[level]),
                _ordinal(level),
            )

        return result

    # ---- 3. check_reaction_status ----------------------------------------

    def check_reaction_status(
        self,
        beneficiaries: dict[int, list[dict]],
        event_date: str,
    ) -> dict[str, list[dict]]:
        """Check post-event price reaction for each beneficiary.

        Parameters
        ----------
        beneficiaries : dict[int, list[dict]]
            Output of ``identify_beneficiaries``.
        event_date : str
            Event date in ``YYYYMMDD`` format.

        Returns
        -------
        dict[str, list[dict]]
            Three lists: ``fully_reacted``, ``partially_reacted``,
            ``unreacted``.  Each item is the beneficiary dict augmented
            with ``post_event_return`` and ``reaction_status``.
        """
        result: dict[str, list[dict]] = {
            "fully_reacted": [],
            "partially_reacted": [],
            "unreacted": [],
        }

        # Compute date range for reaction window
        try:
            event_dt = datetime.strptime(event_date, "%Y%m%d")
        except ValueError:
            event_dt = datetime.now(tz=_CST)

        start_date = event_dt.strftime("%Y%m%d")
        end_date = (event_dt + timedelta(days=self.REACTION_WINDOW_DAYS + 5)).strftime("%Y%m%d")

        for level, items in beneficiaries.items():
            for item in items:
                code = item.get("stock_code", "")
                if not code or len(code) != 6:
                    # Cannot look up price without a valid code; assume unreacted
                    enriched = {**item, "propagation_level": level,
                                "post_event_return": 0.0,
                                "reaction_status": "unknown"}
                    result["unreacted"].append(enriched)
                    continue

                ret = self._compute_post_event_return(code, start_date, end_date)
                abs_ret = abs(ret) if ret is not None else 0.0

                if abs_ret >= _FULLY_REACTED_THRESHOLD:
                    status = "fully_reacted"
                elif abs_ret >= _PARTIALLY_REACTED_THRESHOLD:
                    status = "partially_reacted"
                else:
                    status = "unreacted"

                enriched = {
                    **item,
                    "propagation_level": level,
                    "post_event_return": round(ret or 0.0, 6),
                    "reaction_status": status,
                }
                result[status].append(enriched)

        for status, items in result.items():
            logger.info("Reaction check: %d stocks %s.", len(items), status)

        return result

    def _compute_post_event_return(
        self, code: str, start: str, end: str
    ) -> float | None:
        """Compute the total return between *start* and *end* for *code*."""
        df = self._market.get_daily_quotes(code, start, end)
        if df.empty or len(df) < 2:
            return None

        close_col = "收盘" if "收盘" in df.columns else df.columns[2]
        try:
            first_close = float(df[close_col].iloc[0])
            last_close = float(df[close_col].iloc[-1])
            if first_close == 0:
                return None
            return (last_close - first_close) / first_close
        except (ValueError, TypeError, IndexError):
            return None

    # ---- 4. llm_propagation_analysis -------------------------------------

    def llm_propagation_analysis(
        self,
        event: dict,
        unreacted: list[dict],
    ) -> list[dict]:
        """Use LLM to assess propagation strength for unreacted companies.

        Parameters
        ----------
        event : dict
            The major event dict.
        unreacted : list[dict]
            Unreacted beneficiary dicts (from ``check_reaction_status``).

        Returns
        -------
        list[dict]
            Each unreacted item augmented with ``llm_impact_score`` (0-1),
            ``expected_delay_days`` (int), ``llm_confidence`` (0-1),
            ``llm_reasoning`` (str).
        """
        if not unreacted:
            return []

        llm = self._ensure_llm()

        # Batch process in chunks to save tokens
        batch_size = 10
        results: list[dict] = []

        for i in range(0, len(unreacted), batch_size):
            batch = unreacted[i : i + batch_size]
            batch_results = self._llm_assess_batch(llm, event, batch)
            results.extend(batch_results)

        return results

    def _llm_assess_batch(
        self,
        llm: LLMClient,
        event: dict,
        batch: list[dict],
    ) -> list[dict]:
        """Assess a batch of unreacted stocks via a single LLM call."""
        companies_block = ""
        for j, item in enumerate(batch, 1):
            companies_block += (
                f"[{j}] 公司: {item.get('stock_name', item.get('node_name', ''))}\n"
                f"    股票代码: {item.get('stock_code', 'N/A')}\n"
                f"    传播层级: 第{item.get('propagation_level', '?')}层\n"
                f"    传播路径: {item.get('relation_path', 'N/A')}\n"
                f"    事件后涨跌幅: {item.get('post_event_return', 0):.2%}\n\n"
            )

        affected_str = ", ".join(event.get("affected_industries", []))
        prompt = f"""\
你是A股事件传播分析专家。

重大事件: {event.get('event_title', '')}
事件类型: {event.get('event_type', '')}
影响方向: {event.get('impact_direction', '')}
直接影响行业: {affected_str}

以下公司是该事件的间接受益方，但股价尚未明显反应。请分析每家公司受到事件
传播影响的可能性。

公司列表:
{companies_block}

请输出JSON，格式:
{{
  "assessments": [
    {{
      "company_index": <int>,
      "impact_score": <0.0-1.0, 事件对该公司的影响强度>,
      "expected_delay_days": <int, 预计市场反应延迟天数, 1-15>,
      "confidence": <0.0-1.0, 你的判断置信度>,
      "reasoning": "<简要分析, 50字以内>"
    }}
  ]
}}

评估要点:
- 考虑供应链关系紧密度、收入占比、替代性
- 第2层公司通常3-5天反应, 第3层5-10天
- 高impact_score意味着事件对公司基本面有实质影响
- 如果传播路径牵强, 给低分
"""
        messages = [{"role": "user", "content": prompt}]

        try:
            result = llm.chat_json(messages, temperature=0.2)
        except Exception as exc:
            logger.error("LLM propagation analysis failed: %s", exc)
            # Return batch items with default low scores
            return [
                {
                    **item,
                    "llm_impact_score": 0.0,
                    "expected_delay_days": 5,
                    "llm_confidence": 0.0,
                    "llm_reasoning": f"LLM analysis failed: {exc}",
                }
                for item in batch
            ]

        # Merge LLM results back into batch items
        llm_map: dict[int, dict] = {}
        for assessment in result.get("assessments", []):
            idx = assessment.get("company_index", 0)
            llm_map[idx] = assessment

        enriched: list[dict] = []
        for j, item in enumerate(batch, 1):
            a = llm_map.get(j, {})
            enriched.append({
                **item,
                "llm_impact_score": float(a.get("impact_score", 0.0)),
                "expected_delay_days": int(a.get("expected_delay_days", 5)),
                "llm_confidence": float(a.get("confidence", 0.0)),
                "llm_reasoning": str(a.get("reasoning", "")),
            })

        return enriched

    # ---- 5. run ----------------------------------------------------------

    def run(
        self,
        stock_codes: list[str] | None = None,
        graph_id: str | None = None,
        event_date: str | None = None,
    ) -> list[StrategySignal]:
        """Run the full event propagation pipeline.

        Parameters
        ----------
        stock_codes : list[str] | None
            Not directly used for event detection (the strategy is
            event-driven, not stock-driven).  If provided, final signals
            are filtered to this universe.
        graph_id : str | None
            Zep graph ID for beneficiary traversal.  Required for
            graph-based propagation analysis.
        event_date : str | None
            Date label (``YYYYMMDD``) for reaction measurement.
            Defaults to today CST.

        Returns
        -------
        list[StrategySignal]
        """
        if event_date is None:
            event_date = datetime.now(tz=_CST).strftime("%Y%m%d")

        # Step 1: Detect major events
        logger.info("Step 1: Detecting major events ...")
        events = self.detect_major_events(date=event_date)
        if not events:
            logger.info("No major events found. No signals generated.")
            return []

        all_signals: list[StrategySignal] = []

        for event in events:
            logger.info(
                "Processing event: '%s' (type=%s, direction=%s)",
                event["event_title"],
                event["event_type"],
                event["impact_direction"],
            )

            # Step 2: Identify beneficiaries via graph
            if graph_id:
                beneficiaries = self.identify_beneficiaries(event, graph_id)
            else:
                logger.warning(
                    "No graph_id provided; skipping graph-based beneficiary "
                    "identification. Using LLM-only fallback."
                )
                beneficiaries = self._identify_beneficiaries_llm_fallback(event)

            if not any(beneficiaries.values()):
                logger.info("No beneficiaries found for event '%s'.", event["event_title"])
                continue

            # Step 3: Check reaction status
            reaction = self.check_reaction_status(beneficiaries, event_date)

            # Gather first-order stats for metadata
            first_order_reacted = [
                item for item in reaction["fully_reacted"]
                if item.get("propagation_level") == 1
            ]
            first_order_returns = [
                item["post_event_return"]
                for item in first_order_reacted
                if item.get("post_event_return") is not None
            ]
            first_order_avg_ret = (
                sum(first_order_returns) / len(first_order_returns)
                if first_order_returns
                else 0.0
            )
            first_order_codes = [
                item.get("stock_code", "")
                for item in first_order_reacted
                if item.get("stock_code")
            ]

            # Step 4: Analyse unreacted beneficiaries (focus on 2nd/3rd order)
            unreacted_higher_order = [
                item
                for item in reaction["unreacted"] + reaction["partially_reacted"]
                if item.get("propagation_level", 1) >= 2
            ]

            if not unreacted_higher_order:
                logger.info(
                    "Event '%s': all higher-order beneficiaries have reacted.",
                    event["event_title"],
                )
                continue

            assessed = self.llm_propagation_analysis(event, unreacted_higher_order)

            # Step 5: Generate signals
            for item in assessed:
                signal = self._build_signal(
                    event=event,
                    item=item,
                    first_order_codes=first_order_codes,
                    first_order_avg_return=first_order_avg_ret,
                )
                if signal is not None:
                    # Filter to universe if provided
                    if stock_codes is None or signal.stock_code in stock_codes:
                        all_signals.append(signal)

        logger.info(
            "EventPropagationStrategy produced %d signals from %d events.",
            len(all_signals),
            len(events),
        )
        return all_signals

    # ---- 6. run_single ---------------------------------------------------

    def run_single(self, stock_code: str) -> list[StrategySignal]:
        """Run event propagation analysis focused on a single stock.

        Detects events, then checks if *stock_code* appears as an
        unreacted beneficiary.
        """
        events = self.detect_major_events()
        if not events:
            return []

        event_date = datetime.now(tz=_CST).strftime("%Y%m%d")
        signals: list[StrategySignal] = []

        for event in events:
            # Use LLM fallback since we may not have a graph_id
            beneficiaries = self._identify_beneficiaries_llm_fallback(event)

            # Check if stock_code appears at any level
            found_items: list[dict] = []
            for level, items in beneficiaries.items():
                for item in items:
                    if item.get("stock_code") == stock_code:
                        found_items.append({**item, "propagation_level": level})

            if not found_items:
                continue

            # Check reaction
            reaction = self.check_reaction_status(
                {item["propagation_level"]: [item] for item in found_items},
                event_date,
            )

            unreacted = reaction["unreacted"] + reaction["partially_reacted"]
            if not unreacted:
                continue

            assessed = self.llm_propagation_analysis(event, unreacted)

            for item in assessed:
                signal = self._build_signal(
                    event=event,
                    item=item,
                    first_order_codes=[],
                    first_order_avg_return=0.0,
                )
                if signal is not None:
                    signals.append(signal)

        return signals

    # ---- LLM fallback for beneficiary identification ---------------------

    def _identify_beneficiaries_llm_fallback(
        self, event: dict
    ) -> dict[int, list[dict]]:
        """When no graph is available, use LLM to identify beneficiaries."""
        llm = self._ensure_llm()

        affected_str = ", ".join(event.get("affected_industries", []))
        prompt = f"""\
你是A股产业链分析专家。

重大事件: {event.get('event_title', '')}
事件类型: {event.get('event_type', '')}
直接影响行业: {affected_str}

请分析该事件沿产业链传播会影响哪些A股上市公司。

输出JSON:
{{
  "first_order": [
    {{"stock_code": "6位代码", "stock_name": "公司名", "reason": "直接受影响原因"}}
  ],
  "second_order": [
    {{"stock_code": "6位代码", "stock_name": "公司名", "reason": "间接受影响原因", "transmission_path": "传播路径"}}
  ],
  "third_order": [
    {{"stock_code": "6位代码", "stock_name": "公司名", "reason": "间接受影响原因", "transmission_path": "传播路径"}}
  ]
}}

要求:
- 每个层级最多10家公司
- 股票代码必须是真实的A股代码 (6位数字)
- first_order: 政策直接影响的公司
- second_order: 通过供应链/合作关系间接影响的公司
- third_order: 更远的间接影响
"""
        messages = [{"role": "user", "content": prompt}]

        try:
            result = llm.chat_json(messages, temperature=0.3)
        except Exception as exc:
            logger.error("LLM beneficiary identification failed: %s", exc)
            return {1: [], 2: [], 3: []}

        beneficiaries: dict[int, list[dict]] = {1: [], 2: [], 3: []}
        level_keys = {1: "first_order", 2: "second_order", 3: "third_order"}

        for level, key in level_keys.items():
            for item in result.get(key, []):
                code = str(item.get("stock_code", "")).strip()
                name = str(item.get("stock_name", "")).strip()
                path = item.get("transmission_path", item.get("reason", ""))
                beneficiaries[level].append({
                    "stock_code": code,
                    "stock_name": name,
                    "node_name": name,
                    "labels": [],
                    "relation_path": f"{event['event_title']} -> {path}",
                })

        return beneficiaries

    # ---- signal building -------------------------------------------------

    def _build_signal(
        self,
        event: dict,
        item: dict,
        first_order_codes: list[str],
        first_order_avg_return: float,
    ) -> StrategySignal | None:
        """Convert an assessed beneficiary into a StrategySignal."""
        stock_code = item.get("stock_code", "")
        stock_name = item.get("stock_name", item.get("node_name", stock_code))
        level = item.get("propagation_level", 2)
        llm_impact = item.get("llm_impact_score", 0.0)
        llm_conf = item.get("llm_confidence", 0.0)
        delay_days = item.get("expected_delay_days", 5)
        llm_reasoning = item.get("llm_reasoning", "")
        relation_path = item.get("relation_path", "")

        if not stock_code or len(stock_code) != 6:
            return None

        # Compute confidence
        confidence = self.CONFIDENCE_BASE + llm_conf * self.CONFIDENCE_LLM_SCALE

        # Bonus for strong first-order reaction (validates the event impact)
        if first_order_avg_return > 0.05:
            confidence += 0.05
        if first_order_avg_return > 0.08:
            confidence += 0.05

        # Penalty for 3rd-order (weaker signal)
        if level >= 3:
            confidence -= 0.10

        confidence = max(0.0, min(1.0, confidence))

        if confidence < self.MIN_CONFIDENCE:
            return None

        # Determine direction from event impact
        impact_dir = event.get("impact_direction", "positive")
        if impact_dir == "positive":
            direction = "long"
        elif impact_dir == "negative":
            direction = "short"
        else:
            # Mixed: only go long if LLM impact is strong
            direction = "long" if llm_impact >= 0.6 else "neutral"

        if direction == "neutral":
            return None

        # Expected return: scale by LLM impact and first-order benchmark
        if first_order_avg_return > 0:
            # Expect a fraction of first-order return
            decay_factor = {1: 0.8, 2: 0.5, 3: 0.3}.get(level, 0.3)
            expected_return = abs(first_order_avg_return) * decay_factor * llm_impact
        else:
            expected_return = 0.03 * llm_impact  # default 3% base * impact

        expected_return = max(0.01, min(0.15, expected_return))
        if direction == "short":
            expected_return = -expected_return

        # Build reasoning
        reasoning = (
            f"{stock_name}({stock_code}): {_ordinal(level)}-order beneficiary "
            f"of '{event['event_title']}'. "
            f"Post-event return: {item.get('post_event_return', 0):.2%} (unreacted). "
        )
        if first_order_codes:
            reasoning += (
                f"First-order stocks (avg {first_order_avg_return:+.2%}) "
                f"already reacted. "
            )
        if llm_reasoning:
            reasoning += f"LLM: {llm_reasoning}"

        metadata = {
            "event_title": event.get("event_title", ""),
            "event_type": event.get("event_type", ""),
            "propagation_level": level,
            "first_order_stocks": first_order_codes[:10],
            "first_order_avg_return": round(first_order_avg_return, 4),
            "transmission_path": relation_path,
            "expected_delay_days": delay_days,
            "llm_impact_score": round(llm_impact, 4),
            "llm_reasoning": llm_reasoning,
            "impact_direction": impact_dir,
        }

        return StrategySignal(
            strategy_name=self.name,
            stock_code=stock_code,
            stock_name=stock_name,
            direction=direction,
            confidence=round(confidence, 4),
            expected_return=round(abs(expected_return), 4),
            holding_period_days=self.HOLDING_PERIOD,
            reasoning=reasoning,
            metadata=metadata,
        )

    # ---- utility helpers -------------------------------------------------

    @staticmethod
    def _extract_stock_code(node_info: dict) -> str:
        """Try to extract a 6-digit stock code from node attributes or summary."""
        attrs = node_info.get("attributes", {}) or {}
        for key in ("stock_code", "code", "股票代码", "代码"):
            val = attrs.get(key, "")
            if val and len(str(val)) == 6 and str(val).isdigit():
                return str(val)

        # Try summary
        import re
        summary = node_info.get("summary", "")
        match = re.search(r"\b(\d{6})\b", summary)
        if match:
            code = match.group(1)
            if code[0] in ("0", "3", "6"):  # SZ / SH prefixes
                return code
        return ""

    @staticmethod
    def _extract_code_from_name(name: str) -> str:
        """Extract stock code from patterns like '贵州茅台(600519)'."""
        import re
        match = re.search(r"[(\uff08](\d{6})[)\uff09]", name)
        if match:
            return match.group(1)
        return ""


def _ordinal(n: int) -> str:
    """Return ordinal string for propagation level."""
    return {1: "1st", 2: "2nd", 3: "3rd"}.get(n, f"{n}th")
