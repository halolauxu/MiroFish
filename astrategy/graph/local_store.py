"""
Local in-memory graph store backed by NetworkX.

Drop-in replacement for GraphBuilder when Zep Cloud is unavailable.
Implements the same search / get_all_nodes / get_all_edges interface
so S01 and S03 strategies work without modification.
"""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("astrategy.graph.local_store")


class LocalGraphStore:
    """In-memory knowledge graph with simple text search.

    Compatible with ``GraphBuilder`` APIs used by S01/S03:
      - ``search(graph_id, query, limit)``
      - ``get_all_nodes(graph_id)``
      - ``get_all_edges(graph_id)``
      - ``create_graph(name)``
      - ``add_episodes(graph_id, texts, batch_size)``
    """

    def __init__(self, data_dir: str | Path | None = None):
        self._graphs: dict[str, _Graph] = {}
        self._data_dir = Path(data_dir) if data_dir else (
            Path(__file__).resolve().parent.parent / ".data" / "local_graph"
        )

    # ── graph lifecycle ────────────────────────────────────────

    def create_graph(self, name: str) -> str:
        if name not in self._graphs:
            self._graphs[name] = _Graph(name)
            logger.info("Created local graph '%s'", name)
        return name

    def set_ontology(self, graph_id: str, ontology: dict) -> None:
        """No-op for local store (ontology is implicit in the data)."""
        logger.info("Ontology set (local store, no-op) for '%s'", graph_id)

    # ── data ingestion ─────────────────────────────────────────

    def add_node(self, graph_id: str, name: str, labels: list[str] | None = None,
                 summary: str = "", **attrs) -> None:
        g = self._ensure(graph_id)
        g.nodes[name] = {
            "name": name,
            "labels": labels or [],
            "summary": summary,
            "attributes": attrs,
        }

    def add_edge(self, graph_id: str, source: str, target: str,
                 relation: str = "RELATED_TO", fact: str = "",
                 weight: float = 1.0) -> None:
        g = self._ensure(graph_id)
        edge = {
            "source_name": source,
            "target_name": target,
            "relation": relation,
            "fact": fact,
            "weight": weight,
        }
        g.edges.append(edge)
        # Also index for search
        g.facts.append({
            "source": source,
            "target": target,
            "relation": relation,
            "fact": fact,
            "weight": weight,
        })

    def add_companies(self, graph_id: str, companies: list[dict]) -> None:
        for c in companies:
            name = c.get("name", "")
            code = c.get("code", "")
            industry = c.get("industry", "")
            self.add_node(
                graph_id, name,
                labels=["Company"],
                summary=f"{name}({code}), {industry}行业",
                code=code, industry=industry,
            )
            # Also add a code-keyed node for lookup
            if code:
                self.add_node(
                    graph_id, code,
                    labels=["Company"],
                    summary=f"{name}({code}), {industry}行业",
                    display_name=name, industry=industry,
                )
        logger.info("Added %d companies to local graph '%s'", len(companies), graph_id)

    def add_relationships(self, graph_id: str, edges: list[dict]) -> None:
        for e in edges:
            self.add_edge(
                graph_id,
                source=e.get("source", ""),
                target=e.get("target", ""),
                relation=e.get("relation", "RELATED_TO"),
                fact=e.get("description", ""),
            )
        logger.info("Added %d relationships to local graph '%s'", len(edges), graph_id)

    def add_episodes(self, graph_id: str, texts: list[str],
                     batch_size: int = 3) -> None:
        """Parse free-text episodes to extract entities and relations."""
        g = self._ensure(graph_id)
        for text in texts:
            g.episode_texts.append(text)
            # Simple extraction of supply chain patterns
            self._extract_from_text(graph_id, text)
        logger.info("Added %d episodes to local graph '%s'", len(texts), graph_id)

    # ── retrieval (GraphBuilder-compatible) ─────────────────────

    def get_all_nodes(self, graph_id: str, **kwargs) -> list[dict]:
        g = self._graphs.get(graph_id)
        if not g:
            return []
        return list(g.nodes.values())

    def get_all_edges(self, graph_id: str, **kwargs) -> list[dict]:
        g = self._graphs.get(graph_id)
        if not g:
            return []
        return list(g.edges)

    def search(self, graph_id: str, query: str, limit: int = 10) -> list[dict]:
        """Simple keyword-based search over facts and node summaries."""
        g = self._graphs.get(graph_id)
        if not g:
            return []

        keywords = set(query.replace("(", " ").replace(")", " ").split())
        scored: list[tuple[float, dict]] = []

        # Search facts
        for fact in g.facts:
            text = f"{fact['source']} {fact['target']} {fact['relation']} {fact['fact']}"
            score = sum(1.0 for kw in keywords if kw in text)
            if score > 0:
                scored.append((score, {
                    "fact": fact["fact"],
                    "source": fact["source"],
                    "target": fact["target"],
                    "relation": fact["relation"],
                    "score": score / len(keywords) if keywords else 0,
                    "created_at": "",
                }))

        # Search episode texts
        for ep_text in g.episode_texts:
            score = sum(1.0 for kw in keywords if kw in ep_text)
            if score > 0:
                # Extract source/target from text if possible
                scored.append((score, {
                    "fact": ep_text[:200],
                    "source": "",
                    "target": "",
                    "relation": "EPISODE",
                    "score": score / len(keywords) if keywords else 0,
                    "created_at": "",
                }))

        # Search node summaries
        for node in g.nodes.values():
            text = f"{node['name']} {node.get('summary', '')}"
            score = sum(1.0 for kw in keywords if kw in text)
            if score > 0:
                scored.append((score * 0.5, {
                    "fact": node.get("summary", node["name"]),
                    "source": node["name"],
                    "target": "",
                    "relation": "NODE",
                    "score": score * 0.5 / len(keywords) if keywords else 0,
                    "created_at": "",
                }))

        scored.sort(key=lambda x: x[0], reverse=True)
        return [item for _, item in scored[:limit]]

    # ── persistence ────────────────────────────────────────────

    def save(self, graph_id: str) -> Path:
        """Save graph to JSON file."""
        g = self._graphs.get(graph_id)
        if not g:
            raise ValueError(f"Graph '{graph_id}' not found")

        self._data_dir.mkdir(parents=True, exist_ok=True)
        path = self._data_dir / f"{graph_id}.json"
        data = {
            "graph_id": graph_id,
            "nodes": g.nodes,
            "edges": g.edges,
            "facts": g.facts,
            "episode_texts": g.episode_texts,
        }
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("Saved graph '%s' to %s (%d nodes, %d edges)",
                     graph_id, path, len(g.nodes), len(g.edges))
        return path

    def load(self, graph_id: str) -> bool:
        """Load graph from JSON file. Returns True if found."""
        path = self._data_dir / f"{graph_id}.json"
        if not path.exists():
            return False

        data = json.loads(path.read_text(encoding="utf-8"))
        g = _Graph(graph_id)
        g.nodes = data.get("nodes", {})
        g.episode_texts = data.get("episode_texts", [])

        # Normalise edges: unify source_name/target_name → source/target
        seen_facts: set[str] = set()
        for e in data.get("edges", []):
            src = e.get("source") or e.get("source_name") or ""
            tgt = e.get("target") or e.get("target_name") or ""
            norm = dict(e)  # preserve all saved fields (including source_display etc.)
            norm["source"] = src
            norm["target"] = tgt
            norm["source_name"] = src
            norm["target_name"] = tgt
            g.edges.append(norm)
            key = norm["fact"]
            if key not in seen_facts:
                seen_facts.add(key)
                g.facts.append({"source": src, "target": tgt,
                                 "relation": norm["relation"],
                                 "fact": norm["fact"], "weight": norm["weight"]})

        # Merge in any extra facts saved separately
        for f in data.get("facts", []):
            key = f.get("fact", "")
            if key not in seen_facts:
                seen_facts.add(key)
                g.facts.append({
                    "source": f.get("source") or f.get("source_name") or "",
                    "target": f.get("target") or f.get("target_name") or "",
                    "relation": f.get("relation", "RELATED_TO"),
                    "fact": key, "weight": f.get("weight", 1.0),
                })

        self._graphs[graph_id] = g
        logger.info("Loaded graph '%s' from %s (%d nodes, %d edges, %d facts)",
                     graph_id, path, len(g.nodes), len(g.edges), len(g.facts))
        return True

    # ── helpers ─────────────────────────────────────────────────

    def _ensure(self, graph_id: str) -> "_Graph":
        if graph_id not in self._graphs:
            self._graphs[graph_id] = _Graph(graph_id)
        return self._graphs[graph_id]

    def _extract_from_text(self, graph_id: str, text: str) -> None:
        """Extract simple relationships from structured text."""
        g = self._ensure(graph_id)

        # Pattern: "A是B的上游供应商"
        m = re.search(r"(.+?)(?:\(.+?\))?\s*是\s*(.+?)(?:\(.+?\))?\s*的上游供应商", text)
        if m:
            src, tgt = m.group(1).strip(), m.group(2).strip()
            self.add_edge(graph_id, src, tgt, "SUPPLIES_TO", text[:200])
            return

        # Pattern: "A与B是竞争对手"
        m = re.search(r"(.+?)(?:\(.+?\))?\s*与\s*(.+?)(?:\(.+?\))?\s*是竞争对手", text)
        if m:
            src, tgt = m.group(1).strip(), m.group(2).strip()
            self.add_edge(graph_id, src, tgt, "COMPETES_WITH", text[:200])
            return

        # Pattern: "A与B存在合作关系"
        m = re.search(r"(.+?)(?:\(.+?\))?\s*与\s*(.+?)(?:\(.+?\))?\s*存在合作关系", text)
        if m:
            src, tgt = m.group(1).strip(), m.group(2).strip()
            self.add_edge(graph_id, src, tgt, "COOPERATES_WITH", text[:200])
            return

        # Pattern: "A行业是B行业的上游"
        m = re.search(r"(.+?)行业是(.+?)行业的上游", text)
        if m:
            src, tgt = m.group(1).strip() + "行业", m.group(2).strip() + "行业"
            self.add_edge(graph_id, src, tgt, "SUPPLIES_TO", text[:200])
            return

        # Pattern: "A行业与B行业存在协同关系"
        m = re.search(r"(.+?)行业与(.+?)行业存在协同", text)
        if m:
            src, tgt = m.group(1).strip() + "行业", m.group(2).strip() + "行业"
            self.add_edge(graph_id, src, tgt, "COOPERATES_WITH", text[:200])
            return


class _Graph:
    """Internal graph storage."""
    def __init__(self, graph_id: str):
        self.graph_id = graph_id
        self.nodes: dict[str, dict] = {}
        self.edges: list[dict] = []
        self.facts: list[dict] = []
        self.episode_texts: list[str] = []
