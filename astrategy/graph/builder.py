"""
A-share knowledge graph builder using Zep Cloud.

Manages graph lifecycle: creation, ontology setup, entity/relationship
ingestion, and querying. Reuses rate-limiting and pagination patterns
from MiroFish.
"""

from __future__ import annotations

import logging
import time
import re
import threading
from typing import Any, Dict, List, Optional

import yaml

try:
    from zep_cloud.client import Zep
except ImportError:
    Zep = None  # type: ignore[assignment,misc]

from astrategy.config import settings

logger = logging.getLogger("astrategy.graph.builder")

# ── Rate limiter (adapted from MiroFish zep_rate_limiter) ──────

_DEFAULT_MAX_RPM = 5
_DEFAULT_MAX_RETRIES = 6
_DEFAULT_RETRY_BASE = 13.0


class _TokenBucket:
    """Thread-safe token bucket rate limiter."""

    def __init__(self, max_tokens: int, refill_period: float):
        self._max = max_tokens
        self._tokens = float(max_tokens)
        self._refill_rate = max_tokens / refill_period
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, timeout: float = 120.0) -> bool:
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return True
                wait = (1.0 - self._tokens) / self._refill_rate
            if time.monotonic() + wait > deadline:
                return False
            time.sleep(min(wait + 0.05, deadline - time.monotonic()))

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._max, self._tokens + elapsed * self._refill_rate)
        self._last_refill = now


# Module-level rate limiter singleton
_bucket: Optional[_TokenBucket] = None
_bucket_lock = threading.Lock()


def _get_bucket() -> _TokenBucket:
    global _bucket
    if _bucket is None:
        with _bucket_lock:
            if _bucket is None:
                rpm = int(settings.graph.rate_limit_rps)
                _bucket = _TokenBucket(max_tokens=max(rpm, 1), refill_period=60.0)
    return _bucket


def _is_rate_limit_error(exc: Exception) -> bool:
    status = getattr(exc, "status_code", None)
    if status == 429:
        return True
    msg = str(exc).lower()
    return "429" in msg and "rate limit" in msg


def _extract_retry_after(exc: Exception) -> Optional[float]:
    headers = getattr(exc, "headers", None)
    if isinstance(headers, dict):
        val = headers.get("retry-after") or headers.get("Retry-After")
        if val is not None:
            try:
                return float(val)
            except (ValueError, TypeError):
                pass
    msg = str(exc)
    match = re.search(r"retry-after['\"]?:\s*['\"]?(\d+)", msg, re.IGNORECASE)
    if match:
        return float(match.group(1))
    return None


def _rate_limited_call(
    func,
    *args,
    operation_name: str = "Zep API",
    max_retries: int = _DEFAULT_MAX_RETRIES,
    retry_base: float = _DEFAULT_RETRY_BASE,
    **kwargs,
):
    """Execute a Zep API call with rate limiting and 429 retry."""
    bucket = _get_bucket()

    for attempt in range(max_retries + 1):
        if not bucket.acquire(timeout=180.0):
            raise TimeoutError(f"Rate limiter timeout for {operation_name}")

        try:
            return func(*args, **kwargs)
        except Exception as e:
            if _is_rate_limit_error(e):
                if attempt >= max_retries:
                    raise
                retry_after = _extract_retry_after(e)
                if retry_after is not None:
                    wait = retry_after + 1.0
                else:
                    wait = retry_base * (2 ** attempt)
                wait = min(wait, 120.0)
                logger.warning(
                    "[%s] 429 Rate Limit, waiting %.0fs (attempt %d/%d)",
                    operation_name,
                    wait,
                    attempt + 1,
                    max_retries,
                )
                time.sleep(wait)
            else:
                raise


def _fetch_page_with_retry(api_call, *args, page_description="page", **kwargs):
    """Single page fetch with retry on transient errors."""
    max_retries = 3
    delay = 2.0
    last_exc = None

    for attempt in range(max_retries):
        try:
            return _rate_limited_call(
                api_call, *args, operation_name=page_description, **kwargs
            )
        except (ConnectionError, TimeoutError, OSError) as e:
            last_exc = e
            if attempt < max_retries - 1:
                logger.warning(
                    "%s attempt %d failed: %s, retrying in %.1fs",
                    page_description,
                    attempt + 1,
                    str(e)[:100],
                    delay,
                )
                time.sleep(delay)
                delay *= 2
            else:
                logger.error("%s failed after %d attempts", page_description, max_retries)

    assert last_exc is not None
    raise last_exc


# ── GraphBuilder ───────────────────────────────────────────────

_DEFAULT_PAGE_SIZE = 100
_MAX_ITEMS = 2000


class GraphBuilder:
    """
    Build and manage A-share knowledge graphs in Zep Cloud.

    Handles graph creation, ontology configuration, entity ingestion,
    and paginated data retrieval.
    """

    def __init__(self, api_key: Optional[str] = None):
        self._api_key = api_key or settings.graph.zep_api_key
        if not self._api_key:
            raise ValueError("ZEP_API_KEY 未配置。请在 .env 中设置 ZEP_API_KEY。")
        self._client = Zep(api_key=self._api_key)

    @property
    def client(self) -> Zep:
        return self._client

    # ── ontology ───────────────────────────────────────────────

    @staticmethod
    def load_ontology(yaml_path: str) -> dict:
        """
        Load an ontology definition from a YAML file.

        Expected YAML structure:
            entity_types:
              - name: Company
                description: ...
            edge_types:
              - name: SUPPLIES_TO
                source: Company
                target: Company

        Returns:
            Parsed ontology dict.
        """
        with open(yaml_path, "r", encoding="utf-8") as f:
            ontology = yaml.safe_load(f)
        logger.info("Loaded ontology from %s: %d entity types, %d edge types",
                     yaml_path,
                     len(ontology.get("entity_types", [])),
                     len(ontology.get("edge_types", [])))
        return ontology

    # ── graph lifecycle ────────────────────────────────────────

    def create_graph(self, name: str) -> str:
        """
        Create a new graph (Zep user/group) and return its ID.

        In Zep, graphs are scoped by user_id. We use the name as user_id.
        """
        _rate_limited_call(
            self._client.user.add,
            user_id=name,
            operation_name=f"create graph '{name}'",
        )
        logger.info("Created graph (user) '%s'", name)
        return name

    def set_ontology(self, graph_id: str, ontology: dict) -> None:
        """
        Apply an ontology definition to the graph.

        Serialises entity/edge types as an episode that instructs Zep
        to recognize these types during future ingestion.
        """
        raw_entities = ontology.get("entity_types", {})
        raw_edges = ontology.get("edge_types", {})

        # Normalise: YAML may use dict-of-dicts or list-of-dicts
        if isinstance(raw_entities, dict):
            entity_types = [{"name": k, **v} for k, v in raw_entities.items()]
        else:
            entity_types = raw_entities

        if isinstance(raw_edges, dict):
            edge_types = [{"name": k, **v} for k, v in raw_edges.items()]
        else:
            edge_types = raw_edges

        # Build ontology instruction text
        lines = ["[ONTOLOGY DEFINITION]", ""]
        lines.append("Entity Types:")
        for et in entity_types:
            desc = et.get("description", "")
            lines.append(f"  - {et['name']}: {desc}")

        lines.append("")
        lines.append("Edge (Relationship) Types:")
        for et in edge_types:
            src = et.get("source", "")
            tgt = et.get("target", "")
            desc = et.get("description", "")
            lines.append(f"  - {et['name']}: {src} -> {tgt} ({desc})")

        ontology_text = "\n".join(lines)

        _rate_limited_call(
            self._client.graph.add,
            user_id=graph_id,
            data=ontology_text,
            type="text",
            operation_name=f"set ontology for '{graph_id}'",
        )
        logger.info("Applied ontology (%d entities, %d edges) to graph '%s'",
                     len(entity_types), len(edge_types), graph_id)

    # ── entity ingestion ───────────────────────────────────────

    def add_companies(self, graph_id: str, companies: List[Dict[str, Any]]) -> None:
        """
        Batch add company entities to the graph.

        Each company dict should have at least:
          - code: stock code (e.g. "600519")
          - name: company name
          - industry: industry classification
        Optional: market_cap, pe_ratio, sector, description, etc.
        """
        for company in companies:
            text_parts = [
                f"公司: {company.get('name', '')}",
                f"股票代码: {company.get('code', '')}",
                f"行业: {company.get('industry', '')}",
            ]
            for key in ("sector", "market_cap", "pe_ratio", "description"):
                if key in company:
                    text_parts.append(f"{key}: {company[key]}")

            episode_text = "; ".join(text_parts)

            _rate_limited_call(
                self._client.graph.add,
                user_id=graph_id,
                data=episode_text,
                type="text",
                operation_name=f"add company {company.get('code', '?')}",
            )

        logger.info("Added %d companies to graph '%s'", len(companies), graph_id)

    def add_relationships(self, graph_id: str, edges: List[Dict[str, Any]]) -> None:
        """
        Batch add relationship edges to the graph.

        Each edge dict should have:
          - source: source entity name or code
          - target: target entity name or code
          - relation: relationship type (e.g. "SUPPLIES_TO")
          - description: (optional) relationship detail
        """
        for edge in edges:
            text = (
                f"{edge.get('source', '')} {edge.get('relation', 'RELATED_TO')} "
                f"{edge.get('target', '')}"
            )
            desc = edge.get("description", "")
            if desc:
                text += f" ({desc})"

            _rate_limited_call(
                self._client.graph.add,
                user_id=graph_id,
                data=text,
                type="text",
                operation_name=f"add edge {edge.get('source', '?')}->{edge.get('target', '?')}",
            )

        logger.info("Added %d relationships to graph '%s'", len(edges), graph_id)

    def add_episodes(
        self,
        graph_id: str,
        texts: List[str],
        batch_size: int = 3,
    ) -> None:
        """
        Add free-text episodes to the graph for Zep to extract entities/edges.

        Args:
            graph_id: Target graph ID.
            texts: List of text episodes (news, announcements, etc.).
            batch_size: Number of texts to combine per API call to reduce
                        request count (Zep processes them as one episode).
        """
        # Process in batches
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            combined = "\n\n---\n\n".join(batch)

            _rate_limited_call(
                self._client.graph.add,
                user_id=graph_id,
                data=combined,
                type="text",
                operation_name=f"add episodes batch {i // batch_size + 1}",
            )

        logger.info(
            "Added %d episodes (%d batches) to graph '%s'",
            len(texts),
            (len(texts) + batch_size - 1) // batch_size,
            graph_id,
        )

    # ── paginated retrieval ────────────────────────────────────

    def get_all_nodes(
        self,
        graph_id: str,
        page_size: int = _DEFAULT_PAGE_SIZE,
        max_items: int = _MAX_ITEMS,
    ) -> List[Dict[str, Any]]:
        """Fetch all nodes from the graph with automatic pagination."""
        all_nodes: List[Any] = []
        cursor: Optional[str] = None
        page_num = 0

        while True:
            kwargs: Dict[str, Any] = {"limit": page_size}
            if cursor is not None:
                kwargs["uuid_cursor"] = cursor

            page_num += 1
            batch = _fetch_page_with_retry(
                self._client.graph.node.get_by_user_id,
                graph_id,
                page_description=f"fetch nodes page {page_num} (graph={graph_id})",
                **kwargs,
            )
            if not batch:
                break

            all_nodes.extend(batch)
            if len(all_nodes) >= max_items:
                all_nodes = all_nodes[:max_items]
                logger.warning("Node count reached limit (%d) for graph '%s'", max_items, graph_id)
                break
            if len(batch) < page_size:
                break

            cursor = getattr(batch[-1], "uuid_", None) or getattr(batch[-1], "uuid", None)
            if cursor is None:
                break

        # Normalise to dicts
        result = []
        for node in all_nodes:
            result.append(_node_to_dict(node))
        return result

    def get_all_edges(
        self,
        graph_id: str,
        page_size: int = _DEFAULT_PAGE_SIZE,
    ) -> List[Dict[str, Any]]:
        """Fetch all edges from the graph with automatic pagination."""
        all_edges: List[Any] = []
        cursor: Optional[str] = None
        page_num = 0

        while True:
            kwargs: Dict[str, Any] = {"limit": page_size}
            if cursor is not None:
                kwargs["uuid_cursor"] = cursor

            page_num += 1
            batch = _fetch_page_with_retry(
                self._client.graph.edge.get_by_user_id,
                graph_id,
                page_description=f"fetch edges page {page_num} (graph={graph_id})",
                **kwargs,
            )
            if not batch:
                break

            all_edges.extend(batch)
            if len(batch) < page_size:
                break

            cursor = getattr(batch[-1], "uuid_", None) or getattr(batch[-1], "uuid", None)
            if cursor is None:
                break

        result = []
        for edge in all_edges:
            result.append(_edge_to_dict(edge))
        return result

    # ── search ─────────────────────────────────────────────────

    def search(
        self,
        graph_id: str,
        query: str,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Search the graph for relevant facts/entities.

        Args:
            graph_id: Graph to search.
            query: Natural language query.
            limit: Max results.

        Returns:
            List of search result dicts with fact, score, etc.
        """
        response = _rate_limited_call(
            self._client.graph.search,
            user_id=graph_id,
            query=query,
            limit=limit,
            operation_name=f"search graph '{graph_id}'",
        )

        results = []
        if response and hasattr(response, "edges"):
            for edge in response.edges or []:
                results.append(
                    {
                        "fact": getattr(edge, "fact", ""),
                        "source": getattr(edge, "source_node_name", ""),
                        "target": getattr(edge, "target_node_name", ""),
                        "relation": getattr(edge, "name", ""),
                        "score": getattr(edge, "score", 0.0),
                        "created_at": str(getattr(edge, "created_at", "")),
                    }
                )

        return results


# ── helpers ────────────────────────────────────────────────────


def _node_to_dict(node) -> Dict[str, Any]:
    """Convert a Zep node object to a plain dict."""
    return {
        "uuid": getattr(node, "uuid_", None) or getattr(node, "uuid", ""),
        "name": getattr(node, "name", ""),
        "labels": getattr(node, "labels", []) or [],
        "summary": getattr(node, "summary", ""),
        "attributes": getattr(node, "attributes", {}) or {},
    }


def _edge_to_dict(edge) -> Dict[str, Any]:
    """Convert a Zep edge object to a plain dict."""
    return {
        "uuid": getattr(edge, "uuid_", None) or getattr(edge, "uuid", ""),
        "source": getattr(edge, "source_node_uuid", ""),
        "source_name": getattr(edge, "source_node_name", ""),
        "target": getattr(edge, "target_node_uuid", ""),
        "target_name": getattr(edge, "target_node_name", ""),
        "relation": getattr(edge, "name", ""),
        "fact": getattr(edge, "fact", ""),
        "weight": getattr(edge, "weight", 1.0),
        "created_at": str(getattr(edge, "created_at", "")),
    }
