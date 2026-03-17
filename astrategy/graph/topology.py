"""
Pure-Python topology analysis on graph structures.

Computes centrality metrics, connected components, shortest paths,
and community detection without external graph library dependencies.
All algorithms work on adjacency-list representations built from
node/edge dicts returned by GraphBuilder.
"""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("astrategy.graph.topology")


class TopologyAnalyzer:
    """
    Graph topology analysis for computing graph-based factors.

    Operates on lists of node dicts and edge dicts (as returned by
    GraphBuilder.get_all_nodes / get_all_edges).

    Node dict must have at least: {"name": str} or {"uuid": str}
    Edge dict must have at least: {"source_name": str, "target_name": str}
                                  or {"source": str, "target": str}
    """

    @staticmethod
    def _build_adjacency(
        nodes: List[Dict[str, Any]],
        edges: List[Dict[str, Any]],
        directed: bool = False,
    ) -> Tuple[List[str], Dict[str, Dict[str, float]]]:
        """
        Build adjacency list from node/edge dicts.

        Returns:
            (node_ids, adjacency) where adjacency[u][v] = weight.
        """
        node_ids = []
        seen = set()
        for n in nodes:
            nid = n.get("name") or n.get("uuid", "")
            if nid and nid not in seen:
                node_ids.append(nid)
                seen.add(nid)

        adj: Dict[str, Dict[str, float]] = defaultdict(dict)

        for e in edges:
            src = e.get("source_name") or e.get("source", "")
            tgt = e.get("target_name") or e.get("target", "")
            weight = float(e.get("weight", 1.0))
            if not src or not tgt:
                continue

            # Ensure endpoints are in node list
            for nid in (src, tgt):
                if nid not in seen:
                    node_ids.append(nid)
                    seen.add(nid)

            adj[src][tgt] = weight
            if not directed:
                adj[tgt][src] = weight

        return node_ids, dict(adj)

    # ── PageRank ───────────────────────────────────────────────

    @staticmethod
    def pagerank(
        nodes: List[Dict[str, Any]],
        edges: List[Dict[str, Any]],
        damping: float = 0.85,
        max_iter: int = 100,
        tol: float = 1e-6,
    ) -> Dict[str, float]:
        """
        Compute PageRank scores (supply-chain centrality).

        Uses the power-iteration method on a directed graph.

        Args:
            nodes: Node dicts.
            edges: Edge dicts.
            damping: Damping factor (default 0.85).
            max_iter: Maximum iterations.
            tol: Convergence tolerance.

        Returns:
            Dict mapping node_name -> PageRank score.
        """
        node_ids, _ = TopologyAnalyzer._build_adjacency(nodes, edges, directed=True)
        n = len(node_ids)
        if n == 0:
            return {}

        # Build directed adjacency for PageRank
        out_links: Dict[str, List[str]] = defaultdict(list)
        for e in edges:
            src = e.get("source_name") or e.get("source", "")
            tgt = e.get("target_name") or e.get("target", "")
            if src and tgt:
                out_links[src].append(tgt)

        # Initialize
        rank = {nid: 1.0 / n for nid in node_ids}
        node_set = set(node_ids)

        for iteration in range(max_iter):
            new_rank = {}
            # Dangling node mass (nodes with no out-links)
            dangling_sum = sum(
                rank[nid] for nid in node_ids if not out_links.get(nid)
            )

            for nid in node_ids:
                # Teleport + dangling redistribution
                new_rank[nid] = (1.0 - damping) / n + damping * dangling_sum / n

            # Add contributions from incoming edges
            for src, targets in out_links.items():
                if src not in node_set:
                    continue
                out_degree = len(targets)
                contribution = damping * rank[src] / out_degree
                for tgt in targets:
                    if tgt in new_rank:
                        new_rank[tgt] += contribution

            # Check convergence
            diff = sum(abs(new_rank[nid] - rank[nid]) for nid in node_ids)
            rank = new_rank
            if diff < tol:
                logger.debug("PageRank converged after %d iterations", iteration + 1)
                break

        return rank

    # ── Degree Centrality ──────────────────────────────────────

    @staticmethod
    def degree_centrality(
        nodes: List[Dict[str, Any]],
        edges: List[Dict[str, Any]],
    ) -> Dict[str, float]:
        """
        Compute normalized degree centrality.

        Returns:
            Dict mapping node_name -> centrality in [0, 1].
        """
        node_ids, adj = TopologyAnalyzer._build_adjacency(nodes, edges, directed=False)
        n = len(node_ids)
        if n <= 1:
            return {nid: 0.0 for nid in node_ids}

        result = {}
        for nid in node_ids:
            degree = len(adj.get(nid, {}))
            result[nid] = degree / (n - 1)
        return result

    # ── Betweenness Centrality ─────────────────────────────────

    @staticmethod
    def betweenness_centrality(
        nodes: List[Dict[str, Any]],
        edges: List[Dict[str, Any]],
    ) -> Dict[str, float]:
        """
        Compute betweenness centrality (Brandes' algorithm).

        Measures how often a node lies on shortest paths between other nodes.
        Useful for identifying key intermediaries in supply chains.

        Returns:
            Dict mapping node_name -> normalized betweenness centrality.
        """
        node_ids, adj = TopologyAnalyzer._build_adjacency(nodes, edges, directed=False)
        n = len(node_ids)
        if n <= 2:
            return {nid: 0.0 for nid in node_ids}

        centrality = {nid: 0.0 for nid in node_ids}

        for s in node_ids:
            # BFS from s
            stack: List[str] = []
            predecessors: Dict[str, List[str]] = {nid: [] for nid in node_ids}
            sigma: Dict[str, int] = {nid: 0 for nid in node_ids}
            sigma[s] = 1
            dist: Dict[str, int] = {nid: -1 for nid in node_ids}
            dist[s] = 0

            queue = deque([s])
            while queue:
                v = queue.popleft()
                stack.append(v)
                for w in adj.get(v, {}):
                    # First visit
                    if dist[w] < 0:
                        dist[w] = dist[v] + 1
                        queue.append(w)
                    # Shortest path via v?
                    if dist[w] == dist[v] + 1:
                        sigma[w] += sigma[v]
                        predecessors[w].append(v)

            # Back-propagation
            delta = {nid: 0.0 for nid in node_ids}
            while stack:
                w = stack.pop()
                for v in predecessors[w]:
                    if sigma[w] > 0:
                        delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w])
                if w != s:
                    centrality[w] += delta[w]

        # Normalize (undirected: divide by 2)
        norm = (n - 1) * (n - 2)
        if norm > 0:
            for nid in node_ids:
                centrality[nid] /= norm

        return centrality

    # ── Connected Components ───────────────────────────────────

    @staticmethod
    def find_connected_components(
        nodes: List[Dict[str, Any]],
        edges: List[Dict[str, Any]],
    ) -> List[List[str]]:
        """
        Find connected components using BFS.

        Returns:
            List of components, each component is a list of node names.
            Sorted by component size (largest first).
        """
        node_ids, adj = TopologyAnalyzer._build_adjacency(nodes, edges, directed=False)
        visited: Set[str] = set()
        components: List[List[str]] = []

        for nid in node_ids:
            if nid in visited:
                continue
            # BFS
            component: List[str] = []
            queue = deque([nid])
            visited.add(nid)
            while queue:
                v = queue.popleft()
                component.append(v)
                for w in adj.get(v, {}):
                    if w not in visited:
                        visited.add(w)
                        queue.append(w)
            components.append(component)

        components.sort(key=len, reverse=True)
        return components

    # ── Shortest Path ──────────────────────────────────────────

    @staticmethod
    def shortest_path(
        edges: List[Dict[str, Any]],
        source: str,
        target: str,
    ) -> List[str]:
        """
        Find shortest path (unweighted BFS) between source and target.

        Useful for measuring event propagation distance in supply chains.

        Args:
            edges: Edge dicts.
            source: Source node name.
            target: Target node name.

        Returns:
            List of node names forming the path, or empty list if unreachable.
        """
        # Build undirected adjacency
        adj: Dict[str, Set[str]] = defaultdict(set)
        for e in edges:
            src = e.get("source_name") or e.get("source", "")
            tgt = e.get("target_name") or e.get("target", "")
            if src and tgt:
                adj[src].add(tgt)
                adj[tgt].add(src)

        if source == target:
            return [source]

        # BFS
        visited = {source}
        queue = deque([(source, [source])])
        while queue:
            v, path = queue.popleft()
            for w in adj.get(v, set()):
                if w == target:
                    return path + [w]
                if w not in visited:
                    visited.add(w)
                    queue.append((w, path + [w]))

        return []  # unreachable

    # ── N-hop Neighbors ────────────────────────────────────────

    @staticmethod
    def get_neighbors(
        edges: List[Dict[str, Any]],
        node_id: str,
        depth: int = 1,
    ) -> List[str]:
        """
        Get all nodes within N hops of node_id.

        Args:
            edges: Edge dicts.
            node_id: Starting node name.
            depth: Number of hops (default 1).

        Returns:
            List of neighbor node names (excluding the starting node).
        """
        adj: Dict[str, Set[str]] = defaultdict(set)
        for e in edges:
            src = e.get("source_name") or e.get("source", "")
            tgt = e.get("target_name") or e.get("target", "")
            if src and tgt:
                adj[src].add(tgt)
                adj[tgt].add(src)

        visited = {node_id}
        current_level = {node_id}

        for _ in range(depth):
            next_level: Set[str] = set()
            for v in current_level:
                for w in adj.get(v, set()):
                    if w not in visited:
                        next_level.add(w)
                        visited.add(w)
            current_level = next_level

        visited.discard(node_id)
        return sorted(visited)

    # ── Shock Propagation ──────────────────────────────────────

    @staticmethod
    def propagate_shock(
        edges: List[Dict[str, Any]],
        source: str,
        max_hops: int = 3,
        decay: float = 0.5,
        relation_types: Optional[Set[str]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Propagate a shock from *source* along directed edges with decay.

        Models how an event hitting company A ripples through supply-chain
        / industry-chain / capital-flow relationships to downstream companies
        B, C, D, …  Each hop attenuates the shock by *decay* (default 50%).

        Parameters
        ----------
        edges :
            Edge dicts (must contain source_name/target_name + relation).
        source :
            Starting node (stock code or company name).
        max_hops :
            Maximum propagation depth (default 3).
        decay :
            Multiplicative decay per hop (default 0.5).
        relation_types :
            Set of edge relations to traverse.  ``None`` = use defaults
            (SUPPLIES_TO, CUSTOMER_OF, COOPERATES_WITH).

        Returns
        -------
        dict
            ``{node_name: {"shock_weight": float, "hop": int,
            "path": [str], "relation_chain": [str]}}``
            Only includes downstream nodes (excludes *source*).
        """
        if relation_types is None:
            relation_types = {
                "SUPPLIES_TO", "CUSTOMER_OF", "COOPERATES_WITH",
                "COMPETES_WITH", "HOLDS_SHARES",
            }

        # Build directed adjacency: source → [(target, relation, weight)]
        # Bidirectional relations are added in both directions.
        _BIDIR_RELS = {"COOPERATES_WITH", "COMPETES_WITH", "HOLDS_SHARES"}
        directed: Dict[str, List[Tuple[str, str, float]]] = defaultdict(list)
        for e in edges:
            rel = e.get("relation", "")
            if rel not in relation_types:
                continue
            src = e.get("source_name") or e.get("source", "")
            tgt = e.get("target_name") or e.get("target", "")
            w = float(e.get("weight", 1.0))
            if src and tgt:
                directed[src].append((tgt, rel, w))
                if rel in _BIDIR_RELS:
                    directed[tgt].append((src, rel, w))

        # BFS with decay
        result: Dict[str, Dict[str, Any]] = {}
        # queue entries: (node, current_shock_weight, hop, path, relation_chain)
        queue: deque = deque()
        queue.append((source, 1.0, 0, [source], []))
        visited: Set[str] = {source}

        while queue:
            node, shock_w, hop, path, rel_chain = queue.popleft()
            if hop >= max_hops:
                continue
            for neighbor, rel, edge_w in directed.get(node, []):
                if neighbor in visited:
                    continue
                visited.add(neighbor)
                next_shock = shock_w * decay * edge_w
                next_path = path + [neighbor]
                next_rels = rel_chain + [rel]
                result[neighbor] = {
                    "shock_weight": round(next_shock, 6),
                    "hop": hop + 1,
                    "path": next_path,
                    "relation_chain": next_rels,
                }
                queue.append((neighbor, next_shock, hop + 1, next_path, next_rels))

        logger.info(
            "Shock propagation from '%s': %d downstream nodes (max_hops=%d, decay=%.2f)",
            source, len(result), max_hops, decay,
        )
        return result

    # ── Community Detection (Label Propagation) ────────────────

    @staticmethod
    def community_detection(
        nodes: List[Dict[str, Any]],
        edges: List[Dict[str, Any]],
        max_iter: int = 50,
    ) -> Dict[str, int]:
        """
        Simple label propagation for community detection.

        Each node starts with a unique label. In each iteration, each node
        adopts the most frequent label among its neighbors (ties broken by
        smallest label ID for determinism).

        Args:
            nodes: Node dicts.
            edges: Edge dicts.
            max_iter: Maximum iterations.

        Returns:
            Dict mapping node_name -> community_id (int).
        """
        node_ids, adj = TopologyAnalyzer._build_adjacency(nodes, edges, directed=False)
        n = len(node_ids)
        if n == 0:
            return {}

        # Initialize each node with a unique label
        labels = {nid: i for i, nid in enumerate(node_ids)}

        import random

        for iteration in range(max_iter):
            changed = False
            # Process in random order for better convergence
            order = list(node_ids)
            random.shuffle(order)

            for nid in order:
                neighbors = adj.get(nid, {})
                if not neighbors:
                    continue

                # Count neighbor labels (weighted by edge weight)
                label_weights: Dict[int, float] = defaultdict(float)
                for neighbor, weight in neighbors.items():
                    label_weights[labels[neighbor]] += weight

                if not label_weights:
                    continue

                # Pick the label with highest weight (ties: smallest label)
                max_weight = max(label_weights.values())
                best_labels = [
                    lbl for lbl, w in label_weights.items() if w == max_weight
                ]
                best_label = min(best_labels)

                if labels[nid] != best_label:
                    labels[nid] = best_label
                    changed = True

            if not changed:
                logger.debug(
                    "Label propagation converged after %d iterations", iteration + 1
                )
                break

        # Renumber communities to 0..k-1
        unique_labels = sorted(set(labels.values()))
        label_map = {old: new for new, old in enumerate(unique_labels)}
        return {nid: label_map[lbl] for nid, lbl in labels.items()}
