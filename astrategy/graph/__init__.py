"""
AStrategy Graph layer.

Provides graph construction (via Zep Cloud) and pure-Python
topology analysis for computing graph-based factors.
"""

from .topology import TopologyAnalyzer

# GraphBuilder requires zep_cloud — lazy import to avoid hard dependency
try:
    from .builder import GraphBuilder
except ImportError:
    GraphBuilder = None  # type: ignore[assignment,misc]

__all__ = ["GraphBuilder", "TopologyAnalyzer"]
