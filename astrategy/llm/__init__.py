"""
AStrategy LLM layer.

Provides an enhanced LLM client with caching, batch scheduling,
and cost tracking for A-share strategy research.
"""

from typing import Optional

from .client import LLMClient
from .batch_scheduler import BatchScheduler
from .cache import LLMCache
from .cost_tracker import CostTracker

__all__ = ["LLMClient", "BatchScheduler", "LLMCache", "CostTracker"]


# ---------------------------------------------------------------------------
# Factory: creates a fully-wired LLMClient with cache + cost tracking
# ---------------------------------------------------------------------------
_default_cache: Optional[LLMCache] = None
_default_tracker: Optional[CostTracker] = None


def create_llm_client(strategy_name: Optional[str] = None) -> LLMClient:
    """Create an LLMClient with shared LLMCache and CostTracker singletons.

    Parameters
    ----------
    strategy_name : str | None
        If provided, tags all LLM calls for per-strategy cost tracking.

    Returns
    -------
    LLMClient
        Fully-wired client instance.
    """
    global _default_cache, _default_tracker

    if _default_cache is None:
        _default_cache = LLMCache()
    if _default_tracker is None:
        _default_tracker = CostTracker()

    client = LLMClient(cache=_default_cache, cost_tracker=_default_tracker)
    if strategy_name:
        client.set_strategy(strategy_name)
    return client


def get_cost_tracker() -> CostTracker:
    """Return the shared CostTracker singleton."""
    global _default_tracker
    if _default_tracker is None:
        _default_tracker = CostTracker()
    return _default_tracker


def get_cache() -> LLMCache:
    """Return the shared LLMCache singleton."""
    global _default_cache
    if _default_cache is None:
        _default_cache = LLMCache()
    return _default_cache
