"""
Batch scheduler for efficient LLM calls.

Supports concurrent execution with rate limiting, automatic caching,
and per-strategy token usage tracking.
"""

from __future__ import annotations

import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from queue import PriorityQueue
from threading import Lock
from typing import Any, Dict, List, Optional

from .cache import LLMCache
from .client import LLMClient
from .cost_tracker import CostTracker

logger = logging.getLogger("astrategy.llm.batch_scheduler")


@dataclass(order=True)
class _Task:
    """Internal priority-queue item. Lower priority number = higher priority."""

    priority: int
    prompt_id: str = field(compare=False)
    messages: list = field(compare=False)
    task_id: str = field(compare=False)
    strategy: str = field(compare=False, default="")
    temperature: float = field(compare=False, default=0.3)
    max_tokens: int = field(compare=False, default=4096)


class BatchScheduler:
    """
    Schedule and execute batches of LLM calls with concurrency control.

    Features:
      - Priority queue ordering
      - Concurrent execution via ThreadPoolExecutor
      - Automatic LLM cache integration
      - Per-strategy cost tracking
    """

    def __init__(
        self,
        client: Optional[LLMClient] = None,
        cache: Optional[LLMCache] = None,
        cost_tracker: Optional[CostTracker] = None,
        default_strategy: str = "batch",
    ):
        self._client = client or LLMClient()
        self._cache = cache or LLMCache()
        self._cost_tracker = cost_tracker
        self._default_strategy = default_strategy

        # Wire up cost tracker
        if self._cost_tracker:
            self._client._cost_tracker = self._cost_tracker

        self._queue: PriorityQueue[_Task] = PriorityQueue()
        self._results: Dict[str, str] = {}
        self._errors: Dict[str, str] = {}
        self._lock = Lock()

    # ── submit ─────────────────────────────────────────────────

    def submit(
        self,
        prompt_id: str,
        messages: List[Dict[str, str]],
        priority: int = 0,
        strategy: str = "",
        temperature: float = 0.3,
        max_tokens: int = 4096,
    ) -> str:
        """
        Submit a single prompt to the queue.

        Args:
            prompt_id: Caller-defined identifier for this prompt.
            messages: Chat messages.
            priority: Lower number = higher priority (default 0).
            strategy: Strategy name for cost tracking.
            temperature: Sampling temperature.
            max_tokens: Max generation tokens.

        Returns:
            A unique task_id to retrieve results.
        """
        task_id = uuid.uuid4().hex[:12]
        task = _Task(
            priority=priority,
            prompt_id=prompt_id,
            messages=messages,
            task_id=task_id,
            strategy=strategy or self._default_strategy,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        self._queue.put(task)
        logger.debug("Submitted task %s (prompt_id=%s, priority=%d)", task_id, prompt_id, priority)
        return task_id

    def submit_batch(
        self,
        items: List[Dict[str, Any]],
    ) -> List[str]:
        """
        Submit multiple prompts at once.

        Each item dict should contain:
          - prompt_id (str): identifier
          - messages (list): chat messages
          - priority (int, optional): default 0
          - strategy (str, optional): strategy name
          - temperature (float, optional): default 0.3
          - max_tokens (int, optional): default 4096

        Returns:
            List of task_ids in the same order as items.
        """
        task_ids = []
        for item in items:
            tid = self.submit(
                prompt_id=item["prompt_id"],
                messages=item["messages"],
                priority=item.get("priority", 0),
                strategy=item.get("strategy", ""),
                temperature=item.get("temperature", 0.3),
                max_tokens=item.get("max_tokens", 4096),
            )
            task_ids.append(tid)
        return task_ids

    # ── process ────────────────────────────────────────────────

    def process_queue(
        self,
        max_concurrent: int = 5,
    ) -> Dict[str, str]:
        """
        Process all queued tasks with bounded concurrency.

        Uses LLMCache to skip calls whose results are already cached.

        Args:
            max_concurrent: Max number of parallel LLM calls.

        Returns:
            Dict mapping task_id -> response text.
            Tasks that failed have their errors logged but are excluded
            from the result (see self.get_errors()).
        """
        # Drain queue into a list (sorted by priority via PriorityQueue)
        tasks: List[_Task] = []
        while not self._queue.empty():
            tasks.append(self._queue.get_nowait())

        if not tasks:
            logger.info("No tasks in queue")
            return {}

        logger.info(
            "Processing %d tasks with max_concurrent=%d", len(tasks), max_concurrent
        )

        results: Dict[str, str] = {}
        errors: Dict[str, str] = {}

        def _execute_task(task: _Task) -> tuple[str, Optional[str], Optional[str]]:
            """Returns (task_id, result_or_None, error_or_None)."""
            # Check cache first
            cache_key = self._cache.make_key(
                model=self._client.model,
                messages=task.messages,
                temperature=task.temperature,
            )
            cached = self._cache.get(cache_key)
            if cached is not None:
                logger.debug("Cache hit for task %s (prompt_id=%s)", task.task_id, task.prompt_id)
                return task.task_id, cached, None

            # Call LLM
            try:
                self._client.set_strategy(task.strategy)
                result = self._client.chat_with_retry(
                    messages=task.messages,
                    temperature=task.temperature,
                    max_tokens=task.max_tokens,
                    max_retries=3,
                )
                # Cache the result
                ttl_hours = settings_llm_cache_ttl_hours()
                self._cache.set(
                    key=cache_key,
                    result=result,
                    ttl_hours=ttl_hours,
                    model=self._client.model,
                )
                return task.task_id, result, None
            except Exception as e:
                logger.error(
                    "Task %s (prompt_id=%s) failed: %s",
                    task.task_id,
                    task.prompt_id,
                    str(e)[:300],
                )
                return task.task_id, None, str(e)

        with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = {
                executor.submit(_execute_task, task): task for task in tasks
            }
            for future in as_completed(futures):
                task_id, result, error = future.result()
                if result is not None:
                    results[task_id] = result
                if error is not None:
                    errors[task_id] = error

        with self._lock:
            self._results.update(results)
            self._errors.update(errors)

        logger.info(
            "Batch complete: %d succeeded, %d failed",
            len(results),
            len(errors),
        )
        return results

    # ── result access ──────────────────────────────────────────

    def get_result(self, task_id: str) -> Optional[str]:
        """Get the result for a completed task, or None if not found."""
        return self._results.get(task_id)

    def get_errors(self) -> Dict[str, str]:
        """Return all task errors from the last process_queue run."""
        return dict(self._errors)

    def clear(self) -> None:
        """Clear all stored results and errors."""
        with self._lock:
            self._results.clear()
            self._errors.clear()


def settings_llm_cache_ttl_hours() -> float:
    """Get cache TTL from settings, converting seconds to hours."""
    from astrategy.config import settings

    return settings.llm.cache_ttl / 3600.0
