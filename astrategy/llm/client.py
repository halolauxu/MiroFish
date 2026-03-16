"""
Enhanced LLM client for AStrategy.

Based on MiroFish's llm_client.py with additions:
- Automatic retry with exponential backoff
- Token estimation (Chinese / English aware)
- Integration with LLMCache and CostTracker
- Configurable via astrategy.config.settings
"""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any, Dict, List, Optional

from openai import OpenAI

from astrategy.config import settings

logger = logging.getLogger("astrategy.llm.client")


class LLMClient:
    """OpenAI-compatible LLM client with retry, caching, and cost tracking."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        cache: Optional[Any] = None,
        cost_tracker: Optional[Any] = None,
    ):
        self.api_key = api_key or settings.llm.api_key
        self.base_url = base_url or settings.llm.base_url
        self.model = model or settings.llm.model_name

        if not self.api_key:
            raise ValueError(
                "LLM_API_KEY 未配置。请在 .env 中设置 LLM_API_KEY。"
            )

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=settings.llm.request_timeout,
        )
        self._cache = cache
        self._cost_tracker = cost_tracker
        self._strategy_tag: Optional[str] = None

    # ── public helpers ─────────────────────────────────────────

    def set_strategy(self, strategy_name: str) -> None:
        """Tag subsequent calls with a strategy name for cost tracking."""
        self._strategy_tag = strategy_name

    # ── core chat methods ──────────────────────────────────────

    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        response_format: Optional[Dict] = None,
    ) -> str:
        """
        Send a chat completion request.

        Args:
            messages: Conversation messages.
            temperature: Sampling temperature (default from config).
            max_tokens: Max tokens to generate (default from config).
            response_format: Optional response format dict (e.g. JSON mode).

        Returns:
            Model response text (with <think> blocks removed).
        """
        temperature = temperature if temperature is not None else settings.llm.temperature
        max_tokens = max_tokens or settings.llm.max_tokens

        kwargs: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if response_format:
            kwargs["response_format"] = response_format

        response = self.client.chat.completions.create(**kwargs)
        content = response.choices[0].message.content or ""

        # Strip <think>...</think> blocks from reasoning models
        content = re.sub(r"<think>[\s\S]*?</think>", "", content).strip()

        # Track cost if tracker and usage info are available
        usage = getattr(response, "usage", None)
        if usage and self._cost_tracker:
            self._cost_tracker.log_usage(
                strategy=self._strategy_tag or "unknown",
                model=self.model,
                input_tokens=usage.prompt_tokens or 0,
                output_tokens=usage.completion_tokens or 0,
            )

        return content

    def chat_json(
        self,
        messages: List[Dict[str, str]],
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Send a chat request and parse JSON response.

        Uses JSON mode when supported, then strips markdown fences as fallback.

        Returns:
            Parsed JSON dict.

        Raises:
            ValueError: If the response cannot be parsed as JSON.
        """
        temperature = temperature if temperature is not None else 0.3

        raw = self.chat(
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
        )

        # Clean markdown code fences
        cleaned = raw.strip()
        cleaned = re.sub(r"^```(?:json)?\s*\n?", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\n?```\s*$", "", cleaned)
        cleaned = cleaned.strip()

        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            raise ValueError(f"LLM返回的JSON格式无效: {cleaned[:500]}")

    def chat_with_retry(
        self,
        messages: List[Dict[str, str]],
        max_retries: int = 3,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> str:
        """
        Chat with automatic retry and exponential backoff.

        Retries on transient errors (timeouts, rate limits, server errors).
        Non-retryable errors (auth, bad request) are raised immediately.

        Args:
            messages: Conversation messages.
            max_retries: Maximum number of retry attempts.
            temperature: Sampling temperature.
            max_tokens: Max tokens.

        Returns:
            Model response text.

        Raises:
            The last exception if all retries are exhausted.
        """
        last_exc: Optional[Exception] = None
        base_delay = 2.0

        for attempt in range(max_retries + 1):
            try:
                return self.chat(
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
            except Exception as e:
                last_exc = e
                error_str = str(e).lower()

                # Don't retry auth errors or bad requests
                if any(
                    code in error_str
                    for code in ("401", "403", "invalid_api_key", "authentication")
                ):
                    raise

                if attempt < max_retries:
                    delay = base_delay * (2 ** attempt)
                    # Check for rate-limit retry-after header
                    retry_after = getattr(e, "retry_after", None)
                    if retry_after is not None:
                        delay = max(delay, float(retry_after))
                    delay = min(delay, 60.0)

                    logger.warning(
                        "LLM call failed (attempt %d/%d): %s — retrying in %.1fs",
                        attempt + 1,
                        max_retries + 1,
                        str(e)[:200],
                        delay,
                    )
                    time.sleep(delay)
                else:
                    logger.error(
                        "LLM call failed after %d attempts: %s",
                        max_retries + 1,
                        str(e)[:300],
                    )

        assert last_exc is not None
        raise last_exc

    # ── token estimation ───────────────────────────────────────

    @staticmethod
    def estimate_tokens(text: str) -> int:
        """
        Rough token count estimation.

        Chinese characters are approximately 1 token per 1.5 characters.
        English text is approximately 1 token per 4 characters.
        We count Chinese chars separately for better accuracy.
        """
        if not text:
            return 0

        # Count CJK characters
        cjk_chars = sum(
            1
            for ch in text
            if "\u4e00" <= ch <= "\u9fff"
            or "\u3400" <= ch <= "\u4dbf"
            or "\uf900" <= ch <= "\ufaff"
        )
        non_cjk_chars = len(text) - cjk_chars

        # CJK: ~1.5 chars per token; ASCII/Latin: ~4 chars per token
        cjk_tokens = cjk_chars / 1.5
        non_cjk_tokens = non_cjk_chars / 4.0

        return max(1, int(cjk_tokens + non_cjk_tokens))
