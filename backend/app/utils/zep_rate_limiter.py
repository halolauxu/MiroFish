"""Zep Cloud API 全局速率限制器。

Zep 免费计划限制为每分钟 5 次请求。本模块提供：
1. 全局令牌桶限速器 — 确保所有 Zep 调用共享同一速率窗口
2. rate_limited_call() — 自动限速 + 429 重试的通用调用包装
3. get_rate_limited_client() — 获取共享 Zep 客户端（可选）
"""

from __future__ import annotations

import re
import time
import threading
from typing import Any, Callable, TypeVar

from ..utils.logger import get_logger

logger = get_logger('mirofish.zep_rate_limiter')

T = TypeVar('T')

# ── 全局配置（可通过环境变量覆盖） ──────────────────────────
_DEFAULT_MAX_RPM = 5          # Zep FREE 计划: 5 requests/minute
_DEFAULT_MAX_RETRIES = 6      # 429 最多重试 6 次（总等待 ~4 分钟足够冷却）
_DEFAULT_RETRY_BASE = 13.0    # 429 重试基础等待秒数


class _TokenBucket:
    """线程安全的令牌桶限速器。"""

    def __init__(self, max_tokens: int, refill_period: float):
        """
        Args:
            max_tokens: 桶容量（= 每 refill_period 允许的最大请求数）
            refill_period: 令牌填满所需秒数（60.0 = 每分钟）
        """
        self._max = max_tokens
        self._tokens = float(max_tokens)
        self._refill_rate = max_tokens / refill_period  # tokens/sec
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self, timeout: float = 120.0) -> bool:
        """阻塞等待直到获得一个令牌，或超时返回 False。"""
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return True
                # 计算下一个令牌到达时间
                wait = (1.0 - self._tokens) / self._refill_rate
            if time.monotonic() + wait > deadline:
                return False
            time.sleep(min(wait + 0.05, deadline - time.monotonic()))

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._max, self._tokens + elapsed * self._refill_rate)
        self._last_refill = now


# ── 全局单例 ──────────────────────────────────────────────
_bucket: _TokenBucket | None = None
_bucket_lock = threading.Lock()


def _get_bucket() -> _TokenBucket:
    global _bucket
    if _bucket is None:
        with _bucket_lock:
            if _bucket is None:
                import os
                rpm = int(os.environ.get('ZEP_MAX_RPM', str(_DEFAULT_MAX_RPM)))
                _bucket = _TokenBucket(max_tokens=rpm, refill_period=60.0)
                logger.info(f"Zep 速率限制器已初始化: {rpm} 请求/分钟")
    return _bucket


def configure(max_rpm: int = _DEFAULT_MAX_RPM):
    """运行时调整速率限制（例如升级 Zep 套餐后）。"""
    global _bucket
    with _bucket_lock:
        _bucket = _TokenBucket(max_tokens=max_rpm, refill_period=60.0)
    logger.info(f"Zep 速率限制器已重新配置: {max_rpm} 请求/分钟")


def _extract_retry_after(exception: Exception) -> float | None:
    """从 Zep ApiError 的 headers 中提取 retry-after 秒数。"""
    # Zep SDK 的 ApiError 有 headers 属性（dict）或在 str 中包含
    headers = getattr(exception, 'headers', None)
    if isinstance(headers, dict):
        val = headers.get('retry-after') or headers.get('Retry-After')
        if val is not None:
            try:
                return float(val)
            except (ValueError, TypeError):
                pass

    # 尝试从异常消息中解析 retry-after
    msg = str(exception)
    match = re.search(r"retry-after['\"]?:\s*['\"]?(\d+)", msg, re.IGNORECASE)
    if match:
        return float(match.group(1))

    return None


def _is_rate_limit_error(exception: Exception) -> bool:
    """判断异常是否为 429 Rate Limit 错误。"""
    # Zep SDK 抛出 ApiError，status_code=429
    status = getattr(exception, 'status_code', None)
    if status == 429:
        return True
    # 兜底：检查异常消息
    msg = str(exception).lower()
    return '429' in msg and 'rate limit' in msg


def rate_limited_call(
    func: Callable[..., T],
    *args: Any,
    operation_name: str = "Zep API",
    max_retries: int = _DEFAULT_MAX_RETRIES,
    retry_base: float = _DEFAULT_RETRY_BASE,
    **kwargs: Any,
) -> T:
    """
    执行一次 Zep API 调用，自动限速 + 429 重试。

    1. 先从令牌桶获取许可（阻塞等待）
    2. 执行调用
    3. 如果收到 429，等待 retry-after 或指数退避后重试

    Args:
        func: 要调用的 Zep API 函数
        *args, **kwargs: 传给 func 的参数
        operation_name: 日志标识
        max_retries: 429 最大重试次数
        retry_base: 初始退避秒数
    """
    bucket = _get_bucket()

    for attempt in range(max_retries + 1):
        # 限速等待
        if not bucket.acquire(timeout=180.0):
            logger.error(f"[{operation_name}] 速率限制等待超时（180s），放弃")
            raise TimeoutError(f"Zep rate limiter timeout for {operation_name}")

        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            if _is_rate_limit_error(e):
                if attempt >= max_retries:
                    logger.error(f"[{operation_name}] 429 错误已重试 {max_retries} 次仍失败")
                    raise

                # 计算等待时间
                retry_after = _extract_retry_after(e)
                if retry_after is not None:
                    wait = retry_after + 1.0  # 额外 1 秒缓冲
                else:
                    wait = retry_base * (2 ** attempt)
                wait = min(wait, 120.0)

                logger.warning(
                    f"[{operation_name}] 429 Rate Limit, "
                    f"等待 {wait:.0f}s 后重试 ({attempt + 1}/{max_retries})..."
                )
                time.sleep(wait)
            else:
                # 非 429 错误直接抛出，由上层 _call_with_retry 处理
                raise
