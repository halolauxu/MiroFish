"""
LLM response cache backed by SQLite.

Provides persistent, TTL-aware caching keyed on (model + messages + temperature)
to avoid redundant LLM calls during strategy runs.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from astrategy.config import settings

logger = logging.getLogger("astrategy.llm.cache")


class LLMCache:
    """SQLite-backed LLM response cache with TTL support."""

    def __init__(self, db_path: Optional[str] = None):
        """
        Args:
            db_path: Explicit path to the SQLite DB file.
                     Defaults to settings.storage.cache_dir / "llm_cache.db".
        """
        if db_path is None:
            db_path = str(settings.storage.cache_dir / "llm_cache.db")
        self._db_path = db_path
        self._local = threading.local()

        # Stats counters (in-memory, reset on process restart)
        self._hits = 0
        self._misses = 0
        self._lock = threading.Lock()

        self._init_db()

    # ── connection management (thread-local) ───────────────────

    def _get_conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return conn

    def _init_db(self) -> None:
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS llm_cache (
                cache_key   TEXT PRIMARY KEY,
                model       TEXT NOT NULL,
                result      TEXT NOT NULL,
                created_at  REAL NOT NULL,
                expires_at  REAL NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_llm_cache_expires
            ON llm_cache(expires_at)
        """)
        conn.commit()

    # ── key generation ─────────────────────────────────────────

    @staticmethod
    def make_key(
        model: str,
        messages: List[Dict[str, str]],
        temperature: float,
    ) -> str:
        """
        Create a deterministic cache key from request parameters.

        Key = SHA-256 of (model + serialised messages + temperature).
        """
        payload = json.dumps(
            {"model": model, "messages": messages, "temperature": temperature},
            sort_keys=True,
            ensure_ascii=False,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    # ── public API ─────────────────────────────────────────────

    def get(self, key: str) -> Optional[str]:
        """
        Retrieve a cached result if it exists and has not expired.

        Returns:
            The cached result string, or None on miss / expiry.
        """
        conn = self._get_conn()
        row = conn.execute(
            "SELECT result, expires_at FROM llm_cache WHERE cache_key = ?",
            (key,),
        ).fetchone()

        if row is None:
            with self._lock:
                self._misses += 1
            return None

        result, expires_at = row
        if time.time() > expires_at:
            # Expired — evict lazily
            conn.execute("DELETE FROM llm_cache WHERE cache_key = ?", (key,))
            conn.commit()
            with self._lock:
                self._misses += 1
            return None

        with self._lock:
            self._hits += 1
        return result

    def set(
        self,
        key: str,
        result: str,
        ttl_hours: float = 24.0,
        model: str = "",
    ) -> None:
        """
        Store a result in the cache.

        Args:
            key: Cache key (from make_key).
            result: The LLM response text.
            ttl_hours: Time-to-live in hours.
            model: Model name (stored for debugging).
        """
        now = time.time()
        expires_at = now + ttl_hours * 3600.0

        conn = self._get_conn()
        conn.execute(
            """
            INSERT OR REPLACE INTO llm_cache
                (cache_key, model, result, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (key, model, result, now, expires_at),
        )
        conn.commit()

    def invalidate(self, key: str) -> bool:
        """
        Remove a specific entry from the cache.

        Returns:
            True if a row was deleted, False if key was not found.
        """
        conn = self._get_conn()
        cursor = conn.execute(
            "DELETE FROM llm_cache WHERE cache_key = ?", (key,)
        )
        conn.commit()
        return cursor.rowcount > 0

    def clear_expired(self) -> int:
        """Remove all expired entries. Returns the number of rows deleted."""
        conn = self._get_conn()
        cursor = conn.execute(
            "DELETE FROM llm_cache WHERE expires_at < ?", (time.time(),)
        )
        conn.commit()
        removed = cursor.rowcount
        if removed:
            logger.info("Cleared %d expired cache entries", removed)
        return removed

    def stats(self) -> Dict[str, Any]:
        """
        Return cache statistics.

        Returns:
            Dict with hit_rate, total_queries, hits, misses, cache_size,
            and db_size_bytes.
        """
        conn = self._get_conn()
        row = conn.execute(
            "SELECT COUNT(*) FROM llm_cache WHERE expires_at > ?",
            (time.time(),),
        ).fetchone()
        cache_size = row[0] if row else 0

        with self._lock:
            hits = self._hits
            misses = self._misses

        total = hits + misses
        hit_rate = hits / total if total > 0 else 0.0

        try:
            db_size = Path(self._db_path).stat().st_size
        except OSError:
            db_size = 0

        return {
            "hit_rate": round(hit_rate, 4),
            "total_queries": total,
            "hits": hits,
            "misses": misses,
            "cache_size": cache_size,
            "db_size_bytes": db_size,
        }
