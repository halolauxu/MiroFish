"""
LLM cost tracker backed by SQLite.

Logs per-call token usage and provides daily / per-strategy cost rollups.
Pricing is approximate and configurable.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from astrategy.config import settings

logger = logging.getLogger("astrategy.llm.cost_tracker")

# ── default pricing (USD per 1M tokens) ───────────────────────
# These are rough estimates; override via set_pricing().
_DEFAULT_PRICING: Dict[str, Dict[str, float]] = {
    "deepseek-chat": {"input": 0.27, "output": 1.10},
    "deepseek-reasoner": {"input": 0.55, "output": 2.19},
    "qwen-plus": {"input": 0.80, "output": 2.00},
    "qwen-turbo": {"input": 0.30, "output": 0.60},
    "gpt-4o": {"input": 2.50, "output": 10.00},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
}


class CostTracker:
    """Track LLM token usage and estimate costs per strategy."""

    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            db_path = str(settings.storage.cache_dir / "llm_costs.db")
        self._db_path = db_path
        self._local = threading.local()
        self._pricing = dict(_DEFAULT_PRICING)
        self._init_db()

    # ── connection ─────────────────────────────────────────────

    def _get_conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn = conn
        return conn

    def _init_db(self) -> None:
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS llm_usage (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy    TEXT NOT NULL,
                model       TEXT NOT NULL,
                input_tokens  INTEGER NOT NULL,
                output_tokens INTEGER NOT NULL,
                cost_usd    REAL NOT NULL DEFAULT 0.0,
                created_at  TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_usage_date
            ON llm_usage(created_at)
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_usage_strategy
            ON llm_usage(strategy)
        """)
        conn.commit()

    # ── pricing ────────────────────────────────────────────────

    def set_pricing(self, model: str, input_per_1m: float, output_per_1m: float) -> None:
        """Set or update pricing for a model (USD per 1M tokens)."""
        self._pricing[model] = {"input": input_per_1m, "output": output_per_1m}

    def _estimate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        """Calculate approximate cost in USD."""
        pricing = self._pricing.get(model)
        if pricing is None:
            # Fallback: use a rough average
            pricing = {"input": 1.0, "output": 3.0}
        return (
            input_tokens * pricing["input"] / 1_000_000
            + output_tokens * pricing["output"] / 1_000_000
        )

    # ── logging ────────────────────────────────────────────────

    def log_usage(
        self,
        strategy: str,
        model: str,
        input_tokens: int,
        output_tokens: int,
    ) -> None:
        """
        Record a single LLM call's token usage.

        Args:
            strategy: Strategy or module name.
            model: Model identifier.
            input_tokens: Prompt tokens consumed.
            output_tokens: Completion tokens generated.
        """
        cost = self._estimate_cost(model, input_tokens, output_tokens)
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        conn = self._get_conn()
        conn.execute(
            """
            INSERT INTO llm_usage (strategy, model, input_tokens, output_tokens, cost_usd, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (strategy, model, input_tokens, output_tokens, cost, now),
        )
        conn.commit()

        logger.debug(
            "Logged usage: strategy=%s model=%s in=%d out=%d cost=$%.6f",
            strategy,
            model,
            input_tokens,
            output_tokens,
            cost,
        )

    # ── queries ────────────────────────────────────────────────

    def get_daily_cost(self, target_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Get aggregated cost for a specific date.

        Args:
            target_date: Date string "YYYY-MM-DD". Defaults to today (UTC).

        Returns:
            Dict with date, total_cost, total_input_tokens, total_output_tokens,
            call_count, and per-model breakdown.
        """
        if target_date is None:
            target_date = datetime.utcnow().strftime("%Y-%m-%d")

        conn = self._get_conn()

        # Overall totals
        row = conn.execute(
            """
            SELECT COUNT(*),
                   COALESCE(SUM(input_tokens), 0),
                   COALESCE(SUM(output_tokens), 0),
                   COALESCE(SUM(cost_usd), 0)
            FROM llm_usage
            WHERE created_at LIKE ? || '%'
            """,
            (target_date,),
        ).fetchone()

        call_count, total_in, total_out, total_cost = row

        # Per-model breakdown
        model_rows = conn.execute(
            """
            SELECT model,
                   COUNT(*),
                   SUM(input_tokens),
                   SUM(output_tokens),
                   SUM(cost_usd)
            FROM llm_usage
            WHERE created_at LIKE ? || '%'
            GROUP BY model
            """,
            (target_date,),
        ).fetchall()

        by_model = {}
        for m_name, m_count, m_in, m_out, m_cost in model_rows:
            by_model[m_name] = {
                "call_count": m_count,
                "input_tokens": m_in,
                "output_tokens": m_out,
                "cost_usd": round(m_cost, 6),
            }

        return {
            "date": target_date,
            "call_count": call_count,
            "total_input_tokens": total_in,
            "total_output_tokens": total_out,
            "total_cost_usd": round(total_cost, 6),
            "by_model": by_model,
        }

    def get_strategy_cost(
        self,
        strategy: str,
        days: int = 30,
    ) -> Dict[str, Any]:
        """
        Get cost summary for a strategy over the last N days.

        Args:
            strategy: Strategy name.
            days: Look-back window in days.

        Returns:
            Dict with strategy, period, total_cost, daily_breakdown.
        """
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime(
            "%Y-%m-%d 00:00:00"
        )

        conn = self._get_conn()

        # Overall
        row = conn.execute(
            """
            SELECT COUNT(*),
                   COALESCE(SUM(input_tokens), 0),
                   COALESCE(SUM(output_tokens), 0),
                   COALESCE(SUM(cost_usd), 0)
            FROM llm_usage
            WHERE strategy = ? AND created_at >= ?
            """,
            (strategy, cutoff),
        ).fetchone()

        call_count, total_in, total_out, total_cost = row

        # Daily breakdown
        daily_rows = conn.execute(
            """
            SELECT SUBSTR(created_at, 1, 10) AS day,
                   COUNT(*),
                   SUM(input_tokens),
                   SUM(output_tokens),
                   SUM(cost_usd)
            FROM llm_usage
            WHERE strategy = ? AND created_at >= ?
            GROUP BY day
            ORDER BY day DESC
            """,
            (strategy, cutoff),
        ).fetchall()

        daily = []
        for day, d_count, d_in, d_out, d_cost in daily_rows:
            daily.append(
                {
                    "date": day,
                    "call_count": d_count,
                    "input_tokens": d_in,
                    "output_tokens": d_out,
                    "cost_usd": round(d_cost, 6),
                }
            )

        return {
            "strategy": strategy,
            "period_days": days,
            "call_count": call_count,
            "total_input_tokens": total_in,
            "total_output_tokens": total_out,
            "total_cost_usd": round(total_cost, 6),
            "daily_breakdown": daily,
        }
