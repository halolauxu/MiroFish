"""
Unified configuration for AStrategy platform.

Reads shared secrets from the project root .env and provides typed access
to every configurable knob (data sources, LLM, graph store, strategies,
storage paths).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from dotenv import load_dotenv

# ── locate the project-root .env (two levels up from this file) ──────────
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ENV_PATH = _PROJECT_ROOT / ".env"
load_dotenv(_ENV_PATH)


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int = 0) -> int:
    raw = os.environ.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(key: str, default: float = 0.0) -> float:
    raw = os.environ.get(key)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.environ.get(key, "").lower()
    if raw in ("1", "true", "yes"):
        return True
    if raw in ("0", "false", "no"):
        return False
    return default


def _env_list(key: str, default: list | None = None, sep: str = ",") -> list[str]:
    raw = os.environ.get(key)
    if raw is None:
        return default or []
    return [item.strip() for item in raw.split(sep) if item.strip()]


# ---------------------------------------------------------------------------
# Data-source configuration
# ---------------------------------------------------------------------------

@dataclass
class DataSourceConfig:
    """Configuration for upstream market-data providers."""

    # Tushare (https://tushare.pro)
    tushare_token: str = field(default_factory=lambda: _env("TUSHARE_TOKEN"))
    tushare_api_url: str = field(
        default_factory=lambda: _env("TUSHARE_API_URL", "http://api.tushare.pro"),
    )

    # AkShare (pip install akshare) - no token needed, but we expose
    # a request-timeout and retry count so callers can tune behaviour.
    akshare_request_timeout: int = field(
        default_factory=lambda: _env_int("AKSHARE_REQUEST_TIMEOUT", 30),
    )
    akshare_max_retries: int = field(
        default_factory=lambda: _env_int("AKSHARE_MAX_RETRIES", 3),
    )
    akshare_retry_delay: float = field(
        default_factory=lambda: _env_float("AKSHARE_RETRY_DELAY", 1.0),
    )


# ---------------------------------------------------------------------------
# LLM configuration
# ---------------------------------------------------------------------------

@dataclass
class LLMConfig:
    """Configuration for the LLM backend (OpenAI-compatible)."""

    api_key: str = field(default_factory=lambda: _env("LLM_API_KEY"))
    base_url: str = field(
        default_factory=lambda: _env("LLM_BASE_URL", "https://api.deepseek.com/v1"),
    )
    model_name: str = field(
        default_factory=lambda: _env("LLM_MODEL_NAME", "deepseek-chat"),
    )
    temperature: float = field(
        default_factory=lambda: _env_float("LLM_TEMPERATURE", 0.3),
    )
    max_tokens: int = field(
        default_factory=lambda: _env_int("LLM_MAX_TOKENS", 4096),
    )
    # Prompt-result cache: how many seconds a cached LLM response stays valid.
    cache_ttl: int = field(
        default_factory=lambda: _env_int("LLM_CACHE_TTL", 3600),
    )
    request_timeout: int = field(
        default_factory=lambda: _env_int("LLM_REQUEST_TIMEOUT", 120),
    )


# ---------------------------------------------------------------------------
# Graph / Zep configuration
# ---------------------------------------------------------------------------

@dataclass
class GraphConfig:
    """Configuration for the Zep graph-memory backend."""

    zep_api_key: str = field(default_factory=lambda: _env("ZEP_API_KEY"))
    # Maximum requests-per-second to Zep Cloud to avoid 429s.
    rate_limit_rps: float = field(
        default_factory=lambda: _env_float("ZEP_RATE_LIMIT_RPS", 5.0),
    )
    # Graph group identifier used to namespace A-share entities.
    group_id: str = field(
        default_factory=lambda: _env("ZEP_ASTRATEGY_GROUP_ID", "astrategy"),
    )


# ---------------------------------------------------------------------------
# Strategy configuration
# ---------------------------------------------------------------------------

@dataclass
class StrategyConfig:
    """Which strategies are enabled and when they run."""

    # Comma-separated strategy names in the env, e.g.
    # "sector_rotation,event_driven,fundamental_value"
    enabled_strategies: List[str] = field(
        default_factory=lambda: _env_list(
            "ASTRATEGY_ENABLED",
            default=[
                "sector_rotation",
                "event_driven",
                "fundamental_value",
                "technical_momentum",
                "sentiment",
            ],
        ),
    )
    # Cron-style schedule (consumed by the scheduler module).
    schedule: str = field(
        default_factory=lambda: _env("ASTRATEGY_SCHEDULE", "0 18 * * 1-5"),
    )
    # Maximum number of stocks a single strategy invocation may cover.
    max_stocks_per_run: int = field(
        default_factory=lambda: _env_int("ASTRATEGY_MAX_STOCKS", 100),
    )
    # Default holding-period assumption (days) when a strategy does not specify.
    default_holding_days: int = field(
        default_factory=lambda: _env_int("ASTRATEGY_DEFAULT_HOLD_DAYS", 20),
    )


# ---------------------------------------------------------------------------
# Storage paths
# ---------------------------------------------------------------------------

@dataclass
class StorageConfig:
    """File-system paths for data, caches, and signal output."""

    _base: Path = field(
        default_factory=lambda: Path(
            _env("ASTRATEGY_STORAGE_BASE", str(_PROJECT_ROOT / "astrategy" / ".data")),
        ),
    )

    @property
    def data_dir(self) -> Path:
        """Raw and processed market data."""
        p = self._base / "market"
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def cache_dir(self) -> Path:
        """LLM response caches, intermediate artefacts."""
        p = self._base / "cache"
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def signal_dir(self) -> Path:
        """Strategy signal JSON output."""
        p = self._base / "signals"
        p.mkdir(parents=True, exist_ok=True)
        return p

    @property
    def ontology_path(self) -> Path:
        """Path to the pre-defined A-share ontology YAML."""
        return Path(__file__).resolve().parent / "ontology.yaml"


# ---------------------------------------------------------------------------
# Top-level settings singleton
# ---------------------------------------------------------------------------

@dataclass
class Settings:
    """Aggregated, read-only settings for the AStrategy platform."""

    data_source: DataSourceConfig = field(default_factory=DataSourceConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    graph: GraphConfig = field(default_factory=GraphConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)


# Module-level convenience instance.  Import as:
#     from astrategy.config import settings
settings = Settings()
