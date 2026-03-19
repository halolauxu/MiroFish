"""Shared helpers for data-foundation workflows."""

from __future__ import annotations

from pathlib import Path


def astrategy_root() -> Path:
    return Path(__file__).resolve().parent.parent


def data_root() -> Path:
    return astrategy_root() / ".data"


def datahub_root() -> Path:
    return data_root() / "datahub"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def universe_root() -> Path:
    return datahub_root() / "universe"


def audit_root() -> Path:
    return datahub_root() / "audit"


def market_root() -> Path:
    return datahub_root() / "market"


def ingest_root() -> Path:
    return datahub_root() / "ingest"


def graph_root() -> Path:
    return datahub_root() / "graph"


def graph_path() -> Path:
    return data_root() / "local_graph" / "supply_chain.json"


def event_master_path() -> Path:
    return data_root() / "event_master" / "historical_event_master.json"


def pool_event_master_path() -> Path:
    return ingest_root() / "events" / "event_master.json"


def sentiment_root() -> Path:
    return ingest_root() / "sentiment"
