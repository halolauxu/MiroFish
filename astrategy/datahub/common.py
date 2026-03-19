"""Shared helpers for data-foundation workflows."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable


def astrategy_root() -> Path:
    return Path(__file__).resolve().parent.parent


def workspace_root() -> Path:
    return astrategy_root().parent


def data_root() -> Path:
    return astrategy_root() / ".data"


def datahub_root() -> Path:
    return data_root() / "datahub"


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def repo_relative_path(path: Path | str) -> str:
    raw = path if isinstance(path, Path) else Path(str(path).strip())
    if not str(raw).strip():
        return ""
    for base in (astrategy_root(), workspace_root()):
        try:
            return str(raw.resolve().relative_to(base))
        except Exception:
            try:
                return str(raw.relative_to(base))
            except Exception:
                continue
    return str(raw)


def _candidate_paths(path_value: str | Path | None) -> Iterable[Path]:
    text = str(path_value or "").strip()
    if not text:
        return []
    raw = Path(text)
    normalized = text.replace("\\", "/")
    candidates: list[Path] = []
    if raw.is_absolute():
        candidates.append(raw)
    else:
        candidates.append(astrategy_root() / raw)
        candidates.append(workspace_root() / raw)
        candidates.append(raw)
    if normalized.startswith("astrategy/"):
        candidates.append(workspace_root() / normalized)
    if "/astrategy/" in normalized:
        suffix = normalized.split("/astrategy/", 1)[1]
        candidates.append(workspace_root() / f"astrategy/{suffix}")
    for marker in ("/astrategy/.data/", "astrategy/.data/", "/.data/", ".data/"):
        if marker in normalized:
            suffix = normalized.split(marker, 1)[1]
            candidates.append(data_root() / suffix)
            break
    deduped: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(candidate)
    return deduped


def resolve_repo_path(path_value: str | Path | None, *, fallback: Path | None = None) -> Path:
    candidates = list(_candidate_paths(path_value))
    if fallback is not None:
        candidates.extend(_candidate_paths(fallback))
        candidates.append(fallback)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    if candidates:
        return candidates[-1]
    return fallback if fallback is not None else Path(str(path_value or "").strip())


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


def graph_manifest_path() -> Path:
    return graph_root() / "graph_manifest.json"


def event_master_path() -> Path:
    return data_root() / "event_master" / "historical_event_master.json"


def pool_event_master_path() -> Path:
    return ingest_root() / "events" / "event_master.json"


def sentiment_root() -> Path:
    return ingest_root() / "sentiment"


def sentiment_manifest_path() -> Path:
    return sentiment_root() / "sentiment_manifest.json"


def filings_manifest_path() -> Path:
    return ingest_root() / "filings" / "filings_manifest.json"


def news_manifest_path() -> Path:
    return ingest_root() / "news" / "news_manifest.json"


def event_manifest_path() -> Path:
    return ingest_root() / "events" / "event_manifest.json"
