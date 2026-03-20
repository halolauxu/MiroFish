"""Authoritative historical archive helpers for desk-line research."""

from .authoritative_history import (
    STRATEGY_ARCHIVE_SPECS,
    AuthoritativeHistoryBuilder,
    archive_authoritative_signals,
)

__all__ = [
    "STRATEGY_ARCHIVE_SPECS",
    "AuthoritativeHistoryBuilder",
    "archive_authoritative_signals",
]
