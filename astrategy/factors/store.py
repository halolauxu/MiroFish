"""
Parquet-based historical factor storage.

Stores daily factor snapshots per strategy for walk-forward OOS evaluation.
Path: .data/factors/{strategy}/{date}.parquet
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from astrategy.config import settings

logger = logging.getLogger("astrategy.factors.store")


def _parquet_engine() -> str:
    """Return best available Parquet engine."""
    try:
        import pyarrow  # noqa: F401
        return "pyarrow"
    except ImportError:
        try:
            import fastparquet  # noqa: F401
            return "fastparquet"
        except ImportError:
            return "auto"


class FactorStore:
    """Save and load factor DataFrames as Parquet files.

    Directory layout::

        {base_dir}/
        └── {strategy}/
            ├── 20260314.parquet
            ├── 20260315.parquet
            └── ...
    """

    def __init__(self, base_dir: str | Path | None = None):
        self._base = Path(base_dir) if base_dir else (
            settings.storage._base / "factors"
        )

    def save(
        self,
        strategy: str,
        date: str,
        df: pd.DataFrame,
    ) -> Path:
        """Save a factor DataFrame for a given strategy and date.

        Parameters
        ----------
        strategy : str
            Strategy identifier (e.g. "s07_graph_factors").
        date : str
            Date string in YYYYMMDD format.
        df : pd.DataFrame
            Factor DataFrame (index = stock_code, columns = factor names).

        Returns
        -------
        Path
            Path to the written Parquet file.
        """
        dir_path = self._base / strategy
        dir_path.mkdir(parents=True, exist_ok=True)
        file_path = dir_path / f"{date}.parquet"
        df.to_parquet(file_path, engine=_parquet_engine())
        logger.info(
            "Saved %d×%d factors → %s", len(df), len(df.columns), file_path,
        )
        return file_path

    def load(
        self,
        strategy: str,
        date: str,
    ) -> Optional[pd.DataFrame]:
        """Load factors for a single date.

        Returns None if the file does not exist.
        """
        file_path = self._base / strategy / f"{date}.parquet"
        if not file_path.exists():
            return None
        df = pd.read_parquet(file_path, engine=_parquet_engine())
        logger.debug("Loaded factors from %s (%d rows)", file_path, len(df))
        return df

    def load_range(
        self,
        strategy: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Load and concatenate factors over a date range.

        Returns a DataFrame with a MultiIndex (date, stock_code).
        If no files exist in the range, returns an empty DataFrame.
        """
        dir_path = self._base / strategy
        if not dir_path.exists():
            return pd.DataFrame()

        frames = []
        for f in sorted(dir_path.glob("*.parquet")):
            date_str = f.stem
            if start_date <= date_str <= end_date:
                df = pd.read_parquet(f, engine=_parquet_engine())
                df["date"] = date_str
                frames.append(df)

        if not frames:
            return pd.DataFrame()

        combined = pd.concat(frames)
        if "date" in combined.columns:
            combined = combined.set_index("date", append=True)
            combined = combined.reorder_levels(["date", combined.index.names[0]])
        return combined

    def list_dates(self, strategy: str) -> list[str]:
        """Return sorted list of available dates for a strategy."""
        dir_path = self._base / strategy
        if not dir_path.exists():
            return []
        return sorted(f.stem for f in dir_path.glob("*.parquet"))
