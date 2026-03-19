"""Portfolio-level simulation helpers."""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from astrategy.research.metrics import compute_metrics

from .allocator import allocate_portfolio


def simulate_portfolio(
    signals: List[Dict[str, Any]],
    *,
    max_positions: int = 10,
    holding_days: int = 5,
) -> Dict[str, Any]:
    """Run a lightweight date-bucketed portfolio simulation."""
    if not signals:
        return {
            "metrics": {},
            "daily_books": [],
            "avg_long_weight": 0.0,
            "avg_defensive_weight": 0.0,
            "avg_positions": 0.0,
            "avg_dynamic_scale": 0.0,
        }

    df = pd.DataFrame(signals)
    if df.empty or "event_date" not in df.columns:
        return {
            "metrics": {},
            "daily_books": [],
            "avg_long_weight": 0.0,
            "avg_defensive_weight": 0.0,
            "avg_positions": 0.0,
            "avg_dynamic_scale": 0.0,
        }

    daily_books: List[Dict[str, Any]] = []
    daily_records: List[Dict[str, Any]] = []

    for event_date, group in df.groupby("event_date", sort=True):
        book = allocate_portfolio(group.to_dict("records"), max_positions=max_positions)
        long_positions = book.get("positions", [])
        long_weight = float(book.get("gross_long_weight", 0.0))
        defensive_weight = float(book.get("defensive_weight", 0.0))

        portfolio_return = sum(
            float(item.get("target_weight", 0.0)) * float(item.get("direction_adjusted_return", 0.0))
            for item in long_positions
        )
        shadow_defensive_alpha = sum(
            float(item.get("target_weight", 0.0)) * float(item.get("direction_adjusted_return", 0.0))
            for item in book.get("defensive_positions", [])
        )

        daily_books.append({
            "event_date": event_date,
            "portfolio_return": round(portfolio_return, 6),
            "shadow_defensive_alpha": round(shadow_defensive_alpha, 6),
            "gross_long_weight": round(long_weight, 4),
            "defensive_weight": round(defensive_weight, 4),
            "num_positions": int(book.get("num_positions", 0)),
            "num_defensive": int(book.get("num_defensive", 0)),
            "dynamic_scale": float(book.get("dynamic_book", {}).get("scale", 0.0)),
            "top_theme": (
                book.get("theme_weights", [{}])[0].get("name", "")
                if book.get("theme_weights") else ""
            ),
        })
        daily_records.append({
            "event_date": event_date,
            "portfolio_return": portfolio_return,
        })

    daily_df = pd.DataFrame(daily_records)
    books_df = pd.DataFrame(daily_books)
    active_books = books_df[books_df["num_positions"] > 0]
    metrics = compute_metrics(daily_df["portfolio_return"], holding_days=holding_days)
    metrics["avg_long_weight"] = round(float(books_df["gross_long_weight"].mean()), 4)
    metrics["avg_defensive_weight"] = round(float(books_df["defensive_weight"].mean()), 4)
    metrics["avg_positions"] = round(float(books_df["num_positions"].mean()), 2)
    metrics["avg_dynamic_scale"] = round(
        float(active_books["dynamic_scale"].mean()) if not active_books.empty else 0.0,
        4,
    )

    return {
        "metrics": metrics,
        "daily_books": daily_books,
        "avg_long_weight": metrics["avg_long_weight"],
        "avg_defensive_weight": metrics["avg_defensive_weight"],
        "avg_positions": metrics["avg_positions"],
        "avg_dynamic_scale": metrics["avg_dynamic_scale"],
    }
