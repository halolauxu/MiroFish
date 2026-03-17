#!/usr/bin/env python3
"""
Coordinate-descent weight optimizer for S07 Graph Factors strategy.

For each factor, sweeps candidate weights while holding others fixed.
Uses quick-mode backtest (30 stocks, 3 windows) as the objective.

Usage:
    python3 astrategy/backtest/weight_optimizer.py
"""
import json
import logging
import math
import os
import statistics
import sys
import time
from copy import deepcopy
from datetime import datetime, timedelta
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import pandas as pd

from astrategy.strategies.s07_graph_factors import (
    DEFAULT_WEIGHTS,
    GraphFactorsStrategy,
    _GRAPH_FACTOR_COLS,
)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("weight_optimizer")
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Quick evaluation function
# ---------------------------------------------------------------------------

# Cache stock universe & rolling evaluator across calls
_cached_codes: Optional[list] = None
_cached_rolling = None


def _get_quick_setup():
    """Lazily initialise stock codes and rolling evaluator."""
    global _cached_codes, _cached_rolling
    if _cached_codes is not None:
        return _cached_codes, _cached_rolling

    from astrategy.run_backtest import (
        QUICK_SAMPLE,
        RollingEvaluator,
        fetch_csi800_codes,
    )

    try:
        codes = fetch_csi800_codes()
    except Exception:
        codes = QUICK_SAMPLE

    import random
    random.seed(42)
    codes = random.sample(codes, min(30, len(codes)))
    _cached_codes = codes
    _cached_rolling = RollingEvaluator()
    return codes, _cached_rolling


def quick_evaluate(weights: Dict[str, float]) -> Dict[str, float]:
    """Run S07 with given weights and return key metrics.

    Returns dict with sharpe_ratio, hit_rate, consistency, avg_return.
    """
    codes, rolling = _get_quick_setup()

    strategy = GraphFactorsStrategy(weights=weights)
    try:
        signals = strategy.run(stock_codes=codes)
    except Exception as exc:
        logger.warning("Strategy run failed: %s", exc)
        return {"sharpe_ratio": -99.0, "hit_rate": 0.0, "consistency": 0.0, "avg_return": -1.0}

    if not signals:
        return {"sharpe_ratio": -99.0, "hit_rate": 0.0, "consistency": 0.0, "avg_return": -1.0}

    from astrategy.run_backtest import RollingEvaluator
    roll_df = rolling.evaluate_batch_rolling(signals)
    metrics = RollingEvaluator.compute_rolling_metrics(roll_df)
    return metrics


# ---------------------------------------------------------------------------
# Coordinate descent
# ---------------------------------------------------------------------------

def coordinate_descent(
    initial_weights: Dict[str, float],
    candidates: Optional[Dict[str, List[float]]] = None,
    max_rounds: int = 2,
    objective: str = "sharpe_ratio",
) -> Dict[str, any]:
    """Run coordinate descent over factor weights.

    Parameters
    ----------
    initial_weights : dict
        Starting weights for all factors.
    candidates : dict | None
        Per-factor candidate values to try. If None, uses a default grid.
    max_rounds : int
        Number of full sweeps over all factors.
    objective : str
        Metric to maximise (sharpe_ratio, hit_rate, etc.)

    Returns
    -------
    dict with keys: best_weights, best_metrics, history
    """
    if candidates is None:
        # Default candidate grid for each factor type
        graph_candidates = [-1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0]
        trad_candidates = [-1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5]
        candidates = {}
        for f in initial_weights:
            if f in _GRAPH_FACTOR_COLS:
                candidates[f] = graph_candidates
            else:
                candidates[f] = trad_candidates

    weights = deepcopy(initial_weights)
    history = []

    # Evaluate baseline
    logger.info("Evaluating baseline weights...")
    baseline = quick_evaluate(weights)
    best_obj = baseline.get(objective, -99)
    logger.info("Baseline: %s = %.4f  (hit=%.1f%%, cons=%.1f%%)",
                objective, best_obj,
                baseline.get("hit_rate", 0) * 100,
                baseline.get("consistency", 0) * 100)
    history.append({"round": 0, "factor": "baseline", "weights": deepcopy(weights), "metrics": baseline})

    for round_num in range(1, max_rounds + 1):
        improved_this_round = False
        factors = list(initial_weights.keys())

        for fi, factor in enumerate(factors):
            current_val = weights[factor]
            best_val = current_val
            best_sharpe_for_factor = best_obj

            factor_candidates = [c for c in candidates[factor] if c != current_val]
            if not factor_candidates:
                continue

            logger.info("Round %d [%d/%d] Sweeping '%s' (current=%.1f) over %s",
                        round_num, fi + 1, len(factors), factor, current_val, factor_candidates)

            for cand in factor_candidates:
                weights[factor] = cand
                metrics = quick_evaluate(weights)
                obj_val = metrics.get(objective, -99)

                logger.info("  %s=%.1f → %s=%.4f (hit=%.1f%%, cons=%.1f%%)",
                            factor, cand, objective, obj_val,
                            metrics.get("hit_rate", 0) * 100,
                            metrics.get("consistency", 0) * 100)

                if obj_val > best_sharpe_for_factor:
                    best_val = cand
                    best_sharpe_for_factor = obj_val

            weights[factor] = best_val
            if best_val != current_val:
                improved_this_round = True
                logger.info("  → Updated '%s': %.1f → %.1f (%s: %.4f → %.4f)",
                            factor, current_val, best_val, objective, best_obj, best_sharpe_for_factor)
                best_obj = best_sharpe_for_factor
                history.append({
                    "round": round_num,
                    "factor": factor,
                    "old_val": current_val,
                    "new_val": best_val,
                    "metrics": quick_evaluate(weights),
                })

        if not improved_this_round:
            logger.info("Round %d: no improvement, stopping early.", round_num)
            break

    # Final evaluation with best weights
    final_metrics = quick_evaluate(weights)
    logger.info("=== Final: %s = %.4f (hit=%.1f%%, cons=%.1f%%) ===",
                objective, final_metrics.get(objective, -99),
                final_metrics.get("hit_rate", 0) * 100,
                final_metrics.get("consistency", 0) * 100)

    return {
        "best_weights": weights,
        "best_metrics": final_metrics,
        "baseline_metrics": baseline,
        "history": history,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    t0 = time.time()

    result = coordinate_descent(
        initial_weights=deepcopy(DEFAULT_WEIGHTS),
        max_rounds=2,
        objective="sharpe_ratio",
    )

    elapsed = time.time() - t0

    print("\n" + "=" * 60)
    print("WEIGHT OPTIMIZATION RESULTS")
    print("=" * 60)

    print("\nBaseline metrics:")
    for k, v in result["baseline_metrics"].items():
        print(f"  {k}: {v}")

    print("\nOptimised metrics:")
    for k, v in result["best_metrics"].items():
        print(f"  {k}: {v}")

    print("\nOptimised weights:")
    for k, v in sorted(result["best_weights"].items()):
        default = DEFAULT_WEIGHTS.get(k, 0)
        marker = " ← CHANGED" if abs(v - default) > 0.01 else ""
        print(f"  {k}: {v:.1f}{marker}")

    print(f"\nTotal time: {elapsed:.0f}s ({elapsed/60:.1f}min)")

    # Save results
    out_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        ".data", "weight_optimization_results.json",
    )
    with open(out_path, "w") as f:
        # Convert history metrics for JSON serialization
        serializable = deepcopy(result)
        for h in serializable["history"]:
            if "metrics" in h:
                h["metrics"] = {k: float(v) if isinstance(v, (int, float)) else v
                                for k, v in h["metrics"].items()}
        json.dump(serializable, f, indent=2, ensure_ascii=False, default=str)
    print(f"\nResults saved to: {out_path}")
