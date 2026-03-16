"""
Backtesting and evaluation tools.

Provides signal-quality evaluation, Freqtrade bridging, and strategy
comparison utilities.
"""

from astrategy.backtest.evaluator import Evaluator
from astrategy.backtest.freqtrade_bridge import FreqtradeBridge

__all__ = ["Evaluator", "FreqtradeBridge"]
