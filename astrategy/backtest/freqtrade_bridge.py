"""
Freqtrade Bridge -- connect AStrategy signals to Freqtrade.

Converts signals into Freqtrade-compatible dataframes, generates
strategy files, and exports trade logs.
"""

from __future__ import annotations

import json
import logging
import textwrap
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd

from astrategy.strategies.base import StrategySignal

logger = logging.getLogger("astrategy.backtest.freqtrade_bridge")


class FreqtradeBridge:
    """Bridge between AStrategy signals and Freqtrade."""

    # ── signal -> dataframe conversion ────────────────────────────────

    @staticmethod
    def signals_to_dataframe(
        signals: List[StrategySignal],
        timeframe: str = "1d",
    ) -> pd.DataFrame:
        """Convert signals into a Freqtrade-compatible signal dataframe.

        The output has columns compatible with Freqtrade's
        ``populate_entry_trend`` / ``populate_exit_trend``:
            date, pair, enter_long, exit_long, enter_short, exit_short,
            signal_strength

        Parameters
        ----------
        signals:
            AStrategy signals to convert.
        timeframe:
            Candle timeframe (for informational purposes; doesn't change
            the conversion logic).
        """
        rows = []
        for sig in signals:
            pair = _stock_code_to_pair(sig.stock_code)
            entry_date = sig.timestamp
            exit_date = sig.timestamp + timedelta(days=sig.holding_period_days)

            # Entry row
            entry_row = {
                "date": entry_date,
                "pair": pair,
                "enter_long": 1 if sig.direction == "long" else 0,
                "exit_long": 0,
                "enter_short": 1 if sig.direction == "short" else 0,
                "exit_short": 0,
                "signal_strength": sig.confidence,
                "strategy": sig.strategy_name,
                "expected_return": sig.expected_return,
                "timeframe": timeframe,
            }
            rows.append(entry_row)

            # Exit row
            exit_row = {
                "date": exit_date,
                "pair": pair,
                "enter_long": 0,
                "exit_long": 1 if sig.direction == "long" else 0,
                "enter_short": 0,
                "exit_short": 1 if sig.direction == "short" else 0,
                "signal_strength": sig.confidence,
                "strategy": sig.strategy_name,
                "expected_return": sig.expected_return,
                "timeframe": timeframe,
            }
            rows.append(exit_row)

        if not rows:
            return pd.DataFrame(columns=[
                "date", "pair", "enter_long", "exit_long",
                "enter_short", "exit_short", "signal_strength",
                "strategy", "expected_return", "timeframe",
            ])

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df = df.sort_values("date").reset_index(drop=True)
        return df

    # ── strategy file generation ──────────────────────────────────────

    @staticmethod
    def create_strategy_file(
        strategy_name: str,
        signals_dir: str,
        output_dir: Optional[str] = None,
    ) -> str:
        """Generate a Freqtrade strategy Python file that reads AStrategy
        signals from JSON files.

        Parameters
        ----------
        strategy_name:
            Name for the generated strategy class.
        signals_dir:
            Directory where signal JSON files are stored.
        output_dir:
            Directory to write the strategy file.  Defaults to the same
            directory as *signals_dir*.

        Returns
        -------
        Absolute path to the generated ``.py`` file.
        """
        if output_dir is None:
            output_dir = signals_dir

        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        class_name = "".join(
            word.capitalize() for word in strategy_name.split("_")
        )
        file_name = f"{strategy_name}.py"
        file_path = out_path / file_name

        code = textwrap.dedent(f'''\
            """
            Auto-generated Freqtrade strategy: {class_name}

            Reads AStrategy signals from JSON files in:
                {signals_dir}
            """

            import json
            from datetime import datetime
            from pathlib import Path

            import pandas as pd
            from freqtrade.strategy import IStrategy, DecimalParameter
            from pandas import DataFrame


            class {class_name}(IStrategy):
                """Freqtrade strategy driven by AStrategy signals."""

                INTERFACE_VERSION = 3

                # Minimal ROI -- let signals control exits
                minimal_roi = {{"0": 0.15, "60": 0.05, "120": 0.02}}

                stoploss = -0.08
                trailing_stop = True
                trailing_stop_positive = 0.03
                trailing_stop_positive_offset = 0.05
                trailing_only_offset_is_reached = True

                timeframe = "1d"
                startup_candle_count = 5

                # Confidence threshold for entering a trade
                min_confidence = DecimalParameter(
                    0.3, 0.9, default=0.5, decimals=2, space="buy",
                )

                _signals_dir = Path("{signals_dir}")
                _signal_cache: dict = {{}}

                def _load_signals(self, date_str: str) -> list:
                    """Load signals for a given date from all strategy subdirs."""
                    if date_str in self._signal_cache:
                        return self._signal_cache[date_str]

                    signals = []
                    if not self._signals_dir.exists():
                        return signals

                    for strat_dir in self._signals_dir.iterdir():
                        if not strat_dir.is_dir():
                            continue
                        sig_file = strat_dir / f"{{date_str}}.json"
                        if sig_file.exists():
                            try:
                                data = json.loads(sig_file.read_text("utf-8"))
                                signals.extend(data)
                            except Exception:
                                pass

                    self._signal_cache[date_str] = signals
                    return signals

                def _get_signal_for_pair(
                    self, pair: str, date_str: str
                ) -> dict | None:
                    """Find the strongest signal for a pair on a date."""
                    signals = self._load_signals(date_str)
                    # Convert pair back to stock code
                    # Pair format: "600000.SH/CNY" -> "600000.SH"
                    stock_code = pair.split("/")[0]

                    matching = [
                        s for s in signals if s.get("stock_code") == stock_code
                    ]
                    if not matching:
                        return None

                    return max(matching, key=lambda s: s.get("confidence", 0))

                def populate_indicators(
                    self, dataframe: DataFrame, metadata: dict
                ) -> DataFrame:
                    return dataframe

                def populate_entry_trend(
                    self, dataframe: DataFrame, metadata: dict
                ) -> DataFrame:
                    dataframe["enter_long"] = 0
                    dataframe["enter_short"] = 0

                    for idx, row in dataframe.iterrows():
                        date_str = row["date"].strftime("%Y%m%d")
                        sig = self._get_signal_for_pair(
                            metadata["pair"], date_str
                        )
                        if sig is None:
                            continue
                        conf = sig.get("confidence", 0)
                        if conf < float(self.min_confidence.value):
                            continue
                        direction = sig.get("direction", "")
                        if direction == "long":
                            dataframe.at[idx, "enter_long"] = 1
                        elif direction == "short":
                            dataframe.at[idx, "enter_short"] = 1

                    return dataframe

                def populate_exit_trend(
                    self, dataframe: DataFrame, metadata: dict
                ) -> DataFrame:
                    dataframe["exit_long"] = 0
                    dataframe["exit_short"] = 0
                    return dataframe
        ''')

        file_path.write_text(code, encoding="utf-8")
        logger.info("Generated Freqtrade strategy at %s", file_path)
        return str(file_path.resolve())

    # ── trade log export ──────────────────────────────────────────────

    @staticmethod
    def export_trades(
        signals: List[StrategySignal],
        prices: pd.DataFrame,
    ) -> pd.DataFrame:
        """Convert signals and price data into a trade log.

        Parameters
        ----------
        signals:
            List of strategy signals.
        prices:
            DataFrame with columns ``['stock_code', 'date', 'close']``
            covering the full signal period.

        Returns
        -------
        DataFrame with columns: pair, open_date, close_date, open_rate,
        close_rate, profit_pct, direction, strategy, confidence.
        """
        if prices.empty:
            return pd.DataFrame(columns=[
                "pair", "open_date", "close_date", "open_rate",
                "close_rate", "profit_pct", "direction", "strategy",
                "confidence",
            ])

        # Ensure date column is datetime
        prices = prices.copy()
        prices["date"] = pd.to_datetime(prices["date"])
        prices = prices.sort_values(["stock_code", "date"])

        trades = []
        for sig in signals:
            stock_prices = prices[prices["stock_code"] == sig.stock_code]
            if stock_prices.empty:
                continue

            entry_dt = pd.Timestamp(sig.timestamp).tz_localize(None)
            exit_dt = entry_dt + pd.Timedelta(days=sig.holding_period_days)

            # Find nearest available entry price (on or after signal date)
            entry_candidates = stock_prices[stock_prices["date"] >= entry_dt]
            if entry_candidates.empty:
                continue
            entry_row = entry_candidates.iloc[0]

            # Find nearest available exit price (on or after target exit date)
            exit_candidates = stock_prices[stock_prices["date"] >= exit_dt]
            if exit_candidates.empty:
                # Use last available price
                exit_row = stock_prices.iloc[-1]
            else:
                exit_row = exit_candidates.iloc[0]

            open_rate = float(entry_row["close"])
            close_rate = float(exit_row["close"])

            if open_rate == 0:
                continue

            profit_pct = (close_rate - open_rate) / open_rate
            if sig.direction == "short":
                profit_pct = -profit_pct

            pair = _stock_code_to_pair(sig.stock_code)

            trades.append({
                "pair": pair,
                "open_date": entry_row["date"],
                "close_date": exit_row["date"],
                "open_rate": open_rate,
                "close_rate": close_rate,
                "profit_pct": round(profit_pct, 6),
                "direction": sig.direction,
                "strategy": sig.strategy_name,
                "confidence": sig.confidence,
            })

        if not trades:
            return pd.DataFrame(columns=[
                "pair", "open_date", "close_date", "open_rate",
                "close_rate", "profit_pct", "direction", "strategy",
                "confidence",
            ])

        return pd.DataFrame(trades)

    # ── signal export / import ────────────────────────────────────────

    @staticmethod
    def export_signals_json(
        signals: List[StrategySignal],
        output_path: str | Path,
    ) -> Path:
        """Export signals to a JSON file readable by generated Freqtrade
        strategies."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = [sig.to_dict() for sig in signals]
        path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        return path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stock_code_to_pair(stock_code: str) -> str:
    """Convert A-share stock code to Freqtrade pair format.

    ``600000.SH`` -> ``600000.SH/CNY``
    ``000001.SZ`` -> ``000001.SZ/CNY``
    """
    if "/" not in stock_code:
        return f"{stock_code}/CNY"
    return stock_code
