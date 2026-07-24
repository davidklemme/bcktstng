"""Backtest runner for the Geopolitical Whisper strategy using Lumibot.

Loads local 1-minute CSV data (Polygon / Massive format) into Lumibot's
``PandasDataBacktesting`` backend and runs the strategy with IBKR-tiered
commissions and a slippage penalty.

Usage:
    python -m quant.strategies.lumibot.run_backtest
"""

from __future__ import annotations

import logging
from datetime import datetime, time
from pathlib import Path

import pandas as pd
from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.traders import Trader

from .geopolitical_whisper_lumibot import GeopoliticalWhisperLumibot

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "dummy"
CSV_PATH = DATA_DIR / "minute_bars.csv"


def load_pandas_data(csv_path: Path, symbol: str = "AAPL") -> Data:
    """Load a local minute-bars CSV into a Lumibot ``Data`` object."""
    df = pd.read_csv(csv_path)

    # Parse timestamps
    if "ts_local" in df.columns:
        df["datetime"] = pd.to_datetime(df["ts_local"])
    elif "ts" in df.columns:
        df["datetime"] = pd.to_datetime(df["ts"])
    else:
        raise ValueError("CSV must have 'ts_local' or 'ts' column")

    df = df.set_index("datetime")
    df.index = df.index.tz_localize("America/New_York")

    # Ensure required OHLCV columns
    for col in ("open", "high", "low", "close", "volume"):
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Keep only OHLCV
    df = df[["open", "high", "low", "close", "volume"]].copy()
    df = df.sort_index()

    asset = Asset(symbol=symbol, asset_type="stock")
    return Data(
        asset,
        df,
        timestep="minute",
        trading_hours_start=time(9, 30),
        trading_hours_end=time(16, 0),
        timezone="America/New_York",
    )


def main() -> None:
    pandas_data = {
        "AAPL": load_pandas_data(CSV_PATH, "AAPL"),
    }

    date_start = datetime(2024, 1, 2)
    date_end = datetime(2024, 1, 4)

    GeopoliticalWhisperLumibot.backtest(
        PandasDataBacktesting,
        date_start,
        date_end,
        pandas_data=pandas_data,
        parameters={"symbol": "AAPL"},
        budget=10_000,
        name="GeopoliticalWhisper",
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
    )


if __name__ == "__main__":
    main()
