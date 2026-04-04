"""Shared fixtures for simple-strategy integration tests.

Provides ready-to-use ``PITDataReader`` and ``BarsStore`` instances
backed by the dummy CSV data shipped with the repository.
"""

from __future__ import annotations

import pytest
from datetime import datetime
from pathlib import Path

from quant.data.bars_loader import load_daily_bars_csv, load_minute_bars_csv
from quant.data.pit_reader import PITDataReader, BarsStore
from quant.data.symbols_repository import create_sqlite_engine, load_symbols_csv_to_db
from quant.data.fx_repository import create_engine as create_fx_engine, load_fx_csv_to_db

DUMMY_DIR = Path("quant/data/dummy")


@pytest.fixture()
def daily_reader_and_store():
    """Return (PITDataReader, BarsStore) for daily dummy bars."""
    rows, validation = load_daily_bars_csv(str(DUMMY_DIR / "daily_bars.csv"), "XNYS")
    assert not validation.missing_dates
    store = BarsStore.from_rows(rows)

    symbols_engine = create_sqlite_engine(":memory:")
    fx_engine = create_fx_engine(":memory:")
    load_symbols_csv_to_db(str(DUMMY_DIR / "symbols.csv"), symbols_engine)
    load_fx_csv_to_db(str(DUMMY_DIR / "fx.csv"), fx_engine)

    reader = PITDataReader(fx_engine, symbols_engine, store)
    return reader, store


@pytest.fixture()
def minute_reader_and_store():
    """Return (PITDataReader, BarsStore) for 1-minute dummy bars."""
    csv_path = DUMMY_DIR / "minute_bars.csv"
    if not csv_path.exists():
        pytest.skip("minute_bars.csv not generated; run generate_minute_bars.py first")

    rows, validation = load_minute_bars_csv(str(csv_path), "XNYS")
    store = BarsStore.from_rows(rows)

    symbols_engine = create_sqlite_engine(":memory:")
    fx_engine = create_fx_engine(":memory:")
    load_symbols_csv_to_db(str(DUMMY_DIR / "symbols.csv"), symbols_engine)
    load_fx_csv_to_db(str(DUMMY_DIR / "fx.csv"), fx_engine)

    reader = PITDataReader(fx_engine, symbols_engine, store)
    return reader, store
