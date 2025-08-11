from __future__ import annotations

from datetime import datetime, timezone

import pytest

from quant.data.pit_reader import BarsStore, PITDataReader
from quant.data.bars_loader import BarRow
from quant.data.fx_repository import ensure_schema as fx_ensure_schema, load_fx_csv_to_db
from quant.data.symbols_repository import ensure_schema as sym_ensure_schema, create_sqlite_engine


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def test_no_peek_guard_on_bars() -> None:
    # Prepare bars store with two bars
    rows = [
        BarRow(ts=_dt("2024-06-03T20:00:00Z"), symbol_id=1, open=1, high=1, low=1, close=1, volume=1, dt=_dt("2024-06-03T00:00:00Z").date()),
        BarRow(ts=_dt("2024-06-04T20:00:00Z"), symbol_id=1, open=1, high=1, low=1, close=1, volume=1, dt=_dt("2024-06-04T00:00:00Z").date()),
    ]
    store = BarsStore.from_rows(rows)

    # FX/Symbol engines (empty; we only test bars here)
    fx_engine = create_sqlite_engine()
    fx_ensure_schema(fx_engine)
    sym_engine = create_sqlite_engine()
    sym_ensure_schema(sym_engine)

    rdr = PITDataReader(fx_engine, sym_engine, store)

    # asof before second bar; requesting end beyond asof should raise
    with pytest.raises(ValueError):
        rdr.get_bars(1, None, _dt("2024-06-05T00:00:00Z"), asof=_dt("2024-06-04T00:00:00Z"))

    # valid request up to asof returns only first bar
    out = rdr.get_bars(1, None, None, asof=_dt("2024-06-03T23:59:59Z"))
    assert [b.ts.isoformat() for b in out] == ["2024-06-03T20:00:00+00:00"]


def test_fx_pit_reader_returns_last_available() -> None:
    fx_engine = create_sqlite_engine()
    fx_ensure_schema(fx_engine)

    # Load two days of FX
    from pathlib import Path
    import tempfile
    import os

    tmpdir = tempfile.mkdtemp()
    path = os.path.join(tmpdir, "fx.csv")
    with open(path, "w", newline="") as f:
        f.write("ts,base_ccy,quote_ccy,rate\n")
        f.write("2024-06-01T00:00:00Z,USD,EUR,0.92\n")
        f.write("2024-06-03T00:00:00Z,USD,EUR,0.93\n")

    load_fx_csv_to_db(path, fx_engine)

    sym_engine = create_sqlite_engine()
    sym_ensure_schema(sym_engine)

    rdr = PITDataReader(fx_engine, sym_engine, BarsStore.from_rows([]))
    fx = rdr.get_fx("USD", "EUR", asof=_dt("2024-06-02T12:00:00Z"))
    assert fx.rate == pytest.approx(0.92)