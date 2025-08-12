from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from quant.data.bars_loader import BarRow
from quant.data.pit_reader import BarsStore, PITDataReader
from quant.data.fx_repository import ensure_schema as fx_ensure_schema, load_fx_csv_to_db
from quant.data.symbols_repository import ensure_schema as sym_ensure_schema, create_sqlite_engine, load_symbols_csv_to_db
from quant.examples.ma_cross import MACross
from quant.orchestrator.backtest import run_backtest


def _dt(s: str):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def test_seed_determinism(tmp_path: Path) -> None:
    # Prepare bars for a single symbol (id=1)
    rows = [
        BarRow(ts=_dt("2024-06-03T20:00:00Z"), symbol_id=1, open=100, high=101, low=99, close=100, volume=1000, dt=_dt("2024-06-03T00:00:00Z").date()),
        BarRow(ts=_dt("2024-06-04T20:00:00Z"), symbol_id=1, open=101, high=102, low=100, close=101, volume=1100, dt=_dt("2024-06-04T00:00:00Z").date()),
        BarRow(ts=_dt("2024-06-05T20:00:00Z"), symbol_id=1, open=102, high=103, low=101, close=102, volume=1200, dt=_dt("2024-06-05T00:00:00Z").date()),
    ]
    store = BarsStore.from_rows(rows)

    # FX & Symbols
    fx_engine = create_sqlite_engine()
    fx_ensure_schema(fx_engine)
    # Simple FX with constant USD/EUR
    import tempfile, os
    tmpdir = tempfile.mkdtemp()
    fx_path = os.path.join(tmpdir, "fx.csv")
    with open(fx_path, "w", newline="") as f:
        f.write("ts,base_ccy,quote_ccy,rate\n")
        f.write("2024-06-01T00:00:00Z,USD,EUR,0.92\n")
    load_fx_csv_to_db(fx_path, fx_engine)

    sym_engine = create_sqlite_engine()
    sym_ensure_schema(sym_engine)
    # AAPL as symbol 1 in USD
    import tempfile as _tf, os as _os
    sym_tmpdir = _tf.mkdtemp()
    sym_path = _os.path.join(sym_tmpdir, "symbols.csv")
    with open(sym_path, "w", newline="") as f:
        f.write("symbol_id,ticker,exchange,currency,active_from,active_to\n")
        f.write("1,AAPL,XNAS,USD,2020-01-01T00:00:00Z,\n")
    load_symbols_csv_to_db(sym_path, sym_engine)

    reader = PITDataReader(fx_engine, sym_engine, store)

    strat = MACross(symbol="AAPL", fast=2, slow=3)

    out1 = tmp_path / "r1"
    out2 = tmp_path / "r2"

    res1 = run_backtest(
        strategy=strat,
        reader=reader,
        bars_store=store,
        start=rows[0].ts,
        end=rows[-1].ts,
        seed=42,
        out_dir=out1,
    )

    # Run again with same seed
    strat2 = MACross(symbol="AAPL", fast=2, slow=3)
    res2 = run_backtest(
        strategy=strat2,
        reader=reader,
        bars_store=store,
        start=rows[0].ts,
        end=rows[-1].ts,
        seed=42,
        out_dir=out2,
    )

    # Compare key artifacts
    eq1 = (out1 / "equity.csv").read_text()
    eq2 = (out2 / "equity.csv").read_text()
    assert eq1 == eq2

    m1 = (out1 / "metrics.json").read_text()
    m2 = (out2 / "metrics.json").read_text()
    assert m1 == m2

    o1 = (out1 / "orders.csv").read_text()
    o2 = (out2 / "orders.csv").read_text()
    assert o1 == o2

    f1 = (out1 / "fills.csv").read_text()
    f2 = (out2 / "fills.csv").read_text()
    assert f1 == f2