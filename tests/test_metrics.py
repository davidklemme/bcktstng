from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient

from quant.data.bars_loader import BarRow
from quant.data.pit_reader import BarsStore, PITDataReader
from quant.data.fx_repository import ensure_schema as fx_ensure_schema
from quant.data.symbols_repository import ensure_schema as sym_ensure_schema, create_sqlite_engine, load_symbols_csv_to_db
from quant.examples.ma_cross import MACross
from quant.orchestrator.backtest import run_backtest
from quant.orchestrator.service import app


def _dt(s: str):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def test_metrics_endpoint_contains_quant_metrics(tmp_path) -> None:
    # Minimal setup to exercise metrics
    rows = [
        BarRow(ts=_dt("2024-06-03T20:00:00Z"), symbol_id=1, open=100, high=101, low=99, close=100, volume=1000, dt=_dt("2024-06-03T00:00:00Z").date()),
        BarRow(ts=_dt("2024-06-04T20:00:00Z"), symbol_id=1, open=101, high=102, low=100, close=101, volume=1100, dt=_dt("2024-06-04T00:00:00Z").date()),
    ]
    store = BarsStore.from_rows(rows)

    fx_engine = create_sqlite_engine()
    fx_ensure_schema(fx_engine)

    sym_engine = create_sqlite_engine()
    sym_ensure_schema(sym_engine)
    # Add AAPL symbol
    import tempfile, os
    d = tempfile.mkdtemp()
    p = os.path.join(d, "symbols.csv")
    with open(p, "w", newline="") as f:
        f.write("symbol_id,ticker,exchange,currency,active_from,active_to\n")
        f.write("1,AAPL,XNAS,USD,2020-01-01T00:00:00Z,\n")
    load_symbols_csv_to_db(p, sym_engine)

    reader = PITDataReader(fx_engine, sym_engine, store)
    strat = MACross(symbol="AAPL", fast=2, slow=3)

    run_backtest(
        strategy=strat,
        reader=reader,
        bars_store=store,
        start=rows[0].ts,
        end=rows[-1].ts,
        seed=123,
        out_dir=tmp_path / "out",
    )

    client = TestClient(app)
    resp = client.get("/metrics")
    assert resp.status_code == 200
    body = resp.text
    assert "quant_events_total" in body
    assert "quant_fill_slippage_bps" in body