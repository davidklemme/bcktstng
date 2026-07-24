"""Regression tests for backtest fill timing, marking and accounting.

Each test here pins a defect that previously made results silently wrong.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, List

import pytest

from quant.data.bars_loader import BarRow
from quant.data.fx_repository import create_engine as create_fx_engine, load_fx_csv_to_db
from quant.data.pit_reader import BarsStore, PITDataReader
from quant.data.symbols_repository import create_sqlite_engine, load_symbols_csv_to_db
from quant.orchestrator.backtest import (
    FILL_TIMING_NEXT_OPEN,
    FILL_TIMING_SAME_CLOSE,
    run_backtest,
)
from quant.sdk.strategy import Context, Strategy

DUMMY = Path("quant/data/dummy")


def _bars(symbol_id: int, specs: List[tuple]) -> List[BarRow]:
    """specs: list of (ts, open, high, low, close, volume)."""
    return [
        BarRow(ts=ts, symbol_id=symbol_id, open=o, high=h, low=l, close=c,
               volume=v, dt=ts.date())
        for ts, o, h, l, c, v in specs
    ]


def _reader(store: BarsStore) -> PITDataReader:
    se = create_sqlite_engine(":memory:")
    fe = create_fx_engine(":memory:")
    load_symbols_csv_to_db(str(DUMMY / "symbols.csv"), se)
    load_fx_csv_to_db(str(DUMMY / "fx.csv"), fe)
    return PITDataReader(fe, se, store)


class _BuyOnceAtBar(Strategy):
    """Buys `qty` the first time it sees a bar, then never again."""

    def __init__(self, ticker: str = "AAPL", qty: int = 10) -> None:
        self.ticker = ticker
        self.qty = qty
        self.done = False

    def on_event(self, evt: Any, ctx: Context) -> None:
        if self.done:
            return
        self.done = True
        ctx.order(self.ticker, self.qty, side="BUY", type="MKT")


def test_orders_fill_at_next_bar_open_not_same_bar_close(tmp_path):
    """The strategy observes bar 0 and orders; the fill must come from bar 1's
    open, never from bar 0 (which it had already seen when deciding)."""
    t0 = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(days=1)
    # Bar 0 close is 100; bar 1 open is a very distinct 200 so the fill
    # price identifies unambiguously which bar was used.
    store = BarsStore.from_rows(_bars(1, [
        (t0, 100.0, 100.0, 100.0, 100.0, 1_000_000),
        (t1, 200.0, 200.0, 200.0, 200.0, 1_000_000),
    ]))
    res = run_backtest(
        strategy=_BuyOnceAtBar(), reader=_reader(store), bars_store=store,
        start=t0, end=t1, starting_cash=100_000.0, out_dir=tmp_path,
        fill_timing=FILL_TIMING_NEXT_OPEN,
    )
    assert len(res.fills) == 1
    fill = res.fills[0]
    assert fill["ts"] == t1, "fill must be stamped on the NEXT bar"
    # ~200 plus half the synthetic spread, definitively not ~100.
    assert 199.0 < fill["price"] < 201.0, f"filled at {fill['price']}, expected ~200 (bar 1 open)"


def test_same_close_timing_still_available_but_differs(tmp_path):
    """The legacy same-bar mode is retained for reproducing old runs and must
    demonstrably differ from the correct mode."""
    t0 = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(days=1)
    store = BarsStore.from_rows(_bars(1, [
        (t0, 100.0, 100.0, 100.0, 100.0, 1_000_000),
        (t1, 200.0, 200.0, 200.0, 200.0, 1_000_000),
    ]))
    res = run_backtest(
        strategy=_BuyOnceAtBar(), reader=_reader(store), bars_store=store,
        start=t0, end=t1, starting_cash=100_000.0, out_dir=tmp_path,
        fill_timing=FILL_TIMING_SAME_CLOSE,
    )
    assert res.fills[0]["ts"] == t0
    assert 99.0 < res.fills[0]["price"] < 101.0


def test_position_keeps_last_mark_when_symbol_does_not_print(tmp_path):
    """A symbol that stops printing must retain its previous mark rather than
    dropping out of equity and manufacturing a phantom drawdown."""
    t0 = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
    ts = [t0 + timedelta(days=i) for i in range(4)]
    # AAPL (id 1) prints on every bar. MSFT (id 2) stops after bar 1.
    rows = _bars(1, [(t, 100.0, 100.0, 100.0, 100.0, 1_000_000) for t in ts])
    rows += _bars(2, [(t, 50.0, 50.0, 50.0, 50.0, 1_000_000) for t in ts[:2]])
    store = BarsStore.from_rows(rows)

    res = run_backtest(
        strategy=_BuyOnceAtBar(ticker="MSFT", qty=100), reader=_reader(store),
        bars_store=store, start=ts[0], end=ts[-1], starting_cash=100_000.0,
        out_dir=tmp_path,
    )
    equity = [p["equity_eur"] for p in res.equity]
    # Once MSFT stops printing its 100 shares must still be marked at 50.
    assert equity[-1] == pytest.approx(equity[-2], rel=1e-9), (
        f"equity collapsed when a held symbol stopped printing: {equity}"
    )
    assert equity[-1] > 99_000, f"position vanished from equity: {equity}"


def test_unfunded_run_reports_undefined_return_not_zero(tmp_path):
    """A run with no capital must not report a confident 0.0% return."""
    t0 = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
    store = BarsStore.from_rows(_bars(1, [
        (t0 + timedelta(days=i), 100.0, 100.0, 100.0, 100.0 + i, 1_000_000)
        for i in range(5)
    ]))
    res = run_backtest(
        strategy=_BuyOnceAtBar(), reader=_reader(store), bars_store=store,
        start=t0, end=t0 + timedelta(days=4), starting_cash=0.0, out_dir=tmp_path,
    )
    assert res.metrics["total_return"] is None
    assert any("starting_cash" in w for w in res.metrics["warnings"])


def test_funded_run_reports_real_metrics(tmp_path):
    """A funded run must produce the risk metrics the docs advertise."""
    t0 = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
    store = BarsStore.from_rows(_bars(1, [
        (t0 + timedelta(days=i), 100.0 + i, 100.0 + i, 100.0 + i, 100.0 + i, 1_000_000)
        for i in range(30)
    ]))
    res = run_backtest(
        strategy=_BuyOnceAtBar(qty=100), reader=_reader(store), bars_store=store,
        start=t0, end=t0 + timedelta(days=29), starting_cash=100_000.0,
        out_dir=tmp_path,
    )
    m = res.metrics
    assert m["initial_capital"] == 100_000.0
    assert m["total_return"] is not None
    for key in ("sharpe", "max_drawdown", "ann_volatility", "exposure", "turnover"):
        assert key in m, f"missing advertised metric: {key}"
    assert m["max_drawdown"] <= 0.0
    assert 0.0 <= m["exposure"] <= 1.0


def test_orders_submitted_in_on_end_are_recorded_not_silently_dropped(tmp_path):
    """on_end runs after the last bar, so its orders cannot fill - but they
    must be visible in the artifacts rather than disappearing."""

    class _OrdersInOnEnd(Strategy):
        def on_end(self, ctx: Context) -> None:
            ctx.order("AAPL", 5, side="SELL", type="MKT")

    t0 = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
    store = BarsStore.from_rows(_bars(1, [
        (t0 + timedelta(days=i), 100.0, 100.0, 100.0, 100.0, 1_000_000)
        for i in range(3)
    ]))
    res = run_backtest(
        strategy=_OrdersInOnEnd(), reader=_reader(store), bars_store=store,
        start=t0, end=t0 + timedelta(days=2), starting_cash=10_000.0,
        out_dir=tmp_path,
    )
    notes = [o["note"] for o in res.orders]
    assert "submitted_in_on_end_not_executable" in notes
    assert res.fills == []


def test_order_ids_are_unique_across_bars(tmp_path):
    """Order ids previously restarted at order_1 every bar, making any
    fills-to-orders join wrong."""

    class _BuyEveryBar(Strategy):
        def on_event(self, evt: Any, ctx: Context) -> None:
            ctx.order("AAPL", 1, side="BUY", type="MKT")

    t0 = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
    store = BarsStore.from_rows(_bars(1, [
        (t0 + timedelta(days=i), 100.0, 100.0, 100.0, 100.0, 1_000_000)
        for i in range(5)
    ]))
    res = run_backtest(
        strategy=_BuyEveryBar(), reader=_reader(store), bars_store=store,
        start=t0, end=t0 + timedelta(days=4), starting_cash=10_000.0,
        out_dir=tmp_path,
    )
    ids = [o["order_id"] for o in res.orders]
    assert len(ids) == len(set(ids)), f"duplicate order ids: {ids}"
    # Filled orders must no longer be reported as state=NEW.
    filled = [o for o in res.orders if o["filled_quantity"] > 0]
    assert filled and all(o["state"] != "NEW" for o in filled)
