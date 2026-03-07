"""Phase 3: Integration tests — walk-forward validation and Sharpe comparison.

These tests run full backtests with dummy data to validate:
1. Walk-forward produces per-fold metrics
2. Sharpe comparison with/without costs quantifies simulation gap
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from quant.data.bars_loader import BarRow
from quant.data.pit_reader import BarsStore, PITDataReader
from quant.data.fx_repository import (
    create_engine as create_fx_engine,
    load_fx_csv_to_db,
)
from quant.data.symbols_repository import (
    create_sqlite_engine as create_symbols_engine,
    load_symbols_csv_to_db,
)
from quant.orchestrator.backtest import run_backtest
from quant.research.validation import (
    make_walk_forward_folds,
    run_walk_forward,
)
from quant.sdk.strategy import Strategy, Context


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_bar(symbol_id, day_offset, close, volume=10000):
    ts = datetime(2024, 1, 2 + day_offset, 21, 0, tzinfo=timezone.utc)
    return BarRow(
        ts=ts, symbol_id=symbol_id,
        open=close - 1, high=close + 2, low=close - 2, close=close,
        volume=volume, dt=ts.date(),
    )


@pytest.fixture
def integration_setup(tmp_path):
    """Build FX, symbols, and bars for integration tests."""
    # FX
    fx_engine = create_fx_engine(str(tmp_path / "fx.sqlite"))
    fx_csv = tmp_path / "fx.csv"
    fx_csv.write_text("ts,base_ccy,quote_ccy,rate\n2024-01-01T00:00:00Z,USD,EUR,0.85\n")
    load_fx_csv_to_db(str(fx_csv), fx_engine)

    # Symbols
    sym_engine = create_symbols_engine(str(tmp_path / "symbols.sqlite"))
    sym_csv = tmp_path / "symbols.csv"
    sym_csv.write_text(
        "symbol_id,ticker,exchange,currency,active_from,active_to\n"
        "1,TEST,XNYS,EUR,2020-01-01T00:00:00Z,\n"
    )
    load_symbols_csv_to_db(str(sym_csv), sym_engine)

    # Bars: 30 days of slowly rising prices
    bars = [_make_bar(1, i, 100.0 + i * 0.5) for i in range(30)]
    store = BarsStore.from_rows(bars)
    reader = PITDataReader(fx_engine, sym_engine, store)

    # Cost profile
    cost_yaml = tmp_path / "costs.yml"
    cost_yaml.write_text(
        "XNYS:\n"
        "  type: per_share_plus_fees\n"
        "  commission_per_share: 0.005\n"
        "  sec_fee_bps: 2.78\n"
        "  taf_per_share: 0.000166\n"
    )

    return reader, store, str(cost_yaml), tmp_path


# ---------------------------------------------------------------------------
# Simple strategy: buy on first bar, sell on last
# ---------------------------------------------------------------------------
class BuyAndHoldStrategy(Strategy):
    """Buys 10 shares on the first bar, holds until end."""

    def __init__(self):
        self._entered = False

    def on_event(self, evt, ctx: Context):
        if not self._entered:
            ctx.order_api.order(symbol_id=1, side="BUY", qty=10, type="MKT", tif="DAY")
            self._entered = True


class AlwaysBuyStrategy(Strategy):
    """Buys 1 share on every bar — generates many fills for cost comparison."""

    def on_event(self, evt, ctx: Context):
        ctx.order_api.order(symbol_id=1, side="BUY", qty=1, type="MKT", tif="DAY")


# ---------------------------------------------------------------------------
# Integration: full backtest
# ---------------------------------------------------------------------------
class TestFullBacktest:
    def test_backtest_produces_artifacts(self, integration_setup):
        reader, store, cost_yaml, tmp_path = integration_setup
        out_dir = tmp_path / "run_test"

        result = run_backtest(
            strategy=BuyAndHoldStrategy(),
            reader=reader,
            bars_store=store,
            start=datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc),
            end=datetime(2024, 1, 31, 21, 0, tzinfo=timezone.utc),
            costs_yaml_path=cost_yaml,
            out_dir=out_dir,
            seed=42,
            starting_cash=100_000.0,
        )

        assert result.run_id.startswith("run_")
        assert len(result.equity) > 0
        assert len(result.fills) > 0
        assert result.metrics["num_fills"] > 0
        assert (out_dir / "equity.csv").exists()
        assert (out_dir / "metrics.json").exists()
        assert (out_dir / "run_manifest.json").exists()

    def test_backtest_sharpe_is_finite(self, integration_setup):
        reader, store, cost_yaml, tmp_path = integration_setup
        result = run_backtest(
            strategy=BuyAndHoldStrategy(),
            reader=reader,
            bars_store=store,
            start=datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc),
            end=datetime(2024, 1, 31, 21, 0, tzinfo=timezone.utc),
            out_dir=tmp_path / "sharpe_test",
            seed=42,
            starting_cash=100_000.0,
        )
        import math
        assert math.isfinite(result.metrics["sharpe_ratio"])


# ---------------------------------------------------------------------------
# Integration: walk-forward
# ---------------------------------------------------------------------------
class TestWalkForwardIntegration:
    def test_walk_forward_produces_per_fold_metrics(self, integration_setup):
        reader, store, cost_yaml, tmp_path = integration_setup
        start = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 31, 21, 0, tzinfo=timezone.utc)

        folds = make_walk_forward_folds(
            store=store, start=start, end=end,
            train_window=10, test_window=5,
        )
        assert len(folds) >= 1

        results = run_walk_forward(
            strategy_factory=lambda params: BuyAndHoldStrategy(),
            strategy_params={"name": "buy_hold"},
            reader=reader,
            store=store,
            folds=folds,
            costs_yaml_path=cost_yaml,
            out_dir=tmp_path / "wf_run",
            base_seed=123,
        )

        assert len(results) == len(folds)
        for r in results:
            assert "metrics" in r
            assert "sharpe_ratio" in r["metrics"]
            assert r["fold_index"] >= 1

        # Summary file written
        summary = tmp_path / "wf_run" / "folds_summary.json"
        assert summary.exists()
        data = json.loads(summary.read_text())
        assert len(data) == len(folds)


# ---------------------------------------------------------------------------
# Integration: Sharpe comparison (with costs vs without)
# ---------------------------------------------------------------------------
class TestSharpeComparison:
    def test_costs_reduce_sharpe(self, integration_setup):
        reader, store, cost_yaml, tmp_path = integration_setup
        start = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 31, 21, 0, tzinfo=timezone.utc)

        # Run WITHOUT costs
        result_no_cost = run_backtest(
            strategy=AlwaysBuyStrategy(),
            reader=reader,
            bars_store=store,
            start=start, end=end,
            costs_yaml_path=None,
            out_dir=tmp_path / "no_cost",
            seed=42,
            starting_cash=100_000.0,
        )

        # Run WITH costs
        result_with_cost = run_backtest(
            strategy=AlwaysBuyStrategy(),
            reader=reader,
            bars_store=store,
            start=start, end=end,
            costs_yaml_path=cost_yaml,
            out_dir=tmp_path / "with_cost",
            seed=42,
            starting_cash=100_000.0,
        )

        # Costs should reduce final equity (and thus Sharpe)
        assert result_with_cost.metrics["final_equity_eur"] <= result_no_cost.metrics["final_equity_eur"]

        # The gap between the two measures the simulation cost drag
        gap = result_no_cost.metrics["final_equity_eur"] - result_with_cost.metrics["final_equity_eur"]
        assert gap >= 0, "Cost drag should be non-negative"
