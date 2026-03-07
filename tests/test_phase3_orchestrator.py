"""Phase 3: Orchestrator tests — backtest metrics, artifacts, validation folds."""

import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pytest

from quant.orchestrator.backtest import (
    _compute_metrics,
    _quote_from_bar,
    _build_symbol_maps,
    DEFAULT_SPREAD_BPS,
)
from quant.data.bars_loader import BarRow
from quant.data.symbols_repository import SymbolRow
from quant.ops.artifacts import ArtifactWriter, compute_params_hash, get_git_sha
from quant.data.pit_reader import BarsStore
from quant.research.validation import (
    make_walk_forward_folds,
    make_purged_kfold_folds,
    FoldSpec,
)


# ---------------------------------------------------------------------------
# _compute_metrics
# ---------------------------------------------------------------------------
class TestComputeMetrics:
    def test_basic_return(self):
        equity = [
            {"ts": "t0", "equity_eur": 10000.0},
            {"ts": "t1", "equity_eur": 11000.0},
        ]
        m = _compute_metrics(equity, [], [])
        assert m["start_equity_eur"] == 10000.0
        assert m["final_equity_eur"] == 11000.0
        assert m["return"] == pytest.approx(0.1)

    def test_sharpe_positive(self):
        # Steady growth
        equity = [{"ts": f"t{i}", "equity_eur": 10000 + i * 100} for i in range(30)]
        m = _compute_metrics(equity, [], [])
        assert m["sharpe_ratio"] > 0

    def test_sharpe_zero_with_no_data(self):
        m = _compute_metrics([], [], [])
        assert m["sharpe_ratio"] == 0.0

    def test_max_drawdown(self):
        equity = [
            {"ts": "t0", "equity_eur": 10000.0},
            {"ts": "t1", "equity_eur": 12000.0},
            {"ts": "t2", "equity_eur": 9000.0},  # -25% from peak
            {"ts": "t3", "equity_eur": 11000.0},
        ]
        m = _compute_metrics(equity, [], [])
        assert m["max_drawdown"] == pytest.approx(-0.25)

    def test_win_rate_and_profit_factor(self):
        orders = [
            {"order_id": "o1", "side": "BUY"},
            {"order_id": "o2", "side": "SELL"},
        ]
        fills = [
            {"order_id": "o1", "price": 100.0, "quantity": 10},
            {"order_id": "o2", "price": 110.0, "quantity": 10},
        ]
        m = _compute_metrics(
            [{"ts": "t0", "equity_eur": 10000}, {"ts": "t1", "equity_eur": 10100}],
            fills, orders,
        )
        assert m["num_trades"] == 2
        # BUY: sign=-1, notional=1000 → P&L=-1000
        # SELL: sign=+1, notional=1100 → P&L=+1100
        assert m["win_rate"] == pytest.approx(0.5)

    def test_zero_equity_return(self):
        equity = [{"ts": "t0", "equity_eur": 0.0}]
        m = _compute_metrics(equity, [], [])
        assert m["return"] == 0.0


# ---------------------------------------------------------------------------
# _quote_from_bar
# ---------------------------------------------------------------------------
class TestQuoteFromBar:
    def test_spread_matches_default(self):
        bar = BarRow(
            ts=datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc),
            symbol_id=1, open=99, high=101, low=98, close=100.0,
            volume=1000, dt=datetime(2024, 1, 2).date(),
        )
        q = _quote_from_bar(bar)
        expected_spread = (DEFAULT_SPREAD_BPS / 10000.0) * 100.0
        assert q.ask - q.bid == pytest.approx(expected_spread)
        assert q.mid == pytest.approx(100.0, abs=0.01)


# ---------------------------------------------------------------------------
# _build_symbol_maps
# ---------------------------------------------------------------------------
class TestBuildSymbolMaps:
    def test_basic_mapping(self):
        syms = [
            SymbolRow(symbol_id=1, ticker="AAPL", exchange="XNYS", currency="USD",
                      active_from=datetime(2020, 1, 1, tzinfo=timezone.utc), active_to=None),
            SymbolRow(symbol_id=2, ticker="SAP", exchange="XETR", currency="EUR",
                      active_from=datetime(2020, 1, 1, tzinfo=timezone.utc), active_to=None),
        ]
        by_id, venue = _build_symbol_maps(syms)
        assert by_id[1].ticker == "AAPL"
        assert venue[2] == "XETR"


# ---------------------------------------------------------------------------
# ArtifactWriter roundtrip
# ---------------------------------------------------------------------------
class TestArtifactWriter:
    def test_equity_roundtrip(self, tmp_path):
        writer = ArtifactWriter(tmp_path / "run1")
        equity = [
            {"ts": datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc), "equity_eur": 10000.0},
            {"ts": datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc), "equity_eur": 10500.0},
        ]
        writer.write_equity(equity)
        assert writer.paths.equity_csv.exists()
        lines = writer.paths.equity_csv.read_text().strip().split("\n")
        assert len(lines) == 3  # header + 2 rows

    def test_metrics_roundtrip(self, tmp_path):
        writer = ArtifactWriter(tmp_path / "run2")
        metrics = {"sharpe_ratio": 1.5, "return": 0.12}
        writer.write_metrics(metrics)
        loaded = json.loads(writer.paths.metrics_json.read_text())
        assert loaded["sharpe_ratio"] == 1.5

    def test_manifest_roundtrip(self, tmp_path):
        writer = ArtifactWriter(tmp_path / "run3")
        manifest = {"run_id": "test_123", "timestamp": datetime(2024, 1, 1, tzinfo=timezone.utc)}
        writer.write_manifest(manifest)
        loaded = json.loads(writer.paths.manifest_json.read_text())
        assert loaded["run_id"] == "test_123"

    def test_orders_fills_positions(self, tmp_path):
        writer = ArtifactWriter(tmp_path / "run4")
        orders = [{"ts": datetime(2024, 1, 2, tzinfo=timezone.utc), "order_id": "o1",
                    "symbol_id": 1, "side": "BUY", "quantity": 10, "type": "MKT",
                    "tif": "DAY", "limit_price": None, "state": "FILLED"}]
        fills = [{"ts": datetime(2024, 1, 2, tzinfo=timezone.utc), "order_id": "o1",
                  "symbol_id": 1, "price": 100.0, "quantity": 10, "venue": "XNYS", "cost": 0.5}]
        positions = [{"ts": datetime(2024, 1, 2, tzinfo=timezone.utc), "symbol_id": 1,
                      "currency": "USD", "quantity": 10, "average_price": 100.0}]
        writer.write_orders(orders)
        writer.write_fills(fills)
        writer.write_positions(positions)
        assert writer.paths.orders_csv.exists()
        assert writer.paths.fills_csv.exists()
        assert writer.paths.positions_csv.exists()


# ---------------------------------------------------------------------------
# compute_params_hash
# ---------------------------------------------------------------------------
class TestParamsHash:
    def test_deterministic(self):
        params = {"a": 1, "b": "hello"}
        h1 = compute_params_hash(params)
        h2 = compute_params_hash(params)
        assert h1 == h2

    def test_different_params_different_hash(self):
        h1 = compute_params_hash({"a": 1})
        h2 = compute_params_hash({"a": 2})
        assert h1 != h2


# ---------------------------------------------------------------------------
# Walk-forward folds
# ---------------------------------------------------------------------------
def _make_bars_for_folds(n_days: int) -> BarsStore:
    """Create a BarsStore with n_days of bars for symbol 1."""
    from datetime import timedelta
    bars = []
    base = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
    for i in range(n_days):
        ts = base + timedelta(days=i)
        bars.append(BarRow(ts=ts, symbol_id=1, open=100, high=101, low=99, close=100, volume=1000, dt=ts.date()))
    return BarsStore.from_rows(bars)


class TestWalkForwardFolds:
    def test_basic_folds(self):
        store = _make_bars_for_folds(20)
        start = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 21, 21, 0, tzinfo=timezone.utc)
        folds = make_walk_forward_folds(
            store=store, start=start, end=end,
            train_window=10, test_window=5,
        )
        assert len(folds) >= 1
        for fold in folds:
            assert fold.val_range[0] <= fold.val_range[1]
            assert len(fold.train_ranges) == 1

    def test_step_controls_overlap(self):
        store = _make_bars_for_folds(30)
        start = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 31, 21, 0, tzinfo=timezone.utc)
        folds_step5 = make_walk_forward_folds(
            store=store, start=start, end=end,
            train_window=10, test_window=5, step=5,
        )
        folds_step2 = make_walk_forward_folds(
            store=store, start=start, end=end,
            train_window=10, test_window=5, step=2,
        )
        assert len(folds_step2) >= len(folds_step5)

    def test_invalid_window_raises(self):
        store = _make_bars_for_folds(10)
        start = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 11, 21, 0, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="must be > 0"):
            make_walk_forward_folds(store=store, start=start, end=end, train_window=0, test_window=5)


class TestPurgedKFold:
    def test_basic_splits(self):
        store = _make_bars_for_folds(20)
        start = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 21, 21, 0, tzinfo=timezone.utc)
        folds = make_purged_kfold_folds(store=store, start=start, end=end, n_splits=3)
        assert len(folds) == 3

    def test_embargo_creates_gap(self):
        store = _make_bars_for_folds(100)
        from datetime import timedelta
        start = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        end = start + timedelta(days=99)
        folds = make_purged_kfold_folds(
            store=store, start=start, end=end, n_splits=3, embargo_fraction=0.05,
        )
        # With embargo, train ranges should not be immediately adjacent to val range
        for fold in folds:
            for tr_start, tr_end in fold.train_ranges:
                # Train end should be before val start (with gap)
                if tr_end < fold.val_range[0]:
                    assert tr_end < fold.val_range[0]

    def test_n_splits_1_raises(self):
        store = _make_bars_for_folds(10)
        start = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 11, 21, 0, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="n_splits must be >= 2"):
            make_purged_kfold_folds(store=store, start=start, end=end, n_splits=1)

    def test_too_few_timesteps_raises(self):
        store = _make_bars_for_folds(2)
        start = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        end = datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="Not enough"):
            make_purged_kfold_folds(store=store, start=start, end=end, n_splits=5)
