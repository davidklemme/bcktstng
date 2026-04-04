"""Tests for the Geopolitical Whisper strategy.

Covers:
1. Pure signal evaluation (``evaluate_signal``) — no Lumibot runtime needed.
2. Look-ahead bias verification — signal must not fire on wrong rows.
3. Edge cases: zero volume, insufficient data, boundary conditions.
4. Custom engine integration smoke test.
"""

from __future__ import annotations

import json
import math
from datetime import datetime
from pathlib import Path
from typing import List

import pytest

from quant.strategies.simple.geopolitical_whisper import (
    GeopoliticalWhisper,
    evaluate_signal,
    LOOKBACK,
    RVOL_THRESHOLD,
    PRICE_CHANGE_BAND,
)
from quant.strategies.lumibot.geopolitical_whisper_lumibot import (
    evaluate_signal as evaluate_signal_lumibot,
)
from quant.orchestrator.backtest import run_backtest


# =====================================================================
# Unit tests for evaluate_signal (pure function, no framework deps)
# =====================================================================

class TestEvaluateSignal:
    """Tests for the pure-function signal evaluator."""

    def _make_flat_data(
        self,
        n: int = LOOKBACK + 1,
        base_price: float = 100.0,
        base_volume: int = 1000,
        spike_volume: int | None = None,
    ) -> tuple[List[float], List[int]]:
        """Generate a flat-price series with optional volume spike on the last bar."""
        closes = [base_price] * n
        volumes = [base_volume] * n
        if spike_volume is not None:
            volumes[-1] = spike_volume
        return closes, volumes

    def test_signal_fires_on_valid_spike(self):
        """Classic case: huge volume spike, flat price -> signal."""
        closes, volumes = self._make_flat_data(
            spike_volume=int(1000 * (RVOL_THRESHOLD + 1))  # well above threshold
        )
        result = evaluate_signal(closes, volumes)
        assert result["signal"] is True
        assert result["reason"] == "signal_triggered"
        assert result["rvol"] > RVOL_THRESHOLD

    def test_no_signal_when_volume_normal(self):
        """Normal volume, flat price -> no signal."""
        closes, volumes = self._make_flat_data(spike_volume=1000)
        result = evaluate_signal(closes, volumes)
        assert result["signal"] is False
        assert result["reason"] == "rvol_too_low"

    def test_no_signal_when_price_moves_too_much(self):
        """Huge volume spike, but price drifted 1% -> no signal."""
        closes, volumes = self._make_flat_data(
            spike_volume=int(1000 * (RVOL_THRESHOLD + 1))
        )
        # Make the current close 1% higher than the lookback-ago close
        closes[-1] = closes[0] * 1.01
        result = evaluate_signal(closes, volumes)
        assert result["signal"] is False
        assert result["reason"] == "price_moved_too_much"

    def test_no_signal_beyond_boundary(self):
        """Price change clearly beyond ±0.5% -> no signal."""
        closes, volumes = self._make_flat_data(
            spike_volume=int(1000 * (RVOL_THRESHOLD + 1))
        )
        # Set price change to clearly above +0.5% (use +0.6%)
        closes[-1] = closes[0] * 1.006
        result = evaluate_signal(closes, volumes)
        assert result["signal"] is False, "Signal should NOT fire above +0.5%"

        # Set price change to clearly below -0.5% (use -0.6%)
        closes[-1] = closes[0] * 0.994
        result = evaluate_signal(closes, volumes)
        assert result["signal"] is False, "Signal should NOT fire below -0.5%"

    def test_signal_fires_just_inside_boundary(self):
        """Price change at ±0.499% -> signal (just inside the band)."""
        closes, volumes = self._make_flat_data(
            spike_volume=int(1000 * (RVOL_THRESHOLD + 1))
        )
        closes[-1] = closes[0] * (1 + 0.00499)
        result = evaluate_signal(closes, volumes)
        assert result["signal"] is True

    def test_insufficient_data_returns_no_signal(self):
        """Too few bars -> no signal."""
        closes = [100.0] * 5
        volumes = [1000] * 5
        result = evaluate_signal(closes, volumes, lookback=15)
        assert result["signal"] is False
        assert result["reason"] == "insufficient_data"

    def test_zero_volume_sma_returns_no_signal(self):
        """All historical volumes zero -> division-by-zero guard."""
        closes = [100.0] * (LOOKBACK + 1)
        volumes = [0] * LOOKBACK + [5000]
        result = evaluate_signal(closes, volumes)
        assert result["signal"] is False
        assert result["reason"] == "zero_vol_sma"

    def test_zero_price_ago_returns_no_signal(self):
        """Price at index 0 is zero -> guard against division by zero."""
        closes = [0.0] + [100.0] * LOOKBACK
        volumes = [1000] * (LOOKBACK + 1)
        result = evaluate_signal(closes, volumes)
        assert result["signal"] is False
        assert result["reason"] == "zero_price_ago"

    def test_rvol_at_exact_threshold_no_signal(self):
        """RVOL exactly at threshold (4.0) -> no signal (must be strictly >)."""
        base_vol = 1000
        spike_vol = int(base_vol * RVOL_THRESHOLD)  # exactly 4x
        closes = [100.0] * (LOOKBACK + 1)
        volumes = [base_vol] * LOOKBACK + [spike_vol]
        result = evaluate_signal(closes, volumes)
        assert result["signal"] is False
        assert result["reason"] == "rvol_too_low"

    def test_rvol_just_above_threshold_signal(self):
        """RVOL just above threshold -> signal."""
        base_vol = 1000
        spike_vol = int(base_vol * RVOL_THRESHOLD) + 1  # 4001
        closes = [100.0] * (LOOKBACK + 1)
        volumes = [base_vol] * LOOKBACK + [spike_vol]
        result = evaluate_signal(closes, volumes)
        assert result["signal"] is True


class TestLookAheadBias:
    """Verify that signal evaluation uses ONLY prior data — never the current
    bar's volume in the SMA, and never future bars for price comparison."""

    def test_signal_only_on_correct_row(self):
        """Build a 30-bar series.  Insert a valid spike at bar 16 (the bar
        *after* 15 bars of history).  Verify that scanning a sliding window
        fires the signal ONLY when the window's last element is bar 16."""
        n = 30
        base_price = 100.0
        base_vol = 1000
        spike_bar_index = 16

        closes = [base_price] * n
        volumes = [base_vol] * n
        volumes[spike_bar_index] = int(base_vol * (RVOL_THRESHOLD + 2))

        fired_at: list[int] = []
        for end_idx in range(LOOKBACK, n):
            window_closes = closes[end_idx - LOOKBACK: end_idx + 1]
            window_volumes = volumes[end_idx - LOOKBACK: end_idx + 1]
            result = evaluate_signal(window_closes, window_volumes)
            if result["signal"]:
                fired_at.append(end_idx)

        # Signal must fire at exactly bar 16 and nowhere else
        assert fired_at == [spike_bar_index], (
            f"Expected signal only at bar {spike_bar_index}, got {fired_at}"
        )

    def test_spike_in_historical_window_does_not_fire(self):
        """Put a volume spike in bar 10 (within the SMA window when evaluating
        bar 16).  It should inflate the SMA but NOT trigger a signal, because
        the *current* bar (16) has normal volume."""
        n = 20
        closes = [100.0] * n
        volumes = [1000] * n
        volumes[10] = 50000  # spike in history
        # Evaluate at bar 16 — spike is inside the SMA window
        window_closes = closes[1:17]  # bars 1..16 -> 16 elements
        window_volumes = volumes[1:17]
        result = evaluate_signal(window_closes, window_volumes)
        assert result["signal"] is False

    def test_current_bar_volume_not_in_sma(self):
        """The current bar's volume must NOT be included in the SMA calculation."""
        base_vol = 1000
        spike_vol = int(base_vol * (RVOL_THRESHOLD + 1))
        closes = [100.0] * (LOOKBACK + 1)
        volumes = [base_vol] * LOOKBACK + [spike_vol]

        result = evaluate_signal(closes, volumes)
        # SMA should be exactly base_vol (1000), not inflated by the spike
        assert result["vol_sma"] == base_vol
        assert result["signal"] is True


class TestLumibotSignalParity:
    """The Lumibot ``evaluate_signal`` and the custom engine ``evaluate_signal``
    must produce identical results for the same inputs."""

    def test_identical_results(self):
        closes = [100.0] * (LOOKBACK + 1)
        volumes = [1000] * LOOKBACK + [6000]

        r1 = evaluate_signal(closes, volumes)
        r2 = evaluate_signal_lumibot(closes, volumes)

        assert r1["signal"] == r2["signal"]
        assert r1["rvol"] == r2["rvol"]
        assert r1["pct_change"] == r2["pct_change"]
        assert r1["vol_sma"] == r2["vol_sma"]
        assert r1["reason"] == r2["reason"]


# =====================================================================
# Integration test: Run the custom engine strategy on minute bars
# =====================================================================

class TestCustomEngineIntegration:
    """Smoke-test the GeopoliticalWhisper strategy on the custom backtest engine."""

    def test_backtest_runs_without_error(self, minute_reader_and_store):
        reader, store = minute_reader_and_store

        start = datetime.fromisoformat("2024-01-02")
        end = datetime.fromisoformat("2024-01-04")

        strat = GeopoliticalWhisper(
            rvol_threshold=4.0,
            lookback=15,
            price_change_band=0.005,
            trailing_stop_pct=0.02,
            time_exit_minutes=30,
            allocation_fraction=0.10,
            slippage_penalty_pct=0.0005,
        )

        result = run_backtest(
            strategy=strat,
            reader=reader,
            bars_store=store,
            start=start,
            end=end,
            base_currency="EUR",
            costs_yaml_path=str(Path("quant/data/cost_profiles.yml")),
            out_dir=Path("runs/tests/geopolitical_whisper"),
            starting_cash=10_000.0,
        )

        print("GW metrics:", json.dumps(result.metrics, indent=2))
        print("GW orders:", len(result.orders))
        print("GW fills:", len(result.fills))

        assert len(result.equity) > 0
        assert isinstance(result.metrics.get("return"), float)

    def test_backtest_generates_trades(self, minute_reader_and_store):
        """The dummy data contains two valid signal events.
        At least one should generate a trade."""
        reader, store = minute_reader_and_store

        start = datetime.fromisoformat("2024-01-02")
        end = datetime.fromisoformat("2024-01-04")

        strat = GeopoliticalWhisper(
            rvol_threshold=4.0,
            lookback=15,
            allocation_fraction=0.10,
            slippage_penalty_pct=0.0005,
        )

        result = run_backtest(
            strategy=strat,
            reader=reader,
            bars_store=store,
            start=start,
            end=end,
            base_currency="EUR",
            costs_yaml_path=str(Path("quant/data/cost_profiles.yml")),
            out_dir=Path("runs/tests/geopolitical_whisper_trades"),
            starting_cash=10_000.0,
        )

        # We expect at least one entry order
        assert result.metrics["num_orders"] >= 1, (
            f"Expected at least 1 order from signal events, got {result.metrics['num_orders']}"
        )


# =====================================================================
# Position sizing tests
# =====================================================================

class TestPositionSizing:
    """Verify whole-share sizing logic."""

    def test_whole_shares_only(self):
        """10% of $10,000 = $1,000.  At $185/share with 0.05% slippage,
        effective price ~$185.0925.  Floor($1000 / $185.0925) = 5 shares."""
        portfolio_value = 10_000.0
        alloc = portfolio_value * 0.10
        price = 185.0
        effective_price = price * (1 + 0.0005)
        shares = int(math.floor(alloc / effective_price))
        assert shares == 5
        assert isinstance(shares, int)

    def test_zero_shares_when_price_too_high(self):
        """If the price is higher than the allocation, 0 shares -> skip."""
        alloc = 100.0  # very small allocation
        price = 500.0
        effective_price = price * (1 + 0.0005)
        shares = int(math.floor(alloc / effective_price))
        assert shares == 0
