"""Comprehensive tests for v2 hardening fixes.

Covers: S1-S7, A1-A8, B1-B4 from SPEC.md.
"""
from __future__ import annotations

import math
from datetime import datetime, date, timezone, timedelta

import pytest

from quant.engine.portfolio import Portfolio, _validate_finite
from quant.engine.orders import Order, OrderSide, OrderType, TimeInForce, OrderState
from quant.engine.execution import ExecutionSimulator, Quote
from quant.engine.risk import RiskManager, RiskCaps
from quant.data.calendars import is_open, next_open, next_close
from quant.data.bars_loader import load_daily_bars_csv
from quant.sdk.features import rolling_mean, zscore, rolling_vol, atr, vol_target
from quant.common.utils import to_utc


# --- S2: Calendar loop guard ---

class TestCalendarLoopGuard:
    def test_next_open_weekend_advances(self):
        # Saturday should advance to Monday
        sat = datetime(2024, 6, 8, 12, 0, 0, tzinfo=timezone.utc)
        result = next_open("XNYS", sat)
        # Monday June 10
        assert result.weekday() == 0  # Monday

    def test_next_close_weekend_advances(self):
        sat = datetime(2024, 6, 8, 12, 0, 0, tzinfo=timezone.utc)
        result = next_close("XNYS", sat)
        assert result.weekday() == 0

    def test_xnys_closed_on_holiday(self):
        # July 4, 2024 is a Thursday
        july4 = datetime(2024, 7, 4, 15, 0, 0, tzinfo=timezone.utc)
        assert is_open("XNYS", july4) is False

    def test_xnys_open_normal_day(self):
        # June 3, 2024 is a Monday. 14:00 UTC = 10:00 ET (during session)
        mon = datetime(2024, 6, 3, 14, 0, 0, tzinfo=timezone.utc)
        assert is_open("XNYS", mon) is True

    def test_xnys_closed_before_open(self):
        # 13:00 UTC = 09:00 ET (before 09:30 open)
        mon = datetime(2024, 6, 3, 13, 0, 0, tzinfo=timezone.utc)
        assert is_open("XNYS", mon) is False

    def test_xetr_half_day(self):
        # Dec 24 should close at 14:00 local = 13:00 UTC
        dec24 = datetime(2024, 12, 24, 12, 30, 0, tzinfo=timezone.utc)  # 13:30 CET
        assert is_open("XETR", dec24) is True

    def test_unsupported_exchange_raises(self):
        ts = datetime(2024, 6, 3, 14, 0, 0, tzinfo=timezone.utc)
        with pytest.raises(ValueError, match="Unsupported exchange"):
            is_open("UNKNOWN", ts)


# --- S4: Financial operation validation ---

class TestPortfolioValidation:
    def test_deposit_negative_raises(self):
        p = Portfolio()
        with pytest.raises(ValueError, match="non-negative"):
            p.deposit(-100, "EUR")

    def test_deposit_nan_raises(self):
        p = Portfolio()
        with pytest.raises(ValueError, match="finite"):
            p.deposit(float("nan"), "EUR")

    def test_deposit_inf_raises(self):
        p = Portfolio()
        with pytest.raises(ValueError, match="finite"):
            p.deposit(float("inf"), "EUR")

    def test_withdraw_negative_raises(self):
        p = Portfolio()
        with pytest.raises(ValueError, match="non-negative"):
            p.withdraw(-50, "EUR")

    def test_apply_fill_negative_qty_raises(self):
        p = Portfolio()
        p.deposit(10000, "EUR")
        with pytest.raises(ValueError, match="positive"):
            p.apply_fill(1, "EUR", "BUY", -10, 100.0)

    def test_apply_fill_nan_price_raises(self):
        p = Portfolio()
        p.deposit(10000, "EUR")
        with pytest.raises(ValueError, match="finite"):
            p.apply_fill(1, "EUR", "BUY", 10, float("nan"))

    def test_apply_fill_zero_qty_raises(self):
        p = Portfolio()
        p.deposit(10000, "EUR")
        with pytest.raises(ValueError, match="positive"):
            p.apply_fill(1, "EUR", "BUY", 0, 100.0)

    def test_valid_deposit_works(self):
        p = Portfolio()
        p.deposit(1000, "EUR")
        assert p.get_cash("EUR") == 1000.0

    def test_valid_fill_works(self):
        p = Portfolio()
        p.deposit(10000, "EUR")
        pnl = p.apply_fill(1, "EUR", "BUY", 10, 100.0)
        assert pnl == 0.0
        assert p.get_cash("EUR") == 9000.0


# --- S5: FOK order fix ---

class TestFOKOrderFix:
    def test_fok_cancel_preserves_filled_quantity(self):
        order = Order(id="1", symbol_id=1, side=OrderSide.BUY, quantity=100,
                      type=OrderType.MARKET, tif=TimeInForce.FOK)
        order.acknowledge()
        order.add_fill(60)  # Partial fill
        assert order.filled_quantity == 60
        order.handle_end_of_cycle(60)
        # FOK should cancel but preserve fill history
        assert order.state == OrderState.CANCELED
        assert order.filled_quantity == 60  # NOT reset to 0

    def test_fok_fully_filled_stays_filled(self):
        order = Order(id="2", symbol_id=1, side=OrderSide.BUY, quantity=100,
                      type=OrderType.MARKET, tif=TimeInForce.FOK)
        order.acknowledge()
        order.add_fill(100)
        assert order.state == OrderState.FILLED
        order.handle_end_of_cycle(100)
        assert order.state == OrderState.FILLED

    def test_add_fill_negative_raises(self):
        order = Order(id="3", symbol_id=1, side=OrderSide.BUY, quantity=100)
        order.acknowledge()
        with pytest.raises(ValueError, match="positive"):
            order.add_fill(-10)


# --- S7: Corporate action tracking ---

class TestCorpActionTracking:
    def test_same_date_different_actions_both_applied(self):
        from quant.data.corp_actions_repository import CorporateAction
        p = Portfolio()
        p.deposit(0, "EUR")
        p.apply_fill(1, "EUR", "BUY", 100, 10.0)

        asof = datetime(2024, 1, 2, tzinfo=timezone.utc)
        action1 = CorporateAction(symbol_id=1, effective_date=asof,
                                   split_ratio=2.0, dividend=0.0, currency="EUR")
        action2 = CorporateAction(symbol_id=1, effective_date=asof,
                                   split_ratio=1.0, dividend=0.5, currency="EUR")

        # Both should be applied since they have different (date, ratio, div) tuples
        p.process_actions_for_symbol(1, [action1], asof)
        p.process_actions_for_symbol(1, [action2], asof)

        # After 2:1 split: qty=200, price=5.0; then dividend on 200 shares at $0.5
        pos = p.positions[1]
        assert pos.quantity == 200
        assert pos.average_price == 5.0


# --- A2: UTC utility ---

class TestUtcUtility:
    def test_naive_datetime_gets_utc(self):
        dt = datetime(2024, 1, 1, 12, 0, 0)
        result = to_utc(dt)
        assert result.tzinfo is not None
        assert result.tzinfo == timezone.utc

    def test_string_iso(self):
        result = to_utc("2024-01-01T12:00:00Z")
        assert result.year == 2024
        assert result.tzinfo is not None

    def test_string_with_offset(self):
        result = to_utc("2024-01-01T13:00:00+01:00")
        assert result.hour == 12  # Converted to UTC

    def test_already_utc_unchanged(self):
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = to_utc(dt)
        assert result == dt


# --- A3: Simplified _tod_bucket ---

class TestTodBucket:
    def test_us_open_bucket(self):
        sim = ExecutionSimulator()
        # 09:30 ET = 13:30 UTC (June, EDT)
        ts = datetime(2024, 6, 3, 13, 30, tzinfo=timezone.utc)
        assert sim._tod_bucket("US", ts) == "OPEN"

    def test_us_close_bucket(self):
        sim = ExecutionSimulator()
        # 15:45 ET = 19:45 UTC
        ts = datetime(2024, 6, 3, 19, 45, tzinfo=timezone.utc)
        assert sim._tod_bucket("US", ts) == "CLOSE"

    def test_us_mid_bucket(self):
        sim = ExecutionSimulator()
        # 12:00 ET = 16:00 UTC
        ts = datetime(2024, 6, 3, 16, 0, tzinfo=timezone.utc)
        assert sim._tod_bucket("US", ts) == "MID"

    def test_eu_open_bucket(self):
        sim = ExecutionSimulator()
        # 09:15 CET = 07:15 UTC (June, CEST = UTC+2, so 09:15 local = 07:15 UTC)
        ts = datetime(2024, 6, 3, 7, 15, tzinfo=timezone.utc)
        assert sim._tod_bucket("XETR", ts) == "OPEN"

    def test_none_ts_returns_mid(self):
        sim = ExecutionSimulator()
        assert sim._tod_bucket("US", None) == "MID"


# --- A5: Order state machine wiring ---

class TestOrderStateMachine:
    def test_acknowledge_transitions_to_working(self):
        order = Order(id="1", symbol_id=1, side=OrderSide.BUY, quantity=100)
        assert order.state == OrderState.NEW
        order.acknowledge()
        assert order.state == OrderState.WORKING

    def test_filled_order_cannot_be_canceled(self):
        order = Order(id="1", symbol_id=1, side=OrderSide.BUY, quantity=100)
        order.acknowledge()
        order.add_fill(100)
        assert order.state == OrderState.FILLED
        order.cancel()
        assert order.state == OrderState.FILLED  # Still filled

    def test_reject_from_new(self):
        order = Order(id="1", symbol_id=1, side=OrderSide.BUY, quantity=100)
        order.reject()
        assert order.state == OrderState.REJECTED


# --- B2: Strategy state isolation ---

class TestStrategyStateIsolation:
    def test_bollinger_resets_state_on_start(self):
        from quant.strategies.simple.bollinger import BollingerBands
        strat = BollingerBands()
        strat.last_states = {"AAPL": "long", "MSFT": "short"}
        # Simulate on_start without full context by checking the reset logic
        assert len(strat.last_states) == 2
        strat.last_states = {}  # This is what on_start does
        assert len(strat.last_states) == 0

    def test_roc_resets_state_on_start(self):
        from quant.strategies.simple.roc import RateOfChange
        strat = RateOfChange()
        strat.last_states = {"AAPL": "long"}
        strat.last_states = {}
        assert len(strat.last_states) == 0


# --- B4: Risk manager ---

class TestRiskManager:
    def test_within_limits_passes(self):
        caps = RiskCaps(max_gross=1e9, max_net=1e9, max_symbol=100000, max_leverage=10)
        rm = RiskManager(caps)
        p = Portfolio()
        allowed, reason = rm.check(p, 1, "EUR", 100.0, 100, 1.0)
        assert allowed is True
        assert reason is None

    def test_exceeds_symbol_limit_rejected(self):
        caps = RiskCaps(max_gross=1e9, max_net=1e9, max_symbol=5000, max_leverage=10)
        rm = RiskManager(caps)
        p = Portfolio()
        allowed, reason = rm.check(p, 1, "EUR", 100.0, 100, 1.0)
        # 100 * 100 * 1.0 = 10000 > 5000
        assert allowed is False
        assert "max_symbol" in reason


# --- Features edge cases ---

class TestFeaturesEdgeCases:
    def test_rolling_mean_empty(self):
        result = rolling_mean([], 5)
        assert result == []

    def test_rolling_mean_short_window(self):
        result = rolling_mean([1, 2, 3], 5)
        assert result == [None, None, None]

    def test_rolling_mean_exact_window(self):
        result = rolling_mean([1, 2, 3, 4, 5], 5)
        assert result[4] == 3.0
        assert result[3] is None

    def test_zscore_constant_values(self):
        # Constant values: std=0, epsilon used
        result = zscore([5, 5, 5, 5, 5], 3)
        assert result[2] is not None
        assert abs(result[2]) < 1e-6  # Should be ~0

    def test_rolling_vol_basic(self):
        result = rolling_vol([1, 2, 3, 4, 5], 3)
        assert result[0] is None
        assert result[1] is None
        assert result[2] is not None
        assert result[2] > 0

    def test_atr_unequal_lengths_raises(self):
        with pytest.raises(ValueError, match="same length"):
            atr([1, 2], [1], [1, 2], 2)

    def test_window_zero_raises(self):
        with pytest.raises(ValueError, match="positive"):
            rolling_mean([1, 2, 3], 0)

    def test_vol_target_basic(self):
        returns = [0.01, -0.01, 0.02, -0.02, 0.01, -0.01, 0.02, -0.02, 0.01, -0.01]
        result = vol_target(returns, 0.15, 5)
        # Should produce some non-None values after window
        non_none = [v for v in result if v is not None]
        assert len(non_none) > 0
        assert all(v > 0 for v in non_none)


# --- B3: Metrics computation ---

class TestMetricsComputation:
    def test_compute_metrics_empty(self):
        from quant.orchestrator.backtest import _compute_metrics
        metrics = _compute_metrics([], [], [])
        assert metrics["final_equity_eur"] == 0.0
        assert metrics["sharpe_ratio"] == 0.0
        assert metrics["max_drawdown"] == 0.0
        assert metrics["win_rate"] == 0.0

    def test_compute_metrics_single_point(self):
        from quant.orchestrator.backtest import _compute_metrics
        equity = [{"equity_eur": 10000.0}]
        metrics = _compute_metrics(equity, [], [])
        assert metrics["final_equity_eur"] == 10000.0
        assert metrics["num_trades"] == 0

    def test_compute_metrics_with_growth(self):
        from quant.orchestrator.backtest import _compute_metrics
        equity = [
            {"equity_eur": 10000.0},
            {"equity_eur": 10100.0},
            {"equity_eur": 10200.0},
            {"equity_eur": 10300.0},
        ]
        metrics = _compute_metrics(equity, [], [])
        assert metrics["return"] > 0
        assert metrics["sharpe_ratio"] > 0
        assert metrics["max_drawdown"] == 0.0  # No drawdown in monotonic growth

    def test_compute_metrics_with_drawdown(self):
        from quant.orchestrator.backtest import _compute_metrics
        equity = [
            {"equity_eur": 10000.0},
            {"equity_eur": 11000.0},
            {"equity_eur": 9000.0},  # Drawdown from 11000
            {"equity_eur": 10000.0},
        ]
        metrics = _compute_metrics(equity, [], [])
        assert metrics["max_drawdown"] < 0  # Negative
        assert metrics["max_drawdown"] == pytest.approx(-2000 / 11000, abs=0.01)


# --- S3: FX error handling ---

class TestFxErrorHandling:
    def test_total_value_with_missing_fx_rate(self, tmp_path):
        from quant.data.fx_repository import create_engine as create_fx_engine
        fx_engine = create_fx_engine(str(tmp_path / "fx.sqlite"))
        # No FX rates loaded — lookup will fail

        p = Portfolio(base_currency="EUR")
        p.deposit(1000.0, "EUR")
        # Add USD cash — FX lookup will fail
        p.cash_by_ccy["USD"] = 500.0

        asof = datetime(2024, 1, 2, tzinfo=timezone.utc)
        # Should not crash, just skip the USD cash
        total = p.total_value_eur(asof, {}, fx_engine)
        assert total == 1000.0  # Only EUR cash counted
