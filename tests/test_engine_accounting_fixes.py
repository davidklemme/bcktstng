"""Regression tests for accounting, FX and liquidity defects."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from quant.data.fx_repository import (
    create_engine as create_fx_engine,
    ensure_schema,
    get_rate_asof,
)
from quant.engine.execution import ExecutionSimulator, Quote
from quant.engine.orders import Order, OrderSide, OrderType, TimeInForce
from quant.engine.portfolio import Position


class TestPositionFlip:
    def test_flip_through_zero_uses_new_fill_price_as_basis(self):
        """Short 100 @ 100 then buy 300 @ 110 leaves 200 long acquired
        entirely at 110. Averaging the two prices gave 105.0."""
        pos = Position(symbol_id=1, currency="USD")
        pos.apply_fill(-100, 100.0)
        assert pos.quantity == -100
        pos.apply_fill(300, 110.0)
        assert pos.quantity == 200
        assert pos.average_price == pytest.approx(110.0)

    def test_partial_reduction_keeps_original_basis(self):
        """Reducing a position must not re-base it to the exit price."""
        pos = Position(symbol_id=1, currency="USD")
        pos.apply_fill(100, 50.0)
        pos.apply_fill(-30, 80.0)
        assert pos.quantity == 70
        assert pos.average_price == pytest.approx(50.0)

    def test_full_close_resets_basis(self):
        pos = Position(symbol_id=1, currency="USD")
        pos.apply_fill(100, 50.0)
        _, realized = pos.apply_fill(-100, 60.0)
        assert pos.quantity == 0
        assert pos.average_price == 0.0
        assert realized == pytest.approx(1000.0)


class TestFxLookup:
    def _engine(self):
        eng = create_fx_engine(":memory:")
        ensure_schema(eng)
        from sqlalchemy import MetaData, insert
        from quant.data.fx_repository import define_fx_table

        md = MetaData()
        table = define_fx_table(md)
        with eng.begin() as conn:
            conn.execute(insert(table), [{
                "ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "base_ccy": "USD", "quote_ccy": "EUR", "rate": 0.5,
            }])
        return eng

    def test_direct_pair(self):
        r = get_rate_asof(self._engine(), "USD", "EUR", datetime(2024, 6, 1, tzinfo=timezone.utc))
        assert r.rate == pytest.approx(0.5)

    def test_inverse_pair_is_derived(self):
        """Only USD/EUR is stored; EUR/USD previously raised LookupError."""
        r = get_rate_asof(self._engine(), "EUR", "USD", datetime(2024, 6, 1, tzinfo=timezone.utc))
        assert r.rate == pytest.approx(2.0)

    def test_identity_pair(self):
        r = get_rate_asof(self._engine(), "EUR", "EUR", datetime(2024, 6, 1, tzinfo=timezone.utc))
        assert r.rate == 1.0

    def test_case_insensitive(self):
        r = get_rate_asof(self._engine(), "usd", "eur", datetime(2024, 6, 1, tzinfo=timezone.utc))
        assert r.rate == pytest.approx(0.5)

    def test_missing_pair_still_raises(self):
        with pytest.raises(LookupError):
            get_rate_asof(self._engine(), "USD", "JPY", datetime(2024, 6, 1, tzinfo=timezone.utc))


class TestLiquidityCap:
    def _sim(self):
        return ExecutionSimulator(adv_by_symbol={1: 1000}, adv_cap_fraction=0.1)

    def _order(self, tif: TimeInForce, qty: int = 10_000_000) -> Order:
        return Order(id="o", symbol_id=1, side=OrderSide.BUY, quantity=qty,
                     type=OrderType.MARKET, tif=tif)

    @pytest.mark.parametrize("tif", [TimeInForce.DAY, TimeInForce.IOC])
    def test_adv_cap_applies(self, tif):
        fills, _ = self._sim().simulate(
            self._order(tif), Quote(bid=99.9, ask=100.1), "US", available_liquidity=1000
        )
        assert sum(f.quantity for f in fills) <= 100

    def test_fok_no_longer_bypasses_the_cap(self):
        """FOK previously skipped the ADV cap entirely, so flipping the TIF
        granted unlimited size at the touch."""
        fills, _ = self._sim().simulate(
            self._order(TimeInForce.FOK), Quote(bid=99.9, ask=100.1), "US",
            available_liquidity=1000,
        )
        # FOK cannot be filled in full within the cap, so it must not fill.
        assert sum(f.quantity for f in fills) == 0

    def test_fok_fills_when_it_fits_within_cap(self):
        fills, _ = self._sim().simulate(
            self._order(TimeInForce.FOK, qty=50), Quote(bid=99.9, ask=100.1), "US",
            available_liquidity=1000,
        )
        assert sum(f.quantity for f in fills) == 50
