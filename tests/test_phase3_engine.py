"""Phase 3: Engine tests — events queue, risk with positions, execution edge cases."""

from datetime import datetime, timezone

import pytest

from quant.engine.events import Event, EventType, EventQueue
from quant.engine.risk import RiskCaps, RiskManager
from quant.engine.portfolio import Portfolio, Position
from quant.engine.orders import Order, OrderSide, OrderType, OrderState, TimeInForce
from quant.engine.execution import ExecutionSimulator, Quote, Fill


# ---------------------------------------------------------------------------
# EventQueue
# ---------------------------------------------------------------------------
class TestEventQueue:
    def test_priority_ordering_by_type(self):
        """CLOCK < BAR < FX < CORPORATE_ACTION at same timestamp."""
        q = EventQueue()
        ts = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        q.push(ts, EventType.CORPORATE_ACTION)
        q.push(ts, EventType.CLOCK)
        q.push(ts, EventType.BAR)
        q.push(ts, EventType.FX)

        events = [q.pop() for _ in range(4)]
        types = [e.type for e in events]
        assert types == [EventType.CLOCK, EventType.BAR, EventType.FX, EventType.CORPORATE_ACTION]

    def test_same_type_same_ts_ordered_by_seq(self):
        q = EventQueue()
        ts = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        e1 = q.push(ts, EventType.BAR, payload="first")
        e2 = q.push(ts, EventType.BAR, payload="second")

        r1 = q.pop()
        r2 = q.pop()
        assert r1.payload == "first"
        assert r2.payload == "second"

    def test_timestamp_ordering(self):
        q = EventQueue()
        t1 = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc)
        q.push(t2, EventType.BAR)
        q.push(t1, EventType.BAR)

        assert q.pop().ts == t1
        assert q.pop().ts == t2

    def test_pop_empty_raises(self):
        q = EventQueue()
        with pytest.raises(IndexError, match="empty"):
            q.pop()

    def test_peek_empty_returns_none(self):
        q = EventQueue()
        assert q.peek() is None

    def test_peek_returns_next(self):
        q = EventQueue()
        ts = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        q.push(ts, EventType.BAR, payload="peek_me")
        assert q.peek().payload == "peek_me"
        assert len(q) == 1  # peek doesn't remove

    def test_clear(self):
        q = EventQueue()
        ts = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        q.push(ts, EventType.BAR)
        q.push(ts, EventType.FX)
        assert len(q) == 2
        q.clear()
        assert len(q) == 0

    def test_len(self):
        q = EventQueue()
        assert len(q) == 0
        ts = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        q.push(ts, EventType.CLOCK)
        assert len(q) == 1


# ---------------------------------------------------------------------------
# Event frozen dataclass
# ---------------------------------------------------------------------------
class TestEvent:
    def test_sort_index(self):
        ts = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        e = Event(ts=ts, type=EventType.BAR, seq=5)
        assert e.sort_index == (ts, 1, 5)  # BAR=1

    def test_event_ordering(self):
        ts = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        e1 = Event(ts=ts, type=EventType.CLOCK, seq=0)
        e2 = Event(ts=ts, type=EventType.BAR, seq=0)
        assert e1 < e2


# ---------------------------------------------------------------------------
# RiskManager with existing positions
# ---------------------------------------------------------------------------
class TestRiskWithPositions:
    def test_existing_position_plus_order_exceeds_cap(self):
        p = Portfolio(base_currency="EUR")
        p.deposit(100_000.0, "EUR")
        # Existing long: 50 shares at 10 EUR = 500 EUR
        p.apply_fill(symbol_id=1, currency="EUR", side="BUY", qty=50, price=10.0)

        caps = RiskCaps(max_gross=1e9, max_net=1e9, max_symbol=600.0, max_leverage=10.0)
        rm = RiskManager(caps)
        # Adding 20 more at 10 EUR → total exposure = 70*10 = 700 > 600
        ok, reason = rm.check(p, 1, "EUR", 10.0, 20.0, 1.0)
        assert not ok
        assert "max_symbol" in reason

    def test_existing_position_within_cap(self):
        p = Portfolio(base_currency="EUR")
        p.deposit(100_000.0, "EUR")
        p.apply_fill(symbol_id=1, currency="EUR", side="BUY", qty=50, price=10.0)

        caps = RiskCaps(max_gross=1e9, max_net=1e9, max_symbol=1000.0, max_leverage=10.0)
        rm = RiskManager(caps)
        ok, reason = rm.check(p, 1, "EUR", 10.0, 20.0, 1.0)
        assert ok
        assert reason is None

    def test_zero_qty_position_ignored(self):
        p = Portfolio(base_currency="EUR")
        p.deposit(100_000.0, "EUR")
        p.apply_fill(symbol_id=1, currency="EUR", side="BUY", qty=50, price=10.0)
        p.apply_fill(symbol_id=1, currency="EUR", side="SELL", qty=50, price=10.0)
        # Position qty is now 0
        caps = RiskCaps(max_gross=1e9, max_net=1e9, max_symbol=500.0, max_leverage=10.0)
        rm = RiskManager(caps)
        ok, reason = rm.check(p, 1, "EUR", 10.0, 40.0, 1.0)
        assert ok  # 40*10=400 < 500

    def test_fx_rate_scales_exposure(self):
        p = Portfolio(base_currency="EUR")
        caps = RiskCaps(max_gross=1e9, max_net=1e9, max_symbol=100.0, max_leverage=10.0)
        rm = RiskManager(caps)
        # 10 shares at 10 USD with fx_rate 1.5 → 150 EUR > 100
        ok, reason = rm.check(p, 1, "USD", 10.0, 10.0, 1.5)
        assert not ok


# ---------------------------------------------------------------------------
# Order state machine edge cases
# ---------------------------------------------------------------------------
class TestOrderStateMachine:
    def test_cancel_already_filled(self):
        o = Order(id="o1", symbol_id=1, side=OrderSide.BUY, quantity=10)
        o.acknowledge()
        o.add_fill(10)
        assert o.state == OrderState.FILLED
        o.cancel()  # should be no-op
        assert o.state == OrderState.FILLED

    def test_reject_already_canceled(self):
        o = Order(id="o1", symbol_id=1, side=OrderSide.BUY, quantity=10)
        o.acknowledge()
        o.cancel()
        assert o.state == OrderState.CANCELED
        o.reject()  # no-op
        assert o.state == OrderState.CANCELED

    def test_add_fill_to_canceled_noop(self):
        o = Order(id="o1", symbol_id=1, side=OrderSide.BUY, quantity=10)
        o.acknowledge()
        o.cancel()
        o.add_fill(5)  # no-op
        assert o.filled_quantity == 0

    def test_negative_fill_raises(self):
        o = Order(id="o1", symbol_id=1, side=OrderSide.BUY, quantity=10)
        o.acknowledge()
        with pytest.raises(ValueError, match="positive"):
            o.add_fill(-5)

    def test_partial_fill_then_complete(self):
        o = Order(id="o1", symbol_id=1, side=OrderSide.BUY, quantity=10)
        o.acknowledge()
        o.add_fill(3)
        assert o.state == OrderState.PARTIALLY_FILLED
        o.add_fill(7)
        assert o.state == OrderState.FILLED
        assert o.filled_quantity == 10

    def test_overfill_clamps(self):
        o = Order(id="o1", symbol_id=1, side=OrderSide.BUY, quantity=10)
        o.acknowledge()
        o.add_fill(15)
        assert o.filled_quantity == 10
        assert o.state == OrderState.FILLED

    def test_fok_unfilled_canceled(self):
        o = Order(id="o1", symbol_id=1, side=OrderSide.BUY, quantity=100, tif=TimeInForce.FOK)
        o.acknowledge()
        o.handle_end_of_cycle(50)
        assert o.state == OrderState.CANCELED

    def test_ioc_partially_filled_canceled(self):
        o = Order(id="o1", symbol_id=1, side=OrderSide.BUY, quantity=100, tif=TimeInForce.IOC)
        o.acknowledge()
        o.add_fill(30)
        o.handle_end_of_cycle(50)
        assert o.state == OrderState.CANCELED


# ---------------------------------------------------------------------------
# Position edge cases
# ---------------------------------------------------------------------------
class TestPositionEdgeCases:
    def test_position_flip(self):
        """Long 10 → sell 15 → short 5 with realized P&L."""
        pos = Position(symbol_id=1, currency="EUR", quantity=0, average_price=0.0)
        pos.apply_fill(10.0, 100.0)  # buy 10 at 100
        assert pos.quantity == 10.0
        assert pos.average_price == 100.0

        qty, pnl = pos.apply_fill(-15.0, 110.0)  # sell 15 at 110
        assert qty == -5.0
        assert pnl == pytest.approx(100.0)  # (110-100)*10 = 100 on closed portion
        # Implementation uses (avg_price + fill_price) / 2 for flipped residual
        assert pos.average_price == pytest.approx(105.0)

    def test_zero_qty_delta_noop(self):
        pos = Position(symbol_id=1, currency="EUR", quantity=10.0, average_price=50.0)
        qty, pnl = pos.apply_fill(0.0, 100.0)
        assert qty == 10.0
        assert pnl == 0.0


# ---------------------------------------------------------------------------
# ExecutionSimulator edge cases
# ---------------------------------------------------------------------------
class TestExecutionSimEdgeCases:
    def test_limit_order_missing_price_raises(self):
        sim = ExecutionSimulator()
        order = Order(id="o1", symbol_id=1, side=OrderSide.BUY, quantity=10, type=OrderType.LIMIT, limit_price=None)
        quote = Quote(bid=99.0, ask=101.0)
        with pytest.raises(ValueError, match="limit_price"):
            sim.simulate(order, quote, "XNYS", 1000)

    def test_zero_liquidity_no_fill(self):
        sim = ExecutionSimulator()
        order = Order(id="o1", symbol_id=1, side=OrderSide.BUY, quantity=10, type=OrderType.MARKET)
        quote = Quote(bid=99.0, ask=101.0)
        fills, cost = sim.simulate(order, quote, "XNYS", 0)
        assert fills == []
        assert cost == 0.0

    def test_fok_bypasses_adv_cap(self):
        """FOK orders bypass ADV cap — they fill full qty or nothing."""
        sim = ExecutionSimulator(adv_cap_fraction=0.01)
        order = Order(id="o1", symbol_id=1, side=OrderSide.BUY, quantity=200, type=OrderType.MARKET, tif=TimeInForce.FOK)
        quote = Quote(bid=99.0, ask=101.0)
        # FOK sets max_fillable = order.quantity regardless of cap
        fills, cost = sim.simulate(order, quote, "XNYS", 100)
        assert len(fills) == 1
        assert fills[0].quantity == 200
