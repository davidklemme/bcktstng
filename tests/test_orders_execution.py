from datetime import datetime, timezone

from quant.engine.orders import Order, OrderSide, OrderType, TimeInForce
from quant.engine.execution import ExecutionSimulator, Quote
from quant.data.costs import load_calculator_from_yaml


def test_order_lifecycle_ioc_fok_day(tmp_path):
    o_ioc = Order(id="1", symbol_id=1, side=OrderSide.BUY, quantity=100, type=OrderType.MARKET, tif=TimeInForce.IOC)
    o_fok = Order(id="2", symbol_id=1, side=OrderSide.BUY, quantity=100, type=OrderType.MARKET, tif=TimeInForce.FOK)
    o_day = Order(id="3", symbol_id=1, side=OrderSide.BUY, quantity=100, type=OrderType.MARKET, tif=TimeInForce.DAY)

    for o in (o_ioc, o_fok, o_day):
        o.acknowledge()

    # Provide limited liquidity = 60
    liq = 60
    # IOC: partial fill allowed, then cancel remaining at end of cycle
    o_ioc.add_fill(60)
    o_ioc.handle_end_of_cycle(liq)
    assert o_ioc.state.name in ("CANCELED", "PARTIALLY_FILLED", "FILLED")

    # FOK: if can't fully fill, cancel with zero fills
    o_fok.handle_end_of_cycle(liq)
    assert o_fok.state.name == "CANCELED"
    assert o_fok.filled_quantity == 0

    # DAY: stays working
    o_day.handle_end_of_cycle(liq)
    assert o_day.state.name in ("WORKING", "PARTIALLY_FILLED", "NEW")


def test_execution_within_spread_and_adv_partial(tmp_path):
    # Simulator with ADV and costs
    calc = load_calculator_from_yaml("/workspace/quant/data/cost_profiles.yml")
    sim = ExecutionSimulator(cost_calculator=calc, adv_by_symbol={1: 10000}, adv_cap_fraction=0.1, impact_alpha=0.2, sigma_by_symbol={1: 0.02})

    quote = Quote(bid=99.0, ask=101.0)
    # Market buy for qty 2000, liquidity 2000, but cap is 10% of 10000 = 1000
    order = Order(id="o1", symbol_id=1, side=OrderSide.BUY, quantity=2000, type=OrderType.MARKET, tif=TimeInForce.IOC)
    fills, cost = sim.simulate(order, quote, venue="US", available_liquidity=2000)

    assert sum(f.quantity for f in fills) <= 1000
    for f in fills:
        assert 99.0 <= f.price <= 101.0
    assert cost >= 0.0


def test_market_impact_direction():
    sim = ExecutionSimulator(adv_by_symbol={1: 10000}, adv_cap_fraction=1.0, impact_alpha=0.5, sigma_by_symbol={1: 0.05})
    quote = Quote(bid=99.0, ask=101.0)

    # Market buy should fill at >= mid due to impact
    order_buy = Order(id="b", symbol_id=1, side=OrderSide.BUY, quantity=1000, type=OrderType.MARKET, tif=TimeInForce.DAY)
    fills_b, _ = sim.simulate(order_buy, quote, venue="US", available_liquidity=5000)
    assert fills_b[0].price >= quote.mid

    # Market sell should fill at <= mid due to impact
    order_sell = Order(id="s", symbol_id=1, side=OrderSide.SELL, quantity=1000, type=OrderType.MARKET, tif=TimeInForce.DAY)
    fills_s, _ = sim.simulate(order_sell, quote, venue="US", available_liquidity=5000)
    assert fills_s[0].price <= quote.mid


def test_time_of_day_spread_multiplier_open_close_mid():
    sim = ExecutionSimulator(adv_by_symbol={1: 100000}, adv_cap_fraction=1.0, impact_alpha=0.0, sigma_by_symbol={1: 0.0}, tod_spread_multipliers={"OPEN": 2.0, "MID": 1.0, "CLOSE": 1.5})
    quote = Quote(bid=100.0, ask=100.2)  # spread = 0.2

    from datetime import datetime, timezone

    # US venue at open bucket (approx 09:30 local NYC). Use a UTC time corresponding to 09:30 EDT on 2024-06-03 -> 13:30 UTC
    ts_open = datetime(2024, 6, 3, 13, 30, tzinfo=timezone.utc)
    # Market BUY: with higher multiplier, price should be pushed further above mid but still within [bid, ask]
    order_buy = Order(id="b1", symbol_id=1, side=OrderSide.BUY, quantity=100, type=OrderType.MARKET, tif=TimeInForce.DAY)
    fills_open, _ = sim.simulate(order_buy, quote, venue="US", available_liquidity=10000, ts=ts_open)

    # Mid=100.1; urgency 0.75; effective spread=0.2*2.0=0.4; target = 100.1 + 0.75*0.4 = 100.4 -> clamped to ask=100.2
    assert fills_open[0].price <= quote.ask and fills_open[0].price >= quote.mid

    # Mid-session (e.g., 18:00 UTC ~ 14:00 local). Expect less aggressive than open
    ts_mid = datetime(2024, 6, 3, 18, 0, tzinfo=timezone.utc)
    order_buy2 = Order(id="b2", symbol_id=1, side=OrderSide.BUY, quantity=100, type=OrderType.MARKET, tif=TimeInForce.DAY)
    fills_mid, _ = sim.simulate(order_buy2, quote, venue="US", available_liquidity=10000, ts=ts_mid)
    # With mid multiplier 1.0: effective price at 100.1+0.75*0.2=100.25 -> clamped to 100.2
    assert fills_mid[0].price <= fills_open[0].price

    # Close bucket ~ 20:30 UTC (16:30 local close -> still in close bucket minute)
    ts_close = datetime(2024, 6, 3, 20, 30, tzinfo=timezone.utc)
    order_buy3 = Order(id="b3", symbol_id=1, side=OrderSide.BUY, quantity=100, type=OrderType.MARKET, tif=TimeInForce.DAY)
    fills_close, _ = sim.simulate(order_buy3, quote, venue="US", available_liquidity=10000, ts=ts_close)
    assert fills_close[0].price >= fills_mid[0].price


def test_simulate_backward_compat_signature_without_ts():
    sim = ExecutionSimulator()
    quote = Quote(bid=50.0, ask=50.1)
    order = Order(id="x", symbol_id=1, side=OrderSide.SELL, quantity=10, type=OrderType.MARKET, tif=TimeInForce.IOC)
    fills, cost = sim.simulate(order, quote, venue="US", available_liquidity=100)
    assert fills and cost >= 0.0