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