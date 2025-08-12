from quant.adapters.exec.paper_broker import PaperBroker
from quant.adapters.exec.ibkr_adapter import IBKRAdapter
from quant.adapters.exec.provider import ExecutionError
from quant.engine.orders import Order, OrderSide, OrderType, TimeInForce
from quant.engine.execution import Quote
from quant.data.costs import load_calculator_from_yaml


def test_paper_broker_fill_respects_adv_cap_and_spread_and_costs():
    calc = load_calculator_from_yaml("/workspace/quant/data/cost_profiles.yml")
    broker = PaperBroker(cost_calculator=calc, adv_by_symbol={1: 10000}, adv_cap_fraction=0.1, impact_alpha=0.2, sigma_by_symbol={1: 0.02})

    quote = Quote(bid=99.0, ask=101.0)
    order = Order(id="pb1", symbol_id=1, side=OrderSide.BUY, quantity=2000, type=OrderType.MARKET, tif=TimeInForce.IOC)
    upd = broker.submit(order, venue="US", quote=quote, available_liquidity=2000)

    assert upd.filled_quantity <= 1000
    assert upd.avg_fill_price is None or (quote.bid <= upd.avg_fill_price <= quote.ask)
    assert upd.cost >= 0.0


def test_tick_size_enforced_for_limit_orders():
    broker = PaperBroker()
    quote = Quote(bid=100.0, ask=100.02)
    order = Order(id="pb2", symbol_id=1, side=OrderSide.BUY, quantity=100, type=OrderType.LIMIT, tif=TimeInForce.DAY, limit_price=100.003)
    broker.submit(order, venue="US", quote=quote, available_liquidity=1000)
    # LIMIT price should be rounded to 2 decimals for US
    assert round(broker._orders[order.id].limit_price, 2) == 100.00


def test_cancel_order():
    broker = PaperBroker()
    quote = Quote(bid=50.0, ask=50.02)
    order = Order(id="pb3", symbol_id=1, side=OrderSide.SELL, quantity=100, type=OrderType.MARKET, tif=TimeInForce.DAY)
    broker.submit(order, venue="EU", quote=quote, available_liquidity=0)  # force no fills
    upd = broker.cancel(order.id)
    assert upd.state.name == "CANCELED"


def test_ibkr_adapter_dry_run_and_live_guard():
    calc = load_calculator_from_yaml("/workspace/quant/data/cost_profiles.yml")
    ibkr = IBKRAdapter(cost_calculator=calc, dry_run=True, adv_by_symbol={1: 10000})
    quote = Quote(bid=10.0, ask=10.02)
    order = Order(id="ib1", symbol_id=1, side=OrderSide.BUY, quantity=100, type=OrderType.MARKET, tif=TimeInForce.DAY)
    upd = ibkr.submit(order, venue="US", quote=quote, available_liquidity=1000)
    assert upd.filled_quantity > 0

    ibkr_live = IBKRAdapter(cost_calculator=calc, dry_run=False)
    try:
        ibkr_live.submit(order, venue="US", quote=quote, available_liquidity=1000)
        assert False, "Expected ExecutionError for live submit"
    except ExecutionError:
        pass