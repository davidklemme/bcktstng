"""Phase 3: SDK strategy context, order API, data API tests."""

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from quant.sdk.strategy import (
    Strategy,
    MultiSymbolStrategy,
    Context,
    _DataAPI,
    _OrderAPI,
    _RiskAPI,
    _CalendarAPI,
    _FeaturesAPI,
)
from quant.engine.orders import OrderSide, OrderType, OrderState, TimeInForce
from quant.engine.portfolio import Portfolio
from quant.engine.risk import RiskCaps
from quant.data.pit_reader import PITDataReader, BarsStore
from quant.data.bars_loader import BarRow
from quant.data.fx_repository import create_engine as create_fx_engine, load_fx_csv_to_db
from quant.data.symbols_repository import (
    create_sqlite_engine as create_symbols_engine,
    load_symbols_csv_to_db,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_bar(symbol_id, ts, close=100.0):
    return BarRow(ts=ts, symbol_id=symbol_id, open=99, high=101, low=98, close=close, volume=1000, dt=ts.date())


@pytest.fixture
def reader_and_store(tmp_path):
    """Build a PITDataReader with a simple symbol and bars."""
    fx_engine = create_fx_engine(str(tmp_path / "fx.sqlite"))
    fx_csv = tmp_path / "fx.csv"
    fx_csv.write_text("ts,base_ccy,quote_ccy,rate\n2024-01-01T00:00:00Z,USD,EUR,0.85\n")
    load_fx_csv_to_db(str(fx_csv), fx_engine)

    symbols_engine = create_symbols_engine(str(tmp_path / "symbols.sqlite"))
    sym_csv = tmp_path / "symbols.csv"
    sym_csv.write_text(
        "symbol_id,ticker,exchange,currency,active_from,active_to\n"
        "1,AAPL,XNYS,USD,2020-01-01T00:00:00Z,\n"
        "2,MSFT,XNYS,USD,2020-01-01T00:00:00Z,\n"
    )
    load_symbols_csv_to_db(str(sym_csv), symbols_engine)

    t1 = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
    t2 = datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc)
    t3 = datetime(2024, 1, 4, 21, 0, tzinfo=timezone.utc)
    bars = [
        _make_bar(1, t1, 150.0), _make_bar(1, t2, 155.0), _make_bar(1, t3, 160.0),
        _make_bar(2, t1, 300.0), _make_bar(2, t2, 310.0),
    ]
    store = BarsStore.from_rows(bars)
    reader = PITDataReader(fx_engine, symbols_engine, store)
    return reader, store


# ---------------------------------------------------------------------------
# _OrderAPI
# ---------------------------------------------------------------------------
class TestOrderAPI:
    def test_order_creates_valid_object(self):
        api = _OrderAPI()
        o = api.order(symbol_id=1, side="BUY", qty=100, type="MKT", tif="DAY")
        assert o.side == OrderSide.BUY
        assert o.type == OrderType.MARKET
        assert o.quantity == 100
        assert o.state == OrderState.NEW

    def test_order_limit(self):
        api = _OrderAPI()
        o = api.order(symbol_id=1, side="SELL", qty=50, type="LMT", limit_price=105.0)
        assert o.type == OrderType.LIMIT
        assert o.limit_price == 105.0
        assert o.side == OrderSide.SELL

    def test_order_with_tag(self):
        api = _OrderAPI()
        o = api.order(symbol_id=1, side="BUY", qty=10, tag="my_tag")
        assert o.id == "my_tag"

    def test_cancel(self):
        api = _OrderAPI()
        api.cancel("order_1")
        assert "order_1" in api._canceled

    def test_multiple_orders(self):
        api = _OrderAPI()
        api.order(symbol_id=1, side="BUY", qty=10)
        api.order(symbol_id=2, side="SELL", qty=20)
        assert len(api._orders) == 2

    def test_tif_variants(self):
        api = _OrderAPI()
        o_ioc = api.order(symbol_id=1, side="BUY", qty=10, tif="IOC")
        assert o_ioc.tif == TimeInForce.IOC
        o_fok = api.order(symbol_id=1, side="BUY", qty=10, tif="FOK")
        assert o_fok.tif == TimeInForce.FOK


# ---------------------------------------------------------------------------
# _RiskAPI
# ---------------------------------------------------------------------------
class TestRiskAPI:
    def test_default_caps(self):
        api = _RiskAPI()
        assert api._caps.max_gross == 1e9

    def test_set_caps(self):
        api = _RiskAPI()
        api.set(max_symbol=5000.0)
        assert api._caps.max_symbol == 5000.0
        assert api._caps.max_gross == 1e9  # unchanged

    def test_manager_updates(self):
        api = _RiskAPI()
        old_manager = api.manager
        api.set(max_gross=500.0)
        assert api.manager is not old_manager  # new instance


# ---------------------------------------------------------------------------
# _DataAPI
# ---------------------------------------------------------------------------
class TestDataAPI:
    def test_get_requires_at(self, reader_and_store):
        reader, store = reader_and_store
        api = _DataAPI(reader)
        with pytest.raises(ValueError, match="'at' must be provided"):
            api.get("AAPL", ["close"], lookback=5)

    def test_get_close_series(self, reader_and_store):
        reader, store = reader_and_store
        api = _DataAPI(reader)
        at = datetime(2024, 1, 4, 21, 0, tzinfo=timezone.utc)
        result = api.get("AAPL", ["close"], lookback=3, at=at)
        assert len(result["close"]) == 3
        assert result["close"] == [150.0, 155.0, 160.0]

    def test_get_with_lookback(self, reader_and_store):
        reader, store = reader_and_store
        api = _DataAPI(reader)
        at = datetime(2024, 1, 4, 21, 0, tzinfo=timezone.utc)
        result = api.get("AAPL", ["close"], lookback=1, at=at)
        assert len(result["close"]) == 1
        assert result["close"] == [160.0]

    def test_get_by_symbol_id(self, reader_and_store):
        reader, store = reader_and_store
        api = _DataAPI(reader)
        at = datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc)
        result = api.get(1, ["close"], lookback=2, at=at)
        assert result["close"] == [150.0, 155.0]

    def test_get_unsupported_field(self, reader_and_store):
        reader, store = reader_and_store
        api = _DataAPI(reader)
        at = datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc)
        with pytest.raises(KeyError, match="Unsupported field"):
            api.get("AAPL", ["vwap"], lookback=1, at=at)

    def test_get_unknown_ticker(self, reader_and_store):
        reader, store = reader_and_store
        api = _DataAPI(reader)
        at = datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc)
        with pytest.raises(KeyError, match="Unknown symbol"):
            api.get("GOOG", ["close"], lookback=1, at=at)

    def test_get_ts_field(self, reader_and_store):
        reader, store = reader_and_store
        api = _DataAPI(reader)
        at = datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc)
        result = api.get("AAPL", ["ts"], lookback=2, at=at)
        assert len(result["ts"]) == 2


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------
class TestContext:
    def test_order_by_ticker(self, reader_and_store):
        reader, store = reader_and_store
        portfolio = Portfolio(base_currency="EUR")
        ctx = Context(
            now=datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc),
            data=_DataAPI(reader),
            order_api=_OrderAPI(),
            portfolio=portfolio,
            risk=_RiskAPI(),
        )
        o = ctx.order("AAPL", 10, side="BUY", type="MKT")
        assert o.symbol_id == 1
        assert o.quantity == 10

    def test_order_by_id(self, reader_and_store):
        reader, store = reader_and_store
        portfolio = Portfolio(base_currency="EUR")
        ctx = Context(
            now=datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc),
            data=_DataAPI(reader),
            order_api=_OrderAPI(),
            portfolio=portfolio,
            risk=_RiskAPI(),
        )
        o = ctx.order(2, 5, side="SELL")
        assert o.symbol_id == 2

    def test_order_unknown_ticker_raises(self, reader_and_store):
        reader, store = reader_and_store
        ctx = Context(
            now=datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc),
            data=_DataAPI(reader),
            order_api=_OrderAPI(),
            portfolio=Portfolio(base_currency="EUR"),
            risk=_RiskAPI(),
        )
        with pytest.raises(KeyError, match="Unknown"):
            ctx.order("GOOG", 10)

    def test_cancel(self, reader_and_store):
        reader, store = reader_and_store
        ctx = Context(
            now=datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc),
            data=_DataAPI(reader),
            order_api=_OrderAPI(),
            portfolio=Portfolio(base_currency="EUR"),
            risk=_RiskAPI(),
        )
        ctx.cancel("tag1")
        assert "tag1" in ctx.order_api._canceled

    def test_get_available_symbols(self, reader_and_store):
        reader, store = reader_and_store
        ctx = Context(
            now=datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc),
            data=_DataAPI(reader),
            order_api=_OrderAPI(),
            portfolio=Portfolio(base_currency="EUR"),
            risk=_RiskAPI(),
        )
        syms = ctx.get_available_symbols()
        tickers = {s.ticker for s in syms}
        assert "AAPL" in tickers and "MSFT" in tickers


# ---------------------------------------------------------------------------
# Strategy lifecycle
# ---------------------------------------------------------------------------
class TestStrategyLifecycle:
    def test_base_strategy_noop(self):
        s = Strategy()
        # These should not raise
        s.on_start(MagicMock())
        s.on_event(None, MagicMock())
        s.on_end(MagicMock())

    def test_multi_symbol_strategy(self, reader_and_store):
        reader, store = reader_and_store

        class MyStrategy(MultiSymbolStrategy):
            def __init__(self):
                self.events_per_symbol = []

            def on_symbol_event(self, symbol, evt, ctx):
                self.events_per_symbol.append(symbol.ticker)

        portfolio = Portfolio(base_currency="EUR")
        ctx = Context(
            now=datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc),
            data=_DataAPI(reader),
            order_api=_OrderAPI(),
            portfolio=portfolio,
            risk=_RiskAPI(),
        )
        strat = MyStrategy()
        strat.on_start(ctx)
        assert len(strat.available_symbols) == 2
        strat.on_event(None, ctx)
        assert len(strat.events_per_symbol) == 2
