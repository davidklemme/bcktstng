from datetime import datetime, timezone

import pytest

from quant.engine.portfolio import Portfolio
from quant.data.corp_actions_repository import CorporateAction
from quant.data.fx_repository import create_engine as create_fx_engine, load_fx_csv_to_db


def test_portfolio_apply_fill_and_cash():
    p = Portfolio(base_currency="EUR")
    p.deposit(10000.0, "EUR")

    # Buy 100 shares at 10 EUR
    pnl = p.apply_fill(symbol_id=1, currency="EUR", side="BUY", qty=100, price=10.0)
    assert pnl == 0.0
    assert round(p.get_cash("EUR"), 2) == 9000.0
    # Sell 50 at 12 EUR → realized P&L = (12-10)*50 = 100 (not added to cash directly)
    pnl = p.apply_fill(symbol_id=1, currency="EUR", side="SELL", qty=50, price=12.0)
    assert round(pnl, 2) == 100.0
    # Cash should increase by sale notional 600 only
    assert round(p.get_cash("EUR"), 2) == 9600.0


def test_portfolio_corporate_actions_split_and_dividend():
    p = Portfolio(base_currency="EUR")
    p.deposit(0.0, "EUR")

    # Long 100 shares at 10 EUR
    p.apply_fill(symbol_id=1, currency="EUR", side="BUY", qty=100, price=10.0)

    # Apply 2:1 split and 0.5 dividend
    asof = datetime(2024, 1, 2, tzinfo=timezone.utc)
    actions = [
        CorporateAction(symbol_id=1, effective_date=asof, split_ratio=2.0, dividend=0.5, currency="EUR")
    ]
    div_cash = p.process_actions_for_symbol(1, actions, asof)

    # Quantity doubles, avg price halves
    pos = p.positions[1]
    assert pos.quantity == 200
    assert pos.average_price == 5.0
    # Dividend credited on adjusted shares; cash reflects initial buy (-1000) + dividend (100) = -900
    assert round(div_cash, 2) == 100.0  # 200 * 0.5
    assert round(p.get_cash("EUR"), 2) == -900.0


def test_portfolio_total_value_eur_with_fx(tmp_path):
    # FX: USD/EUR = 0.8 at asof
    fx_engine = create_fx_engine(str(tmp_path / "fx.sqlite"))
    csv_path = tmp_path / "fx.csv"
    csv_path.write_text("ts,base_ccy,quote_ccy,rate\n2024-01-01T00:00:00Z,USD,EUR,0.8\n")
    load_fx_csv_to_db(str(csv_path), fx_engine)

    p = Portfolio(base_currency="EUR")
    p.deposit(1000.0, "EUR")
    p.deposit(100.0, "USD")

    # Position: 10 shares in USD at price 50 USD → 500 USD market value
    p.apply_fill(symbol_id=42, currency="USD", side="BUY", qty=10, price=50.0)

    asof = datetime(2024, 1, 2, tzinfo=timezone.utc)
    total = p.total_value_eur(asof, mark_prices={42: 50.0}, fx_engine=fx_engine)

    # EUR cash 1000 + USD cash (100 - 500) * 0.8 + position 10*50 * 0.8
    # USD cash after buy = -400; position MV = 500; net USD exposure = 100
    expected = 1000.0 + (100.0 - 500.0) * 0.8 + 500.0 * 0.8
    assert round(total, 2) == round(expected, 2)