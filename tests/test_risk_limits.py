from quant.engine.portfolio import Portfolio
from quant.engine.risk import RiskManager, RiskCaps


def test_risk_blocks_symbol_exposure():
    p = Portfolio(base_currency="EUR")
    p.deposit(100000.0, "EUR")

    # No existing position; proposed order would exceed symbol cap
    caps = RiskCaps(max_gross=1e9, max_net=1e9, max_symbol=1000.0, max_leverage=10.0)
    rm = RiskManager(caps)

    ok, reason = rm.check(portfolio=p, symbol_id=1, symbol_currency="EUR", price=200.0, qty=10.0, fx_rate_to_eur=1.0)
    assert not ok
    assert "max_symbol" in (reason or "")