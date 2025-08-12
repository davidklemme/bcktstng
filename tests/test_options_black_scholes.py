import math

import pytest

from quant.options.black_scholes import bs_price, bs_greeks, implied_volatility


@pytest.mark.parametrize(
    "spot,strike,rate,time_years,vol,div_yield,right,price_ref,delta_ref",
    [
        (100.0, 100.0, 0.01, 0.5, 0.2, 0.0, "C", 5.876, 0.5349),
        (100.0, 100.0, 0.01, 0.5, 0.2, 0.0, "P", 5.377, -0.4651),
        (100.0, 110.0, 0.02, 1.0, 0.25, 0.01, "C", 6.468, 0.4103),
    ],
)
def test_bs_price_and_delta_matches_reference(spot, strike, rate, time_years, vol, div_yield, right, price_ref, delta_ref):
    price = bs_price(spot=spot, strike=strike, rate=rate, time_years=time_years, vol=vol, div_yield=div_yield, right=right)
    greeks = bs_greeks(spot=spot, strike=strike, rate=rate, time_years=time_years, vol=vol, div_yield=div_yield, right=right)
    assert math.isfinite(price)
    assert math.isfinite(greeks.delta)
    # loose tolerance to accommodate reference rounding
    assert abs(price - price_ref) < 0.05
    assert abs(greeks.delta - delta_ref) < 0.01


def test_implied_vol_recovers_vol_from_price():
    spot = 100.0
    strike = 95.0
    rate = 0.01
    time_years = 0.75
    vol_true = 0.3
    right = "C"
    price = bs_price(spot=spot, strike=strike, rate=rate, time_years=time_years, vol=vol_true, right=right, div_yield=0.0)
    iv = implied_volatility(target_price=price, spot=spot, strike=strike, rate=rate, time_years=time_years, right=right)
    assert abs(iv - vol_true) < 1e-4