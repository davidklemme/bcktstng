from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

from .black_scholes import Greeks, bs_greeks, bs_price


Key = Tuple[
    float,  # spot
    float,  # strike
    float,  # rate
    float,  # time_years
    float,  # vol
    float,  # div_yield
    str,    # right
]


@dataclass
class GreeksCache:
    _cache_greeks: Dict[Key, Greeks]
    _cache_price: Dict[Key, float]

    def __init__(self) -> None:
        self._cache_greeks = {}
        self._cache_price = {}

    def price(self, *, spot: float, strike: float, rate: float, time_years: float, vol: float, div_yield: float, right: str) -> float:
        key: Key = (spot, strike, rate, time_years, vol, div_yield, right)
        if key not in self._cache_price:
            self._cache_price[key] = bs_price(
                spot=spot, strike=strike, rate=rate, time_years=time_years, vol=vol, div_yield=div_yield, right=right  # type: ignore[arg-type]
            )
        return self._cache_price[key]

    def greeks(self, *, spot: float, strike: float, rate: float, time_years: float, vol: float, div_yield: float, right: str) -> Greeks:
        key: Key = (spot, strike, rate, time_years, vol, div_yield, right)
        if key not in self._cache_greeks:
            self._cache_greeks[key] = bs_greeks(
                spot=spot, strike=strike, rate=rate, time_years=time_years, vol=vol, div_yield=div_yield, right=right  # type: ignore[arg-type]
            )
        return self._cache_greeks[key]