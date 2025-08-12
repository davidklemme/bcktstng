from __future__ import annotations

from dataclasses import dataclass
from math import erf, exp, log, sqrt
from typing import Literal, Tuple


def _norm_cdf(x: float) -> float:
    # standard normal CDF via error function
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return (1.0 / sqrt(2.0 * 3.141592653589793)) * exp(-0.5 * x * x)


def _d1_d2(spot: float, strike: float, rate: float, time_years: float, vol: float, div_yield: float = 0.0) -> Tuple[float, float]:
    if spot <= 0.0 or strike <= 0.0 or time_years <= 0.0 or vol <= 0.0:
        raise ValueError("spot, strike, time, vol must be positive")
    num = log(spot / strike) + (rate - div_yield + 0.5 * vol * vol) * time_years
    den = vol * sqrt(time_years)
    d1 = num / den
    d2 = d1 - vol * sqrt(time_years)
    return d1, d2


def bs_price(
    *,
    spot: float,
    strike: float,
    rate: float,
    time_years: float,
    vol: float,
    div_yield: float = 0.0,
    right: Literal["C", "P"] = "C",
) -> float:
    d1, d2 = _d1_d2(spot, strike, rate, time_years, vol, div_yield)
    df = exp(-rate * time_years)
    dq = exp(-div_yield * time_years)
    if right == "C":
        return dq * spot * _norm_cdf(d1) - df * strike * _norm_cdf(d2)
    else:
        return df * strike * _norm_cdf(-d2) - dq * spot * _norm_cdf(-d1)


@dataclass(frozen=True)
class Greeks:
    delta: float
    gamma: float
    vega: float
    theta: float
    rho: float


def bs_greeks(
    *,
    spot: float,
    strike: float,
    rate: float,
    time_years: float,
    vol: float,
    div_yield: float = 0.0,
    right: Literal["C", "P"] = "C",
) -> Greeks:
    d1, d2 = _d1_d2(spot, strike, rate, time_years, vol, div_yield)
    df = exp(-rate * time_years)
    dq = exp(-div_yield * time_years)
    pdf_d1 = _norm_pdf(d1)

    if right == "C":
        delta = dq * _norm_cdf(d1)
        theta = (
            - (spot * dq * pdf_d1 * vol) / (2.0 * sqrt(time_years))
            - rate * df * strike * _norm_cdf(d2)
            + div_yield * dq * spot * _norm_cdf(d1)
        )
        rho = time_years * df * strike * _norm_cdf(d2)
    else:
        delta = -dq * _norm_cdf(-d1)
        theta = (
            - (spot * dq * pdf_d1 * vol) / (2.0 * sqrt(time_years))
            + rate * df * strike * _norm_cdf(-d2)
            - div_yield * dq * spot * _norm_cdf(-d1)
        )
        rho = -time_years * df * strike * _norm_cdf(-d2)

    gamma = dq * pdf_d1 / (spot * vol * sqrt(time_years))
    vega = spot * dq * pdf_d1 * sqrt(time_years)

    return Greeks(delta=delta, gamma=gamma, vega=vega, theta=theta, rho=rho)


def implied_volatility(
    *,
    target_price: float,
    spot: float,
    strike: float,
    rate: float,
    time_years: float,
    div_yield: float = 0.0,
    right: Literal["C", "P"] = "C",
    tol: float = 1e-8,
    max_iter: int = 100,
    vol_lower: float = 1e-6,
    vol_upper: float = 5.0,
) -> float:
    # Bisection method for robustness
    low = vol_lower
    high = vol_upper

    def _price(v: float) -> float:
        return bs_price(spot=spot, strike=strike, rate=rate, time_years=time_years, vol=v, div_yield=div_yield, right=right)

    p_low = _price(low)
    p_high = _price(high)
    if (p_low - target_price) * (p_high - target_price) > 0:
        # Expand bounds heuristically
        for _ in range(10):
            high *= 2.0
            p_high = _price(high)
            if (p_low - target_price) * (p_high - target_price) <= 0:
                break
        else:
            raise ValueError("Could not bracket implied volatility")

    for _ in range(max_iter):
        mid = 0.5 * (low + high)
        p_mid = _price(mid)
        if abs(p_mid - target_price) < tol:
            return mid
        if (p_mid - target_price) * (p_low - target_price) < 0:
            high = mid
            p_high = p_mid
        else:
            low = mid
            p_low = p_mid
    return 0.5 * (low + high)