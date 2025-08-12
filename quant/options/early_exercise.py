from __future__ import annotations

from math import isfinite


def early_exercise_probability_call(
    *,
    spot: float,
    strike: float,
    rate: float,
    time_years: float,
    dividend_cash: float,
    option_time_value: float,
) -> float:
    """
    Heuristic probability (0..1) of early exercise for ITM calls before ex-dividend.

    Rationale: Early exercise is rational when dividend exceeds time value plus financing edge.
    We map ratio r = dividend_cash / max(option_time_value, eps) through a squashing function.
    """
    if time_years <= 0 or not all(isfinite(x) for x in [spot, strike, rate, dividend_cash, option_time_value]):
        return 0.0
    if spot <= strike:
        return 0.0  # OTM or ATM: negligible probability under this simple model

    eps = 1e-6
    r = max(0.0, dividend_cash) / max(option_time_value, eps)
    # Convert ratio to probability via bounded function
    # p = r / (1 + r) yields p~0.5 when dividend ~= time value
    p = r / (1.0 + r)
    # Cap extremes for numerical safety
    if p < 0.0:
        p = 0.0
    if p > 1.0:
        p = 1.0
    return p


def should_exercise_early_call(*, probability: float, threshold: float = 0.5) -> bool:
    return probability >= threshold