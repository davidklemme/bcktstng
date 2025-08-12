from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from statistics import mean, pstdev
from typing import List, Optional, Sequence


def _rolling_apply(values: Sequence[float], window: int, fn) -> List[Optional[float]]:
    if window <= 0:
        raise ValueError("window must be positive")
    n = len(values)
    out: List[Optional[float]] = [None] * n
    if n == 0:
        return out
    acc: List[float] = []
    for i, v in enumerate(values):
        acc.append(float(v))
        if len(acc) > window:
            acc.pop(0)
        if len(acc) == window:
            out[i] = float(fn(acc))
    return out


def rolling_mean(values: Sequence[float], window: int) -> List[Optional[float]]:
    return _rolling_apply(values, window, mean)


def zscore(values: Sequence[float], window: int) -> List[Optional[float]]:
    def _z(acc: Sequence[float]) -> float:
        m = mean(acc)
        # population std; add small epsilon to avoid div by zero
        s = pstdev(acc) or 1e-12
        return (acc[-1] - m) / s

    return _rolling_apply(values, window, _z)


def rolling_vol(values: Sequence[float], window: int) -> List[Optional[float]]:
    def _vol(acc: Sequence[float]) -> float:
        s = pstdev(acc) or 0.0
        return s

    return _rolling_apply(values, window, _vol)


def atr(high: Sequence[float], low: Sequence[float], close: Sequence[float], window: int) -> List[Optional[float]]:
    if not (len(high) == len(low) == len(close)):
        raise ValueError("high, low, close must have same length")
    trs: List[float] = []
    out: List[Optional[float]] = [None] * len(close)
    prev_close: Optional[float] = None
    for i in range(len(close)):
        h = float(high[i])
        l = float(low[i])
        c = float(close[i])
        if prev_close is None:
            tr = h - l
        else:
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = c
        if len(trs) > window:
            trs.pop(0)
        if len(trs) == window:
            out[i] = sum(trs) / float(window)
    return out


def vol_target(returns: Sequence[float], target_annual_vol: float, window: int, periods_per_year: int = 252) -> List[Optional[float]]:
    # rolling annualized volatility and target leverage = target_vol / realized_vol
    if window <= 1:
        raise ValueError("window must be >1")
    def _lev(acc: Sequence[float]) -> float:
        s = pstdev(acc) or 1e-12
        ann = s * sqrt(periods_per_year)
        return target_annual_vol / ann
    return _rolling_apply(returns, window, _lev)