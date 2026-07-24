"""Performance metrics for a backtest equity curve.

Separate from :mod:`quant.ops.metrics`, which is Prometheus process
instrumentation, not strategy performance.

Every function here is pure and dependency-free so it can be unit-tested
without running a backtest.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Trading periods per year, used to annualise volatility and Sharpe.
TRADING_DAYS_PER_YEAR = 252
US_EQUITY_MINUTES_PER_DAY = 390
SECONDS_PER_DAY = 86_400


def infer_periods_per_year(timestamps: Sequence[datetime]) -> Optional[int]:
    """Infer the number of observations per year from bar spacing.

    Uses the *median* gap so that overnight and weekend gaps in an intraday
    series do not distort the estimate.  Returns ``None`` when there are too
    few points to tell, in which case annualised figures are omitted rather
    than guessed.
    """
    if len(timestamps) < 3:
        return None
    gaps = [
        (b - a).total_seconds()
        for a, b in zip(timestamps, timestamps[1:])
        if (b - a).total_seconds() > 0
    ]
    if not gaps:
        return None
    gaps.sort()
    median_gap = gaps[len(gaps) // 2]
    if median_gap <= 0:
        return None
    if median_gap >= SECONDS_PER_DAY * 0.9:
        # Daily or coarser: scale by calendar spacing against a 252-day year.
        days = median_gap / SECONDS_PER_DAY
        return max(1, int(round(TRADING_DAYS_PER_YEAR / days)))
    # Intraday: assume a 6.5-hour US equity session.
    bars_per_day = (US_EQUITY_MINUTES_PER_DAY * 60) / median_gap
    return max(1, int(round(bars_per_day * TRADING_DAYS_PER_YEAR)))


def simple_returns(equity: Sequence[float]) -> List[float]:
    """Period-over-period simple returns, skipping non-positive bases."""
    out: List[float] = []
    for prev, curr in zip(equity, equity[1:]):
        if prev is None or curr is None or prev <= 0:
            continue
        out.append(curr / prev - 1.0)
    return out


def max_drawdown(equity: Sequence[float]) -> Tuple[float, int, int]:
    """Return ``(max_drawdown_fraction, peak_index, trough_index)``.

    The drawdown is negative (e.g. ``-0.25`` for a 25% peak-to-trough fall).
    Non-positive equity is skipped, since a percentage drawdown is undefined
    once capital is wiped out.
    """
    peak = None
    peak_idx = 0
    best_peak_idx = 0
    best_trough_idx = 0
    worst = 0.0
    for i, v in enumerate(equity):
        if v is None or v <= 0:
            continue
        if peak is None or v > peak:
            peak, peak_idx = v, i
        if peak and peak > 0:
            dd = v / peak - 1.0
            if dd < worst:
                worst, best_peak_idx, best_trough_idx = dd, peak_idx, i
    return worst, best_peak_idx, best_trough_idx


def _mean(xs: Sequence[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _stdev(xs: Sequence[float]) -> float:
    """Sample standard deviation (n-1)."""
    if len(xs) < 2:
        return 0.0
    m = _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def compute_performance(
    *,
    equity: Sequence[float],
    timestamps: Sequence[datetime],
    initial_capital: float,
    risk_free_rate: float = 0.0,
    gross_traded_notional: float = 0.0,
    periods_in_market: int = 0,
) -> Dict[str, Any]:
    """Compute a performance summary for an equity curve.

    ``initial_capital`` is the capital actually funded at the start of the
    run — *not* ``equity[0]``, which is sampled after the first bar's fills
    and costs and is therefore already contaminated.

    When ``initial_capital <= 0`` the percentage-based figures are reported
    as ``None`` rather than ``0.0``.  A return on zero capital is undefined,
    and silently reporting ``0.0`` makes a broken run look flat.
    """
    n = len(equity)
    out: Dict[str, Any] = {
        "initial_capital": float(initial_capital),
        "final_equity": float(equity[-1]) if n else None,
        "num_periods": n,
        "total_return": None,
        "cagr": None,
        "ann_volatility": None,
        "sharpe": None,
        "sortino": None,
        "max_drawdown": None,
        "calmar": None,
        "exposure": None,
        "turnover": None,
        "periods_per_year": None,
        "warnings": [],
    }
    warnings: List[str] = out["warnings"]

    if n == 0:
        warnings.append("empty_equity_curve")
        return out

    final_equity = float(equity[-1])

    if initial_capital > 0:
        out["total_return"] = final_equity / initial_capital - 1.0
    else:
        warnings.append(
            "initial_capital<=0: return-based metrics are undefined and reported as null"
        )

    rets = simple_returns(equity)
    ppy = infer_periods_per_year(list(timestamps)) if timestamps else None
    out["periods_per_year"] = ppy

    if len(rets) >= 2:
        sd = _stdev(rets)
        mean_r = _mean(rets)
        if ppy:
            ann_vol = sd * math.sqrt(ppy)
            out["ann_volatility"] = ann_vol
            rf_per_period = risk_free_rate / ppy
            excess = [r - rf_per_period for r in rets]
            ex_sd = _stdev(excess)
            if ex_sd > 0:
                out["sharpe"] = (_mean(excess) / ex_sd) * math.sqrt(ppy)
            else:
                warnings.append("zero_return_variance: sharpe undefined")
            downside = [min(0.0, e) for e in excess]
            dd_sd = math.sqrt(sum(d * d for d in downside) / len(downside)) if downside else 0.0
            if dd_sd > 0:
                out["sortino"] = (_mean(excess) / dd_sd) * math.sqrt(ppy)
        else:
            warnings.append("could_not_infer_bar_frequency: annualised metrics omitted")
        del mean_r
    else:
        warnings.append("fewer_than_3_equity_points: risk metrics omitted")

    mdd, _, _ = max_drawdown(equity)
    out["max_drawdown"] = mdd

    # CAGR from wall-clock elapsed time, which is unambiguous.
    if initial_capital > 0 and final_equity > 0 and len(timestamps) >= 2:
        years = (timestamps[-1] - timestamps[0]).total_seconds() / (365.25 * SECONDS_PER_DAY)
        if years > 0:
            out["cagr"] = (final_equity / initial_capital) ** (1.0 / years) - 1.0
            if mdd < 0 and out["cagr"] is not None:
                out["calmar"] = out["cagr"] / abs(mdd)

    if n > 0:
        out["exposure"] = periods_in_market / n

    avg_equity = _mean([e for e in equity if e is not None])
    if avg_equity > 0:
        out["turnover"] = gross_traded_notional / avg_equity

    return out
