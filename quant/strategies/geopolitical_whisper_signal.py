"""Geopolitical Whisper — pure signal logic.

Framework-agnostic.  Deliberately has **zero** imports from Lumibot or from
the in-house engine so that it can be unit-tested in isolation and so that
both runtime adapters share exactly one implementation.

Signal
------
Entry when BOTH hold on the just-closed 1-minute bar:

* ``RVOL = current_volume / SMA(prior `lookback` volumes) > rvol_threshold``
* ``pct_change`` of close over the last ``lookback`` bars lies strictly
  inside ``(-price_change_band, +price_change_band)``

No-look-ahead contract
----------------------
``closes``/``volumes`` must be ordered oldest-first and contain exactly
``lookback + 1`` elements.  Index ``lookback`` (the last element) is the
*current, already-closed* bar.  Indices ``0 .. lookback-1`` are strictly
prior bars.  The current bar's volume is never included in the SMA it is
measured against.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
RVOL_THRESHOLD: float = 4.0
LOOKBACK: int = 15
PRICE_CHANGE_BAND: float = 0.005      # +/- 0.5 %
TRAILING_STOP_PCT: float = 0.02       # 2 %
TIME_EXIT_MINUTES: int = 30
ALLOCATION_FRACTION: float = 0.10     # 10 %
SLIPPAGE_PENALTY_PCT: float = 0.0005  # 0.05 %


def evaluate_signal(
    closes: Sequence[float],
    volumes: Sequence[float],
    lookback: int = LOOKBACK,
    rvol_threshold: float = RVOL_THRESHOLD,
    price_change_band: float = PRICE_CHANGE_BAND,
) -> Dict[str, Any]:
    """Evaluate the entry signal on a single ``lookback + 1`` window.

    Returns a dict with keys ``signal`` (bool), ``rvol``, ``pct_change``,
    ``vol_sma`` (float | None) and ``reason`` (str).  Never raises on
    degenerate input — it reports a ``reason`` instead, so a live strategy
    cannot be taken down by a single bad bar.
    """
    required = lookback + 1
    result: Dict[str, Any] = {
        "signal": False,
        "rvol": None,
        "pct_change": None,
        "vol_sma": None,
        "reason": "",
    }
    if lookback < 1:
        result["reason"] = "invalid_lookback"
        return result
    if len(closes) < required or len(volumes) < required:
        result["reason"] = "insufficient_data"
        return result

    # Use the LAST `required` elements so an over-long window is handled
    # correctly rather than silently comparing against stale bars.
    closes = list(closes)[-required:]
    volumes = list(volumes)[-required:]

    hist_volumes = volumes[:lookback]
    current_volume = volumes[lookback]

    # Reject non-finite / negative inputs rather than propagating NaN.
    if not _all_finite(hist_volumes) or not _finite(current_volume):
        result["reason"] = "non_finite_volume"
        return result
    if any(v < 0 for v in hist_volumes) or current_volume < 0:
        result["reason"] = "negative_volume"
        return result

    vol_sma = sum(hist_volumes) / float(lookback)
    result["vol_sma"] = vol_sma
    if vol_sma <= 0:
        result["reason"] = "zero_vol_sma"
        return result

    rvol = current_volume / vol_sma
    result["rvol"] = rvol

    price_now = closes[lookback]
    price_ago = closes[0]
    if not _finite(price_now) or not _finite(price_ago):
        result["reason"] = "non_finite_price"
        return result
    if price_ago <= 0:
        result["reason"] = "zero_price_ago"
        return result

    pct_change = (price_now - price_ago) / price_ago
    result["pct_change"] = pct_change

    if rvol <= rvol_threshold:
        result["reason"] = "rvol_too_low"
        return result
    if not (-price_change_band < pct_change < price_change_band):
        result["reason"] = "price_moved_too_much"
        return result

    result["signal"] = True
    result["reason"] = "signal_triggered"
    return result


def size_position(
    portfolio_value: float,
    price: float,
    allocation_fraction: float = ALLOCATION_FRACTION,
    slippage_penalty_pct: float = SLIPPAGE_PENALTY_PCT,
) -> int:
    """Whole-share position size for *price*, conservative on slippage.

    The slippage penalty inflates the assumed fill price so the resulting
    share count can never overspend the intended allocation.  Returns 0 when
    the allocation cannot fund a single share (IBKR requires whole shares
    for most equities).
    """
    if portfolio_value <= 0 or price <= 0 or allocation_fraction <= 0:
        return 0
    if not _finite(portfolio_value) or not _finite(price):
        return 0
    effective_price = price * (1.0 + slippage_penalty_pct)
    if effective_price <= 0:
        return 0
    return int(portfolio_value * allocation_fraction // effective_price)


def should_exit(
    *,
    minutes_held: float,
    current_price: float,
    intrabar_low: Optional[float],
    highest_since_entry: float,
    trailing_stop_pct: float = TRAILING_STOP_PCT,
    time_exit_minutes: int = TIME_EXIT_MINUTES,
) -> Dict[str, Any]:
    """Decide whether an open position should be closed.

    ``intrabar_low`` is the low of the current bar.  A real resting trailing
    stop fires the moment price *touches* the stop level, so the low — not
    the close — is the correct trigger.  Passing ``None`` falls back to
    ``current_price`` (close-only), which is optimistic and should only be
    used when intrabar data is genuinely unavailable.

    Returns ``{"exit": bool, "reason": str, "stop_price": float}``.  The
    trailing stop is evaluated *before* the time exit so that a stop breach
    is not mislabelled as a time exit on the 30-minute bar.
    """
    stop_price = highest_since_entry * (1.0 - trailing_stop_pct)
    trigger = intrabar_low if intrabar_low is not None else current_price

    if _finite(trigger) and trigger <= stop_price:
        return {"exit": True, "reason": "TRAILING_STOP", "stop_price": stop_price}
    if minutes_held >= time_exit_minutes:
        return {"exit": True, "reason": "TIME_EXIT", "stop_price": stop_price}
    return {"exit": False, "reason": "", "stop_price": stop_price}


def _finite(x: Any) -> bool:
    try:
        f = float(x)
    except (TypeError, ValueError):
        return False
    return f == f and f not in (float("inf"), float("-inf"))


def _all_finite(xs: Sequence[Any]) -> bool:
    return all(_finite(x) for x in xs)
