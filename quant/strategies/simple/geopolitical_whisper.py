from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ...sdk.strategy import MultiSymbolStrategy, Context
from ...data.symbols_repository import SymbolRow


@dataclass
class _OpenPosition:
    """Tracks an open position for trailing-stop and time-exit logic."""

    symbol_id: int
    ticker: str
    entry_time: datetime
    entry_price: float
    quantity: int
    highest_since_entry: float  # tracks running high for trailing stop


# ---------------------------------------------------------------------------
# Strategy parameters (frozen after construction)
# ---------------------------------------------------------------------------
RVOL_THRESHOLD = 4.0          # current volume / SMA(volume, 15)
LOOKBACK = 15                 # number of bars for SMA and price-change
PRICE_CHANGE_BAND = 0.005     # +/- 0.5 %
TRAILING_STOP_PCT = 0.02      # 2 % trailing stop
TIME_EXIT_MINUTES = 30        # close after 30 minutes
ALLOCATION_FRACTION = 0.10    # 10 % of portfolio per trade
STARTING_CAPITAL = 10_000.0   # reference starting capital
SLIPPAGE_PENALTY_PCT = 0.0005  # 0.05 % per fill (on top of engine spread)


@dataclass
class GeopoliticalWhisper(MultiSymbolStrategy):
    """Detects institutional accumulation: massive volume spike while price
    stays flat on 1-minute bars.  Uses a 2 % trailing stop and a 30-minute
    time-based exit.

    Parameters
    ----------
    rvol_threshold : float
        Minimum Relative Volume (current bar vol / SMA-15 vol).
    lookback : int
        Number of bars for the volume SMA and price-change window.
    price_change_band : float
        Maximum absolute price change over *lookback* bars (fraction, e.g. 0.005 = 0.5 %).
    trailing_stop_pct : float
        Trailing stop distance as a fraction of the running high.
    time_exit_minutes : int
        Forcibly exit this many minutes after entry.
    allocation_fraction : float
        Fraction of portfolio equity to allocate per trade.
    slippage_penalty_pct : float
        Extra slippage penalty applied at entry sizing (conservative).
    """

    rvol_threshold: float = RVOL_THRESHOLD
    lookback: int = LOOKBACK
    price_change_band: float = PRICE_CHANGE_BAND
    trailing_stop_pct: float = TRAILING_STOP_PCT
    time_exit_minutes: int = TIME_EXIT_MINUTES
    allocation_fraction: float = ALLOCATION_FRACTION
    slippage_penalty_pct: float = SLIPPAGE_PENALTY_PCT

    # Internal mutable state — NOT parameters
    _open_positions: Dict[int, _OpenPosition] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def on_start(self, ctx: Context) -> None:
        super().on_start(ctx)
        ctx.log.info(
            "GeopoliticalWhisper starting — rvol>=%.1f, lookback=%d, "
            "band=±%.2f%%, trail=%.1f%%, time_exit=%dm",
            self.rvol_threshold,
            self.lookback,
            self.price_change_band * 100,
            self.trailing_stop_pct * 100,
            self.time_exit_minutes,
        )

    # ------------------------------------------------------------------
    # Per-symbol, per-bar logic
    # ------------------------------------------------------------------
    def on_symbol_event(self, symbol: SymbolRow, evt: Any, ctx: Context) -> None:
        now = ctx.now
        sym_id = symbol.symbol_id
        ticker = symbol.ticker

        # ------- EXIT LOGIC (checked first, every bar) -------
        if sym_id in self._open_positions:
            self._check_exit(sym_id, ticker, ctx)
            # If position was closed in check_exit, skip entry evaluation
            if sym_id not in self._open_positions:
                return

        # ------- ENTRY LOGIC -------
        # Skip if we already hold this symbol
        if sym_id in self._open_positions:
            return

        # Need lookback+1 bars: lookback historical + current
        # The data API returns bars up to and including ctx.now (PIT-safe).
        # The LAST bar in the returned list is the *completed* bar whose
        # timestamp == ctx.now.  We treat it as the "just closed" bar,
        # and use the *previous* lookback bars for the SMA — this avoids
        # using the current bar's volume in the SMA that we compare it against.
        required_bars = self.lookback + 1
        data = ctx.data.get(
            ticker,
            ["close", "volume", "ts"],
            lookback=required_bars,
            at=now,
        )
        closes: List[float] = data["close"]
        volumes: List[int] = data["volume"]
        timestamps: List[datetime] = data["ts"]

        if len(closes) < required_bars:
            return  # not enough history yet

        # ---- Compute RVOL (no look-ahead) ----
        # SMA is computed over the PREVIOUS `lookback` bars (indices 0..lookback-1)
        # Current bar is the LAST element (index lookback)
        hist_volumes = volumes[:-1]  # lookback bars preceding current
        current_volume = volumes[-1]

        vol_sma = sum(hist_volumes) / len(hist_volumes) if hist_volumes else 0.0
        if vol_sma <= 0:
            return  # division-by-zero guard
        rvol = current_volume / vol_sma

        # ---- Price action check (no look-ahead) ----
        # Compare current close to the close `lookback` bars ago
        price_now = closes[-1]
        price_ago = closes[0]  # lookback bars ago
        if price_ago <= 0:
            return
        pct_change = (price_now - price_ago) / price_ago

        # ---- Signal evaluation ----
        signal = (
            rvol > self.rvol_threshold
            and -self.price_change_band < pct_change < self.price_change_band
        )

        # ---- Verbose logging at every evaluation ----
        ctx.log.info(
            "[%s] %s | close=%.4f  vol=%d  vol_sma=%.1f  RVOL=%.2f  "
            "pct_chg=%.4f%%  signal=%s",
            now.isoformat(),
            ticker,
            price_now,
            current_volume,
            vol_sma,
            rvol,
            pct_change * 100,
            signal,
        )

        if not signal:
            return

        # ---- Position sizing (whole shares, conservative) ----
        portfolio_value = self._estimate_portfolio_value(ctx)
        alloc = portfolio_value * self.allocation_fraction
        # Deduct slippage penalty from allocation to be conservative
        effective_price = price_now * (1.0 + self.slippage_penalty_pct)
        if effective_price <= 0:
            return
        raw_shares = alloc / effective_price
        shares = int(math.floor(raw_shares))  # whole shares only
        if shares <= 0:
            ctx.log.warning(
                "[%s] %s | Signal triggered but allocation too small for 1 share "
                "(alloc=%.2f, price=%.4f)",
                now.isoformat(),
                ticker,
                alloc,
                effective_price,
            )
            return

        # ---- Place market buy ----
        slippage_cost = shares * price_now * self.slippage_penalty_pct
        ctx.log.info(
            ">>> ENTRY [%s] %s | BUY %d shares @ ~%.4f  "
            "alloc=%.2f  slippage_cost=%.4f  RVOL=%.2f",
            now.isoformat(),
            ticker,
            shares,
            price_now,
            alloc,
            slippage_cost,
            rvol,
        )
        ctx.order(ticker, shares, side="BUY", type="MKT", tag=f"gw_entry_{ticker}")

        self._open_positions[sym_id] = _OpenPosition(
            symbol_id=sym_id,
            ticker=ticker,
            entry_time=now,
            entry_price=price_now,
            quantity=shares,
            highest_since_entry=price_now,
        )

    # ------------------------------------------------------------------
    # Exit logic
    # ------------------------------------------------------------------
    def _check_exit(self, sym_id: int, ticker: str, ctx: Context) -> None:
        pos = self._open_positions[sym_id]
        now = ctx.now

        # Fetch current price (just need the latest bar)
        data = ctx.data.get(ticker, ["close", "high"], lookback=1, at=now)
        if not data["close"]:
            return
        current_close = data["close"][-1]
        current_high = data["high"][-1]

        # Update trailing high
        pos.highest_since_entry = max(pos.highest_since_entry, current_high)

        # ---- Time-based exit ----
        elapsed = now - pos.entry_time
        time_exit = elapsed >= timedelta(minutes=self.time_exit_minutes)

        # ---- Trailing stop ----
        trail_price = pos.highest_since_entry * (1.0 - self.trailing_stop_pct)
        trailing_stop_hit = current_close <= trail_price

        if not (time_exit or trailing_stop_hit):
            return

        reason = "TIME_EXIT" if time_exit else "TRAILING_STOP"
        slippage_cost = pos.quantity * current_close * self.slippage_penalty_pct

        ctx.log.info(
            "<<< EXIT  [%s] %s | SELL %d shares @ ~%.4f  reason=%s  "
            "entry=%.4f  high=%.4f  trail=%.4f  elapsed=%s  slippage_cost=%.4f",
            now.isoformat(),
            ticker,
            pos.quantity,
            current_close,
            reason,
            pos.entry_price,
            pos.highest_since_entry,
            trail_price,
            str(elapsed),
            slippage_cost,
        )
        ctx.order(ticker, pos.quantity, side="SELL", type="MKT", tag=f"gw_exit_{ticker}")
        del self._open_positions[sym_id]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _estimate_portfolio_value(ctx: Context) -> float:
        """Return total cash in base currency as a sizing proxy.

        For minute-bar backtests with starting_cash, this gives a reasonable
        estimate without needing FX conversion of mark prices every tick.
        Falls back to STARTING_CAPITAL if portfolio has no cash yet.
        """
        # NOTE: This sums cash across all currencies without FX conversion.
        # Acceptable for single-currency backtests.  For multi-currency live
        # trading, replace with proper FX-adjusted portfolio valuation.
        total_cash = sum(ctx.portfolio.cash_by_ccy.values())
        if total_cash <= 0:
            return STARTING_CAPITAL
        return total_cash

    def on_end(self, ctx: Context) -> None:
        # Force-close any remaining positions at end-of-data
        for sym_id in list(self._open_positions.keys()):
            pos = self._open_positions[sym_id]
            data = ctx.data.get(pos.ticker, ["close"], lookback=1, at=ctx.now)
            if data["close"]:
                ctx.log.info(
                    "<<< EOD FORCE EXIT [%s] %s | SELL %d shares @ ~%.4f",
                    ctx.now.isoformat(),
                    pos.ticker,
                    pos.quantity,
                    data["close"][-1],
                )
                ctx.order(
                    pos.ticker,
                    pos.quantity,
                    side="SELL",
                    type="MKT",
                    tag=f"gw_eod_{pos.ticker}",
                )
            del self._open_positions[sym_id]
        ctx.log.info("GeopoliticalWhisper finished")
        super().on_end(ctx)


# ---------------------------------------------------------------------------
# Pure-function signal evaluator (for unit testing without Context)
# ---------------------------------------------------------------------------
def evaluate_signal(
    closes: List[float],
    volumes: List[int],
    lookback: int = LOOKBACK,
    rvol_threshold: float = RVOL_THRESHOLD,
    price_change_band: float = PRICE_CHANGE_BAND,
) -> Dict[str, Any]:
    """Evaluate the geopolitical-whisper entry signal on a single window.

    Parameters
    ----------
    closes : list[float]
        Must have exactly ``lookback + 1`` elements.  The last element is the
        current bar; the first ``lookback`` are historical.
    volumes : list[int]
        Same length requirement as *closes*.

    Returns
    -------
    dict with keys: signal (bool), rvol (float|None), pct_change (float|None),
    vol_sma (float|None), reason (str).
    """
    required = lookback + 1
    result: Dict[str, Any] = {
        "signal": False,
        "rvol": None,
        "pct_change": None,
        "vol_sma": None,
        "reason": "",
    }
    if len(closes) < required or len(volumes) < required:
        result["reason"] = "insufficient_data"
        return result

    hist_volumes = volumes[:lookback]
    current_volume = volumes[lookback]

    vol_sma = sum(hist_volumes) / len(hist_volumes) if hist_volumes else 0.0
    result["vol_sma"] = vol_sma
    if vol_sma <= 0:
        result["reason"] = "zero_vol_sma"
        return result

    rvol = current_volume / vol_sma
    result["rvol"] = rvol

    price_now = closes[lookback]
    price_ago = closes[0]
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
