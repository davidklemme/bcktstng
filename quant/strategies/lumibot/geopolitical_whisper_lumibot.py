"""Geopolitical Whisper — Lumibot Strategy Implementation.

Detects institutional accumulation: a massive volume spike while price
stays flat on 1-minute bars.  Designed for backtesting with
``PandasDataBacktesting`` using local CSV data (Polygon / Massive) and
for live deployment on Interactive Brokers.

Entry
-----
* RVOL (current 1-min volume / SMA-15 of prior 15 bars' volume) > 4.0
* Price change over the last 15 minutes is strictly between -0.5 % and +0.5 %
* Market buy, allocating 10 % of portfolio (whole shares).

Exit
----
* 2 % trailing stop **OR**
* 30-minute time-based exit (whichever comes first).

Costs
-----
* IBKR tiered commissions are configured via Lumibot's backtest parameters.
* An additional 0.05 % slippage penalty is deducted from the allocation at
  sizing time as a conservative fill assumption.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from lumibot.strategies import Strategy

logger = logging.getLogger("geopolitical_whisper")

# ---------------------------------------------------------------------------
# Tunable constants
# ---------------------------------------------------------------------------
RVOL_THRESHOLD: float = 4.0
LOOKBACK: int = 15
PRICE_CHANGE_BAND: float = 0.005      # ± 0.5 %
TRAILING_STOP_PCT: float = 0.02       # 2 %
TIME_EXIT_MINUTES: int = 30
ALLOCATION_FRACTION: float = 0.10     # 10 %
SLIPPAGE_PENALTY_PCT: float = 0.0005  # 0.05 %


# ---------------------------------------------------------------------------
# Pure-function signal evaluator (for unit testing without Lumibot runtime)
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
        *current* (just-closed) bar; the preceding ``lookback`` are historical.
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

    # SMA over the PRIOR lookback bars (indices 0 .. lookback-1).
    # Current bar is index ``lookback``.  This guarantees no look-ahead:
    # we never include the current bar's volume in its own baseline.
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


# ---------------------------------------------------------------------------
# Lumibot Strategy
# ---------------------------------------------------------------------------
class GeopoliticalWhisperLumibot(Strategy):
    """Lumibot implementation of the Geopolitical Whisper strategy."""

    # Lumibot calls ``initialize`` once before the first iteration.
    def initialize(self, parameters: dict = None) -> None:
        params = parameters or {}
        self.rvol_threshold: float = params.get("rvol_threshold", RVOL_THRESHOLD)
        self.lookback: int = params.get("lookback", LOOKBACK)
        self.price_change_band: float = params.get("price_change_band", PRICE_CHANGE_BAND)
        self.trailing_stop_pct: float = params.get("trailing_stop_pct", TRAILING_STOP_PCT)
        self.time_exit_minutes: int = params.get("time_exit_minutes", TIME_EXIT_MINUTES)
        self.allocation_fraction: float = params.get("allocation_fraction", ALLOCATION_FRACTION)
        self.slippage_penalty_pct: float = params.get("slippage_penalty_pct", SLIPPAGE_PENALTY_PCT)

        # Internal bookkeeping — tracks open positions for trailing stop / time exit.
        # Key = asset symbol (str), Value = dict with entry metadata.
        self._open_trades: Dict[str, Dict[str, Any]] = {}

        self.sleeptime = "1M"  # 1-minute iteration cadence

        self.set_market("NYSE")

        logger.info(
            "GeopoliticalWhisperLumibot initialized — rvol>=%.1f, lookback=%d, "
            "band=±%.2f%%, trail=%.1f%%, time_exit=%dm",
            self.rvol_threshold,
            self.lookback,
            self.price_change_band * 100,
            self.trailing_stop_pct * 100,
            self.time_exit_minutes,
        )

    # ------------------------------------------------------------------
    # Main iteration (called every minute by Lumibot)
    # ------------------------------------------------------------------
    def on_trading_iteration(self) -> None:
        now: datetime = self.get_datetime()
        symbol: str = self.parameters.get("symbol", "SPY") if hasattr(self, 'parameters') and self.parameters else "SPY"

        # ------- EXIT LOGIC (always checked first) -------
        if symbol in self._open_trades:
            self._check_exit(symbol, now)
            if symbol not in self._open_trades:
                return  # position was closed

        # ------- Skip entry if already holding -------
        if symbol in self._open_trades:
            return

        # ------- Fetch historical bars (PIT-safe via Lumibot) -------
        # get_historical_prices returns bars *up to but not including*
        # the current incomplete bar.  We ask for lookback+1 completed bars.
        required_bars = self.lookback + 1
        bars = self.get_historical_prices(
            symbol,
            length=required_bars,
            timestep="minute",
        )
        if bars is None or bars.df is None or len(bars.df) < required_bars:
            return

        df = bars.df  # DataFrame with columns: open, high, low, close, volume

        closes = df["close"].tolist()
        volumes = [int(v) for v in df["volume"].tolist()]

        # ------- Evaluate signal (pure function — testable) -------
        sig = evaluate_signal(
            closes,
            volumes,
            lookback=self.lookback,
            rvol_threshold=self.rvol_threshold,
            price_change_band=self.price_change_band,
        )

        # ------- Verbose logging -------
        current_close = closes[-1] if closes else 0.0
        current_vol = volumes[-1] if volumes else 0
        logger.info(
            "[%s] %s | close=%.4f  vol=%d  vol_sma=%s  RVOL=%s  "
            "pct_chg=%s%%  signal=%s  reason=%s",
            now.isoformat(),
            symbol,
            current_close,
            current_vol,
            f"{sig['vol_sma']:.1f}" if sig["vol_sma"] is not None else "N/A",
            f"{sig['rvol']:.2f}" if sig["rvol"] is not None else "N/A",
            f"{sig['pct_change'] * 100:.4f}" if sig["pct_change"] is not None else "N/A",
            sig["signal"],
            sig["reason"],
        )

        if not sig["signal"]:
            return

        # ------- Position sizing (whole shares, conservative) -------
        portfolio_value = self.get_portfolio_value()
        if portfolio_value is None or portfolio_value <= 0:
            return
        alloc = portfolio_value * self.allocation_fraction
        effective_price = current_close * (1.0 + self.slippage_penalty_pct)
        if effective_price <= 0:
            return
        shares = int(math.floor(alloc / effective_price))
        if shares <= 0:
            logger.warning(
                "[%s] %s | Signal triggered but allocation too small for 1 share "
                "(alloc=%.2f, eff_price=%.4f)",
                now.isoformat(),
                symbol,
                alloc,
                effective_price,
            )
            return

        # ------- Place market buy -------
        slippage_cost = shares * current_close * self.slippage_penalty_pct
        logger.info(
            ">>> ENTRY [%s] %s | BUY %d shares @ ~%.4f  "
            "alloc=%.2f  slippage_cost=%.4f  RVOL=%.2f",
            now.isoformat(),
            symbol,
            shares,
            current_close,
            alloc,
            slippage_cost,
            sig["rvol"],
        )

        order = self.create_order(symbol, shares, "buy")
        self.submit_order(order)

        self._open_trades[symbol] = {
            "entry_time": now,
            "entry_price": current_close,
            "quantity": shares,
            "highest_since_entry": current_close,
        }

    # ------------------------------------------------------------------
    # Exit logic
    # ------------------------------------------------------------------
    def _check_exit(self, symbol: str, now: datetime) -> None:
        trade = self._open_trades[symbol]

        last_price = self.get_last_price(symbol)
        if last_price is None or last_price <= 0:
            return

        # Update trailing high
        trade["highest_since_entry"] = max(trade["highest_since_entry"], last_price)

        # ---- Time-based exit ----
        elapsed = now - trade["entry_time"]
        time_exit = elapsed >= timedelta(minutes=self.time_exit_minutes)

        # ---- Trailing stop ----
        trail_price = trade["highest_since_entry"] * (1.0 - self.trailing_stop_pct)
        trailing_stop_hit = last_price <= trail_price

        if not (time_exit or trailing_stop_hit):
            return

        reason = "TIME_EXIT" if time_exit else "TRAILING_STOP"
        slippage_cost = trade["quantity"] * last_price * self.slippage_penalty_pct

        logger.info(
            "<<< EXIT  [%s] %s | SELL %d shares @ ~%.4f  reason=%s  "
            "entry=%.4f  high=%.4f  trail=%.4f  elapsed=%s  slippage_cost=%.4f",
            now.isoformat(),
            symbol,
            trade["quantity"],
            last_price,
            reason,
            trade["entry_price"],
            trade["highest_since_entry"],
            trail_price,
            str(elapsed),
            slippage_cost,
        )

        order = self.create_order(symbol, trade["quantity"], "sell")
        self.submit_order(order)
        del self._open_trades[symbol]

    # ------------------------------------------------------------------
    # End-of-day cleanup
    # ------------------------------------------------------------------
    def before_market_closes(self) -> None:
        """Close any open positions before market close to avoid overnight risk
        and to handle the case where 30 minutes haven't elapsed yet."""
        for symbol in list(self._open_trades.keys()):
            trade = self._open_trades[symbol]
            last_price = self.get_last_price(symbol) or trade["entry_price"]
            logger.info(
                "<<< EOD FORCE EXIT [%s] %s | SELL %d shares @ ~%.4f",
                self.get_datetime().isoformat(),
                symbol,
                trade["quantity"],
                last_price,
            )
            order = self.create_order(symbol, trade["quantity"], "sell")
            self.submit_order(order)
            del self._open_trades[symbol]
