"""Geopolitical Whisper - Lumibot adapter (for IBKR deployment).

Signal logic is imported from :mod:`quant.strategies.geopolitical_whisper_signal`
and is shared verbatim with the in-house engine adapter, so the two cannot
drift apart.

.. warning::
   **This adapter has not been executed end-to-end.** Lumibot could not be
   installed in the development environment: its dependency closure includes
   ``ibapi``, ``free-proxy`` and ``schwab`` packages that fail to build, and
   the published wheel imports every broker module eagerly at
   ``import lumibot``. The signal logic below is unit-tested through the
   shared module, but ``on_trading_iteration`` itself, the ``PandasData``
   wiring, and the lifecycle-hook names have **not** been verified against a
   running Lumibot. Treat this file as a reviewed draft, not as tested code,
   and run :mod:`quant.strategies.lumibot.run_backtest` against a real
   install before relying on it.
"""

from __future__ import annotations

import logging
from datetime import datetime, time as dtime, timedelta
from typing import Any, Dict, Optional

from lumibot.strategies import Strategy

from ..geopolitical_whisper_signal import (
    ALLOCATION_FRACTION,
    LOOKBACK,
    PRICE_CHANGE_BAND,
    RVOL_THRESHOLD,
    SLIPPAGE_PENALTY_PCT,
    TIME_EXIT_MINUTES,
    TRAILING_STOP_PCT,
    evaluate_signal,
    should_exit,
    size_position,
)

logger = logging.getLogger("geopolitical_whisper")

# Flatten this many minutes before the closing bell rather than carrying a
# position overnight when the 30-minute timer has not yet expired.
FLATTEN_BEFORE_CLOSE_MINUTES = 5


class GeopoliticalWhisperLumibot(Strategy):
    """Volume-spike-with-flat-price entry, trailing-stop / timed exit."""

    def initialize(self, parameters: dict = None) -> None:
        # Lumibot populates ``self.parameters`` from the ``parameters=`` kwarg
        # given to ``backtest()``/the constructor, but different versions
        # differ on whether they also pass it into ``initialize``. Merge both
        # so configuration cannot silently fall back to defaults.
        params: Dict[str, Any] = {}
        params.update(getattr(self, "parameters", None) or {})
        params.update(parameters or {})
        self.parameters = params

        self.symbol: str = params.get("symbol", "SPY")
        self.rvol_threshold: float = params.get("rvol_threshold", RVOL_THRESHOLD)
        self.lookback: int = params.get("lookback", LOOKBACK)
        self.price_change_band: float = params.get("price_change_band", PRICE_CHANGE_BAND)
        self.trailing_stop_pct: float = params.get("trailing_stop_pct", TRAILING_STOP_PCT)
        self.time_exit_minutes: int = params.get("time_exit_minutes", TIME_EXIT_MINUTES)
        self.allocation_fraction: float = params.get("allocation_fraction", ALLOCATION_FRACTION)
        self.slippage_penalty_pct: float = params.get("slippage_penalty_pct", SLIPPAGE_PENALTY_PCT)
        self.verbose: bool = params.get("verbose", False)

        self._open_trades: Dict[str, Dict[str, Any]] = {}
        self.sleeptime = "1M"
        self.set_market(params.get("market", "NYSE"))

        logger.info(
            "GeopoliticalWhisperLumibot init - symbol=%s rvol>%.2f lookback=%d "
            "band=+/-%.3f%% trail=%.2f%% time_exit=%dm alloc=%.1f%%",
            self.symbol, self.rvol_threshold, self.lookback,
            self.price_change_band * 100, self.trailing_stop_pct * 100,
            self.time_exit_minutes, self.allocation_fraction * 100,
        )

    # ------------------------------------------------------------------
    def on_trading_iteration(self) -> None:
        now: datetime = self.get_datetime()
        symbol = self.symbol

        # Reconcile against the broker before acting: an order may have been
        # rejected or partially filled, and trading on stale internal state
        # would compound the error.
        self._reconcile(symbol)

        if symbol in self._open_trades:
            if self._near_close(now):
                self._flatten(symbol, "EOD_FLATTEN", now)
            else:
                self._check_exit(symbol, now)
            return

        # Do not open a position that the closing bell would immediately
        # force out of at an uncontrolled price.
        if self._near_close(now):
            return

        required = self.lookback + 1
        bars = self.get_historical_prices(symbol, length=required, timestep="minute")
        df = getattr(bars, "df", None) if bars is not None else None
        if df is None or len(df) < required:
            return

        closes = df["close"].tolist()
        volumes = df["volume"].tolist()

        sig = evaluate_signal(
            closes, volumes,
            lookback=self.lookback,
            rvol_threshold=self.rvol_threshold,
            price_change_band=self.price_change_band,
        )

        if self.verbose:
            logger.info(
                "[%s] %s close=%.4f vol=%s RVOL=%s pct_chg=%s signal=%s (%s)",
                now.isoformat(), symbol, closes[-1], volumes[-1],
                _fmt(sig["rvol"], ".2f"), _fmt_pct(sig["pct_change"]),
                sig["signal"], sig["reason"],
            )
        if not sig["signal"]:
            return

        price = float(closes[-1])
        portfolio_value = self.get_portfolio_value()
        if not portfolio_value or portfolio_value <= 0:
            return
        shares = size_position(
            float(portfolio_value), price,
            self.allocation_fraction, self.slippage_penalty_pct,
        )
        if shares <= 0:
            logger.warning(
                "[%s] %s signal fired but allocation funds 0 whole shares at %.4f",
                now.isoformat(), symbol, price,
            )
            return

        logger.info(
            ">>> ENTRY [%s] %s BUY %d @ ~%.4f RVOL=%.2f pct_chg=%s est_slippage=%.4f",
            now.isoformat(), symbol, shares, price, sig["rvol"],
            _fmt_pct(sig["pct_change"]), shares * price * self.slippage_penalty_pct,
        )
        self.submit_order(self.create_order(symbol, shares, "buy"))
        self._open_trades[symbol] = {
            "entry_time": now,
            "entry_price": price,
            "quantity": shares,
            "highest_since_entry": price,
        }

    # ------------------------------------------------------------------
    def _check_exit(self, symbol: str, now: datetime) -> None:
        trade = self._open_trades[symbol]
        last_price = self.get_last_price(symbol)
        if last_price is None or last_price <= 0:
            return
        last_price = float(last_price)
        trade["highest_since_entry"] = max(trade["highest_since_entry"], last_price)

        minutes_held = (now - trade["entry_time"]).total_seconds() / 60.0
        decision = should_exit(
            minutes_held=minutes_held,
            current_price=last_price,
            # Live quotes are point-in-time, so the last trade price is the
            # only observable. Unlike the backtest adapter there is no
            # intrabar low to test the stop against.
            intrabar_low=None,
            highest_since_entry=trade["highest_since_entry"],
            trailing_stop_pct=self.trailing_stop_pct,
            time_exit_minutes=self.time_exit_minutes,
        )
        if decision["exit"]:
            self._flatten(symbol, decision["reason"], now, price=last_price)

    def _flatten(self, symbol: str, reason: str, now: datetime, price: Optional[float] = None) -> None:
        trade = self._open_trades.get(symbol)
        if trade is None:
            return
        px = price if price is not None else (self.get_last_price(symbol) or trade["entry_price"])
        logger.info(
            "<<< EXIT [%s] %s SELL %d @ ~%.4f reason=%s entry=%.4f high=%.4f "
            "held=%.1fm gross_pnl=%.2f",
            now.isoformat(), symbol, trade["quantity"], float(px), reason,
            trade["entry_price"], trade["highest_since_entry"],
            (now - trade["entry_time"]).total_seconds() / 60.0,
            (float(px) - trade["entry_price"]) * trade["quantity"],
        )
        self.submit_order(self.create_order(symbol, trade["quantity"], "sell"))
        self._open_trades.pop(symbol, None)

    def _reconcile(self, symbol: str) -> None:
        """Clear internal state when the broker shows no matching position."""
        if symbol not in self._open_trades:
            return
        try:
            pos = self.get_position(symbol)
        except Exception:  # broker unavailable; keep state and retry next bar
            return
        qty = float(getattr(pos, "quantity", 0) or 0) if pos is not None else 0.0
        if qty <= 0:
            logger.warning(
                "%s tracked as open but broker reports no position; clearing state", symbol
            )
            self._open_trades.pop(symbol, None)

    def _near_close(self, now: datetime) -> bool:
        """True within ``FLATTEN_BEFORE_CLOSE_MINUTES`` of the closing bell.

        Uses the ``session_close`` parameter (local exchange time, default
        16:00) so the 30-minute timer can never silently roll a position
        overnight on a broker that does not fire ``before_market_closes``.
        """
        close_t = self.parameters.get("session_close", dtime(16, 0))
        cutoff = (
            datetime.combine(now.date(), close_t)
            - timedelta(minutes=FLATTEN_BEFORE_CLOSE_MINUTES)
        ).time()
        return now.time() >= cutoff

    # Belt-and-braces: Lumibot calls this before the bell on brokers that
    # support it. The _near_close check above covers brokers that do not.
    def before_market_closes(self) -> None:
        now = self.get_datetime()
        for symbol in list(self._open_trades.keys()):
            self._flatten(symbol, "EOD_FLATTEN", now)


def _fmt(value: Optional[float], spec: str) -> str:
    return format(value, spec) if value is not None else "n/a"


def _fmt_pct(value: Optional[float]) -> str:
    return f"{value * 100:.4f}%" if value is not None else "n/a"
