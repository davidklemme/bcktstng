"""Geopolitical Whisper — in-house engine adapter.

Signal logic lives in :mod:`quant.strategies.geopolitical_whisper_signal` and
is shared verbatim with the Lumibot adapter so the two cannot drift.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ...data.symbols_repository import SymbolRow
from ...sdk.strategy import Context, MultiSymbolStrategy
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

__all__ = [
    "GeopoliticalWhisper",
    "evaluate_signal",
    "LOOKBACK",
    "RVOL_THRESHOLD",
    "PRICE_CHANGE_BAND",
]


@dataclass
class _OpenPosition:
    symbol_id: int
    ticker: str
    entry_time: datetime
    entry_price: float
    quantity: int
    highest_since_entry: float


@dataclass
class GeopoliticalWhisper(MultiSymbolStrategy):
    """Volume-spike-with-flat-price entry, trailing-stop / timed exit.

    See :mod:`quant.strategies.geopolitical_whisper_signal` for the signal
    definition and its no-look-ahead contract.
    """

    rvol_threshold: float = RVOL_THRESHOLD
    lookback: int = LOOKBACK
    price_change_band: float = PRICE_CHANGE_BAND
    trailing_stop_pct: float = TRAILING_STOP_PCT
    time_exit_minutes: int = TIME_EXIT_MINUTES
    allocation_fraction: float = ALLOCATION_FRACTION
    slippage_penalty_pct: float = SLIPPAGE_PENALTY_PCT
    verbose: bool = False

    _open_positions: Dict[int, _OpenPosition] = field(default_factory=dict)

    def on_start(self, ctx: Context) -> None:
        super().on_start(ctx)
        ctx.log.info(
            "GeopoliticalWhisper starting - rvol>%.2f, lookback=%d, band=+/-%.3f%%, "
            "trail=%.2f%%, time_exit=%dm, alloc=%.1f%%",
            self.rvol_threshold,
            self.lookback,
            self.price_change_band * 100,
            self.trailing_stop_pct * 100,
            self.time_exit_minutes,
            self.allocation_fraction * 100,
        )

    def on_symbol_event(self, symbol: SymbolRow, evt: Any, ctx: Context) -> None:
        sym_id = symbol.symbol_id
        ticker = symbol.ticker

        # Reconcile before acting: an order can be silently dropped by the
        # engine when no bar exists for the symbol at that timestamp, which
        # would otherwise leave this strategy believing it holds a position
        # the portfolio never opened.
        self._reconcile(sym_id, ctx)

        if sym_id in self._open_positions:
            self._check_exit(sym_id, ticker, ctx)
            return

        required_bars = self.lookback + 1
        data = ctx.data.get(ticker, ["close", "volume"], lookback=required_bars, at=ctx.now)
        closes = data.get("close") or []
        volumes = data.get("volume") or []
        if len(closes) < required_bars:
            return

        sig = evaluate_signal(
            closes,
            volumes,
            lookback=self.lookback,
            rvol_threshold=self.rvol_threshold,
            price_change_band=self.price_change_band,
        )

        if self.verbose:
            ctx.log.info(
                "[%s] %s close=%.4f vol=%s vol_sma=%s RVOL=%s pct_chg=%s signal=%s (%s)",
                ctx.now.isoformat(),
                ticker,
                closes[-1],
                volumes[-1],
                _fmt(sig["vol_sma"], ".1f"),
                _fmt(sig["rvol"], ".2f"),
                _fmt_pct(sig["pct_change"]),
                sig["signal"],
                sig["reason"],
            )

        if not sig["signal"]:
            return

        price = float(closes[-1])
        shares = size_position(
            self._investable_capital(ctx, symbol.currency),
            price,
            self.allocation_fraction,
            self.slippage_penalty_pct,
        )
        if shares <= 0:
            ctx.log.warning(
                "[%s] %s signal fired but allocation funds 0 whole shares at %.4f",
                ctx.now.isoformat(),
                ticker,
                price,
            )
            return

        ctx.log.info(
            ">>> ENTRY [%s] %s BUY %d @ ~%.4f RVOL=%.2f pct_chg=%s est_slippage=%.4f",
            ctx.now.isoformat(),
            ticker,
            shares,
            price,
            sig["rvol"],
            _fmt_pct(sig["pct_change"]),
            shares * price * self.slippage_penalty_pct,
        )
        ctx.order(ticker, shares, side="BUY", type="MKT", tag=f"gw_entry_{ticker}_{ctx.now:%Y%m%d%H%M}")
        self._open_positions[sym_id] = _OpenPosition(
            symbol_id=sym_id,
            ticker=ticker,
            entry_time=ctx.now,
            entry_price=price,
            quantity=shares,
            highest_since_entry=price,
        )

    def _check_exit(self, sym_id: int, ticker: str, ctx: Context) -> None:
        pos = self._open_positions[sym_id]
        data = ctx.data.get(ticker, ["close", "high", "low"], lookback=1, at=ctx.now)
        closes = data.get("close") or []

        minutes_held = (ctx.now - pos.entry_time).total_seconds() / 60.0

        if not closes:
            # No price this bar. The trailing stop cannot be evaluated, but a
            # timed exit still must fire rather than silently extending the
            # hold indefinitely.
            if minutes_held >= self.time_exit_minutes:
                self._close(pos, pos.entry_price, "TIME_EXIT_NO_QUOTE", minutes_held, ctx)
            return

        close = float(closes[-1])
        highs = data.get("high") or []
        lows = data.get("low") or []
        if highs:
            pos.highest_since_entry = max(pos.highest_since_entry, float(highs[-1]))
        low = float(lows[-1]) if lows else None

        decision = should_exit(
            minutes_held=minutes_held,
            current_price=close,
            intrabar_low=low,
            highest_since_entry=pos.highest_since_entry,
            trailing_stop_pct=self.trailing_stop_pct,
            time_exit_minutes=self.time_exit_minutes,
        )
        if not decision["exit"]:
            return

        # A touched stop fills at the stop level, not at the (better) close.
        fill = decision["stop_price"] if decision["reason"] == "TRAILING_STOP" else close
        self._close(pos, fill, decision["reason"], minutes_held, ctx)

    def _close(
        self,
        pos: _OpenPosition,
        price: float,
        reason: str,
        minutes_held: float,
        ctx: Context,
    ) -> None:
        ctx.log.info(
            "<<< EXIT [%s] %s SELL %d @ ~%.4f reason=%s entry=%.4f high=%.4f held=%.1fm "
            "gross_pnl=%.2f est_slippage=%.4f",
            ctx.now.isoformat(),
            pos.ticker,
            pos.quantity,
            price,
            reason,
            pos.entry_price,
            pos.highest_since_entry,
            minutes_held,
            (price - pos.entry_price) * pos.quantity,
            pos.quantity * price * self.slippage_penalty_pct,
        )
        ctx.order(
            pos.ticker,
            pos.quantity,
            side="SELL",
            type="MKT",
            tag=f"gw_exit_{pos.ticker}_{ctx.now:%Y%m%d%H%M}",
        )
        self._open_positions.pop(pos.symbol_id, None)

    def _reconcile(self, sym_id: int, ctx: Context) -> None:
        """Drop internal state when the portfolio shows no matching position.

        Guards against the engine dropping an unfillable order, which would
        otherwise strand this strategy in a permanently "long" state that
        blocks all future entries for the symbol.
        """
        tracked = self._open_positions.get(sym_id)
        if tracked is None:
            return
        # Allow one bar of settle time: the entry order is submitted during the
        # same iteration and is only applied to the portfolio afterwards.
        if ctx.now <= tracked.entry_time:
            return
        actual = ctx.portfolio.positions.get(sym_id)
        if actual is None or actual.quantity <= 0:
            ctx.log.warning(
                "[%s] %s tracked position of %d shares has no portfolio counterpart "
                "(order never filled); clearing internal state",
                ctx.now.isoformat(),
                tracked.ticker,
                tracked.quantity,
            )
            self._open_positions.pop(sym_id, None)

    def _investable_capital(self, ctx: Context, currency: str) -> float:
        """Total cash converted into the instrument's currency.

        Summing raw balances across currencies would overstate buying power,
        so each balance is converted at the point-in-time rate. A balance
        whose rate cannot be resolved is excluded rather than counted at par.
        """
        reader = ctx.data._reader
        total = 0.0
        for ccy, amount in ctx.portfolio.cash_by_ccy.items():
            if not amount:
                continue
            if ccy == currency:
                total += float(amount)
                continue
            try:
                total += float(amount) * reader.get_fx(ccy, currency, ctx.now).rate
            except Exception:
                ctx.log.warning(
                    "No FX rate %s/%s at %s; excluding %.2f %s from buying power",
                    ccy, currency, ctx.now.isoformat(), amount, ccy,
                )
        return total

    def on_end(self, ctx: Context) -> None:
        # NOTE: orders submitted from on_end are NOT executed by
        # quant.orchestrator.backtest.run_backtest (the fill loop has already
        # finished). Positions still open here are reported so the equity
        # curve's final mark is not mistaken for a realised exit.
        for pos in self._open_positions.values():
            ctx.log.warning(
                "Backtest ended with %s still open: %d shares from %s (unrealised, never exited)",
                pos.ticker,
                pos.quantity,
                pos.entry_time.isoformat(),
            )
        ctx.log.info("GeopoliticalWhisper finished (%d position(s) left open)", len(self._open_positions))
        self._open_positions.clear()
        super().on_end(ctx)


def _fmt(value: Optional[float], spec: str) -> str:
    return format(value, spec) if value is not None else "n/a"


def _fmt_pct(value: Optional[float]) -> str:
    return f"{value * 100:.4f}%" if value is not None else "n/a"
