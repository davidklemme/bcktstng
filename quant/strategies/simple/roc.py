from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Dict

from ...sdk.strategy import MultiSymbolStrategy, Context
from ...data.symbols_repository import SymbolRow


@dataclass
class RateOfChange(MultiSymbolStrategy):
    window: int = 10
    upper: float = 0.02  # +2%
    lower: float = -0.02  # -2%
    position_size: int = 100
    last_states: Dict[str, Optional[str]] = field(default_factory=dict)  # symbol ticker -> 'long', 'short', or 'flat'

    def on_start(self, ctx: Context) -> None:
        super().on_start(ctx)
        ctx.log.info("RateOfChange starting (window=%d, upper=%.4f, lower=%.4f)", self.window, self.upper, self.lower)

    def on_symbol_event(self, symbol: SymbolRow, evt: Any, ctx: Context) -> None:
        symbol_ticker = symbol.ticker
        
        # Initialize state for this symbol if not exists
        if symbol_ticker not in self.last_states:
            self.last_states[symbol_ticker] = None

        lookback = self.window + 1
        data = ctx.data.get(symbol_ticker, ["close"], lookback=lookback, at=ctx.now)
        closes = data.get("close", [])
        if len(closes) < lookback:
            return
        current = float(closes[-1])
        past = float(closes[-self.window - 1])
        if past == 0:
            return
        roc = (current / past) - 1.0

        if roc > self.upper:
            state = "long"
        elif roc < self.lower:
            state = "short"
        else:
            state = "flat"

        if self.last_states[symbol_ticker] is None:
            self.last_states[symbol_ticker] = state
            return

        if state != self.last_states[symbol_ticker]:
            if state == "long":
                ctx.order(symbol_ticker, self.position_size, side="BUY", type="MKT", tag=f"roc_long_{symbol_ticker}")
            elif state == "short":
                ctx.order(symbol_ticker, self.position_size, side="SELL", type="MKT", tag=f"roc_short_{symbol_ticker}")
            else:
                side = "SELL" if self.last_states[symbol_ticker] == "long" else "BUY"
                ctx.order(symbol_ticker, self.position_size, side=side, type="MKT", tag=f"roc_flatten_{symbol_ticker}")
            self.last_states[symbol_ticker] = state

    def on_end(self, ctx: Context) -> None:
        ctx.log.info("RateOfChange finished")
        super().on_end(ctx)