from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional, Dict

from ...sdk.strategy import MultiSymbolStrategy, Context
from ...data.symbols_repository import SymbolRow


@dataclass
class BollingerBands(MultiSymbolStrategy):
    window: int = 20
    num_std: float = 2.0
    position_size: int = 100
    last_states: Dict[str, Optional[str]] = field(default_factory=dict)  # symbol ticker -> 'long', 'short', or 'flat'

    def on_start(self, ctx: Context) -> None:
        super().on_start(ctx)
        ctx.log.info("BollingerBands starting (window=%d, k=%.2f)", self.window, self.num_std)

    def on_symbol_event(self, symbol: SymbolRow, evt: Any, ctx: Context) -> None:
        symbol_ticker = symbol.ticker
        
        # Initialize state for this symbol if not exists
        if symbol_ticker not in self.last_states:
            self.last_states[symbol_ticker] = None

        data = ctx.data.get(symbol_ticker, ["close"], lookback=self.window + 1, at=ctx.now)
        closes = data.get("close", [])
        if len(closes) < self.window:
            return
        mean_series = ctx.features.rolling_mean(closes, self.window)
        vol_series = ctx.features.rolling_vol(closes, self.window)
        mean = mean_series[-1]
        vol = vol_series[-1]
        if mean is None or vol is None:
            return
        upper = mean + self.num_std * vol
        lower = mean - self.num_std * vol
        price = float(closes[-1])

        state: str
        # Mean-reversion: buy when below lower band, sell when above upper band
        if price < lower:
            state = "long"
        elif price > upper:
            state = "short"
        else:
            state = "flat"

        if self.last_states[symbol_ticker] is None:
            self.last_states[symbol_ticker] = state
            return

        if state != self.last_states[symbol_ticker]:
            if state == "long":
                ctx.order(symbol_ticker, self.position_size, side="BUY", type="MKT", tag=f"bb_long_{symbol_ticker}")
            elif state == "short":
                ctx.order(symbol_ticker, self.position_size, side="SELL", type="MKT", tag=f"bb_short_{symbol_ticker}")
            else:
                # flatten: if last was long -> sell; if last was short -> buy
                side = "SELL" if self.last_states[symbol_ticker] == "long" else "BUY"
                ctx.order(symbol_ticker, self.position_size, side=side, type="MKT", tag=f"bb_flatten_{symbol_ticker}")
            self.last_states[symbol_ticker] = state

    def on_end(self, ctx: Context) -> None:
        ctx.log.info("BollingerBands finished")
        super().on_end(ctx)