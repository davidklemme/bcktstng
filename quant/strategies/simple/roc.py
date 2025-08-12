from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from ...sdk.strategy import Strategy, Context


@dataclass
class RateOfChange(Strategy):
    symbol: str
    window: int = 10
    upper: float = 0.02  # +2%
    lower: float = -0.02  # -2%
    position_size: int = 100
    last_state: Optional[str] = None  # 'long', 'short', or 'flat'

    def on_start(self, ctx: Context) -> None:
        ctx.log.info("RateOfChange starting for %s (window=%d, upper=%.4f, lower=%.4f)", self.symbol, self.window, self.upper, self.lower)

    def on_event(self, evt: Any, ctx: Context) -> None:
        lookback = self.window + 1
        data = ctx.data.get(self.symbol, ["close"], lookback=lookback, at=ctx.now)
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

        if self.last_state is None:
            self.last_state = state
            return

        if state != self.last_state:
            if state == "long":
                ctx.order(self.symbol, self.position_size, side="BUY", type="MKT", tag="roc_long")
            elif state == "short":
                ctx.order(self.symbol, self.position_size, side="SELL", type="MKT", tag="roc_short")
            else:
                side = "SELL" if self.last_state == "long" else "BUY"
                ctx.order(self.symbol, self.position_size, side=side, type="MKT", tag="roc_flatten")
            self.last_state = state

    def on_end(self, ctx: Context) -> None:
        ctx.log.info("RateOfChange finished for %s", self.symbol)