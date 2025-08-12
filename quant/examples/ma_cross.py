from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..sdk.strategy import Strategy, Context


@dataclass
class MACross(Strategy):
    symbol: str
    fast: int = 10
    slow: int = 30
    last_state: str | None = None

    def on_start(self, ctx: Context) -> None:
        ctx.log.info("MACross starting for %s", self.symbol)

    def on_event(self, evt: Any, ctx: Context) -> None:
        # Use close prices
        data = ctx.data.get(self.symbol, ["close"], lookback=self.slow + 1, at=ctx.now)
        closes = data.get("close", [])
        if len(closes) < self.slow:
            return
        ma_fast = ctx.features.rolling_mean(closes, self.fast)[-1]
        ma_slow = ctx.features.rolling_mean(closes, self.slow)[-1]
        if ma_fast is None or ma_slow is None:
            return
        # Signal
        state = "above" if ma_fast > ma_slow else "below"
        if self.last_state is None:
            self.last_state = state
            return
        if state != self.last_state:
            # Cross occurred
            if state == "above":
                ctx.order(self.symbol, 100, side="BUY", type="MKT", tag="ma_cross_buy")
            else:
                ctx.order(self.symbol, 100, side="SELL", type="MKT", tag="ma_cross_sell")
            self.last_state = state

    def on_end(self, ctx: Context) -> None:
        ctx.log.info("MACross finished for %s", self.symbol)