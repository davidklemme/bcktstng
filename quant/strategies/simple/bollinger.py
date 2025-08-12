from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from ...sdk.strategy import Strategy, Context


@dataclass
class BollingerBands(Strategy):
    symbol: str
    window: int = 20
    num_std: float = 2.0
    position_size: int = 100
    last_state: Optional[str] = None  # 'long', 'short', or 'flat'

    def on_start(self, ctx: Context) -> None:
        ctx.log.info("BollingerBands starting for %s (window=%d, k=%.2f)", self.symbol, self.window, self.num_std)

    def on_event(self, evt: Any, ctx: Context) -> None:
        data = ctx.data.get(self.symbol, ["close"], lookback=self.window + 1, at=ctx.now)
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

        if self.last_state is None:
            self.last_state = state
            return

        if state != self.last_state:
            if state == "long":
                ctx.order(self.symbol, self.position_size, side="BUY", type="MKT", tag="bb_long")
            elif state == "short":
                ctx.order(self.symbol, self.position_size, side="SELL", type="MKT", tag="bb_short")
            else:
                # flatten: if last was long -> sell; if last was short -> buy
                side = "SELL" if self.last_state == "long" else "BUY"
                ctx.order(self.symbol, self.position_size, side=side, type="MKT", tag="bb_flatten")
            self.last_state = state

    def on_end(self, ctx: Context) -> None:
        ctx.log.info("BollingerBands finished for %s", self.symbol)