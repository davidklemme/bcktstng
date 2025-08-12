from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional

from ..sdk.strategy import Strategy, Context


@dataclass
class VolTargetedCarry(Strategy):
    symbol: str
    window: int = 20
    target_annual_vol: float = 0.1
    periods_per_year: int = 252
    last_weight: Optional[float] = None

    def on_start(self, ctx: Context) -> None:
        ctx.log.info("VolTargetedCarry starting for %s", self.symbol)

    def on_event(self, evt: Any, ctx: Context) -> None:
        data = ctx.data.get(self.symbol, ["close"], lookback=self.window + 2, at=ctx.now)
        closes = data.get("close", [])
        if len(closes) < self.window + 1:
            return
        # simple returns
        returns: List[float] = []
        for i in range(1, len(closes)):
            if closes[i - 1] == 0:
                returns.append(0.0)
            else:
                returns.append((closes[i] - closes[i - 1]) / closes[i - 1])
        lev_series = ctx.features.vol_target(returns, self.target_annual_vol, self.window, self.periods_per_year)
        weight = lev_series[-1]
        if weight is None:
            return
        self.last_weight = float(weight)
        # Example: scale position to 100 * weight (demo only)
        qty = int(max(0, round(100 * self.last_weight)))
        if qty > 0:
            ctx.order(self.symbol, qty, side="BUY", type="MKT", tag="carry_buy")

    def on_end(self, ctx: Context) -> None:
        ctx.log.info("VolTargetedCarry finished for %s", self.symbol)