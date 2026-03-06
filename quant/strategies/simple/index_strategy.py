"""
Index Strategy - Buy and hold.

Purchases all available symbols once at the start and holds for the duration.
Serves as a baseline for comparing systematic trading strategies against
passive buy-and-hold performance.

NOTE: This strategy uses the same PIT data reader as all other strategies.
No external data fetching is performed.
"""

from dataclasses import dataclass
from typing import Any

from ...sdk.strategy import MultiSymbolStrategy, Context
from ...data.symbols_repository import SymbolRow


@dataclass
class IndexStrategy(MultiSymbolStrategy):
    """Buy-and-hold strategy that purchases all available symbols once at start."""

    position_size: int = 100
    has_initialized: bool = False

    def on_start(self, ctx: Context) -> None:
        self.has_initialized = False
        super().on_start(ctx)
        ctx.log.info("IndexStrategy starting (position_size=%d)", self.position_size)

    def on_symbol_event(self, symbol: SymbolRow, evt: Any, ctx: Context) -> None:
        if not self.has_initialized:
            ctx.order(symbol.ticker, self.position_size, side="BUY", type="MKT",
                      tag=f"index_initial_{symbol.ticker}")
            self.has_initialized = True

    def on_end(self, ctx: Context) -> None:
        ctx.log.info("IndexStrategy finished")
        super().on_end(ctx)
