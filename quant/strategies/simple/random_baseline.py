"""
Random Trading Strategy - Baseline for comparison.

This strategy randomly buys and sells positions to serve as a baseline
for comparing systematic trading strategies against random trading.
"""

import random
from dataclasses import dataclass, field
from typing import Any, Optional, Dict, Set

from ...sdk.strategy import MultiSymbolStrategy, Context
from ...data.symbols_repository import SymbolRow


@dataclass
class RandomBaseline(MultiSymbolStrategy):
    """
    Random trading strategy that serves as a baseline.
    
    Randomly buys and sells positions with no systematic logic,
    useful for comparing against systematic strategies.
    """
    
    position_size: int = 100
    trade_probability: float = 0.1  # 10% chance of trading each day
    max_positions: int = 5
    seed: Optional[int] = None
    current_positions: Set[str] = field(default_factory=set)
    last_states: Dict[str, Optional[str]] = field(default_factory=dict)  # symbol ticker -> 'long', 'short', or 'flat'
    
    def on_start(self, ctx: Context) -> None:
        super().on_start(ctx)
        if self.seed is not None:
            random.seed(self.seed)
        ctx.log.info("RandomBaseline starting (trade_probability=%.2f, max_positions=%d)", self.trade_probability, self.max_positions)

    def on_symbol_event(self, symbol: SymbolRow, evt: Any, ctx: Context) -> None:
        symbol_ticker = symbol.ticker
        
        # Initialize state for this symbol if not exists
        if symbol_ticker not in self.last_states:
            self.last_states[symbol_ticker] = None
        
        # Randomly decide whether to trade today
        if random.random() > self.trade_probability:
            return
        
        # Randomly decide to buy, sell, or close position
        action = random.choice(['BUY', 'SELL', 'CLOSE'])
        
        if action == 'BUY':
            if symbol_ticker not in self.current_positions and len(self.current_positions) < self.max_positions:
                ctx.order(symbol_ticker, self.position_size, side="BUY", type="MKT", tag=f"random_buy_{symbol_ticker}")
                self.current_positions.add(symbol_ticker)
                self.last_states[symbol_ticker] = "long"
        
        elif action == 'SELL':
            if symbol_ticker not in self.current_positions and len(self.current_positions) < self.max_positions:
                ctx.order(symbol_ticker, self.position_size, side="SELL", type="MKT", tag=f"random_sell_{symbol_ticker}")
                self.current_positions.add(symbol_ticker)
                self.last_states[symbol_ticker] = "short"
        
        elif action == 'CLOSE':
            if symbol_ticker in self.current_positions:
                # Close position based on last state
                if self.last_states[symbol_ticker] == "long":
                    ctx.order(symbol_ticker, self.position_size, side="SELL", type="MKT", tag=f"random_close_{symbol_ticker}")
                elif self.last_states[symbol_ticker] == "short":
                    ctx.order(symbol_ticker, self.position_size, side="BUY", type="MKT", tag=f"random_close_{symbol_ticker}")
                self.current_positions.discard(symbol_ticker)
                self.last_states[symbol_ticker] = "flat"

    def on_end(self, ctx: Context) -> None:
        ctx.log.info("RandomBaseline finished")
        super().on_end(ctx)
    
    def __str__(self) -> str:
        """String representation of the strategy."""
        return f"RandomBaseline(position_size={self.position_size}, trade_probability={self.trade_probability})"
