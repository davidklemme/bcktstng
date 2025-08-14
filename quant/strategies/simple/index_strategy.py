"""
Index Strategy - Buy and hold market index.

This strategy implements a simple buy-and-hold approach on a market index
to serve as a baseline for comparing systematic trading strategies against
passive market performance.
"""

import requests
import pandas as pd
import os
import pickle
from io import StringIO
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Optional, Dict

from ...sdk.strategy import MultiSymbolStrategy, Context
from ...data.symbols_repository import SymbolRow


@dataclass
class IndexStrategy(MultiSymbolStrategy):
    """
    Index strategy that implements buy-and-hold on market indices.
    
    This strategy simulates buying and holding a market index (S&P 500)
    by extrapolating index performance against the starting budget.
    """
    
    position_size: int = 100
    index_symbol: str = "^spx"  # S&P 500 index symbol
    index_data: pd.DataFrame = None
    initial_index_value: float = None
    starting_cash: float = 100000
    has_initialized: bool = False
    _cache_file: str = "data/sp500_cache.pkl"
    
    def on_start(self, ctx: Context) -> None:
        super().on_start(ctx)
        self.starting_cash = ctx.portfolio.cash_by_ccy.get('EUR', 100000)
        ctx.log.info("IndexStrategy starting (index_symbol=%s, starting_cash=%.2f)", 
                    self.index_symbol, self.starting_cash)
        
        # Fetch index data
        self._fetch_index_data(ctx.now)

    def on_symbol_event(self, symbol: SymbolRow, evt: Any, ctx: Context) -> None:
        # Make one initial trade to establish a position
        # The equity will then be calculated based on portfolio value
        if not self.has_initialized:
            symbol_ticker = symbol.ticker
            # Buy a position that will track the S&P 500 performance
            ctx.order(symbol_ticker, self.position_size, side="BUY", type="MKT", tag=f"sp500_initial_{symbol_ticker}")
            self.has_initialized = True
            print(f"IndexStrategy: Initialized with {symbol_ticker} to track S&P 500 performance")
            return
    
    def _fetch_index_data(self, start_date: datetime) -> None:
        """Fetch index data from Stooq with caching."""
        # Try to load from cache first
        if self._load_from_cache():
            print(f"IndexStrategy: Loaded {self.index_symbol} data from cache")
            return
        
        try:
            # Format dates for Stooq API
            start_str = start_date.strftime("%Y%m%d")
            end_str = "20241231"  # End of 2024
            
            url = f"https://stooq.com/q/d/l/?s={self.index_symbol}&d1={start_str}&d2={end_str}&i=d"
            
            response = requests.get(url)
            if response.status_code == 200 and response.text.strip() != "No data":
                # Parse CSV data
                df = pd.read_csv(StringIO(response.text))
                df['Date'] = pd.to_datetime(df['Date'])
                df = df.set_index('Date')
                self.index_data = df
                
                # Get initial index value
                if not df.empty:
                    self.initial_index_value = df['Close'].iloc[0]
                    print(f"IndexStrategy: Fetched {self.index_symbol} data from {df.index[0]} to {df.index[-1]}")
                    print(f"Initial index value: {self.initial_index_value}")
                    
                    # Save to cache
                    self._save_to_cache()
                else:
                    print(f"IndexStrategy: No data available for {self.index_symbol}")
            else:
                print(f"IndexStrategy: Failed to fetch data for {self.index_symbol}")
                
        except Exception as e:
            print(f"IndexStrategy: Error fetching index data: {e}")
    
    def _load_from_cache(self) -> bool:
        """Load index data from cache file."""
        try:
            if os.path.exists(self._cache_file):
                with open(self._cache_file, 'rb') as f:
                    cached_data = pickle.load(f)
                    self.index_data = cached_data['index_data']
                    self.initial_index_value = cached_data['initial_index_value']
                    return True
        except Exception as e:
            print(f"IndexStrategy: Error loading from cache: {e}")
        return False
    
    def _save_to_cache(self) -> None:
        """Save index data to cache file."""
        try:
            # Ensure data directory exists
            os.makedirs(os.path.dirname(self._cache_file), exist_ok=True)
            
            cached_data = {
                'index_data': self.index_data,
                'initial_index_value': self.initial_index_value
            }
            
            with open(self._cache_file, 'wb') as f:
                pickle.dump(cached_data, f)
                
            print(f"IndexStrategy: Saved {self.index_symbol} data to cache")
        except Exception as e:
            print(f"IndexStrategy: Error saving to cache: {e}")
    
    def _get_index_value_at_date(self, date: datetime) -> Optional[float]:
        """Get index value at a specific date."""
        if self.index_data is None:
            return None
        
        # Find the closest date in the index data
        date_str = date.strftime("%Y-%m-%d")
        if date_str in self.index_data.index:
            return self.index_data.loc[date_str, 'Close']
        
        # If exact date not found, find the closest previous date
        available_dates = self.index_data.index[self.index_data.index <= date_str]
        if len(available_dates) > 0:
            closest_date = available_dates[-1]
            return self.index_data.loc[closest_date, 'Close']
        
        return None

    def on_end(self, ctx: Context) -> None:
        # Calculate final performance
        if self.index_data is not None and not self.index_data.empty:
            final_index_value = self.index_data['Close'].iloc[-1]
            total_return = ((final_index_value - self.initial_index_value) / self.initial_index_value) * 100
            final_equity = self.starting_cash * (final_index_value / self.initial_index_value)
            ctx.log.info(f"IndexStrategy finished - Index: {self.initial_index_value:.2f} -> {final_index_value:.2f} ({total_return:+.2f}%)")
            ctx.log.info(f"IndexStrategy finished - Equity: {self.starting_cash:.2f} -> {final_equity:.2f} ({total_return:+.2f}%)")
        else:
            ctx.log.info("IndexStrategy finished - No index data available")
        super().on_end(ctx)
    
    def __str__(self) -> str:
        """String representation of the strategy."""
        return f"IndexStrategy(index_symbol={self.index_symbol}, position_size={self.position_size})"
