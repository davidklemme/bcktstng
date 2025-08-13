#!/usr/bin/env python3
"""
Convert Stooq data format to bars format for backtesting.

This script converts the Stooq CSV format to the bars format expected by the backtesting system.
"""

import pandas as pd
from pathlib import Path
import sys


def convert_stooq_to_bars(stooq_csv_path: str, bars_csv_path: str, symbol_id_start: int = 1):
    """
    Convert Stooq data to bars format.
    
    Args:
        stooq_csv_path: Path to Stooq CSV file
        bars_csv_path: Path to output bars CSV file
        symbol_id_start: Starting symbol ID for the conversion
    """
    
    # Read Stooq data
    print(f"Reading Stooq data from {stooq_csv_path}")
    stooq_data = pd.read_csv(stooq_csv_path)
    
    print(f"Found {len(stooq_data)} data points for {stooq_data['symbol'].nunique()} symbols")
    
    # Create symbol mapping
    unique_symbols = stooq_data['symbol'].unique()
    symbol_to_id = {symbol: i + symbol_id_start for i, symbol in enumerate(unique_symbols)}
    
    print(f"Symbol mapping:")
    for symbol, symbol_id in symbol_to_id.items():
        print(f"  {symbol} -> {symbol_id}")
    
    # Convert to bars format
    bars_data = stooq_data.copy()
    bars_data['symbol_id'] = bars_data['symbol'].map(symbol_to_id)
    bars_data['dt'] = pd.to_datetime(bars_data['date']).dt.date  # Just the date part
    bars_data['open'] = bars_data['open']
    bars_data['high'] = bars_data['high']
    bars_data['low'] = bars_data['low']
    bars_data['close'] = bars_data['close']
    
    # Select and reorder columns for bars format
    bars_data = bars_data[[
        'symbol_id', 'dt', 'open', 'high', 'low', 'close', 'volume'
    ]]
    
    # Sort by dt and symbol_id
    bars_data = bars_data.sort_values(['dt', 'symbol_id'])
    
    # Save to CSV
    print(f"Saving bars data to {bars_csv_path}")
    bars_data.to_csv(bars_csv_path, index=False)
    
    print(f"Conversion complete!")
    print(f"  Input: {len(stooq_data)} data points")
    print(f"  Output: {len(bars_data)} data points")
    print(f"  Symbols: {len(unique_symbols)}")
    print(f"  Date range: {bars_data['dt'].min()} to {bars_data['dt'].max()}")
    
    return bars_data


def main():
    if len(sys.argv) < 3:
        print("Usage: python convert_stooq_to_bars.py <stooq_csv> <bars_csv> [symbol_id_start]")
        print("Example: python convert_stooq_to_bars.py quant/data/stooq_data.csv quant/data/bars_from_stooq.csv")
        sys.exit(1)
    
    stooq_csv = sys.argv[1]
    bars_csv = sys.argv[2]
    symbol_id_start = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    
    if not Path(stooq_csv).exists():
        print(f"Error: Stooq CSV file {stooq_csv} does not exist")
        sys.exit(1)
    
    convert_stooq_to_bars(stooq_csv, bars_csv, symbol_id_start)


if __name__ == "__main__":
    main() 