#!/usr/bin/env python3
"""
Example script demonstrating Stooq data fetching functionality.

This script shows how to:
1. Fetch data for individual symbols
2. Fetch data for multiple symbols with delays
3. Check missing data
4. View data summaries
5. Handle different exchanges and symbol formats
"""

import sys
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from quant.data.stooq_data_fetcher import StooqDataFetcher
from datetime import datetime, timedelta, timezone


def main():
    print("=== Stooq Data Fetching Example ===\n")
    
    # Initialize the fetcher with a 2-second delay between requests
    fetcher = StooqDataFetcher(delay_seconds=2.0)
    
    # Define output path
    output_path = Path("examples/stooq_example_data.csv")
    
    # Set date range (last 3 months)
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=90)
    
    print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"Output file: {output_path}\n")
    
    # Example 1: Fetch data for individual symbols from different exchanges
    print("1. Fetching data for individual symbols...")
    
    symbols_to_fetch = [
        ("AAPL", "XNAS"),  # NASDAQ
        ("JPM", "XNYS"),   # NYSE
        ("HSBA", "XLON"),  # London
        ("7203", "XTOK"),  # Tokyo
    ]
    
    for symbol, exchange in symbols_to_fetch:
        print(f"   Fetching {symbol} ({exchange})...")
        
        # Check if we already have data for this symbol
        existing_dates = fetcher.get_existing_data_dates(output_path, symbol)
        if existing_dates:
            print(f"     Found existing data for {len(existing_dates)} dates")
        
        # Fetch missing data
        data_points = fetcher.fetch_missing_data(symbol, exchange, output_path, start_date, end_date)
        
        if data_points:
            fetcher.save_data_to_csv(data_points, symbol, exchange, output_path)
            print(f"     Saved {len(data_points)} new data points")
        else:
            print(f"     No new data to fetch")
    
    print()
    
    # Example 2: Show data summary
    print("2. Data summary:")
    summary = fetcher.get_data_summary(output_path)
    
    if summary:
        for symbol, info in summary.items():
            first_date = info['first_date'].strftime('%Y-%m-%d') if info['first_date'] else 'N/A'
            last_date = info['last_date'].strftime('%Y-%m-%d') if info['last_date'] else 'N/A'
            print(f"   {symbol:<10} | {info['exchange']:<8} | {info['data_points']:>6} points | {first_date} to {last_date}")
    else:
        print("   No data found")
    
    print()
    
    # Example 3: Check missing data for a larger set
    print("3. Checking missing data for sample symbols...")
    
    # Create a sample symbols list
    sample_symbols = [
        ("AAPL", "XNAS"), ("MSFT", "XNAS"), ("GOOGL", "XNAS"),
        ("JPM", "XNYS"), ("BAC", "XNYS"), ("WMT", "XNYS"),
        ("HSBA", "XLON"), ("GSK", "XLON"),
        ("7203", "XTOK"), ("6758", "XTOK"),
    ]
    
    missing_symbols = []
    existing_symbols = []
    
    for symbol, exchange in sample_symbols:
        existing_dates = fetcher.get_existing_data_dates(output_path, symbol)
        
        # Check if we have data for the period
        has_data = False
        for date in existing_dates:
            if start_date.date() <= date <= end_date.date():
                has_data = True
                break
        
        if has_data:
            existing_symbols.append((symbol, exchange))
        else:
            missing_symbols.append((symbol, exchange))
    
    print(f"   Symbols with data: {len(existing_symbols)}")
    print(f"   Symbols missing data: {len(missing_symbols)}")
    
    if missing_symbols:
        print("   Missing symbols:")
        for symbol, exchange in missing_symbols:
            print(f"     {symbol} ({exchange})")
    
    print()
    
    # Example 4: Fetch data for multiple symbols with intelligent missing data detection
    print("4. Fetching data for multiple symbols (only missing data)...")
    
    # Use the missing symbols from the previous check
    if missing_symbols:
        print(f"   Fetching data for {len(missing_symbols)} missing symbols...")
        
        results = fetcher.fetch_symbols_data(missing_symbols, output_path, start_date, end_date, force_refresh=False)
        
        print("   Results:")
        total_points = 0
        for symbol, count in results.items():
            if count > 0:
                print(f"     {symbol}: {count} data points")
                total_points += count
        
        print(f"   Total: {total_points} new data points")
    else:
        print("   All symbols already have data!")
    
    print()
    
    # Example 5: Final data summary
    print("5. Final data summary:")
    final_summary = fetcher.get_data_summary(output_path)
    
    if final_summary:
        # Sort by number of data points
        sorted_summary = sorted(final_summary.items(), key=lambda x: x[1]['data_points'], reverse=True)
        
        for symbol, info in sorted_summary:
            first_date = info['first_date'].strftime('%Y-%m-%d') if info['first_date'] else 'N/A'
            last_date = info['last_date'].strftime('%Y-%m-%d') if info['last_date'] else 'N/A'
            print(f"   {symbol:<10} | {info['exchange']:<8} | {info['data_points']:>6} points | {first_date} to {last_date}")
    else:
        print("   No data found")
    
    print(f"\nData saved to: {output_path}")
    print("You can use the CLI commands to manage this data:")
    print("  python3 -m quant.orchestrator.cli stooq-data-summary --data-path examples/stooq_example_data.csv")
    print("  python3 -m quant.orchestrator.cli check-missing-data --data-path examples/stooq_example_data.csv")


if __name__ == "__main__":
    main() 