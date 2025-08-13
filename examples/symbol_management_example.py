#!/usr/bin/env python3
"""
Example script demonstrating how to use the symbol management features.

This script shows how to:
1. List available markets
2. Fetch symbols from specific markets
3. Compare with existing data
4. Save symbols to CSV
5. Update existing symbol files
"""

from pathlib import Path
import sys
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from quant.data.market_data_fetcher import MarketDataFetcher, MarketSymbol


def main():
    print("=== Symbol Management Example ===\n")
    
    # Initialize the fetcher
    fetcher = MarketDataFetcher()
    
    # 1. List all available markets
    print("1. Available Markets:")
    print("-" * 60)
    markets = fetcher.get_major_markets()
    for code, name in markets.items():
        currency = fetcher.get_currency_for_exchange(code)
        print(f"{code:<8} | {name:<35} | {currency}")
    print()
    
    # 2. Fetch symbols from specific markets
    print("2. Fetching symbols from NASDAQ and NYSE:")
    print("-" * 60)
    symbols = fetcher.fetch_all_markets(['XNAS', 'XNYS'])
    
    # Group by exchange
    by_exchange = {}
    for symbol in symbols:
        if symbol.exchange not in by_exchange:
            by_exchange[symbol.exchange] = []
        by_exchange[symbol.exchange].append(symbol)
    
    for exchange, exchange_symbols in by_exchange.items():
        exchange_name = markets.get(exchange, exchange)
        print(f"\n{exchange_name} ({exchange}):")
        for symbol in exchange_symbols[:5]:  # Show first 5
            print(f"  {symbol.ticker:<8} | {symbol.currency}")
        if len(exchange_symbols) > 5:
            print(f"  ... and {len(exchange_symbols) - 5} more")
    
    print(f"\nTotal: {len(symbols)} symbols across {len(by_exchange)} exchanges")
    print()
    
    # 3. Compare with existing dummy data
    print("3. Comparing with existing dummy data:")
    print("-" * 60)
    dummy_path = Path("quant/data/dummy/symbols.csv")
    if dummy_path.exists():
        added, removed, unchanged = fetcher.compare_with_existing(symbols, dummy_path)
        print(f"Added: {len(added)} symbols")
        print(f"Removed: {len(removed)} symbols")
        print(f"Unchanged: {len(unchanged)} symbols")
        
        if added:
            print("\nNew symbols to add:")
            for symbol in added[:5]:
                print(f"  {symbol.ticker} ({symbol.exchange})")
            if len(added) > 5:
                print(f"  ... and {len(added) - 5} more")
    else:
        print("No existing dummy data found for comparison")
    print()
    
    # 4. Save symbols to a new CSV file
    print("4. Saving symbols to CSV:")
    print("-" * 60)
    output_path = Path("examples/generated_symbols.csv")
    output_path.parent.mkdir(exist_ok=True)
    
    fetcher.save_symbols_to_csv(symbols, output_path)
    print(f"Saved {len(symbols)} symbols to {output_path}")
    print()
    
    # 5. Demonstrate historic symbols
    print("5. Historic Symbols Example:")
    print("-" * 60)
    
    # Create some example historic symbols
    from datetime import datetime, timezone
    
    historic_symbols = [
        # Facebook was renamed to Meta
        MarketSymbol(
            ticker='FB',
            exchange='XNAS',
            currency='USD',
            name='Facebook Inc.',
            active_from=datetime(2020, 1, 1, tzinfo=timezone.utc),
            active_to=datetime(2021, 10, 28, tzinfo=timezone.utc)
        ),
        # Google was renamed
        MarketSymbol(
            ticker='GOOG',
            exchange='XNAS',
            currency='USD',
            name='Alphabet Inc. (Class C)',
            active_from=datetime(2020, 1, 1, tzinfo=timezone.utc),
            active_to=datetime(2022, 7, 15, tzinfo=timezone.utc)
        ),
        # Current symbols
        MarketSymbol(
            ticker='META',
            exchange='XNAS',
            currency='USD',
            name='Meta Platforms Inc.',
            active_from=datetime(2021, 10, 28, tzinfo=timezone.utc),
            active_to=None
        ),
        MarketSymbol(
            ticker='GOOGL',
            exchange='XNAS',
            currency='USD',
            name='Alphabet Inc. (Class A)',
            active_from=datetime(2022, 7, 15, tzinfo=timezone.utc),
            active_to=None
        ),
    ]
    
    print("Historic symbol changes:")
    for symbol in historic_symbols:
        status = "ACTIVE" if symbol.active_to is None else "INACTIVE"
        active_to_str = symbol.active_to.isoformat() if symbol.active_to else "Present"
        print(f"  {symbol.ticker:<8} | {symbol.name:<25} | {status:<8} | {active_to_str}")
    
    print()
    print("=== Example completed successfully! ===")
    print(f"Generated file: {output_path}")
    print("You can now use these symbols with the backtesting system.")


if __name__ == "__main__":
    main() 