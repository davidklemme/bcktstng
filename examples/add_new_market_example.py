#!/usr/bin/env python3
"""
Example showing how to add new markets using the generic approach.

This demonstrates how easy it is to extend the system for any market
without writing specific code for each one.
"""

import sys
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from quant.data.market_data_fetcher import MarketDataFetcher


def add_new_market_example():
    """Example of how to add a new market configuration."""
    
    print("=== Adding New Market Example ===\n")
    
    # Initialize the fetcher
    fetcher = MarketDataFetcher()
    
    # Example: Let's add a new market configuration for the Swiss Exchange (XSWX)
    print("1. Adding Swiss Exchange (XSWX) configuration:")
    print("-" * 60)
    
    # This is how you would add a new market configuration
    # In practice, you'd add this to the get_market_config method
    swiss_config = {
        'name': 'SIX Swiss Exchange',
        'currency': 'CHF',
        'data_sources': [
            {
                'name': 'SIX Website',
                'type': 'web_scrape',
                'url': 'https://www.six-group.com/en/home/markets/indices/swiss-market-indices.html',
                'parser': 'generic_web'
            },
            {
                'name': 'Yahoo Finance',
                'type': 'api',
                'url': 'https://query1.finance.yahoo.com/v8/finance/chart/%5ESSMI',
                'parser': 'yahoo_finance'
            }
        ],
        'fallback_symbols': [
            'NESN', 'ROG', 'NOVN', 'CSGN', 'UBSG', 'ABBN', 'ZURN', 'SREN', 'GIVN', 'LONN',
            'SCMN', 'ATLN', 'SGS', 'GEBN', 'SGSN', 'BAER', 'CLN', 'KER', 'LHN', 'SWTQ'
        ]
    }
    
    print(f"Market: {swiss_config['name']}")
    print(f"Currency: {swiss_config['currency']}")
    print(f"Data Sources: {len(swiss_config['data_sources'])}")
    print(f"Fallback Symbols: {len(swiss_config['fallback_symbols'])}")
    print()
    
    # Example: Let's add a new market configuration for the Australian Securities Exchange (XASX)
    print("2. Adding Australian Securities Exchange (XASX) configuration:")
    print("-" * 60)
    
    australian_config = {
        'name': 'Australian Securities Exchange',
        'currency': 'AUD',
        'data_sources': [
            {
                'name': 'ASX Website',
                'type': 'web_scrape',
                'url': 'https://www.asx.com.au/markets/market-resources/asx-market-data',
                'parser': 'generic_web'
            },
            {
                'name': 'Yahoo Finance',
                'type': 'api',
                'url': 'https://query1.finance.yahoo.com/v8/finance/chart/%5EAXJO',
                'parser': 'yahoo_finance'
            }
        ],
        'fallback_symbols': [
            'CSL', 'NAB', 'ANZ', 'WBC', 'RIO', 'BHP', 'CBA', 'WES', 'MQG', 'TLS',
            'WOW', 'TCL', 'QBE', 'AMP', 'ORG', 'STO', 'WPL', 'SUN', 'AGL', 'IAG'
        ]
    }
    
    print(f"Market: {australian_config['name']}")
    print(f"Currency: {australian_config['currency']}")
    print(f"Data Sources: {len(australian_config['data_sources'])}")
    print(f"Fallback Symbols: {len(australian_config['fallback_symbols'])}")
    print()
    
    # Example: Let's add a new market configuration for the Toronto Stock Exchange (XTSX)
    print("3. Adding Toronto Stock Exchange (XTSX) configuration:")
    print("-" * 60)
    
    canadian_config = {
        'name': 'Toronto Stock Exchange',
        'currency': 'CAD',
        'data_sources': [
            {
                'name': 'TSX Website',
                'type': 'web_scrape',
                'url': 'https://www.tsx.com/listings/listing-with-us/listed-companies',
                'parser': 'generic_web'
            },
            {
                'name': 'Yahoo Finance',
                'type': 'api',
                'url': 'https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPTSE',
                'parser': 'yahoo_finance'
            }
        ],
        'fallback_symbols': [
            'RY', 'TD', 'BNS', 'BMO', 'CM', 'ENB', 'CNR', 'CP', 'TRP', 'SU',
            'ABX', 'GOLD', 'WCN', 'ATD', 'L', 'CTC', 'MRU', 'TFII', 'CSU', 'SHOP'
        ]
    }
    
    print(f"Market: {canadian_config['name']}")
    print(f"Currency: {canadian_config['currency']}")
    print(f"Data Sources: {len(canadian_config['data_sources'])}")
    print(f"Fallback Symbols: {len(canadian_config['fallback_symbols'])}")
    print()
    
    # Show how to use the existing markets
    print("4. Using existing markets with the generic approach:")
    print("-" * 60)
    
    # Test with multiple markets
    markets_to_test = ['XETR', 'XNAS', 'XNYS', 'XLON']
    
    for market in markets_to_test:
        print(f"\nTesting market: {market}")
        try:
            symbols = fetcher.fetch_market_symbols(market)
            market_config = fetcher.get_market_config(market)
            market_name = market_config.get('name', market) if market_config else market
            print(f"  Market: {market_name}")
            print(f"  Symbols found: {len(symbols)}")
            print(f"  Currency: {market_config.get('currency', 'Unknown') if market_config else 'Unknown'}")
            
            # Show first few symbols
            if symbols:
                print(f"  Sample symbols: {', '.join([s.ticker for s in symbols[:5]])}")
        except Exception as e:
            print(f"  Error: {e}")
    
    print("\n=== Key Benefits of the Generic Approach ===")
    print("1. Easy to add new markets - just add a configuration")
    print("2. Consistent interface for all markets")
    print("3. Multiple data sources per market")
    print("4. Fallback symbols if APIs fail")
    print("5. Automatic currency detection")
    print("6. Extensible parser system")
    print("\n=== Example Usage ===")
    print("# Add a new market to the configuration:")
    print("new_market_config = {")
    print("    'name': 'Your Market Name',")
    print("    'currency': 'XXX',")
    print("    'data_sources': [...],")
    print("    'fallback_symbols': [...]")
    print("}")
    print("\n# Use it immediately:")
    print("symbols = fetcher.fetch_market_symbols('YOUR_EXCHANGE_CODE')")


if __name__ == "__main__":
    add_new_market_example() 