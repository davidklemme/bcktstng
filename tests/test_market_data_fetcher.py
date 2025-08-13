import pytest
from datetime import datetime, timezone
from pathlib import Path
from quant.data.market_data_fetcher import MarketDataFetcher, MarketSymbol


def test_market_data_fetcher_initialization():
    """Test that MarketDataFetcher can be initialized."""
    fetcher = MarketDataFetcher()
    assert fetcher is not None


def test_get_major_markets():
    """Test that get_major_markets returns expected markets."""
    fetcher = MarketDataFetcher()
    markets = fetcher.get_major_markets()
    
    # Check that we have the expected major markets
    assert 'XNAS' in markets
    assert 'XNYS' in markets
    assert 'XLON' in markets
    assert 'XTOK' in markets
    
    # Check that market names are strings
    assert isinstance(markets['XNAS'], str)
    assert isinstance(markets['XNYS'], str)


def test_get_currency_for_exchange():
    """Test that get_currency_for_exchange returns correct currencies."""
    fetcher = MarketDataFetcher()
    
    assert fetcher.get_currency_for_exchange('XNAS') == 'USD'
    assert fetcher.get_currency_for_exchange('XNYS') == 'USD'
    assert fetcher.get_currency_for_exchange('XLON') == 'GBP'
    assert fetcher.get_currency_for_exchange('XTOK') == 'JPY'
    assert fetcher.get_currency_for_exchange('XHKG') == 'HKD'


def test_fetch_nasdaq_symbols():
    """Test that fetch_nasdaq_symbols returns expected symbols."""
    fetcher = MarketDataFetcher()
    symbols = fetcher.fetch_nasdaq_symbols()
    
    assert len(symbols) > 0
    assert all(isinstance(s, MarketSymbol) for s in symbols)
    
    # Check that all symbols are from NASDAQ
    assert all(s.exchange == 'XNAS' for s in symbols)
    assert all(s.currency == 'USD' for s in symbols)
    
    # Check that we have some expected symbols
    tickers = [s.ticker for s in symbols]
    assert 'AAPL' in tickers
    assert 'MSFT' in tickers
    assert 'GOOGL' in tickers


def test_fetch_nyse_symbols():
    """Test that fetch_nyse_symbols returns expected symbols."""
    fetcher = MarketDataFetcher()
    symbols = fetcher.fetch_nyse_symbols()
    
    assert len(symbols) > 0
    assert all(isinstance(s, MarketSymbol) for s in symbols)
    
    # Check that all symbols are from NYSE
    assert all(s.exchange == 'XNYS' for s in symbols)
    assert all(s.currency == 'USD' for s in symbols)
    
    # Check that we have some expected symbols
    tickers = [s.ticker for s in symbols]
    assert 'JPM' in tickers
    assert 'BAC' in tickers
    assert 'WMT' in tickers


def test_fetch_all_markets():
    """Test that fetch_all_markets returns symbols from multiple markets."""
    fetcher = MarketDataFetcher()
    symbols = fetcher.fetch_all_markets(['XNAS', 'XNYS'])
    
    assert len(symbols) > 0
    
    # Check that we have symbols from both markets
    exchanges = set(s.exchange for s in symbols)
    assert 'XNAS' in exchanges
    assert 'XNYS' in exchanges


def test_save_symbols_to_csv(tmp_path):
    """Test that save_symbols_to_csv creates a valid CSV file."""
    fetcher = MarketDataFetcher()
    symbols = fetcher.fetch_nasdaq_symbols()[:5]  # Just a few symbols for testing
    
    output_path = tmp_path / "test_symbols.csv"
    fetcher.save_symbols_to_csv(symbols, output_path)
    
    assert output_path.exists()
    
    # Check that the CSV has the expected format
    with open(output_path, 'r') as f:
        lines = f.readlines()
        assert len(lines) == len(symbols) + 1  # +1 for header
        
        # Check header
        assert lines[0].strip() == 'symbol_id,ticker,exchange,currency,active_from,active_to'
        
        # Check that we have data rows
        assert len(lines) > 1


def test_compare_with_existing(tmp_path):
    """Test that compare_with_existing correctly identifies differences."""
    fetcher = MarketDataFetcher()
    
    # Create a test CSV with some symbols
    test_csv = tmp_path / "existing.csv"
    with open(test_csv, 'w') as f:
        f.write("symbol_id,ticker,exchange,currency,active_from,active_to\n")
        f.write("1,AAPL,XNAS,USD,2020-01-01T00:00:00Z,\n")
        f.write("2,MSFT,XNAS,USD,2020-01-01T00:00:00Z,\n")
    
    # Create new symbols (some overlapping, some new)
    new_symbols = [
        MarketSymbol('AAPL', 'XNAS', 'USD', 'Apple Inc'),
        MarketSymbol('GOOGL', 'XNAS', 'USD', 'Alphabet Inc'),
        MarketSymbol('MSFT', 'XNAS', 'USD', 'Microsoft Corp'),
    ]
    
    added, removed, unchanged = fetcher.compare_with_existing(new_symbols, test_csv)
    
    # AAPL and MSFT should be unchanged
    assert len(unchanged) == 2
    assert any(s.ticker == 'AAPL' for s in unchanged)
    assert any(s.ticker == 'MSFT' for s in unchanged)
    
    # GOOGL should be added
    assert len(added) == 1
    assert added[0].ticker == 'GOOGL'
    
    # Nothing should be removed in this case since we're not removing any symbols


def test_market_symbol_creation():
    """Test that MarketSymbol can be created with various parameters."""
    # Test with all parameters
    symbol = MarketSymbol(
        ticker='AAPL',
        exchange='XNAS',
        currency='USD',
        name='Apple Inc.',
        sector='Technology',
        market_cap=2000000000000.0,
        active_from=datetime(2020, 1, 1, tzinfo=timezone.utc),
        active_to=None
    )
    
    assert symbol.ticker == 'AAPL'
    assert symbol.exchange == 'XNAS'
    assert symbol.currency == 'USD'
    assert symbol.name == 'Apple Inc.'
    assert symbol.sector == 'Technology'
    assert symbol.market_cap == 2000000000000.0
    assert symbol.active_from == datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert symbol.active_to is None
    
    # Test with minimal parameters
    symbol2 = MarketSymbol(
        ticker='MSFT',
        exchange='XNAS',
        currency='USD',
        name='Microsoft Corp.'
    )
    
    assert symbol2.ticker == 'MSFT'
    assert symbol2.sector is None
    assert symbol2.market_cap is None
    assert symbol2.active_from is None
    assert symbol2.active_to is None 