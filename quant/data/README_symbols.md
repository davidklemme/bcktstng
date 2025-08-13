# Symbol Management

This directory contains tools for managing stock symbols from major global markets. The system supports both current and historic symbols with proper date ranges.

## Data Management

**Important**: The repository only contains minimal dummy data. Comprehensive market data files are excluded from version control to keep the repository clean and lightweight.

### Files in Repository

- `dummy/symbols.csv` - Minimal dummy data for testing (3 symbols)
- `comprehensive_symbols.csv` - Template with 10 sample symbols from different markets

### Files Excluded from Repository

- Large comprehensive symbol files
- Generated market data files
- Backup files
- Database files

## CLI Commands

The quant CLI includes several commands for managing symbols:

### List Available Markets

```bash
python3 -m quant.orchestrator.cli list-markets
```

This shows all available major markets with their exchange codes and currencies.

### Fetch Symbols (Dry Run)

```bash
python3 -m quant.orchestrator.cli fetch-symbols --dry-run
```

This shows what symbols would be fetched without actually saving them. Useful for previewing changes.

### Fetch Symbols from Specific Markets

```bash
python3 -m quant.orchestrator.cli fetch-symbols --markets XNAS,XNYS,XLON --output-path my_symbols.csv
```

Fetches symbols from NASDAQ, NYSE, and London Stock Exchange.

### Compare with Existing File

```bash
python3 -m quant.orchestrator.cli fetch-symbols --compare-existing quant/data/dummy/symbols.csv
```

Shows differences between what would be fetched and an existing symbols file.

### Update Existing Symbols File

```bash
python3 -m quant.orchestrator.cli update-symbols --symbols-csv quant/data/comprehensive_symbols.csv --dry-run
```

Shows what changes would be made to an existing symbols file without actually updating it.

### Update with Backup

```bash
python3 -m quant.orchestrator.cli update-symbols --symbols-csv quant/data/comprehensive_symbols.csv --backup
```

Updates the symbols file and creates a timestamped backup before making changes.

## Supported Markets

The system supports symbols from major global exchanges:

### North America

- **XNAS** - NASDAQ (USD)
- **XNYS** - New York Stock Exchange (USD)
- **XTSX** - Toronto Stock Exchange (CAD)

### Europe

- **XLON** - London Stock Exchange (GBP)
- **XAMS** - Euronext Amsterdam (EUR)
- **XPAR** - Euronext Paris (EUR)
- **XBRU** - Euronext Brussels (EUR)
- **XLIS** - Euronext Lisbon (EUR)
- **XOSL** - Oslo Stock Exchange (NOK)
- **XSTO** - Stockholm Stock Exchange (SEK)
- **XHEL** - Helsinki Stock Exchange (EUR)
- **XCOP** - Copenhagen Stock Exchange (DKK)
- **XICE** - Iceland Stock Exchange (ISK)
- **XWAR** - Warsaw Stock Exchange (PLN)
- **XPRA** - Prague Stock Exchange (CZK)
- **XBUD** - Budapest Stock Exchange (HUF)
- **XVIE** - Vienna Stock Exchange (EUR)
- **XSWX** - SIX Swiss Exchange (CHF)
- **XETR** - Deutsche Börse (EUR)
- **XMIL** - Borsa Italiana (EUR)
- **XMAD** - Madrid Stock Exchange (EUR)

### Asia-Pacific

- **XTOK** - Tokyo Stock Exchange (JPY)
- **XHKG** - Hong Kong Stock Exchange (HKD)
- **XSHG** - Shanghai Stock Exchange (CNY)
- **XSHE** - Shenzhen Stock Exchange (CNY)
- **XBOM** - Bombay Stock Exchange (INR)
- **XNSE** - National Stock Exchange of India (INR)
- **XASX** - Australian Securities Exchange (AUD)

### Other Regions

- **XSAO** - São Paulo Stock Exchange (BRL)
- **XBMF** - B3 Brazilian Mercantile and Futures Exchange (BRL)
- **XJSE** - Johannesburg Stock Exchange (ZAR)
- **XTAE** - Tel Aviv Stock Exchange (ILS)
- **XKAR** - Karachi Stock Exchange (PKR)
- **XCAI** - Cairo Stock Exchange (EGP)
- **XRIY** - Riyadh Stock Exchange (SAR)
- **XADX** - Abu Dhabi Securities Exchange (AED)
- **XDFM** - Dubai Financial Market (AED)

## Symbol Data Format

The CSV format includes:

- `symbol_id` - Unique identifier
- `ticker` - Stock symbol/ticker
- `exchange` - Exchange code
- `currency` - Trading currency
- `active_from` - When the symbol became active (ISO 8601 format)
- `active_to` - When the symbol became inactive (empty for current symbols)

## Historic Symbols

The system supports historic symbols with proper date ranges. For example:

- **FB** (Facebook) was renamed to **META** on October 28, 2021
- **GOOG** (Google) was renamed to **GOOGL** on July 15, 2022

These changes are reflected in the `active_to` field.

## Integration with Backtesting

The symbols can be used with the existing backtesting system:

```bash
python3 -m quant.orchestrator.cli run-backtest bollinger \
  --start 2023-01-01T00:00:00Z \
  --end 2023-12-31T00:00:00Z \
  --bars-csv path/to/bars.csv \
  --symbols-db path/to/symbols.db
```

## Generic Market Approach

The system uses a generic, configuration-driven approach that makes it easy to add support for any market:

### Adding New Markets

To add a new market, simply add a configuration to the `get_market_config` method:

```python
'XNEW': {  # New Exchange
    'name': 'New Stock Exchange',
    'currency': 'XXX',
    'data_sources': [
        {
            'name': 'Exchange Website',
            'type': 'web_scrape',
            'url': 'https://exchange.com/market-data',
            'parser': 'generic_web'
        }
    ],
    'fallback_symbols': ['SYMB1', 'SYMB2', 'SYMB3']
}
```

### Benefits

1. **Easy to extend** - Add markets without changing core code
2. **Consistent interface** - Same API for all markets
3. **Multiple data sources** - Each market can have multiple data sources
4. **Fallback system** - Predefined symbols if APIs fail
5. **Automatic currency detection** - Each market knows its currency

## Data Sources

The current implementation includes curated lists of major symbols from each exchange. In a production environment, you would want to:

1. Integrate with real-time data providers (Yahoo Finance, Alpha Vantage, etc.)
2. Add web scraping for exchange websites
3. Implement proper rate limiting and error handling
4. Add more comprehensive symbol metadata (company names, sectors, market cap, etc.)

## Future Enhancements

- Real-time symbol updates
- Automatic delisting detection
- Sector and industry classification
- Market cap and volume data
- Options and futures symbols
- Cryptocurrency exchanges

## Workflow

1. **Development**: Use dummy data for testing
2. **Production**: Fetch comprehensive data using CLI commands
3. **Updates**: Use dry-run to preview changes before applying
4. **Backup**: Always create backups before major updates
5. **Version Control**: Keep only minimal data in repository
