# Symbol Management

This directory contains tools for managing stock symbols from major global markets. The system supports both current and historic symbols with proper date ranges.

## Files

- `comprehensive_symbols.csv` - Comprehensive list of symbols from major global markets
- `market_data_fetcher.py` - Module for fetching symbols from various exchanges
- `symbols_repository.py` - Database interface for symbol management

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
python -m quant.orchestrator.cli run-backtest bollinger \
  --start 2023-01-01T00:00:00Z \
  --end 2023-12-31T00:00:00Z \
  --bars-csv path/to/bars.csv \
  --symbols-db path/to/symbols.db
```

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
