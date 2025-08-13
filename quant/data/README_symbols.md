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
- Stooq historical data files

## CLI Commands

The quant CLI includes several commands for managing symbols and fetching historical data:

### Symbol Management

#### List Available Markets

```bash
python3 -m quant.orchestrator.cli list-markets
```

This shows all available major markets with their exchange codes and currencies.

#### Fetch Symbols (Dry Run)

```bash
python3 -m quant.orchestrator.cli fetch-symbols --dry-run
```

This shows what symbols would be fetched without actually saving them. Useful for previewing changes.

#### Fetch Symbols from Specific Markets

```bash
python3 -m quant.orchestrator.cli fetch-symbols --markets XNAS,XNYS,XLON --output-path my_symbols.csv
```

Fetches symbols from NASDAQ, NYSE, and London Stock Exchange.

#### Compare with Existing File

```bash
python3 -m quant.orchestrator.cli fetch-symbols --compare-existing quant/data/dummy/symbols.csv
```

Shows differences between what would be fetched and an existing symbols file.

#### Update Existing Symbols File

```bash
python3 -m quant.orchestrator.cli update-symbols --symbols-csv quant/data/comprehensive_symbols.csv --dry-run
```

Shows what changes would be made to an existing symbols file without actually updating it.

### Historical Data Fetching (Stooq)

#### Fetch Data for Single Symbol

```bash
python3 -m quant.orchestrator.cli fetch-stooq-data --symbol AAPL --exchange XNAS --start-date 2024-01-01 --end-date 2024-08-13
```

Fetches historical data for a specific symbol from Stooq.

#### Fetch Data for Multiple Symbols

```bash
python3 -m quant.orchestrator.cli fetch-stooq-data --symbols-csv quant/data/comprehensive_symbols.csv --delay 2.0
```

Fetches data for all symbols in the CSV with a 2-second delay between requests.

#### Dry Run (Preview)

```bash
python3 -m quant.orchestrator.cli fetch-stooq-data --symbols-csv quant/data/comprehensive_symbols.csv --dry-run
```

Shows what would be fetched without actually downloading data.

#### Force Refresh All Data

```bash
python3 -m quant.orchestrator.cli fetch-stooq-data --symbols-csv quant/data/comprehensive_symbols.csv --force-refresh
```

Ignores existing data and fetches everything fresh.

#### View Data Summary

```bash
python3 -m quant.orchestrator.cli stooq-data-summary --data-path quant/data/stooq_data.csv
```

Shows summary of existing historical data.

#### Check Missing Data

```bash
python3 -m quant.orchestrator.cli check-missing-data --symbols-csv quant/data/comprehensive_symbols.csv --data-path quant/data/stooq_data.csv
```

Identifies which symbols are missing data for a given date range.

## Stooq Data Features

### Intelligent Missing Data Detection

- Automatically detects existing data for each symbol
- Only fetches missing data points
- Saves time and bandwidth
- Prevents duplicate data

### Rate Limiting

- Configurable delays between requests
- Respects Stooq's rate limits
- Prevents being blocked

### Multi-Exchange Support

- Automatic symbol format conversion for different exchanges
- Supports major global exchanges
- Handles different date formats and currencies

### Data Quality

- Validates data before saving
- Handles missing or corrupted data gracefully
- Provides detailed logging and error reporting

### Flexible Date Ranges

- Defaults to last year of data
- Customizable start and end dates
- Supports any date range

## Supported Exchanges for Stooq Data

The system automatically converts symbol formats for different exchanges:

### North America

- **XNAS** - NASDAQ (symbol.US)
- **XNYS** - New York Stock Exchange (symbol.US)

### Europe

- **XLON** - London Stock Exchange (symbol.L)
- **XETR** - Deutsche Börse (symbol.DE)
- **XAMS** - Euronext Amsterdam (symbol.AS)
- **XPAR** - Euronext Paris (symbol.PA)
- **XBRU** - Euronext Brussels (symbol.BR)
- **XSWX** - SIX Swiss Exchange (symbol.SW)

### Asia-Pacific

- **XTOK** - Tokyo Stock Exchange (symbol - no suffix)
- **XHKG** - Hong Kong Stock Exchange (symbol.HK)
- **XASX** - Australian Securities Exchange (symbol.AX)

### Other Regions

- **XTSX** - Toronto Stock Exchange (symbol.TO)

## Data Format

The Stooq data CSV format includes:

- `symbol` - Stock symbol/ticker
- `exchange` - Exchange code
- `date` - Trading date (ISO 8601 format)
- `open` - Opening price
- `high` - High price
- `low` - Low price
- `close` - Closing price
- `volume` - Trading volume

## Workflow Examples

### 1. Initial Setup

```bash
# List available markets
python3 -m quant.orchestrator.cli list-markets

# Fetch symbols for major markets
python3 -m quant.orchestrator.cli fetch-symbols --markets XNAS,XNYS,XLON --output-path my_symbols.csv

# Preview what data would be fetched
python3 -m quant.orchestrator.cli fetch-stooq-data --symbols-csv my_symbols.csv --dry-run
```

### 2. Fetch Historical Data

```bash
# Fetch data for last year with 2-second delays
python3 -m quant.orchestrator.cli fetch-stooq-data --symbols-csv my_symbols.csv --delay 2.0

# Check what's missing
python3 -m quant.orchestrator.cli check-missing-data --symbols-csv my_symbols.csv

# View summary
python3 -m quant.orchestrator.cli stooq-data-summary
```

### 3. Incremental Updates

```bash
# Fetch only missing data (default behavior)
python3 -m quant.orchestrator.cli fetch-stooq-data --symbols-csv my_symbols.csv

# Force refresh all data
python3 -m quant.orchestrator.cli fetch-stooq-data --symbols-csv my_symbols.csv --force-refresh
```

### 4. Individual Symbol Management

```bash
# Fetch data for specific symbol
python3 -m quant.orchestrator.cli fetch-stooq-data --symbol AAPL --exchange XNAS --start-date 2024-01-01

# Check data for specific symbol
python3 -m quant.orchestrator.cli stooq-data-summary | grep AAPL
```

## Integration with Backtesting

The Stooq data can be used with the existing backtesting system by converting the format:

```python
# Convert Stooq data to bars format for backtesting
import pandas as pd

# Load Stooq data
stooq_data = pd.read_csv('quant/data/stooq_data.csv')

# Convert to bars format
bars_data = stooq_data.rename(columns={
    'symbol': 'symbol_id',
    'date': 'timestamp',
    'open': 'open_price',
    'high': 'high_price',
    'low': 'low_price',
    'close': 'close_price',
    'volume': 'volume'
})

# Save in bars format
bars_data.to_csv('quant/data/bars_from_stooq.csv', index=False)
```

## Error Handling

The system handles various error conditions:

- **Network errors**: Retries with exponential backoff
- **Invalid symbols**: Logs warning and continues
- **Missing data**: Gracefully handles symbols with no data
- **Rate limiting**: Respects delays and handles 429 responses
- **Data corruption**: Validates data before saving

## Performance Considerations

- **Rate limiting**: Default 1-second delay, increase for large datasets
- **Memory usage**: Processes data in chunks for large files
- **Disk space**: Historical data can be large, monitor storage
- **Network**: Consider bandwidth for large datasets

## Future Enhancements

- Real-time data streaming
- Additional data providers (Yahoo Finance, Alpha Vantage)
- Options and futures data
- Fundamental data integration
- Data validation and quality metrics
- Automated data updates via cron jobs
