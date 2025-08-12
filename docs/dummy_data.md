# Dummy Data for Quick Backtests

Small deterministic datasets are provided for rapid iteration and hypothesis testing.

Location: `quant/data/dummy`

Files:
- `daily_bars.csv` – Columns: `dt,symbol_id,open,high,low,close,volume` (exchange `XNYS`)
- `symbols.csv` – Columns: `symbol_id,ticker,exchange,currency,active_from,active_to`
- `fx.csv` – Columns: `ts,base_ccy,quote_ccy,rate` (e.g., USD/EUR)

These are used by the test suite and can be used directly with the CLI.

## Using with CLI

Pass the bars CSV and (optionally) in-memory symbol/FX databases:
```bash
python -m quant.orchestrator.cli run-backtest \
  bollinger \
  --strategy-symbol AAPL \
  --start 2024-01-03T00:00:00 \
  --end 2024-02-29T00:00:00 \
  --bars-csv /workspace/quant/data/dummy/daily_bars.csv \
  --exchange XNYS \
  --symbols-db :memory: \
  --fx-db :memory:
```

## Persisting symbol/FX databases (optional)

You can persist the symbol and FX metadata to SQLite files for reuse.

```python
from pathlib import Path
from quant.data.symbols_repository import create_sqlite_engine, load_symbols_csv_to_db
from quant.data.fx_repository import create_engine as create_fx_engine, load_fx_csv_to_db

DATA = Path("/workspace/quant/data/dummy")
SYMBOLS_DB = "/workspace/data/symbols.db"
FX_DB = "/workspace/data/fx.db"

symbols_engine = create_sqlite_engine(SYMBOLS_DB)
fx_engine = create_fx_engine(FX_DB)
load_symbols_csv_to_db(str(DATA/"symbols.csv"), symbols_engine)
load_fx_csv_to_db(str(DATA/"fx.csv"), fx_engine)
print("Wrote:", SYMBOLS_DB, FX_DB)
```

Then run CLI pointing at these files:
```bash
python -m quant.orchestrator.cli run-backtest \
  bollinger \
  --strategy-symbol AAPL \
  --start 2024-01-03T00:00:00 \
  --end 2024-02-29T00:00:00 \
  --bars-csv /workspace/quant/data/dummy/daily_bars.csv \
  --exchange XNYS \
  --symbols-db /workspace/data/symbols.db \
  --fx-db /workspace/data/fx.db
```

## Tests

The test suite loads these CSVs directly and prints metrics to console:
```bash
pytest -q tests/simple_strategies -s
```