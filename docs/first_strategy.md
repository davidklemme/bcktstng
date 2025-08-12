# Your First Strategy

This guide shows how to run the built-in `ma_cross` strategy.

1. Prepare inputs:
   - Bars CSV with columns: ts,symbol_id,open,high,low,close,volume,dt
   - Symbols SQLite DB with PIT symbol rows
   - FX SQLite DB with USD/EUR rates

2. Run via CLI:

```bash
python -m quant.orchestrator.cli run-backtest \
  --strategy-name ma_cross \
  --strategy-symbol AAPL \
  --start 2024-06-03T00:00:00Z \
  --end 2024-06-10T00:00:00Z \
  --bars-csv /path/to/bars.csv \
  --exchange XNYS
```

Artifacts are written under `runs/cli/<run_id>/` including `equity.csv`, `orders.csv`, `fills.csv`, `positions.csv`, `metrics.json`, and `run_manifest.json`.