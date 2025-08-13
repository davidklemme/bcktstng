# Simple Strategies: Bollinger Bands and Rate of Change

This repository includes two simple trading strategies for quick hypothesis testing and end-to-end validation.

## BollingerBands

- Mean-reversion around a rolling mean with k standard deviations.
- Module: `quant/strategies/simple/bollinger.py`
- Class: `BollingerBands`

Parameters:

- `window` (int, default 20): Rolling window size
- `num_std` (float, default 2.0): Number of standard deviations for the bands
- `position_size` (int, default 100): Trade size when signals trigger

Signal logic:

- Long when price < lower band; short when price > upper band; flat otherwise.
- **Automatically processes all available symbols in the dataset.**

## RateOfChange (ROC)

- Momentum based on n-period rate of change.
- Module: `quant/strategies/simple/roc.py`
- Class: `RateOfChange`

Parameters:

- `window` (int, default 10): ROC lookback
- `upper` (float, default +0.02): Long threshold (e.g., +2%)
- `lower` (float, default -0.02): Short threshold (e.g., -2%)
- `position_size` (int, default 100)

Signal logic:

- Long when ROC > upper; short when ROC < lower; flat otherwise.
- **Automatically processes all available symbols in the dataset.**

## Run via CLI (on dummy data)

- Bollinger:

```bash
python -m quant.orchestrator.cli run-backtest \
  bollinger \
  --start 2024-01-03T00:00:00 \
  --end 2024-02-29T00:00:00 \
  --bars-csv quant/data/dummy/daily_bars.csv \
  --exchange XNYS \
  --symbols-db :memory: \
  --fx-db :memory:
```

- Rate of Change:

```bash
python -m quant.orchestrator.cli run-backtest \
  roc \
  --start 2024-01-03T00:00:00 \
  --end 2024-02-29T00:00:00 \
  --bars-csv quant/data/dummy/daily_bars.csv \
  --exchange XNYS \
  --symbols-db :memory: \
  --fx-db :memory:
```

Artifacts are written to `runs/cli`.

## Run tests (prints metrics to console)

```bash
pytest -q tests/simple_strategies -s
```

## Notes

- These strategies use the shared Strategy SDK (`quant/sdk/strategy.py`) and features (`quant/sdk/features.py`).
- **Multi-Symbol Support**: Both strategies now automatically process all available symbols in the dataset without requiring a specific symbol parameter.
- The dummy dataset includes AAPL, MSFT, and GOOGL for testing multi-symbol functionality.
- Each strategy maintains separate state tracking for each symbol to ensure independent signal generation.
