# Simple Strategies: Bollinger Bands and Rate of Change

This repository includes two simple trading strategies for quick hypothesis testing and end-to-end validation.

## BollingerBands

- Mean-reversion around a rolling mean with k standard deviations.
- Module: `quant/strategies/simple/bollinger.py`
- Class: `BollingerBands`

Parameters:
- `symbol` (str): Ticker (e.g., `AAPL`)
- `window` (int, default 20): Rolling window size
- `num_std` (float, default 2.0): Number of standard deviations for the bands
- `position_size` (int, default 100): Trade size when signals trigger

Signal logic:
- Long when price < lower band; short when price > upper band; flat otherwise.

## RateOfChange (ROC)

- Momentum based on n-period rate of change.
- Module: `quant/strategies/simple/roc.py`
- Class: `RateOfChange`

Parameters:
- `symbol` (str): Ticker
- `window` (int, default 10): ROC lookback
- `upper` (float, default +0.02): Long threshold (e.g., +2%)
- `lower` (float, default -0.02): Short threshold (e.g., -2%)
- `position_size` (int, default 100)

Signal logic:
- Long when ROC > upper; short when ROC < lower; flat otherwise.

## Run via CLI (on dummy data)

- Bollinger:
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

- Rate of Change:
```bash
python -m quant.orchestrator.cli run-backtest \
  roc \
  --strategy-symbol AAPL \
  --start 2024-01-03T00:00:00 \
  --end 2024-02-29T00:00:00 \
  --bars-csv /workspace/quant/data/dummy/daily_bars.csv \
  --exchange XNYS \
  --symbols-db :memory: \
  --fx-db :memory:
```

Artifacts are written to `/workspace/runs/cli`.

## Run tests (prints metrics to console)

```bash
pytest -q tests/simple_strategies -s
```

## Notes
- These strategies use the shared Strategy SDK (`quant/sdk/strategy.py`) and features (`quant/sdk/features.py`).
- They are intentionally simple and deterministic to validate the backtesting loop and costs end-to-end.