# Hypothesis Testing Suite

This folder documents how to run simple trading strategies (Bollinger Bands and Rate of Change) on dummy data for quick hypothesis testing.

Run via CLI:

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

Replace `bollinger` with `roc` for the Rate-of-Change strategy.

Pytest suite (prints metrics to console):

```bash
pytest -q tests/simple_strategies -s
```

Artifacts are written under `/workspace/runs/tests/...` and `/workspace/runs/cli` when invoked via CLI.