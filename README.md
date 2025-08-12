# Quant Backtesting Engine

Event-driven backtesting with realistic costs, PIT-safe data access, and a simple Strategy SDK.

## Quickstart

### Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Run simple strategies on dummy data (console output)

- Bollinger Bands:
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

Artifacts (equity.csv, orders.csv, fills.csv, metrics.json) are written under `/workspace/runs/cli`.

### Run tests (prints metrics to console)

```bash
pytest -q tests/simple_strategies -s
```

### Hypothesis testing

- The simple strategies are intended to be used for rapid hypothesis testing on small, deterministic datasets.
- See `docs/simple_strategies.md` and `docs/dummy_data.md` for details.

## Documentation

- docs/simple_strategies.md – Bollinger Bands and Rate-of-Change strategies, parameters, and CLI usage.
- docs/dummy_data.md – Dummy data format and how to load or use it.
- docs/first_strategy.md – Walkthrough of writing your first strategy.
- docs/costs.md – Cost profiles and configuration.

For project context and architecture, see `Context.md`.