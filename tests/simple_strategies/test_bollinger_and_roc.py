import json
from datetime import datetime
from pathlib import Path

from quant.orchestrator.backtest import run_backtest
from quant.strategies.simple.bollinger import BollingerBands
from quant.strategies.simple.roc import RateOfChange


def test_bollinger_backtest_smoke(daily_reader_and_store, capsys):
    reader, store = daily_reader_and_store

    start = datetime.fromisoformat("2024-01-03")
    end = datetime.fromisoformat("2024-02-29")

    strat = BollingerBands(window=10, num_std=2.0, position_size=50)

    result = run_backtest(
        strategy=strat,
        reader=reader,
        bars_store=store,
        start=start,
        end=end,
        base_currency="EUR",
        # Must be funded: an unfunded run buys on implicit unlimited credit and
        # leaves every return metric undefined.
        starting_cash=100_000.0,
        costs_yaml_path=str(Path("quant/data/cost_profiles.yml")),
        out_dir=Path("runs/tests/bollinger"),
    )

    # Print some console output for visual check
    print("Bollinger metrics:", json.dumps(result.metrics, indent=2))
    print("Bollinger final equity:", result.metrics.get("final_equity_eur"))
    print("Bollinger orders:", len(result.orders))

    # Basic assertions to ensure viability
    assert len(result.equity) > 0
    assert isinstance(result.metrics.get("total_return"), float)
    assert result.metrics["initial_capital"] == 100_000.0
    assert result.metrics["max_drawdown"] <= 0.0

    # Check that the strategy processed multiple symbols
    print("Bollinger available symbols:", len(strat.available_symbols))
    assert len(strat.available_symbols) == 3  # AAPL, MSFT, GOOGL


def test_roc_backtest_smoke(daily_reader_and_store, capsys):
    reader, store = daily_reader_and_store

    start = datetime.fromisoformat("2024-01-03")
    end = datetime.fromisoformat("2024-02-29")

    strat = RateOfChange(window=5, upper=0.03, lower=-0.03, position_size=50)

    result = run_backtest(
        strategy=strat,
        reader=reader,
        bars_store=store,
        start=start,
        end=end,
        base_currency="EUR",
        # Must be funded: an unfunded run buys on implicit unlimited credit and
        # leaves every return metric undefined.
        starting_cash=100_000.0,
        costs_yaml_path=str(Path("quant/data/cost_profiles.yml")),
        out_dir=Path("runs/tests/roc"),
    )

    print("ROC metrics:", json.dumps(result.metrics, indent=2))
    print("ROC final equity:", result.metrics.get("final_equity_eur"))
    print("ROC orders:", len(result.orders))

    assert len(result.equity) > 0
    assert isinstance(result.metrics.get("total_return"), float)
    assert result.metrics["initial_capital"] == 100_000.0
    assert result.metrics["max_drawdown"] <= 0.0

    # Check that the strategy processed multiple symbols
    print("ROC available symbols:", len(strat.available_symbols))
    assert len(strat.available_symbols) == 3  # AAPL, MSFT, GOOGL
