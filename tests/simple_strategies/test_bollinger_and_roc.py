import json
from datetime import datetime
from pathlib import Path

from quant.data.bars_loader import load_daily_bars_csv
from quant.data.pit_reader import PITDataReader, BarsStore
from quant.data.symbols_repository import create_sqlite_engine, load_symbols_csv_to_db
from quant.data.fx_repository import create_engine as create_fx_engine, load_fx_csv_to_db
from quant.orchestrator.backtest import run_backtest
from quant.strategies.simple.bollinger import BollingerBands
from quant.strategies.simple.roc import RateOfChange

DATA_DIR = Path("quant/data/dummy")


def _prepare_reader_and_store():
    rows, validation = load_daily_bars_csv(str(DATA_DIR / "daily_bars.csv"), "XNYS")
    assert not validation.missing_dates  # our dummy data has continuous sessions for used range
    store = BarsStore.from_rows(rows)

    symbols_engine = create_sqlite_engine(":memory:")
    fx_engine = create_fx_engine(":memory:")

    # Load symbols and fx
    load_symbols_csv_to_db(str(DATA_DIR / "symbols.csv"), symbols_engine)
    load_fx_csv_to_db(str(DATA_DIR / "fx.csv"), fx_engine)

    reader = PITDataReader(fx_engine, symbols_engine, store)
    return reader, store


def test_bollinger_backtest_smoke(capsys):
    reader, store = _prepare_reader_and_store()

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
        costs_yaml_path=str(Path("quant/data/cost_profiles.yml")),
        out_dir=Path("runs/tests/bollinger"),
    )

    # Print some console output for visual check
    print("Bollinger metrics:", json.dumps(result.metrics, indent=2))
    print("Bollinger final equity:", result.metrics.get("final_equity_eur"))
    print("Bollinger orders:", len(result.orders))

    # Basic assertions to ensure viability
    assert len(result.equity) > 0
    assert isinstance(result.metrics.get("return"), float)
    
    # Check that the strategy processed multiple symbols
    print("Bollinger available symbols:", len(strat.available_symbols))
    assert len(strat.available_symbols) == 3  # AAPL, MSFT, GOOGL


def test_roc_backtest_smoke(capsys):
    reader, store = _prepare_reader_and_store()

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
        costs_yaml_path=str(Path("quant/data/cost_profiles.yml")),
        out_dir=Path("runs/tests/roc"),
    )

    print("ROC metrics:", json.dumps(result.metrics, indent=2))
    print("ROC final equity:", result.metrics.get("final_equity_eur"))
    print("ROC orders:", len(result.orders))

    assert len(result.equity) > 0
    assert isinstance(result.metrics.get("return"), float)
    
    # Check that the strategy processed multiple symbols
    print("ROC available symbols:", len(strat.available_symbols))
    assert len(strat.available_symbols) == 3  # AAPL, MSFT, GOOGL