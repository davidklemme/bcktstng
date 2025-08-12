from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer

from ..data.bars_loader import load_daily_bars_csv
from ..data.pit_reader import PITDataReader, BarsStore
from ..data.symbols_repository import create_sqlite_engine
from ..ops.artifacts import ArtifactWriter
from .backtest import run_backtest
from .config import get_settings
from ..examples.ma_cross import MACross

app = typer.Typer(help="Quant orchestration CLI")


@app.command("ingest-bars")
def ingest_bars(
    csv_path: Path = typer.Argument(..., help="Path to input bars CSV"),
    exchange: str = typer.Option("XNYS", help="Exchange code for the CSV data"),
    parquet_out: Optional[Path] = typer.Option(None, help="Optional Parquet output path"),
):
    rows, validation = load_daily_bars_csv(str(csv_path), exchange)
    typer.echo(json.dumps({"missing_dates": [d.isoformat() for d in validation.missing_dates], "nan_rows": validation.nan_row_indices}))
    if parquet_out is not None:
        try:
            from ..data.bars_loader import write_parquet

            write_parquet(str(parquet_out), rows)
            typer.echo(f"Wrote Parquet to {parquet_out}")
        except Exception as exc:
            typer.echo(f"Parquet write skipped/failed: {exc}")


@app.command("run-backtest")
def run_backtest_cmd(
    strategy_name: str = typer.Argument("ma_cross", help="Strategy to run (e.g., ma_cross)"),
    strategy_symbol: str = typer.Option("AAPL", help="Primary symbol for the strategy"),
    start: str = typer.Option(..., help="Start datetime ISO-8601"),
    end: str = typer.Option(..., help="End datetime ISO-8601"),
    bars_csv: Path = typer.Option(..., help="Bars CSV path"),
    exchange: str = typer.Option("XNYS", help="Exchange code for bars"),
    symbols_db: Optional[Path] = typer.Option(None, help="SQLite symbols DB path"),
    fx_db: Optional[Path] = typer.Option(None, help="SQLite FX DB path"),
    costs_yaml: Optional[Path] = typer.Option(None, help="Costs profile YAML path"),
    out_dir: Optional[Path] = typer.Option(None, help="Output directory for artifacts"),
):
    settings = get_settings()
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)

    # Load data
    rows, _ = load_daily_bars_csv(str(bars_csv), exchange)
    bars_store = BarsStore.from_rows(rows)

    # Engines
    symbols_engine = create_sqlite_engine(str(symbols_db or settings.symbols_db_path))
    fx_engine = create_sqlite_engine(str(fx_db or settings.fx_db_path))

    reader = PITDataReader(fx_engine, symbols_engine, bars_store)

    # Strategy
    if strategy_name.lower() == "ma_cross":
        strat = MACross(symbol=strategy_symbol)
    else:
        typer.echo(f"Unknown strategy: {strategy_name}")
        raise typer.Exit(code=2)

    result = run_backtest(
        strategy=strat,
        reader=reader,
        bars_store=bars_store,
        start=start_dt,
        end=end_dt,
        costs_yaml_path=str(costs_yaml or settings.cost_profiles_path),
        out_dir=str(out_dir or Path(settings.runs_dir) / "cli"),
    )

    typer.echo(json.dumps({"run_id": result.run_id, "metrics": result.metrics}, indent=2))


@app.command("run-signal")
def run_signal_cmd(
    strategy_name: str = typer.Argument("ma_cross", help="Strategy to run"),
    strategy_symbol: str = typer.Option("AAPL", help="Primary symbol for the strategy"),
    asof: str = typer.Option(..., help="As-of datetime ISO-8601"),
    bars_csv: Path = typer.Option(..., help="Bars CSV path"),
    exchange: str = typer.Option("XNYS", help="Exchange code for bars"),
    symbols_db: Optional[Path] = typer.Option(None, help="SQLite symbols DB path"),
    fx_db: Optional[Path] = typer.Option(None, help="SQLite FX DB path"),
    costs_yaml: Optional[Path] = typer.Option(None, help="Costs profile YAML path"),
):
    # For simplicity, run a single-timestep backtest
    return run_backtest_cmd(
        strategy_name=strategy_name,
        strategy_symbol=strategy_symbol,
        start=asof,
        end=asof,
        bars_csv=bars_csv,
        exchange=exchange,
        symbols_db=symbols_db,
        fx_db=fx_db,
        costs_yaml=costs_yaml,
        out_dir=None,
    )


if __name__ == "__main__":
    app()