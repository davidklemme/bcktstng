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
from ..research.validation import make_walk_forward_folds, make_purged_kfold_folds, run_walk_forward
from ..research.search import run_hyperparameter_search

app = typer.Typer(help="Quant orchestration CLI")


def _strategy_factory(name: str):
    lname = name.lower()
    if lname == "ma_cross":
        def factory(params: dict):
            return MACross(
                symbol=params.get("symbol", params.get("strategy_symbol", "AAPL")),
                fast=int(params.get("fast", 10)),
                slow=int(params.get("slow", 30)),
            )
        return factory
    raise ValueError(f"Unknown strategy: {name}")


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


@app.command("walk-forward")
def walk_forward_cmd(
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
    # fold params
    mode: str = typer.Option("walk", help="walk for walk-forward, kfold for purged k-fold", show_default=True),
    train_window: int = typer.Option(60, help="Train window (timesteps) for walk-forward"),
    test_window: int = typer.Option(20, help="Validation window (timesteps) for walk-forward"),
    n_splits: int = typer.Option(5, help="Number of splits for k-fold"),
    embargo_fraction: float = typer.Option(0.01, help="Embargo fraction around validation"),
    seed: Optional[int] = typer.Option(None, help="Base seed for determinism"),
):
    settings = get_settings()
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)

    rows, _ = load_daily_bars_csv(str(bars_csv), exchange)
    bars_store = BarsStore.from_rows(rows)

    symbols_engine = create_sqlite_engine(str(symbols_db or settings.symbols_db_path))
    fx_engine = create_sqlite_engine(str(fx_db or settings.fx_db_path))
    reader = PITDataReader(fx_engine, symbols_engine, bars_store)

    if mode.lower() == "walk":
        folds = make_walk_forward_folds(store=bars_store, start=start_dt, end=end_dt, train_window=train_window, test_window=test_window, embargo_fraction=embargo_fraction)
    elif mode.lower() == "kfold":
        folds = make_purged_kfold_folds(store=bars_store, start=start_dt, end=end_dt, n_splits=n_splits, embargo_fraction=embargo_fraction)
    else:
        typer.echo(f"Unknown mode: {mode}")
        raise typer.Exit(code=2)

    factory = _strategy_factory(strategy_name)
    params = {"symbol": strategy_symbol}

    out_base = Path(out_dir or Path(settings.runs_dir) / "cli" / "walk_forward")
    results = run_walk_forward(
        strategy_factory=factory,
        strategy_params=params,
        reader=reader,
        store=bars_store,
        folds=folds,
        costs_yaml_path=str(costs_yaml or settings.cost_profiles_path),
        out_dir=out_base,
        base_seed=seed,
    )

    typer.echo(json.dumps({"folds": len(results), "mean_return": sum(r.get("metrics", {}).get("return", 0.0) for r in results) / max(1, len(results))}, indent=2))


@app.command("hyper-search")
def hyper_search_cmd(
    strategy_name: str = typer.Argument("ma_cross", help="Strategy to run (e.g., ma_cross)"),
    start: str = typer.Option(..., help="Start datetime ISO-8601"),
    end: str = typer.Option(..., help="End datetime ISO-8601"),
    bars_csv: Path = typer.Option(..., help="Bars CSV path"),
    exchange: str = typer.Option("XNYS", help="Exchange code for bars"),
    symbols_db: Optional[Path] = typer.Option(None, help="SQLite symbols DB path"),
    fx_db: Optional[Path] = typer.Option(None, help="SQLite FX DB path"),
    costs_yaml: Optional[Path] = typer.Option(None, help="Costs profile YAML path"),
    out_dir: Optional[Path] = typer.Option(None, help="Output directory for artifacts"),
    # fold params
    mode: str = typer.Option("walk", help="walk for walk-forward, kfold for purged k-fold"),
    train_window: int = typer.Option(60, help="Train window (timesteps) for walk-forward"),
    test_window: int = typer.Option(20, help="Validation window (timesteps) for walk-forward"),
    n_splits: int = typer.Option(5, help="Number of splits for k-fold"),
    embargo_fraction: float = typer.Option(0.01, help="Embargo fraction around validation"),
    # search params
    search_mode: str = typer.Option("grid", help="grid or random"),
    fast_values: Optional[str] = typer.Option(None, help="Comma-separated values for fast MA (grid)"),
    slow_values: Optional[str] = typer.Option(None, help="Comma-separated values for slow MA (grid)"),
    n_trials: Optional[int] = typer.Option(None, help="Number of random trials"),
    seed: Optional[int] = typer.Option(None, help="Base seed for determinism"),
    parallel_workers: Optional[int] = typer.Option(None, help="Number of parallel workers"),
):
    settings = get_settings()
    start_dt = datetime.fromisoformat(start)
    end_dt = datetime.fromisoformat(end)

    rows, _ = load_daily_bars_csv(str(bars_csv), exchange)
    bars_store = BarsStore.from_rows(rows)

    symbols_engine = create_sqlite_engine(str(symbols_db or settings.symbols_db_path))
    fx_engine = create_sqlite_engine(str(fx_db or settings.fx_db_path))
    reader = PITDataReader(fx_engine, symbols_engine, bars_store)

    if mode.lower() == "walk":
        folds = make_walk_forward_folds(store=bars_store, start=start_dt, end=end_dt, train_window=train_window, test_window=test_window, embargo_fraction=embargo_fraction)
    elif mode.lower() == "kfold":
        folds = make_purged_kfold_folds(store=bars_store, start=start_dt, end=end_dt, n_splits=n_splits, embargo_fraction=embargo_fraction)
    else:
        typer.echo(f"Unknown mode: {mode}")
        raise typer.Exit(code=2)

    factory = _strategy_factory(strategy_name)

    from ..research.search import run_hyperparameter_search

    out_base = Path(out_dir or Path(settings.runs_dir) / "cli" / "hyper_search")

    if search_mode.lower() == "grid":
        if not fast_values or not slow_values:
            typer.echo("For grid mode, provide --fast-values and --slow-values (comma-separated)")
            raise typer.Exit(code=2)
        grid = {
            "symbol": ["AAPL"],
            "fast": [int(x) for x in fast_values.split(",")],
            "slow": [int(x) for x in slow_values.split(",")],
        }
        results = run_hyperparameter_search(
            strategy_factory=factory,
            reader=reader,
            store=bars_store,
            folds=folds,
            mode="grid",
            param_grid=grid,
            costs_yaml_path=str(costs_yaml or settings.cost_profiles_path),
            out_dir=out_base,
            base_seed=seed,
            parallel_workers=parallel_workers,
        )
    else:
        # random search over fast/slow ranges as example
        random_spec = {
            "symbol": {"mode": "choice", "values": ["AAPL"]},
            "fast": {"mode": "int", "min": 5, "max": 30},
            "slow": {"mode": "int", "min": 20, "max": 120},
        }
        # Filter unsupported 'choice' mode simply by fixing symbol for now
        random_spec = {
            "fast": random_spec["fast"],
            "slow": random_spec["slow"],
        }
        results = run_hyperparameter_search(
            strategy_factory=factory,
            reader=reader,
            store=bars_store,
            folds=folds,
            mode="random",
            random_spec=random_spec,
            n_trials=n_trials or 10,
            costs_yaml_path=str(costs_yaml or settings.cost_profiles_path),
            out_dir=out_base,
            base_seed=seed,
            parallel_workers=parallel_workers,
        )

    # Summarize
    mean_best = max((r.summary.get("mean_return", 0.0) for r in results), default=0.0)
    typer.echo(json.dumps({"trials": len(results), "best_mean_return": mean_best, "out": str(out_base)}, indent=2))


if __name__ == "__main__":
    app()