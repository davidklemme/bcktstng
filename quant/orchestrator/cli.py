from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer

from ..data.bars_loader import load_daily_bars_csv
from ..data.pit_reader import PITDataReader, BarsStore
from ..data.symbols_repository import create_sqlite_engine
from ..data.market_data_fetcher import MarketDataFetcher
from ..ops.artifacts import ArtifactWriter
from .backtest import run_backtest
from .config import get_settings
from ..examples.ma_cross import MACross
from ..research.validation import make_walk_forward_folds, make_purged_kfold_folds, run_walk_forward
from ..research.search import run_hyperparameter_search
from ..strategies.simple.bollinger import BollingerBands
from ..strategies.simple.roc import RateOfChange

app = typer.Typer(help="Quant orchestration CLI")


def _load_dummy_data_if_needed(symbols_db: Optional[Path], fx_db: Optional[Path], settings):
    """Load dummy data into in-memory databases if needed."""
    if str(symbols_db or settings.symbols_db_path) == ":memory:":
        from ..data.symbols_repository import load_symbols_csv_to_db
        from ..data.fx_repository import load_fx_csv_to_db
        dummy_data_dir = Path("quant/data/dummy")
        if dummy_data_dir.exists():
            symbols_engine = create_sqlite_engine(":memory:")
            fx_engine = create_sqlite_engine(":memory:")
            load_symbols_csv_to_db(str(dummy_data_dir / "symbols.csv"), symbols_engine)
            load_fx_csv_to_db(str(dummy_data_dir / "fx.csv"), fx_engine)
            return symbols_engine, fx_engine
    return None, None


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
    if lname == "bollinger":
        def factory(params: dict):
            return BollingerBands(
                window=int(params.get("window", 20)),
                num_std=float(params.get("num_std", 2.0)),
                position_size=int(params.get("position_size", 100)),
            )
        return factory
    if lname == "roc":
        def factory(params: dict):
            return RateOfChange(
                window=int(params.get("window", 10)),
                upper=float(params.get("upper", 0.02)),
                lower=float(params.get("lower", -0.02)),
                position_size=int(params.get("position_size", 100)),
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

    # Load dummy data if using in-memory databases
    dummy_symbols_engine, dummy_fx_engine = _load_dummy_data_if_needed(symbols_db, fx_db, settings)
    if dummy_symbols_engine is not None:
        symbols_engine = dummy_symbols_engine
        fx_engine = dummy_fx_engine

    reader = PITDataReader(fx_engine, symbols_engine, bars_store)

    # Strategy
    if strategy_name.lower() == "ma_cross":
        strat = MACross(symbol="AAPL")  # Keep single symbol for backward compatibility
    elif strategy_name.lower() == "bollinger":
        strat = BollingerBands()
    elif strategy_name.lower() == "roc":
        strat = RateOfChange()
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

    typer.echo(f"Backtest completed. Run ID: {result.run_id}")
    typer.echo(f"Results written to: {out_dir or Path(settings.runs_dir) / 'cli'}")


@app.command("run-signal")
def run_signal_cmd(
    strategy_name: str = typer.Argument("ma_cross", help="Strategy to run"),
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

    out_base = Path(out_dir or Path(settings.runs_dir) / "cli" / "walk_forward")

    # For multi-symbol strategies, we don't need symbol-specific params
    strategy_params = {}
    if strategy_name.lower() == "ma_cross":
        strategy_params = {"symbol": "AAPL"}  # Keep for backward compatibility

    results = run_walk_forward(
        strategy_factory=factory,
        strategy_params=strategy_params,
        reader=reader,
        store=bars_store,
        folds=folds,
        costs_yaml_path=str(costs_yaml or settings.cost_profiles_path),
        out_dir=out_base,
        base_seed=seed,
    )

    typer.echo(f"Walk-forward completed. Results written to: {out_base}")


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
        
        # For multi-symbol strategies, we don't include symbol in the grid
        if strategy_name.lower() in ["bollinger", "roc"]:
            grid = {
                "fast": [int(x) for x in fast_values.split(",")],
                "slow": [int(x) for x in slow_values.split(",")],
            }
        else:
            # Keep symbol for backward compatibility with ma_cross
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
        if strategy_name.lower() in ["bollinger", "roc"]:
            random_spec = {
                "fast": {"mode": "int", "min": 5, "max": 30},
                "slow": {"mode": "int", "min": 20, "max": 120},
            }
        else:
            # Keep symbol for backward compatibility with ma_cross
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

    typer.echo(f"Hyperparameter search completed. Results written to: {out_base}")


@app.command("list-markets")
def list_markets():
    """List all available major markets with their exchange codes."""
    fetcher = MarketDataFetcher()
    markets = fetcher.get_major_markets()
    
    typer.echo("Available major markets:")
    typer.echo("-" * 60)
    for code, name in markets.items():
        currency = fetcher.get_currency_for_exchange(code)
        typer.echo(f"{code:<8} | {name:<35} | {currency}")


@app.command("fetch-symbols")
def fetch_symbols(
    markets: str = typer.Option("XNAS,XNYS,XLON,XTOK", help="Comma-separated list of exchange codes"),
    output_path: Path = typer.Option(Path("quant/data/comprehensive_symbols.csv"), help="Output CSV path"),
    dry_run: bool = typer.Option(False, help="Show what would be fetched without saving"),
    include_historic: bool = typer.Option(True, help="Include historic symbols"),
    compare_existing: Optional[Path] = typer.Option(None, help="Compare with existing CSV file"),
):
    """Fetch symbols from major markets and save to CSV."""
    fetcher = MarketDataFetcher()
    
    # Parse markets
    market_list = [m.strip() for m in markets.split(",")]
    
    typer.echo(f"Fetching symbols from markets: {', '.join(market_list)}")
    
    # Fetch symbols
    symbols = fetcher.fetch_all_markets(market_list)
    
    if dry_run:
        typer.echo(f"\nDRY RUN - Would fetch {len(symbols)} symbols:")
        typer.echo("-" * 60)
        
        # Group by exchange
        by_exchange = {}
        for symbol in symbols:
            if symbol.exchange not in by_exchange:
                by_exchange[symbol.exchange] = []
            by_exchange[symbol.exchange].append(symbol)
        
        for exchange, exchange_symbols in by_exchange.items():
            exchange_name = fetcher.get_major_markets().get(exchange, exchange)
            typer.echo(f"\n{exchange_name} ({exchange}):")
            for symbol in exchange_symbols[:10]:  # Show first 10
                typer.echo(f"  {symbol.ticker:<8} | {symbol.currency}")
            if len(exchange_symbols) > 10:
                typer.echo(f"  ... and {len(exchange_symbols) - 10} more")
        
        typer.echo(f"\nTotal: {len(symbols)} symbols across {len(by_exchange)} exchanges")
        return
    
    # Compare with existing if requested
    if compare_existing and compare_existing.exists():
        added, removed, unchanged = fetcher.compare_with_existing(symbols, compare_existing)
        typer.echo(f"\nComparison with {compare_existing}:")
        typer.echo(f"  Added: {len(added)} symbols")
        typer.echo(f"  Removed: {len(removed)} symbols")
        typer.echo(f"  Unchanged: {len(unchanged)} symbols")
        
        if added:
            typer.echo("\nNew symbols:")
            for symbol in added[:10]:
                typer.echo(f"  {symbol.ticker} ({symbol.exchange})")
            if len(added) > 10:
                typer.echo(f"  ... and {len(added) - 10} more")
    
    # Save to CSV
    fetcher.save_symbols_to_csv(symbols, output_path, include_historic)
    typer.echo(f"\nSaved {len(symbols)} symbols to {output_path}")


@app.command("update-symbols")
def update_symbols(
    symbols_csv: Path = typer.Option(Path("quant/data/comprehensive_symbols.csv"), help="Path to symbols CSV"),
    markets: str = typer.Option("XNAS,XNYS,XLON,XTOK", help="Comma-separated list of exchange codes"),
    dry_run: bool = typer.Option(False, help="Show changes without updating"),
    backup: bool = typer.Option(True, help="Create backup before updating"),
):
    """Update existing symbols CSV with fresh data from markets."""
    fetcher = MarketDataFetcher()
    
    if not symbols_csv.exists():
        typer.echo(f"Symbols file {symbols_csv} does not exist. Use fetch-symbols instead.")
        raise typer.Exit(code=1)
    
    # Parse markets
    market_list = [m.strip() for m in markets.split(",")]
    
    typer.echo(f"Updating symbols from markets: {', '.join(market_list)}")
    
    # Fetch new symbols
    new_symbols = fetcher.fetch_all_markets(market_list)
    
    # Compare with existing
    added, removed, unchanged = fetcher.compare_with_existing(new_symbols, symbols_csv)
    
    typer.echo(f"\nChanges detected:")
    typer.echo(f"  Added: {len(added)} symbols")
    typer.echo(f"  Removed: {len(removed)} symbols")
    typer.echo(f"  Unchanged: {len(unchanged)} symbols")
    
    if added:
        typer.echo("\nNew symbols to add:")
        for symbol in added[:10]:
            typer.echo(f"  {symbol.ticker} ({symbol.exchange})")
        if len(added) > 10:
            typer.echo(f"  ... and {len(added) - 10} more")
    
    if removed:
        typer.echo("\nSymbols to remove:")
        for ticker, exchange in removed[:10]:
            typer.echo(f"  {ticker} ({exchange})")
        if len(removed) > 10:
            typer.echo(f"  ... and {len(removed) - 10} more")
    
    if dry_run:
        typer.echo("\nDRY RUN - No changes made")
        return
    
    # Create backup if requested
    if backup:
        backup_path = symbols_csv.with_suffix(f".backup.{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        import shutil
        shutil.copy2(symbols_csv, backup_path)
        typer.echo(f"Created backup: {backup_path}")
    
    # Save updated symbols
    fetcher.save_symbols_to_csv(new_symbols, symbols_csv)
    typer.echo(f"\nUpdated {symbols_csv} with {len(new_symbols)} symbols")


if __name__ == "__main__":
    app()