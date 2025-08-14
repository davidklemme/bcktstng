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
from ..strategies.simple.random_baseline import RandomBaseline
from ..strategies.simple.index_strategy import IndexStrategy
from ..data.stooq_data_fetcher import StooqDataFetcher
from ..data.fx_rate_fetcher import FxRateFetcher

app = typer.Typer(help="Quant orchestration CLI")


def _load_data_if_needed(symbols_db: Optional[Path], fx_db: Optional[Path], settings):
    """Load data into in-memory databases if needed."""
    symbols_engine = None
    fx_engine = None
    
    # Handle symbols database
    if str(symbols_db or settings.symbols_db_path) == ":memory:":
        from ..data.symbols_repository import load_symbols_csv_to_db
        symbols_engine = create_sqlite_engine(":memory:")
        # Load actual symbols data from comprehensive_symbols.csv
        symbols_csv = Path("quant/data/comprehensive_symbols.csv")
        if symbols_csv.exists():
            load_symbols_csv_to_db(str(symbols_csv), symbols_engine)
    
    # Handle FX database
    if str(fx_db or settings.fx_db_path) == ":memory:":
        from ..data.fx_repository import load_fx_csv_to_db
        fx_engine = create_sqlite_engine(":memory:")
        # Load actual FX data from fx_rates.csv
        fx_csv = Path("quant/data/fx_rates.csv")
        if fx_csv.exists():
            load_fx_csv_to_db(str(fx_csv), fx_engine)
    elif fx_db and fx_db.suffix == '.csv':
        # Load FX data from provided CSV file
        from ..data.fx_repository import load_fx_csv_to_db
        fx_engine = create_sqlite_engine(":memory:")
        load_fx_csv_to_db(str(fx_db), fx_engine)
    
    return symbols_engine, fx_engine


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
    if lname == "randombaseline":
        def factory(params: dict):
            return RandomBaseline(
                position_size=int(params.get("position_size", 100)),
                trade_probability=float(params.get("trade_probability", 0.1)),
                max_positions=int(params.get("max_positions", 5)),
                seed=int(params.get("seed", 42)) if params.get("seed") else None,
            )
        return factory
    if lname == "index":
        def factory(params: dict):
            return IndexStrategy(
                position_size=int(params.get("position_size", 100)),
                rebalance_frequency=int(params.get("rebalance_frequency", 252)),
                equal_weight=bool(params.get("equal_weight", True)),
                max_positions=int(params.get("max_positions", 10)),
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
    starting_cash: float = typer.Option(100000.0, help="Starting cash in base currency"),
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

    # Load data if using in-memory databases
    loaded_symbols_engine, loaded_fx_engine = _load_data_if_needed(symbols_db, fx_db, settings)
    if loaded_symbols_engine is not None:
        symbols_engine = loaded_symbols_engine
    if loaded_fx_engine is not None:
        fx_engine = loaded_fx_engine

    reader = PITDataReader(fx_engine, symbols_engine, bars_store)

    # Strategy
    if strategy_name.lower() == "ma_cross":
        strat = MACross(symbol="AAPL")  # Keep single symbol for backward compatibility
    elif strategy_name.lower() == "bollinger":
        strat = BollingerBands()
    elif strategy_name.lower() == "roc":
        strat = RateOfChange()
    elif strategy_name.lower() == "randombaseline":
        strat = RandomBaseline()
    elif strategy_name.lower() == "index":
        strat = IndexStrategy()
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
        starting_cash=starting_cash,
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


@app.command("fetch-stooq-data")
def fetch_stooq_data(
    symbols_csv: Path = typer.Option(Path("quant/data/comprehensive_symbols.csv"), help="Path to symbols CSV"),
    output_path: Path = typer.Option(Path("quant/data/bars_from_stooq.csv"), help="Output CSV path for bars data"),
    symbols_db: Optional[Path] = typer.Option(Path("data/symbols.db"), help="Path to symbols database"),
    symbol: Optional[str] = typer.Option(None, help="Fetch data for specific symbol only"),
    exchange: Optional[str] = typer.Option(None, help="Exchange code for specific symbol"),
    start_date: Optional[str] = typer.Option(None, help="Start date (YYYY-MM-DD), defaults to 1 year ago"),
    end_date: Optional[str] = typer.Option(None, help="End date (YYYY-MM-DD), defaults to today"),
    delay: float = typer.Option(1.0, help="Delay between requests in seconds"),
    force_refresh: bool = typer.Option(False, help="Force refresh all data (ignore existing)"),
    dry_run: bool = typer.Option(False, help="Show what would be fetched without saving"),
    verbose: bool = typer.Option(False, help="Enable verbose logging with per-symbol details"),
):
    """Fetch historical data from Stooq for symbols."""
    from datetime import datetime, timedelta, timezone
    import logging
    
    # Configure logging level based on verbose flag
    if verbose:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    
    # Initialize fetcher with symbols database
    fetcher = StooqDataFetcher(delay_seconds=delay, symbols_db_path=str(symbols_db) if symbols_db.exists() else None)
    
    # Parse dates
    if start_date:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    else:
        start_dt = datetime.now(timezone.utc) - timedelta(days=365)  # Default to 1 year
    
    if end_date:
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    else:
        end_dt = datetime.now(timezone.utc)
    
    typer.echo(f"Fetching data from {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")
    
    if symbol and exchange:
        # Fetch data for specific symbol
        typer.echo(f"Fetching data for {symbol} ({exchange})")
        
        if dry_run:
            typer.echo(f"DRY RUN - Would fetch data for {symbol} from Stooq")
            return
        
        if force_refresh:
            data_points = fetcher.fetch_symbol_data(symbol, exchange, start_dt, end_dt)
        else:
            data_points = fetcher.fetch_missing_data(symbol, exchange, output_path, start_dt, end_dt)
        
        if data_points:
            # Get symbol ID if available
            symbol_id = fetcher.get_symbol_id(symbol, exchange, datetime.now(timezone.utc))
            fetcher.save_data_to_csv(data_points, symbol, exchange, output_path, symbol_id)
            typer.echo(f"Saved {len(data_points)} data points for {symbol}")
        else:
            typer.echo(f"No data found for {symbol}")
    
    else:
        # Fetch data for all symbols in CSV
        if not symbols_csv.exists():
            typer.echo(f"Symbols file {symbols_csv} does not exist")
            raise typer.Exit(code=1)
        
        symbols = fetcher.load_symbols_from_csv(symbols_csv)
        
        if not symbols:
            typer.echo("No symbols found in CSV file")
            raise typer.Exit(code=1)
        
        typer.echo(f"Found {len(symbols)} symbols to process")
        
        if dry_run:
            typer.echo("DRY RUN - Would fetch data for the following symbols:")
            for ticker, exch in symbols[:10]:  # Show first 10
                typer.echo(f"  {ticker} ({exch})")
            if len(symbols) > 10:
                typer.echo(f"  ... and {len(symbols) - 10} more")
            return
        
        # Show existing data summary
        summary = fetcher.get_data_summary(output_path)
        if summary:
            typer.echo(f"\nExisting data summary:")
            for sym, info in list(summary.items())[:5]:  # Show first 5
                typer.echo(f"  {sym}: {info['data_points']} points ({info['first_date']} to {info['last_date']})")
            if len(summary) > 5:
                typer.echo(f"  ... and {len(summary) - 5} more symbols")
        
        # Fetch data
        results = fetcher.fetch_symbols_data(symbols, output_path, start_dt, end_dt, force_refresh)
        
        # Show results
        typer.echo(f"\nFetch results:")
        total_points = 0
        for sym, count in results.items():
            if count > 0:
                typer.echo(f"  {sym}: {count} data points")
                total_points += count
        
        typer.echo(f"\nTotal: {total_points} data points saved to {output_path}")


@app.command("stooq-data-summary")
def stooq_data_summary(
    data_path: Path = typer.Option(Path("quant/data/bars_from_stooq.csv"), help="Path to Stooq data CSV"),
):
    """Show summary of existing Stooq data."""
    fetcher = StooqDataFetcher()
    
    if not data_path.exists():
        typer.echo(f"Data file {data_path} does not exist")
        raise typer.Exit(code=1)
    
    summary = fetcher.get_data_summary(data_path)
    
    if not summary:
        typer.echo("No data found in file")
        return
    
    typer.echo(f"Data summary for {data_path}:")
    typer.echo("-" * 80)
    
    # Sort by number of data points
    sorted_summary = sorted(summary.items(), key=lambda x: x[1]['data_points'], reverse=True)
    
    for symbol, info in sorted_summary:
        first_date = info['first_date'].strftime('%Y-%m-%d') if info['first_date'] else 'N/A'
        last_date = info['last_date'].strftime('%Y-%m-%d') if info['last_date'] else 'N/A'
        
        typer.echo(f"{symbol:<10} | {info['exchange']:<8} | {info['data_points']:>6} points | {first_date} to {last_date}")


@app.command("check-missing-data")
def check_missing_data(
    symbols_csv: Path = typer.Option(Path("quant/data/comprehensive_symbols.csv"), help="Path to symbols CSV"),
    data_path: Path = typer.Option(Path("quant/data/bars_from_stooq.csv"), help="Path to Stooq data CSV"),
    start_date: Optional[str] = typer.Option(None, help="Start date (YYYY-MM-DD), defaults to 1 year ago"),
    end_date: Optional[str] = typer.Option(None, help="End date (YYYY-MM-DD), defaults to today"),
):
    """Check which symbols are missing data."""
    from datetime import datetime, timedelta, timezone
    
    fetcher = StooqDataFetcher()
    
    # Parse dates
    if start_date:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    else:
        start_dt = datetime.now(timezone.utc) - timedelta(days=365)
    
    if end_date:
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    else:
        end_dt = datetime.now(timezone.utc)
    
    typer.echo(f"Checking missing data from {start_dt.strftime('%Y-%m-%d')} to {end_dt.strftime('%Y-%m-%d')}")
    
    if not symbols_csv.exists():
        typer.echo(f"Symbols file {symbols_csv} does not exist")
        raise typer.Exit(code=1)
    
    symbols = fetcher.load_symbols_from_csv(symbols_csv)
    
    if not symbols:
        typer.echo("No symbols found in CSV file")
        raise typer.Exit(code=1)
    
    typer.echo(f"\nChecking {len(symbols)} symbols...")
    
    missing_symbols = []
    existing_symbols = []
    
    for symbol, exchange in symbols:
        existing_dates = fetcher.get_existing_data_dates(data_path, symbol)
        
        # Check if we have data for the period
        has_data = False
        for date in existing_dates:
            if start_dt.date() <= date <= end_dt.date():
                has_data = True
                break
        
        if has_data:
            existing_symbols.append((symbol, exchange))
        else:
            missing_symbols.append((symbol, exchange))
    
    typer.echo(f"\nResults:")
    typer.echo(f"  Symbols with data: {len(existing_symbols)}")
    typer.echo(f"  Symbols missing data: {len(missing_symbols)}")
    
    if missing_symbols:
        typer.echo(f"\nMissing symbols:")
        for symbol, exchange in missing_symbols[:20]:  # Show first 20
            typer.echo(f"  {symbol} ({exchange})")
        if len(missing_symbols) > 20:
            typer.echo(f"  ... and {len(missing_symbols) - 20} more")
    
    if existing_symbols:
        typer.echo(f"\nSymbols with data:")
        for symbol, exchange in existing_symbols[:10]:  # Show first 10
            typer.echo(f"  {symbol} ({exchange})")
        if len(existing_symbols) > 10:
            typer.echo(f"  ... and {len(existing_symbols) - 10} more")


@app.command("fetch-fx-rates")
def fetch_fx_rates(
    output_path: Path = typer.Option(Path("quant/data/fx_rates.csv"), help="Output CSV file path"),
    start_date: str = typer.Option("1990-01-01", help="Start date (YYYY-MM-DD), defaults to match oldest symbol data"),
    end_date: str = typer.Option("2024-12-31", help="End date (YYYY-MM-DD)"),
    delay: float = typer.Option(1.0, help="Delay between requests in seconds"),
    verbose: bool = typer.Option(False, help="Enable verbose logging"),
    auto_range: bool = typer.Option(False, help="Automatically determine date range from existing symbol data"),
):
    """Fetch historical FX rates from Stooq and save to CSV."""
    from datetime import datetime, timezone
    import logging
    
    # Configure logging
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    # Parse dates
    if auto_range:
        # Automatically determine date range from existing symbol data
        bars_data_path = Path("quant/data/bars_from_stooq.csv")
        if bars_data_path.exists():
            fetcher = StooqDataFetcher()
            summary = fetcher.get_data_summary(bars_data_path)
            if summary:
                # Find the earliest and latest dates across all symbols
                earliest_date = None
                latest_date = None
                for symbol_info in summary.values():
                    if symbol_info['first_date']:
                        if earliest_date is None or symbol_info['first_date'] < earliest_date:
                            earliest_date = symbol_info['first_date']
                    if symbol_info['last_date']:
                        if latest_date is None or symbol_info['last_date'] > latest_date:
                            latest_date = symbol_info['last_date']
                
                if earliest_date and latest_date:
                    start_dt = earliest_date
                    end_dt = latest_date
                    typer.echo(f"Auto-determined date range from symbol data: {earliest_date.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')}")
                else:
                    start_dt = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    typer.echo(f"Could not determine date range from symbol data, using defaults: {start_date} to {end_date}")
            else:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                typer.echo(f"No symbol data found, using defaults: {start_date} to {end_date}")
        else:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            typer.echo(f"No symbol data file found, using defaults: {start_date} to {end_date}")
    else:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        typer.echo(f"Fetching FX rates from {start_date} to {end_date}")
    
    typer.echo(f"Output: {output_path}")
    
    # Fetch FX rates
    fetcher = FxRateFetcher(delay_seconds=delay)
    result = fetcher.fetch_major_currencies(
        output_path=output_path,
        start_date=start_dt,
        end_date=end_dt
    )
    
    typer.echo(f"\nFX rates saved to: {output_path}")
    typer.echo(f"Total data points: {result['total_points']}")
    typer.echo(f"Successful pairs: {result['successful']}")
    typer.echo(f"Failed pairs: {result['failed']}")


@app.command("visualize")
def visualize_cmd(
	run_dirs: list[Path] = typer.Argument(..., help="Run directory(ies) to visualize"),
	width: int = typer.Option(None, help="Width of ASCII chart in characters (auto-detected if not specified)"),
	height: int = typer.Option(None, help="Height of ASCII chart in characters (auto-detected if not specified)"),
):
	"""Generate ASCII art visualization for run directory(ies). Single run or comparison based on number of directories."""
	if len(run_dirs) == 1:
		from ..ops.visualize import visualize_run_ascii
		ascii_chart = visualize_run_ascii(str(run_dirs[0]), width, height)
	else:
		from ..ops.visualize import visualize_runs_comparison
		ascii_chart = visualize_runs_comparison([str(d) for d in run_dirs], width, height)
	typer.echo(ascii_chart)


if __name__ == "__main__":
    app()