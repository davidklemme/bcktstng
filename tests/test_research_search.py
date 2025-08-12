from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import List

from quant.data.bars_loader import BarRow
from quant.data.pit_reader import BarsStore, PITDataReader
from quant.data.symbols_repository import create_sqlite_engine
from quant.research.validation import make_walk_forward_folds
from quant.research.search import run_hyperparameter_search


def _utc(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, 16, 0, 0, tzinfo=timezone.utc)


def _make_rows(symbol_id: int, start_dt: date, num_days: int, price_start: float = 100.0, price_step: float = 1.0) -> List[BarRow]:
    rows: List[BarRow] = []
    price = price_start
    for i in range(num_days):
        ts = _utc(start_dt.year, start_dt.month, start_dt.day) + timedelta(days=i)
        rows.append(
            BarRow(
                ts=ts,
                symbol_id=symbol_id,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=1000,
                dt=start_dt + timedelta(days=i),
            )
        )
        price += price_step
    return rows


def test_hyperparameter_search_writes_leaderboard(tmp_path: Path) -> None:
    rows = _make_rows(1, date(2020, 1, 1), 15)
    store = BarsStore.from_rows(rows)

    fx_engine = create_sqlite_engine()  # reuse sqlite creator for convenience
    symbols_engine = create_sqlite_engine()

    # Insert symbol row (EUR to avoid FX)
    from sqlalchemy import MetaData, insert
    from quant.data.symbols_repository import define_symbols_table

    metadata = MetaData()
    table = define_symbols_table(metadata)
    metadata.create_all(symbols_engine)
    with symbols_engine.begin() as conn:
        conn.execute(
            insert(table),
            [
                {
                    "symbol_id": 1,
                    "ticker": "AAPL",
                    "exchange": "XNYS",
                    "currency": "EUR",
                    "active_from": _utc(2019, 1, 1),
                    "active_to": None,
                }
            ],
        )

    reader = PITDataReader(fx_engine, symbols_engine, store)

    folds = make_walk_forward_folds(store=store, start=rows[0].ts, end=rows[-1].ts, train_window=6, test_window=3)

    from quant.examples.ma_cross import MACross

    def factory(params: dict):
        return MACross(symbol=params.get("symbol", "AAPL"), fast=int(params.get("fast", 3)), slow=int(params.get("slow", 5)))

    results = run_hyperparameter_search(
        strategy_factory=factory,
        reader=reader,
        store=store,
        folds=folds,
        mode="grid",
        param_grid={"symbol": ["AAPL"], "fast": [2, 3], "slow": [4, 6]},
        out_dir=tmp_path / "hs",
        base_seed=7,
        parallel_workers=1,
    )

    leaderboard_csv = tmp_path / "hs" / "leaderboard.csv"
    assert leaderboard_csv.exists()
    assert len(results) == 4  # 2x2 grid