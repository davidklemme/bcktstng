from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import List

import os
import tempfile

from quant.data.bars_loader import BarRow
from quant.data.pit_reader import BarsStore, PITDataReader
from quant.data.symbols_repository import create_sqlite_engine, ensure_schema as symbols_ensure_schema
from quant.research.validation import make_walk_forward_folds, make_purged_kfold_folds, run_walk_forward


def _utc(y: int, m: int, d: int) -> datetime:
    return datetime(y, m, d, 16, 0, 0, tzinfo=timezone.utc)


def _make_rows(symbol_id: int, start_dt: date, num_days: int) -> List[BarRow]:
    rows: List[BarRow] = []
    for i in range(num_days):
        ts = _utc(start_dt.year, start_dt.month, start_dt.day) + timedelta(days=i)
        rows.append(
            BarRow(
                ts=ts,
                symbol_id=symbol_id,
                open=100.0 + i,
                high=101.0 + i,
                low=99.0 + i,
                close=100.0 + i,
                volume=1000 + i,
                dt=start_dt + timedelta(days=i),
            )
        )
    return rows


def test_make_walk_forward_folds_no_overlap_embargo() -> None:
    store = BarsStore.from_rows(_make_rows(1, date(2020, 1, 1), 20))
    folds = make_walk_forward_folds(store=store, start=_utc(2020, 1, 1), end=_utc(2020, 1, 20), train_window=8, test_window=4, embargo_fraction=0.1)
    assert len(folds) >= 1
    # Ensure validation ranges do not intersect any training range within same fold
    for f in folds:
        v0, v1 = f.val_range
        for t0, t1 in f.train_ranges:
            assert not (t0 <= v1 and v0 <= t1), "Validation overlaps with training"


def test_make_purged_kfold_folds_purges_around_validation() -> None:
    store = BarsStore.from_rows(_make_rows(1, date(2020, 1, 1), 30))
    embargo_fraction = 0.1
    folds = make_purged_kfold_folds(store=store, start=_utc(2020, 1, 1), end=_utc(2020, 1, 30), n_splits=3, embargo_fraction=embargo_fraction)
    assert len(folds) == 3
    # Check that for each fold there is a gap (embargo) between train and validation on both sides where applicable
    for f in folds:
        (v0, v1) = f.val_range
        for (t0, t1) in f.train_ranges:
            assert t1 < v0 or t0 > v1, "Training ranges should not touch validation (purged)"


def test_run_walk_forward_writes_summary_and_fold_artifacts(tmp_path: Path) -> None:
    # Minimal setup with one symbol and EUR currency to avoid FX
    rows = _make_rows(1, date(2020, 1, 1), 12)
    store = BarsStore.from_rows(rows)

    symbols_engine = create_sqlite_engine()
    symbols_ensure_schema(symbols_engine)
    # Insert a single symbol directly using SQLAlchemy for simplicity
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

    # FX engine can be empty since currency is EUR
    from quant.data.fx_repository import create_engine as create_fx_engine

    fx_engine = create_fx_engine()

    reader = PITDataReader(fx_engine, symbols_engine, store)

    # Build folds (walk-forward)
    folds = make_walk_forward_folds(store=store, start=rows[0].ts, end=rows[-1].ts, train_window=6, test_window=3)

    # Strategy factory uses ma_cross params
    from quant.examples.ma_cross import MACross

    def factory(params: dict):
        return MACross(symbol=params.get("symbol", "AAPL"), fast=3, slow=5)

    out_dir = tmp_path / "wf"
    results = run_walk_forward(
        strategy_factory=factory,
        strategy_params={"symbol": "AAPL"},
        reader=reader,
        store=store,
        folds=folds,
        costs_yaml_path=None,
        out_dir=out_dir,
        base_seed=42,
    )

    # Summary file written
    summary = out_dir / "folds_summary.json"
    assert summary.exists()
    # Per-fold directories with metrics.json
    assert len(results) == len(folds)
    for r in results:
        fold_dir = Path(r["out_dir"])  # recorded path
        metrics = fold_dir / "metrics.json"
        assert metrics.exists()