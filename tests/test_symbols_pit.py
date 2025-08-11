from __future__ import annotations

from datetime import datetime, timezone
import os
import tempfile

import pytest

from quant.data.symbols_repository import (
    create_sqlite_engine,
    ensure_schema,
    load_symbols_csv_to_db,
    get_symbols_asof,
)


CSV_HEADER = "symbol_id,ticker,exchange,currency,active_from,active_to\n"


def _write_csv(contents: str) -> str:
    tmpdir = tempfile.mkdtemp()
    path = os.path.join(tmpdir, "symbols.csv")
    with open(path, "w", newline="") as f:
        f.write(CSV_HEADER)
        f.write(contents)
    return path


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def test_get_symbols_asof_filters_correctly() -> None:
    engine = create_sqlite_engine()
    ensure_schema(engine)

    csv_path = _write_csv(
        """
1,AAPL,XNAS,USD,2020-01-01T00:00:00Z,2023-01-01T00:00:00Z
2,MSFT,XNAS,USD,2021-01-01T00:00:00Z,
3,VOD,XLON,GBP,2022-06-01T00:00:00Z,
""".strip()
    )
    n = load_symbols_csv_to_db(csv_path, engine)
    assert n == 3

    # As of mid-2020: only AAPL
    rows = get_symbols_asof(engine, _dt("2020-06-01T00:00:00Z"))
    assert [r.symbol_id for r in rows] == [1]

    # As of 2022-07-01: AAPL, MSFT, VOD
    rows = get_symbols_asof(engine, _dt("2022-07-01T00:00:00Z"))
    assert [r.symbol_id for r in rows] == [1, 2, 3]

    # Boundary: exactly at AAPL active_to -> AAPL should be excluded (active_to exclusive)
    rows = get_symbols_asof(engine, _dt("2023-01-01T00:00:00Z"))
    assert [r.symbol_id for r in rows] == [2, 3]

    # Future: only MSFT and VOD continue
    rows = get_symbols_asof(engine, _dt("2024-01-01T00:00:00Z"))
    assert [r.symbol_id for r in rows] == [2, 3]


def test_csv_missing_columns_errors() -> None:
    engine = create_sqlite_engine()
    ensure_schema(engine)

    tmpdir = tempfile.mkdtemp()
    path = os.path.join(tmpdir, "bad.csv")
    with open(path, "w", newline="") as f:
        f.write("symbol_id,ticker\n1,ABC\n")

    with pytest.raises(ValueError):
        load_symbols_csv_to_db(path, engine)