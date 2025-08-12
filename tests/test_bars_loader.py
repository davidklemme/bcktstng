from __future__ import annotations

from datetime import datetime, timezone, date
import os
import tempfile

import pytest

from quant.data.bars_loader import load_daily_bars_csv


CSV_HEADER = "dt,symbol_id,open,high,low,close,volume\n"


def _write_csv(contents: str) -> str:
    tmpdir = tempfile.mkdtemp()
    path = os.path.join(tmpdir, "bars.csv")
    with open(path, "w", newline="") as f:
        f.write(CSV_HEADER)
        f.write(contents)
    return path


def test_normalizes_to_utc_close_and_flags_gaps_and_nans() -> None:
    # Choose XNYS in a week with no holidays: June 3-7, 2024
    csv_path = _write_csv(
        """
2024-06-03,1,10,11,9,10.5,1000
2024-06-04,1,10.5,11,10,10.8,1100
2024-06-06,1,10.8,11.2,10.7,11.0,1200
2024-06-07,1,,11.3,10.9,11.1,1300
""".strip()
    )

    rows, validation = load_daily_bars_csv(csv_path, exchange="XNYS")

    # Expect 4 rows, with ts being UTC close times
    assert len(rows) == 4
    for r in rows:
        assert r.ts.tzinfo is not None and r.ts.tzinfo.utcoffset(r.ts) == timezone.utc.utcoffset(r.ts)

    # One missing session (2024-06-05) should be flagged as a gap
    assert date(2024, 6, 5) in validation.missing_dates

    # One NaN row (missing open) at index 3 in input
    assert 3 in validation.nan_row_indices


def test_minute_bars_excludes_out_of_session_and_converts_to_utc() -> None:
    # XNYS: local open 09:30, close 16:00. Provide rows at 09:15 (OOS), 09:30 (in), 16:05 (OOS)
    header = "ts_local,symbol_id,open,high,low,close,volume\n"
    contents = """
2024-06-03 09:15,1,10,10.1,9.9,10.05,100
2024-06-03 09:30,1,10.05,10.2,10.0,10.1,200
2024-06-03 16:05,1,10.1,10.1,10.0,10.05,50
""".strip()

    tmpdir = tempfile.mkdtemp()
    path = os.path.join(tmpdir, "minute.csv")
    with open(path, "w", newline="") as f:
        f.write(header)
        f.write(contents)

    from quant.data.bars_loader import load_minute_bars_csv

    rows, validation = load_minute_bars_csv(path, exchange="XNYS")
    # Expect only the 09:30 row included
    assert len(rows) == 1
    r = rows[0]
    assert r.symbol_id == 1
    # Ensure UTC timezone
    assert r.ts.tzinfo is not None and r.ts.tzinfo.utcoffset(r.ts) == timezone.utc.utcoffset(r.ts)
    # Validation currently tracks only NaNs for minutes; should be empty here
    assert validation.nan_row_indices == []