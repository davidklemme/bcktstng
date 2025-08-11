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