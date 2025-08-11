from __future__ import annotations

from datetime import datetime, timezone
import os
import tempfile

import pytest
from sqlalchemy import create_engine

from quant.data.fx_repository import ensure_schema, load_fx_csv_to_db, get_rate_asof


CSV_HEADER = "ts,base_ccy,quote_ccy,rate\n"


def _write_csv(contents: str) -> str:
    tmpdir = tempfile.mkdtemp()
    path = os.path.join(tmpdir, "fx.csv")
    with open(path, "w", newline="") as f:
        f.write(CSV_HEADER)
        f.write(contents)
    return path


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def test_fx_ingestion_and_weekend_handling() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    ensure_schema(engine)

    csv_path = _write_csv(
        """
2024-05-31T00:00:00Z,USD,EUR,0.92
2024-06-01T00:00:00Z,USD,EUR,0.921
2024-06-03T00:00:00Z,USD,EUR,0.93
""".strip()
    )
    n = load_fx_csv_to_db(csv_path, engine)
    assert n == 3

    # Query on weekend date (2024-06-02 is Sunday): should return last available (2024-06-01)
    rate = get_rate_asof(engine, "USD", "EUR", _dt("2024-06-02T12:00:00Z"))
    assert rate.rate == pytest.approx(0.921)

    # Query on Monday after weekend
    rate2 = get_rate_asof(engine, "USD", "EUR", _dt("2024-06-03T12:00:00Z"))
    assert rate2.rate == pytest.approx(0.93)