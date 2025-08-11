from __future__ import annotations

from datetime import datetime, timezone
import os
import tempfile

import pytest

from quant.data.corp_actions_repository import (
    create_engine as _create_engine,  # alias not used, compatibility placeholder
)
from sqlalchemy import create_engine

from quant.data.corp_actions_repository import (
    ensure_schema,
    load_corp_actions_csv_to_db,
    get_actions_for_symbol,
    CorporateAction,
)
from quant.data.adjusters import apply_actions


CSV_HEADER = "symbol_id,effective_date,split_ratio,dividend,currency\n"


def _write_csv(contents: str) -> str:
    tmpdir = tempfile.mkdtemp()
    path = os.path.join(tmpdir, "corp_actions.csv")
    with open(path, "w", newline="") as f:
        f.write(CSV_HEADER)
        f.write(contents)
    return path


def test_split_adjustment_2_for_1() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    ensure_schema(engine)

    csv_path = _write_csv(
        """
1,2021-06-01T00:00:00Z,2.0,0,USD
""".strip()
    )
    n = load_corp_actions_csv_to_db(csv_path, engine)
    assert n == 1

    actions = get_actions_for_symbol(engine, 1)
    assert len(actions) == 1

    before_price, before_qty = 100.0, 10.0
    after = apply_actions(before_price, before_qty, actions)

    # For 2:1 split, halve price, double qty
    assert after.price == pytest.approx(50.0)
    assert after.qty == pytest.approx(20.0)


def test_dividend_cashflow_record_present() -> None:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    ensure_schema(engine)

    csv_path = _write_csv(
        """
1,2021-07-01T00:00:00Z,1.0,0.5,USD
""".strip()
    )
    n = load_corp_actions_csv_to_db(csv_path, engine)
    assert n == 1

    actions = get_actions_for_symbol(engine, 1)
    assert len(actions) == 1
    a = actions[0]
    assert a.dividend == pytest.approx(0.5)
    assert a.currency == "USD"