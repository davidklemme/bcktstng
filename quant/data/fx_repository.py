from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import (
    Table,
    Column,
    MetaData,
    String as SAString,
    Float,
    DateTime,
    insert,
    select,
    and_,
)
from sqlalchemy.engine import Engine


FX_TABLE_NAME = "fx_rates"


def _utc_dt(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class FxRate:
    ts: datetime
    base_ccy: str
    quote_ccy: str
    rate: float


def define_fx_table(metadata: MetaData) -> Table:
    return Table(
        FX_TABLE_NAME,
        metadata,
        Column("ts", DateTime(timezone=True), nullable=False, index=True),
        Column("base_ccy", SAString(8), nullable=False, index=True),
        Column("quote_ccy", SAString(8), nullable=False, index=True),
        Column("rate", Float, nullable=False),
    )


def ensure_schema(engine: Engine) -> None:
    metadata = MetaData()
    define_fx_table(metadata)
    metadata.create_all(engine)


def load_fx_csv_to_db(csv_path: str, engine: Engine) -> int:
    ensure_schema(engine)
    metadata = MetaData()
    table = define_fx_table(metadata)

    rows: List[dict] = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        required = {"ts", "base_ccy", "quote_ccy", "rate"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")
        for r in reader:
            rows.append(
                {
                    "ts": _utc_dt(r["ts"]),
                    "base_ccy": r["base_ccy"],
                    "quote_ccy": r["quote_ccy"],
                    "rate": float(r["rate"]),
                }
            )

    with engine.begin() as conn:
        if rows:
            conn.execute(insert(table), rows)
    return len(rows)


def get_rate_asof(engine: Engine, base_ccy: str, quote_ccy: str, asof: datetime) -> FxRate:
    ensure_schema(engine)

    metadata = MetaData()
    table = define_fx_table(metadata)

    asof_utc = _utc_dt(asof)
    stmt = (
        select(table.c.ts, table.c.base_ccy, table.c.quote_ccy, table.c.rate)
        .where(and_(table.c.base_ccy == base_ccy, table.c.quote_ccy == quote_ccy))
        .where(table.c.ts <= asof_utc)
        .order_by(table.c.ts.desc())
        .limit(1)
    )

    with engine.begin() as conn:
        row = conn.execute(stmt).fetchone()

    if row is None:
        raise LookupError(f"No FX rate for {base_ccy}/{quote_ccy} as of {asof_utc.isoformat()}")

    return FxRate(ts=_utc_dt(row.ts), base_ccy=row.base_ccy, quote_ccy=row.quote_ccy, rate=float(row.rate))


def create_engine(path: str = ":memory:") -> Engine:
    from sqlalchemy import create_engine as sa_create_engine
    return sa_create_engine(f"sqlite+pysqlite:///{path}", future=True)