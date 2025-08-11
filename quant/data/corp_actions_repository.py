from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import (
    Table,
    Column,
    MetaData,
    Integer,
    Float,
    String as SAString,
    DateTime,
    insert,
    select,
)
from sqlalchemy.engine import Engine


CORP_ACTIONS_TABLE_NAME = "corporate_actions"


def _utc_dt(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class CorporateAction:
    symbol_id: int
    effective_date: datetime
    split_ratio: float  # 1.0 if none; 2.0 for 2:1
    dividend: float     # 0.0 if none; cash per share in local currency
    currency: str


def define_corp_actions_table(metadata: MetaData) -> Table:
    return Table(
        CORP_ACTIONS_TABLE_NAME,
        metadata,
        Column("symbol_id", Integer, nullable=False, index=True),
        Column("effective_date", DateTime(timezone=True), nullable=False, index=True),
        Column("split_ratio", Float, nullable=False, default=1.0),
        Column("dividend", Float, nullable=False, default=0.0),
        Column("currency", SAString(8), nullable=False),
    )


def ensure_schema(engine: Engine) -> None:
    metadata = MetaData()
    define_corp_actions_table(metadata)
    metadata.create_all(engine)


def load_corp_actions_csv_to_db(csv_path: str, engine: Engine) -> int:
    ensure_schema(engine)
    metadata = MetaData()
    table = define_corp_actions_table(metadata)

    rows: List[dict] = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        required = {"symbol_id", "effective_date", "split_ratio", "dividend", "currency"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")
        for r in reader:
            rows.append(
                {
                    "symbol_id": int(r["symbol_id"]),
                    "effective_date": _utc_dt(r["effective_date"]),
                    "split_ratio": float(r["split_ratio"]) if r["split_ratio"] else 1.0,
                    "dividend": float(r["dividend"]) if r["dividend"] else 0.0,
                    "currency": r["currency"],
                }
            )

    with engine.begin() as conn:
        if rows:
            conn.execute(insert(table), rows)
    return len(rows)


def get_actions_for_symbol(engine: Engine, symbol_id: int) -> List[CorporateAction]:
    ensure_schema(engine)

    metadata = MetaData()
    table = define_corp_actions_table(metadata)
    stmt = (
        select(
            table.c.symbol_id,
            table.c.effective_date,
            table.c.split_ratio,
            table.c.dividend,
            table.c.currency,
        )
        .where(table.c.symbol_id == symbol_id)
        .order_by(table.c.effective_date.asc())
    )

    with engine.begin() as conn:
        rows = conn.execute(stmt).fetchall()

    out: List[CorporateAction] = []
    for r in rows:
        out.append(
            CorporateAction(
                symbol_id=int(r.symbol_id),
                effective_date=_utc_dt(r.effective_date),
                split_ratio=float(r.split_ratio or 1.0),
                dividend=float(r.dividend or 0.0),
                currency=str(r.currency),
            )
        )
    return out


def create_engine(path: str = ":memory:") -> Engine:
    from sqlalchemy import create_engine as sa_create_engine

    return sa_create_engine(f"sqlite+pysqlite:///{path}", future=True)