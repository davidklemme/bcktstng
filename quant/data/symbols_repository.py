from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Sequence

from sqlalchemy import (
    Table,
    Column,
    MetaData,
    Integer,
    String as SAString,
    DateTime,
    create_engine,
    select,
    insert,
)
from sqlalchemy.engine import Engine


SYMBOLS_TABLE_NAME = "symbols"


def _utc_dt(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        # Expect ISO 8601
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class SymbolRow:
    symbol_id: int
    ticker: str
    exchange: str
    currency: str
    active_from: datetime
    active_to: Optional[datetime]


def define_symbols_table(metadata: MetaData) -> Table:
    return Table(
        SYMBOLS_TABLE_NAME,
        metadata,
        Column("symbol_id", Integer, primary_key=True, nullable=False),
        Column("ticker", SAString(32), nullable=False),
        Column("exchange", SAString(16), nullable=False),
        Column("currency", SAString(8), nullable=False),
        Column("active_from", DateTime(timezone=True), nullable=False),
        Column("active_to", DateTime(timezone=True), nullable=True),
        sqlite_autoincrement=False,
    )


def ensure_schema(engine: Engine) -> None:
    metadata = MetaData()
    define_symbols_table(metadata)
    metadata.create_all(engine)


def load_symbols_csv_to_db(csv_path: str, engine: Engine) -> int:
    ensure_schema(engine)
    metadata = MetaData()
    table = define_symbols_table(metadata)

    rows: List[dict] = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        required = {"symbol_id", "ticker", "exchange", "currency", "active_from", "active_to"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")
        for r in reader:
            rows.append(
                {
                    "symbol_id": int(r["symbol_id"]),
                    "ticker": r["ticker"],
                    "exchange": r["exchange"],
                    "currency": r["currency"],
                    "active_from": _utc_dt(r["active_from"]),
                    "active_to": None if not r["active_to"] else _utc_dt(r["active_to"]),
                }
            )

    with engine.begin() as conn:
        if rows:
            conn.execute(insert(table), rows)
    return len(rows)


def get_symbols_asof(engine: Engine, asof: datetime) -> List[SymbolRow]:
    ensure_schema(engine)
    asof_utc = _utc_dt(asof)

    metadata = MetaData()
    table = define_symbols_table(metadata)

    # active_from <= asof < active_to (if active_to not null) else active_to is open-ended
    stmt = (
        select(
            table.c.symbol_id,
            table.c.ticker,
            table.c.exchange,
            table.c.currency,
            table.c.active_from,
            table.c.active_to,
        )
        .where(table.c.active_from <= asof_utc)
        .where((table.c.active_to.is_(None)) | (table.c.active_to > asof_utc))
        .order_by(table.c.symbol_id.asc())
    )

    with engine.begin() as conn:
        result = conn.execute(stmt)
        rows = result.fetchall()

    out: List[SymbolRow] = []
    for r in rows:
        out.append(
            SymbolRow(
                symbol_id=int(r.symbol_id),
                ticker=str(r.ticker),
                exchange=str(r.exchange),
                currency=str(r.currency),
                active_from=_utc_dt(r.active_from),
                active_to=None if r.active_to is None else _utc_dt(r.active_to),
            )
        )
    return out


def create_sqlite_engine(path: str = ":memory:") -> Engine:
    return create_engine(f"sqlite+pysqlite:///{path}", future=True)