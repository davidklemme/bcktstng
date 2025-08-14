from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

from sqlalchemy import (
	Table,
	Column,
	MetaData,
	Integer,
	String as SAString,
	Float,
	DateTime,
	insert,
	select,
	and_,
)
from sqlalchemy.engine import Engine


RATIOS_TABLE = "fundamentals_ratios"
INCOME_TABLE = "fundamentals_income"
BALANCE_TABLE = "fundamentals_balance"
CASHFLOW_TABLE = "fundamentals_cashflow"


def _utc_dt(value: str | datetime) -> datetime:
	if isinstance(value, datetime):
		dt = value
	else:
		dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
	if dt.tzinfo is None:
		return dt.replace(tzinfo=timezone.utc)
	return dt.astimezone(timezone.utc)


@dataclass(frozen=True)
class RatioSnapshot:
	symbol_id: int
	asof: datetime
	currency: Optional[str]
	pe: Optional[float] = None
	ev_ebitda: Optional[float] = None
	fcf_yield: Optional[float] = None
	debt_ebitda: Optional[float] = None
	roic: Optional[float] = None
	interest_coverage: Optional[float] = None


@dataclass(frozen=True)
class StatementSnapshot:
	symbol_id: int
	period_end: datetime
	asof: datetime
	currency: Optional[str]
	fields: Dict[str, Optional[float]]


# --- Table definitions ---

def define_ratios_table(metadata: MetaData) -> Table:
	return Table(
		RATIOS_TABLE,
		metadata,
		Column("symbol_id", Integer, nullable=False, index=True),
		Column("asof", DateTime(timezone=True), nullable=False, index=True),
		Column("currency", SAString(8), nullable=True),
		Column("pe", Float, nullable=True),
		Column("ev_ebitda", Float, nullable=True),
		Column("fcf_yield", Float, nullable=True),
		Column("debt_ebitda", Float, nullable=True),
		Column("roic", Float, nullable=True),
		Column("interest_coverage", Float, nullable=True),
	)


def define_income_table(metadata: MetaData) -> Table:
	return Table(
		INCOME_TABLE,
		metadata,
		Column("symbol_id", Integer, nullable=False, index=True),
		Column("period_end", DateTime(timezone=True), nullable=False, index=True),
		Column("asof", DateTime(timezone=True), nullable=False, index=True),
		Column("currency", SAString(8), nullable=True),
		Column("revenue", Float, nullable=True),
		Column("ebitda", Float, nullable=True),
		Column("net_income", Float, nullable=True),
		Column("interest_expense", Float, nullable=True),
	)


def define_balance_table(metadata: MetaData) -> Table:
	return Table(
		BALANCE_TABLE,
		metadata,
		Column("symbol_id", Integer, nullable=False, index=True),
		Column("period_end", DateTime(timezone=True), nullable=False, index=True),
		Column("asof", DateTime(timezone=True), nullable=False, index=True),
		Column("currency", SAString(8), nullable=True),
		Column("total_assets", Float, nullable=True),
		Column("total_liabilities", Float, nullable=True),
		Column("total_equity", Float, nullable=True),
		Column("net_debt", Float, nullable=True),
	)


def define_cashflow_table(metadata: MetaData) -> Table:
	return Table(
		CASHFLOW_TABLE,
		metadata,
		Column("symbol_id", Integer, nullable=False, index=True),
		Column("period_end", DateTime(timezone=True), nullable=False, index=True),
		Column("asof", DateTime(timezone=True), nullable=False, index=True),
		Column("currency", SAString(8), nullable=True),
		Column("operating_cf", Float, nullable=True),
		Column("investing_cf", Float, nullable=True),
		Column("financing_cf", Float, nullable=True),
		Column("free_cash_flow", Float, nullable=True),
	)


def ensure_schema(engine: Engine) -> None:
	metadata = MetaData()
	define_ratios_table(metadata)
	define_income_table(metadata)
	define_balance_table(metadata)
	define_cashflow_table(metadata)
	metadata.create_all(engine)


# --- Writers with PIT guards ---

def upsert_ratios_snapshots(engine: Engine, rows: List[RatioSnapshot]) -> int:
	"""Insert ratio snapshots. Enforces timezone and ordering semantics minimally.
	- asof must be timezone-aware
	"""
	ensure_schema(engine)
	metadata = MetaData()
	table = define_ratios_table(metadata)

	payload: List[dict] = []
	for r in rows:
		asof = _utc_dt(r.asof)
		payload.append({
			"symbol_id": int(r.symbol_id),
			"asof": asof,
			"currency": r.currency,
			"pe": r.pe,
			"ev_ebitda": r.ev_ebitda,
			"fcf_yield": r.fcf_yield,
			"debt_ebitda": r.debt_ebitda,
			"roic": r.roic,
			"interest_coverage": r.interest_coverage,
		})

	with engine.begin() as conn:
		if payload:
			conn.execute(insert(table), payload)
	return len(payload)


def write_statement_snapshots(engine: Engine, table_name: str, rows: List[StatementSnapshot]) -> int:
	"""Write statement snapshots to the specified table, enforcing asof >= period_end."""
	ensure_schema(engine)
	metadata = MetaData()
	if table_name == INCOME_TABLE:
		table = define_income_table(metadata)
	elif table_name == BALANCE_TABLE:
		table = define_balance_table(metadata)
	elif table_name == CASHFLOW_TABLE:
		table = define_cashflow_table(metadata)
	else:
		raise ValueError("Unknown table name for statement snapshots")

	payload: List[dict] = []
	for r in rows:
		period_end = _utc_dt(r.period_end)
		asof = _utc_dt(r.asof)
		if asof < period_end:
			raise ValueError("asof must be on or after period_end for statement snapshots")
		row: Dict[str, Optional[float] | int | datetime | str] = {
			"symbol_id": int(r.symbol_id),
			"period_end": period_end,
			"asof": asof,
			"currency": r.currency,
		}
		# Merge numeric fields safely
		for k, v in r.fields.items():
			row[k] = v
		payload.append(row)

	with engine.begin() as conn:
		if payload:
			conn.execute(insert(table), payload)
	return len(payload)


# --- Readers with PIT guards ---

def get_ratios_asof(engine: Engine, symbol_id: int, asof: datetime) -> RatioSnapshot:
	ensure_schema(engine)
	metadata = MetaData()
	table = define_ratios_table(metadata)
	asof_utc = _utc_dt(asof)
	stmt = (
		select(
			table.c.symbol_id,
			table.c.asof,
			table.c.currency,
			table.c.pe,
			table.c.ev_ebitda,
			table.c.fcf_yield,
			table.c.debt_ebitda,
			table.c.roic,
			table.c.interest_coverage,
		)
		.where(table.c.symbol_id == int(symbol_id))
		.where(table.c.asof <= asof_utc)
		.order_by(table.c.asof.desc())
		.limit(1)
	)
	with engine.begin() as conn:
		row = conn.execute(stmt).fetchone()
	if row is None:
		raise LookupError(f"No ratios snapshot for symbol {symbol_id} as of {asof_utc.isoformat()}")
	return RatioSnapshot(
		symbol_id=int(row.symbol_id),
		asof=_utc_dt(row.asof),
		currency=row.currency,
		pe=_opt_float(row.pe),
		ev_ebitda=_opt_float(row.ev_ebitda),
		fcf_yield=_opt_float(row.fcf_yield),
		debt_ebitda=_opt_float(row.debt_ebitda),
		roic=_opt_float(row.roic),
		interest_coverage=_opt_float(row.interest_coverage),
	)


def _opt_float(v: Optional[float]) -> Optional[float]:
	if v is None:
		return None
	return float(v)