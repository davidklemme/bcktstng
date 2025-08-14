from __future__ import annotations

from datetime import date, datetime, timezone, timedelta
from pathlib import Path

import pytest

from quant.data.schemas import (
	INCOME_STATEMENT_SCHEMA,
	BALANCE_SHEET_SCHEMA,
	CASHFLOW_STATEMENT_SCHEMA,
	FUNDAMENTAL_RATIOS_SCHEMA,
	Schema,
)
from quant.discovery.fundamentals_repository import (
	RatioSnapshot,
	StatementSnapshot,
	upsert_ratios_snapshots,
	write_statement_snapshots,
	get_ratios_asof,
	INCOME_TABLE,
	BALANCE_TABLE,
	CASHFLOW_TABLE,
)
from quant.data.symbols_repository import create_sqlite_engine, ensure_schema as symbols_ensure_schema, define_symbols_table
from sqlalchemy import MetaData, insert
from quant.data.bars_loader import BarRow
from quant.data.pit_reader import BarsStore
from quant.discovery.sector_stats import RatioPoint, compute_sector_stats
from quant.discovery.screener import UniverseFilters, filter_universe, rank_candidates, Candidate


def _utc(y: int, m: int, d: int, h: int = 16) -> datetime:
	return datetime(y, m, d, h, 0, 0, tzinfo=timezone.utc)


def test_fundamentals_schemas_instantiation() -> None:
	assert isinstance(INCOME_STATEMENT_SCHEMA, Schema)
	assert isinstance(BALANCE_SHEET_SCHEMA, Schema)
	assert isinstance(CASHFLOW_STATEMENT_SCHEMA, Schema)
	assert isinstance(FUNDAMENTAL_RATIOS_SCHEMA, Schema)
	# Required fields presence
	income_fields = set(INCOME_STATEMENT_SCHEMA.field_names())
	assert {"symbol_id", "period_end", "asof"}.issubset(income_fields)
	ratio_fields = set(FUNDAMENTAL_RATIOS_SCHEMA.field_names())
	assert {"symbol_id", "asof"}.issubset(ratio_fields)


def test_snapshot_writer_enforces_asof_guard(tmp_path: Path) -> None:
	engine = create_sqlite_engine(str(tmp_path / "funds.db"))
	# Valid write
	ok = write_statement_snapshots(
		engine,
		INCOME_TABLE,
		[
			StatementSnapshot(
				symbol_id=1,
				period_end=_utc(2024, 3, 31),
				asof=_utc(2024, 5, 15),
				currency="USD",
				fields={"revenue": 100.0, "net_income": 10.0},
			)
		]
	)
	assert ok == 1
	# Invalid write should raise
	with pytest.raises(ValueError):
		write_statement_snapshots(
			engine,
			BALANCE_TABLE,
			[
				StatementSnapshot(
					symbol_id=1,
					period_end=_utc(2024, 3, 31),
					asof=_utc(2024, 3, 1),
					currency="USD",
					fields={"total_assets": 1000.0},
				)
			]
		)


def test_ratios_pit_reader(tmp_path: Path) -> None:
	engine = create_sqlite_engine(str(tmp_path / "funds.db"))
	# Insert two snapshots and ensure PIT retrieves the latest <= asof
	upsert_ratios_snapshots(
		engine,
		[
			RatioSnapshot(symbol_id=1, asof=_utc(2024, 1, 15), currency="USD", pe=20.0, fcf_yield=0.05, roic=0.1),
			RatioSnapshot(symbol_id=1, asof=_utc(2024, 4, 10), currency="USD", pe=18.0, fcf_yield=0.06, roic=0.12),
		]
	)
	got = get_ratios_asof(engine, 1, _utc(2024, 4, 20))
	assert got.pe == 18.0
	assert got.fcf_yield == 0.06
	# No-peek: earlier asof gets older snapshot
	got2 = get_ratios_asof(engine, 1, _utc(2024, 2, 1))
	assert got2.pe == 20.0


def test_sector_stats_and_screener_ranker(tmp_path: Path) -> None:
	# Minimal symbols and bars
	symbols_engine = create_sqlite_engine(str(tmp_path / "sym.db"))
	symbols_ensure_schema(symbols_engine)
	metadata = MetaData()
	table = define_symbols_table(metadata)
	metadata.create_all(symbols_engine)
	with symbols_engine.begin() as conn:
		conn.execute(
			insert(table),
			[
				{"symbol_id": 1, "ticker": "AAA", "exchange": "XNAS", "currency": "USD", "active_from": _utc(2020,1,1), "active_to": None},
				{"symbol_id": 2, "ticker": "BBB", "exchange": "XNAS", "currency": "USD", "active_from": _utc(2020,1,1), "active_to": None},
				{"symbol_id": 3, "ticker": "CCC", "exchange": "XETR", "currency": "EUR", "active_from": _utc(2020,1,1), "active_to": None},
			]
		)
	# Bars: simple constant closing prices and volumes
	rows = []
	base = date(2024, 1, 1)
	for i in range(30):
		ts = _utc(2024, 1, 1) + timedelta(days=i)
		rows.append(BarRow(ts=ts, symbol_id=1, open=100+i, high=101+i, low=99+i, close=100+i, volume=1000+i, dt=base + timedelta(days=i)))
		rows.append(BarRow(ts=ts, symbol_id=2, open=100+i, high=101+i, low=99+i, close=100+i, volume=500+i, dt=base + timedelta(days=i)))
		rows.append(BarRow(ts=ts, symbol_id=3, open=100+i, high=101+i, low=99+i, close=100+i, volume=2000+i, dt=base + timedelta(days=i)))
	store = BarsStore.from_rows(rows)
	# Ratios/sector points
	ratio_points = {
		1: RatioPoint(symbol_id=1, sector="Tech", metrics={"fcf_yield": 0.05, "roic": 0.12}),
		2: RatioPoint(symbol_id=2, sector="Tech", metrics={"fcf_yield": 0.02, "roic": 0.08}),
		3: RatioPoint(symbol_id=3, sector="Industrial", metrics={"fcf_yield": 0.08, "roic": 0.10}),
	}
	stats = compute_sector_stats(list(ratio_points.values()), ["fcf_yield", "roic"])
	assert "Tech" in stats and "Industrial" in stats
	# Filter universe by US region only
	class _Sym:
		def __init__(self, sid, ticker, exch):
			self.symbol_id = sid
			self.ticker = ticker
			self.exchange = exch
	
	symbols = [_Sym(1, "AAA", "XNAS"), _Sym(2, "BBB", "XNAS"), _Sym(3, "CCC", "XETR")]
	filtered = filter_universe(symbols, UniverseFilters(regions=["US"]))
	assert all(s.exchange in ("XNAS", "XNYS") for s in filtered)
	# Rank candidates with ADDV filter
	asof = _utc(2024, 2, 1)
	cands = rank_candidates(
		store=store,
		symbols=filtered,
		ratio_points=ratio_points,
		asof=asof,
		filters=UniverseFilters(regions=["US"], min_addv=1e5, addv_window_days=10),
		top_k=5,
		metric_weights={"fcf_yield": 1.0, "roic": 0.5},
	)
	# Should rank symbol 1 above 2 within US Tech
	assert cands and isinstance(cands[0], Candidate)
	assert cands[0].symbol_id == 1