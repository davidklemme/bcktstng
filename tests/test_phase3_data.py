"""Phase 3: Data layer tests — bars_loader, pit_reader, adjusters, corp_actions."""

import os
import tempfile
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

import pytest

from quant.data.bars_loader import (
    BarRow,
    load_daily_bars_csv,
    load_minute_bars_csv,
    _parse_float,
    _parse_int,
)
from quant.data.pit_reader import BarsStore, PITDataReader
from quant.data.adjusters import apply_actions, dividend_cashflow_on_exdate, PriceQty
from quant.data.corp_actions_repository import CorporateAction
from quant.data.fx_repository import create_engine as create_fx_engine, load_fx_csv_to_db
from quant.data.symbols_repository import (
    create_sqlite_engine as create_symbols_engine,
    load_symbols_csv_to_db,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _write_csv(path: Path, content: str) -> str:
    path.write_text(content)
    return str(path)


def _make_bar(symbol_id: int, ts: datetime, close: float = 100.0, volume: int = 1000) -> BarRow:
    return BarRow(
        ts=ts, symbol_id=symbol_id,
        open=close - 1, high=close + 1, low=close - 2, close=close,
        volume=volume, dt=ts.date(),
    )


# ---------------------------------------------------------------------------
# _parse_float / _parse_int
# ---------------------------------------------------------------------------
class TestParseHelpers:
    def test_parse_float_normal(self):
        assert _parse_float("3.14") == pytest.approx(3.14)

    def test_parse_float_nan(self):
        assert _parse_float("nan") is None
        assert _parse_float("NaN") is None

    def test_parse_float_empty(self):
        assert _parse_float("") is None

    def test_parse_int_normal(self):
        assert _parse_int("42") == 42

    def test_parse_int_nan(self):
        assert _parse_int("nan") is None


# ---------------------------------------------------------------------------
# load_daily_bars_csv
# ---------------------------------------------------------------------------
class TestLoadDailyBars:
    def test_basic_load(self, tmp_path):
        csv_content = (
            "dt,symbol_id,open,high,low,close,volume\n"
            "2024-01-02,1,100,105,99,103,10000\n"
            "2024-01-03,1,103,108,102,107,12000\n"
        )
        path = _write_csv(tmp_path / "bars.csv", csv_content)
        rows, val = load_daily_bars_csv(path, "XNYS")
        assert len(rows) == 2
        assert rows[0].close == 103
        assert rows[0].ts.tzinfo is not None  # UTC
        assert len(val.missing_dates) == 0

    def test_nan_rows_skipped(self, tmp_path):
        csv_content = (
            "dt,symbol_id,open,high,low,close,volume\n"
            "2024-01-02,1,100,105,99,103,10000\n"
            "2024-01-03,1,nan,108,102,107,12000\n"
        )
        path = _write_csv(tmp_path / "bars.csv", csv_content)
        rows, val = load_daily_bars_csv(path, "XNYS")
        assert len(rows) == 1  # NaN row skipped
        assert val.nan_row_indices == [1]

    def test_gap_detection(self, tmp_path):
        # Jan 2 (Tue) and Jan 4 (Thu) — missing Jan 3 (Wed)
        csv_content = (
            "dt,symbol_id,open,high,low,close,volume\n"
            "2024-01-02,1,100,105,99,103,10000\n"
            "2024-01-04,1,103,108,102,107,12000\n"
        )
        path = _write_csv(tmp_path / "bars.csv", csv_content)
        rows, val = load_daily_bars_csv(path, "XNYS")
        assert date(2024, 1, 3) in val.missing_dates

    def test_missing_columns_raises(self, tmp_path):
        csv_content = "dt,symbol_id,open\n2024-01-02,1,100\n"
        path = _write_csv(tmp_path / "bars.csv", csv_content)
        with pytest.raises(ValueError, match="missing required columns"):
            load_daily_bars_csv(path, "XNYS")


# ---------------------------------------------------------------------------
# load_minute_bars_csv
# ---------------------------------------------------------------------------
class TestLoadMinuteBars:
    def test_session_filter(self, tmp_path):
        """Out-of-session rows should be excluded."""
        csv_content = (
            "ts_local,symbol_id,open,high,low,close,volume\n"
            "2024-01-02 10:00,1,100,101,99,100,500\n"   # in session (10:00 ET)
            "2024-01-02 08:00,1,100,101,99,100,500\n"   # before open (08:00 ET)
            "2024-01-02 16:30,1,100,101,99,100,500\n"   # after close (16:30 ET)
        )
        path = _write_csv(tmp_path / "minute.csv", csv_content)
        rows, val = load_minute_bars_csv(path, "XNYS")
        assert len(rows) == 1  # only the 10:00 bar

    def test_absolute_timestamp_column(self, tmp_path):
        """Using 'ts' column with UTC timestamps."""
        csv_content = (
            "ts,symbol_id,open,high,low,close,volume\n"
            "2024-01-02T15:00:00+00:00,1,100,101,99,100,500\n"  # 10:00 ET, in session
        )
        path = _write_csv(tmp_path / "minute.csv", csv_content)
        rows, val = load_minute_bars_csv(path, "XNYS")
        assert len(rows) == 1

    def test_missing_ts_columns_raises(self, tmp_path):
        csv_content = "symbol_id,open,high,low,close,volume\n1,100,101,99,100,500\n"
        path = _write_csv(tmp_path / "minute.csv", csv_content)
        with pytest.raises(ValueError, match="either 'ts_local' or 'ts'"):
            load_minute_bars_csv(path, "XNYS")


# ---------------------------------------------------------------------------
# BarsStore
# ---------------------------------------------------------------------------
class TestBarsStore:
    def test_from_rows_sorts_by_ts(self):
        t1 = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc)
        b1 = _make_bar(1, t2)
        b2 = _make_bar(1, t1)
        store = BarsStore.from_rows([b1, b2])  # out of order
        assert store.by_symbol[1][0].ts == t1  # sorted

    def test_get_between_filters(self):
        t1 = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc)
        t3 = datetime(2024, 1, 4, 21, 0, tzinfo=timezone.utc)
        store = BarsStore.from_rows([_make_bar(1, t1), _make_bar(1, t2), _make_bar(1, t3)])
        result = store.get_between(1, t2, t2)
        assert len(result) == 1
        assert result[0].ts == t2

    def test_get_between_empty_symbol(self):
        store = BarsStore(by_symbol={})
        assert store.get_between(999, None, None) == []


# ---------------------------------------------------------------------------
# PITDataReader
# ---------------------------------------------------------------------------
class TestPITDataReader:
    @pytest.fixture
    def setup_reader(self, tmp_path):
        fx_engine = create_fx_engine(str(tmp_path / "fx.sqlite"))
        fx_csv = tmp_path / "fx.csv"
        fx_csv.write_text("ts,base_ccy,quote_ccy,rate\n2024-01-01T00:00:00Z,USD,EUR,0.85\n")
        load_fx_csv_to_db(str(fx_csv), fx_engine)

        symbols_engine = create_symbols_engine(str(tmp_path / "symbols.sqlite"))
        sym_csv = tmp_path / "symbols.csv"
        sym_csv.write_text(
            "symbol_id,ticker,exchange,currency,active_from,active_to\n"
            "1,AAPL,XNYS,USD,2020-01-01T00:00:00Z,\n"
        )
        load_symbols_csv_to_db(str(sym_csv), symbols_engine)

        t1 = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        t2 = datetime(2024, 1, 3, 21, 0, tzinfo=timezone.utc)
        store = BarsStore.from_rows([_make_bar(1, t1), _make_bar(1, t2)])

        reader = PITDataReader(fx_engine, symbols_engine, store)
        return reader, t1, t2

    def test_get_bars_within_asof(self, setup_reader):
        reader, t1, t2 = setup_reader
        bars = reader.get_bars(1, start=None, end=t1, asof=t1)
        assert len(bars) == 1

    def test_no_peek_guard(self, setup_reader):
        reader, t1, t2 = setup_reader
        with pytest.raises(ValueError, match="no-peek guard"):
            reader.get_bars(1, start=None, end=t2, asof=t1)

    def test_end_none_defaults_to_asof(self, setup_reader):
        reader, t1, t2 = setup_reader
        bars = reader.get_bars(1, start=None, end=None, asof=t2)
        assert len(bars) == 2

    def test_get_fx(self, setup_reader):
        reader, t1, t2 = setup_reader
        rate = reader.get_fx("USD", "EUR", t1)
        assert rate.rate == pytest.approx(0.85)

    def test_get_symbols(self, setup_reader):
        reader, t1, t2 = setup_reader
        syms = reader.get_symbols(t1)
        assert len(syms) == 1
        assert syms[0].ticker == "AAPL"

    def test_naive_ts_treated_as_utc(self, setup_reader):
        reader, t1, t2 = setup_reader
        naive = t1.replace(tzinfo=None)
        bars = reader.get_bars(1, start=None, end=None, asof=naive)
        assert len(bars) == 1


# ---------------------------------------------------------------------------
# Adjusters
# ---------------------------------------------------------------------------
class TestAdjusters:
    def test_split_2_for_1(self):
        action = CorporateAction(symbol_id=1, effective_date=datetime(2024, 1, 2, tzinfo=timezone.utc), split_ratio=2.0, dividend=0.0, currency="USD")
        result = apply_actions(100.0, 10.0, [action])
        assert result.price == pytest.approx(50.0)
        assert result.qty == pytest.approx(20.0)

    def test_split_ratio_1_noop(self):
        action = CorporateAction(symbol_id=1, effective_date=datetime(2024, 1, 2, tzinfo=timezone.utc), split_ratio=1.0, dividend=0.0, currency="USD")
        result = apply_actions(100.0, 10.0, [action])
        assert result.price == pytest.approx(100.0)
        assert result.qty == pytest.approx(10.0)

    def test_multiple_actions(self):
        a1 = CorporateAction(symbol_id=1, effective_date=datetime(2024, 1, 2, tzinfo=timezone.utc), split_ratio=2.0, dividend=0.0, currency="USD")
        a2 = CorporateAction(symbol_id=1, effective_date=datetime(2024, 2, 1, tzinfo=timezone.utc), split_ratio=3.0, dividend=0.0, currency="USD")
        result = apply_actions(300.0, 10.0, [a1, a2])
        # 300/2 = 150, 10*2 = 20, then 150/3 = 50, 20*3 = 60
        assert result.price == pytest.approx(50.0)
        assert result.qty == pytest.approx(60.0)

    def test_dividend_cashflow(self):
        a = CorporateAction(symbol_id=1, effective_date=datetime(2024, 1, 2, tzinfo=timezone.utc), split_ratio=1.0, dividend=0.5, currency="USD")
        cash = dividend_cashflow_on_exdate(100.0, [a])
        assert cash == pytest.approx(50.0)

    def test_dividend_cashflow_multiple(self):
        a1 = CorporateAction(symbol_id=1, effective_date=datetime(2024, 1, 2, tzinfo=timezone.utc), split_ratio=1.0, dividend=0.5, currency="USD")
        a2 = CorporateAction(symbol_id=1, effective_date=datetime(2024, 4, 1, tzinfo=timezone.utc), split_ratio=1.0, dividend=0.3, currency="USD")
        cash = dividend_cashflow_on_exdate(100.0, [a1, a2])
        assert cash == pytest.approx(80.0)

    def test_no_dividend_zero(self):
        a = CorporateAction(symbol_id=1, effective_date=datetime(2024, 1, 2, tzinfo=timezone.utc), split_ratio=2.0, dividend=0.0, currency="USD")
        cash = dividend_cashflow_on_exdate(100.0, [a])
        assert cash == 0.0
