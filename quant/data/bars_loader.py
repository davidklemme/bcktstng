from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Dict, List, Optional, Tuple

from .calendars import is_open, next_close, EXCHANGE_TZ


@dataclass(frozen=True)
class BarRow:
    ts: datetime  # UTC close timestamp
    symbol_id: int
    open: float
    high: float
    low: float
    close: float
    volume: int
    dt: date  # original session date (local calendar date)


@dataclass(frozen=True)
class ValidationResult:
    missing_dates: List[date]
    nan_row_indices: List[int]


def _parse_float(value: str) -> Optional[float]:
    value = value.strip()
    if value == "" or value.lower() == "nan":
        return None
    return float(value)


def _parse_int(value: str) -> Optional[int]:
    value = value.strip()
    if value == "" or value.lower() == "nan":
        return None
    return int(value)


def _session_close_utc(exchange: str, session_dt: date) -> datetime:
    # Use local timezone noon to find today's close via next_close
    tz = EXCHANGE_TZ.get(exchange)
    if tz is None:
        raise ValueError(f"Unsupported exchange: {exchange}")
    local_noon = datetime(session_dt.year, session_dt.month, session_dt.day, 12, 0, 0, tzinfo=tz)
    return next_close(exchange, local_noon)


def _is_session_day(exchange: str, session_dt: date) -> bool:
    tz = EXCHANGE_TZ.get(exchange)
    if tz is None:
        raise ValueError(f"Unsupported exchange: {exchange}")
    # Check if the market is open at local noon for that date
    local_noon = datetime(session_dt.year, session_dt.month, session_dt.day, 12, 0, 0, tzinfo=tz)
    return is_open(exchange, local_noon)


def _daterange(start: date, end: date) -> List[date]:
    out: List[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur = date.fromordinal(cur.toordinal() + 1)
    return out


def load_daily_bars_csv(csv_path: str, exchange: str) -> Tuple[List[BarRow], ValidationResult]:
    # Expected columns: dt (YYYY-MM-DD), symbol_id, open, high, low, close, volume
    rows: List[BarRow] = []
    nan_row_indices: List[int] = []
    seen_dates: List[date] = []

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        required = {"dt", "symbol_id", "open", "high", "low", "close", "volume"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")

        for idx, r in enumerate(reader):
            session_dt = date.fromisoformat(r["dt"])  # date only
            seen_dates.append(session_dt)

            parsed_open = _parse_float(r["open"])
            parsed_high = _parse_float(r["high"])
            parsed_low = _parse_float(r["low"])
            parsed_close = _parse_float(r["close"])
            parsed_volume = _parse_int(r["volume"])

            if (
                parsed_open is None
                or parsed_high is None
                or parsed_low is None
                or parsed_close is None
                or parsed_volume is None
            ):
                nan_row_indices.append(idx)
                # Still attempt to compute ts for diagnostics

            ts_utc = _session_close_utc(exchange, session_dt)

            rows.append(
                BarRow(
                    ts=ts_utc,
                    symbol_id=int(r["symbol_id"]),
                    open=float(parsed_open) if parsed_open is not None else float("nan"),
                    high=float(parsed_high) if parsed_high is not None else float("nan"),
                    low=float(parsed_low) if parsed_low is not None else float("nan"),
                    close=float(parsed_close) if parsed_close is not None else float("nan"),
                    volume=int(parsed_volume) if parsed_volume is not None else 0,
                    dt=session_dt,
                )
            )

    # Compute gaps between min and max seen dates limited to session days
    missing_dates: List[date] = []
    if seen_dates:
        min_dt = min(seen_dates)
        max_dt = max(seen_dates)
        seen_set = set(seen_dates)
        for d in _daterange(min_dt, max_dt):
            if _is_session_day(exchange, d) and d not in seen_set:
                missing_dates.append(d)

    return rows, ValidationResult(missing_dates=missing_dates, nan_row_indices=nan_row_indices)


def write_parquet(path: str, rows: List[BarRow]) -> None:
    # Optional: requires pyarrow at runtime
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except Exception as exc:
        raise ImportError("pyarrow required to write parquet") from exc

    table = pa.table(
        {
            "ts": [r.ts for r in rows],
            "symbol_id": [r.symbol_id for r in rows],
            "open": [r.open for r in rows],
            "high": [r.high for r in rows],
            "low": [r.low for r in rows],
            "close": [r.close for r in rows],
            "volume": [r.volume for r in rows],
        }
    )
    pq.write_table(table, path)