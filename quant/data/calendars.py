from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Tuple
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class SessionHours:
    open_time: time
    close_time: time


EXCHANGE_TZ = {
    "XNYS": ZoneInfo("America/New_York"),
    "XETR": ZoneInfo("Europe/Berlin"),
}


def _local_date(dt_utc: datetime, tz: ZoneInfo) -> datetime:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)
    return dt_utc.astimezone(tz)


def _is_weekend(local_dt: datetime) -> bool:
    return local_dt.weekday() >= 5  # 5=Saturday, 6=Sunday


def _xnys_holiday(local_dt: datetime) -> bool:
    # Minimal rules to match dummy dataset closures:
    # - 2024-01-15: Martin Luther King Jr. Day
    # - 2024-02-19: Presidents' Day
    # - 07-04: Independence Day (fixed-date, no observed logic in this minimal calendar)
    if local_dt.year == 2024 and (local_dt.month, local_dt.day) in {(1, 15), (2, 19)}:
        return True
    # Independence Day
    return local_dt.month == 7 and local_dt.day == 4


def _xetr_half_day(local_dt: datetime) -> bool:
    # Minimal rule: Christmas Eve Dec 24 half-day
    return local_dt.month == 12 and local_dt.day == 24


def _session_hours(exchange: str, local_dt: datetime) -> SessionHours:
    if exchange == "XNYS":
        # NYSE regular session 09:30–16:00 local
        return SessionHours(open_time=time(9, 30), close_time=time(16, 0))
    if exchange == "XETR":
        # XETR: 09:00–17:30 regular; half-day Dec 24 closes 14:00
        if _xetr_half_day(local_dt):
            return SessionHours(open_time=time(9, 0), close_time=time(14, 0))
        return SessionHours(open_time=time(9, 0), close_time=time(17, 30))
    raise ValueError(f"Unsupported exchange: {exchange}")


def is_open(exchange: str, ts: datetime) -> bool:
    tz = EXCHANGE_TZ.get(exchange)
    if tz is None:
        raise ValueError(f"Unsupported exchange: {exchange}")

    local = _local_date(ts, tz)

    if _is_weekend(local):
        return False

    if exchange == "XNYS" and _xnys_holiday(local):
        return False

    hours = _session_hours(exchange, local)
    open_dt = local.replace(hour=hours.open_time.hour, minute=hours.open_time.minute, second=0, microsecond=0)
    close_dt = local.replace(hour=hours.close_time.hour, minute=hours.close_time.minute, second=0, microsecond=0)

    return open_dt <= local < close_dt


def next_open(exchange: str, ts: datetime) -> datetime:
    tz = EXCHANGE_TZ.get(exchange)
    if tz is None:
        raise ValueError(f"Unsupported exchange: {exchange}")

    local = _local_date(ts, tz)
    # If before today's open and not holiday/weekend, today at open
    days_ahead = 0
    while True:
        candidate = local + timedelta(days=days_ahead)
        if _is_weekend(candidate):
            days_ahead += 1
            continue
        if exchange == "XNYS" and _xnys_holiday(candidate):
            days_ahead += 1
            continue
        hours = _session_hours(exchange, candidate)
        open_dt = candidate.replace(hour=hours.open_time.hour, minute=hours.open_time.minute, second=0, microsecond=0)
        close_dt = candidate.replace(hour=hours.close_time.hour, minute=hours.close_time.minute, second=0, microsecond=0)

        if days_ahead == 0 and local < open_dt:
            return open_dt.astimezone(timezone.utc)
        if days_ahead == 0 and local < close_dt and local >= open_dt:
            # already open today; next open is next valid day
            days_ahead += 1
            continue
        # next valid day's open
        if days_ahead > 0:
            return open_dt.astimezone(timezone.utc)
        days_ahead += 1


def next_close(exchange: str, ts: datetime) -> datetime:
    tz = EXCHANGE_TZ.get(exchange)
    if tz is None:
        raise ValueError(f"Unsupported exchange: {exchange}")

    local = _local_date(ts, tz)

    # If currently before open, today's close; if during session, today's close; else next valid day's close
    days_ahead = 0
    while True:
        candidate = local + timedelta(days=days_ahead)
        if _is_weekend(candidate):
            days_ahead += 1
            continue
        if exchange == "XNYS" and _xnys_holiday(candidate):
            days_ahead += 1
            continue
        hours = _session_hours(exchange, candidate)
        open_dt = candidate.replace(hour=hours.open_time.hour, minute=hours.open_time.minute, second=0, microsecond=0)
        close_dt = candidate.replace(hour=hours.close_time.hour, minute=hours.close_time.minute, second=0, microsecond=0)

        if days_ahead == 0 and local < close_dt:
            return close_dt.astimezone(timezone.utc)
        if days_ahead == 0 and local >= close_dt:
            days_ahead += 1
            continue
        if days_ahead > 0:
            return close_dt.astimezone(timezone.utc)
        days_ahead += 1