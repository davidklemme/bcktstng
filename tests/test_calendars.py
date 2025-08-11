from __future__ import annotations

from datetime import datetime, timezone

import pytest

from quant.data.calendars import is_open, next_open, next_close


def _dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def test_xnys_closed_on_july_4() -> None:
    # July 4, 2023 15:00 UTC -> morning NY time, should be closed all day
    t = _dt("2023-07-04T15:00:00Z")
    assert is_open("XNYS", t) is False

    # Next open should be July 5 09:30 NY, which is 13:30 UTC (EDT)
    nxt = next_open("XNYS", t)
    assert nxt.isoformat().startswith("2023-07-05T13:30:00+")


def test_xetr_half_day_dec24() -> None:
    # Dec 24, 2024 13:30 CET -> market open, half-day close at 14:00 local
    t = _dt("2024-12-24T12:30:00Z")  # 13:30 CET
    assert is_open("XETR", t) is True

    # Next close should be 14:00 CET = 13:00 UTC
    nxt_close = next_close("XETR", t)
    assert nxt_close.isoformat().startswith("2024-12-24T13:00:00+")