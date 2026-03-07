"""Phase 3: Comprehensive calendar tests — DST, holidays, next_open/next_close edge cases."""

from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import pytest

from quant.data.calendars import is_open, next_open, next_close, EXCHANGE_TZ


# ---------------------------------------------------------------------------
# is_open basics
# ---------------------------------------------------------------------------
class TestIsOpen:
    def test_xnys_regular_session(self):
        # 2024-01-02 (Tue) at 10:00 ET = 15:00 UTC
        ts = datetime(2024, 1, 2, 15, 0, tzinfo=timezone.utc)
        assert is_open("XNYS", ts) is True

    def test_xnys_before_open(self):
        # 2024-01-02 (Tue) at 09:00 ET = 14:00 UTC
        ts = datetime(2024, 1, 2, 14, 0, tzinfo=timezone.utc)
        assert is_open("XNYS", ts) is False

    def test_xnys_at_close(self):
        # 16:00 ET = not open (close boundary exclusive)
        ts = datetime(2024, 1, 2, 21, 0, tzinfo=timezone.utc)
        assert is_open("XNYS", ts) is False

    def test_xnys_weekend(self):
        # 2024-01-06 is Saturday
        ts = datetime(2024, 1, 6, 15, 0, tzinfo=timezone.utc)
        assert is_open("XNYS", ts) is False

    def test_xnys_july4_holiday(self):
        # July 4, 2024 is a holiday
        ts = datetime(2024, 7, 4, 15, 0, tzinfo=timezone.utc)
        assert is_open("XNYS", ts) is False

    def test_xnys_mlk_day_holiday(self):
        # 2024-01-15 MLK Day
        ts = datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc)
        assert is_open("XNYS", ts) is False

    def test_xnys_presidents_day_holiday(self):
        # 2024-02-19 Presidents Day
        ts = datetime(2024, 2, 19, 15, 0, tzinfo=timezone.utc)
        assert is_open("XNYS", ts) is False

    def test_xetr_regular_session(self):
        # 2024-01-02 (Tue) at 12:00 CET = 11:00 UTC
        ts = datetime(2024, 1, 2, 11, 0, tzinfo=timezone.utc)
        assert is_open("XETR", ts) is True

    def test_xetr_before_open(self):
        # 08:30 CET = 07:30 UTC
        ts = datetime(2024, 1, 2, 7, 30, tzinfo=timezone.utc)
        assert is_open("XETR", ts) is False

    def test_xetr_half_day_dec24_during(self):
        # Dec 24 half-day closes at 14:00 CET; 12:00 CET = 11:00 UTC should be open
        ts = datetime(2024, 12, 24, 11, 0, tzinfo=timezone.utc)
        assert is_open("XETR", ts) is True

    def test_xetr_half_day_dec24_after_close(self):
        # Dec 24 closes at 14:00 CET; 14:30 CET = 13:30 UTC should be closed
        ts = datetime(2024, 12, 24, 13, 30, tzinfo=timezone.utc)
        assert is_open("XETR", ts) is False

    def test_unsupported_exchange_raises(self):
        with pytest.raises(ValueError, match="Unsupported"):
            is_open("XLON", datetime(2024, 1, 2, 12, 0, tzinfo=timezone.utc))


# ---------------------------------------------------------------------------
# DST transitions
# ---------------------------------------------------------------------------
class TestDSTTransitions:
    def test_xnys_spring_forward(self):
        """US spring forward: 2024-03-10 clocks jump 2am→3am ET."""
        # Day after DST change, session should still be 9:30-16:00 ET
        # 9:30 ET in EDT = 13:30 UTC
        ts_open = datetime(2024, 3, 11, 13, 30, tzinfo=timezone.utc)
        assert is_open("XNYS", ts_open) is True
        # Before open: 9:00 ET = 13:00 UTC
        ts_before = datetime(2024, 3, 11, 13, 0, tzinfo=timezone.utc)
        assert is_open("XNYS", ts_before) is False

    def test_xnys_fall_back(self):
        """US fall back: 2024-11-03 clocks go 2am→1am ET."""
        # Day after DST change: 2024-11-04 Mon, 9:30 EST = 14:30 UTC
        ts_open = datetime(2024, 11, 4, 14, 30, tzinfo=timezone.utc)
        assert is_open("XNYS", ts_open) is True

    def test_xetr_spring_forward(self):
        """EU spring forward: 2024-03-31 clocks jump 2am→3am CET→CEST."""
        # 2024-04-01 Mon, 9:00 CEST = 07:00 UTC
        ts_open = datetime(2024, 4, 1, 7, 0, tzinfo=timezone.utc)
        assert is_open("XETR", ts_open) is True

    def test_xetr_fall_back(self):
        """EU fall back: 2024-10-27 clocks go 3am→2am CEST→CET."""
        # 2024-10-28 Mon, 9:00 CET = 08:00 UTC
        ts_open = datetime(2024, 10, 28, 8, 0, tzinfo=timezone.utc)
        assert is_open("XETR", ts_open) is True


# ---------------------------------------------------------------------------
# next_open
# ---------------------------------------------------------------------------
class TestNextOpen:
    def test_before_open_same_day(self):
        # 2024-01-02 Tue, 08:00 ET = 13:00 UTC → should return 09:30 ET
        ts = datetime(2024, 1, 2, 13, 0, tzinfo=timezone.utc)
        result = next_open("XNYS", ts)
        # 09:30 ET (EST) = 14:30 UTC
        assert result.hour == 14
        assert result.minute == 30

    def test_during_session(self):
        # During session → next open is next trading day
        ts = datetime(2024, 1, 2, 15, 0, tzinfo=timezone.utc)  # 10:00 ET
        result = next_open("XNYS", ts)
        # Should be Jan 3 09:30 ET
        local = result.astimezone(ZoneInfo("America/New_York"))
        assert local.day == 3
        assert local.hour == 9

    def test_friday_close_to_monday_open(self):
        # 2024-01-05 (Fri) at 21:01 UTC (after close) → next open Mon Jan 8
        ts = datetime(2024, 1, 5, 21, 1, tzinfo=timezone.utc)
        result = next_open("XNYS", ts)
        local = result.astimezone(ZoneInfo("America/New_York"))
        assert local.weekday() == 0  # Monday
        assert local.hour == 9 and local.minute == 30

    def test_across_holiday(self):
        # July 3 2024 (Wed) after close → skip July 4 holiday → July 5
        ts = datetime(2024, 7, 3, 21, 0, tzinfo=timezone.utc)
        result = next_open("XNYS", ts)
        local = result.astimezone(ZoneInfo("America/New_York"))
        assert local.month == 7 and local.day == 5

    def test_xetr_weekend(self):
        # Saturday → next Monday
        ts = datetime(2024, 1, 6, 12, 0, tzinfo=timezone.utc)
        result = next_open("XETR", ts)
        local = result.astimezone(ZoneInfo("Europe/Berlin"))
        assert local.weekday() == 0  # Monday
        assert local.hour == 9

    def test_unsupported_exchange_raises(self):
        with pytest.raises(ValueError, match="Unsupported"):
            next_open("XLON", datetime(2024, 1, 2, 12, tzinfo=timezone.utc))


# ---------------------------------------------------------------------------
# next_close
# ---------------------------------------------------------------------------
class TestNextClose:
    def test_before_close_same_day(self):
        # 2024-01-02 at 14:00 ET = 19:00 UTC → close is 16:00 ET = 21:00 UTC
        ts = datetime(2024, 1, 2, 19, 0, tzinfo=timezone.utc)
        result = next_close("XNYS", ts)
        assert result.hour == 21
        assert result.minute == 0

    def test_after_close_next_day(self):
        # After close → next trading day's close
        ts = datetime(2024, 1, 2, 21, 30, tzinfo=timezone.utc)  # past 16:00 ET
        result = next_close("XNYS", ts)
        local = result.astimezone(ZoneInfo("America/New_York"))
        assert local.day == 3
        assert local.hour == 16

    def test_xetr_half_day(self):
        # Dec 24 before close → should be 14:00 CET
        ts = datetime(2024, 12, 24, 10, 0, tzinfo=timezone.utc)  # 11:00 CET
        result = next_close("XETR", ts)
        local = result.astimezone(ZoneInfo("Europe/Berlin"))
        assert local.hour == 14

    def test_xetr_regular_day_close(self):
        # Regular day close is 17:30 CET
        ts = datetime(2024, 1, 2, 10, 0, tzinfo=timezone.utc)  # 11:00 CET
        result = next_close("XETR", ts)
        local = result.astimezone(ZoneInfo("Europe/Berlin"))
        assert local.hour == 17 and local.minute == 30

    def test_unsupported_exchange_raises(self):
        with pytest.raises(ValueError, match="Unsupported"):
            next_close("XLON", datetime(2024, 1, 2, 12, tzinfo=timezone.utc))
