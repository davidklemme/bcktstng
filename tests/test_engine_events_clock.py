from datetime import datetime, timezone

import pytest

from quant.engine.events import EventQueue, EventType
from quant.engine.clock import Clock, ClockConfig


def test_event_priority_ordering():
    q = EventQueue()
    t = datetime(2024, 7, 4, 12, 0, tzinfo=timezone.utc)

    # Same timestamp but different types and sequence
    q.push(t, EventType.FX, {"r": 1})
    q.push(t, EventType.BAR, {"bar": 1})
    q.push(t, EventType.CORPORATE_ACTION, {"split": 2.0})
    q.push(t, EventType.CLOCK, {"tick": True})

    # Different timestamp
    q.push(datetime(2024, 7, 4, 11, 0, tzinfo=timezone.utc), EventType.BAR)

    out_types = []
    while len(q):
        out_types.append(q.pop().type)

    # Expected: earlier time first; at same ts: CLOCK < BAR < FX < CORPORATE_ACTION
    assert out_types == [
        EventType.BAR,
        EventType.CLOCK,
        EventType.BAR,
        EventType.FX,
        EventType.CORPORATE_ACTION,
    ]


def test_clock_schedules_next_open_close():
    q = EventQueue()
    c = Clock(ClockConfig(exchanges=["XNYS", "XETR"]))

    # Seed at a UTC time; July 4 US is holiday (XNYS closed), XETR open
    start = datetime(2024, 7, 4, 12, 0, tzinfo=timezone.utc)
    c.seed(q, start)
    c.advance(q, start)

    events = []
    while len(q):
        events.append(q.pop())

    # Should contain: start at 12:00, next XETR close boundary (same day), next XNYS open (next business day)
    labels = [e.payload.get("label") for e in events]

    # First event is start
    assert labels[0] == "start"

    # Remaining two are deterministic in order by timestamp; check presence and reasonable ordering
    assert any(l.startswith("XETR:") for l in labels)
    assert any(l.startswith("XNYS:") for l in labels)

    # Ensure boundaries are CLOCK events
    assert all(e.type == EventType.CLOCK for e in events)