from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List

from ..data.calendars import is_open, next_open, next_close
from .events import EventQueue, EventType


@dataclass(frozen=True)
class ClockConfig:
    exchanges: List[str]


class Clock:
    def __init__(self, config: ClockConfig) -> None:
        self._exchanges = list(config.exchanges)
        if not self._exchanges:
            raise ValueError("Clock requires at least one exchange")

    def seed(self, q: EventQueue, start: datetime) -> None:
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        # Seed initial CLOCK event at start time
        q.push(start, EventType.CLOCK, {"label": "start"})

    def advance(self, q: EventQueue, now: datetime) -> None:
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        # For each exchange, schedule next open/close boundaries deterministically
        for ex in sorted(self._exchanges):
            if is_open(ex, now):
                boundary = next_close(ex, now)
                label = f"{ex}:close"
            else:
                boundary = next_open(ex, now)
                label = f"{ex}:open"
            # Push a CLOCK event to mark the boundary
            q.push(boundary, EventType.CLOCK, {"exchange": ex, "label": label})