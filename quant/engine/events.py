from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum
from typing import Any, List, Tuple
import heapq


class EventType(IntEnum):
    CLOCK = 0
    BAR = 1
    FX = 2
    CORPORATE_ACTION = 3


@dataclass(order=True, frozen=True)
class Event:
    sort_index: Tuple[datetime, int, int] = field(init=False, repr=False, compare=True)
    ts: datetime = field(compare=False)
    type: EventType = field(compare=False)
    payload: Any = field(compare=False, default=None)
    seq: int = field(compare=False, default=0)

    def __post_init__(self):
        # use object.__setattr__ due to frozen dataclass
        object.__setattr__(self, "sort_index", (self.ts, int(self.type), self.seq))


class EventQueue:
    def __init__(self) -> None:
        self._heap: List[Tuple[datetime, int, int, Event]] = []
        self._seq_counter: int = 0

    def push(self, ts: datetime, type: EventType, payload: Any | None = None) -> Event:
        evt = Event(ts=ts, type=type, payload=payload, seq=self._seq_counter)
        self._seq_counter += 1
        heapq.heappush(self._heap, (evt.ts, int(evt.type), evt.seq, evt))
        return evt

    def pop(self) -> Event:
        if not self._heap:
            raise IndexError("pop from empty EventQueue")
        return heapq.heappop(self._heap)[3]

    def peek(self) -> Event | None:
        return self._heap[0][3] if self._heap else None

    def __len__(self) -> int:
        return len(self._heap)

    def clear(self) -> None:
        self._heap.clear()
        self._seq_counter = 0