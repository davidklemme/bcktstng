from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from .bars_loader import BarRow
from .fx_repository import get_rate_asof as fx_get_rate_asof
from .symbols_repository import get_symbols_asof as symbols_get_asof
from sqlalchemy.engine import Engine


@dataclass
class BarsStore:
    by_symbol: Dict[int, List[BarRow]]

    @classmethod
    def from_rows(cls, rows: Iterable[BarRow]) -> "BarsStore":
        by_symbol: Dict[int, List[BarRow]] = {}
        for r in rows:
            by_symbol.setdefault(r.symbol_id, []).append(r)
        # ensure sorted by ts
        for sym, lst in by_symbol.items():
            lst.sort(key=lambda x: x.ts)
        return cls(by_symbol=by_symbol)

    def get_between(self, symbol_id: int, start: Optional[datetime], end: Optional[datetime]) -> List[BarRow]:
        data = self.by_symbol.get(symbol_id, [])
        out: List[BarRow] = []
        for r in data:
            if start is not None and r.ts < start:
                continue
            if end is not None and r.ts > end:
                continue
            out.append(r)
        return out


class PITDataReader:
    def __init__(self, fx_engine: Engine, symbols_engine: Engine, bars_store: BarsStore) -> None:
        self._fx_engine = fx_engine
        self._symbols_engine = symbols_engine
        self._bars = bars_store

    @staticmethod
    def _ensure_utc(ts: datetime) -> datetime:
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    def get_symbols(self, asof: datetime):
        asof_utc = self._ensure_utc(asof)
        return symbols_get_asof(self._symbols_engine, asof_utc)

    def get_fx(self, base_ccy: str, quote_ccy: str, asof: datetime):
        asof_utc = self._ensure_utc(asof)
        return fx_get_rate_asof(self._fx_engine, base_ccy, quote_ccy, asof_utc)

    def get_bars(self, symbol_id: int, start: Optional[datetime], end: Optional[datetime], asof: datetime) -> List[BarRow]:
        asof_utc = self._ensure_utc(asof)
        start_utc = self._ensure_utc(start) if start is not None else None
        end_utc = self._ensure_utc(end) if end is not None else None

        # No-peek guard: cannot request beyond asof
        if end_utc is not None and end_utc > asof_utc:
            raise ValueError("Requested end exceeds asof; no-peek guard triggered")
        if end_utc is None:
            end_utc = asof_utc

        bars = self._bars.get_between(symbol_id, start_utc, end_utc)

        # Additional guard in case store has future data accidentally
        if any(r.ts > asof_utc for r in bars):
            raise RuntimeError("Bars data includes timestamps beyond asof; dataset violation")

        return bars