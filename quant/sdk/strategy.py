from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import logging

from ..data.pit_reader import PITDataReader, BarsStore
from ..data.bars_loader import BarRow
from ..data.calendars import is_open as calendar_is_open, next_open as calendar_next_open, next_close as calendar_next_close
from ..data.symbols_repository import SymbolRow
from ..engine.orders import Order, OrderSide, OrderType, TimeInForce
from ..engine.portfolio import Portfolio
from ..engine.risk import RiskCaps, RiskManager
from .features import rolling_mean, zscore, rolling_vol, atr, vol_target


class Strategy:
    """Base Strategy interface.

    Users subclass this and implement lifecycle methods.
    """

    def on_start(self, ctx: "Context") -> None:  # noqa: D401
        """Called once before the first event."""
        return None

    def on_event(self, evt: Any, ctx: "Context") -> None:  # noqa: D401
        """Called for every event in the backtest/paper/live loop."""
        return None

    def on_end(self, ctx: "Context") -> None:  # noqa: D401
        """Called once after the last event."""
        return None


@dataclass
class _CalendarAPI:
    def is_open(self, exchange: str, ts: datetime) -> bool:
        return calendar_is_open(exchange, ts)

    def next_open(self, exchange: str, ts: datetime) -> datetime:
        return calendar_next_open(exchange, ts)

    def next_close(self, exchange: str, ts: datetime) -> datetime:
        return calendar_next_close(exchange, ts)


@dataclass
class _FeaturesAPI:
    def rolling_mean(self, values: Sequence[float], window: int) -> List[Optional[float]]:
        return rolling_mean(values, window)

    def zscore(self, values: Sequence[float], window: int) -> List[Optional[float]]:
        return zscore(values, window)

    def rolling_vol(self, values: Sequence[float], window: int) -> List[Optional[float]]:
        return rolling_vol(values, window)

    def atr(self, high: Sequence[float], low: Sequence[float], close: Sequence[float], window: int) -> List[Optional[float]]:
        return atr(high, low, close, window)

    def vol_target(self, returns: Sequence[float], target_annual_vol: float, window: int, periods_per_year: int = 252) -> List[Optional[float]]:
        return vol_target(returns, target_annual_vol, window, periods_per_year)


@dataclass
class _DataAPI:
    _reader: PITDataReader
    _symbol_cache: Dict[str, SymbolRow] = field(default_factory=dict)

    def _ensure_symbol_cache(self, asof: datetime) -> None:
        # Build cache ticker->row for given asof
        if self._symbol_cache:
            return
        rows = self._reader.get_symbols(asof)
        self._symbol_cache = {row.ticker: row for row in rows}

    def get(self, symbol: str | int, fields: Sequence[str], lookback: int, at: Optional[datetime] = None) -> Dict[str, List[Any]]:
        if at is None:
            raise ValueError("'at' must be provided to data.get for PIT safety")
        if at.tzinfo is None:
            at = at.replace(tzinfo=timezone.utc)
        self._ensure_symbol_cache(at)
        if isinstance(symbol, str):
            row = self._symbol_cache.get(symbol)
            if row is None:
                raise KeyError(f"Unknown symbol ticker: {symbol}")
            symbol_id = row.symbol_id
        else:
            symbol_id = int(symbol)
        # fetch bars up to 'at' and slice last 'lookback'
        bars: List[BarRow] = self._reader.get_bars(symbol_id, start=None, end=at, asof=at)
        if lookback > 0:
            bars = bars[-lookback:]
        out: Dict[str, List[Any]] = {}
        for f in fields:
            if f not in ("open", "high", "low", "close", "volume", "ts"):
                raise KeyError(f"Unsupported field: {f}")
            if f == "ts":
                out[f] = [b.ts for b in bars]
            else:
                out[f] = [getattr(b, f) for b in bars]
        return out


@dataclass
class _OrderAPI:
    _orders: List[Order] = field(default_factory=list)
    _canceled: set[str] = field(default_factory=set)

    def order(
        self,
        *,
        symbol_id: int,
        side: str,
        qty: int,
        type: str = "LMT",
        limit_price: Optional[float] = None,
        tif: str = "DAY",
        tag: Optional[str] = None,
    ) -> Order:
        order = Order(
            id=str(tag) if tag else f"order_{len(self._orders)+1}",
            symbol_id=symbol_id,
            side=OrderSide.BUY if side.upper() == "BUY" else OrderSide.SELL,
            quantity=int(qty),
            type=OrderType.LIMIT if type.upper() in ("LMT", "LIMIT") else OrderType.MARKET,
            tif=TimeInForce[tif.upper()],
            limit_price=limit_price,
        )
        self._orders.append(order)
        return order

    def cancel(self, tag_or_id: str) -> None:
        self._canceled.add(str(tag_or_id))


@dataclass
class _RiskAPI:
    _caps: RiskCaps = field(default_factory=lambda: RiskCaps(max_gross=1e9, max_net=1e9, max_symbol=1e9, max_leverage=10.0))
    _manager: RiskManager = field(init=False)

    def __post_init__(self) -> None:
        self._manager = RiskManager(self._caps)

    def set(self, *, max_gross: Optional[float] = None, max_net: Optional[float] = None, max_symbol: Optional[float] = None, max_leverage: Optional[float] = None) -> None:
        self._caps = RiskCaps(
            max_gross=max_gross if max_gross is not None else self._caps.max_gross,
            max_net=max_net if max_net is not None else self._caps.max_net,
            max_symbol=max_symbol if max_symbol is not None else self._caps.max_symbol,
            max_leverage=max_leverage if max_leverage is not None else self._caps.max_leverage,
        )
        self._manager = RiskManager(self._caps)

    @property
    def manager(self) -> RiskManager:
        return self._manager


@dataclass
class Context:
    now: datetime
    data: _DataAPI
    order_api: _OrderAPI
    portfolio: Portfolio
    risk: _RiskAPI
    calendar: _CalendarAPI = field(default_factory=_CalendarAPI)
    log: logging.Logger = field(default_factory=lambda: logging.getLogger("quant.strategy"))
    features: _FeaturesAPI = field(default_factory=_FeaturesAPI)

    def order(self, symbol: str | int, qty: int, *, side: str = "BUY", type: str = "LMT", limit_price: Optional[float] = None, tif: str = "DAY", tag: Optional[str] = None) -> Order:
        # Resolve symbol to id if needed
        symbol_id: int
        if isinstance(symbol, str):
            # Try to resolve via data API symbol cache using 'now'
            rows = self.data._reader.get_symbols(self.now)
            mapping = {r.ticker: r.symbol_id for r in rows}
            if symbol not in mapping:
                raise KeyError(f"Unknown symbol ticker: {symbol}")
            symbol_id = mapping[symbol]
        else:
            symbol_id = int(symbol)
        return self.order_api.order(symbol_id=symbol_id, side=side, qty=qty, type=type, limit_price=limit_price, tif=tif, tag=tag)

    def cancel(self, tag_or_id: str) -> None:
        self.order_api.cancel(tag_or_id)