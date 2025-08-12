from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

from .orders import Order, OrderSide
from .portfolio import Portfolio
from ..data.fx_repository import Engine as _EngineType  # type: ignore


@dataclass(frozen=True)
class RiskCaps:
    max_gross: float
    max_net: float
    max_symbol: float
    max_leverage: float


class RiskManager:
    def __init__(self, caps: RiskCaps) -> None:
        self._caps = caps

    def _notional_eur(self, portfolio: Portfolio, symbol_currency: str, price: float, qty: float, fx_rate: float) -> float:
        notional = price * qty
        return notional * fx_rate

    def check(self, portfolio: Portfolio, symbol_id: int, symbol_currency: str, price: float, qty: float, fx_rate_to_eur: float) -> Tuple[bool, str | None]:
        # Per-symbol check
        symbol_mv_eur = 0.0
        pos = portfolio.positions.get(symbol_id)
        if pos and pos.quantity != 0:
            symbol_mv_eur += pos.quantity * price * fx_rate_to_eur
        symbol_mv_eur += qty * price * fx_rate_to_eur
        if abs(symbol_mv_eur) > self._caps.max_symbol:
            return False, f"max_symbol exceeded: {abs(symbol_mv_eur):.2f} > {self._caps.max_symbol:.2f}"

        # Gross and net exposure approximation using mark_prices isn't fully available here; rely on caller to pass aggregate if needed.
        # For now, enforce symbol-level which is most critical pre-trade.
        return True, None