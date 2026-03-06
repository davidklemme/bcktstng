from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from ..data.adjusters import apply_actions, dividend_cashflow_on_exdate
from ..data.corp_actions_repository import CorporateAction
from ..data.fx_repository import get_rate_asof
from sqlalchemy.engine import Engine

logger = logging.getLogger("quant.portfolio")


def _validate_finite(value: float, name: str) -> None:
    if math.isnan(value) or math.isinf(value):
        raise ValueError(f"{name} must be finite, got {value}")


@dataclass
class Position:
    symbol_id: int
    currency: str
    quantity: float = 0.0
    average_price: float = 0.0

    def apply_fill(self, qty_delta: float, price: float) -> Tuple[float, float]:
        realized_pnl = 0.0
        if qty_delta == 0:
            return self.quantity, realized_pnl
        # If same sign or moving from flat to non-flat, update average price
        if self.quantity == 0 or (self.quantity > 0 and qty_delta > 0) or (self.quantity < 0 and qty_delta < 0):
            new_qty = self.quantity + qty_delta
            # weighted average
            self.average_price = (
                (self.average_price * self.quantity + price * qty_delta) / new_qty
            ) if new_qty != 0 else 0.0
            self.quantity = new_qty
            return self.quantity, realized_pnl
        # Reducing or flipping the position
        close_qty = min(abs(qty_delta), abs(self.quantity))
        sign = 1.0 if self.quantity > 0 else -1.0
        realized_pnl = (price - self.average_price) * (close_qty * sign)
        self.quantity += qty_delta
        if self.quantity == 0:
            self.average_price = 0.0
        else:
            # If flipped beyond zero, set avg price to fill price of the residual open portion
            if (self.quantity > 0 and qty_delta > 0) or (self.quantity < 0 and qty_delta < 0):
                # this path theoretically won't occur because we handled same sign earlier
                self.average_price = (self.average_price + price) / 2.0
            else:
                self.average_price = price
        return self.quantity, realized_pnl

    def apply_corporate_actions(self, actions: List[CorporateAction]) -> None:
        if not actions or self.quantity == 0:
            return
        adjusted = apply_actions(self.average_price, self.quantity, actions)
        self.average_price = adjusted.price
        self.quantity = adjusted.qty


@dataclass
class Portfolio:
    base_currency: str = "EUR"
    cash_by_ccy: Dict[str, float] = field(default_factory=dict)
    positions: Dict[int, Position] = field(default_factory=dict)
    _processed_actions: Dict[int, List[Tuple[datetime, float, float]]] = field(default_factory=dict)

    def get_cash(self, currency: str) -> float:
        return self.cash_by_ccy.get(currency, 0.0)

    def deposit(self, amount: float, currency: str) -> None:
        _validate_finite(amount, "deposit amount")
        if amount < 0:
            raise ValueError(f"deposit amount must be non-negative, got {amount}")
        self.cash_by_ccy[currency] = self.get_cash(currency) + float(amount)

    def withdraw(self, amount: float, currency: str) -> None:
        _validate_finite(amount, "withdraw amount")
        if amount < 0:
            raise ValueError(f"withdraw amount must be non-negative, got {amount}")
        self.cash_by_ccy[currency] = self.get_cash(currency) - float(amount)

    def get_or_create_position(self, symbol_id: int, currency: str) -> Position:
        pos = self.positions.get(symbol_id)
        if pos is None:
            pos = Position(symbol_id=symbol_id, currency=currency)
            self.positions[symbol_id] = pos
        return pos

    def apply_fill(self, symbol_id: int, currency: str, side: str, qty: float, price: float) -> float:
        # Validate all inputs before any mutation
        _validate_finite(qty, "fill qty")
        _validate_finite(price, "fill price")
        if qty <= 0:
            raise ValueError(f"fill qty must be positive, got {qty}")
        if price < 0:
            raise ValueError(f"fill price must be non-negative, got {price}")

        pos = self.get_or_create_position(symbol_id, currency)
        qty_signed = qty if side.upper() == "BUY" else -qty
        new_qty, realized_pnl = pos.apply_fill(qty_signed, price)
        notional = qty * price
        # Update cash
        if side.upper() == "BUY":
            self.cash_by_ccy[currency] = self.get_cash(currency) - notional
        else:
            self.cash_by_ccy[currency] = self.get_cash(currency) + notional
        return realized_pnl

    def apply_transaction_cost(self, currency: str, cost: float) -> None:
        if cost == 0.0:
            return
        _validate_finite(cost, "transaction cost")
        self.cash_by_ccy[currency] = self.get_cash(currency) - cost

    def process_actions_for_symbol(self, symbol_id: int, actions: List[CorporateAction], asof: datetime) -> float:
        if asof.tzinfo is None:
            asof = asof.replace(tzinfo=timezone.utc)
        pos = self.positions.get(symbol_id)
        if pos is None or pos.quantity == 0:
            return 0.0
        processed = self._processed_actions.setdefault(symbol_id, [])
        to_apply: List[CorporateAction] = []
        div_actions: List[CorporateAction] = []
        for a in actions:
            action_key = (a.effective_date, a.split_ratio, a.dividend)
            if a.effective_date <= asof and action_key not in processed:
                to_apply.append(a)
                if a.dividend and a.dividend != 0.0:
                    div_actions.append(a)
                processed.append(action_key)
        if not to_apply:
            return 0.0
        # Apply splits first via adjuster
        pos.apply_corporate_actions(to_apply)
        # Credit dividends as cashflow in instrument currency
        div_cash = dividend_cashflow_on_exdate(pos.quantity, div_actions)
        if div_cash != 0.0:
            self.deposit(div_cash, pos.currency)
        return div_cash

    def total_value_eur(self, asof: datetime, mark_prices: Dict[int, float], fx_engine: Engine) -> float:
        if asof.tzinfo is None:
            asof = asof.replace(tzinfo=timezone.utc)
        total_eur = 0.0
        # Cash
        for ccy, amount in self.cash_by_ccy.items():
            if ccy == self.base_currency:
                total_eur += amount
            else:
                try:
                    rate = get_rate_asof(fx_engine, ccy, self.base_currency, asof)
                    total_eur += amount * rate.rate
                except LookupError:
                    logger.warning("Missing FX rate for %s/%s at %s; skipping cash from total", ccy, self.base_currency, asof)
        # Positions
        for symbol_id, pos in self.positions.items():
            if pos.quantity == 0:
                continue
            price = mark_prices.get(symbol_id)
            if price is None:
                continue
            mv = pos.quantity * price
            if pos.currency == self.base_currency:
                total_eur += mv
            else:
                try:
                    rate = get_rate_asof(fx_engine, pos.currency, self.base_currency, asof)
                    total_eur += mv * rate.rate
                except LookupError:
                    logger.warning("Missing FX rate for %s/%s at %s; skipping position %d from total", pos.currency, self.base_currency, asof, symbol_id)
        return round(total_eur, 10)
