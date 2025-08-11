from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

from .corp_actions_repository import CorporateAction


@dataclass(frozen=True)
class PriceQty:
    price: float
    qty: float


def apply_actions(price: float, qty: float, actions: Iterable[CorporateAction]) -> PriceQty:
    adj_price = float(price)
    adj_qty = float(qty)

    for a in actions:
        # Splits: 2:1 means price halves, quantity doubles
        if a.split_ratio and a.split_ratio > 0 and a.split_ratio != 1.0:
            adj_price = adj_price / a.split_ratio
            adj_qty = adj_qty * a.split_ratio
        # Dividends do not change price immediately here (price adjustment depends on data vendor convention)
        # For P&L, dividend appears as separate cashflow handled elsewhere; we don't alter qty for dividends
    return PriceQty(price=round(adj_price, 10), qty=round(adj_qty, 10))


def dividend_cashflow_on_exdate(shares: float, actions: Iterable[CorporateAction]) -> float:
    total = 0.0
    for a in actions:
        if a.dividend and a.dividend != 0.0:
            total += shares * a.dividend
    return round(total, 10)