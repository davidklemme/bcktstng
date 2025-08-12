from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, List, Tuple


Right = Literal["C", "P"]


@dataclass(frozen=True)
class OptionLeg:
    symbol_id: int  # underlying equity symbol id
    expiry: datetime
    strike: float
    right: Right
    quantity: int  # positive for long, negative for short


@dataclass(frozen=True)
class UnderlyingLeg:
    symbol_id: int
    quantity: int  # positive for long, negative for short


@dataclass(frozen=True)
class MultiLegStrategy:
    underlying: UnderlyingLeg
    options: List[OptionLeg]


def build_covered_call(*, symbol_id: int, shares: int, call_expiry: datetime, call_strike: float, call_qty: int = 1) -> MultiLegStrategy:
    if shares <= 0:
        raise ValueError("shares must be positive")
    if call_qty <= 0:
        raise ValueError("call_qty must be positive")
    # One standard equity option contract controls 100 shares
    contracts = call_qty
    option = OptionLeg(symbol_id=symbol_id, expiry=call_expiry, strike=call_strike, right="C", quantity=-contracts)
    underlying = UnderlyingLeg(symbol_id=symbol_id, quantity=shares)
    return MultiLegStrategy(underlying=underlying, options=[option])


def build_vertical(
    *,
    symbol_id: int,
    expiry: datetime,
    lower_strike: float,
    upper_strike: float,
    right: Right = "C",
    qty: int = 1,
) -> MultiLegStrategy:
    if lower_strike >= upper_strike:
        raise ValueError("lower_strike must be < upper_strike")
    long = OptionLeg(symbol_id=symbol_id, expiry=expiry, strike=lower_strike, right=right, quantity=qty)
    short = OptionLeg(symbol_id=symbol_id, expiry=expiry, strike=upper_strike, right=right, quantity=-qty)
    underlying = UnderlyingLeg(symbol_id=symbol_id, quantity=0)
    return MultiLegStrategy(underlying=underlying, options=[long, short])


def roll_rule_on_time(*, now: datetime, current_expiry: datetime, days_before_expiry: int = 5) -> bool:
    delta = current_expiry - now
    return delta.days <= days_before_expiry


def roll_rule_on_delta(*, current_delta: float, target_range: Tuple[float, float] = (0.2, 0.4)) -> bool:
    lo, hi = target_range
    return not (lo <= abs(current_delta) <= hi)