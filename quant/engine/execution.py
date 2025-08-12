from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from typing import List, Optional, Tuple
from datetime import datetime, timezone

from .orders import Order, OrderSide, OrderType, TimeInForce
from ..data.costs import CostCalculator, Order as CostOrder
from ..data.calendars import EXCHANGE_TZ


@dataclass(frozen=True)
class Quote:
    bid: float
    ask: float

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread(self) -> float:
        return self.ask - self.bid


@dataclass(frozen=True)
class Fill:
    price: float
    quantity: int


class ExecutionSimulator:
    def __init__(
        self,
        *,
        cost_calculator: Optional[CostCalculator] = None,
        adv_by_symbol: Optional[dict[int, int]] = None,
        adv_cap_fraction: float = 0.1,
        impact_alpha: float = 0.1,
        sigma_by_symbol: Optional[dict[int, float]] = None,
        tod_spread_multipliers: Optional[dict[str, float]] = None,
    ) -> None:
        self._costs = cost_calculator
        self._adv = adv_by_symbol or {}
        self._cap = float(adv_cap_fraction)
        self._alpha = float(impact_alpha)
        self._sigma = sigma_by_symbol or {}
        # Time-of-day spread multipliers keyed by bucket label
        # Defaults chosen to widen at open/close, neutral mid-session
        self._tod_mults = tod_spread_multipliers or {
            "OPEN": 1.5,
            "CLOSE": 1.3,
            "MID": 1.0,
        }

    def _tod_bucket(self, venue: str, ts: Optional[datetime]) -> str:
        if ts is None:
            return "MID"
        tz = EXCHANGE_TZ.get("XNYS" if venue.upper() == "US" else ("XETR" if venue.upper() in ("EU", "DE", "XETR") else "XNYS"))
        ts = ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
        local = ts.astimezone(tz) if tz is not None else ts
        # Define buckets: first 30 minutes as OPEN, last 30 minutes as CLOSE
        hour = local.hour
        minute = local.minute
        # Rough session assumptions: XNYS 09:30-16:00, XETR 09:00-17:30
        if tz is not None and tz.key == "America/New_York":
            if (hour == 9 and minute >= 30) or (hour == 10 and minute < 0):
                pass
            # OPEN bucket: 09:30-10:00
            if (hour == 9 and minute >= 30) or (hour == 9 and minute >= 30 and minute < 60) or (hour == 10 and minute == 0):
                if hour == 9 and minute >= 30 or (hour == 10 and minute == 0):
                    if hour == 9 or (hour == 10 and minute == 0):
                        if hour == 9 and minute >= 30 or (hour == 10 and minute == 0):
                            if hour == 9 and minute >= 30 and minute < 60:
                                return "OPEN"
                            if hour == 10 and minute == 0:
                                return "OPEN"
            # CLOSE bucket: 15:30-16:00
            if (hour == 15 and minute >= 30) or (hour == 16 and minute == 0):
                return "CLOSE"
        else:
            # Assume XETR 09:00-17:30
            # OPEN bucket: 09:00-09:30
            if hour == 9 and minute < 30:
                return "OPEN"
            # CLOSE bucket: 17:00-17:30
            if (hour == 17 and minute >= 0 and minute < 30):
                return "CLOSE"
        return "MID"

    def simulate(
        self,
        order: Order,
        quote: Quote,
        venue: str,
        available_liquidity: int,
        *,
        ts: Optional[datetime] = None,
    ) -> Tuple[List[Fill], float]:
        if order.type == OrderType.LIMIT and order.limit_price is None:
            raise ValueError("Limit order missing limit_price")

        # Determine max fillable per ADV cap and available liquidity
        adv = self._adv.get(order.symbol_id, available_liquidity)
        cap = int(max(0, min(available_liquidity, self._cap * adv)))
        max_fillable = min(order.quantity, cap if order.tif != TimeInForce.FOK else order.quantity)

        fills: List[Fill] = []
        filled = 0
        if max_fillable <= 0:
            return fills, 0.0

        # Determine baseline price target within bid/ask
        mid = quote.mid
        spread = quote.spread
        bucket = self._tod_bucket(venue, ts)
        spread_multiplier = self._tod_mults.get(bucket, 1.0)
        effective_spread = spread * spread_multiplier
        urgency_k = 0.5 if order.type == OrderType.LIMIT else 0.75

        # Market impact component
        sigma = self._sigma.get(order.symbol_id, 0.0)
        qty_for_impact = max_fillable
        impact = (1 if order.side == OrderSide.BUY else -1) * sigma * sqrt(max(qty_for_impact, 1) / max(adv, 1)) * self._alpha
        impacted_mid = mid + impact

        # Determine executable price respecting limit and [bid, ask]
        target = impacted_mid + (urgency_k * effective_spread if order.side == OrderSide.BUY else -urgency_k * effective_spread)
        # Clamp to [bid, ask] widened by TOD does not change quoted bounds; keep original quote bounds
        target = min(max(target, quote.bid), quote.ask)

        # Respect limit constraints
        if order.type == OrderType.LIMIT:
            if order.side == OrderSide.BUY and target > order.limit_price:
                target = order.limit_price
            if order.side == OrderSide.SELL and target < order.limit_price:
                target = order.limit_price
            # Still ensure within [bid, ask]
            target = min(max(target, quote.bid), quote.ask)

        # FOK: only fill if full qty available under constraints
        if order.tif == TimeInForce.FOK and max_fillable < order.quantity:
            return [], 0.0

        fill_qty = max_fillable if order.tif != TimeInForce.IOC else min(max_fillable, available_liquidity)
        if fill_qty > 0:
            fills.append(Fill(price=round(target, 10), quantity=int(fill_qty)))
            filled = fill_qty

        # Compute costs
        cost_total = 0.0
        if self._costs and filled > 0:
            cost_total = self._costs.cost(venue, CostOrder(side=order.side.value, qty=filled, price=target))

        return fills, cost_total