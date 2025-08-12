from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

from ...engine.execution import ExecutionSimulator, Quote
from ...engine.orders import Order, OrderState, OrderType
from ...data.costs import CostCalculator
from .provider import ExecutionProvider, OrderUpdate, ExecutionError


def _tick_size_for_venue(venue: str) -> float:
    v = venue.upper()
    # Simple baseline tick sizes; can be extended per exchange if needed
    if v in ("US", "EU", "UK"):
        return 0.01
    return 0.01


def _enforce_tick(price: float, tick: float) -> float:
    if tick <= 0:
        return price
    # Round to nearest tick
    rounded = round(round(price / tick) * tick, 10)
    return max(0.0, rounded)


@dataclass
class PaperBroker(ExecutionProvider):
    cost_calculator: Optional[CostCalculator] = None
    adv_by_symbol: Optional[Dict[int, int]] = None
    adv_cap_fraction: float = 0.1
    impact_alpha: float = 0.1
    sigma_by_symbol: Optional[Dict[int, float]] = None
    _orders: Dict[str, Order] = field(default_factory=dict)
    _updates: Dict[str, OrderUpdate] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._sim = ExecutionSimulator(
            cost_calculator=self.cost_calculator,
            adv_by_symbol=self.adv_by_symbol,
            adv_cap_fraction=self.adv_cap_fraction,
            impact_alpha=self.impact_alpha,
            sigma_by_symbol=self.sigma_by_symbol,
        )

    def _clone_remaining(self, order: Order) -> Order:
        remaining_qty = max(0, order.quantity - order.filled_quantity)
        return Order(
            id=order.id,
            symbol_id=order.symbol_id,
            side=order.side,
            quantity=remaining_qty,
            type=order.type,
            tif=order.tif,
            limit_price=order.limit_price,
            state=order.state,
            filled_quantity=0,
        )

    def submit(self, order: Order, *, venue: str, quote: Quote, available_liquidity: int) -> OrderUpdate:
        if not isinstance(order, Order):
            raise ExecutionError("submit expects an Order instance")
        # If order id already exists and not final, treat as replace of attributes (limit/qty/type)
        existing = self._orders.get(order.id)
        if existing is not None and existing.state in (OrderState.CANCELED, OrderState.FILLED, OrderState.REJECTED):
            raise ExecutionError(f"Order {order.id} is final and cannot be modified")

        # Enforce tick-size for LIMIT orders
        if order.type == OrderType.LIMIT and order.limit_price is not None:
            tick = _tick_size_for_venue(venue)
            order.limit_price = _enforce_tick(order.limit_price, tick)

        # Register or replace stored order
        if existing is None:
            self._orders[order.id] = order
            order.acknowledge()
        else:
            # Replace attributes on existing order
            existing.type = order.type
            existing.limit_price = order.limit_price
            existing.quantity = order.quantity
            order = existing

        # Simulate fill for remaining quantity only
        temp_order = self._clone_remaining(order)
        fills, cost_total = self._sim.simulate(temp_order, quote, venue, available_liquidity)

        filled_qty = sum(f.quantity for f in fills)
        avg_price: Optional[float] = None
        if filled_qty > 0:
            notional = 0.0
            for f in fills:
                order.add_fill(f.quantity)
                notional += f.quantity * f.price
            avg_price = round(notional / filled_qty, 10)

        # End-of-cycle lifecycle handling
        order.handle_end_of_cycle(available_liquidity)

        update = OrderUpdate(
            order_id=order.id,
            state=order.state,
            filled_quantity=order.filled_quantity,
            avg_fill_price=avg_price,
            cost=cost_total,
        )
        self._updates[order.id] = update
        return update

    def cancel(self, order_id: str) -> OrderUpdate:
        order = self._orders.get(order_id)
        if order is None:
            raise ExecutionError(f"Unknown order_id: {order_id}")
        order.cancel()
        update = OrderUpdate(order_id=order.id, state=order.state, filled_quantity=order.filled_quantity, avg_fill_price=None, cost=0.0)
        self._updates[order.id] = update
        return update

    def status(self, order_id: str) -> OrderUpdate:
        upd = self._updates.get(order_id)
        if upd is not None:
            return upd
        order = self._orders.get(order_id)
        if order is None:
            raise ExecutionError(f"Unknown order_id: {order_id}")
        return OrderUpdate(order_id=order.id, state=order.state, filled_quantity=order.filled_quantity, avg_fill_price=None, cost=0.0)