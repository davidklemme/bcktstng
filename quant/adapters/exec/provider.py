from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from ...engine.orders import OrderState


class ExecutionError(Exception):
    """Unified execution error for provider operations."""


@dataclass(frozen=True)
class OrderUpdate:
    order_id: str
    state: OrderState
    filled_quantity: int
    avg_fill_price: Optional[float] = None
    cost: float = 0.0


class ExecutionProvider:
    """Abstract execution provider interface for paper/live adapters."""

    def submit(self, *args, **kwargs) -> OrderUpdate:  # pragma: no cover - interface only
        raise NotImplementedError

    def cancel(self, order_id: str) -> OrderUpdate:  # pragma: no cover - interface only
        raise NotImplementedError

    def status(self, order_id: str) -> OrderUpdate:  # pragma: no cover - interface only
        raise NotImplementedError