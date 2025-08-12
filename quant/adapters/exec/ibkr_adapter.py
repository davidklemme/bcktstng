from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from ...engine.execution import Quote
from ...engine.orders import Order
from ...data.costs import CostCalculator
from .provider import ExecutionProvider, OrderUpdate, ExecutionError
from .paper_broker import PaperBroker


@dataclass
class IBKRAdapter(ExecutionProvider):
    cost_calculator: Optional[CostCalculator] = None
    dry_run: bool = True
    adv_by_symbol: Optional[Dict[int, int]] = None
    adv_cap_fraction: float = 0.1
    impact_alpha: float = 0.1
    sigma_by_symbol: Optional[Dict[int, float]] = None

    def __post_init__(self) -> None:
        # In dry-run, we back the adapter with a PaperBroker for behavior parity
        self._paper: Optional[PaperBroker] = None
        if self.dry_run:
            self._paper = PaperBroker(
                cost_calculator=self.cost_calculator,
                adv_by_symbol=self.adv_by_symbol,
                adv_cap_fraction=self.adv_cap_fraction,
                impact_alpha=self.impact_alpha,
                sigma_by_symbol=self.sigma_by_symbol,
            )

    def _ensure_live_disabled(self) -> None:
        if not self.dry_run:
            # Guardrail: live mode is not implemented in this skeleton
            raise ExecutionError("IBKR live mode is not available in this skeleton (dry_run=False)")

    def submit(self, order: Order, *, venue: str, quote: Quote, available_liquidity: int) -> OrderUpdate:
        if self.dry_run and self._paper is not None:
            return self._paper.submit(order, venue=venue, quote=quote, available_liquidity=available_liquidity)
        self._ensure_live_disabled()
        # Not reachable due to exception above
        raise ExecutionError("submit not available in live mode")

    def cancel(self, order_id: str) -> OrderUpdate:
        if self.dry_run and self._paper is not None:
            return self._paper.cancel(order_id)
        self._ensure_live_disabled()
        raise ExecutionError("cancel not available in live mode")

    def status(self, order_id: str) -> OrderUpdate:
        if self.dry_run and self._paper is not None:
            return self._paper.status(order_id)
        self._ensure_live_disabled()
        raise ExecutionError("status not available in live mode")