from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MKT"
    LIMIT = "LMT"


class TimeInForce(str, Enum):
    DAY = "DAY"
    IOC = "IOC"
    FOK = "FOK"


class OrderState(str, Enum):
    NEW = "NEW"
    WORKING = "WORKING"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"


@dataclass
class Order:
    id: str
    symbol_id: int
    side: OrderSide
    quantity: int
    type: OrderType = OrderType.MARKET
    tif: TimeInForce = TimeInForce.DAY
    limit_price: Optional[float] = None
    state: OrderState = field(default=OrderState.NEW)
    filled_quantity: int = field(default=0)

    def acknowledge(self) -> None:
        if self.state != OrderState.NEW:
            return
        self.state = OrderState.WORKING

    def cancel(self) -> None:
        if self.state in (OrderState.FILLED, OrderState.CANCELED, OrderState.REJECTED):
            return
        self.state = OrderState.CANCELED

    def reject(self) -> None:
        if self.state in (OrderState.FILLED, OrderState.CANCELED, OrderState.REJECTED):
            return
        self.state = OrderState.REJECTED

    def add_fill(self, qty: int) -> None:
        if self.state in (OrderState.CANCELED, OrderState.REJECTED, OrderState.FILLED):
            return
        self.filled_quantity += qty
        if self.filled_quantity >= self.quantity:
            self.filled_quantity = self.quantity
            self.state = OrderState.FILLED
        else:
            self.state = OrderState.PARTIALLY_FILLED

    def handle_end_of_cycle(self, liquidity_available: int) -> None:
        # Lifecycle semantics for IOC/FOK at end of matching cycle
        if self.tif == TimeInForce.IOC and self.state in (OrderState.NEW, OrderState.WORKING, OrderState.PARTIALLY_FILLED):
            if self.filled_quantity < self.quantity:
                self.cancel()
        if self.tif == TimeInForce.FOK and self.state in (OrderState.NEW, OrderState.WORKING, OrderState.PARTIALLY_FILLED):
            # If not fully filled, cancel all
            if self.filled_quantity < self.quantity:
                # Reset fills to zero to emulate FOK behavior if partially filled during cycle
                self.filled_quantity = 0
                self.cancel()