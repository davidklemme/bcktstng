from __future__ import annotations

from dataclasses import dataclass
from typing import Dict
import math
import os

import yaml


@dataclass(frozen=True)
class Order:
    side: str  # 'BUY' or 'SELL'
    qty: int
    price: float


class CostCalculator:
    def __init__(self, profiles: Dict[str, dict]) -> None:
        self._profiles = profiles

    def cost(self, venue: str, order: Order) -> float:
        p = self._profiles.get(venue)
        if p is None:
            raise ValueError(f"Unknown venue: {venue}")

        notional = order.qty * order.price

        if p["type"] == "per_share_plus_fees":
            commission = p.get("commission_per_share", 0.0) * order.qty
            sec_fee = 0.0
            taf_fee = 0.0
            if order.side.upper() == "SELL":
                sec_fee = notional * (p.get("sec_fee_bps", 0.0) / 10000.0)
                taf_fee = p.get("taf_per_share", 0.0) * order.qty
            return round(commission + sec_fee + taf_fee, 10)

        if p["type"] == "bps":
            commission = notional * (p.get("commission_bps", 0.0) / 10000.0)
            return round(commission, 10)

        if p["type"] == "bps_with_stamp":
            commission = notional * (p.get("commission_bps", 0.0) / 10000.0)
            stamp = 0.0
            if p.get("stamp_enabled", True) and order.side.upper() == "BUY":
                stamp = notional * (p.get("stamp_duty_bps", 0.0) / 10000.0)
            return round(commission + stamp, 10)

        raise ValueError(f"Unsupported profile type for venue {venue}: {p.get('type')}")


def load_calculator_from_yaml(yaml_path: str) -> CostCalculator:
    with open(yaml_path, "r") as f:
        data = yaml.safe_load(f) or {}
    return CostCalculator(data)