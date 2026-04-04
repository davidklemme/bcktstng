"""Generate synthetic 1-minute OHLCV data for backtesting the Geopolitical Whisper strategy.

Creates a CSV file with realistic 1-minute bars for AAPL including:
- Normal trading volume (~500k per minute)
- Two fabricated "geopolitical whisper" events where volume spikes 5x+ while price stays flat
- One false-positive event where volume spikes but price moves too much

Usage:
    python -m quant.data.dummy.generate_minute_bars
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, time, timedelta
from pathlib import Path

SYMBOL_ID = 1
TICKER = "AAPL"
OUTPUT_DIR = Path(__file__).parent
OUTPUT_FILE = OUTPUT_DIR / "minute_bars.csv"

# Generate 3 trading days: 2024-01-02, 2024-01-03, 2024-01-04
TRADING_DAYS = [
    datetime(2024, 1, 2),
    datetime(2024, 1, 3),
    datetime(2024, 1, 4),
]
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)
BASE_PRICE = 185.00
BASE_VOLUME = 500_000

# Events: (day_index, minute_offset_from_open, vol_multiplier, price_drift_pct)
# Event 1: Clean signal — huge volume, flat price (day 1, 11:00 AM = 90 min after open)
# Event 2: Clean signal — huge volume, flat price (day 2, 2:00 PM = 270 min after open)
# Event 3: False positive — huge volume but price moved 1% (day 3, 10:30 AM = 60 min)
EVENTS = [
    {"day": 1, "minute": 90, "vol_mult": 5.5, "price_drift": 0.001},   # signal
    {"day": 2, "minute": 270, "vol_mult": 6.0, "price_drift": -0.002}, # signal
    {"day": 2, "minute": 60, "vol_mult": 5.0, "price_drift": 0.012},   # NO signal (price moved)
]


def _generate() -> None:
    random.seed(42)
    rows: list[dict] = []
    price = BASE_PRICE

    for day_idx, day in enumerate(TRADING_DAYS):
        current_time = datetime.combine(day, MARKET_OPEN)
        close_time = datetime.combine(day, MARKET_CLOSE)
        minute_idx = 0

        while current_time < close_time:
            # Check if this minute is an event bar
            event = None
            for e in EVENTS:
                if e["day"] == day_idx and e["minute"] == minute_idx:
                    event = e
                    break

            # Normal random walk
            drift = random.gauss(0, 0.0002)  # ~0.02% per minute std dev
            volume = int(BASE_VOLUME * random.uniform(0.7, 1.3))

            if event:
                # Spike volume, control price drift
                volume = int(BASE_VOLUME * event["vol_mult"])
                drift = event["price_drift"]

            new_price = price * (1 + drift)
            high = max(price, new_price) * (1 + abs(random.gauss(0, 0.0001)))
            low = min(price, new_price) * (1 - abs(random.gauss(0, 0.0001)))

            rows.append({
                "ts_local": current_time.strftime("%Y-%m-%d %H:%M"),
                "symbol_id": SYMBOL_ID,
                "open": round(price, 4),
                "high": round(high, 4),
                "low": round(low, 4),
                "close": round(new_price, 4),
                "volume": volume,
            })

            price = new_price
            current_time += timedelta(minutes=1)
            minute_idx += 1

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ts_local", "symbol_id", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Generated {len(rows)} minute bars to {OUTPUT_FILE}")
    # Print event locations for verification
    for e in EVENTS:
        day = TRADING_DAYS[e["day"]]
        event_time = datetime.combine(day, MARKET_OPEN) + timedelta(minutes=e["minute"])
        print(f"  Event: day={day.date()}, time={event_time.strftime('%H:%M')}, "
              f"vol_mult={e['vol_mult']}, price_drift={e['price_drift']}")


if __name__ == "__main__":
    _generate()
