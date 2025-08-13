#!/usr/bin/env python3
"""
Set up basic FX rates for backtesting.

This script creates basic FX rates for USD/EUR to enable backtesting with real market data.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from datetime import datetime, timezone, timedelta
from sqlalchemy import create_engine, insert
from quant.data.fx_repository import define_fx_table, ensure_schema, MetaData

def setup_fx_rates(db_path: str = "data/fx.db"):
    """
    Set up basic FX rates for USD/EUR.
    
    Args:
        db_path: Path to FX SQLite database
    """
    
    # Create database engine
    engine = create_engine(f"sqlite:///{db_path}")
    
    # Ensure schema exists
    ensure_schema(engine)
    
    # Define table
    metadata = MetaData()
    table = define_fx_table(metadata)
    
    # Create basic FX rates for 2024 (approximate USD/EUR rates)
    # Using a simple rate around 0.85 EUR per USD for 2024
    base_rate = 0.85
    rates = []
    
    # Create daily rates for 2024
    start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_date = datetime(2024, 12, 31, tzinfo=timezone.utc)
    current_date = start_date
    
    while current_date <= end_date:
        # Add some small variation to make it realistic
        import random
        random.seed(hash(current_date.date()))  # Deterministic variation
        variation = random.uniform(-0.02, 0.02)  # ±2% variation
        rate = base_rate + variation
        
        rates.append({
            "ts": current_date,
            "base_ccy": "USD",
            "quote_ccy": "EUR", 
            "rate": rate
        })
        
        current_date += timedelta(days=1)
    
    # Insert rates into database
    with engine.begin() as conn:
        if rates:
            conn.execute(insert(table), rates)
    
    print(f"Created {len(rates)} FX rates for USD/EUR from {start_date.date()} to {end_date.date()}")
    print(f"Rate range: {min(r['rate'] for r in rates):.4f} - {max(r['rate'] for r in rates):.4f} EUR/USD")

if __name__ == "__main__":
    setup_fx_rates()