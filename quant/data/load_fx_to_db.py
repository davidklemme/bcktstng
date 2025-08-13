#!/usr/bin/env python3
"""
Load FX rates from CSV into SQLite database.

This script loads FX rates from a CSV file into the SQLite database
for use in backtesting.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import create_engine
from quant.data.fx_repository import load_fx_csv_to_db


def load_fx_to_database(csv_path: str, db_path: str = "data/fx.db"):
    """
    Load FX rates from CSV to SQLite database.
    
    Args:
        csv_path: Path to FX rates CSV file
        db_path: Path to output SQLite database
    """
    
    # Create data directory if it doesn't exist
    db_dir = Path(db_path).parent
    db_dir.mkdir(exist_ok=True)
    
    # Create database engine
    engine = create_engine(f"sqlite:///{db_path}")
    
    # Load CSV to database
    count = load_fx_csv_to_db(csv_path, engine)
    
    print(f"Loaded {count} FX rates from {csv_path} to {db_path}")
    
    # Show some statistics
    with engine.connect() as conn:
        from sqlalchemy import text
        
        result = conn.execute(text("SELECT COUNT(*) FROM fx_rates"))
        total_count = result.scalar()
        
        result = conn.execute(text("SELECT base_ccy, quote_ccy, COUNT(*) FROM fx_rates GROUP BY base_ccy, quote_ccy"))
        pairs = result.fetchall()
        
        print(f"\nDatabase statistics:")
        print(f"  Total FX rates: {total_count}")
        print(f"  Currency pairs: {len(pairs)}")
        
        print(f"\nCurrency pairs:")
        for base_ccy, quote_ccy, count in pairs:
            print(f"  {base_ccy}/{quote_ccy}: {count} rates")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python quant/data/load_fx_to_db.py <csv_path>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    load_fx_to_database(csv_path) 