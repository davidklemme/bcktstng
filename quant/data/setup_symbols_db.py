#!/usr/bin/env python3
"""
Set up symbols database from comprehensive symbols CSV.

This script creates the symbols database and loads the comprehensive symbols CSV into it.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from sqlalchemy import create_engine
from quant.data.symbols_repository import load_symbols_csv_to_db, ensure_schema


def setup_symbols_database(csv_path: str, db_path: str = "data/symbols.db"):
    """
    Set up symbols database from CSV file.
    
    Args:
        csv_path: Path to comprehensive symbols CSV
        db_path: Path to output SQLite database
    """
    
    # Create data directory if it doesn't exist
    db_dir = Path(db_path).parent
    db_dir.mkdir(exist_ok=True)
    
    # Create database engine
    engine = create_engine(f"sqlite:///{db_path}")
    
    # Ensure schema exists
    print(f"Creating schema in {db_path}")
    ensure_schema(engine)
    
    # Load symbols from CSV
    print(f"Loading symbols from {csv_path}")
    count = load_symbols_csv_to_db(csv_path, engine)
    
    print(f"Successfully loaded {count} symbols into database")
    return engine


def main():
    if len(sys.argv) < 2:
        print("Usage: python setup_symbols_db.py <comprehensive_symbols_csv> [db_path]")
        print("Example: python setup_symbols_db.py quant/data/comprehensive_symbols.csv")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    db_path = sys.argv[2] if len(sys.argv) > 2 else "data/symbols.db"
    
    if not Path(csv_path).exists():
        print(f"Error: CSV file {csv_path} does not exist")
        sys.exit(1)
    
    setup_symbols_database(csv_path, db_path)


if __name__ == "__main__":
    main() 