from __future__ import annotations

import csv
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import requests
import pandas as pd
from dataclasses import dataclass
from sqlalchemy import create_engine, text
from quant.data.symbols_repository import get_symbols_asof

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StooqDataPoint:
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


class StooqDataFetcher:
    """Fetches historical stock data from Stooq with intelligent missing data detection."""
    
    def __init__(self, delay_seconds: float = 1.0, symbols_db_path: Optional[str] = None):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.delay_seconds = delay_seconds
        self.symbols_db_path = symbols_db_path
        self._symbol_cache = {}
    
    def get_symbol_id(self, symbol: str, exchange: str, asof_date: datetime) -> Optional[int]:
        """Get symbol ID from symbols database."""
        if not self.symbols_db_path:
            return None
            
        if (symbol, exchange) not in self._symbol_cache:
            try:
                engine = create_engine(f"sqlite:///{self.symbols_db_path}")
                symbols = get_symbols_asof(engine, asof_date)
                
                # Find matching symbol
                for sym in symbols:
                    if sym.ticker == symbol and sym.exchange == exchange:
                        self._symbol_cache[(symbol, exchange)] = sym.symbol_id
                        break
                else:
                    self._symbol_cache[(symbol, exchange)] = None
                    
            except Exception as e:
                logger.warning(f"Could not get symbol ID for {symbol} ({exchange}): {e}")
                self._symbol_cache[(symbol, exchange)] = None
        
        return self._symbol_cache[(symbol, exchange)]
    
    def get_stooq_url(self, symbol: str, exchange: str) -> str:
        """Generate Stooq URL for a given symbol and exchange."""
        # Map exchange codes to Stooq format
        exchange_map = {
            'XNAS': 'us',  # NASDAQ
            'XNYS': 'us',  # NYSE
            'XLON': 'uk',  # London
            'XETR': 'de',  # Deutsche Börse
            'XTOK': 'jp',  # Tokyo
            'XHKG': 'hk',  # Hong Kong
            'XAMS': 'nl',  # Amsterdam
            'XPAR': 'fr',  # Paris
            'XBRU': 'be',  # Brussels
            'XLIS': 'pt',  # Lisbon
            'XOSL': 'no',  # Oslo
            'XSTO': 'se',  # Stockholm
            'XHEL': 'fi',  # Helsinki
            'XCOP': 'dk',  # Copenhagen
            'XWAR': 'pl',  # Warsaw
            'XPRA': 'cz',  # Prague
            'XBUD': 'hu',  # Budapest
            'XVIE': 'at',  # Vienna
            'XSWX': 'ch',  # Swiss
            'XMIL': 'it',  # Milan
            'XMAD': 'es',  # Madrid
            'XBOM': 'in',  # Bombay
            'XNSE': 'in',  # National Stock Exchange of India
            'XASX': 'au',  # Australian
            'XTSX': 'ca',  # Toronto
            'XSAO': 'br',  # São Paulo
            'XJSE': 'za',  # Johannesburg
            'XTAE': 'il',  # Tel Aviv
        }
        
        stooq_exchange = exchange_map.get(exchange, 'us')
        
        # Handle different symbol formats for Stooq
        if exchange in ['XNAS', 'XNYS']:
            # US symbols need .US suffix
            stooq_symbol = f"{symbol}.US"
        elif exchange == 'XTOK':
            # Japanese symbols are typically numeric, no suffix needed
            stooq_symbol = symbol
        elif exchange == 'XHKG':
            # Hong Kong symbols need .HK suffix
            stooq_symbol = f"{symbol}.HK"
        elif exchange == 'XETR':
            # German symbols need .DE suffix
            stooq_symbol = f"{symbol}.DE"
        elif exchange == 'XLON':
            # UK symbols need .L suffix
            stooq_symbol = f"{symbol}.L"
        elif exchange == 'XAMS':
            # Dutch symbols need .AS suffix
            stooq_symbol = f"{symbol}.AS"
        elif exchange == 'XPAR':
            # French symbols need .PA suffix
            stooq_symbol = f"{symbol}.PA"
        elif exchange == 'XBRU':
            # Belgian symbols need .BR suffix
            stooq_symbol = f"{symbol}.BR"
        elif exchange == 'XSWX':
            # Swiss symbols need .SW suffix
            stooq_symbol = f"{symbol}.SW"
        elif exchange == 'XASX':
            # Australian symbols need .AX suffix
            stooq_symbol = f"{symbol}.AX"
        elif exchange == 'XTSX':
            # Canadian symbols need .TO suffix
            stooq_symbol = f"{symbol}.TO"
        else:
            # Default to symbol as is
            stooq_symbol = symbol
        
        # Stooq URL format: https://stooq.com/q/d/l/?s={symbol}&d1={start_date}&d2={end_date}&i=d
        return f"https://stooq.com/q/d/l/?s={stooq_symbol}&d1={{start_date}}&d2={{end_date}}&i=d"
    
    def fetch_symbol_data(self, symbol: str, exchange: str, start_date: datetime, end_date: datetime) -> List[StooqDataPoint]:
        """Fetch historical data for a single symbol from Stooq."""
        fetch_start_time = time.time()
        
        try:
            url = self.get_stooq_url(symbol, exchange)
            
            # Format dates for Stooq (YYYYMMDD format)
            start_str = start_date.strftime('%Y%m%d')
            end_str = end_date.strftime('%Y%m%d')
            
            full_url = url.format(start_date=start_str, end_date=end_str)
            
            logger.debug(f"Fetching data for {symbol} ({exchange}) from {start_str} to {end_str}")
            logger.debug(f"URL: {full_url}")
            
            response = self.session.get(full_url, timeout=30)
            fetch_time = time.time() - fetch_start_time
            
            if response.status_code == 200:
                # Parse CSV data
                data_points = []
                lines = response.text.strip().split('\n')
                
                if len(lines) > 1:  # Has header and data
                    logger.debug(f"Received {len(lines)-1} data lines for {symbol}")
                    
                    # Skip header line
                    for line_num, line in enumerate(lines[1:], 1):
                        if line.strip():
                            parts = line.split(',')
                            if len(parts) >= 6:
                                try:
                                    date_str = parts[0]
                                    date = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                                    
                                    data_point = StooqDataPoint(
                                        date=date,
                                        open=float(parts[1]),
                                        high=float(parts[2]),
                                        low=float(parts[3]),
                                        close=float(parts[4]),
                                        volume=int(float(parts[5]))  # Handle decimal volume values
                                    )
                                    data_points.append(data_point)
                                except (ValueError, IndexError) as e:
                                    logger.warning(f"Error parsing line {line_num} '{line.strip()}' for {symbol}: {e}")
                                    continue
                else:
                    logger.warning(f"No data lines found for {symbol} (only header)")
                
                total_time = time.time() - fetch_start_time
                logger.debug(f"Successfully fetched {len(data_points)} data points for {symbol} in {total_time:.2f}s (fetch: {fetch_time:.2f}s)")
                return data_points
            else:
                total_time = time.time() - fetch_start_time
                logger.warning(f"Failed to fetch data for {symbol}: HTTP {response.status_code} in {total_time:.2f}s")
                return []
                
        except Exception as e:
            total_time = time.time() - fetch_start_time
            logger.error(f"Error fetching data for {symbol}: {e} (after {total_time:.2f}s)")
            return []
    
    def get_existing_data_dates(self, csv_path: Path, symbol: str) -> Set[datetime]:
        """Get dates that already exist in the CSV file for a given symbol."""
        existing_dates = set()
        
        if not csv_path.exists():
            return existing_dates
        
        try:
            with open(csv_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('symbol') == symbol:
                        try:
                            date = datetime.fromisoformat(row['date'].replace('Z', '+00:00'))
                            existing_dates.add(date.date())  # Use date only for comparison
                        except (ValueError, KeyError):
                            continue
        except Exception as e:
            logger.error(f"Error reading existing data for {symbol}: {e}")
        
        return existing_dates
    
    def fetch_missing_data(self, symbol: str, exchange: str, csv_path: Path, 
                          start_date: datetime, end_date: datetime) -> List[StooqDataPoint]:
        """Fetch only missing data for a symbol."""
        # Get existing dates
        existing_dates = self.get_existing_data_dates(csv_path, symbol)
        
        if existing_dates:
            logger.debug(f"Found {len(existing_dates)} existing data points for {symbol}")
        
        # Fetch all data for the period
        all_data = self.fetch_symbol_data(symbol, exchange, start_date, end_date)
        
        if not all_data:
            logger.debug(f"No data fetched for {symbol}, nothing to check for missing data")
            return []
        
        # Filter out existing data
        missing_data = []
        for data_point in all_data:
            if data_point.date.date() not in existing_dates:
                missing_data.append(data_point)
        
        logger.debug(f"Found {len(missing_data)} missing data points for {symbol} out of {len(all_data)} total")
        
        if missing_data and existing_dates:
            # Show date range of missing data
            missing_dates = sorted([dp.date.date() for dp in missing_data])
            if len(missing_dates) > 0:
                logger.debug(f"Missing data for {symbol}: {missing_dates[0]} to {missing_dates[-1]} ({len(missing_dates)} dates)")
        
        return missing_data
    
    def save_data_to_csv(self, data_points: List[StooqDataPoint], symbol: str, 
                        exchange: str, csv_path: Path, symbol_id: Optional[int] = None) -> None:
        """Save data points to CSV file in bars format."""
        # Create directory if it doesn't exist
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Check if file exists and has header
        file_exists = csv_path.exists()
        
        with open(csv_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write header if file is new
            if not file_exists:
                writer.writerow(['symbol_id', 'dt', 'open', 'high', 'low', 'close', 'volume'])
            
            # Write data points in bars format
            for data_point in data_points:
                writer.writerow([
                    symbol_id if symbol_id is not None else symbol,  # Use symbol_id if provided, otherwise symbol
                    data_point.date.date().isoformat(),  # Just the date part
                    data_point.open,
                    data_point.high,
                    data_point.low,
                    data_point.close,
                    data_point.volume
                ])
        
        logger.info(f"Saved {len(data_points)} data points for {symbol} to {csv_path}")
    
    def fetch_symbols_data(self, symbols: List[Tuple[str, str]], output_path: Path,
                          start_date: Optional[datetime] = None, end_date: Optional[datetime] = None,
                          force_refresh: bool = False) -> Dict[str, int]:
        """Fetch data for multiple symbols with delays."""
        if start_date is None:
            start_date = datetime.now(timezone.utc) - timedelta(days=365)  # Last year
        
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        
        results = {}
        total_symbols = len(symbols)
        successful_symbols = 0
        failed_symbols = 0
        total_data_points = 0
        start_time = time.time()
        
        logger.info(f"Starting batch fetch for {total_symbols} symbols")
        logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        logger.info(f"Force refresh: {force_refresh}")
        logger.info(f"Delay between requests: {self.delay_seconds} seconds")
        logger.info("-" * 80)
        
        for i, (symbol, exchange) in enumerate(symbols):
            symbol_start_time = time.time()
            progress = f"[{i+1:3d}/{total_symbols:3d}]"
            
            logger.info(f"{progress} Processing {symbol} ({exchange})...")
            
            try:
                # Check existing data first
                existing_dates = set()
                if not force_refresh:
                    existing_dates = self.get_existing_data_dates(output_path, symbol)
                    if existing_dates:
                        logger.info(f"{progress}   Found {len(existing_dates)} existing data points for {symbol}")
                
                # Fetch data
                if force_refresh:
                    logger.info(f"{progress}   Fetching all data for {symbol} (force refresh)")
                    data_points = self.fetch_symbol_data(symbol, exchange, start_date, end_date)
                else:
                    logger.info(f"{progress}   Fetching missing data for {symbol}")
                    data_points = self.fetch_missing_data(symbol, exchange, output_path, start_date, end_date)
                
                # Process results
                if data_points:
                    # Get symbol ID if available
                    symbol_id = self.get_symbol_id(symbol, exchange, datetime.now(timezone.utc))
                    self.save_data_to_csv(data_points, symbol, exchange, output_path, symbol_id)
                    results[symbol] = len(data_points)
                    total_data_points += len(data_points)
                    successful_symbols += 1
                    
                    symbol_time = time.time() - symbol_start_time
                    logger.info(f"{progress}   ✓ Success: {len(data_points)} data points saved in {symbol_time:.2f}s")
                else:
                    results[symbol] = 0
                    if not force_refresh and existing_dates:
                        logger.info(f"{progress}   ⚪ Skipped: {symbol} already has complete data")
                    else:
                        logger.warning(f"{progress}   ⚠ No data found for {symbol}")
                
                # Add delay between requests
                if i < len(symbols) - 1:  # Don't delay after the last request
                    logger.info(f"{progress}   Waiting {self.delay_seconds}s before next request...")
                    time.sleep(self.delay_seconds)
                    
            except Exception as e:
                failed_symbols += 1
                results[symbol] = 0
                symbol_time = time.time() - symbol_start_time
                logger.error(f"{progress}   ✗ Error processing {symbol}: {e} (after {symbol_time:.2f}s)")
        
        # Final summary
        total_time = time.time() - start_time
        logger.info("-" * 80)
        logger.info("BATCH FETCH SUMMARY:")
        logger.info(f"  Total symbols processed: {total_symbols}")
        logger.info(f"  Successful: {successful_symbols}")
        logger.info(f"  Failed: {failed_symbols}")
        logger.info(f"  Total data points fetched: {total_data_points}")
        logger.info(f"  Total time: {total_time:.2f} seconds")
        logger.info(f"  Average time per symbol: {total_time/total_symbols:.2f} seconds")
        logger.info(f"  Success rate: {successful_symbols/total_symbols*100:.1f}%")
        
        if failed_symbols > 0:
            logger.warning(f"  Failed symbols: {failed_symbols}")
        
        return results
    
    def load_symbols_from_csv(self, symbols_csv_path: Path) -> List[Tuple[str, str]]:
        """Load symbols from a CSV file."""
        symbols = []
        
        try:
            with open(symbols_csv_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    ticker = row.get('ticker')
                    exchange = row.get('exchange')
                    if ticker and exchange:
                        symbols.append((ticker, exchange))
        except Exception as e:
            logger.error(f"Error loading symbols from {symbols_csv_path}: {e}")
        
        return symbols
    
    def get_data_summary(self, csv_path: Path) -> Dict[str, Dict]:
        """Get summary of existing data in CSV file."""
        summary = {}
        
        if not csv_path.exists():
            return summary
        
        try:
            with open(csv_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    symbol = row.get('symbol')
                    if symbol:
                        if symbol not in summary:
                            summary[symbol] = {
                                'exchange': row.get('exchange', ''),
                                'data_points': 0,
                                'first_date': None,
                                'last_date': None
                            }
                        
                        summary[symbol]['data_points'] += 1
                        
                        try:
                            date = datetime.fromisoformat(row['date'].replace('Z', '+00:00'))
                            if summary[symbol]['first_date'] is None or date < summary[symbol]['first_date']:
                                summary[symbol]['first_date'] = date
                            if summary[symbol]['last_date'] is None or date > summary[symbol]['last_date']:
                                summary[symbol]['last_date'] = date
                        except (ValueError, KeyError):
                            continue
        except Exception as e:
            logger.error(f"Error reading data summary from {csv_path}: {e}")
        
        return summary 