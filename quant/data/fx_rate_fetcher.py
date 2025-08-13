#!/usr/bin/env python3
"""
Fetch historical FX rates from Stooq and save to CSV format.

This script downloads historical currency data from Stooq.com and saves it
in the format expected by the backtesting system.
"""

import csv
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import requests
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class FxDataPoint:
    """A single FX rate data point."""
    date: datetime
    base_ccy: str
    quote_ccy: str
    rate: float


class FxRateFetcher:
    """Fetch historical FX rates from Stooq.com."""
    
    def __init__(self, delay_seconds: float = 1.0):
        self.delay_seconds = delay_seconds
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_stooq_url(self, base_ccy: str, quote_ccy: str) -> str:
        """Generate Stooq URL for currency pair."""
        # Stooq uses format like EURUSD for EUR/USD
        pair = f"{base_ccy}{quote_ccy}"
        return f"https://stooq.com/q/d/l/?s={pair}&d1=20220101&d2=20241231&i=d"
    
    def fetch_currency_data(self, base_ccy: str, quote_ccy: str, 
                          start_date: datetime, end_date: datetime) -> List[FxDataPoint]:
        """Fetch FX data for a currency pair."""
        
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')
        
        url = f"https://stooq.com/q/d/l/?s={base_ccy}{quote_ccy}&d1={start_str}&d2={end_str}&i=d"
        
        logger.debug(f"Fetching {base_ccy}/{quote_ccy} from {start_str} to {end_str}")
        logger.debug(f"URL: {url}")
        
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data_points = []
            lines = response.text.strip().split('\n')
            
            if len(lines) < 2:
                logger.warning(f"No data found for {base_ccy}/{quote_ccy}")
                return []
            
            # Skip header line
            for line in lines[1:]:
                if not line.strip():
                    continue
                    
                parts = line.split(',')
                if len(parts) < 5:
                    continue
                
                try:
                    date_str = parts[0]
                    close_rate = float(parts[4])  # Use close price as FX rate
                    
                    # Parse date
                    date = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                    
                    data_points.append(FxDataPoint(
                        date=date,
                        base_ccy=base_ccy,
                        quote_ccy=quote_ccy,
                        rate=close_rate
                    ))
                    
                except (ValueError, IndexError) as e:
                    logger.warning(f"Failed to parse line for {base_ccy}/{quote_ccy}: {line} - {e}")
                    continue
            
            logger.debug(f"Successfully fetched {len(data_points)} data points for {base_ccy}/{quote_ccy}")
            return data_points
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {base_ccy}/{quote_ccy}: {e}")
            return []
    
    def save_data_to_csv(self, data_points: List[FxDataPoint], csv_path: Path) -> None:
        """Save FX data to CSV file."""
        
        # Create directory if it doesn't exist
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow(['ts', 'base_ccy', 'quote_ccy', 'rate'])
            
            # Write data points
            for data_point in data_points:
                writer.writerow([
                    data_point.date.isoformat(),
                    data_point.base_ccy,
                    data_point.quote_ccy,
                    data_point.rate
                ])
        
        logger.info(f"Saved {len(data_points)} FX data points to {csv_path}")
    
    def fetch_major_currencies(self, output_path: Path, 
                             start_date: Optional[datetime] = None,
                             end_date: Optional[datetime] = None) -> Dict[str, int]:
        """Fetch data for major currency pairs."""
        
        if start_date is None:
            # Go back as far as the oldest symbol data (1990-01-02)
            start_date = datetime(1990, 1, 1, tzinfo=timezone.utc)
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        
        # Major currency pairs to fetch
        currency_pairs = [
            ('USD', 'EUR'),  # US Dollar to Euro
            ('EUR', 'USD'),  # Euro to US Dollar
            ('USD', 'GBP'),  # US Dollar to British Pound
            ('GBP', 'USD'),  # British Pound to US Dollar
            ('USD', 'JPY'),  # US Dollar to Japanese Yen
            ('JPY', 'USD'),  # Japanese Yen to US Dollar
            ('USD', 'CHF'),  # US Dollar to Swiss Franc
            ('CHF', 'USD'),  # Swiss Franc to US Dollar
            ('EUR', 'GBP'),  # Euro to British Pound
            ('GBP', 'EUR'),  # British Pound to Euro
            ('EUR', 'JPY'),  # Euro to Japanese Yen
            ('JPY', 'EUR'),  # Japanese Yen to Euro
            ('EUR', 'CHF'),  # Euro to Swiss Franc
            ('CHF', 'EUR'),  # Swiss Franc to Euro
            ('GBP', 'JPY'),  # British Pound to Japanese Yen
            ('JPY', 'GBP'),  # Japanese Yen to British Pound
        ]
        
        all_data_points = []
        successful_pairs = 0
        failed_pairs = 0
        
        logger.info(f"Starting FX data fetch for {len(currency_pairs)} currency pairs")
        logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        logger.info(f"Delay between requests: {self.delay_seconds} seconds")
        logger.info("-" * 80)
        
        for i, (base_ccy, quote_ccy) in enumerate(currency_pairs):
            progress = f"[{i+1:2d}/{len(currency_pairs):2d}]"
            logger.info(f"{progress} Processing {base_ccy}/{quote_ccy}...")
            
            start_time = time.time()
            
            data_points = self.fetch_currency_data(base_ccy, quote_ccy, start_date, end_date)
            
            if data_points:
                all_data_points.extend(data_points)
                successful_pairs += 1
                logger.info(f"{progress}   ✓ Success: {len(data_points)} data points in {time.time() - start_time:.2f}s")
            else:
                failed_pairs += 1
                logger.warning(f"{progress}   ✗ Failed: No data found for {base_ccy}/{quote_ccy}")
            
            # Delay between requests
            if i < len(currency_pairs) - 1:
                time.sleep(self.delay_seconds)
        
        # Save all data to CSV
        if all_data_points:
            self.save_data_to_csv(all_data_points, output_path)
        
        # Summary
        logger.info("FX FETCH SUMMARY:")
        logger.info(f"  Total currency pairs: {len(currency_pairs)}")
        logger.info(f"  Successful: {successful_pairs}")
        logger.info(f"  Failed: {failed_pairs}")
        logger.info(f"  Total data points: {len(all_data_points)}")
        logger.info(f"  Success rate: {successful_pairs/len(currency_pairs)*100:.1f}%")
        
        return {
            'successful': successful_pairs,
            'failed': failed_pairs,
            'total_points': len(all_data_points)
        }


def main():
    """Main function to fetch FX rates."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Fetch historical FX rates from Stooq')
    parser.add_argument('--output', '-o', default='quant/data/fx_rates.csv', 
                       help='Output CSV file path')
    parser.add_argument('--start-date', default='2022-01-01',
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', default='2024-12-31',
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between requests in seconds')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Configure logging
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    # Parse dates
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    end_date = datetime.strptime(args.end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    
    # Fetch FX rates
    fetcher = FxRateFetcher(delay_seconds=args.delay)
    result = fetcher.fetch_major_currencies(
        output_path=Path(args.output),
        start_date=start_date,
        end_date=end_date
    )
    
    print(f"\nFX rates saved to: {args.output}")
    print(f"Total data points: {result['total_points']}")


if __name__ == "__main__":
    main() 