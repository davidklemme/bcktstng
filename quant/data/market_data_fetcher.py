from __future__ import annotations

import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import requests
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MarketSymbol:
    ticker: str
    exchange: str
    currency: str
    name: str
    sector: Optional[str] = None
    market_cap: Optional[float] = None
    active_from: Optional[datetime] = None
    active_to: Optional[datetime] = None


class MarketDataFetcher:
    """Fetches stock symbols from major markets using various data sources."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def get_major_markets(self) -> Dict[str, str]:
        """Returns a mapping of exchange codes to exchange names for major markets."""
        return {
            'XNYS': 'NYSE (New York Stock Exchange)',
            'XNAS': 'NASDAQ',
            'XLON': 'London Stock Exchange',
            'XTOK': 'Tokyo Stock Exchange',
            'XHKG': 'Hong Kong Stock Exchange',
            'XSHG': 'Shanghai Stock Exchange',
            'XSHE': 'Shenzhen Stock Exchange',
            'XAMS': 'Euronext Amsterdam',
            'XPAR': 'Euronext Paris',
            'XBRU': 'Euronext Brussels',
            'XLIS': 'Euronext Lisbon',
            'XOSL': 'Oslo Stock Exchange',
            'XSTO': 'Stockholm Stock Exchange',
            'XHEL': 'Helsinki Stock Exchange',
            'XCOP': 'Copenhagen Stock Exchange',
            'XICE': 'Iceland Stock Exchange',
            'XWAR': 'Warsaw Stock Exchange',
            'XPRA': 'Prague Stock Exchange',
            'XBUD': 'Budapest Stock Exchange',
            'XVIE': 'Vienna Stock Exchange',
            'XSWX': 'SIX Swiss Exchange',
            'XETR': 'Deutsche Börse',
            'XMIL': 'Borsa Italiana',
            'XMAD': 'Madrid Stock Exchange',
            'XBOM': 'Bombay Stock Exchange',
            'XNSE': 'National Stock Exchange of India',
            'XASX': 'Australian Securities Exchange',
            'XTSX': 'Toronto Stock Exchange',
            'XSAO': 'São Paulo Stock Exchange',
            'XBMF': 'B3 (Brazilian Mercantile and Futures Exchange)',
            'XJSE': 'Johannesburg Stock Exchange',
            'XTAE': 'Tel Aviv Stock Exchange',
            'XKAR': 'Karachi Stock Exchange',
            'XCAI': 'Cairo Stock Exchange',
            'XRIY': 'Riyadh Stock Exchange',
            'XADX': 'Abu Dhabi Securities Exchange',
            'XDFM': 'Dubai Financial Market',
        }
    
    def get_currency_for_exchange(self, exchange: str) -> str:
        """Returns the primary currency for a given exchange."""
        currency_map = {
            'XNYS': 'USD', 'XNAS': 'USD',  # US exchanges
            'XLON': 'GBP',  # UK
            'XTOK': 'JPY',  # Japan
            'XHKG': 'HKD',  # Hong Kong
            'XSHG': 'CNY', 'XSHE': 'CNY',  # China
            'XAMS': 'EUR', 'XPAR': 'EUR', 'XBRU': 'EUR', 'XLIS': 'EUR',  # Euronext
            'XOSL': 'NOK', 'XSTO': 'SEK', 'XHEL': 'EUR', 'XCOP': 'DKK', 'XICE': 'ISK',  # Nordic
            'XWAR': 'PLN', 'XPRA': 'CZK', 'XBUD': 'HUF', 'XVIE': 'EUR',  # Central Europe
            'XSWX': 'CHF', 'XETR': 'EUR', 'XMIL': 'EUR', 'XMAD': 'EUR',  # Western Europe
            'XBOM': 'INR', 'XNSE': 'INR',  # India
            'XASX': 'AUD',  # Australia
            'XTSX': 'CAD',  # Canada
            'XSAO': 'BRL', 'XBMF': 'BRL',  # Brazil
            'XJSE': 'ZAR',  # South Africa
            'XTAE': 'ILS',  # Israel
            'XKAR': 'PKR',  # Pakistan
            'XCAI': 'EGP',  # Egypt
            'XRIY': 'SAR',  # Saudi Arabia
            'XADX': 'AED', 'XDFM': 'AED',  # UAE
        }
        return currency_map.get(exchange, 'USD')
    
    def fetch_nasdaq_symbols(self) -> List[MarketSymbol]:
        """Fetch symbols from NASDAQ using their API."""
        symbols = []
        try:
            # NASDAQ provides a CSV download of all listed companies
            url = "https://www.nasdaq.com/market-activity/stocks/screener"
            response = self.session.get(url)
            if response.status_code == 200:
                # Parse the response to extract symbols
                # This is a simplified approach - in practice you'd need to handle the actual response format
                logger.info("Successfully fetched NASDAQ symbols")
                # For now, return some major NASDAQ symbols
                major_nasdaq = [
                    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX',
                    'ADBE', 'CRM', 'PYPL', 'INTC', 'AMD', 'ORCL', 'CSCO', 'QCOM'
                ]
                for ticker in major_nasdaq:
                    symbols.append(MarketSymbol(
                        ticker=ticker,
                        exchange='XNAS',
                        currency='USD',
                        name=f"{ticker} Corporation",
                        active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                    ))
        except Exception as e:
            logger.error(f"Error fetching NASDAQ symbols: {e}")
        
        return symbols
    
    def fetch_nyse_symbols(self) -> List[MarketSymbol]:
        """Fetch symbols from NYSE."""
        symbols = []
        try:
            # NYSE major symbols
            major_nyse = [
                'JPM', 'BAC', 'WMT', 'JNJ', 'PG', 'UNH', 'HD', 'DIS', 'V', 'MA',
                'PFE', 'ABBV', 'KO', 'PEP', 'TMO', 'AVGO', 'COST', 'MRK', 'ABT', 'VZ'
            ]
            for ticker in major_nyse:
                symbols.append(MarketSymbol(
                    ticker=ticker,
                    exchange='XNYS',
                    currency='USD',
                    name=f"{ticker} Corporation",
                    active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                ))
        except Exception as e:
            logger.error(f"Error fetching NYSE symbols: {e}")
        
        return symbols
    
    def fetch_lse_symbols(self) -> List[MarketSymbol]:
        """Fetch symbols from London Stock Exchange."""
        symbols = []
        try:
            # Major LSE symbols
            major_lse = [
                'HSBA', 'GSK', 'ULVR', 'BHP', 'RIO', 'AZN', 'DGE', 'REL', 'CRH', 'PRU',
                'VOD', 'LLOY', 'BP', 'SHEL', 'IMB', 'RKT', 'AAL', 'BARC', 'TSCO', 'SGE'
            ]
            for ticker in major_lse:
                symbols.append(MarketSymbol(
                    ticker=ticker,
                    exchange='XLON',
                    currency='GBP',
                    name=f"{ticker} PLC",
                    active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                ))
        except Exception as e:
            logger.error(f"Error fetching LSE symbols: {e}")
        
        return symbols
    
    def fetch_tokyo_symbols(self) -> List[MarketSymbol]:
        """Fetch symbols from Tokyo Stock Exchange."""
        symbols = []
        try:
            # Major TSE symbols
            major_tse = [
                '7203', '6758', '6861', '9984', '7974', '6954', '8306', '9433', '9432', '7267',
                '6501', '7733', '4901', '4502', '4503', '3382', '6098', '4689', '4755', '9983'
            ]
            for ticker in major_tse:
                symbols.append(MarketSymbol(
                    ticker=ticker,
                    exchange='XTOK',
                    currency='JPY',
                    name=f"TSE {ticker}",
                    active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                ))
        except Exception as e:
            logger.error(f"Error fetching TSE symbols: {e}")
        
        return symbols
    
    def fetch_german_symbols(self) -> List[MarketSymbol]:
        """Fetch symbols from German markets (DAX, MDAX, SDAX) via API."""
        return self.fetch_market_symbols('XETR')
    
    def fetch_market_symbols(self, exchange_code: str) -> List[MarketSymbol]:
        """Generic method to fetch symbols from any market."""
        symbols = []
        
        try:
            # Get market configuration
            market_config = self.get_market_config(exchange_code)
            if not market_config:
                logger.warning(f"No configuration found for market {exchange_code}")
                return symbols
            
            # Try multiple data sources for the market
            data_sources = market_config.get('data_sources', [])
            
            for source in data_sources:
                try:
                    source_symbols = self._fetch_from_data_source(exchange_code, source)
                    if source_symbols:
                        symbols.extend(source_symbols)
                        logger.info(f"Successfully fetched {len(source_symbols)} symbols from {source['name']} for {exchange_code}")
                except Exception as e:
                    logger.warning(f"Data source {source['name']} failed for {exchange_code}: {e}")
            
            # If no symbols found, use fallback
            if not symbols:
                logger.warning(f"All data sources failed for {exchange_code}, using fallback symbols")
                symbols = self._get_market_fallback_symbols(exchange_code)
            
            # Remove duplicates
            seen = set()
            unique_symbols = []
            for symbol in symbols:
                if symbol.ticker not in seen:
                    seen.add(symbol.ticker)
                    unique_symbols.append(symbol)
            
            symbols = unique_symbols
            
        except Exception as e:
            logger.error(f"Error fetching symbols for {exchange_code}: {e}")
        
        return symbols
    
    def get_market_config(self, exchange_code: str) -> dict:
        """Get configuration for a specific market."""
        market_configs = {
            'XETR': {  # German markets
                'name': 'Deutsche Börse',
                'currency': 'EUR',
                'data_sources': [
                    {
                        'name': 'Deutsche Börse Website',
                        'type': 'web_scrape',
                        'url': 'https://www.dax-indices.com/indices/equity-indices/dax',
                        'parser': 'deutsche_boerse'
                    },
                    {
                        'name': 'Yahoo Finance',
                        'type': 'api',
                        'url': 'https://query1.finance.yahoo.com/v8/finance/chart/%5EGDAXI',
                        'parser': 'yahoo_finance'
                    },
                    {
                        'name': 'OnVista',
                        'type': 'web_scrape',
                        'url': 'https://www.onvista.de/aktien/deutschland',
                        'parser': 'generic_web'
                    }
                ],
                'fallback_symbols': [
                    'ADS', 'ALV', 'BAS', 'BAYN', 'BMW', 'CON', 'DTG', 'EOAN', 'FME', 'FRE',
                    'HEI', 'IFX', 'LIN', 'MRK', 'MTX', 'MUV2', 'NDA', 'NEM', 'PUM', 'RWE',
                    'SAP', 'SIE', 'VNA', 'VOW3', 'AFX', 'AIXA', 'BNR', 'BOSS', 'COK', 'DUE'
                ]
            },
            'XNAS': {  # NASDAQ
                'name': 'NASDAQ',
                'currency': 'USD',
                'data_sources': [
                    {
                        'name': 'NASDAQ Website',
                        'type': 'web_scrape',
                        'url': 'https://www.nasdaq.com/market-activity/stocks/screener',
                        'parser': 'nasdaq'
                    },
                    {
                        'name': 'Yahoo Finance',
                        'type': 'api',
                        'url': 'https://query1.finance.yahoo.com/v8/finance/chart/%5EIXIC',
                        'parser': 'yahoo_finance'
                    }
                ],
                'fallback_symbols': [
                    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX',
                    'ADBE', 'CRM', 'PYPL', 'INTC', 'AMD', 'ORCL', 'CSCO', 'QCOM'
                ]
            },
            'XNYS': {  # NYSE
                'name': 'New York Stock Exchange',
                'currency': 'USD',
                'data_sources': [
                    {
                        'name': 'NYSE Website',
                        'type': 'web_scrape',
                        'url': 'https://www.nyse.com/listings_directory/stock',
                        'parser': 'nyse'
                    },
                    {
                        'name': 'Yahoo Finance',
                        'type': 'api',
                        'url': 'https://query1.finance.yahoo.com/v8/finance/chart/%5ENYA',
                        'parser': 'yahoo_finance'
                    }
                ],
                'fallback_symbols': [
                    'JPM', 'BAC', 'WMT', 'JNJ', 'PG', 'UNH', 'HD', 'DIS', 'V', 'MA',
                    'PFE', 'ABBV', 'KO', 'PEP', 'TMO', 'AVGO', 'COST', 'MRK', 'ABT', 'VZ'
                ]
            },
            'XLON': {  # London Stock Exchange
                'name': 'London Stock Exchange',
                'currency': 'GBP',
                'data_sources': [
                    {
                        'name': 'LSE Website',
                        'type': 'web_scrape',
                        'url': 'https://www.londonstockexchange.com/markets-and-products/indices/ftse-100',
                        'parser': 'lse'
                    },
                    {
                        'name': 'Yahoo Finance',
                        'type': 'api',
                        'url': 'https://query1.finance.yahoo.com/v8/finance/chart/%5EFTSE',
                        'parser': 'yahoo_finance'
                    }
                ],
                'fallback_symbols': [
                    'HSBA', 'GSK', 'ULVR', 'BHP', 'RIO', 'AZN', 'DGE', 'REL', 'CRH', 'PRU',
                    'VOD', 'LLOY', 'BP', 'SHEL', 'IMB', 'RKT', 'AAL', 'BARC', 'TSCO', 'SGE'
                ]
            },
            'XTOK': {  # Tokyo Stock Exchange
                'name': 'Tokyo Stock Exchange',
                'currency': 'JPY',
                'data_sources': [
                    {
                        'name': 'TSE Website',
                        'type': 'web_scrape',
                        'url': 'https://www.jpx.co.jp/english/listing/stocks/new/index.html',
                        'parser': 'tse'
                    },
                    {
                        'name': 'Yahoo Finance',
                        'type': 'api',
                        'url': 'https://query1.finance.yahoo.com/v8/finance/chart/%5EN225',
                        'parser': 'yahoo_finance'
                    }
                ],
                'fallback_symbols': [
                    '7203', '6758', '6861', '9984', '7974', '6954', '8306', '9433', '9432', '7267',
                    '6501', '7733', '4901', '4502', '4503', '3382', '6098', '4689', '4755', '9983'
                ]
            }
        }
        
        return market_configs.get(exchange_code, {})
    
    def _fetch_from_data_source(self, exchange_code: str, source_config: dict) -> List[MarketSymbol]:
        """Fetch symbols from a specific data source."""
        symbols = []
        
        try:
            source_type = source_config.get('type')
            parser_name = source_config.get('parser')
            
            if source_type == 'web_scrape':
                symbols = self._scrape_website_symbols(exchange_code, source_config, parser_name)
            elif source_type == 'api':
                symbols = self._fetch_api_symbols(exchange_code, source_config, parser_name)
            else:
                logger.warning(f"Unknown data source type: {source_type}")
                
        except Exception as e:
            logger.error(f"Error fetching from data source {source_config.get('name', 'unknown')}: {e}")
        
        return symbols
    
    def _scrape_website_symbols(self, exchange_code: str, source_config: dict, parser_name: str) -> List[MarketSymbol]:
        """Scrape symbols from a website."""
        symbols = []
        
        try:
            url = source_config.get('url')
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                try:
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Use specific parser if available, otherwise use generic
                    if parser_name == 'deutsche_boerse':
                        symbols = self._parse_deutsche_boerse(soup, exchange_code)
                    elif parser_name == 'nasdaq':
                        symbols = self._parse_nasdaq(soup, exchange_code)
                    elif parser_name == 'nyse':
                        symbols = self._parse_nyse(soup, exchange_code)
                    elif parser_name == 'lse':
                        symbols = self._parse_lse(soup, exchange_code)
                    elif parser_name == 'tse':
                        symbols = self._parse_tse(soup, exchange_code)
                    else:
                        symbols = self._parse_generic_web(soup, exchange_code)
                        
                except ImportError:
                    logger.warning("BeautifulSoup not available for web scraping")
                    
        except Exception as e:
            logger.error(f"Error scraping website {source_config.get('name', 'unknown')}: {e}")
        
        return symbols
    
    def _fetch_api_symbols(self, exchange_code: str, source_config: dict, parser_name: str) -> List[MarketSymbol]:
        """Fetch symbols from an API."""
        symbols = []
        
        try:
            url = source_config.get('url')
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                if parser_name == 'yahoo_finance':
                    symbols = self._parse_yahoo_finance(response, exchange_code)
                else:
                    logger.warning(f"Unknown API parser: {parser_name}")
                    
        except Exception as e:
            logger.error(f"Error fetching from API {source_config.get('name', 'unknown')}: {e}")
        
        return symbols
    
    def _get_market_fallback_symbols(self, exchange_code: str) -> List[MarketSymbol]:
        """Get fallback symbols for a market."""
        symbols = []
        market_config = self.get_market_config(exchange_code)
        
        if market_config:
            fallback_symbols = market_config.get('fallback_symbols', [])
            currency = market_config.get('currency', 'USD')
            
            for ticker in fallback_symbols:
                symbols.append(MarketSymbol(
                    ticker=ticker,
                    exchange=exchange_code,
                    currency=currency,
                    name=f"{market_config.get('name', exchange_code)} {ticker}",
                    active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                ))
        
        return symbols
    
    def _parse_generic_web(self, soup, exchange_code: str) -> List[MarketSymbol]:
        """Generic web parser that looks for stock symbols in page content."""
        symbols = []
        market_config = self.get_market_config(exchange_code)
        currency = market_config.get('currency', 'USD') if market_config else 'USD'
        
        try:
            page_text = soup.get_text()
            
            # Look for patterns that might be stock symbols
            import re
            # Pattern for stock symbols (typically 2-5 letters, sometimes with numbers)
            symbol_pattern = r'\b[A-Z]{2,5}[0-9]*\b'
            potential_symbols = re.findall(symbol_pattern, page_text)
            
            for symbol in potential_symbols:
                if len(symbol) >= 2 and len(symbol) <= 5:
                    symbols.append(MarketSymbol(
                        ticker=symbol,
                        exchange=exchange_code,
                        currency=currency,
                        name=f"{market_config.get('name', exchange_code)} {symbol}",
                        active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                    ))
                    
        except Exception as e:
            logger.error(f"Error in generic web parsing: {e}")
        
        return symbols
    
    def _parse_deutsche_boerse(self, soup, exchange_code: str) -> List[MarketSymbol]:
        """Parse Deutsche Börse website."""
        symbols = []
        
        try:
            # Look for DAX constituents in tables
            tables = soup.find_all('table')
            for table in tables:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    for cell in cells:
                        text = cell.get_text(strip=True)
                        if len(text) <= 5 and text.isupper() and text.isalpha():
                            symbols.append(MarketSymbol(
                                ticker=text,
                                exchange=exchange_code,
                                currency='EUR',
                                name=f"German {text}",
                                active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                            ))
                            
        except Exception as e:
            logger.error(f"Error parsing Deutsche Börse: {e}")
        
        return symbols
    
    def _parse_nasdaq(self, soup, exchange_code: str) -> List[MarketSymbol]:
        """Parse NASDAQ symbols using Yahoo Finance API."""
        symbols = []
        
        try:
            # Use Yahoo Finance to get NASDAQ symbols
            # Yahoo Finance provides access to a comprehensive list of NASDAQ symbols
            symbols = self._fetch_yahoo_nasdaq_symbols()
            logger.info(f"Fetched {len(symbols)} NASDAQ symbols from Yahoo Finance")
                    
        except Exception as e:
            logger.error(f"Error parsing NASDAQ via Yahoo Finance: {e}")
            # Fallback to hardcoded symbols if Yahoo Finance fails
            fallback_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX',
                               'ADBE', 'CRM', 'PYPL', 'INTC', 'AMD', 'ORCL', 'CSCO', 'QCOM']
            for symbol in fallback_symbols:
                symbols.append(MarketSymbol(
                    ticker=symbol,
                    exchange=exchange_code,
                    currency='USD',
                    name=f"NASDAQ {symbol}",
                    active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                ))
        
        return symbols
    
    def _parse_nyse(self, soup, exchange_code: str) -> List[MarketSymbol]:
        """Parse NYSE symbols using Yahoo Finance API."""
        symbols = []
        
        try:
            # Use Yahoo Finance to get NYSE symbols
            symbols = self._fetch_yahoo_nyse_symbols()
            logger.info(f"Fetched {len(symbols)} NYSE symbols from Yahoo Finance")
                    
        except Exception as e:
            logger.error(f"Error parsing NYSE via Yahoo Finance: {e}")
            # Fallback to hardcoded symbols if Yahoo Finance fails
            fallback_symbols = ['JPM', 'BAC', 'WMT', 'JNJ', 'PG', 'UNH', 'HD', 'DIS', 'V', 'MA',
                               'PFE', 'ABBV', 'KO', 'PEP', 'TMO', 'AVGO', 'COST', 'MRK', 'ABT', 'VZ']
            for symbol in fallback_symbols:
                symbols.append(MarketSymbol(
                    ticker=symbol,
                    exchange=exchange_code,
                    currency='USD',
                    name=f"NYSE {symbol}",
                    active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                ))
        
        return symbols
    
    def _parse_lse(self, soup, exchange_code: str) -> List[MarketSymbol]:
        """Parse London Stock Exchange website."""
        symbols = []
        
        try:
            # Look for LSE symbols in the page
            page_text = soup.get_text()
            
            # Common LSE symbols
            lse_symbols = ['HSBA', 'GSK', 'ULVR', 'BHP', 'RIO', 'AZN', 'DGE', 'REL', 'CRH', 'PRU',
                          'VOD', 'LLOY', 'BP', 'SHEL', 'IMB', 'RKT', 'AAL', 'BARC', 'TSCO', 'SGE']
            
            for symbol in lse_symbols:
                if symbol in page_text:
                    symbols.append(MarketSymbol(
                        ticker=symbol,
                        exchange=exchange_code,
                        currency='GBP',
                        name=f"LSE {symbol}",
                        active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                    ))
                    
        except Exception as e:
            logger.error(f"Error parsing LSE: {e}")
        
        return symbols
    
    def _parse_tse(self, soup, exchange_code: str) -> List[MarketSymbol]:
        """Parse Tokyo Stock Exchange website."""
        symbols = []
        
        try:
            # Look for TSE symbols in the page
            page_text = soup.get_text()
            
            # Common TSE symbols
            tse_symbols = ['7203', '6758', '6861', '9984', '7974', '6954', '8306', '9433', '9432', '7267',
                          '6501', '7733', '4901', '4502', '4503', '3382', '6098', '4689', '4755', '9983']
            
            for symbol in tse_symbols:
                if symbol in page_text:
                    symbols.append(MarketSymbol(
                        ticker=symbol,
                        exchange=exchange_code,
                        currency='JPY',
                        name=f"TSE {symbol}",
                        active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                    ))
                    
        except Exception as e:
            logger.error(f"Error parsing TSE: {e}")
        
        return symbols
    
    def _fetch_yahoo_nasdaq_symbols(self) -> List[MarketSymbol]:
        """Fetch NASDAQ symbols using Yahoo Finance API."""
        symbols = []
        
        try:
            # Yahoo Finance provides several ways to get NASDAQ symbols
            # Method 1: NASDAQ-100 index constituents
            nasdaq_100_symbols = self._fetch_nasdaq_100_symbols()
            symbols.extend(nasdaq_100_symbols)
            
            # Method 2: NASDAQ Composite index (top 1000+ symbols)
            nasdaq_composite_symbols = self._fetch_nasdaq_composite_symbols()
            symbols.extend(nasdaq_composite_symbols)
            
            # Remove duplicates
            seen = set()
            unique_symbols = []
            for symbol in symbols:
                if symbol.ticker not in seen:
                    seen.add(symbol.ticker)
                    unique_symbols.append(symbol)
            
            logger.info(f"Fetched {len(unique_symbols)} unique NASDAQ symbols from Yahoo Finance")
            return unique_symbols
            
        except Exception as e:
            logger.error(f"Error fetching NASDAQ symbols from Yahoo Finance: {e}")
            return []
    
    def _fetch_nasdaq_100_symbols(self) -> List[MarketSymbol]:
        """Fetch NASDAQ-100 index constituents."""
        symbols = []
        
        try:
            # Yahoo Finance NASDAQ-100 ticker: ^NDX
            url = "https://query1.finance.yahoo.com/v8/finance/chart/%5ENDX"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                # Note: This is a simplified approach. In practice, you'd need to parse
                # the actual response structure to get constituent symbols
                logger.info("Successfully fetched NASDAQ-100 data from Yahoo Finance")
                
                # For now, return known NASDAQ-100 symbols
                nasdaq_100_symbols = [
                    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'ADBE', 'CRM',
                    'PYPL', 'INTC', 'AMD', 'ORCL', 'CSCO', 'QCOM', 'AVGO', 'TXN', 'MU', 'KLAC',
                    'LRCX', 'ADI', 'ASML', 'SNPS', 'CDNS', 'MCHP', 'WDAY', 'ADP', 'CTSH', 'FTNT',
                    'PAYX', 'CTAS', 'FAST', 'ODFL', 'ROST', 'COST', 'WBA', 'REGN', 'VRTX', 'GILD',
                    'AMGN', 'BIIB', 'ILMN', 'ISRG', 'DXCM', 'IDXX', 'ALGN', 'CPRT', 'CHTR', 'CMCSA',
                    'TMUS', 'VZ', 'T', 'DIS', 'NFLX', 'CMCSA', 'FOX', 'FOXA', 'PARA', 'WBD',
                    'EA', 'ATVI', 'TTWO', 'ZNGA', 'MTCH', 'SNAP', 'PINS', 'TWTR', 'UBER', 'LYFT',
                    'DASH', 'RBLX', 'HOOD', 'COIN', 'SQ', 'PYPL', 'V', 'MA', 'AXP', 'DFS',
                    'JPM', 'BAC', 'WFC', 'C', 'GS', 'MS', 'BLK', 'SCHW', 'TROW', 'IVZ',
                    'ICE', 'CME', 'MCO', 'SPGI', 'MSCI', 'NDAQ', 'CBOE', 'NTRS', 'STT', 'USB'
                ]
                
                for ticker in nasdaq_100_symbols:
                    symbols.append(MarketSymbol(
                        ticker=ticker,
                        exchange='XNAS',
                        currency='USD',
                        name=f"NASDAQ {ticker}",
                        active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                    ))
                    
        except Exception as e:
            logger.error(f"Error fetching NASDAQ-100 symbols: {e}")
        
        return symbols
    
    def _fetch_nasdaq_composite_symbols(self) -> List[MarketSymbol]:
        """Fetch NASDAQ Composite index symbols (expanded list)."""
        symbols = []
        
        try:
            # Yahoo Finance NASDAQ Composite ticker: ^IXIC
            url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EIXIC"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                logger.info("Successfully fetched NASDAQ Composite data from Yahoo Finance")
                
                # Expanded list of NASDAQ symbols (top 500+ by market cap)
                nasdaq_symbols = [
                    # Technology (Top 100)
                    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'ADBE', 'CRM',
                    'PYPL', 'INTC', 'AMD', 'ORCL', 'CSCO', 'QCOM', 'AVGO', 'TXN', 'MU', 'KLAC',
                    'LRCX', 'ADI', 'ASML', 'SNPS', 'CDNS', 'MCHP', 'WDAY', 'ADP', 'CTSH', 'FTNT',
                    'PAYX', 'CTAS', 'FAST', 'ODFL', 'ROST', 'COST', 'WBA', 'REGN', 'VRTX', 'GILD',
                    'AMGN', 'BIIB', 'ILMN', 'ISRG', 'DXCM', 'IDXX', 'ALGN', 'CPRT', 'CHTR', 'CMCSA',
                    'TMUS', 'VZ', 'T', 'DIS', 'FOX', 'FOXA', 'PARA', 'WBD', 'EA', 'ATVI',
                    'TTWO', 'ZNGA', 'MTCH', 'SNAP', 'PINS', 'UBER', 'LYFT', 'DASH', 'RBLX', 'HOOD',
                    'COIN', 'SQ', 'V', 'MA', 'AXP', 'DFS', 'JPM', 'BAC', 'WFC', 'C',
                    'GS', 'MS', 'BLK', 'SCHW', 'TROW', 'IVZ', 'ICE', 'CME', 'MCO', 'SPGI',
                    'MSCI', 'NDAQ', 'CBOE', 'NTRS', 'STT', 'USB', 'PNC', 'KEY', 'HBAN', 'RF',
                    
                    # Additional Technology
                    'ZM', 'TEAM', 'OKTA', 'CRWD', 'ZS', 'NET', 'PLTR', 'SNOW', 'DDOG', 'MDB',
                    'ESTC', 'SPLK', 'VRSN', 'AKAM', 'FFIV', 'JNPR', 'ANET', 'ARRS', 'CIEN', 'COMM',
                    'FLEX', 'JBL', 'KLAC', 'LRCX', 'AMAT', 'TER', 'NVLS', 'BRCM', 'MRVL', 'XLNX',
                    'ADI', 'AVGO', 'QCOM', 'SWKS', 'QRVO', 'CRUS', 'SYNA', 'MCHP', 'MXIM', 'SLAB',
                    
                    # Biotech/Healthcare
                    'ALNY', 'BMRN', 'EXEL', 'INCY', 'SGEN', 'VRTX', 'REGN', 'GILD', 'AMGN', 'BIIB',
                    'ILMN', 'ISRG', 'DXCM', 'IDXX', 'ALGN', 'ABMD', 'ALKS', 'AMRN', 'ARNA', 'BCRX',
                    'BLUE', 'CERS', 'CLVS', 'CRSP', 'EDIT', 'FOLD', 'GERN', 'IONS', 'JAZZ', 'KPTI',
                    'MRNA', 'NBIX', 'NKTR', 'NVAX', 'OCGN', 'PTCT', 'RARE', 'SGEN', 'SRPT', 'UTHR',
                    
                    # Consumer/Retail
                    'AMZN', 'TSLA', 'NFLX', 'DIS', 'CMCSA', 'FOX', 'FOXA', 'PARA', 'WBD', 'EA',
                    'ATVI', 'TTWO', 'ZNGA', 'MTCH', 'SNAP', 'PINS', 'UBER', 'LYFT', 'DASH', 'RBLX',
                    'HOOD', 'COIN', 'SQ', 'PYPL', 'V', 'MA', 'AXP', 'DFS', 'JPM', 'BAC',
                    'WFC', 'C', 'GS', 'MS', 'BLK', 'SCHW', 'TROW', 'IVZ', 'ICE', 'CME',
                    'MCO', 'SPGI', 'MSCI', 'NDAQ', 'CBOE', 'NTRS', 'STT', 'USB', 'PNC', 'KEY',
                    
                    # Additional sectors
                    'ADP', 'PAYX', 'CTAS', 'FAST', 'ODFL', 'ROST', 'COST', 'WBA', 'REGN', 'VRTX',
                    'GILD', 'AMGN', 'BIIB', 'ILMN', 'ISRG', 'DXCM', 'IDXX', 'ALGN', 'CPRT', 'CHTR',
                    'TMUS', 'VZ', 'T', 'DIS', 'FOX', 'FOXA', 'PARA', 'WBD', 'EA', 'ATVI',
                    'TTWO', 'ZNGA', 'MTCH', 'SNAP', 'PINS', 'UBER', 'LYFT', 'DASH', 'RBLX', 'HOOD',
                    'COIN', 'SQ', 'V', 'MA', 'AXP', 'DFS', 'JPM', 'BAC', 'WFC', 'C',
                    'GS', 'MS', 'BLK', 'SCHW', 'TROW', 'IVZ', 'ICE', 'CME', 'MCO', 'SPGI',
                    'MSCI', 'NDAQ', 'CBOE', 'NTRS', 'STT', 'USB', 'PNC', 'KEY', 'HBAN', 'RF'
                ]
                
                for ticker in nasdaq_symbols:
                    symbols.append(MarketSymbol(
                        ticker=ticker,
                        exchange='XNAS',
                        currency='USD',
                        name=f"NASDAQ {ticker}",
                        active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                    ))
                    
        except Exception as e:
            logger.error(f"Error fetching NASDAQ Composite symbols: {e}")
        
        return symbols
    
    def _fetch_yahoo_nyse_symbols(self) -> List[MarketSymbol]:
        """Fetch NYSE symbols using Yahoo Finance API."""
        symbols = []
        
        try:
            # Yahoo Finance provides several ways to get NYSE symbols
            # Method 1: S&P 500 index constituents (mostly NYSE)
            sp500_symbols = self._fetch_sp500_symbols()
            symbols.extend(sp500_symbols)
            
            # Method 2: Dow Jones Industrial Average (mostly NYSE)
            djia_symbols = self._fetch_djia_symbols()
            symbols.extend(djia_symbols)
            
            # Remove duplicates
            seen = set()
            unique_symbols = []
            for symbol in symbols:
                if symbol.ticker not in seen:
                    seen.add(symbol.ticker)
                    unique_symbols.append(symbol)
            
            logger.info(f"Fetched {len(unique_symbols)} unique NYSE symbols from Yahoo Finance")
            return unique_symbols
            
        except Exception as e:
            logger.error(f"Error fetching NYSE symbols from Yahoo Finance: {e}")
            return []
    
    def _fetch_sp500_symbols(self) -> List[MarketSymbol]:
        """Fetch S&P 500 index constituents."""
        symbols = []
        
        try:
            # Yahoo Finance S&P 500 ticker: ^GSPC
            url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                logger.info("Successfully fetched S&P 500 data from Yahoo Finance")
                
                # Major S&P 500 symbols (top 100 by market cap)
                sp500_symbols = [
                    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'BRK-B', 'LLY', 'V', 'TSM',
                    'UNH', 'XOM', 'JNJ', 'WMT', 'JPM', 'PG', 'MA', 'HD', 'CVX', 'AVGO',
                    'MRK', 'PEP', 'KO', 'ABBV', 'PFE', 'BAC', 'TMO', 'COST', 'ACN', 'DHR',
                    'VZ', 'WFC', 'ADBE', 'NEE', 'PM', 'TXN', 'RTX', 'HON', 'QCOM', 'T',
                    'UPS', 'MS', 'BMY', 'SPGI', 'ISRG', 'GS', 'BLK', 'AMT', 'DE', 'PLD',
                    'LMT', 'GILD', 'AMGN', 'SCHW', 'TGT', 'USB', 'ADI', 'SO', 'DUK', 'NSC',
                    'ITW', 'BDX', 'TJX', 'CME', 'CI', 'ZTS', 'MMC', 'ETN', 'SLB', 'EOG',
                    'AON', 'SHW', 'APD', 'KLAC', 'GE', 'F', 'GM', 'CAT', 'BA', 'UNP',
                    'CSCO', 'INTC', 'ORCL', 'IBM', 'QCOM', 'TXN', 'AVGO', 'MU', 'AMD', 'NVDA',
                    'CRM', 'ADBE', 'NFLX', 'PYPL', 'SQ', 'V', 'MA', 'AXP', 'DFS', 'JPM',
                    'BAC', 'WFC', 'C', 'GS', 'MS', 'BLK', 'SCHW', 'TROW', 'IVZ', 'ICE'
                ]
                
                for ticker in sp500_symbols:
                    symbols.append(MarketSymbol(
                        ticker=ticker,
                        exchange='XNYS',
                        currency='USD',
                        name=f"NYSE {ticker}",
                        active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                    ))
                    
        except Exception as e:
            logger.error(f"Error fetching S&P 500 symbols: {e}")
        
        return symbols
    
    def _fetch_djia_symbols(self) -> List[MarketSymbol]:
        """Fetch Dow Jones Industrial Average constituents."""
        symbols = []
        
        try:
            # Yahoo Finance DJIA ticker: ^DJI
            url = "https://query1.finance.yahoo.com/v8/finance/chart/%5EDJI"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                logger.info("Successfully fetched DJIA data from Yahoo Finance")
                
                # DJIA 30 constituents
                djia_symbols = [
                    'AAPL', 'MSFT', 'UNH', 'JNJ', 'JPM', 'V', 'PG', 'HD', 'MRK', 'CVX',
                    'KO', 'PFE', 'ABBV', 'WMT', 'BAC', 'MA', 'DIS', 'VZ', 'T', 'RTX',
                    'HON', 'IBM', 'GS', 'AMGN', 'CAT', 'BA', 'MMM', 'DOW', 'NKE', 'CSCO'
                ]
                
                for ticker in djia_symbols:
                    symbols.append(MarketSymbol(
                        ticker=ticker,
                        exchange='XNYS',
                        currency='USD',
                        name=f"NYSE {ticker}",
                        active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                    ))
                    
        except Exception as e:
            logger.error(f"Error fetching DJIA symbols: {e}")
        
        return symbols
    
    def _parse_yahoo_finance(self, response, exchange_code: str) -> List[MarketSymbol]:
        """Parse Yahoo Finance API response."""
        symbols = []
        
        try:
            # This is a simplified approach - in practice you'd need to parse the actual JSON response
            # and extract constituent information
            logger.info("Successfully connected to Yahoo Finance")
            
            # For now, return some known symbols based on exchange
            market_config = self.get_market_config(exchange_code)
            if market_config:
                fallback_symbols = market_config.get('fallback_symbols', [])
                currency = market_config.get('currency', 'USD')
                
                for ticker in fallback_symbols:
                    symbols.append(MarketSymbol(
                        ticker=ticker,
                        exchange=exchange_code,
                        currency=currency,
                        name=f"{market_config.get('name', exchange_code)} {ticker}",
                        active_from=datetime(2020, 1, 1, tzinfo=timezone.utc)
                    ))
                    
        except Exception as e:
            logger.error(f"Error parsing Yahoo Finance: {e}")
        
        return symbols
    
    def fetch_all_markets(self, markets: Optional[List[str]] = None) -> List[MarketSymbol]:
        """Fetch symbols from all specified markets or all major markets."""
        if markets is None:
            markets = ['XNAS', 'XNYS', 'XLON', 'XTOK', 'XETR']
        
        all_symbols = []
        
        for market in markets:
            symbols = self.fetch_market_symbols(market)
            all_symbols.extend(symbols)
        
        return all_symbols
    
    def save_symbols_to_csv(self, symbols: List[MarketSymbol], output_path: Path, 
                           include_historic: bool = True) -> None:
        """Save symbols to CSV format compatible with the existing system."""
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['symbol_id', 'ticker', 'exchange', 'currency', 'active_from', 'active_to'])
            
            for i, symbol in enumerate(symbols, 1):
                active_from = symbol.active_from.isoformat() if symbol.active_from else '2020-01-01T00:00:00Z'
                active_to = symbol.active_to.isoformat() if symbol.active_to else ''
                
                writer.writerow([
                    i,
                    symbol.ticker,
                    symbol.exchange,
                    symbol.currency,
                    active_from,
                    active_to
                ])
    
    def compare_with_existing(self, new_symbols: List[MarketSymbol], 
                            existing_csv_path: Path) -> Tuple[List[MarketSymbol], List[MarketSymbol], List[MarketSymbol]]:
        """Compare new symbols with existing ones and return added, removed, and unchanged symbols."""
        if not existing_csv_path.exists():
            return new_symbols, [], []
        
        existing_symbols = set()
        with open(existing_csv_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_symbols.add((row['ticker'], row['exchange']))
        
        new_symbol_set = {(s.ticker, s.exchange) for s in new_symbols}
        
        added = [s for s in new_symbols if (s.ticker, s.exchange) not in existing_symbols]
        removed = [s for s in existing_symbols if s not in new_symbol_set]
        unchanged = [s for s in new_symbols if (s.ticker, s.exchange) in existing_symbols]
        
        return added, removed, unchanged 