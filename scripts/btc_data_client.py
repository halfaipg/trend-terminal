"""
Bitcoin Data Client for Polygon.io

Fetches Bitcoin OHLCV data from Polygon.io and stores in TimescaleDB.
Provides real-time crypto data with low latency.
"""
import os
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
from loguru import logger
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine
import requests
from polygon import RESTClient

# Import from symlinked backend
import sys
sys.path.append('backend')
from backend.db.database import get_db
from backend.utils.config import settings


class BitcoinDataClient:
    """
    Client for fetching Bitcoin data from Polygon.io
    
    Polygon Bitcoin Symbol: X:BTCUSD (crypto ticker)
    """
    
    def __init__(self, api_key: Optional[str] = None, db_url: Optional[str] = None):
        """
        Initialize Bitcoin data client
        
        Args:
            api_key: Polygon API key (or set POLYGON_API_KEY env var)
            db_url: PostgreSQL connection URL (or set DATABASE_URL env var)
        """
        self.api_key = api_key or os.getenv('POLYGON_API_KEY')
        if not self.api_key:
            raise ValueError("Polygon API key required. Set POLYGON_API_KEY env var or pass api_key parameter.")
        
        self.client = RESTClient(self.api_key)
        
        # Database connection
        self.db_url = db_url or os.getenv('DATABASE_URL')
        if self.db_url:
            try:
                self.engine = create_engine(self.db_url)
                self.db_conn = psycopg2.connect(self.db_url)
                logger.info("✓ Connected to TimescaleDB for Bitcoin data")
            except Exception as e:
                logger.warning(f"Database connection failed: {e}")
                self.engine = None
                self.db_conn = None
        else:
            logger.warning("No DATABASE_URL provided. Data will only be saved to files.")
            self.engine = None
            self.db_conn = None
        
        # Bitcoin asset metadata
        # Note: Databento uses different symbols - we'll use CME Bitcoin futures
        # BRN for CME Bitcoin (GLBX.MDP3 dataset)
        self.btc_assets = {
            'BTC': {
                'name': 'Bitcoin CME Futures',
                'exchange': 'CME',
                'tick_size': 5.0,  # $5 minimum price movement
                'multiplier': 5,   # $5 per point
                'currency': 'USD',
                'databento_symbol': 'BRN',  # CME Bitcoin root symbol
                'dataset': 'GLBX.MDP3'
            },
            'BTCUSD': {
                'name': 'Bitcoin Spot',
                'exchange': 'COINBASE',
                'tick_size': 0.01,
                'multiplier': 1,
                'currency': 'USD',
                'databento_symbol': 'BTC-USD',
                'dataset': 'DBEQ.BASIC'
            }
        }
        
        logger.info("Bitcoin data client initialized")
    
    def _get_or_create_asset(self, symbol: str) -> int:
        """
        Get or create asset in crypto_assets table
        
        Args:
            symbol: Asset symbol (e.g., 'BTC1!')
            
        Returns:
            asset_id from database
        """
        if not self.db_conn:
            return None
        
        cursor = self.db_conn.cursor()
        
        asset_info = self.btc_assets.get(symbol, {
            'name': f'Bitcoin {symbol}',
            'exchange': 'UNKNOWN',
            'tick_size': 0.01,
            'multiplier': 1,
            'currency': 'USD'
        })
        
        # Insert or get asset
        cursor.execute("""
            INSERT INTO crypto_assets 
                (symbol, name, exchange, tick_size, multiplier, currency)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, exchange) 
            DO UPDATE SET 
                last_trade_date = EXCLUDED.last_trade_date
            RETURNING asset_id
        """, (
            symbol, 
            asset_info['name'], 
            asset_info['exchange'], 
            asset_info['tick_size'], 
            asset_info['multiplier'], 
            asset_info['currency']
        ))
        
        asset_id = cursor.fetchone()[0]
        self.db_conn.commit()
        cursor.close()
        
        return asset_id
    
    def fetch_btc_data_from_yahoo(
        self,
        symbol: str = 'BTC-USD',
        start_date: str = None,
        end_date: str = None,
        interval: str = '1h'
    ) -> Optional[pd.DataFrame]:
        """
        Fetch Bitcoin data from Yahoo Finance (free alternative)
        
        Args:
            symbol: Symbol (BTC-USD)
            start_date: Start date 'YYYY-MM-DD'
            end_date: End date 'YYYY-MM-DD'
            interval: '1h' or '1d'
            
        Returns:
            DataFrame with OHLCV data
        """
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        try:
            import yfinance as yf
            
            logger.info(f"Fetching {symbol} from Yahoo Finance ({start_date} to {end_date}, interval={interval})")
            
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_date, end=end_date, interval=interval)
            
            if not df.empty:
                # Rename columns to match our schema
                df = df.rename(columns={
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume'
                })
                df = df[['open', 'high', 'low', 'close', 'volume']]
                logger.info(f"Fetched {len(df)} records for {symbol} from Yahoo Finance")
                return df
            else:
                logger.warning(f"No data returned for {symbol}")
                return None
                
        except ImportError:
            logger.error("yfinance not installed. Install with: pip install yfinance")
            return None
        except Exception as e:
            logger.error(f"Error fetching from Yahoo Finance: {e}")
            return None
    
    def fetch_btc_data(
        self,
        symbol: str = 'BTC',
        start_date: str = None,
        end_date: str = None,
        schema: str = 'ohlcv-1h'
    ) -> Optional[pd.DataFrame]:
        """
        Fetch Bitcoin data (uses Yahoo Finance as fallback for now)
        
        Args:
            symbol: Symbol to fetch ('BTC', 'BTCUSD', etc.)
            start_date: Start date 'YYYY-MM-DD' (default: 30 days ago)
            end_date: End date 'YYYY-MM-DD' (default: today)
            schema: Data schema ('ohlcv-1h', 'ohlcv-1d')
            
        Returns:
            DataFrame with OHLCV data
        """
        # For now, use Yahoo Finance as Databento Bitcoin requires specific contracts
        interval_map = {
            'ohlcv-1h': '1h',
            'ohlcv-1d': '1d'
        }
        interval = interval_map.get(schema, '1h')
        
        return self.fetch_btc_data_from_yahoo('BTC-USD', start_date, end_date, interval)
    
    def _insert_ohlcv_data(self, asset_id: int, symbol: str, df: pd.DataFrame):
        """
        Insert OHLCV data into TimescaleDB
        
        Args:
            asset_id: Asset ID from crypto_assets table
            symbol: Asset symbol
            df: DataFrame with OHLCV data
        """
        if not self.db_conn or df.empty:
            return
        
        cursor = self.db_conn.cursor()
        
        # Prepare data for bulk insert
        records = []
        for idx, row in df.iterrows():
            # Convert timestamp
            time = pd.to_datetime(idx).to_pydatetime() if not isinstance(idx, datetime) else idx
            
            # Convert prices to integers (multiply by 100 to avoid float issues)
            # For Bitcoin futures, prices are already in dollars, so multiply by 100 for cents
            price_multiplier = 100
            
            records.append((
                time,
                asset_id,
                symbol,
                int(row.get('open', 0) * price_multiplier),
                int(row.get('high', 0) * price_multiplier),
                int(row.get('low', 0) * price_multiplier),
                int(row.get('close', 0) * price_multiplier),
                int(row.get('volume', 0)),
            ))
        
        # Bulk insert with ON CONFLICT DO NOTHING
        execute_values(
            cursor,
            """
            INSERT INTO crypto_ohlcv 
                (time, asset_id, symbol, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (time, asset_id) DO NOTHING
            """,
            records
        )
        
        self.db_conn.commit()
        cursor.close()
        
        logger.info(f"Inserted {len(records)} records for {symbol} into TimescaleDB")
    
    def fetch_multiple_timeframes(
        self,
        symbol: str = 'BTC1!',
        start_date: str = None,
        end_date: str = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch Bitcoin data for multiple timeframes (1h, 4h, 1d)
        
        Args:
            symbol: Symbol to fetch
            start_date: Start date 'YYYY-MM-DD'
            end_date: End date 'YYYY-MM-DD'
            
        Returns:
            Dictionary mapping timeframe to DataFrame
        """
        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        # Databento only supports 1h and 1d, we'll resample 1h to 4h ourselves
        timeframes = {
            '1h': 'ohlcv-1h',
            '1d': 'ohlcv-1d'
        }
        
        results = {}
        
        for tf, schema in timeframes.items():
            logger.info(f"Fetching {symbol} {tf} data from {start_date} to {end_date}")
            
            try:
                df = self.fetch_btc_data(symbol, start_date, end_date, schema)
                if df is not None and not df.empty:
                    results[tf] = df
                    logger.info(f"✓ Fetched {len(df)} {tf} records for {symbol}")
                else:
                    logger.warning(f"✗ No {tf} data for {symbol}")
                    
            except Exception as e:
                logger.error(f"Error fetching {tf} data for {symbol}: {e}")
                results[tf] = None
        
        # Resample 1h to 4h if we got hourly data
        if '1h' in results and results['1h'] is not None and not results['1h'].empty:
            try:
                df_1h = results['1h']
                # Resample to 4h
                df_4h = df_1h.resample('4H').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).dropna()
                results['4h'] = df_4h
                logger.info(f"✓ Resampled {len(df_4h)} 4h records from 1h data")
            except Exception as e:
                logger.error(f"Error resampling to 4h: {e}")
                results['4h'] = None
        
        return results

    def update_btc_data(self, symbol: str = 'BTC1!', days_back: int = 7) -> Dict:
        """
        Update Bitcoin data in database for all timeframes (1h, 4h, 1d)
        
        Args:
            symbol: Symbol to update
            days_back: How many days back to fetch
            
        Returns:
            Dictionary with update results
        """
        logger.info(f"Updating {symbol} data for last {days_back} days (1h, 4h, 1d)")
        
        # Get or create asset
        asset_id = self._get_or_create_asset(symbol)
        if not asset_id:
            return {'status': 'error', 'message': 'Could not create asset'}
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Fetch data for all timeframes
        timeframe_data = self.fetch_multiple_timeframes(
            symbol=symbol,
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d')
        )
        
        results = {
            'status': 'success',
            'symbol': symbol,
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'timeframes': {}
        }
        
        # Insert data for each timeframe
        for tf, df in timeframe_data.items():
            if df is not None and not df.empty:
                try:
                    self._insert_ohlcv_data(asset_id, symbol, df)
                    results['timeframes'][tf] = {
                        'status': 'success',
                        'records': len(df)
                    }
                    logger.info(f"✓ Inserted {len(df)} {tf} records for {symbol}")
                except Exception as e:
                    logger.error(f"Error inserting {tf} data: {e}")
                    results['timeframes'][tf] = {
                        'status': 'error',
                        'message': str(e)
                    }
            else:
                results['timeframes'][tf] = {
                    'status': 'no_data',
                    'records': 0
                }
        
        return results
    
    def get_latest_btc_data(self, symbol: str = 'BTC1!', timeframe: str = '1h', limit: int = 100) -> Optional[pd.DataFrame]:
        """
        Get latest Bitcoin data from database
        
        Args:
            symbol: Symbol to fetch
            timeframe: Timeframe ('1h', '4h', '1d')
            limit: Number of records to return
            
        Returns:
            DataFrame with latest OHLCV data
        """
        if not self.db_conn:
            return None
        
        # Map timeframe to interval in hours
        interval_map = {
            '1h': 1,
            '4h': 4,
            '1d': 24
        }
        
        interval_hours = interval_map.get(timeframe, 1)
        
        cursor = self.db_conn.cursor()
        
        # For daily data, filter by midnight UTC bars
        # For intraday, filter by time interval
        if timeframe == '1d':
            cursor.execute("""
                SELECT time, open, high, low, close, volume
                FROM crypto_ohlcv co
                JOIN crypto_assets ca ON co.asset_id = ca.asset_id
                WHERE ca.symbol = %s
                AND EXTRACT(HOUR FROM (co.time AT TIME ZONE 'UTC')) = 0
                AND EXTRACT(MINUTE FROM (co.time AT TIME ZONE 'UTC')) = 0
                ORDER BY co.time DESC
                LIMIT %s
            """, (symbol, limit))
        else:
            # For hourly/4h, use interval-based filtering
            cursor.execute("""
                WITH filtered_data AS (
                    SELECT 
                        co.time,
                        co.open,
                        co.high,
                        co.low,
                        co.close,
                        co.volume,
                        EXTRACT(EPOCH FROM (co.time - LAG(co.time) OVER (ORDER BY co.time))) / 3600.0 AS hours_diff
                    FROM crypto_ohlcv co
                    JOIN crypto_assets ca ON co.asset_id = ca.asset_id
                    WHERE ca.symbol = %s
                    ORDER BY co.time DESC
                )
                SELECT time, open, high, low, close, volume
                FROM filtered_data
                WHERE hours_diff IS NULL 
                   OR (hours_diff >= %s - 0.5 AND hours_diff <= %s + 0.5)
                ORDER BY time DESC
                LIMIT %s
            """, (symbol, interval_hours, interval_hours, limit))
        
        columns = ['time', 'open', 'high', 'low', 'close', 'volume']
        data = cursor.fetchall()
        cursor.close()
        
        if not data:
            return None
        
        df = pd.DataFrame(data, columns=columns)
        df['time'] = pd.to_datetime(df['time'], utc=True)
        df.set_index('time', inplace=True)
        
        # Convert prices back from integers (divide by 100)
        price_columns = ['open', 'high', 'low', 'close']
        for col in price_columns:
            df[col] = df[col] / 100.0
        
        return df.sort_index()
    
    def get_all_timeframes(self, symbol: str = 'BTC1!', limit: int = 100) -> Dict[str, pd.DataFrame]:
        """
        Get latest Bitcoin data for all timeframes
        
        Args:
            symbol: Symbol to fetch
            limit: Number of records to return per timeframe
            
        Returns:
            Dictionary mapping timeframe to DataFrame
        """
        timeframes = ['1h', '4h', '1d']
        results = {}
        
        for tf in timeframes:
            df = self.get_latest_btc_data(symbol, tf, limit)
            if df is not None:
                results[tf] = df
            else:
                results[tf] = None
                
        return results


# Convenience function for quick use
def update_bitcoin_data(symbol: str = 'BTC1!', days_back: int = 7):
    """
    Quick function to update Bitcoin data
    
    Args:
        symbol: Bitcoin symbol to update
        days_back: Days back to fetch
    """
    client = BitcoinDataClient()
    return client.update_btc_data(symbol, days_back)


if __name__ == '__main__':
    # Example usage
    client = BitcoinDataClient()
    
    # Update Bitcoin data
    result = client.update_btc_data('BTC1!', days_back=30)
    print(f"Update result: {result}")
    
    # Get latest data
    df = client.get_latest_btc_data('BTC1!', limit=10)
    if df is not None:
        print(f"\nLatest Bitcoin data:\n{df.head()}")
