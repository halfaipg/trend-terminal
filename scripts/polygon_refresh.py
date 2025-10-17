"""
Refresh recent Bitcoin data from Polygon.io (last 2 years)
Keep older historical data from Yahoo Finance
"""
import os
import sys
sys.path.append('backend')

import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from polygon import RESTClient
from loguru import logger

# Polygon API key from env
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY', '9noF6D2y_ZyvKeV3ZJXaa5Ol63ECVZgz')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/aitrader')

def fetch_polygon_data(symbol='X:BTCUSD', start_date='2023-10-15', end_date=None, timespan='hour', multiplier=1):
    """
    Fetch data from Polygon.io
    
    Args:
        symbol: Polygon crypto ticker (X:BTCUSD)
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD (default: yesterday)
        timespan: 'minute', 'hour', 'day'
        multiplier: Multiplier for timespan
    
    Returns:
        DataFrame with OHLCV data
    """
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    logger.info(f"Fetching {symbol} from Polygon: {start_date} to {end_date} ({multiplier}{timespan})")
    
    client = RESTClient(POLYGON_API_KEY)
    
    data = []
    try:
        for bar in client.list_aggs(
            ticker=symbol,
            multiplier=multiplier,
            timespan=timespan,
            from_=start_date,
            to=end_date,
            limit=50000
        ):
            data.append({
                'timestamp': pd.Timestamp(bar.timestamp, unit='ms', tz='UTC'),
                'open': bar.open,
                'high': bar.high,
                'low': bar.low,
                'close': bar.close,
                'volume': bar.volume
            })
    except Exception as e:
        logger.error(f"Error fetching from Polygon: {e}")
        return None
    
    if not data:
        logger.warning("No data returned from Polygon")
        return None
    
    df = pd.DataFrame(data)
    df.set_index('timestamp', inplace=True)
    df = df[['open', 'high', 'low', 'close', 'volume']]
    
    logger.info(f"✓ Fetched {len(df)} bars from Polygon")
    return df


def resample_to_4h(df_1h):
    """Resample 1-hour data to 4-hour bars"""
    df_4h = df_1h.resample('4H').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    return df_4h


def insert_into_db(symbol, df, asset_id, conn):
    """Insert data into crypto_ohlcv table"""
    if df is None or df.empty:
        return 0
    
    cursor = conn.cursor()
    records = []
    
    for idx, row in df.iterrows():
        time = pd.to_datetime(idx).to_pydatetime()
        price_multiplier = 100
        
        records.append((
            time,
            asset_id,
            symbol,
            int(row['open'] * price_multiplier),
            int(row['high'] * price_multiplier),
            int(row['low'] * price_multiplier),
            int(row['close'] * price_multiplier),
            int(row['volume']),
        ))
    
    from psycopg2.extras import execute_values
    execute_values(
        cursor,
        """
        INSERT INTO crypto_ohlcv 
            (time, asset_id, symbol, open, high, low, close, volume)
        VALUES %s
        ON CONFLICT (time, asset_id) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
        """,
        records
    )
    
    conn.commit()
    cursor.close()
    
    logger.info(f"✓ Upserted {len(records)} records")
    return len(records)


def refresh_polygon_data():
    """Main function to refresh data from Polygon"""
    
    logger.info("=" * 60)
    logger.info("Starting Polygon data refresh for BTC")
    logger.info("=" * 60)
    
    # Connect to database
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Get asset_id for BTC
    cursor.execute("SELECT asset_id FROM crypto_assets WHERE symbol = 'BTC'")
    result = cursor.fetchone()
    
    if not result:
        logger.error("BTC asset not found in database!")
        cursor.close()
        conn.close()
        return
    
    asset_id = result[0]
    logger.info(f"BTC asset_id: {asset_id}")
    
    # Check current data range in database
    cursor.execute("""
        SELECT MIN(time), MAX(time), COUNT(*) 
        FROM crypto_ohlcv 
        WHERE symbol = 'BTC'
    """)
    min_time, max_time, count = cursor.fetchone()
    logger.info(f"Current DB data: {min_time} to {max_time} ({count} bars)")
    
    cursor.close()
    
    # Polygon data starts from 2023-10-15
    # We'll refresh from that date to yesterday
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Fetch daily data (all history available from Polygon)
    logger.info("\n📅 Fetching DAILY data from Polygon...")
    df_daily = fetch_polygon_data(
        symbol='X:BTCUSD',
        start_date='2023-10-15',
        end_date=yesterday,
        timespan='day',
        multiplier=1
    )
    
    if df_daily is not None:
        records = insert_into_db('BTC', df_daily, asset_id, conn)
        logger.info(f"Daily: {records} bars")
    
    # Fetch hourly data (last 2 years)
    logger.info("\n⏰ Fetching HOURLY data from Polygon...")
    df_hourly = fetch_polygon_data(
        symbol='X:BTCUSD',
        start_date='2023-10-15',
        end_date=yesterday,
        timespan='hour',
        multiplier=1
    )
    
    if df_hourly is not None:
        records = insert_into_db('BTC', df_hourly, asset_id, conn)
        logger.info(f"Hourly: {records} bars")
        
        # Create 4-hour data from hourly
        logger.info("\n🔄 Resampling to 4-HOUR data...")
        df_4h = resample_to_4h(df_hourly)
        records = insert_into_db('BTC', df_4h, asset_id, conn)
        logger.info(f"4-Hour: {records} bars")
    
    # Final stats
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MIN(time), MAX(time), COUNT(*) 
        FROM crypto_ohlcv 
        WHERE symbol = 'BTC'
    """)
    min_time, max_time, count = cursor.fetchone()
    
    logger.info("\n" + "=" * 60)
    logger.info("✅ Polygon refresh complete!")
    logger.info(f"Total data range: {min_time} to {max_time}")
    logger.info(f"Total bars: {count}")
    logger.info("=" * 60)
    
    cursor.close()
    conn.close()


if __name__ == '__main__':
    refresh_polygon_data()


