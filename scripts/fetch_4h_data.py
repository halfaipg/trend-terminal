"""
Fetch 4-hour Bitcoin data directly from Polygon.io
"""
import os
from datetime import datetime, timedelta
from polygon import RESTClient
import psycopg2
from loguru import logger

# Get credentials from env
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY', '9noF6D2y_ZyvKeV3ZJXaa5Ol63ECVZgz')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/aitrader')

def fetch_4h_btc():
    """Fetch 4-hour Bitcoin data from Polygon"""
    client = RESTClient(POLYGON_API_KEY)
    
    # Get BTC asset_id
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM crypto_assets WHERE symbol = 'BTC' LIMIT 1")
    result = cursor.fetchone()
    if not result:
        logger.error("BTC asset not found in database")
        return
    asset_id = result[0]
    
    # Fetch 4-hour data from Polygon (last 2 years for indicators)
    start_date = datetime.now() - timedelta(days=730)
    end_date = datetime.now()
    
    logger.info(f"Fetching 4H data from {start_date.date()} to {end_date.date()}")
    
    try:
        # Polygon aggregates endpoint with 4-hour multiplier
        aggs = client.list_aggs(
            ticker="X:BTCUSD",
            multiplier=4,
            timespan="hour",
            from_=start_date.strftime('%Y-%m-%d'),
            to=end_date.strftime('%Y-%m-%d'),
            limit=50000
        )
        
        records = []
        for agg in aggs:
            # Polygon returns timestamp in milliseconds
            timestamp = datetime.fromtimestamp(agg.timestamp / 1000)
            records.append((
                timestamp,
                asset_id,
                'BTC',
                int(agg.open * 100),    # Convert to cents
                int(agg.high * 100),
                int(agg.low * 100),
                int(agg.close * 100),
                int(agg.volume),
                int(agg.volume * agg.vwap) if hasattr(agg, 'vwap') and agg.vwap else 0
            ))
        
        logger.info(f"Fetched {len(records)} 4H bars")
        
        if records:
            # Bulk insert with upsert
            from psycopg2.extras import execute_values
            execute_values(
                cursor,
                """
                INSERT INTO crypto_ohlcv (time, asset_id, symbol, open, high, low, close, volume, volume_notional)
                VALUES %s
                ON CONFLICT (time, asset_id) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    volume_notional = EXCLUDED.volume_notional
                """,
                records
            )
            conn.commit()
            logger.info(f"✓ Inserted {len(records)} 4H bars")
        
    except Exception as e:
        logger.error(f"Error fetching 4H data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    fetch_4h_btc()
