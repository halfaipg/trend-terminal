"""
Fetch recent daily bars from Polygon to fill gaps
"""
import os
from datetime import datetime, timedelta
from polygon import RESTClient
import psycopg2
from psycopg2.extras import execute_values
from loguru import logger

POLYGON_API_KEY = os.getenv('POLYGON_API_KEY', '9noF6D2y_ZyvKeV3ZJXaa5Ol63ECVZgz')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/aitrader')

def update_daily_from_polygon():
    """Fetch missing daily bars from Polygon"""
    
    client = RESTClient(POLYGON_API_KEY)
    
    # Connect to DB
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Get BTC_DAILY asset_id
    cursor.execute("SELECT asset_id FROM crypto_assets WHERE symbol = 'BTC_DAILY' LIMIT 1")
    result = cursor.fetchone()
    if not result:
        logger.error("BTC_DAILY asset not found")
        return
    asset_id = result[0]
    
    # Get last date in DB
    cursor.execute("""
        SELECT MAX(time) FROM crypto_ohlcv WHERE symbol = 'BTC_DAILY'
    """)
    last_date = cursor.fetchone()[0]
    logger.info(f"Last date in DB: {last_date}")
    
    # Fetch from day after last date to today
    start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    logger.info(f"Fetching daily bars from {start_date} to {end_date}...")
    
    try:
        aggs = client.list_aggs(
            ticker="X:BTCUSD",
            multiplier=1,
            timespan="day",
            from_=start_date,
            to=end_date,
            limit=50000
        )
        
        records = []
        for agg in aggs:
            timestamp = datetime.fromtimestamp(agg.timestamp / 1000)
            records.append((
                timestamp,
                asset_id,
                'BTC_DAILY',
                int(agg.open * 100),
                int(agg.high * 100),
                int(agg.low * 100),
                int(agg.close * 100),
                int(agg.volume)
            ))
        
        logger.info(f"✓ Fetched {len(records)} daily bars")
        
        if records:
            # Insert with upsert
            execute_values(
                cursor,
                """
                INSERT INTO crypto_ohlcv (time, asset_id, symbol, open, high, low, close, volume)
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
            logger.info(f"✅ Inserted {len(records)} daily bars")
            
            # Show what we added
            for rec in records:
                logger.info(f"  {rec[0].date()}: ${rec[6]/100:,.2f}")
        else:
            logger.info("No new bars to add")
        
    except Exception as e:
        logger.error(f"Error fetching from Polygon: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    update_daily_from_polygon()


