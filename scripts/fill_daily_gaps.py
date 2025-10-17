"""
Find and fill missing daily bars from Polygon
"""
import os
from datetime import datetime, timedelta
from polygon import RESTClient
import psycopg2
from psycopg2.extras import execute_values
from loguru import logger
import pandas as pd

POLYGON_API_KEY = os.getenv('POLYGON_API_KEY', '9noF6D2y_ZyvKeV3ZJXaa5Ol63ECVZgz')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/aitrader')

def fill_daily_gaps():
    """Find gaps in daily data and fill from Polygon"""
    
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Get BTC_DAILY asset_id
    cursor.execute("SELECT asset_id FROM crypto_assets WHERE symbol = 'BTC_DAILY' LIMIT 1")
    asset_id = cursor.fetchone()[0]
    
    # Get all existing dates
    cursor.execute("""
        SELECT time::date as date 
        FROM crypto_ohlcv 
        WHERE symbol = 'BTC_DAILY' 
        ORDER BY time
    """)
    existing_dates = [row[0] for row in cursor.fetchall()]
    
    logger.info(f"Found {len(existing_dates)} existing daily bars")
    logger.info(f"Date range: {existing_dates[0]} to {existing_dates[-1]}")
    
    # Find gaps (missing dates)
    start_date = existing_dates[0]
    end_date = existing_dates[-1]
    
    all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
    existing_dates_set = set(existing_dates)
    missing_dates = [d.date() for d in all_dates if d.date() not in existing_dates_set]
    
    logger.info(f"Found {len(missing_dates)} missing dates")
    
    if not missing_dates:
        logger.info("✅ No gaps to fill!")
        cursor.close()
        conn.close()
        return
    
    # Show first 10 missing dates
    logger.info(f"First missing dates: {missing_dates[:10]}")
    
    # Fetch missing data from Polygon
    client = RESTClient(POLYGON_API_KEY)
    
    logger.info("Fetching missing bars from Polygon...")
    
    try:
        # Fetch all data from start to end
        aggs = client.list_aggs(
            ticker="X:BTCUSD",
            multiplier=1,
            timespan="day",
            from_=str(start_date),
            to=str(end_date),
            limit=50000
        )
        
        # Filter to only missing dates
        records = []
        for agg in aggs:
            agg_date = datetime.fromtimestamp(agg.timestamp / 1000).date()
            if agg_date in missing_dates:
                timestamp = datetime.combine(agg_date, datetime.min.time())
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
        
        logger.info(f"✓ Fetched {len(records)} bars to fill gaps")
        
        if records:
            # Insert
            execute_values(
                cursor,
                """
                INSERT INTO crypto_ohlcv (time, asset_id, symbol, open, high, low, close, volume)
                VALUES %s
                ON CONFLICT (time, asset_id) DO NOTHING
                """,
                records
            )
            conn.commit()
            logger.info(f"✅ Filled {len(records)} missing daily bars")
        
    except Exception as e:
        logger.error(f"Error fetching from Polygon: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    fill_daily_gaps()


