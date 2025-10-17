"""
Import Kaspa (KAS) daily data from Polygon.io
"""
import os
from datetime import datetime
from polygon import RESTClient
import psycopg2
from psycopg2.extras import execute_values
from loguru import logger

POLYGON_API_KEY = os.getenv('POLYGON_API_KEY', '9noF6D2y_ZyvKeV3ZJXaa5Ol63ECVZgz')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/aitrader')

def import_kaspa():
    """Import Kaspa daily data"""
    
    client = RESTClient(POLYGON_API_KEY)
    
    # Connect to DB
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Create KAS_DAILY asset
    cursor.execute("""
        INSERT INTO crypto_assets (symbol, name, exchange)
        VALUES ('KAS_DAILY', 'Kaspa Daily', 'AGGREGATE')
        ON CONFLICT (symbol, exchange) DO NOTHING
        RETURNING asset_id
    """)
    result = cursor.fetchone()
    if result:
        asset_id = result[0]
    else:
        cursor.execute("SELECT asset_id FROM crypto_assets WHERE symbol = 'KAS_DAILY' LIMIT 1")
        asset_id = cursor.fetchone()[0]
    
    logger.info(f"✓ KAS_DAILY asset_id: {asset_id}")
    
    # Fetch all available data
    logger.info("Fetching Kaspa data from Polygon...")
    
    try:
        aggs = client.list_aggs(
            ticker="X:KASUSD",
            multiplier=1,
            timespan="day",
            from_="2020-01-01",
            to="2025-10-15",
            limit=50000
        )
        
        records = []
        for agg in aggs:
            timestamp = datetime.fromtimestamp(agg.timestamp / 1000)
            records.append((
                timestamp,
                asset_id,
                'KAS_DAILY',
                int(agg.open * 100000),    # KAS prices are small, use more precision
                int(agg.high * 100000),
                int(agg.low * 100000),
                int(agg.close * 100000),
                int(agg.volume)
            ))
        
        logger.info(f"✓ Fetched {len(records)} daily bars")
        
        if records:
            # Insert
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
                records,
                page_size=1000
            )
            conn.commit()
            logger.info(f"✅ Inserted {len(records)} Kaspa daily bars")
            logger.info(f"Date range: {records[0][0].date()} to {records[-1][0].date()}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    import_kaspa()


