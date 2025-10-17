"""
Import hourly BTC data from CSV to TimescaleDB
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from loguru import logger
import os

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/aitrader')
CSV_PATH = '/tmp/btc_hourly.csv'

def import_hourly_data():
    """Import hourly BTC data to database"""
    
    # Load CSV
    logger.info(f"Loading {CSV_PATH}...")
    df = pd.read_csv(CSV_PATH)
    logger.info(f"✓ Loaded {len(df):,} hourly bars")
    
    # Convert timestamp
    df['timestamp'] = pd.to_datetime(df['TIME_UNIX'], unit='s', utc=True)
    
    # Connect to DB
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Get BTC asset_id
    cursor.execute("SELECT asset_id FROM crypto_assets WHERE symbol = 'BTC' LIMIT 1")
    result = cursor.fetchone()
    if not result:
        logger.error("BTC asset not found in database")
        return
    asset_id = result[0]
    logger.info(f"✓ BTC asset_id: {asset_id}")
    
    # Prepare records for bulk insert
    records = []
    for _, row in df.iterrows():
        records.append((
            row['timestamp'],
            asset_id,
            'BTC',
            int(row['OPEN_PRICE'] * 100),      # Convert to cents
            int(row['HIGH_PRICE'] * 100),
            int(row['LOW_PRICE'] * 100),
            int(row['CLOSE_PRICE'] * 100),
            int(row['VOLUME_FROM'])             # Volume in BTC
        ))
    
    logger.info(f"✓ Prepared {len(records):,} records for insert")
    
    # Bulk insert with ON CONFLICT DO UPDATE
    logger.info("Inserting into crypto_ohlcv...")
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
    logger.info(f"✅ Successfully inserted {len(records):,} hourly bars")
    
    # Verify
    cursor.execute("""
        SELECT COUNT(*), MIN(time), MAX(time) 
        FROM crypto_ohlcv 
        WHERE asset_id = %s
    """, (asset_id,))
    count, min_time, max_time = cursor.fetchone()
    logger.info(f"✓ Total bars in DB: {count:,}")
    logger.info(f"✓ Date range: {min_time} to {max_time}")
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    import_hourly_data()

