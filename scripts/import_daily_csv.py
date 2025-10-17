"""
Import daily Bitcoin data from CSV
Keep recent bars but overwrite old ones with this higher-quality data
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from loguru import logger
import os

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/aitrader')
CSV_PATH = '/Users/j/Downloads/Bitcoin_history_data.csv'

def import_daily_data():
    """Import daily BTC data"""
    
    # Load CSV
    logger.info(f"Loading {CSV_PATH}...")
    df = pd.read_csv(CSV_PATH)
    logger.info(f"✓ Loaded {len(df):,} daily bars")
    
    # Parse date
    df['Date'] = pd.to_datetime(df['Date'], utc=True)
    
    logger.info(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
    
    # Connect to DB
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Get or create BTC_DAILY asset
    cursor.execute("""
        INSERT INTO crypto_assets (symbol, name, exchange)
        VALUES ('BTC_DAILY', 'Bitcoin Daily', 'AGGREGATE')
        ON CONFLICT (symbol, exchange) DO NOTHING
        RETURNING asset_id
    """)
    result = cursor.fetchone()
    if result:
        asset_id = result[0]
    else:
        cursor.execute("SELECT asset_id FROM crypto_assets WHERE symbol = 'BTC_DAILY' LIMIT 1")
        asset_id = cursor.fetchone()[0]
    
    logger.info(f"✓ BTC_DAILY asset_id: {asset_id}")
    
    # Prepare records
    records = []
    for _, row in df.iterrows():
        records.append((
            row['Date'],
            asset_id,
            'BTC_DAILY',
            int(row['Open'] * 100),      # Convert to cents
            int(row['High'] * 100),
            int(row['Low'] * 100),
            int(row['Close'] * 100),
            int(row['Volume'])
        ))
    
    logger.info(f"✓ Prepared {len(records):,} records")
    
    # Insert with upsert (will overwrite old data, keep recent)
    logger.info("Inserting daily bars...")
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
    logger.info(f"✅ Successfully inserted {len(records):,} daily bars")
    
    # Verify
    cursor.execute("""
        SELECT COUNT(*), MIN(time), MAX(time) 
        FROM crypto_ohlcv 
        WHERE symbol = 'BTC_DAILY'
    """)
    count, min_time, max_time = cursor.fetchone()
    logger.info(f"✓ Total daily bars in DB: {count:,}")
    logger.info(f"✓ Date range: {min_time} to {max_time}")
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    import_daily_data()


