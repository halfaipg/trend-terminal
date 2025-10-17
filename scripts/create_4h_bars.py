"""
Create 4-hour bars from hourly data and store in DB
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from loguru import logger
import os

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/aitrader')

def create_4h_bars():
    """Resample hourly data to 4h and insert"""
    
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Get BTC hourly asset_id
    cursor.execute("SELECT asset_id FROM crypto_assets WHERE symbol = 'BTC' LIMIT 1")
    result = cursor.fetchone()
    if not result:
        logger.error("BTC asset not found")
        return
    btc_asset_id = result[0]
    
    # Get BTC_4H asset_id
    cursor.execute("SELECT asset_id FROM crypto_assets WHERE symbol = 'BTC_4H' LIMIT 1")
    result = cursor.fetchone()
    if not result:
        logger.error("BTC_4H asset not found")
        return
    asset_id_4h = result[0]
    logger.info(f"✓ BTC hourly asset_id: {btc_asset_id}, 4H asset_id: {asset_id_4h}")
    
    # Fetch all hourly data
    logger.info("Fetching hourly data...")
    query = """
        SELECT time, open, high, low, close, volume
        FROM crypto_ohlcv
        WHERE asset_id = %s
        ORDER BY time
    """
    df = pd.read_sql(query, conn, params=(btc_asset_id,), parse_dates=['time'])
    logger.info(f"✓ Loaded {len(df):,} hourly bars")
    
    # Set time as index
    df.set_index('time', inplace=True)
    
    # Resample to 4h
    logger.info("Resampling to 4h...")
    df_4h = df.resample('4h').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    logger.info(f"✓ Created {len(df_4h):,} 4h bars")
    
    # Prepare records for insert
    records = []
    for timestamp, row in df_4h.iterrows():
        records.append((
            timestamp,
            asset_id_4h,
            'BTC_4H',
            int(row['open']),
            int(row['high']),
            int(row['low']),
            int(row['close']),
            int(row['volume'])
        ))
    
    logger.info(f"✓ Prepared {len(records):,} records")
    
    # Insert with upsert
    logger.info("Inserting 4h bars...")
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
    logger.info(f"✅ Successfully inserted {len(records):,} 4h bars")
    
    # Verify
    cursor.execute("""
        SELECT COUNT(*) FROM crypto_ohlcv WHERE symbol = 'BTC_4H'
    """)
    count = cursor.fetchone()[0]
    logger.info(f"✓ Total 4h bars in DB: {count:,}")
    
    cursor.close()
    conn.close()

if __name__ == '__main__':
    create_4h_bars()

