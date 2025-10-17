#!/usr/bin/env python3
"""
Populate weekly Bitcoin data from Polygon API
"""
import os
import psycopg2
import requests
from datetime import datetime, timezone
from loguru import logger

# Configuration
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/aitrader')
POLYGON_API_KEY = os.getenv('POLYGON_API_KEY')
POLYGON_BASE_URL = "https://api.polygon.io/v2/aggs/ticker/X:BTCUSD/range/1/week"

def fetch_weekly_data():
    """Fetch weekly BTC data from Polygon"""
    logger.info("Fetching weekly data from Polygon...")
    
    params = {
        "from": "2014-09-01",  # Start from earliest BTC data
        "to": datetime.now(timezone.utc).strftime('%Y-%m-%d'),
        "adjusted": "true",
        "sort": "asc",
        "limit": 50000,
        "apiKey": POLYGON_API_KEY
    }
    
    response = requests.get(POLYGON_BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()
    
    if data.get('status') != 'OK' or 'results' not in data:
        logger.error(f"Failed to fetch data: {data}")
        return []
    
    results = data['results']
    logger.info(f"Fetched {len(results)} weekly bars from Polygon")
    return results

def populate_database(bars):
    """Insert weekly bars into database"""
    if not bars:
        logger.warning("No bars to insert")
        return
    
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Get BTC_WEEKLY asset_id
    cursor.execute("SELECT asset_id FROM crypto_assets WHERE symbol = 'BTC_WEEKLY'")
    result = cursor.fetchone()
    if not result:
        logger.error("BTC_WEEKLY asset not found in database")
        conn.close()
        return
    
    asset_id = result[0]
    logger.info(f"Using asset_id {asset_id} for BTC_WEEKLY")
    
    # Insert bars
    inserted = 0
    updated = 0
    
    for bar in bars:
        # Convert timestamp from milliseconds to datetime
        timestamp = datetime.fromtimestamp(bar['t'] / 1000, tz=timezone.utc)
        
        # Prices are in cents in our DB
        open_price = int(bar['o'] * 100)
        high_price = int(bar['h'] * 100)
        low_price = int(bar['l'] * 100)
        close_price = int(bar['c'] * 100)
        volume = int(bar['v'])
        
        cursor.execute("""
            INSERT INTO crypto_ohlcv 
                (time, asset_id, symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time, asset_id) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
            RETURNING (xmax = 0) AS inserted
        """, (
            timestamp, asset_id, 'BTC_WEEKLY',
            open_price, high_price, low_price, close_price, volume
        ))
        
        result = cursor.fetchone()
        if result and result[0]:
            inserted += 1
        else:
            updated += 1
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info(f"✅ Inserted {inserted} new weekly bars, updated {updated} existing bars")

if __name__ == '__main__':
    logger.info("Starting weekly data population...")
    bars = fetch_weekly_data()
    populate_database(bars)
    logger.info("✅ Weekly data population complete!")

