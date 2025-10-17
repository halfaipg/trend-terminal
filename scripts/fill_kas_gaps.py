"""Fill gaps in KAS daily data from Polygon"""
import os
import sys
from datetime import datetime, timedelta
import psycopg2
from polygon import RESTClient
from dotenv import load_dotenv

load_dotenv()

POLYGON_API_KEY = os.getenv('POLYGON_API_KEY', '9noF6D2y_ZyvKeV3ZJXaa5Ol63ECVZgz')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/aitrader')

def fill_kas_gaps():
    """Fill missing KAS data from Polygon"""
    
    # Connect to database
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    # Get asset_id for KAS_DAILY
    cursor.execute("SELECT asset_id FROM assets WHERE symbol = 'KAS_DAILY'")
    result = cursor.fetchone()
    if not result:
        print("❌ KAS_DAILY asset not found in database")
        return
    
    asset_id = result[0]
    print(f"✅ Found KAS_DAILY asset_id: {asset_id}")
    
    # Gap: Aug 4, 2024 to Jan 22, 2025
    gap_start = datetime(2024, 8, 4)
    gap_end = datetime(2025, 1, 22)
    
    print(f"\n📊 Fetching KAS data from {gap_start.date()} to {gap_end.date()}")
    
    try:
        client = RESTClient(POLYGON_API_KEY)
        
        # Fetch aggregates
        aggs = client.get_aggs(
            ticker="X:KASUSD",
            multiplier=1,
            timespan="day",
            from_=gap_start.strftime('%Y-%m-%d'),
            to=gap_end.strftime('%Y-%m-%d'),
            limit=50000
        )
        
        if not aggs:
            print("❌ No data returned from Polygon")
            return
        
        bars_inserted = 0
        bars_updated = 0
        
        for agg in aggs:
            # Convert from milliseconds to datetime
            bar_time = datetime.fromtimestamp(agg.timestamp / 1000).replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Convert prices to cents (integer)
            open_cents = int(agg.open * 100)
            high_cents = int(agg.high * 100)
            low_cents = int(agg.low * 100)
            close_cents = int(agg.close * 100)
            volume = int(agg.volume)
            
            # Insert/update bar
            cursor.execute("""
                INSERT INTO crypto_ohlcv 
                    (time, asset_id, symbol, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (time, asset_id) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = GREATEST(crypto_ohlcv.high, EXCLUDED.high),
                    low = LEAST(crypto_ohlcv.low, EXCLUDED.low),
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
                RETURNING (xmax = 0) AS inserted
            """, (bar_time, asset_id, 'KAS_DAILY', open_cents, high_cents, low_cents, close_cents, volume))
            
            was_inserted = cursor.fetchone()[0]
            if was_inserted:
                bars_inserted += 1
            else:
                bars_updated += 1
        
        conn.commit()
        print(f"✅ Inserted {bars_inserted} new bars, updated {bars_updated} bars")
        
    except Exception as e:
        print(f"❌ Error fetching from Polygon: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    fill_kas_gaps()
