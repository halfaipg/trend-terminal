"""Import KAS data correctly from CoinGecko"""
import os
import requests
import psycopg2
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/aitrader')

def get_asset_id(cursor):
    """Get or create asset_id for KAS_DAILY"""
    cursor.execute("SELECT asset_id FROM crypto_assets WHERE symbol = 'KAS_DAILY'")
    result = cursor.fetchone()
    if result:
        return result[0]
    
    cursor.execute("""
        INSERT INTO crypto_assets (symbol, name, asset_type)
        VALUES ('KAS_DAILY', 'Kaspa', 'crypto')
        RETURNING asset_id
    """)
    return cursor.fetchone()[0]

def fetch_kas_history():
    """Fetch all available KAS history from CoinGecko"""
    print("🦎 Fetching KAS history from CoinGecko (max range)...")
    
    # Get from inception to now
    from_timestamp = int(datetime(2021, 1, 1).timestamp())
    to_timestamp = int(datetime.now().timestamp())
    
    url = "https://api.coingecko.com/api/v3/coins/kaspa/market_chart/range"
    params = {
        'vs_currency': 'usd',
        'from': from_timestamp,
        'to': to_timestamp
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Got {len(data.get('prices', []))} price points")
            return data
        else:
            print(f"❌ Status {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def process_data(data, cursor, asset_id):
    """Process and insert CoinGecko data with correct USD prices"""
    if not data or 'prices' not in data:
        return 0
    
    prices = data['prices']
    volumes = data.get('total_volumes', [])
    
    # Group by day and create OHLC
    daily_data = {}
    for price_point in prices:
        timestamp_ms, price = price_point
        date = datetime.fromtimestamp(timestamp_ms / 1000).date()
        
        if date not in daily_data:
            daily_data[date] = {
                'prices': [],
                'volume': 0
            }
        daily_data[date]['prices'].append(price)
    
    # Add volumes
    for vol_point in volumes:
        timestamp_ms, volume = vol_point
        date = datetime.fromtimestamp(timestamp_ms / 1000).date()
        if date in daily_data:
            daily_data[date]['volume'] = max(daily_data[date]['volume'], volume)
    
    bars_inserted = 0
    for date, day_data in sorted(daily_data.items()):
        prices_list = day_data['prices']
        if not prices_list:
            continue
        
        # OHLC from the price points
        open_price = prices_list[0]
        high_price = max(prices_list)
        low_price = min(prices_list)
        close_price = prices_list[-1]
        volume = int(day_data['volume'])
        
        # Convert USD to cents for storage (multiply by 100)
        open_cents = int(open_price * 100)
        high_cents = int(high_price * 100)
        low_cents = int(low_price * 100)
        close_cents = int(close_price * 100)
        
        bar_time = datetime.combine(date, datetime.min.time())
        
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
        """, (bar_time, asset_id, 'KAS_DAILY', open_cents, high_cents, low_cents, close_cents, volume))
        
        if cursor.rowcount > 0:
            bars_inserted += 1
    
    return bars_inserted

def main():
    """Import KAS data correctly"""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    try:
        asset_id = get_asset_id(cursor)
        print(f"✅ Using asset_id: {asset_id}")
        
        # Fetch data
        data = fetch_kas_history()
        if not data:
            print("❌ Failed to fetch data")
            return
        
        # Process and insert
        inserted = process_data(data, cursor, asset_id)
        conn.commit()
        
        print(f"\n🎉 Inserted {inserted} daily bars")
        
        # Show sample of recent data
        cursor.execute("""
            SELECT time::date, close::float / 100 as close_usd 
            FROM crypto_ohlcv 
            WHERE symbol = 'KAS_DAILY' 
            ORDER BY time DESC 
            LIMIT 5
        """)
        print("\nRecent prices:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: ${row[1]:.4f}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
