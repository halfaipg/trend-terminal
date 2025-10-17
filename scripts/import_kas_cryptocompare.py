"""Import correct KAS data from CryptoCompare"""
import os
import requests
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/aitrader')

def get_asset_id(cursor):
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

def fetch_all_kas_data():
    """Fetch all KAS daily data from CryptoCompare"""
    print("💎 Fetching KAS history from CryptoCompare...")
    
    url = "https://min-api.cryptocompare.com/data/v2/histoday"
    params = {
        'fsym': 'KAS',
        'tsym': 'USD',
        'limit': 2000,  # Max allowed
        'toTs': int(datetime.now().timestamp())
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get('Response') == 'Success':
                bars = data.get('Data', {}).get('Data', [])
                print(f"✅ Got {len(bars)} daily bars")
                return bars
            else:
                print(f"❌ Error: {data.get('Message')}")
                return None
        else:
            print(f"❌ Status {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def main():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    try:
        asset_id = get_asset_id(cursor)
        print(f"✅ Using asset_id: {asset_id}")
        
        bars = fetch_all_kas_data()
        if not bars:
            print("❌ Failed to fetch data")
            return
        
        inserted = 0
        for bar in bars:
            timestamp = bar['time']
            bar_time = datetime.fromtimestamp(timestamp).replace(hour=0, minute=0, second=0, microsecond=0)
            
            # These are already in USD, convert to cents
            open_cents = int(bar['open'] * 100)
            high_cents = int(bar['high'] * 100)
            low_cents = int(bar['low'] * 100)
            close_cents = int(bar['close'] * 100)
            volume = int(bar.get('volumeto', 0))
            
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
            
            inserted += 1
        
        conn.commit()
        print(f"\n🎉 Inserted/updated {inserted} bars")
        
        # Show recent data
        cursor.execute("""
            SELECT time::date, 
                   close::float / 100 as close_usd,
                   high::float / 100 as high_usd
            FROM crypto_ohlcv 
            WHERE symbol = 'KAS_DAILY' 
            ORDER BY time DESC 
            LIMIT 5
        """)
        print("\nRecent KAS prices:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: Close=${row[1]:.4f}, High=${row[2]:.4f}")
        
        # Show total count
        cursor.execute("SELECT COUNT(*) FROM crypto_ohlcv WHERE symbol = 'KAS_DAILY'")
        total = cursor.fetchone()[0]
        print(f"\nTotal KAS bars: {total}")
        
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
