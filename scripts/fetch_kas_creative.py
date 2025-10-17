"""Fetch KAS historical data from multiple sources"""
import os
import requests
import psycopg2
from datetime import datetime, timedelta
import time
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/aitrader')

def get_asset_id(cursor, symbol):
    """Get or create asset_id for symbol"""
    cursor.execute("""
        SELECT asset_id FROM crypto_assets WHERE symbol = %s
    """, (symbol,))
    result = cursor.fetchone()
    if result:
        return result[0]
    
    # Create new asset
    cursor.execute("""
        INSERT INTO crypto_assets (symbol, name, asset_type)
        VALUES (%s, %s, %s)
        RETURNING asset_id
    """, (symbol, 'Kaspa', 'crypto'))
    return cursor.fetchone()[0]

def fetch_from_coingecko():
    """Fetch KAS data from CoinGecko API"""
    print("\n🦎 Trying CoinGecko...")
    
    # Gap: Aug 4, 2024 to Jan 22, 2025
    gap_start = datetime(2024, 8, 4)
    gap_end = datetime(2025, 1, 22)
    
    from_timestamp = int(gap_start.timestamp())
    to_timestamp = int(gap_end.timestamp())
    
    url = f"https://api.coingecko.com/api/v3/coins/kaspa/market_chart/range"
    params = {
        'vs_currency': 'usd',
        'from': from_timestamp,
        'to': to_timestamp
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Got {len(data.get('prices', []))} price points from CoinGecko")
            return data
        else:
            print(f"❌ CoinGecko returned status {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ CoinGecko error: {e}")
        return None

def fetch_from_coinpaprika():
    """Fetch KAS data from CoinPaprika API"""
    print("\n📊 Trying CoinPaprika...")
    
    gap_start = datetime(2024, 8, 4)
    gap_end = datetime(2025, 1, 22)
    
    url = "https://api.coinpaprika.com/v1/tickers/kas-kaspa/historical"
    params = {
        'start': gap_start.strftime('%Y-%m-%d'),
        'end': gap_end.strftime('%Y-%m-%d'),
        'interval': '1d'
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Got {len(data)} daily bars from CoinPaprika")
            return data
        else:
            print(f"❌ CoinPaprika returned status {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ CoinPaprika error: {e}")
        return None

def fetch_from_cryptocompare():
    """Fetch KAS data from CryptoCompare API"""
    print("\n💎 Trying CryptoCompare...")
    
    gap_start = datetime(2024, 8, 4)
    
    url = "https://min-api.cryptocompare.com/data/v2/histoday"
    params = {
        'fsym': 'KAS',
        'tsym': 'USD',
        'limit': 200,  # 200 days
        'toTs': int(datetime(2025, 1, 22).timestamp())
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get('Response') == 'Success':
                bars = data.get('Data', {}).get('Data', [])
                print(f"✅ Got {len(bars)} daily bars from CryptoCompare")
                return bars
            else:
                print(f"❌ CryptoCompare error: {data.get('Message')}")
                return None
        else:
            print(f"❌ CryptoCompare returned status {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ CryptoCompare error: {e}")
        return None

def process_coingecko_data(data, cursor, asset_id):
    """Process and insert CoinGecko data"""
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
        prices = day_data['prices']
        if not prices:
            continue
        
        open_price = prices[0]
        high_price = max(prices)
        low_price = min(prices)
        close_price = prices[-1]
        volume = int(day_data['volume'])
        
        # Convert to cents
        open_cents = int(open_price * 100)
        high_cents = int(high_price * 100)
        low_cents = int(low_price * 100)
        close_cents = int(close_price * 100)
        
        bar_time = datetime.combine(date, datetime.min.time())
        
        cursor.execute("""
            INSERT INTO crypto_ohlcv 
                (time, asset_id, symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time, asset_id) DO NOTHING
        """, (bar_time, asset_id, 'KAS_DAILY', open_cents, high_cents, low_cents, close_cents, volume))
        
        if cursor.rowcount > 0:
            bars_inserted += 1
    
    return bars_inserted

def process_coinpaprika_data(data, cursor, asset_id):
    """Process and insert CoinPaprika data"""
    if not data:
        return 0
    
    bars_inserted = 0
    for bar in data:
        timestamp = bar['timestamp']
        bar_time = datetime.fromisoformat(timestamp.replace('Z', '+00:00')).replace(hour=0, minute=0, second=0, microsecond=0)
        
        open_cents = int(bar['open'] * 100)
        high_cents = int(bar['high'] * 100)
        low_cents = int(bar['low'] * 100)
        close_cents = int(bar['close'] * 100)
        volume = int(bar.get('volume', 0))
        
        cursor.execute("""
            INSERT INTO crypto_ohlcv 
                (time, asset_id, symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time, asset_id) DO NOTHING
        """, (bar_time, asset_id, 'KAS_DAILY', open_cents, high_cents, low_cents, close_cents, volume))
        
        if cursor.rowcount > 0:
            bars_inserted += 1
    
    return bars_inserted

def process_cryptocompare_data(data, cursor, asset_id):
    """Process and insert CryptoCompare data"""
    if not data:
        return 0
    
    bars_inserted = 0
    for bar in data:
        timestamp = bar['time']
        bar_time = datetime.fromtimestamp(timestamp).replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Skip if not in our gap range
        if bar_time < datetime(2024, 8, 4) or bar_time > datetime(2025, 1, 22):
            continue
        
        open_cents = int(bar['open'] * 100)
        high_cents = int(bar['high'] * 100)
        low_cents = int(bar['low'] * 100)
        close_cents = int(bar['close'] * 100)
        volume = int(bar.get('volumeto', 0))
        
        cursor.execute("""
            INSERT INTO crypto_ohlcv 
                (time, asset_id, symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time, asset_id) DO NOTHING
        """, (bar_time, asset_id, 'KAS_DAILY', open_cents, high_cents, low_cents, close_cents, volume))
        
        if cursor.rowcount > 0:
            bars_inserted += 1
    
    return bars_inserted

def main():
    """Try multiple sources to fill KAS gap"""
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    try:
        asset_id = get_asset_id(cursor, 'KAS_DAILY')
        print(f"✅ Using asset_id: {asset_id}")
        
        total_inserted = 0
        
        # Try CoinPaprika first (usually has OHLC)
        paprika_data = fetch_from_coinpaprika()
        if paprika_data:
            inserted = process_coinpaprika_data(paprika_data, cursor, asset_id)
            print(f"   Inserted {inserted} bars from CoinPaprika")
            total_inserted += inserted
            conn.commit()
        
        time.sleep(1)  # Be nice to APIs
        
        # Try CryptoCompare (has OHLC)
        compare_data = fetch_from_cryptocompare()
        if compare_data:
            inserted = process_cryptocompare_data(compare_data, cursor, asset_id)
            print(f"   Inserted {inserted} bars from CryptoCompare")
            total_inserted += inserted
            conn.commit()
        
        time.sleep(1)
        
        # Try CoinGecko (price points only, we create OHLC)
        gecko_data = fetch_from_coingecko()
        if gecko_data:
            inserted = process_coingecko_data(gecko_data, cursor, asset_id)
            print(f"   Inserted {inserted} bars from CoinGecko")
            total_inserted += inserted
            conn.commit()
        
        print(f"\n🎉 Total bars inserted: {total_inserted}")
        
        # Check remaining gaps
        cursor.execute("""
            WITH kas_dates AS (
              SELECT 
                time::date as date,
                LAG(time::date) OVER (ORDER BY time) as prev_date,
                time::date - LAG(time::date) OVER (ORDER BY time) as gap_days
              FROM crypto_ohlcv 
              WHERE symbol = 'KAS_DAILY'
              ORDER BY time
            )
            SELECT COUNT(*) as gaps
            FROM kas_dates
            WHERE gap_days > 1
        """)
        gaps = cursor.fetchone()[0]
        print(f"📊 Remaining gaps: {gaps}")
        
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
