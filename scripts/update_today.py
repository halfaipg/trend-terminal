"""
Update today's Bitcoin candle in the database with live data from CoinGecko
Continuously overwrites today's bar until end of day
"""
import psycopg2
from datetime import datetime, timezone
from polygon_realtime import get_realtime_btc_price
from loguru import logger
import os

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/aitrader')


def update_today_candle():
    """
    Fetch live price and update/insert today's candles for ALL timeframes
    Updates: hourly (BTC), 4-hour (BTC_4H), and daily (BTC_DAILY)
    """
    try:
        # Get live price data
        price_data = get_realtime_btc_price()
        if not price_data:
            logger.error("Failed to get live price data")
            return False
        
        # Get current time
        now = datetime.now(timezone.utc)
        
        # Connect to database
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()
        
        # Update 1H bar (current hour)
        current_hour = datetime(now.year, now.month, now.day, now.hour, 0, 0, tzinfo=timezone.utc)
        cursor.execute("SELECT asset_id FROM crypto_assets WHERE symbol = 'BTC'")
        btc_1h_id = cursor.fetchone()[0]
        
        cursor.execute("""
            INSERT INTO crypto_ohlcv 
                (time, asset_id, symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time, asset_id) DO UPDATE SET
                high = GREATEST(crypto_ohlcv.high, EXCLUDED.high),
                low = LEAST(crypto_ohlcv.low, EXCLUDED.low),
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
        """, (
            current_hour, btc_1h_id, 'BTC',
            int(price_data['open'] * 100),
            int(price_data['high'] * 100),
            int(price_data['low'] * 100),
            int(price_data['price'] * 100),
            int(price_data['volume_24h'])
        ))
        
        # Update 4H bar (current 4-hour period)
        current_4h = datetime(now.year, now.month, now.day, (now.hour // 4) * 4, 0, 0, tzinfo=timezone.utc)
        cursor.execute("SELECT asset_id FROM crypto_assets WHERE symbol = 'BTC_4H'")
        btc_4h_id = cursor.fetchone()[0]
        
        cursor.execute("""
            INSERT INTO crypto_ohlcv 
                (time, asset_id, symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time, asset_id) DO UPDATE SET
                high = GREATEST(crypto_ohlcv.high, EXCLUDED.high),
                low = LEAST(crypto_ohlcv.low, EXCLUDED.low),
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
        """, (
            current_4h, btc_4h_id, 'BTC_4H',
            int(price_data['open'] * 100),
            int(price_data['high'] * 100),
            int(price_data['low'] * 100),
            int(price_data['price'] * 100),
            int(price_data['volume_24h'])
        ))
        
        # Update Daily bar (today at midnight)
        today_midnight = datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=timezone.utc)
        cursor.execute("SELECT asset_id FROM crypto_assets WHERE symbol = 'BTC_DAILY'")
        btc_daily_id = cursor.fetchone()[0]
        
        cursor.execute("""
            INSERT INTO crypto_ohlcv 
                (time, asset_id, symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (time, asset_id) DO UPDATE SET
                high = GREATEST(crypto_ohlcv.high, EXCLUDED.high),
                low = LEAST(crypto_ohlcv.low, EXCLUDED.low),
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
        """, (
            today_midnight, btc_daily_id, 'BTC_DAILY',
            int(price_data['open'] * 100),
            int(price_data['high'] * 100),
            int(price_data['low'] * 100),
            int(price_data['price'] * 100),
            int(price_data['volume_24h'])
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ Updated all timeframes: O=${price_data['open']:,.2f} H=${price_data['high']:,.2f} L=${price_data['low']:,.2f} C=${price_data['price']:,.2f}")
        return True
        
    except Exception as e:
        logger.error(f"Error updating today's candles: {e}")
        return False


if __name__ == '__main__':
    # Test updating today's candle
    success = update_today_candle()
    if success:
        print("\n✅ Today's candle updated successfully!")
    else:
        print("\n❌ Failed to update today's candle")

