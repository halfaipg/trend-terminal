"""
Real-time Bitcoin price updates from Kraken (free, no rate limits)
Provides current price, today's OHLC, and 24h stats
"""
import os
import requests
from datetime import datetime
from loguru import logger


def get_realtime_btc_price():
    """
    Get the latest Bitcoin price from Kraken
    Free API with real-time data, no rate limits
    
    Returns:
        dict with price info or None
    """
    try:
        # Get ticker data from Kraken
        url = "https://api.kraken.com/0/public/Ticker?pair=XBTUSD"
        
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        data = response.json()
        
        if 'result' in data and 'XXBTZUSD' in data['result']:
            ticker = data['result']['XXBTZUSD']
            
            # Extract price data
            current_price = float(ticker['c'][0])  # Last trade price
            high_24h = float(ticker['h'][1])       # 24h high
            low_24h = float(ticker['l'][1])        # 24h low
            open_24h = float(ticker['o'])          # Today's open
            volume_24h = float(ticker['v'][1])     # 24h volume in BTC
            
            # Calculate 24h change
            change_24h = ((current_price - open_24h) / open_24h) * 100
            
            price_info = {
                'symbol': 'BTC',
                'price': current_price,
                'timestamp': int(datetime.now().timestamp() * 1000),
                'volume_24h': volume_24h * current_price,  # Convert to USD
                'change_24h': change_24h,
                'open': open_24h,
                'high': high_24h,
                'low': low_24h,
                'close': current_price,
                'updated_at': datetime.now().isoformat()
            }
            
            logger.info(f"Live BTC price: ${price_info['price']:,.2f} (Kraken)")
            return price_info
            
        else:
            logger.warning(f"Unexpected Kraken response: {data}")
            return None
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching real-time price from Kraken: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error getting live price: {e}")
        return None


if __name__ == '__main__':
    # Test the real-time price fetch
    price = get_realtime_btc_price()
    if price:
        print(f"\n✅ Current Bitcoin Price: ${price['price']:,.2f}")
        print(f"   24h Change: {price['change_24h']:+.2f}%")
        print(f"   Today's Open: ${price['open']:,.2f}")
        print(f"   Today's High: ${price['high']:,.2f}")
        print(f"   Today's Low: ${price['low']:,.2f}")
        print(f"   24h Volume: ${price['volume_24h']:,.0f}")

