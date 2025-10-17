#!/usr/bin/env python3
"""
Initialize Bitcoin data - create tables and download initial data
"""
import sys
sys.path.append('backend')

from btc_data_client import BitcoinDataClient
from loguru import logger

def main():
    """Initialize database and download Bitcoin data"""
    
    logger.info("Initializing Bitcoin data...")
    
    # Create client
    client = BitcoinDataClient()
    
    # Download last 30 days of Bitcoin data
    logger.info("Downloading Bitcoin data for last 30 days...")
    result = client.update_btc_data('BTC', days_back=30)
    
    logger.info(f"Download complete: {result}")
    
    # Check what we got
    for tf in ['1h', '4h', '1d']:
        df = client.get_latest_btc_data('BTC', tf, limit=10)
        if df is not None and not df.empty:
            logger.info(f"{tf} timeframe: {len(df)} bars loaded")
            logger.info(f"Latest price: ${df['close'].iloc[-1]:.2f}")
        else:
            logger.warning(f"No data for {tf} timeframe")

if __name__ == '__main__':
    main()

