"""
Bitcoin Trend Following API

FastAPI backend for Bitcoin trend following strategy with TradingView charts.
Supports both traditional FastAPI and Cloudflare Workers.
"""
import os
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from loguru import logger
import sys

# Import from symlinked backend
sys.path.append('backend')
from btc_data_client import BitcoinDataClient
from strategy import BitcoinTrendStrategy
from discord_alerts import DiscordAlerts
from polygon_realtime import get_realtime_btc_price
from update_today import update_today_candle

# Pydantic models
class SignalResponse(BaseModel):
    timestamp: str
    symbol: str
    timeframe: str
    current_position: str
    last_signal: Optional[str]
    last_signal_time: Optional[str]
    hull_value: Optional[float]
    trend_value: Optional[float]
    close_price: float
    volume: float
    is_new_signal: bool
    signal_strength: float

class ChartDataResponse(BaseModel):
    symbol: str
    timeframe: str
    data: List[Dict]
    indicators: Dict

class UpdateResponse(BaseModel):
    status: str
    message: str
    data_update: Optional[Dict] = None
    signal_update: Optional[Dict] = None

# Initialize FastAPI app
app = FastAPI(
    title="Bitcoin Trend Following API",
    description="API for Bitcoin trend following strategy with Hull Moving Average",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for Cloudflare Workers
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize components
btc_client = BitcoinDataClient()
strategy = BitcoinTrendStrategy()
discord_alerts = DiscordAlerts()

# Global state for tracking last signals
last_signals = {}

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the frontend dashboard"""
    html = open("frontend/index.html").read()
    return HTMLResponse(
        content=html,
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0"
        }
    )

@app.get("/terminal_styles.css")
async def serve_css():
    """Serve the CSS file"""
    from fastapi.responses import FileResponse
    return FileResponse(
        "frontend/terminal_styles.css",
        media_type="text/css",
        headers={
            "Cache-Control": "no-cache, no-store, must-revalidate"
        }
    )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/api/signal/{timeframe}", response_model=SignalResponse)
async def get_signal(timeframe: str = "1h"):
    """
    Get current strategy signal
    
    Args:
        timeframe: Data timeframe ('1h', '4h', '1d')
    """
    try:
        signal_data = strategy.get_current_signal(timeframe)
        
        if 'error' in signal_data:
            raise HTTPException(status_code=400, detail=signal_data['error'])
        
        return SignalResponse(**signal_data)
        
    except Exception as e:
        logger.error(f"Error getting signal: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/chart/{timeframe}", response_model=ChartDataResponse)
async def get_chart_data(timeframe: str = "1h", limit: int = 500):
    """
    Get chart data with indicators
    
    Args:
        timeframe: Data timeframe ('1h', '4h', '1d')
        limit: Number of bars to return
    """
    try:
        chart_data = strategy.get_strategy_data(timeframe, limit)
        
        if 'error' in chart_data:
            raise HTTPException(status_code=400, detail=chart_data['error'])
        
        return ChartDataResponse(**chart_data)
        
    except Exception as e:
        logger.error(f"Error getting chart data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/update", response_model=UpdateResponse)
async def update_data_and_signals(background_tasks: BackgroundTasks, symbol: str = "BTC1!", days_back: int = 7):
    """
    Update Bitcoin data and check for new signals
    
    Args:
        symbol: Bitcoin symbol to update
        days_back: Days back to fetch data
    """
    try:
        # Update data
        logger.info(f"Updating {symbol} data for last {days_back} days")
        data_result = btc_client.update_btc_data(symbol, days_back)
        
        # Check signals for all timeframes
        signal_results = {}
        new_signals = []
        
        for timeframe in ['1h', '4h', '1d']:
            signal_data = strategy.get_current_signal(timeframe)
            
            if 'error' not in signal_data:
                signal_results[timeframe] = signal_data
                
                # Check if this is a new signal
                signal_key = f"{symbol}_{timeframe}"
                last_signal = last_signals.get(signal_key)
                
                if (signal_data.get('is_new_signal', False) and 
                    signal_data.get('last_signal') != last_signal):
                    
                    new_signals.append(signal_data)
                    last_signals[signal_key] = signal_data.get('last_signal')
                    
                    # Send Discord alert in background
                    background_tasks.add_task(discord_alerts.send_signal_alert, signal_data)
        
        return UpdateResponse(
            status="success",
            message=f"Updated {symbol} data and checked signals",
            data_update=data_result,
            signal_update={
                "timeframes": signal_results,
                "new_signals": len(new_signals)
            }
        )
        
    except Exception as e:
        logger.error(f"Error updating data and signals: {e}")
        # Send error alert
        background_tasks.add_task(
            discord_alerts.send_error_alert, 
            str(e), 
            f"Update failed for {symbol}"
        )
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/ohlcv/{timeframe}")
async def get_ohlcv_data(timeframe: str = "1h", limit: int = 100):
    """
    Get raw OHLCV data
    
    Args:
        timeframe: Data timeframe
        limit: Number of bars to return
    """
    try:
        df = btc_client.get_latest_btc_data("BTC1!", timeframe, limit)
        
        if df is None:
            raise HTTPException(status_code=404, detail="No data available")
        
        # Convert to chart format
        data = []
        for idx, row in df.iterrows():
            data.append({
                'time': int(idx.timestamp() * 1000),
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'volume': float(row['volume'])
            })
        
        return {
            'symbol': 'BTC1!',
            'timeframe': timeframe,
            'data': data
        }
        
    except Exception as e:
        logger.error(f"Error getting OHLCV data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/test-discord")
async def test_discord_alert():
    """Test Discord webhook"""
    try:
        success = discord_alerts.send_test_alert()
        return {"status": "success" if success else "failed", "message": "Discord test alert sent"}
    except Exception as e:
        logger.error(f"Error testing Discord alert: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/price/live")
async def get_live_price():
    """Get current Bitcoin price from CoinGecko (real-time)"""
    try:
        price_data = get_realtime_btc_price()
        if price_data:
            return JSONResponse(content=price_data)
        else:
            raise HTTPException(status_code=503, detail="Unable to fetch live price")
    except Exception as e:
        logger.error(f"Error getting live price: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/update-today")
async def update_today_bar():
    """Update today's candle in the database with live data"""
    try:
        success = update_today_candle()
        if success:
            return {"status": "success", "message": "Today's candle updated"}
        else:
            raise HTTPException(status_code=503, detail="Failed to update today's candle")
    except Exception as e:
        logger.error(f"Error updating today's candle: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Cloudflare Worker compatibility
def cloudflare_handler(request):
    """
    Cloudflare Worker handler
    
    This function can be used with Cloudflare Workers by importing
    the necessary modules and calling this handler.
    """
    # This would need to be adapted for Cloudflare Workers environment
    # For now, return a placeholder
    return JSONResponse({
        "message": "Cloudflare Worker handler not yet implemented",
        "status": "placeholder"
    })

if __name__ == "__main__":
    import uvicorn
    
    # Run the FastAPI server
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
