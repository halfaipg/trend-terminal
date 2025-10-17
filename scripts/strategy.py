"""
Bitcoin Trend Following Strategy

Implements the Hull Moving Average strategy from the Pine script:
- HULL (220-period) for entry/exit signals
- TREND (1000-period) for trend filter
- Long/Short position management
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from loguru import logger
import sys

# Import from symlinked backend
sys.path.append('backend')
from btc_data_client import BitcoinDataClient


def weighted_moving_average(series: pd.Series, length: int) -> pd.Series:
    """Compute Weighted Moving Average (WMA)."""
    if length <= 0:
        return pd.Series(index=series.index, dtype=float)
    weights = np.arange(1, length + 1, dtype=float)
    # Use rolling apply for WMA
    def wma_calc(x: np.ndarray) -> float:
        return float(np.dot(x, weights) / weights.sum())
    return series.rolling(window=length, min_periods=length).apply(wma_calc, raw=True)


def hull_moving_average(series: pd.Series, length: int) -> pd.Series:
    """
    Hull Moving Average - exactly matching Pine Script
    Pine Script: HMA(_src, _length) = wma(2 * wma(_src, _length / 2) - wma(_src, _length), round(sqrt(_length)))
    """
    if length <= 1:
        return series.copy().astype(float)
    
    # Pine Script uses float division and round()
    half_len = int(round(length / 2.0))
    sqrt_len = int(round(np.sqrt(length)))
    
    # Ensure minimum length of 1
    half_len = max(half_len, 1)
    sqrt_len = max(sqrt_len, 1)

    # Calculate WMAs in the same order as Pine Script
    wma_full = weighted_moving_average(series, length)
    wma_half = weighted_moving_average(series, half_len)

    # 2 * wma_half - wma_full
    inner = (2.0 * wma_half) - wma_full
    
    # Final HMA
    hma = weighted_moving_average(inner, sqrt_len)
    return hma


class BitcoinTrendStrategy:
    """
    Bitcoin Trend Following Strategy using Hull Moving Average
    
    Strategy Logic (from Pine script):
    - Long Entry: TREND[0] > TREND[1] AND HULL[0] > HULL[2]
    - Long Exit: TREND[0] > TREND[1] AND HULL[0] < HULL[2]  
    - Short Entry: TREND[0] < TREND[1] AND HULL[0] < HULL[2]
    - Short Exit: TREND[0] < TREND[1] AND HULL[0] > HULL[2]
    """
    
    def __init__(self, 
                 hull_length: int = 220,
                 trend_length: int = 1000,
                 symbol: str = 'BTC'):
        """
        Initialize strategy
        
        Args:
            hull_length: Hull MA length for signals (default 220)
            trend_length: Hull MA length for trend filter (default 1000)
            symbol: Bitcoin symbol to trade
        """
        self.hull_length = hull_length
        self.trend_length = trend_length
        self.symbol = symbol
        self.data_client = BitcoinDataClient()
        
        # Strategy state
        self.current_position = 'flat'  # 'long', 'short', 'flat'
        self.last_signal = None
        self.last_signal_time = None
        
        logger.info(f"Bitcoin Trend Strategy initialized: HULL={hull_length}, TREND={trend_length}")
    
    def calculate_hull_ma(self, series: pd.Series, length: int) -> pd.Series:
        """
        Calculate Hull Moving Average natively (no external TA lib)
        """
        try:
            return hull_moving_average(series.astype(float), length)
        except Exception as e:
            logger.error(f"Error calculating Hull MA: {e}")
            return pd.Series(index=series.index, dtype=float)
    
    def calculate_indicators(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Calculate all strategy indicators
        """
        if df.empty or len(df) < max(self.hull_length, self.trend_length):
            logger.warning("Insufficient data for indicator calculation")
            return {}
        
        indicators = {}
        
        # Calculate Hull MA for signals (220-period)
        indicators['hull'] = self.calculate_hull_ma(df['close'], self.hull_length)
        
        # Calculate Hull MA for trend filter (1000-period)
        indicators['trend'] = self.calculate_hull_ma(df['close'], self.trend_length)
        
        # Calculate Hull MA shifted for comparison (HULL[2])
        indicators['hull_shifted'] = indicators['hull'].shift(2)
        
        # Calculate trend shifted for comparison (TREND[1])
        indicators['trend_shifted'] = indicators['trend'].shift(1)
        
        return indicators
    
    def generate_signals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Generate trading signals based on Hull MA strategy
        """
        if df.empty:
            return pd.DataFrame()
        
        # Calculate indicators
        indicators = self.calculate_indicators(df)
        if not indicators:
            return df
        
        # Create signals dataframe
        signals_df = df.copy()
        
        # Add indicators
        signals_df['hull'] = indicators['hull']
        signals_df['trend'] = indicators['trend']
        signals_df['hull_shifted'] = indicators['hull_shifted']
        signals_df['trend_shifted'] = indicators['trend_shifted']
        
        # Initialize signal columns
        signals_df['signal'] = 'hold'
        signals_df['position'] = 'flat'
        signals_df['signal_strength'] = 0.0
        
        # Generate signals - only on position changes (matching Pine Script logic)
        for i in range(1, len(signals_df)):
            current_hull = signals_df['hull'].iloc[i]
            current_trend = signals_df['trend'].iloc[i]
            hull_shifted = signals_df['hull_shifted'].iloc[i]
            trend_shifted = signals_df['trend_shifted'].iloc[i]
            prev_position = signals_df['position'].iloc[i-1]
            
            # Skip if any indicator is NaN
            if pd.isna(current_hull) or pd.isna(current_trend) or pd.isna(hull_shifted) or pd.isna(trend_shifted):
                signals_df.iloc[i, signals_df.columns.get_loc('position')] = prev_position
                continue
            
            # Check agreement conditions
            hull_up = current_hull > hull_shifted
            hull_down = current_hull < hull_shifted
            trend_up = current_trend > trend_shifted
            trend_down = current_trend < trend_shifted
            
            long_agree = trend_up and hull_up
            short_agree = trend_down and hull_down
            
            # Determine new position
            new_position = prev_position
            signal = 'hold'
            
            # Long entry: both agree long AND not already long
            if long_agree and prev_position != 'long':
                new_position = 'long'
                signal = 'long_entry'
                signals_df.iloc[i, signals_df.columns.get_loc('signal_strength')] = 1.0
            
            # Short entry: both agree short AND not already short
            elif short_agree and prev_position != 'short':
                new_position = 'short'
                signal = 'short_entry'
                signals_df.iloc[i, signals_df.columns.get_loc('signal_strength')] = -1.0
            
            # Exit long: was long but hull turns down
            elif prev_position == 'long' and hull_down:
                new_position = 'flat'
                signal = 'long_exit'
                signals_df.iloc[i, signals_df.columns.get_loc('signal_strength')] = -1.0
            
            # Exit short: was short but hull turns up
            elif prev_position == 'short' and hull_up:
                new_position = 'flat'
                signal = 'short_exit'
                signals_df.iloc[i, signals_df.columns.get_loc('signal_strength')] = 1.0
            
            signals_df.iloc[i, signals_df.columns.get_loc('signal')] = signal
            signals_df.iloc[i, signals_df.columns.get_loc('position')] = new_position
        
        return signals_df
    
    def get_current_signal(self, timeframe: str = '1h') -> Dict:
        """
        Get current strategy signal and state
        """
        try:
            # Use pre-computed bars for faster loading
            if timeframe == '4h':
                symbol = 'BTC_4H'
            elif timeframe == '1d':
                symbol = 'BTC_DAILY'
            else:
                symbol = self.symbol
            
            # Get latest data
            df = self.data_client.get_latest_btc_data(symbol, timeframe, limit=2000)
            if df is None or df.empty:
                return {'error': 'No data available'}
            
            # Generate signals
            signals_df = self.generate_signals(df)
            if signals_df.empty:
                return {'error': 'Could not generate signals'}
            
            # Get latest signal
            latest = signals_df.iloc[-1]
            latest_time = latest.name
            
            # Check if this is a new signal
            is_new_signal = (
                self.last_signal_time is None or 
                latest_time > self.last_signal_time
            )
            
            # Update state if new signal
            if is_new_signal and latest['signal'] != 'hold':
                self.last_signal = latest['signal']
                self.last_signal_time = latest_time
                self.current_position = latest['position']
            
            return {
                'timestamp': latest_time.isoformat(),
                'symbol': self.symbol,
                'timeframe': timeframe,
                'current_position': self.current_position,
                'last_signal': self.last_signal,
                'last_signal_time': self.last_signal_time.isoformat() if self.last_signal_time else None,
                'hull_value': float(latest['hull']) if not pd.isna(latest['hull']) else None,
                'trend_value': float(latest['trend']) if not pd.isna(latest['trend']) else None,
                'hull_shifted': float(latest['hull_shifted']) if not pd.isna(latest['hull_shifted']) else None,
                'trend_shifted': float(latest['trend_shifted']) if not pd.isna(latest['trend_shifted']) else None,
                'close_price': float(latest['close']),
                'volume': float(latest['volume']),
                'is_new_signal': is_new_signal and latest['signal'] != 'hold',
                'signal_strength': float(latest['signal_strength'])
            }
            
        except Exception as e:
            logger.error(f"Error getting current signal: {e}")
            return {'error': str(e)}
    
    def get_strategy_data(self, timeframe: str = '1h', limit: int = 500) -> Dict:
        """
        Get strategy data for charting
        """
        try:
            # Use pre-computed bars for faster loading
            if timeframe == '4h':
                symbol = 'BTC_4H'
            elif timeframe == '1d':
                symbol = 'BTC_DAILY'
            else:
                symbol = self.symbol
            
            # Get MORE data than requested to calculate indicators (need 1000+ bars)
            fetch_limit = max(limit + self.trend_length, 2000)
            df = self.data_client.get_latest_btc_data(symbol, timeframe, fetch_limit)
            if df is None or df.empty:
                return {'error': 'No data available'}
            
            # Try to generate signals
            has_indicators = len(df) >= self.trend_length
            
            if has_indicators:
                signals_df = self.generate_signals(df)
                if signals_df.empty:
                    has_indicators = False
            
            # If not enough data for indicators, just return raw OHLCV
            if not has_indicators:
                logger.warning(f"Not enough data for {timeframe} indicators (need {self.trend_length}, have {len(df)})")
                signals_df = df.copy()
                signals_df['hull'] = None
                signals_df['trend'] = None
                signals_df['hull_shifted'] = None
                signals_df['signal'] = 'hold'
                signals_df['position'] = 'flat'
                signals_df['signal_strength'] = 0.0
            
            # Return only requested number of bars
            signals_df = signals_df.tail(limit)
            
            # Convert to chart-ready format
            chart_data = []
            for idx, row in signals_df.iterrows():
                chart_data.append({
                    'time': int(idx.timestamp() * 1000),  # JavaScript timestamp
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': float(row['volume']),
                    'hull': float(row['hull']) if not pd.isna(row['hull']) else None,
                    'trend': float(row['trend']) if not pd.isna(row['trend']) else None,
                    'hull_shifted': float(row['hull_shifted']) if not pd.isna(row['hull_shifted']) else None,
                    'signal': row['signal'],
                    'position': row['position'],
                    'signal_strength': float(row['signal_strength'])
                })
            
            return {
                'symbol': self.symbol,
                'timeframe': timeframe,
                'data': chart_data,
                'indicators': {
                    'hull_length': self.hull_length,
                    'trend_length': self.trend_length
                },
                'warning': None if has_indicators else f'Insufficient data for strategy (need {self.trend_length}+ bars, showing price only)'
            }
            
        except Exception as e:
            logger.error(f"Error getting strategy data: {e}")
            return {'error': str(e)}
    
    def backtest_strategy(self, 
                         start_date: str, 
                         end_date: str, 
                         timeframe: str = '1h',
                         initial_capital: float = 100000) -> Dict:
        """
        Backtest the strategy (placeholder)
        """
        try:
            logger.info(f"Backtesting strategy from {start_date} to {end_date}")
            return {
                'status': 'placeholder',
                'message': 'Backtesting not yet implemented',
                'start_date': start_date,
                'end_date': end_date,
                'timeframe': timeframe,
                'initial_capital': initial_capital
            }
        except Exception as e:
            logger.error(f"Error in backtest: {e}")
            return {'error': str(e)}


# Convenience function
def get_btc_signal(timeframe: str = '1h') -> Dict:
    """Quick function to get current Bitcoin signal"""
    strategy = BitcoinTrendStrategy()
    return strategy.get_current_signal(timeframe)


if __name__ == '__main__':
    # Example usage
    strategy = BitcoinTrendStrategy()
    
    # Get current signal
    signal = strategy.get_current_signal('1h')
    print(f"Current signal: {signal}")
    
    # Get strategy data for charting
    data = strategy.get_strategy_data('1h', limit=100)
    if 'error' not in data:
        print(f"Got {len(data['data'])} bars of strategy data")
