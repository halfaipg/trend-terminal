"""
Discord Webhook Alerts for Bitcoin Trend Strategy

Sends formatted trade alerts to Discord when strategy signals change.
"""
import os
import requests
import json
from datetime import datetime
from typing import Dict, Optional
from loguru import logger
import sys

# Import from symlinked backend
sys.path.append('backend')
from strategy import BitcoinTrendStrategy


class DiscordAlerts:
    """
    Discord webhook alerts for trading signals
    """
    
    def __init__(self, webhook_url: Optional[str] = None):
        """
        Initialize Discord alerts
        
        Args:
            webhook_url: Discord webhook URL (or set DISCORD_WEBHOOK_URL env var)
        """
        self.webhook_url = webhook_url or os.getenv('DISCORD_WEBHOOK_URL')
        if not self.webhook_url:
            logger.warning("No Discord webhook URL provided. Alerts will be disabled.")
            self.enabled = False
        else:
            self.enabled = True
            logger.info("Discord alerts enabled")
    
    def _format_signal_embed(self, signal_data: Dict) -> Dict:
        """
        Format signal data into Discord embed
        
        Args:
            signal_data: Signal data from strategy
            
        Returns:
            Discord embed dictionary
        """
        # Determine embed color based on signal
        signal = signal_data.get('last_signal', 'hold')
        position = signal_data.get('current_position', 'flat')
        
        if 'long' in signal:
            color = 0x00ff00  # Green
            emoji = "🟢"
        elif 'short' in signal:
            color = 0xff0000  # Red
            emoji = "🔴"
        else:
            color = 0xffff00  # Yellow
            emoji = "🟡"
        
        # Format price
        price = signal_data.get('close_price', 0)
        price_str = f"${price:,.2f}"
        
        # Format timestamp
        timestamp = signal_data.get('timestamp', datetime.now().isoformat())
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            time_str = dt.strftime('%Y-%m-%d %H:%M:%S UTC')
        except:
            time_str = timestamp
        
        # Create embed
        embed = {
            "title": f"{emoji} Bitcoin Trend Signal",
            "color": color,
            "timestamp": timestamp,
            "fields": [
                {
                    "name": "Symbol",
                    "value": signal_data.get('symbol', 'BTC1!'),
                    "inline": True
                },
                {
                    "name": "Timeframe",
                    "value": signal_data.get('timeframe', '1h'),
                    "inline": True
                },
                {
                    "name": "Price",
                    "value": price_str,
                    "inline": True
                },
                {
                    "name": "Signal",
                    "value": signal.upper().replace('_', ' '),
                    "inline": True
                },
                {
                    "name": "Position",
                    "value": position.upper(),
                    "inline": True
                },
                {
                    "name": "Volume",
                    "value": f"{signal_data.get('volume', 0):,.0f}",
                    "inline": True
                },
                {
                    "name": "Hull MA (48)",
                    "value": f"${signal_data.get('hull_value', 0):,.2f}",
                    "inline": True
                },
                {
                    "name": "Trend Filter (1000)",
                    "value": f"${signal_data.get('trend_value', 0):,.2f}",
                    "inline": True
                },
                {
                    "name": "Signal Strength",
                    "value": f"{signal_data.get('signal_strength', 0):.2f}",
                    "inline": True
                }
            ],
            "footer": {
                "text": "Bitcoin Trend Following Strategy"
            }
        }
        
        # Add description based on signal
        if 'long_entry' in signal:
            embed["description"] = "🚀 **LONG ENTRY** - Trend is up and Hull MA is bullish"
        elif 'long_exit' in signal:
            embed["description"] = "📉 **LONG EXIT** - Trend is up but Hull MA turned bearish"
        elif 'short_entry' in signal:
            embed["description"] = "📉 **SHORT ENTRY** - Trend is down and Hull MA is bearish"
        elif 'short_exit' in signal:
            embed["description"] = "🚀 **SHORT EXIT** - Trend is down but Hull MA turned bullish"
        else:
            embed["description"] = "⏸️ **HOLD** - No signal change"
        
        return embed
    
    def send_signal_alert(self, signal_data: Dict) -> bool:
        """
        Send signal alert to Discord
        
        Args:
            signal_data: Signal data from strategy
            
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.enabled:
            logger.debug("Discord alerts disabled")
            return False
        
        try:
            # Format embed
            embed = self._format_signal_embed(signal_data)
            
            # Create webhook payload
            payload = {
                "embeds": [embed],
                "username": "Bitcoin Trend Bot",
                "avatar_url": "https://cdn-icons-png.flaticon.com/512/825/825454.png"
            }
            
            # Send webhook
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 204:
                logger.info(f"Discord alert sent: {signal_data.get('last_signal', 'unknown')}")
                return True
            else:
                logger.error(f"Discord webhook failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending Discord alert: {e}")
            return False
    
    def send_test_alert(self) -> bool:
        """
        Send test alert to Discord
        
        Returns:
            True if sent successfully, False otherwise
        """
        test_data = {
            'timestamp': datetime.now().isoformat(),
            'symbol': 'BTC1!',
            'timeframe': '1h',
            'current_position': 'flat',
            'last_signal': 'test',
            'close_price': 50000.0,
            'volume': 1000000,
            'hull_value': 50100.0,
            'trend_value': 49900.0,
            'signal_strength': 0.0
        }
        
        return self.send_signal_alert(test_data)
    
    def send_error_alert(self, error_message: str, context: str = "") -> bool:
        """
        Send error alert to Discord
        
        Args:
            error_message: Error message
            context: Additional context
            
        Returns:
            True if sent successfully, False otherwise
        """
        if not self.enabled:
            return False
        
        try:
            embed = {
                "title": "🚨 Strategy Error",
                "color": 0xff0000,  # Red
                "timestamp": datetime.now().isoformat(),
                "fields": [
                    {
                        "name": "Error",
                        "value": error_message,
                        "inline": False
                    },
                    {
                        "name": "Context",
                        "value": context or "Unknown",
                        "inline": False
                    }
                ],
                "footer": {
                    "text": "Bitcoin Trend Following Strategy"
                }
            }
            
            payload = {
                "embeds": [embed],
                "username": "Bitcoin Trend Bot",
                "avatar_url": "https://cdn-icons-png.flaticon.com/512/825/825454.png"
            }
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            return response.status_code == 204
            
        except Exception as e:
            logger.error(f"Error sending error alert: {e}")
            return False


# Convenience function
def send_btc_alert(signal_data: Dict) -> bool:
    """
    Quick function to send Bitcoin signal alert
    
    Args:
        signal_data: Signal data from strategy
        
    Returns:
        True if sent successfully
    """
    alerts = DiscordAlerts()
    return alerts.send_signal_alert(signal_data)


if __name__ == '__main__':
    # Test Discord alerts
    alerts = DiscordAlerts()
    
    # Send test alert
    success = alerts.send_test_alert()
    print(f"Test alert sent: {success}")
    
    # Test with real signal data
    strategy = BitcoinTrendStrategy()
    signal = strategy.get_current_signal('1h')
    
    if 'error' not in signal:
        success = alerts.send_signal_alert(signal)
        print(f"Real signal alert sent: {success}")
    else:
        print(f"Error getting signal: {signal['error']}")

