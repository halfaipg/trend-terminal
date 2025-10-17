#!/bin/bash

# Bitcoin Data Update Script
# Run this as a cron job to update data periodically

echo "Updating Bitcoin data..."

# Activate virtual environment
source venv/bin/activate

# Set environment variables
export DATABENTO_API_KEY=${DATABENTO_API_KEY:-"your_databento_api_key_here"}
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/aitrader"
export DISCORD_WEBHOOK_URL=""

# Run data update
python -c "
from btc_data_client import BitcoinDataClient
from strategy import BitcoinTrendStrategy
from discord_alerts import DiscordAlerts

# Update data
client = BitcoinDataClient()
result = client.update_btc_data('BTC1!', days_back=1)
print(f'Data update result: {result}')

# Check for new signals
strategy = BitcoinTrendStrategy()
alerts = DiscordAlerts()

for timeframe in ['1h', '4h', '1d']:
    signal = strategy.get_current_signal(timeframe)
    if 'error' not in signal and signal.get('is_new_signal', False):
        print(f'New signal on {timeframe}: {signal.get(\"last_signal\")}')
        alerts.send_signal_alert(signal)
"

