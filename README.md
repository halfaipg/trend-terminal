# Trend Terminal

A Bitcoin trend-following strategy implementation with real-time charting and Hull Moving Average signals.

## Features

- **Real-time Bitcoin price tracking** with live updates
- **Interactive candlestick charts** using TradingView Lightweight Charts
- **Hull Moving Average strategy** with configurable trend filter
- **Multiple timeframes** (1h, 4h, daily)
- **Dark/light theme** toggle
- **Discord alerts** for trading signals
- **RESTful API** for data access

## Strategy

The strategy uses two Hull Moving Averages:
- **Short-term Hull MA (220)**: Primary signal line (green/red)
- **Long-term Hull MA (1000)**: Trend filter (cyan/orange)

### Trading Logic

**With Trend Filter ON (default):**
- Requires both Hull MA and trend filter to agree
- Can be flat when they disagree
- More conservative approach

**With Trend Filter OFF:**
- Only uses Hull MA direction
- Always in the market (symmetrical)
- Pure momentum strategy

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL database
- Polygon API key (for real-time data)

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/halfaipg/trend-terminal.git
   cd trend-terminal
   ```

2. **Create virtual environment:**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your API keys and database credentials
   ```

5. **Initialize database:**
   ```bash
   python scripts/create_crypto_tables.sql
   ```

6. **Run the application:**
   ```bash
   python app.py
   ```

7. **Access the dashboard:**
   - Open http://localhost:8000 in your browser
   - Or use ngrok for public access: `ngrok http 8000`

## API Endpoints

- `GET /` - Main dashboard
- `GET /api/signal/{timeframe}` - Get current strategy signal
- `GET /api/chart/{timeframe}?limit=N` - Get OHLCV data with indicators
- `GET /api/price/live` - Real-time Bitcoin price
- `POST /api/update` - Update data and check for signals
- `POST /api/test-discord` - Test Discord webhook

## Configuration

### Environment Variables

```env
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/btc_trend

# Polygon API
POLYGON_API_KEY=your_polygon_api_key

# Discord Alerts
DISCORD_WEBHOOK_URL=your_discord_webhook_url
```

### Strategy Parameters

- **Hull MA Length**: 220 (short-term), 1000 (long-term)
- **Trend Filter**: Enable/disable long-term trend requirement
- **Entry Confirmation**: 0-5 bars delay
- **Stop Loss**: Optional protective stops

## Data Sources

- **Primary**: Polygon API for real-time and historical data
- **Backup**: Yahoo Finance (yfinance) for fallback
- **Database**: PostgreSQL for data storage

## File Structure

```
trend-terminal/
├── app.py                 # FastAPI main application
├── frontend/
│   ├── index.html         # Dashboard UI
│   └── terminal_styles.css # Styling
├── scripts/
│   ├── btc_data_client.py # Data fetching
│   ├── strategy.py        # Trading strategy logic
│   ├── discord_alerts.py  # Discord notifications
│   └── *.py              # Other utility scripts
├── requirements.txt       # Python dependencies
└── README.md             # This file
```

## TradingView Strategy

The Pine Script strategy is available in `scripts/btc_trend_following.txt` and can be imported into TradingView for backtesting.

### Key Features:
- Hull Moving Average implementation
- Configurable trend filter toggle
- Symmetrical trading (always in market when trend filter disabled)
- Protective stop loss options
- Multiple Hull MA variations (HMA, EHMA, THMA)

## Development

### Adding New Cryptocurrencies

1. Update the data client to support new symbols
2. Add new database tables if needed
3. Update the frontend to display new options

### Customizing the Strategy

- Modify Hull MA lengths in `strategy.py`
- Adjust signal logic in the strategy functions
- Update the Pine Script for TradingView compatibility

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## Support

For issues and questions, please open an issue on GitHub or contact the maintainers.