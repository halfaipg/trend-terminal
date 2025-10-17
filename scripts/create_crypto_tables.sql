-- Create crypto_assets table
CREATE TABLE IF NOT EXISTS crypto_assets (
    asset_id SERIAL PRIMARY KEY,
    symbol VARCHAR(50) NOT NULL,
    name VARCHAR(200),
    exchange VARCHAR(100),
    tick_size NUMERIC(20, 8),
    multiplier NUMERIC(20, 8),
    currency VARCHAR(10),
    last_trade_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, exchange)
);

-- Create crypto_ohlcv table
CREATE TABLE IF NOT EXISTS crypto_ohlcv (
    time TIMESTAMP WITH TIME ZONE NOT NULL,
    asset_id INTEGER NOT NULL REFERENCES crypto_assets(asset_id),
    symbol VARCHAR(50),
    open BIGINT,
    high BIGINT,
    low BIGINT,
    close BIGINT,
    volume BIGINT,
    PRIMARY KEY (time, asset_id)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_crypto_ohlcv_asset_time ON crypto_ohlcv(asset_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_crypto_ohlcv_symbol_time ON crypto_ohlcv(symbol, time DESC);

-- Insert default BTC assets if not exists
INSERT INTO crypto_assets (symbol, name, exchange, tick_size, multiplier, currency)
VALUES 
    ('BTC', 'Bitcoin Hourly', 'POLYGON', 0.01, 1, 'USD'),
    ('BTC_4H', 'Bitcoin 4-Hour', 'POLYGON', 0.01, 1, 'USD'),
    ('BTC_DAILY', 'Bitcoin Daily', 'POLYGON', 0.01, 1, 'USD'),
    ('KAS_DAILY', 'Kaspa Daily', 'POLYGON', 0.0001, 1, 'USD')
ON CONFLICT (symbol, exchange) DO NOTHING;

