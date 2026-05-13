-- Migration: exchange-native closed 15 minute futures OHLCV candles.
-- Apply on the alt-scraper database (DATABASE_URL used by realtime_daemon.py).

CREATE TABLE IF NOT EXISTS futures_klines_15m (
    candle_open_at      TIMESTAMPTZ  NOT NULL,
    candle_close_at     TIMESTAMPTZ  NOT NULL,
    symbol              VARCHAR(50)  NOT NULL,
    exchange            VARCHAR(50)  NOT NULL,
    base_asset          VARCHAR(20),

    price_open          DECIMAL(24, 8),
    price_high          DECIMAL(24, 8),
    price_low           DECIMAL(24, 8),
    price_close         DECIMAL(24, 8),

    volume_base         DECIMAL(24, 8),
    volume_usd          DECIMAL(24, 4),
    buy_volume_base     DECIMAL(24, 8),
    sell_volume_base    DECIMAL(24, 8),
    volume_delta        DECIMAL(24, 8),
    txn_count           BIGINT,

    polled_at           TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (candle_open_at, symbol, exchange)
);

CREATE INDEX IF NOT EXISTS idx_fk15_symbol_at
    ON futures_klines_15m(symbol, exchange, candle_open_at DESC);
CREATE INDEX IF NOT EXISTS idx_fk15_base_at
    ON futures_klines_15m(base_asset, candle_open_at DESC);
CREATE INDEX IF NOT EXISTS idx_fk15_at
    ON futures_klines_15m(candle_open_at DESC);

DROP TRIGGER IF EXISTS trigger_update_timestamp_fk15 ON futures_klines_15m;
CREATE TRIGGER trigger_update_timestamp_fk15
    BEFORE UPDATE ON futures_klines_15m
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
