-- ==============================================================================
-- ALTS-SCRAPER DATABASE SCHEMA
-- Supabase (PostgreSQL) Schema for Crypto Futures Data
-- ==============================================================================
-- Version: 1.0.0
-- Date: 2026-01-10
-- Description: Schema for storing historical crypto futures/perpetual data
--              from Coinalyze API with support for multiple exchanges.
-- ==============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Migration: Add missing columns if table already exists
DO $$ 
BEGIN
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = 'futures_daily_metrics') THEN
        ALTER TABLE futures_daily_metrics ADD COLUMN IF NOT EXISTS oi_usd_open DECIMAL(24, 4);
        ALTER TABLE futures_daily_metrics ADD COLUMN IF NOT EXISTS oi_usd_high DECIMAL(24, 4);
        ALTER TABLE futures_daily_metrics ADD COLUMN IF NOT EXISTS oi_usd_low DECIMAL(24, 4);
        ALTER TABLE futures_daily_metrics ADD COLUMN IF NOT EXISTS pred_funding_open DECIMAL(18, 10);
        ALTER TABLE futures_daily_metrics ADD COLUMN IF NOT EXISTS pred_funding_high DECIMAL(18, 10);
        ALTER TABLE futures_daily_metrics ADD COLUMN IF NOT EXISTS pred_funding_low DECIMAL(18, 10);
        ALTER TABLE futures_daily_metrics ADD COLUMN IF NOT EXISTS pred_funding_close DECIMAL(18, 10);
        ALTER TABLE futures_daily_metrics ADD COLUMN IF NOT EXISTS ls_acc_global DECIMAL(12, 6);
        ALTER TABLE futures_daily_metrics ADD COLUMN IF NOT EXISTS ls_acc_top DECIMAL(12, 6);
        ALTER TABLE futures_daily_metrics ADD COLUMN IF NOT EXISTS ls_pos_top DECIMAL(12, 6);
        ALTER TABLE futures_daily_metrics ADD COLUMN IF NOT EXISTS sell_volume_base DECIMAL(24, 8);
        ALTER TABLE futures_daily_metrics ADD COLUMN IF NOT EXISTS volume_delta DECIMAL(24, 8);
        ALTER TABLE futures_daily_metrics ADD COLUMN IF NOT EXISTS buy_txn_count BIGINT;
        ALTER TABLE futures_daily_metrics ADD COLUMN IF NOT EXISTS sell_txn_count BIGINT;
    END IF;
    
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = 'spot_daily_ohlcv') THEN
        ALTER TABLE spot_daily_ohlcv ADD COLUMN IF NOT EXISTS buy_volume_base DECIMAL(24, 8);
        ALTER TABLE spot_daily_ohlcv ADD COLUMN IF NOT EXISTS sell_volume_base DECIMAL(24, 8);
        ALTER TABLE spot_daily_ohlcv ADD COLUMN IF NOT EXISTS volume_delta DECIMAL(24, 8);
        ALTER TABLE spot_daily_ohlcv ADD COLUMN IF NOT EXISTS buy_txn_count BIGINT;
        ALTER TABLE spot_daily_ohlcv ADD COLUMN IF NOT EXISTS sell_txn_count BIGINT;
    END IF;
END $$;

-- ==============================================================================
-- EXCHANGES TABLE
-- Stores exchange metadata and identification codes
-- ==============================================================================
CREATE TABLE IF NOT EXISTS exchanges (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(50) NOT NULL UNIQUE,
    code            VARCHAR(10),                    -- Coinalyze exchange code (A, 2, 6, etc.)
    display_name    VARCHAR(100),
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT exchanges_name_check CHECK (name ~ '^[a-z]+$')
);

-- Seed exchange data (codes verified from Coinalyze API)
INSERT INTO exchanges (name, code, display_name) VALUES
    ('binance', 'A', 'Binance Futures'),
    ('bybit', '6', 'Bybit'),
    ('okx', '3', 'OKX'),
    ('deribit', '8', 'Deribit'),
    ('bitget', '4', 'Bitget'),
    ('gate', 'Y', 'Gate.io'),
    ('huobi', 'H', 'Huobi (HTX)'),
    ('kraken', 'K', 'Kraken Futures'),
    ('bitmex', '7', 'BitMEX'),
    ('kucoin', '0', 'KuCoin'),
    ('mexc', 'V', 'MEXC'),
    ('phemex', 'W', 'Phemex'),
    ('coinex', 'F', 'CoinEx')
ON CONFLICT (name) DO UPDATE SET 
    code = EXCLUDED.code,
    display_name = EXCLUDED.display_name;

-- ==============================================================================
-- SYMBOLS TABLE
-- Stores trading pair/symbol metadata
-- ==============================================================================
CREATE TABLE IF NOT EXISTS symbols (
    id              SERIAL PRIMARY KEY,
    base_asset      VARCHAR(20) NOT NULL,           -- Base asset (BTC, ETH, SOL, etc.)
    quote_asset     VARCHAR(20) DEFAULT 'USDT',     -- Quote asset
    symbol          VARCHAR(50) NOT NULL,           -- Full Coinalyze symbol (e.g., BTCUSDT_PERP.A)
    exchange_id     INTEGER REFERENCES exchanges(id),
    contract_type   VARCHAR(20) DEFAULT 'perpetual',
    is_active       BOOLEAN DEFAULT true,
    first_data_date DATE,
    last_data_date  DATE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    
    CONSTRAINT symbols_unique UNIQUE (symbol, exchange_id)
);

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_symbols_base_asset ON symbols(base_asset);
CREATE INDEX IF NOT EXISTS idx_symbols_exchange ON symbols(exchange_id);

-- ==============================================================================
-- ASSET METADATA TABLE
-- Stores token narratives and filtering status (Normalized)
-- ==============================================================================
CREATE TABLE IF NOT EXISTS asset_metadata (
    symbol          VARCHAR(20) PRIMARY KEY,        -- Base asset (BTC, ETH, etc.)
    narrative       VARCHAR(100),                   -- Selected primary category
    is_filtered     BOOLEAN DEFAULT false,          -- Whether it's a stable/wrapped/staked coin
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Index for narrative searches
CREATE INDEX IF NOT EXISTS idx_asset_metadata_narrative ON asset_metadata(narrative);

-- ==============================================================================
-- FUTURES DAILY METRICS TABLE
-- Main table for daily futures/perpetual data
-- ==============================================================================
CREATE TABLE IF NOT EXISTS futures_daily_metrics (
    id                  BIGSERIAL,
    
    -- Primary identifiers
    date                DATE NOT NULL,
    symbol              VARCHAR(50) NOT NULL,       -- Full Coinalyze symbol
    exchange            VARCHAR(50) NOT NULL,       -- Exchange name (binance, bybit, etc.)
    base_asset          VARCHAR(20),                -- Extracted base asset (BTC, ETH, etc.)
    
    -- Open Interest (USD)
    oi_usd_open         DECIMAL(24, 4),
    oi_usd_high         DECIMAL(24, 4),
    oi_usd_low          DECIMAL(24, 4),
    oi_usd_close        DECIMAL(24, 4),
    
    -- Funding Rate (percentage as decimal, e.g., 0.0001 = 0.01%)
    funding_open        DECIMAL(18, 10),
    funding_high        DECIMAL(18, 10),
    funding_low         DECIMAL(18, 10),
    funding_close       DECIMAL(18, 10),
    
    -- Predicted Funding Rate
    pred_funding_open   DECIMAL(18, 10),
    pred_funding_high   DECIMAL(18, 10),
    pred_funding_low    DECIMAL(18, 10),
    pred_funding_close  DECIMAL(18, 10),
    
    -- Long/Short Ratios
    ls_ratio            DECIMAL(12, 6),             -- Generic Longs / Shorts ratio
    longs_qty           DECIMAL(24, 8),             -- Quantity of long positions
    shorts_qty          DECIMAL(24, 8),             -- Quantity of short positions
    ls_acc_global       DECIMAL(12, 6),             -- Global Account L/S Ratio
    ls_acc_top          DECIMAL(12, 6),             -- Top Trader Account L/S Ratio
    ls_pos_top          DECIMAL(12, 6),             -- Top Trader Position L/S Ratio
    
    -- Liquidations (USD)
    liq_longs           DECIMAL(24, 4),             -- Long liquidations volume
    liq_shorts          DECIMAL(24, 4),             -- Short liquidations volume
    liq_total           DECIMAL(24, 4),             -- Total liquidations
    
    -- OHLCV (Price and Volume)
    price_open          DECIMAL(24, 8),
    price_high          DECIMAL(24, 8),
    price_low           DECIMAL(24, 8),
    price_close         DECIMAL(24, 8),
    volume_usd          DECIMAL(24, 4),             -- Volume in USD (estimated)
    volume_base         DECIMAL(24, 8),             -- Total volume in base asset
    buy_volume_base     DECIMAL(24, 8),             -- Buy volume in base asset
    sell_volume_base    DECIMAL(24, 8),             -- Sell volume in base asset
    volume_delta        DECIMAL(24, 8),             -- (Buy - Sell) volume
    txn_count           BIGINT,                     -- Total number of transactions
    buy_txn_count       BIGINT,                     -- Number of buy transactions
    sell_txn_count      BIGINT,                     -- Number of sell transactions
    
    -- Metadata
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    
    -- Composite Primary Key for duplicate detection
    PRIMARY KEY (date, symbol, exchange)
);

-- ==============================================================================
-- SPOT DAILY OHLCV TABLE
-- Table for historical spot market data
-- ==============================================================================
CREATE TABLE IF NOT EXISTS spot_daily_ohlcv (
    id                  BIGSERIAL,
    date                DATE NOT NULL,
    symbol              VARCHAR(50) NOT NULL,       -- Exchange symbol (BTCUSDT, BTC-USDT)
    exchange            VARCHAR(50) NOT NULL,       -- binance, bybit, okx
    
    price_open          DECIMAL(24, 8),
    price_high          DECIMAL(24, 8),
    price_low           DECIMAL(24, 8),
    price_close         DECIMAL(24, 8),
    volume_base         DECIMAL(24, 8),
    volume_usd          DECIMAL(24, 4),
    
    -- Volume Delta (where available, e.g. Binance)
    buy_volume_base     DECIMAL(24, 8),
    sell_volume_base    DECIMAL(24, 8),
    volume_delta        DECIMAL(24, 8),
    txn_count           BIGINT,
    buy_txn_count       BIGINT,
    sell_txn_count      BIGINT,
    
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (date, symbol, exchange)
);

-- ==============================================================================
-- INDEXES FOR PERFORMANCE
-- ==============================================================================

-- Main query patterns
CREATE INDEX IF NOT EXISTS idx_fdm_date ON futures_daily_metrics(date DESC);
CREATE INDEX IF NOT EXISTS idx_fdm_symbol ON futures_daily_metrics(symbol);
CREATE INDEX IF NOT EXISTS idx_fdm_exchange ON futures_daily_metrics(exchange);
CREATE INDEX IF NOT EXISTS idx_fdm_base_asset ON futures_daily_metrics(base_asset);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_fdm_exchange_date ON futures_daily_metrics(exchange, date DESC);
CREATE INDEX IF NOT EXISTS idx_fdm_symbol_date ON futures_daily_metrics(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_fdm_base_exchange_date ON futures_daily_metrics(base_asset, exchange, date DESC);

-- Partial indexes for filtering
CREATE INDEX IF NOT EXISTS idx_fdm_aggregated ON futures_daily_metrics(date DESC) 
    WHERE exchange = 'aggregated';

-- ==============================================================================
-- UPSERT FUNCTION
-- Handles duplicate detection and updates existing records
-- ==============================================================================
CREATE OR REPLACE FUNCTION upsert_futures_daily_metrics(
    p_date DATE,
    p_symbol VARCHAR(50),
    p_exchange VARCHAR(50),
    p_base_asset VARCHAR(20),
    p_oi_usd_open DECIMAL(24,4),
    p_oi_usd_high DECIMAL(24,4),
    p_oi_usd_low DECIMAL(24,4),
    p_oi_usd_close DECIMAL(24,4),
    p_funding_open DECIMAL(18,10),
    p_funding_high DECIMAL(18,10),
    p_funding_low DECIMAL(18,10),
    p_funding_close DECIMAL(18,10),
    p_pred_funding_open DECIMAL(18,10),
    p_pred_funding_high DECIMAL(18,10),
    p_pred_funding_low DECIMAL(18,10),
    p_pred_funding_close DECIMAL(18,10),
    p_ls_ratio DECIMAL(12,6),
    p_longs_qty DECIMAL(24,8),
    p_shorts_qty DECIMAL(24,8),
    p_ls_acc_global DECIMAL(12,6),
    p_ls_acc_top DECIMAL(12,6),
    p_ls_pos_top DECIMAL(12,6),
    p_liq_longs DECIMAL(24,4),
    p_liq_shorts DECIMAL(24,4),
    p_liq_total DECIMAL(24,4),
    p_price_open DECIMAL(24,8),
    p_price_high DECIMAL(24,8),
    p_price_low DECIMAL(24,8),
    p_price_close DECIMAL(24,8),
    p_volume_usd DECIMAL(24,4),
    p_volume_base DECIMAL(24,8),
    p_buy_volume_base DECIMAL(24,8),
    p_sell_volume_base DECIMAL(24,8),
    p_volume_delta DECIMAL(24,8),
    p_txn_count BIGINT,
    p_buy_txn_count BIGINT,
    p_sell_txn_count BIGINT
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO futures_daily_metrics (
        date, symbol, exchange, base_asset,
        oi_usd_open, oi_usd_high, oi_usd_low, oi_usd_close,
        funding_open, funding_high, funding_low, funding_close,
        pred_funding_open, pred_funding_high, pred_funding_low, pred_funding_close,
        ls_ratio, longs_qty, shorts_qty,
        ls_acc_global, ls_acc_top, ls_pos_top,
        liq_longs, liq_shorts, liq_total,
        price_open, price_high, price_low, price_close,
        volume_usd, volume_base, buy_volume_base, sell_volume_base, volume_delta,
        txn_count, buy_txn_count, sell_txn_count,
        updated_at
    ) VALUES (
        p_date, p_symbol, p_exchange, p_base_asset,
        p_oi_usd_open, p_oi_usd_high, p_oi_usd_low, p_oi_usd_close,
        p_funding_open, p_funding_high, p_funding_low, p_funding_close,
        p_pred_funding_open, p_pred_funding_high, p_pred_funding_low, p_pred_funding_close,
        p_ls_ratio, p_longs_qty, p_shorts_qty,
        p_ls_acc_global, p_ls_acc_top, p_ls_pos_top,
        p_liq_longs, p_liq_shorts, p_liq_total,
        p_price_open, p_price_high, p_price_low, p_price_close,
        p_volume_usd, p_volume_base, p_buy_volume_base, p_sell_volume_base, p_volume_delta,
        p_txn_count, p_buy_txn_count, p_sell_txn_count,
        NOW()
    )
    ON CONFLICT (date, symbol, exchange) DO UPDATE SET
        base_asset = COALESCE(EXCLUDED.base_asset, futures_daily_metrics.base_asset),
        oi_usd_open = COALESCE(EXCLUDED.oi_usd_open, futures_daily_metrics.oi_usd_open),
        oi_usd_high = COALESCE(EXCLUDED.oi_usd_high, futures_daily_metrics.oi_usd_high),
        oi_usd_low = COALESCE(EXCLUDED.oi_usd_low, futures_daily_metrics.oi_usd_low),
        oi_usd_close = COALESCE(EXCLUDED.oi_usd_close, futures_daily_metrics.oi_usd_close),
        funding_open = COALESCE(EXCLUDED.funding_open, futures_daily_metrics.funding_open),
        funding_high = COALESCE(EXCLUDED.funding_high, futures_daily_metrics.funding_high),
        funding_low = COALESCE(EXCLUDED.funding_low, futures_daily_metrics.funding_low),
        funding_close = COALESCE(EXCLUDED.funding_close, futures_daily_metrics.funding_close),
        pred_funding_open = COALESCE(EXCLUDED.pred_funding_open, futures_daily_metrics.pred_funding_open),
        pred_funding_high = COALESCE(EXCLUDED.pred_funding_high, futures_daily_metrics.pred_funding_high),
        pred_funding_low = COALESCE(EXCLUDED.pred_funding_low, futures_daily_metrics.pred_funding_low),
        pred_funding_close = COALESCE(EXCLUDED.pred_funding_close, futures_daily_metrics.pred_funding_close),
        ls_ratio = COALESCE(EXCLUDED.ls_ratio, futures_daily_metrics.ls_ratio),
        longs_qty = COALESCE(EXCLUDED.longs_qty, futures_daily_metrics.longs_qty),
        shorts_qty = COALESCE(EXCLUDED.shorts_qty, futures_daily_metrics.shorts_qty),
        ls_acc_global = COALESCE(EXCLUDED.ls_acc_global, futures_daily_metrics.ls_acc_global),
        ls_acc_top = COALESCE(EXCLUDED.ls_acc_top, futures_daily_metrics.ls_acc_top),
        ls_pos_top = COALESCE(EXCLUDED.ls_pos_top, futures_daily_metrics.ls_pos_top),
        liq_longs = COALESCE(EXCLUDED.liq_longs, futures_daily_metrics.liq_longs),
        liq_shorts = COALESCE(EXCLUDED.liq_shorts, futures_daily_metrics.liq_shorts),
        liq_total = COALESCE(EXCLUDED.liq_total, futures_daily_metrics.liq_total),
        price_open = COALESCE(EXCLUDED.price_open, futures_daily_metrics.price_open),
        price_high = COALESCE(EXCLUDED.price_high, futures_daily_metrics.price_high),
        price_low = COALESCE(EXCLUDED.price_low, futures_daily_metrics.price_low),
        price_close = COALESCE(EXCLUDED.price_close, futures_daily_metrics.price_close),
        volume_usd = COALESCE(EXCLUDED.volume_usd, futures_daily_metrics.volume_usd),
        volume_base = COALESCE(EXCLUDED.volume_base, futures_daily_metrics.volume_base),
        buy_volume_base = COALESCE(EXCLUDED.buy_volume_base, futures_daily_metrics.buy_volume_base),
        sell_volume_base = COALESCE(EXCLUDED.sell_volume_base, futures_daily_metrics.sell_volume_base),
        volume_delta = COALESCE(EXCLUDED.volume_delta, futures_daily_metrics.volume_delta),
        txn_count = COALESCE(EXCLUDED.txn_count, futures_daily_metrics.txn_count),
        buy_txn_count = COALESCE(EXCLUDED.buy_txn_count, futures_daily_metrics.buy_txn_count),
        sell_txn_count = COALESCE(EXCLUDED.sell_txn_count, futures_daily_metrics.sell_txn_count),
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- BULK UPSERT FUNCTION
-- For efficient batch inserts from Python using COPY
-- ==============================================================================
CREATE OR REPLACE FUNCTION bulk_upsert_futures_metrics()
RETURNS TRIGGER AS $$
BEGIN
    -- This trigger handles conflicts during bulk inserts
    -- Used with a staging table approach
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- STAGING TABLE FOR BULK LOADS
-- Temporary table for bulk CSV imports before upserting to main table
-- ==============================================================================
CREATE TABLE IF NOT EXISTS futures_daily_metrics_staging (
    LIKE futures_daily_metrics INCLUDING DEFAULTS
);

-- Function to move data from staging to main table with upsert
CREATE OR REPLACE FUNCTION process_staging_to_main()
RETURNS INTEGER AS $$
DECLARE
    rows_affected INTEGER;
BEGIN
    INSERT INTO futures_daily_metrics (
        date, symbol, exchange, base_asset,
        oi_usd_open, oi_usd_high, oi_usd_low, oi_usd_close,
        funding_open, funding_high, funding_low, funding_close,
        pred_funding_open, pred_funding_high, pred_funding_low, pred_funding_close,
        ls_ratio, longs_qty, shorts_qty,
        ls_acc_global, ls_acc_top, ls_pos_top,
        liq_longs, liq_shorts, liq_total,
        price_open, price_high, price_low, price_close,
        volume_usd, volume_base, buy_volume_base, sell_volume_base, volume_delta,
        txn_count, buy_txn_count, sell_txn_count,
        created_at, updated_at
    )
    SELECT 
        date, symbol, exchange, base_asset,
        oi_usd_open, oi_usd_high, oi_usd_low, oi_usd_close,
        funding_open, funding_high, funding_low, funding_close,
        pred_funding_open, pred_funding_high, pred_funding_low, pred_funding_close,
        ls_ratio, longs_qty, shorts_qty,
        ls_acc_global, ls_acc_top, ls_pos_top,
        liq_longs, liq_shorts, liq_total,
        price_open, price_high, price_low, price_close,
        volume_usd, volume_base, buy_volume_base, sell_volume_base, volume_delta,
        txn_count, buy_txn_count, sell_txn_count,
        NOW(), NOW()
    FROM futures_daily_metrics_staging
    ON CONFLICT (date, symbol, exchange) DO UPDATE SET
        base_asset = COALESCE(EXCLUDED.base_asset, futures_daily_metrics.base_asset),
        oi_usd_open = COALESCE(EXCLUDED.oi_usd_open, futures_daily_metrics.oi_usd_open),
        oi_usd_high = COALESCE(EXCLUDED.oi_usd_high, futures_daily_metrics.oi_usd_high),
        oi_usd_low = COALESCE(EXCLUDED.oi_usd_low, futures_daily_metrics.oi_usd_low),
        oi_usd_close = COALESCE(EXCLUDED.oi_usd_close, futures_daily_metrics.oi_usd_close),
        funding_open = COALESCE(EXCLUDED.funding_open, futures_daily_metrics.funding_open),
        funding_high = COALESCE(EXCLUDED.funding_high, futures_daily_metrics.funding_high),
        funding_low = COALESCE(EXCLUDED.funding_low, futures_daily_metrics.funding_low),
        funding_close = COALESCE(EXCLUDED.funding_close, futures_daily_metrics.funding_close),
        pred_funding_open = COALESCE(EXCLUDED.pred_funding_open, futures_daily_metrics.pred_funding_open),
        pred_funding_high = COALESCE(EXCLUDED.pred_funding_high, futures_daily_metrics.pred_funding_high),
        pred_funding_low = COALESCE(EXCLUDED.pred_funding_low, futures_daily_metrics.pred_funding_low),
        pred_funding_close = COALESCE(EXCLUDED.pred_funding_close, futures_daily_metrics.pred_funding_close),
        ls_ratio = COALESCE(EXCLUDED.ls_ratio, futures_daily_metrics.ls_ratio),
        longs_qty = COALESCE(EXCLUDED.longs_qty, futures_daily_metrics.longs_qty),
        shorts_qty = COALESCE(EXCLUDED.shorts_qty, futures_daily_metrics.shorts_qty),
        ls_acc_global = COALESCE(EXCLUDED.ls_acc_global, futures_daily_metrics.ls_acc_global),
        ls_acc_top = COALESCE(EXCLUDED.ls_acc_top, futures_daily_metrics.ls_acc_top),
        ls_pos_top = COALESCE(EXCLUDED.ls_pos_top, futures_daily_metrics.ls_pos_top),
        liq_longs = COALESCE(EXCLUDED.liq_longs, futures_daily_metrics.liq_longs),
        liq_shorts = COALESCE(EXCLUDED.liq_shorts, futures_daily_metrics.liq_shorts),
        liq_total = COALESCE(EXCLUDED.liq_total, futures_daily_metrics.liq_total),
        price_open = COALESCE(EXCLUDED.price_open, futures_daily_metrics.price_open),
        price_high = COALESCE(EXCLUDED.price_high, futures_daily_metrics.price_high),
        price_low = COALESCE(EXCLUDED.price_low, futures_daily_metrics.price_low),
        price_close = COALESCE(EXCLUDED.price_close, futures_daily_metrics.price_close),
        volume_usd = COALESCE(EXCLUDED.volume_usd, futures_daily_metrics.volume_usd),
        volume_base = COALESCE(EXCLUDED.volume_base, futures_daily_metrics.volume_base),
        buy_volume_base = COALESCE(EXCLUDED.buy_volume_base, futures_daily_metrics.buy_volume_base),
        sell_volume_base = COALESCE(EXCLUDED.sell_volume_base, futures_daily_metrics.sell_volume_base),
        volume_delta = COALESCE(EXCLUDED.volume_delta, futures_daily_metrics.volume_delta),
        txn_count = COALESCE(EXCLUDED.txn_count, futures_daily_metrics.txn_count),
        buy_txn_count = COALESCE(EXCLUDED.buy_txn_count, futures_daily_metrics.buy_txn_count),
        sell_txn_count = COALESCE(EXCLUDED.sell_txn_count, futures_daily_metrics.sell_txn_count),
        updated_at = NOW();
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    
    -- Clear staging table
    TRUNCATE futures_daily_metrics_staging;
    
    RETURN rows_affected;
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- UPDATED_AT TRIGGER
-- Automatically updates the updated_at timestamp on row modification
-- ==============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_timestamp
    BEFORE UPDATE ON futures_daily_metrics
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_update_timestamp_exchanges
    BEFORE UPDATE ON exchanges
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_update_timestamp_symbols
    BEFORE UPDATE ON symbols
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ==============================================================================
-- USEFUL VIEWS
-- ==============================================================================

-- View: Latest data per symbol
CREATE OR REPLACE VIEW v_latest_metrics AS
SELECT DISTINCT ON (symbol, exchange)
    date,
    symbol,
    exchange,
    base_asset,
    oi_usd_close,
    funding_close,
    ls_ratio,
    liq_total,
    price_close,
    volume_usd
FROM futures_daily_metrics
ORDER BY symbol, exchange, date DESC;

-- View: Daily summary across all exchanges
CREATE OR REPLACE VIEW v_daily_summary AS
SELECT 
    date,
    COUNT(DISTINCT base_asset) as asset_count,
    COUNT(DISTINCT exchange) as exchange_count,
    SUM(oi_usd_close) as total_oi_usd,
    AVG(funding_close) as avg_funding_rate,
    SUM(liq_total) as total_liquidations,
    SUM(volume_usd) as total_volume
FROM futures_daily_metrics
GROUP BY date
ORDER BY date DESC;

-- ==============================================================================
-- MATERIALIZED VIEWS FOR AGGREGATED DATA
-- These provide pre-computed aggregated metrics across all exchanges
-- ==============================================================================

-- Aggregated metrics per asset (sum across all exchanges)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_aggregated_by_asset AS
SELECT 
    date,
    base_asset,
    
    -- Aggregated Open Interest (sum across exchanges)
    SUM(oi_usd_close) as total_oi_usd,
    SUM(oi_usd_high) as max_oi_usd,
    SUM(oi_usd_low) as min_oi_usd,
    
    -- Weighted Average Funding Rate (by OI)
    CASE 
        WHEN SUM(oi_usd_close) > 0 
        THEN SUM(funding_close * oi_usd_close) / SUM(oi_usd_close)
        ELSE NULL 
    END as weighted_avg_funding,
    
    -- Simple Average Funding Rate
    AVG(funding_close) as avg_funding,
    
    -- Predicted Funding (weighted average)
    CASE 
        WHEN SUM(oi_usd_close) > 0 
        THEN SUM(pred_funding_close * oi_usd_close) / SUM(oi_usd_close)
        ELSE NULL 
    END as weighted_pred_funding,
    
    -- Aggregated Long/Short (sum quantities, recalculate ratio)
    SUM(longs_qty) as total_longs,
    SUM(shorts_qty) as total_shorts,
    CASE 
        WHEN SUM(shorts_qty) > 0 
        THEN SUM(longs_qty) / SUM(shorts_qty)
        ELSE NULL 
    END as aggregated_ls_ratio,
    
    -- Aggregated Liquidations
    SUM(liq_longs) as total_liq_longs,
    SUM(liq_shorts) as total_liq_shorts,
    SUM(liq_total) as total_liquidations,
    
    -- Price (average across exchanges)
    AVG(price_close) as avg_price,
    MAX(price_high) as max_price,
    MIN(price_low) as min_price,
    
    -- Aggregated Volume
    SUM(volume_usd) as total_volume_usd,
    SUM(volume_base) as total_volume_base,
    SUM(buy_volume_base) as total_buy_volume_base,
    SUM(sell_volume_base) as total_sell_volume_base,
    SUM(volume_delta) as total_volume_delta,
    SUM(txn_count) as total_transactions,
    SUM(buy_txn_count) as total_buy_transactions,
    
    -- Exchange coverage
    COUNT(DISTINCT exchange) as exchange_count,
    ARRAY_AGG(DISTINCT exchange) as exchanges,
    
    -- Metadata
    NOW() as refreshed_at
    
FROM futures_daily_metrics
GROUP BY date, base_asset;

-- Index for fast queries on materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_agg_date_asset 
ON mv_aggregated_by_asset(date DESC, base_asset);

CREATE INDEX IF NOT EXISTS idx_mv_agg_asset 
ON mv_aggregated_by_asset(base_asset);

-- ==============================================================================
-- CALCULATED TRADING METRICS VIEW
-- Derived metrics useful for quantitative analysis
-- ==============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_trading_metrics AS
SELECT 
    f.date,
    f.symbol,
    f.exchange,
    f.base_asset,
    f.price_close,
    f.oi_usd_close,
    f.oi_usd_high,
    f.oi_usd_low,
    f.funding_close,
    f.volume_usd as volume_usd_est,
    f.volume_base,
    f.buy_volume_base,
    f.sell_volume_base,
    f.volume_delta,
    f.liq_longs,
    f.liq_shorts,
    f.liq_total,
    f.ls_ratio,
    
    -- OI Change (daily) - requires LAG
    f.oi_usd_close - LAG(f.oi_usd_close) OVER (
        PARTITION BY f.symbol, f.exchange ORDER BY f.date
    ) as oi_change_usd,
    
    -- OI Change % 
    CASE 
        WHEN LAG(f.oi_usd_close) OVER (PARTITION BY f.symbol, f.exchange ORDER BY f.date) > 0
        THEN (f.oi_usd_close - LAG(f.oi_usd_close) OVER (
            PARTITION BY f.symbol, f.exchange ORDER BY f.date
        )) / LAG(f.oi_usd_close) OVER (PARTITION BY f.symbol, f.exchange ORDER BY f.date) * 100
        ELSE NULL
    END as oi_change_pct,
    
    -- Price Change %
    CASE 
        WHEN LAG(f.price_close) OVER (PARTITION BY f.symbol, f.exchange ORDER BY f.date) > 0
        THEN (f.price_close - LAG(f.price_close) OVER (
            PARTITION BY f.symbol, f.exchange ORDER BY f.date
        )) / LAG(f.price_close) OVER (PARTITION BY f.symbol, f.exchange ORDER BY f.date) * 100
        ELSE NULL
    END as price_change_pct,
    
    -- Funding Rate Annualized (assuming 3 funding periods per day)
    f.funding_close * 3 * 365 as funding_annualized,
    
    -- Funding Premium/Discount (predicted vs actual)
    f.pred_funding_close - f.funding_close as funding_spread,
    
    -- Volume/OI Ratio (turnover indicator)
    CASE 
        WHEN f.oi_usd_close > 0 
        THEN f.volume_usd / f.oi_usd_close 
        ELSE NULL 
    END as volume_oi_ratio,
    
    -- Liquidation Intensity (liquidations as % of OI)
    CASE 
        WHEN f.oi_usd_close > 0 
        THEN f.liq_total / f.oi_usd_close * 100 
        ELSE NULL 
    END as liq_intensity_pct,
    
    -- Liquidation Bias (positive = more longs liquidated)
    CASE 
        WHEN f.liq_total > 0 
        THEN (f.liq_longs - f.liq_shorts) / f.liq_total * 100 
        ELSE NULL 
    END as liq_bias_pct,
    
    -- Volume Delta % (relative to total volume)
    CASE 
        WHEN f.volume_base > 0 
        THEN (f.volume_delta / f.volume_base) * 100
        ELSE NULL
    END as delta_pct,
    
    -- Buy/Sell Ratio
    CASE 
        WHEN f.sell_volume_base > 0 
        THEN f.buy_volume_base / f.sell_volume_base
        ELSE NULL
    END as buy_sell_ratio,
    
    -- Long/Short Imbalance (deviation from 1.0)
    f.ls_ratio - 1.0 as ls_imbalance,
    
    -- Daily Range (volatility indicator)
    f.price_high - f.price_low as price_range,
    
    -- Range % of price
    CASE 
        WHEN f.price_close > 0 
        THEN (f.price_high - f.price_low) / f.price_close * 100 
        ELSE NULL 
    END as range_pct,
    
    NOW() as refreshed_at
    
FROM futures_daily_metrics f;

-- Indexes for trading metrics view
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_trading_date_sym_ex 
ON mv_trading_metrics(date DESC, symbol, exchange);

CREATE INDEX IF NOT EXISTS idx_mv_trading_asset 
ON mv_trading_metrics(base_asset, date DESC);

-- ==============================================================================
-- GLOBAL MARKET OVERVIEW
-- Summary metrics for the entire futures market
-- ==============================================================================
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_global_market AS
SELECT 
    date,
    
    -- Total market size
    SUM(oi_usd_close) as global_oi_usd,
    SUM(volume_usd) as global_volume_usd,
    SUM(liq_total) as global_liquidations,
    
    -- Market composition
    COUNT(DISTINCT base_asset) as unique_assets,
    COUNT(DISTINCT exchange) as unique_exchanges,
    COUNT(*) as total_markets,
    
    -- Average metrics
    AVG(funding_close) as avg_funding_rate,
    AVG(ls_ratio) as avg_ls_ratio,
    
    -- Market leaders (by OI)
    (SELECT base_asset FROM futures_daily_metrics f2 
     WHERE f2.date = futures_daily_metrics.date 
     GROUP BY base_asset ORDER BY SUM(oi_usd_close) DESC LIMIT 1) as top_asset_by_oi,
    
    -- Liquidation summary
    SUM(liq_longs) as global_liq_longs,
    SUM(liq_shorts) as global_liq_shorts,
    CASE 
        WHEN SUM(liq_total) > 0 
        THEN (SUM(liq_longs) - SUM(liq_shorts)) / SUM(liq_total) * 100 
        ELSE NULL 
    END as global_liq_bias_pct,
    
    NOW() as refreshed_at
    
FROM futures_daily_metrics
GROUP BY date;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_global_date 
ON mv_global_market(date DESC);

-- ==============================================================================
-- REFRESH FUNCTIONS FOR MATERIALIZED VIEWS
-- ==============================================================================

-- Refresh all materialized views
CREATE OR REPLACE FUNCTION refresh_all_materialized_views()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_aggregated_by_asset;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_trading_metrics;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_global_market;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-refresh after staging import
CREATE OR REPLACE FUNCTION trigger_refresh_views_after_import()
RETURNS TRIGGER AS $$
BEGIN
    -- Only refresh if significant data was added
    IF (TG_OP = 'TRUNCATE') THEN
        PERFORM refresh_all_materialized_views();
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Schedule refresh (call this after each data load)
COMMENT ON FUNCTION refresh_all_materialized_views() IS 
'Call this function after loading new data to update all materialized views. 
Can be scheduled via pg_cron or called from Python after data import.';

-- ==============================================================================
-- ROW LEVEL SECURITY (RLS) - Supabase
-- Enable RLS for production security
-- ==============================================================================
ALTER TABLE futures_daily_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE exchanges ENABLE ROW LEVEL SECURITY;
ALTER TABLE symbols ENABLE ROW LEVEL SECURITY;

-- Policy: Allow read access to all authenticated users
CREATE POLICY "Allow read access to authenticated users"
ON futures_daily_metrics FOR SELECT
TO authenticated
USING (true);

-- Policy: Allow insert/update for service role only
CREATE POLICY "Allow write access to service role"
ON futures_daily_metrics FOR ALL
TO service_role
USING (true)
WITH CHECK (true);

-- Similar policies for other tables
CREATE POLICY "Allow read exchanges"
ON exchanges FOR SELECT TO authenticated USING (true);

CREATE POLICY "Allow read symbols"
ON symbols FOR SELECT TO authenticated USING (true);

-- ==============================================================================
-- UTILITY FUNCTIONS
-- ==============================================================================

-- Get date range for a symbol
CREATE OR REPLACE FUNCTION get_symbol_date_range(p_symbol VARCHAR(50), p_exchange VARCHAR(50))
RETURNS TABLE(first_date DATE, last_date DATE, row_count BIGINT) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        MIN(date)::DATE as first_date,
        MAX(date)::DATE as last_date,
        COUNT(*)::BIGINT as row_count
    FROM futures_daily_metrics
    WHERE symbol = p_symbol AND exchange = p_exchange;
END;
$$ LANGUAGE plpgsql;

-- Get last date for incremental fetching
CREATE OR REPLACE FUNCTION get_last_date(p_exchange VARCHAR(50) DEFAULT NULL)
RETURNS DATE AS $$
DECLARE
    result DATE;
BEGIN
    IF p_exchange IS NULL THEN
        SELECT MAX(date) INTO result FROM futures_daily_metrics;
    ELSE
        SELECT MAX(date) INTO result FROM futures_daily_metrics WHERE exchange = p_exchange;
    END IF;
    RETURN result;
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- COMMENTS
-- ==============================================================================
COMMENT ON TABLE futures_daily_metrics IS 'Daily futures/perpetual contract metrics from Coinalyze API';
COMMENT ON TABLE exchanges IS 'Cryptocurrency exchanges supported by the system';
COMMENT ON TABLE symbols IS 'Trading pair symbols with metadata';
COMMENT ON COLUMN futures_daily_metrics.funding_close IS 'Funding rate as decimal (0.0001 = 0.01%)';
COMMENT ON COLUMN futures_daily_metrics.ls_ratio IS 'Long/Short ratio (longs/shorts)';
COMMENT ON COLUMN futures_daily_metrics.oi_usd_close IS 'Open Interest in USD at daily close';

-- ==============================================================================
-- UPSERT FOR ASSET METADATA
-- ==============================================================================
CREATE OR REPLACE FUNCTION upsert_asset_metadata(
    p_symbol VARCHAR(20),
    p_narrative VARCHAR(100),
    p_is_filtered BOOLEAN
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO asset_metadata (symbol, narrative, is_filtered, updated_at)
    VALUES (p_symbol, p_narrative, p_is_filtered, NOW())
    ON CONFLICT (symbol) DO UPDATE SET
        narrative = EXCLUDED.narrative,
        is_filtered = EXCLUDED.is_filtered,
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- UPSERT FOR SPOT OHLCV
-- ==============================================================================
CREATE OR REPLACE FUNCTION upsert_spot_daily_ohlcv(
    p_date DATE,
    p_symbol VARCHAR(50),
    p_exchange VARCHAR(50),
    p_price_open DECIMAL(24,8),
    p_price_high DECIMAL(24,8),
    p_price_low DECIMAL(24,8),
    p_price_close DECIMAL(24,8),
    p_volume_base DECIMAL(24,8),
    p_volume_usd DECIMAL(24,4),
    p_buy_volume_base DECIMAL(24,8),
    p_sell_volume_base DECIMAL(24,8),
    p_volume_delta DECIMAL(24,8),
    p_txn_count BIGINT,
    p_buy_txn_count BIGINT,
    p_sell_txn_count BIGINT
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO spot_daily_ohlcv (
        date, symbol, exchange,
        price_open, price_high, price_low, price_close,
        volume_base, volume_usd,
        buy_volume_base, sell_volume_base, volume_delta,
        txn_count, buy_txn_count, sell_txn_count,
        updated_at
    ) VALUES (
        p_date, p_symbol, p_exchange,
        p_price_open, p_price_high, p_price_low, p_price_close,
        p_volume_base, p_volume_usd,
        p_buy_volume_base, p_sell_volume_base, p_volume_delta,
        p_txn_count, p_buy_txn_count, p_sell_txn_count,
        NOW()
    )
    ON CONFLICT (date, symbol, exchange) DO UPDATE SET
        price_open = EXCLUDED.price_open,
        price_high = EXCLUDED.price_high,
        price_low = EXCLUDED.price_low,
        price_close = EXCLUDED.price_close,
        volume_base = EXCLUDED.volume_base,
        volume_usd = EXCLUDED.volume_usd,
        buy_volume_base = EXCLUDED.buy_volume_base,
        sell_volume_base = EXCLUDED.sell_volume_base,
        volume_delta = EXCLUDED.volume_delta,
        txn_count = EXCLUDED.txn_count,
        buy_txn_count = EXCLUDED.buy_txn_count,
        sell_txn_count = EXCLUDED.sell_txn_count,
        updated_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- ==============================================================================
-- TRIGGER FOR ASSET METADATA
-- ==============================================================================
CREATE TRIGGER trigger_update_timestamp_metadata
    BEFORE UPDATE ON asset_metadata
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trigger_update_timestamp_spot
    BEFORE UPDATE ON spot_daily_ohlcv
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

ALTER TABLE asset_metadata ENABLE ROW LEVEL SECURITY;
ALTER TABLE spot_daily_ohlcv ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Allow read metadata" ON asset_metadata FOR SELECT TO authenticated USING (true);
CREATE POLICY "Allow read spot" ON spot_daily_ohlcv FOR SELECT TO authenticated USING (true);
