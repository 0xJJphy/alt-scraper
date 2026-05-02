-- migrate_market_type.sql
-- Adds market_type column to orderbook tables and updates PKs.
-- Safe to re-run: uses IF NOT EXISTS / IF EXISTS.
-- All existing rows get market_type='futures' via DEFAULT.

BEGIN;

-- ─── orderbook_snapshots ────────────────────────────────────────────────────
ALTER TABLE orderbook_snapshots
    ADD COLUMN IF NOT EXISTS market_type VARCHAR(10) NOT NULL DEFAULT 'futures';

ALTER TABLE orderbook_snapshots
    DROP CONSTRAINT IF EXISTS orderbook_snapshots_pkey;

ALTER TABLE orderbook_snapshots
    ADD CONSTRAINT orderbook_snapshots_pkey
    PRIMARY KEY (snapshot_at, symbol, exchange, market_type);

-- ─── orderbook_daily_metrics ────────────────────────────────────────────────
ALTER TABLE orderbook_daily_metrics
    ADD COLUMN IF NOT EXISTS market_type VARCHAR(10) NOT NULL DEFAULT 'futures';

ALTER TABLE orderbook_daily_metrics
    DROP CONSTRAINT IF EXISTS orderbook_daily_metrics_pkey;

ALTER TABLE orderbook_daily_metrics
    ADD CONSTRAINT orderbook_daily_metrics_pkey
    PRIMARY KEY (date, symbol, exchange, market_type);

-- ─── orderbook_latest ───────────────────────────────────────────────────────
ALTER TABLE orderbook_latest
    ADD COLUMN IF NOT EXISTS market_type VARCHAR(10) NOT NULL DEFAULT 'futures';

ALTER TABLE orderbook_latest
    DROP CONSTRAINT IF EXISTS orderbook_latest_pkey;

ALTER TABLE orderbook_latest
    ADD CONSTRAINT orderbook_latest_pkey
    PRIMARY KEY (symbol, exchange, market_type);

-- ─── Index for cross-exchange/market comparison queries ─────────────────────
CREATE INDEX IF NOT EXISTS idx_obs_base_market
    ON orderbook_snapshots (base_asset, market_type, snapshot_at DESC);

-- ─── Permissions ────────────────────────────────────────────────────────────
GRANT SELECT, INSERT, UPDATE ON orderbook_snapshots     TO gli_user;
GRANT SELECT, INSERT, UPDATE ON orderbook_daily_metrics TO gli_user;
GRANT SELECT, INSERT, UPDATE ON orderbook_latest        TO gli_user;

COMMIT;
