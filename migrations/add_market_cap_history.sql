-- ==============================================================================
-- Migration: add_market_cap_history
-- Adds daily market cap ranking snapshots to enable survivor-bias-free backtests.
--
-- New table: market_cap_history
--   Stores a daily snapshot of every tracked asset's CoinGecko rank.
--   in_top_50   → was this asset in the top 50 on this specific date?
--   ever_in_top_50 → has this asset EVER been in the top 50 across all recorded dates?
--
-- Modified table: asset_metadata
--   ever_in_top_50 → denormalised flag for fast "load all tracked" queries in daemons.
-- ==============================================================================

-- 1. New column on asset_metadata (safe, idempotent)
ALTER TABLE asset_metadata
    ADD COLUMN IF NOT EXISTS ever_in_top_50 BOOLEAN DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_am_ever_top50
    ON asset_metadata(ever_in_top_50)
    WHERE ever_in_top_50 = true;

-- 2. New table: market_cap_history
CREATE TABLE IF NOT EXISTS market_cap_history (
    date            DATE        NOT NULL,
    symbol          VARCHAR(20) NOT NULL,
    market_cap_rank INTEGER,                        -- CoinGecko global rank on this date
    market_cap_usd  DECIMAL(30, 4),                 -- Market cap in USD on this date
    in_top_50       BOOLEAN     NOT NULL DEFAULT false,  -- Was in top 50 on this date?
    ever_in_top_50  BOOLEAN     NOT NULL DEFAULT false,  -- Has ever been in top 50?
    source          VARCHAR(20) NOT NULL DEFAULT 'coingecko',
    created_at      TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (date, symbol)
);

CREATE INDEX IF NOT EXISTS idx_mch_date
    ON market_cap_history(date DESC);

CREATE INDEX IF NOT EXISTS idx_mch_symbol
    ON market_cap_history(symbol, date DESC);

-- Fast lookup: "all assets that were in top 50 on date X" → used by backtest
CREATE INDEX IF NOT EXISTS idx_mch_date_top50
    ON market_cap_history(date, in_top_50)
    WHERE in_top_50 = true;

-- Fast lookup: "all assets ever tracked" → used by daemons to load full universe
CREATE INDEX IF NOT EXISTS idx_mch_ever_top50
    ON market_cap_history(ever_in_top_50)
    WHERE ever_in_top_50 = true;

-- 3. SQL function: get_universe_at_date
--    Returns the active top-N universe for a given date (for backtest queries).
--    Only includes assets that were in_top_50 on that exact date.
CREATE OR REPLACE FUNCTION get_universe_at_date(
    p_date      DATE,
    p_top_n     INTEGER DEFAULT 50,
    p_exclude_filtered BOOLEAN DEFAULT true
)
RETURNS TABLE(
    symbol          VARCHAR,
    market_cap_rank INTEGER,
    market_cap_usd  DECIMAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        m.symbol,
        m.market_cap_rank,
        m.market_cap_usd
    FROM market_cap_history m
    LEFT JOIN asset_metadata am ON am.symbol = m.symbol
    WHERE m.date = p_date
      AND m.in_top_50 = true
      AND m.market_cap_rank <= p_top_n
      AND (NOT p_exclude_filtered OR COALESCE(am.is_filtered, false) = false)
    ORDER BY m.market_cap_rank ASC;
END;
$$ LANGUAGE plpgsql STABLE;

-- 4. SQL function: get_full_tracked_universe
--    Returns all assets that have EVER been in the top 50.
--    Used by daemons to determine which assets to poll (full universe, no restriction).
CREATE OR REPLACE FUNCTION get_full_tracked_universe()
RETURNS TABLE(
    symbol          VARCHAR,
    market_cap_rank INTEGER,
    ever_in_top_50  BOOLEAN
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        am.symbol,
        am.market_cap_rank,
        am.ever_in_top_50
    FROM asset_metadata am
    WHERE (am.is_filtered = false OR am.is_filtered IS NULL)
      AND am.ever_in_top_50 = true
    ORDER BY am.market_cap_rank ASC NULLS LAST;
END;
$$ LANGUAGE plpgsql STABLE;

-- 5. Backfill ever_in_top_50 on asset_metadata from any existing market_cap_history rows
--    (safe no-op if table is empty)
UPDATE asset_metadata am
SET ever_in_top_50 = true
WHERE am.symbol IN (
    SELECT DISTINCT symbol FROM market_cap_history WHERE ever_in_top_50 = true
);
