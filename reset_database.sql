-- ==============================================================================
-- DATABASE RESET SCRIPT
-- WARNING: THIS WILL DELETE ALL DATA IN THE ALTS-SCRAPER TABLES
-- ==============================================================================

-- 1. Drop Materialized Views (CASCADE handles dependency order)
DROP MATERIALIZED VIEW IF EXISTS mv_global_market CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_trading_metrics CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_aggregated_by_asset CASCADE;

-- 2. Drop Regular Views
DROP VIEW IF EXISTS v_daily_summary CASCADE;
DROP VIEW IF EXISTS v_latest_metrics CASCADE;

-- 3. Drop Tables (CASCADE drops default values, constraints, and triggers)
DROP TABLE IF EXISTS futures_daily_metrics_staging CASCADE;
DROP TABLE IF EXISTS futures_daily_metrics CASCADE;
DROP TABLE IF EXISTS spot_daily_ohlcv CASCADE;
DROP TABLE IF EXISTS asset_metadata CASCADE;
DROP TABLE IF EXISTS symbols CASCADE;
DROP TABLE IF EXISTS exchanges CASCADE;

-- 4. Drop Functions (Dynamic loop to handle overloaded signatures)
DO $$ 
DECLARE 
    r RECORD;
BEGIN 
    -- Iterate over all functions with these names and drop them by correct signature
    FOR r IN (
        SELECT oid::regprocedure as func_signature 
        FROM pg_proc 
        WHERE proname IN (
            'refresh_all_materialized_views', 
            'trigger_refresh_views_after_import', 
            'update_updated_at_column', 
            'process_staging_to_main', 
            'bulk_upsert_futures_metrics', 
            'upsert_futures_daily_metrics', 
            'upsert_spot_daily_ohlcv'
        ) 
        AND pg_function_is_visible(oid) -- Only drop visible functions (in current search path)
    ) LOOP 
        EXECUTE 'DROP FUNCTION IF EXISTS ' || r.func_signature || ' CASCADE'; 
        RAISE NOTICE 'Dropped function: %', r.func_signature;
    END LOOP; 
END $$;

-- 5. Drop Extensions (optional, usually better to keep)
-- DROP EXTENSION IF EXISTS "uuid-ossp";

DO $$
BEGIN
    RAISE NOTICE 'Database reset complete. All Alts-scraper objects have been dropped.';
END $$;
