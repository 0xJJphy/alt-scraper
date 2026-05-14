-- Migration: Normalize legacy base_asset names to current canonical tickers.
--
-- MATIC was renamed to POL by the Polygon Foundation (Sep 2023).
-- RNDR was renamed to RENDER by the Render Network (Nov 2023).
--
-- Bybit and OKX updated their contract tickers accordingly, so intraday/klines
-- tables already store POL and RENDER. This migration aligns the daily metrics
-- and orderbook tables so all tables share the same base_asset values.
--
-- Safe to re-run: WHERE clauses only match old names.

BEGIN;

UPDATE futures_daily_metrics
SET base_asset = 'POL'
WHERE base_asset = 'MATIC';

UPDATE futures_daily_metrics
SET base_asset = 'RENDER'
WHERE base_asset = 'RNDR';

UPDATE orderbook_daily_metrics
SET base_asset = 'POL'
WHERE base_asset = 'MATIC';

UPDATE orderbook_daily_metrics
SET base_asset = 'RENDER'
WHERE base_asset = 'RNDR';

COMMIT;
