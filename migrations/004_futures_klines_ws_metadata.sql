-- Migration: operational provenance for 15m futures klines.
-- The canonical candle identity remains (candle_open_at, symbol, exchange).

ALTER TABLE futures_klines_15m
    ADD COLUMN IF NOT EXISTS source VARCHAR(32),
    ADD COLUMN IF NOT EXISTS ws_received_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS rest_reconciled_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS exchange_event_time TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_fk15_source_at
    ON futures_klines_15m(source, candle_open_at DESC);
