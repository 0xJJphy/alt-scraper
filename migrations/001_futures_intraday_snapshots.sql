-- Migration: 48h operational futures intraday snapshots for GLI Paper Live.
-- Apply on the alt-scraper database (DATABASE_URL used by realtime_daemon.py).

CREATE TABLE IF NOT EXISTS futures_intraday_snapshots (
    snapshot_at     TIMESTAMPTZ  NOT NULL,
    symbol          VARCHAR(50)  NOT NULL,
    exchange        VARCHAR(50)  NOT NULL,
    base_asset      VARCHAR(20),
    oi_usd          DECIMAL(24, 4),
    funding         DECIMAL(18, 10),
    pred_funding    DECIMAL(18, 10),
    ls_acc_global   DECIMAL(12, 6),
    ls_acc_top      DECIMAL(12, 6),
    ls_pos_top      DECIMAL(12, 6),
    price           DECIMAL(24, 8),
    polled_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_at, symbol, exchange)
);

CREATE INDEX IF NOT EXISTS idx_fis_base_at
    ON futures_intraday_snapshots(base_asset, snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_fis_symbol_at
    ON futures_intraday_snapshots(symbol, exchange, snapshot_at DESC);
CREATE INDEX IF NOT EXISTS idx_fis_at
    ON futures_intraday_snapshots(snapshot_at DESC);

CREATE OR REPLACE FUNCTION purge_old_snapshots() RETURNS void AS $$
    DELETE FROM futures_snapshots WHERE snapshot_at < NOW() - INTERVAL '90 days';
    DELETE FROM futures_intraday_snapshots WHERE snapshot_at < NOW() - INTERVAL '48 hours';
$$ LANGUAGE sql;
