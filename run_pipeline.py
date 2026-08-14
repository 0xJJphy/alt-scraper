import os
import subprocess
import sys
import psycopg2
from dotenv import load_dotenv


def run_command(command, label=""):
    tag = f"[{label}] " if label else ""
    print(f"{tag}Executing: {' '.join(command)}", flush=True)
    result = subprocess.run(command, capture_output=False, text=True)
    if result.returncode != 0:
        print(f"{tag}!! Command failed with return code {result.returncode}", flush=True)
    return result.returncode


def main():
    load_dotenv()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    spot_script = os.path.join(base_dir, "spot_scraper.py")
    alts_script = os.path.join(base_dir, "alt_scraper.py")

    top_n = os.getenv("TOP_N", "50")
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        print("DATABASE_URL not found in environment. Exiting.", flush=True)
        sys.exit(1)

    print("--- Starting Alts Scraper Pipeline ---", flush=True)

    failed_steps = []

    # 1. Sync Metadata (must run first — spot and futures depend on it)
    print("\n[1/4] Syncing Global Metadata...", flush=True)
    if run_command([sys.executable, "-u", spot_script, "--limit", top_n, "--metadata-only"]) != 0:
        failed_steps.append("Sync Metadata")

    # 2+3. Run Spot and Futures scrapers SEQUENTIALLY.
    #
    # Son independientes entre sí, pero NO en la cuota de Coinalyze: el límite es
    # de 40 req/min por cuenta, y COINALYZE_API_KEY_SPOT es de la misma cuenta que
    # COINALYZE_API_KEY. En paralelo, spot (1,6 s = 37,5 req/min) y futures (3,2 s
    # = 18,75 req/min) suman 56 req/min y saturan la ventana. El futures acaba
    # perdiendo: cuando un hilo de exchange se topa un 429 duerme 60 s, pero spot
    # sigue disparando, así que al despertar la ventana sigue llena, se come el
    # segundo 429 y aborta ese exchange entero. Medido en el VPS del 11 al 14 de
    # agosto de 2026: dos de los tres exchanges morían CADA día, y sobrevivía solo
    # el que quedaba con la ventana para él.
    #
    # Cada scraper por separado cabe de sobra en los 40/min. Secuencial cuesta
    # ~25 min más de reloj; el timer arranca a las 00:15 UTC y el pipeline de GLI
    # no lee esta base hasta las ~04:00, así que el margen sobra.
    print("\n[2/4] Running Spot scraper...", flush=True)
    if run_command([sys.executable, "-u", spot_script, "--limit", top_n], label="SPOT") != 0:
        failed_steps.append("Spot Scraper")

    print("\n[3/4] Running Futures scraper...", flush=True)
    if run_command([sys.executable, "-u", alts_script, "--limit", top_n,
                    "--exchanges", "binance,bybit,okx"], label="FUTURES") != 0:
        failed_steps.append("Futures Scraper")

    # 4. Compute daily L/S high/low from today's snapshots → futures_daily_metrics
    print("\n[4/5] Computing L/S daily high/low from snapshots...", flush=True)
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("""
            UPDATE futures_daily_metrics fdm
            SET
                ls_acc_global_high = s.ls_global_high,
                ls_acc_global_low  = s.ls_global_low,
                ls_acc_top_high    = s.ls_top_high,
                ls_acc_top_low     = s.ls_top_low,
                ls_pos_top_high    = s.ls_pos_high,
                ls_pos_top_low     = s.ls_pos_low
            FROM (
                SELECT
                    snapshot_at::date AS day,
                    symbol, exchange,
                    MAX(ls_acc_global) AS ls_global_high, MIN(ls_acc_global) AS ls_global_low,
                    MAX(ls_acc_top)    AS ls_top_high,    MIN(ls_acc_top)    AS ls_top_low,
                    MAX(ls_pos_top)    AS ls_pos_high,    MIN(ls_pos_top)    AS ls_pos_low
                FROM futures_snapshots
                WHERE snapshot_at >= CURRENT_DATE - 1 AND snapshot_at < CURRENT_DATE
                GROUP BY 1, 2, 3
            ) s
            WHERE fdm.date = s.day AND fdm.symbol = s.symbol AND fdm.exchange = s.exchange
        """)
        rows_updated = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        print(f"  [DB] Updated L/S high/low for {rows_updated} rows.", flush=True)
    except Exception as e:
        print(f"  [DB ERROR] L/S high/low update failed: {e}", flush=True)
        failed_steps.append("L/S High/Low")

    # 4b. Aggregate yesterday's orderbook snapshots → orderbook_daily_metrics
    print("\n[4b/5] Aggregating yesterday's orderbook snapshots into daily metrics...", flush=True)
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO orderbook_daily_metrics (
                date, symbol, exchange, market_type, base_asset,
                spread_bps_open, spread_bps_high, spread_bps_low, spread_bps_close,
                bid_qty_1pct_close,     ask_qty_1pct_close,
                bid_qty_2_5pct_close,   ask_qty_2_5pct_close,
                bid_qty_5pct_close,     ask_qty_5pct_close,
                bid_qty_10pct_close,    ask_qty_10pct_close,
                imbalance_1pct_high,    imbalance_1pct_low,
                imbalance_2_5pct_high,  imbalance_2_5pct_low,
                imbalance_5pct_high,    imbalance_5pct_low,
                imbalance_10pct_high,   imbalance_10pct_low,
                avg_depth_coverage_pct, snapshot_count
            )
            SELECT
                DATE(snapshot_at AT TIME ZONE 'UTC'),
                symbol, exchange, market_type, MAX(base_asset),
                MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 0  THEN spread_bps END),
                MAX(spread_bps),
                MIN(spread_bps),
                MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN spread_bps END),
                MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN bid_qty_1pct  END),
                MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN ask_qty_1pct  END),
                MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN bid_qty_2_5pct END),
                MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN ask_qty_2_5pct END),
                MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN bid_qty_5pct  END),
                MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN ask_qty_5pct  END),
                MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN bid_qty_10pct END),
                MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN ask_qty_10pct END),
                MAX(imbalance_1pct),   MIN(imbalance_1pct),
                MAX(imbalance_2_5pct), MIN(imbalance_2_5pct),
                MAX(imbalance_5pct),   MIN(imbalance_5pct),
                MAX(imbalance_10pct),  MIN(imbalance_10pct),
                AVG(depth_coverage_pct), COUNT(*)
            FROM orderbook_snapshots
            WHERE snapshot_at >= CURRENT_DATE - 1
              AND snapshot_at <  CURRENT_DATE
            GROUP BY DATE(snapshot_at AT TIME ZONE 'UTC'), symbol, exchange, market_type
            ON CONFLICT (date, symbol, exchange, market_type) DO UPDATE SET
                spread_bps_open         = EXCLUDED.spread_bps_open,
                spread_bps_high         = EXCLUDED.spread_bps_high,
                spread_bps_low          = EXCLUDED.spread_bps_low,
                spread_bps_close        = EXCLUDED.spread_bps_close,
                bid_qty_1pct_close      = EXCLUDED.bid_qty_1pct_close,
                ask_qty_1pct_close      = EXCLUDED.ask_qty_1pct_close,
                bid_qty_2_5pct_close    = EXCLUDED.bid_qty_2_5pct_close,
                ask_qty_2_5pct_close    = EXCLUDED.ask_qty_2_5pct_close,
                bid_qty_5pct_close      = EXCLUDED.bid_qty_5pct_close,
                ask_qty_5pct_close      = EXCLUDED.ask_qty_5pct_close,
                bid_qty_10pct_close     = EXCLUDED.bid_qty_10pct_close,
                ask_qty_10pct_close     = EXCLUDED.ask_qty_10pct_close,
                imbalance_1pct_high     = EXCLUDED.imbalance_1pct_high,
                imbalance_1pct_low      = EXCLUDED.imbalance_1pct_low,
                imbalance_2_5pct_high   = EXCLUDED.imbalance_2_5pct_high,
                imbalance_2_5pct_low    = EXCLUDED.imbalance_2_5pct_low,
                imbalance_5pct_high     = EXCLUDED.imbalance_5pct_high,
                imbalance_5pct_low      = EXCLUDED.imbalance_5pct_low,
                imbalance_10pct_high    = EXCLUDED.imbalance_10pct_high,
                imbalance_10pct_low     = EXCLUDED.imbalance_10pct_low,
                avg_depth_coverage_pct  = EXCLUDED.avg_depth_coverage_pct,
                snapshot_count          = EXCLUDED.snapshot_count,
                updated_at              = NOW()
        """)
        rows_updated = cur.rowcount
        conn.commit()
        cur.close()
        conn.close()
        print(f"  [DB] Upserted orderbook daily metrics for {rows_updated} rows.", flush=True)
    except Exception as e:
        print(f"  [DB ERROR] Orderbook daily aggregation failed: {e}", flush=True)
        failed_steps.append("Orderbook Daily Metrics")

    # 5. Purge intraday_snapshots older than 48h (futures_snapshots kept permanently)
    print("\n[5/5] Purging old intraday snapshots + Refreshing Materialized Views...", flush=True)
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT purge_old_snapshots()")
        for view in ["mv_aggregated_by_asset", "mv_global_market", "mv_trading_metrics"]:
            print(f"  Refreshing {view}...", flush=True)
            cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
        cur.close()
        conn.close()
        print("  [DB] Snapshots purged + materialized views refreshed.", flush=True)
    except Exception as e:
        print(f"  [DB ERROR] Failed: {e}", flush=True)
        failed_steps.append("DB Refresh")

    if failed_steps:
        print(f"\n--- Pipeline Finished with ERRORS in: {', '.join(failed_steps)} ---", flush=True)
        sys.exit(1)
    else:
        print("\n--- Pipeline Completed Successfully ---", flush=True)
        sys.exit(0)

if __name__ == "__main__":
    main()
