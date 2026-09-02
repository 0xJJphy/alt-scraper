import os
import subprocess
import sys
import threading
import psycopg2
from dotenv import load_dotenv


def run_command(command, label=""):
    tag = f"[{label}] " if label else ""
    print(f"{tag}Executing: {' '.join(command)}", flush=True)
    result = subprocess.run(command, capture_output=False, text=True)
    if result.returncode != 0:
        print(f"{tag}!! Command failed with return code {result.returncode}", flush=True)
    return result.returncode


def run_command_parallel(commands):
    """Run multiple commands in parallel threads. Returns list of return codes."""
    results = {}
    threads = []

    def worker(label, cmd):
        results[label] = run_command(cmd, label=label)

    for label, cmd in commands.items():
        t = threading.Thread(target=worker, args=(label, cmd))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    return results


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

    # 2+3. Run Spot and Futures scrapers IN PARALLEL.
    #
    # Es seguro porque el cupo de Coinalyze es POR KEY (comprobado: agotando una
    # hasta el 429, las otras seguían respondiendo 200) y cada proceso usa las
    # suyas: spot va con COINALYZE_API_KEY_SPOT, futures con COINALYZE_API_KEY[_N].
    #
    # Hubo una corrida en la que esto se serializó creyendo que compartían cuota.
    # No era eso: el fallo estaba en que el limitador de alt_scraper contaba
    # LLAMADAS cuando Coinalyze cobra por SÍMBOLO, así que gastaba 4,7x de su
    # propio cupo. Serializar no cambió nada —los exchanges seguían muriendo— y
    # sólo añadía ~25 min de reloj. Arreglado el limitador, el paralelo vuelve.
    print("\n[2-3/4] Running Spot + Futures scrapers in parallel...", flush=True)
    parallel_results = run_command_parallel({
        # Coinbase se nombra aqui y no en el default de spot_scraper.py: asi el nocturno
        # dice explicitamente que venues cubre y una llamada manual sin argumentos sigue
        # comportandose como siempre. Usa COINALYZE_API_KEY_SPOT, cupo aparte del de futuros.
        "SPOT": [sys.executable, "-u", spot_script, "--limit", top_n,
                 "--exchanges", "binance,bybit,okx,coinbase"],
        "FUTURES": [sys.executable, "-u", alts_script, "--limit", top_n, "--exchanges", "binance,bybit,okx"],
    })
    if parallel_results.get("SPOT", 0) != 0:
        failed_steps.append("Spot Scraper")
    if parallel_results.get("FUTURES", 0) != 0:
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
