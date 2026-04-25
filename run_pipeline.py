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

    # 2+3. Run Spot and Futures scrapers IN PARALLEL (independent of each other)
    print("\n[2-3/4] Running Spot + Futures scrapers in parallel...", flush=True)
    parallel_results = run_command_parallel({
        "SPOT": [sys.executable, "-u", spot_script, "--limit", top_n],
        "FUTURES": [sys.executable, "-u", alts_script, "--limit", top_n, "--exchanges", "binance,bybit,okx"],
    })
    if parallel_results.get("SPOT", 0) != 0:
        failed_steps.append("Spot Scraper")
    if parallel_results.get("FUTURES", 0) != 0:
        failed_steps.append("Futures Scraper")

    # 4. Refresh Materialized Views
    print("\n[4/4] Refreshing Materialized Views...", flush=True)
    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        cur = conn.cursor()
        for view in ["mv_aggregated_by_asset", "mv_global_market", "mv_trading_metrics"]:
            print(f"  Refreshing {view}...", flush=True)
            cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
        cur.close()
        conn.close()
        print("  [DB] Materialized views refreshed.", flush=True)
    except Exception as e:
        print(f"  [DB ERROR] Failed to refresh views: {e}", flush=True)
        failed_steps.append("DB Refresh")

    if failed_steps:
        print(f"\n--- Pipeline Finished with ERRORS in: {', '.join(failed_steps)} ---", flush=True)
        sys.exit(1)
    else:
        print("\n--- Pipeline Completed Successfully ---", flush=True)
        sys.exit(0)

if __name__ == "__main__":
    main()
