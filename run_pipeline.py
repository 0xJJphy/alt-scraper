import os
import subprocess
import sys
from dotenv import load_dotenv

def run_command(command):
    print(f"Executing: {' '.join(command)}", flush=True)
    # We use capture_output=False to let the result print to the console/journal in real-time
    result = subprocess.run(command, capture_output=False, text=True)
    if result.returncode != 0:
        print(f"!! Command failed with return code {result.returncode}: {' '.join(command)}", flush=True)
    return result.returncode

def main():
    load_dotenv()
    
    # Get absolute paths to scripts
    base_dir = os.path.dirname(os.path.abspath(__file__))
    spot_script = os.path.join(base_dir, "spot_scraper.py")
    alts_script = os.path.join(base_dir, "alts_scraper.py")
    
    top_n = os.getenv("TOP_N", "50")
    db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        print("DATABASE_URL not found in environment. Exiting.", flush=True)
        sys.exit(1)

    print("--- Starting Alts Scraper Pipeline ---", flush=True)
    
    failed_steps = []

    # 1. Sync Metadata
    print("\n[1/4] Syncing Global Metadata...", flush=True)
    if run_command([sys.executable, "-u", spot_script, "--limit", top_n, "--metadata-only"]) != 0:
        failed_steps.append("Sync Metadata")

    # 2. Run Spot Scraper
    print("\n[2/4] Running Spot Scraper...", flush=True)
    if run_command([sys.executable, "-u", spot_script, "--limit", top_n]) != 0:
        failed_steps.append("Spot Scraper")

    # 3. Run Futures Scraper
    print("\n[3/4] Running Futures Scraper...", flush=True)
    if run_command([sys.executable, "-u", alts_script, "--limit", top_n, "--exchanges", "binance,bybit,okx"]) != 0:
        failed_steps.append("Futures Scraper")

    # 4. Refresh Materialized Views
    print("\n[4/4] Refreshing Materialized Views...", flush=True)
    psql_cmd = [
        "psql", db_url, "-c", 
        "REFRESH MATERIALIZED VIEW mv_aggregated_by_asset; REFRESH MATERIALIZED VIEW mv_global_market; REFRESH MATERIALIZED VIEW mv_trading_metrics;"
    ]
    if run_command(psql_cmd) != 0:
        failed_steps.append("DB Refresh")

    if failed_steps:
        print(f"\n--- Pipeline Finished with ERRORS in: {', '.join(failed_steps)} ---", flush=True)
        sys.exit(1)
    else:
        print("\n--- Pipeline Completed Successfully ---", flush=True)
        sys.exit(0)

if __name__ == "__main__":
    main()
