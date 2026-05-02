#!/usr/bin/env python3
"""
run_backfill.py — Launch parallel historical orderbook backfill.

Reads all assets from asset_metadata, splits them into groups, and starts
one backfill process per group. Logs go to logs/backfill/.

Usage:
    python run_backfill.py                      # 8 parallel workers, auto start date
    python run_backfill.py --workers 4          # 4 parallel workers
    python run_backfill.py --start 2024-01-01   # fixed start date for all assets
    python run_backfill.py --kill               # kill any running backfill processes
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()

REPO_DIR   = Path(__file__).parent.resolve()
LOG_DIR    = REPO_DIR / "logs" / "backfill"
PYTHON     = REPO_DIR / "venv" / "bin" / "python3"
SCRIPT     = REPO_DIR / "orderbook_backfill.py"
DB_URL     = os.getenv("DATABASE_URL")


def get_assets() -> list[str]:
    conn = psycopg2.connect(DB_URL)
    cur  = conn.cursor()
    cur.execute("""
        SELECT symbol FROM asset_metadata
        WHERE is_filtered = FALSE OR is_filtered IS NULL
        ORDER BY market_cap_rank ASC NULLS LAST
    """)
    syms = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return syms


def kill_running() -> int:
    result = subprocess.run(
        ["pgrep", "-f", "orderbook_backfill.py"],
        capture_output=True, text=True,
    )
    pids = [p for p in result.stdout.strip().split("\n") if p]
    for pid in pids:
        try:
            os.kill(int(pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
    return len(pids)


def main():
    parser = argparse.ArgumentParser(description="Launch parallel orderbook backfill")
    parser.add_argument("--workers", type=int, default=8,
                        help="Number of parallel processes (default: 8)")
    parser.add_argument("--exchange", default="binance", choices=["binance", "bybit"])
    parser.add_argument("--start", default=None,
                        help="Start date YYYY-MM-DD. Omit to auto-detect per asset.")
    parser.add_argument("--end", default=None,
                        help="End date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--kill", action="store_true",
                        help="Kill any running backfill processes and exit")
    args = parser.parse_args()

    if not DB_URL:
        print("DATABASE_URL not set in .env", file=sys.stderr)
        sys.exit(1)

    killed = kill_running()
    if killed:
        print(f"Stopped {killed} existing backfill process(es).")
    if args.kill:
        return

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    assets  = get_assets()
    workers = min(args.workers, len(assets))
    groups  = [assets[i::workers] for i in range(workers)]
    end_arg = args.end or str(date.today() - timedelta(days=1))

    print(f"Launching {workers} workers for {len(assets)} assets ({args.exchange})")
    if args.start:
        print(f"  Start date : {args.start}")
    else:
        print(f"  Start date : auto-detect per asset (earliest available)")
    print(f"  End date   : {end_arg}")
    print(f"  Logs       : {LOG_DIR}/")
    print()

    pids = []
    for i, group in enumerate(groups, 1):
        symbols   = ",".join(group)
        log_file  = LOG_DIR / f"worker_{i:02d}.log"
        cmd = [
            str(PYTHON), str(SCRIPT),
            "--exchange", args.exchange,
            "--symbols",  symbols,
            "--end",      end_arg,
        ]
        if args.start:
            cmd += ["--start", args.start]

        with open(log_file, "w") as lf:
            proc = subprocess.Popen(
                cmd,
                stdout=lf, stderr=lf,
                start_new_session=True,
            )
        pids.append(proc.pid)
        print(f"  Worker {i:2d} | PID {proc.pid:6d} | {symbols[:60]}")

    print()
    print("Monitor progress:")
    log_glob = str(LOG_DIR / "worker_*.log")
    print(f"  tail -f {log_glob}")
    print()
    print("Count rows in DB:")
    print(f"  python3 -c \"import psycopg2,os; from dotenv import load_dotenv; load_dotenv(); "
          f"conn=psycopg2.connect(os.getenv('DATABASE_URL')); cur=conn.cursor(); "
          f"cur.execute(\\\"SELECT COUNT(*), COUNT(DISTINCT symbol) FROM orderbook_snapshots "
          f"WHERE exchange='{args.exchange}'\\\"); print(cur.fetchone()); conn.close()\"")
    print()
    print(f"Stop all workers:")
    print(f"  python3 {SCRIPT.name.replace('backfill', 'run_backfill')} --kill")
    print()
    print(f"All {workers} workers running. Safe to close terminal — processes survive via start_new_session.")


if __name__ == "__main__":
    main()
