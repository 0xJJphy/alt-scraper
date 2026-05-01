#!/usr/bin/env python3
"""
orderbook_backfill.py — Historical order book backfill from free exchange portals.

Sources:
  Binance: data.binance.vision/futures/um/daily/bookDepth/{symbol}/
           S_DEPTH format — one snapshot every 5 min, aligned to 4h timestamps.
  Bybit:   public.bybit.com/orderBook_200/{symbol}/ (gzipped CSV, monthly files)
  OKX:     Currently no free public portal for historical L2 snapshots.
           (Tardis.dev covers from Mar 2019 but requires payment.)

Usage:
    python orderbook_backfill.py --exchange binance --start 2020-01-01
    python orderbook_backfill.py --exchange bybit   --start 2020-01-01
    python orderbook_backfill.py --exchange binance --start 2023-01-01 --symbols BTC,ETH
    python orderbook_backfill.py --exchange binance --start 2024-01-01 --end 2024-12-31

Idempotent: uses ON CONFLICT DO UPDATE so re-runs are safe.
"""

import argparse
import csv
import gzip
import io
import json
import logging
import os
import sys
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterator, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

# Import compute_metrics from the daemon (same function, guaranteed consistent)
from orderbook_daemon import compute_metrics, SNAPSHOT_HOURS, BAND_COLS

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("backfill")

# 4h snapshot timestamps for backfill (matching daemon's SNAPSHOT_HOURS)
BACKFILL_HOURS = sorted(SNAPSHOT_HOURS)  # [0, 4, 8, 12, 16, 20]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

SNAPSHOT_COLS = [
    "snapshot_at", "symbol", "exchange", "base_asset",
    "mid_price", "best_bid", "best_ask", "spread_bps", "depth_coverage_pct",
    "bid_qty_1pct", "ask_qty_1pct", "bid_levels_1pct", "ask_levels_1pct", "imbalance_1pct",
    "bid_qty_2_5pct", "ask_qty_2_5pct", "bid_levels_2_5pct", "ask_levels_2_5pct", "imbalance_2_5pct",
    "bid_qty_5pct", "ask_qty_5pct", "bid_levels_5pct", "ask_levels_5pct", "imbalance_5pct",
    "bid_qty_10pct", "ask_qty_10pct", "bid_levels_10pct", "ask_levels_10pct", "imbalance_10pct",
]

SNAPSHOT_SQL = """
INSERT INTO orderbook_snapshots ({cols})
VALUES %s
ON CONFLICT (snapshot_at, symbol, exchange) DO UPDATE SET
    mid_price           = EXCLUDED.mid_price,
    best_bid            = EXCLUDED.best_bid,
    best_ask            = EXCLUDED.best_ask,
    spread_bps          = EXCLUDED.spread_bps,
    depth_coverage_pct  = EXCLUDED.depth_coverage_pct,
    bid_qty_1pct        = EXCLUDED.bid_qty_1pct,
    ask_qty_1pct        = EXCLUDED.ask_qty_1pct,
    bid_levels_1pct     = EXCLUDED.bid_levels_1pct,
    ask_levels_1pct     = EXCLUDED.ask_levels_1pct,
    imbalance_1pct      = EXCLUDED.imbalance_1pct,
    bid_qty_2_5pct      = EXCLUDED.bid_qty_2_5pct,
    ask_qty_2_5pct      = EXCLUDED.ask_qty_2_5pct,
    bid_levels_2_5pct   = EXCLUDED.bid_levels_2_5pct,
    ask_levels_2_5pct   = EXCLUDED.ask_levels_2_5pct,
    imbalance_2_5pct    = EXCLUDED.imbalance_2_5pct,
    bid_qty_5pct        = EXCLUDED.bid_qty_5pct,
    ask_qty_5pct        = EXCLUDED.ask_qty_5pct,
    bid_levels_5pct     = EXCLUDED.bid_levels_5pct,
    ask_levels_5pct     = EXCLUDED.ask_levels_5pct,
    imbalance_5pct      = EXCLUDED.imbalance_5pct,
    bid_qty_10pct       = EXCLUDED.bid_qty_10pct,
    ask_qty_10pct       = EXCLUDED.ask_qty_10pct,
    bid_levels_10pct    = EXCLUDED.bid_levels_10pct,
    ask_levels_10pct    = EXCLUDED.ask_levels_10pct,
    imbalance_10pct     = EXCLUDED.imbalance_10pct
""".format(cols=", ".join(SNAPSHOT_COLS))

DAILY_SQL = """
INSERT INTO orderbook_daily_metrics (
    date, symbol, exchange, base_asset,
    spread_bps_open, spread_bps_high, spread_bps_low, spread_bps_close,
    bid_qty_1pct_close, ask_qty_1pct_close,
    bid_qty_2_5pct_close, ask_qty_2_5pct_close,
    bid_qty_5pct_close, ask_qty_5pct_close,
    bid_qty_10pct_close, ask_qty_10pct_close,
    imbalance_1pct_high, imbalance_1pct_low,
    imbalance_2_5pct_high, imbalance_2_5pct_low,
    imbalance_5pct_high, imbalance_5pct_low,
    imbalance_10pct_high, imbalance_10pct_low,
    avg_depth_coverage_pct, snapshot_count
)
SELECT
    DATE(snapshot_at AT TIME ZONE 'UTC'),
    symbol, exchange, MAX(base_asset),
    MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 0  THEN spread_bps END),
    MAX(spread_bps),
    MIN(spread_bps),
    MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN spread_bps END),
    MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN bid_qty_1pct END),
    MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN ask_qty_1pct END),
    MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN bid_qty_2_5pct END),
    MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN ask_qty_2_5pct END),
    MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN bid_qty_5pct END),
    MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN ask_qty_5pct END),
    MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN bid_qty_10pct END),
    MIN(CASE WHEN EXTRACT(HOUR FROM snapshot_at AT TIME ZONE 'UTC') = 20 THEN ask_qty_10pct END),
    MAX(imbalance_1pct),   MIN(imbalance_1pct),
    MAX(imbalance_2_5pct), MIN(imbalance_2_5pct),
    MAX(imbalance_5pct),   MIN(imbalance_5pct),
    MAX(imbalance_10pct),  MIN(imbalance_10pct),
    AVG(depth_coverage_pct), COUNT(*)
FROM orderbook_snapshots
WHERE DATE(snapshot_at AT TIME ZONE 'UTC') = %s
  AND symbol = %s
  AND exchange = %s
GROUP BY DATE(snapshot_at AT TIME ZONE 'UTC'), symbol, exchange
ON CONFLICT (date, symbol, exchange) DO UPDATE SET
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
"""


def _metrics_to_row(snapshot_at, symbol, exchange, base_asset, m: dict) -> tuple:
    return (
        snapshot_at, symbol, exchange, base_asset,
        m.get("mid_price"), m.get("best_bid"), m.get("best_ask"),
        m.get("spread_bps"), m.get("depth_coverage_pct"),
        m.get("bid_qty_1pct"), m.get("ask_qty_1pct"),
        m.get("bid_levels_1pct"), m.get("ask_levels_1pct"), m.get("imbalance_1pct"),
        m.get("bid_qty_2_5pct"), m.get("ask_qty_2_5pct"),
        m.get("bid_levels_2_5pct"), m.get("ask_levels_2_5pct"), m.get("imbalance_2_5pct"),
        m.get("bid_qty_5pct"), m.get("ask_qty_5pct"),
        m.get("bid_levels_5pct"), m.get("ask_levels_5pct"), m.get("imbalance_5pct"),
        m.get("bid_qty_10pct"), m.get("ask_qty_10pct"),
        m.get("bid_levels_10pct"), m.get("ask_levels_10pct"), m.get("imbalance_10pct"),
    )


def upsert_snapshots(conn, rows: list) -> None:
    if not rows:
        return
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, SNAPSHOT_SQL, rows, page_size=500)
    conn.commit()
    cur.close()


def upsert_daily(conn, day: date, symbol: str, exchange: str) -> None:
    cur = conn.cursor()
    cur.execute(DAILY_SQL, (day, symbol, exchange))
    conn.commit()
    cur.close()


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def date_range(start: date, end: date) -> Iterator[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def _download(url: str, retries: int = 3) -> Optional[bytes]:
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=60, stream=True)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.content
        except Exception as e:
            if attempt == retries - 1:
                log.warning("download failed %s: %s", url, e)
                return None
            time.sleep(2 ** attempt)
    return None


# ---------------------------------------------------------------------------
# Binance Futures backfill
# Docs: https://data.binance.vision/?prefix=futures/um/daily/bookDepth/
# Format: CSV with columns: timestamp_ms, last_update_id, bids_json, asks_json
# One row = one snapshot, every ~5 min.
# ---------------------------------------------------------------------------

BINANCE_BASE = "https://data.binance.vision/futures/um/daily/bookDepth"


def _parse_binance_depth_file(content: bytes) -> Iterator[Tuple[datetime, dict, dict]]:
    """
    Yields (timestamp_utc, bids_dict, asks_dict) for each row in a Binance S_DEPTH file.
    Bids/asks are {price_float: qty_float}.
    """
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        csv_name = zf.namelist()[0]
        with zf.open(csv_name) as f:
            reader = csv.reader(io.TextIOWrapper(f))
            next(reader, None)  # skip header
            for row in reader:
                if len(row) < 4:
                    continue
                try:
                    ts_ms = int(row[0])
                    ts    = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
                    bids_raw = json.loads(row[2])
                    asks_raw = json.loads(row[3])
                    bids = {float(p): float(q) for p, q in bids_raw}
                    asks = {float(p): float(q) for p, q in asks_raw}
                    yield ts, bids, asks
                except (ValueError, json.JSONDecodeError, IndexError):
                    continue


def backfill_binance(
    conn,
    symbol: str,
    base_asset: str,
    start_date: date,
    end_date: date,
) -> None:
    total_snaps = 0
    for day in date_range(start_date, end_date):
        url = f"{BINANCE_BASE}/{symbol}/{symbol}-bookDepth-{day}.zip"
        content = _download(url)
        if content is None:
            log.debug("binance: no data for %s %s", symbol, day)
            continue

        # Collect the snapshot closest to each 4h boundary
        # Key: hour → (ts, bids, asks) for the row with ts just before or at that hour
        candidates: Dict[int, Tuple[datetime, dict, dict]] = {}
        try:
            for ts, bids, asks in _parse_binance_depth_file(content):
                if ts.date() != day:
                    continue
                h = ts.hour
                # Find which 4h boundary this snapshot is closest to (must be within 30 min)
                for snap_h in BACKFILL_HOURS:
                    delta_min = abs((h * 60 + ts.minute) - snap_h * 60)
                    if delta_min <= 30:
                        existing = candidates.get(snap_h)
                        if existing is None:
                            candidates[snap_h] = (ts, bids, asks)
                        else:
                            # prefer the snapshot closest to the boundary
                            existing_delta = abs((existing[0].hour * 60 + existing[0].minute) - snap_h * 60)
                            if delta_min < existing_delta:
                                candidates[snap_h] = (ts, bids, asks)
                        break
        except Exception as e:
            log.warning("binance parse error %s %s: %s", symbol, day, e)
            continue

        rows = []
        for snap_h, (_, bids, asks) in candidates.items():
            snap_ts = datetime(day.year, day.month, day.day, snap_h, 0, 0, tzinfo=timezone.utc)
            m = compute_metrics(bids, asks)
            if m:
                rows.append(_metrics_to_row(snap_ts, symbol, "binance", base_asset, m))

        if rows:
            upsert_snapshots(conn, rows)
            upsert_daily(conn, day, symbol, "binance")
            total_snaps += len(rows)
            log.info("binance %s %s: %d snapshots inserted", symbol, day, len(rows))

    log.info("binance %s done: %d total snapshots", symbol, total_snaps)


# ---------------------------------------------------------------------------
# Bybit Linear backfill
# Docs: https://public.bybit.com/orderBook_200/{symbol}/
# Format: gzipped CSV — timestamp_us, symbol, side, size, price
# Each file covers all updates for a given month.
# Strategy: reconstruct book state at each 4h boundary by replaying updates.
# ---------------------------------------------------------------------------

BYBIT_BASE = "https://public.bybit.com/orderBook_200"


def _list_bybit_files(symbol: str, year: int, month: int) -> Optional[str]:
    """Return the URL of the Bybit orderBook_200 file for the given month."""
    # Bybit files follow: {symbol}{YYYY}-{MM}.csv.gz  (zero-padded month)
    fname = f"{symbol}{year}-{month:02d}.csv.gz"
    return f"{BYBIT_BASE}/{symbol}/{fname}"


def _parse_bybit_orderbook_file(content: bytes) -> Iterator[Tuple[datetime, str, float, float]]:
    """
    Yields (timestamp_utc, side, price, qty) — side is 'Buy' or 'Sell'.
    Bybit uses 'size' for quantity (in base asset).
    """
    with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
        reader = csv.DictReader(io.TextIOWrapper(f))
        for row in reader:
            try:
                ts_us  = int(row.get("timestamp") or row.get("timestamp_us", 0))
                ts     = datetime.fromtimestamp(ts_us / 1_000_000.0, tz=timezone.utc)
                side   = row.get("side", "")
                price  = float(row.get("price", 0))
                size   = float(row.get("size", 0))
                if side and price > 0:
                    yield ts, side, price, size
            except (ValueError, KeyError):
                continue


def backfill_bybit(
    conn,
    symbol: str,
    base_asset: str,
    start_date: date,
    end_date: date,
) -> None:
    """
    Reconstructs Bybit order book by replaying all updates month by month.
    At each 4h boundary, takes a snapshot of the current book state.
    """
    total_snaps = 0
    # Group dates by year-month
    months_needed = set()
    for day in date_range(start_date, end_date):
        months_needed.add((day.year, day.month))

    for year, month in sorted(months_needed):
        url     = _list_bybit_files(symbol, year, month)
        content = _download(url)
        if content is None:
            log.debug("bybit: no data for %s %d-%02d", symbol, year, month)
            continue

        bids: Dict[float, float] = {}
        asks: Dict[float, float] = {}
        rows      = []
        snap_days = set()

        # Determine 4h snapshot boundaries within [start_date, end_date] for this month
        snap_targets = []
        for day in date_range(
            max(start_date, date(year, month, 1)),
            min(end_date, date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)),
        ):
            for h in BACKFILL_HOURS:
                snap_targets.append(datetime(day.year, day.month, day.day, h, 0, 0, tzinfo=timezone.utc))
        snap_targets.sort()
        snap_idx = 0

        try:
            for ts, side, price, size in _parse_bybit_orderbook_file(content):
                # advance through snapshot targets that this update has passed
                while snap_idx < len(snap_targets) and ts >= snap_targets[snap_idx]:
                    snap_ts = snap_targets[snap_idx]
                    if snap_ts.date() >= start_date and snap_ts.date() <= end_date:
                        m = compute_metrics(bids, asks)
                        if m:
                            rows.append(_metrics_to_row(snap_ts, symbol, "bybit", base_asset, m))
                            snap_days.add(snap_ts.date())
                    snap_idx += 1

                if side == "Buy":
                    if size == 0:
                        bids.pop(price, None)
                    else:
                        bids[price] = size
                elif side == "Sell":
                    if size == 0:
                        asks.pop(price, None)
                    else:
                        asks[price] = size
        except Exception as e:
            log.warning("bybit parse error %s %d-%02d: %s", symbol, year, month, e)
            continue

        # flush remaining snapshot targets after last update
        while snap_idx < len(snap_targets):
            snap_ts = snap_targets[snap_idx]
            if snap_ts.date() >= start_date and snap_ts.date() <= end_date:
                m = compute_metrics(bids, asks)
                if m:
                    rows.append(_metrics_to_row(snap_ts, symbol, "bybit", base_asset, m))
                    snap_days.add(snap_ts.date())
            snap_idx += 1

        if rows:
            upsert_snapshots(conn, rows)
            for day in snap_days:
                upsert_daily(conn, day, symbol, "bybit")
            total_snaps += len(rows)
            log.info("bybit %s %d-%02d: %d snapshots inserted", symbol, year, month, len(rows))

    log.info("bybit %s done: %d total snapshots", symbol, total_snaps)


# ---------------------------------------------------------------------------
# Asset metadata loader
# ---------------------------------------------------------------------------

def load_assets(db_url: str, symbols: Optional[List[str]] = None) -> List[dict]:
    conn = psycopg2.connect(db_url)
    cur  = conn.cursor()
    if symbols:
        cur.execute("""
            SELECT symbol FROM asset_metadata
            WHERE symbol = ANY(%s)
            ORDER BY market_cap_rank ASC NULLS LAST
        """, (symbols,))
    else:
        cur.execute("""
            SELECT symbol FROM asset_metadata
            WHERE is_filtered = FALSE OR is_filtered IS NULL
            ORDER BY market_cap_rank ASC NULLS LAST
        """)
    bases = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return [
        {
            "base_asset":     b,
            "symbol_binance": f"{b}USDT",
            "symbol_bybit":   f"{b}USDT",
        }
        for b in bases
    ]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Historical order book backfill")
    parser.add_argument("--exchange", required=True, choices=["binance", "bybit"],
                        help="Exchange to backfill (okx: no free portal available)")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end",   default=None,  help="End date (YYYY-MM-DD), defaults to yesterday")
    parser.add_argument("--symbols", default=None,
                        help="Comma-separated base assets (e.g. BTC,ETH). Defaults to all in DB.")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)

    start_date = date.fromisoformat(args.start)
    end_date   = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)
    symbols    = [s.strip().upper() for s in args.symbols.split(",")] if args.symbols else None

    if start_date > end_date:
        print(f"start {start_date} is after end {end_date}", file=sys.stderr)
        sys.exit(1)

    assets = load_assets(db_url, symbols)
    if not assets:
        print("no assets found in asset_metadata", file=sys.stderr)
        sys.exit(1)

    log.info("backfilling %s for %d assets from %s to %s",
             args.exchange, len(assets), start_date, end_date)

    conn = psycopg2.connect(db_url)

    try:
        for asset in assets:
            base = asset["base_asset"]

            if args.exchange == "binance":
                sym = asset.get("symbol_binance")
                if not sym:
                    log.warning("no binance symbol for %s, skipping", base)
                    continue
                log.info("backfilling binance %s (%s)...", sym, base)
                backfill_binance(conn, sym, base, start_date, end_date)

            elif args.exchange == "bybit":
                sym = asset.get("symbol_bybit")
                if not sym:
                    log.warning("no bybit symbol for %s, skipping", base)
                    continue
                log.info("backfilling bybit %s (%s)...", sym.upper(), base)
                backfill_bybit(conn, sym.upper(), base, start_date, end_date)

    finally:
        conn.close()

    log.info("backfill complete.")


if __name__ == "__main__":
    main()
