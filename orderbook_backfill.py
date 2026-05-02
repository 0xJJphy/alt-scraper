#!/usr/bin/env python3
"""
orderbook_backfill.py — Historical order book backfill from free exchange portals.

Sources:
  Binance: data.binance.vision/data/futures/um/daily/bookDepth/{symbol}/
           Format: timestamp, percentage (-5..+5), cumulative_depth, notional.
           One row per band per snapshot (~30s cadence). Available from 2023-01-01.
           Prices from 1m klines (data.binance.vision/data/futures/um/daily/klines/).
           Note: level counts, best_bid/ask, spread_bps, and 10pct bands are NULL
           (not available in this aggregated format).
  Bybit:   public.bybit.com/orderBook_200/ discontinued in 2023 — no free source.
  OKX:     No free public portal for historical L2 snapshots.
           (Tardis.dev covers from Mar 2019 but requires payment.)

Usage:
    python orderbook_backfill.py --exchange binance --start 2020-01-01
    python orderbook_backfill.py --exchange bybit   --start 2020-01-01
    python orderbook_backfill.py --exchange binance --start 2023-01-01 --symbols BTC,ETH
    python orderbook_backfill.py --exchange binance --start 2024-01-01 --end 2024-12-31

Idempotent: uses ON CONFLICT DO UPDATE so re-runs are safe.
"""

import argparse
import bisect
import csv
import gzip
import io
import logging
import os
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterator, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

# Import shared helpers from the daemon for consistency
from orderbook_daemon import compute_metrics, resolve_exchange_symbols, SNAPSHOT_HOURS, BAND_COLS

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
    "snapshot_at", "symbol", "exchange", "base_asset", "market_type",
    "mid_price", "best_bid", "best_ask", "spread_bps", "depth_coverage_pct",
    "bid_qty_1pct", "ask_qty_1pct", "bid_levels_1pct", "ask_levels_1pct", "imbalance_1pct",
    "bid_qty_2_5pct", "ask_qty_2_5pct", "bid_levels_2_5pct", "ask_levels_2_5pct", "imbalance_2_5pct",
    "bid_qty_5pct", "ask_qty_5pct", "bid_levels_5pct", "ask_levels_5pct", "imbalance_5pct",
    "bid_qty_10pct", "ask_qty_10pct", "bid_levels_10pct", "ask_levels_10pct", "imbalance_10pct",
]

SNAPSHOT_SQL = """
INSERT INTO orderbook_snapshots ({cols})
VALUES %s
ON CONFLICT (snapshot_at, symbol, exchange, market_type) DO UPDATE SET
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
    date, symbol, exchange, market_type, base_asset,
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
    symbol, exchange, market_type, MAX(base_asset),
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
  AND market_type = %s
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
"""


def _metrics_to_row(snapshot_at, symbol, exchange, base_asset, market_type, m: dict) -> tuple:
    return (
        snapshot_at, symbol, exchange, base_asset, market_type,
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


def upsert_daily(conn, day: date, symbol: str, exchange: str, market_type: str = "futures") -> None:
    cur = conn.cursor()
    cur.execute(DAILY_SQL, (day, symbol, exchange, market_type))
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
# Source: data.binance.vision — bookDepth (depth by % band) + 1m klines (price)
# ---------------------------------------------------------------------------

BINANCE_BASE        = "https://data.binance.vision/data/futures/um/daily/bookDepth"
BINANCE_KLINES_BASE = "https://data.binance.vision/data/futures/um/daily/klines"
BINANCE_S3          = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"


def _first_available_date_binance(symbol: str) -> Optional[date]:
    """Query Binance S3 to find the earliest available bookDepth date for a symbol."""
    url = (
        f"{BINANCE_S3}?delimiter=/&max-keys=10"
        f"&prefix=data/futures/um/daily/bookDepth/{symbol}/"
    )
    try:
        r = requests.get(url, timeout=10)
        root = ET.fromstring(r.text)
        ns = "{http://s3.amazonaws.com/doc/2006-03-01/}"
        for c in root.findall(f"{ns}Contents"):
            key = c.find(f"{ns}Key").text
            if key.endswith(".zip") and "CHECKSUM" not in key:
                date_str = key.split("-bookDepth-")[1].replace(".zip", "")
                return date.fromisoformat(date_str)
    except Exception:
        pass
    return None


def _parse_binance_bookdepth(content: bytes) -> List[Tuple[datetime, Dict[int, float]]]:
    """
    Parse bookDepth zip → sorted list of (ts_utc, {pct_int: cumulative_depth}).
    Columns: timestamp (str), percentage (int), depth (float), notional.
    Negative pct = bid side, positive pct = ask side. Range: -5..+5.
    """
    snapshots: Dict[str, Dict[int, float]] = {}
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        with zf.open(zf.namelist()[0]) as f:
            reader = csv.reader(io.TextIOWrapper(f))
            next(reader, None)  # skip header
            for row in reader:
                if len(row) < 3:
                    continue
                try:
                    ts_str = row[0]
                    pct    = int(row[1])
                    depth  = float(row[2])
                    if ts_str not in snapshots:
                        snapshots[ts_str] = {}
                    snapshots[ts_str][pct] = depth
                except (ValueError, IndexError):
                    continue
    result = []
    for ts_str, bands in snapshots.items():
        try:
            ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            result.append((ts, bands))
        except ValueError:
            continue
    result.sort(key=lambda x: x[0])
    return result


def _parse_binance_klines_1m(content: bytes) -> List[Tuple[datetime, float]]:
    """
    Parse 1m klines zip → sorted list of (open_time_utc, close_price).
    Used to obtain mid_price for each backfill snapshot.
    """
    result = []
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        with zf.open(zf.namelist()[0]) as f:
            reader = csv.reader(io.TextIOWrapper(f))
            for row in reader:
                if len(row) < 5:
                    continue
                try:
                    ts    = datetime.fromtimestamp(int(row[0]) / 1000.0, tz=timezone.utc)
                    close = float(row[4])
                    result.append((ts, close))
                except (ValueError, IndexError):
                    continue
    result.sort(key=lambda x: x[0])
    return result


def _nearest(sorted_list: list, target: datetime, max_secs: float) -> Optional[tuple]:
    """Return the item from sorted_list (sorted by item[0]) closest to target, or None."""
    if not sorted_list:
        return None
    times = [x[0] for x in sorted_list]
    idx = bisect.bisect_left(times, target)
    best, best_diff = None, float("inf")
    for i in (idx - 1, idx):
        if 0 <= i < len(sorted_list):
            diff = abs((sorted_list[i][0] - target).total_seconds())
            if diff < best_diff:
                best_diff, best = diff, sorted_list[i]
    return best if best_diff <= max_secs else None


def compute_metrics_from_bands(mid_price: Optional[float], bands: Dict[int, float]) -> dict:
    """
    Compute orderbook metrics from pre-aggregated bookDepth bands.
    bands: {-5: depth, -4: depth, ..., -1: depth, 1: depth, ..., 5: depth}
    - Level counts not available in aggregated data → None.
    - 10pct not in source data → None.
    - 2.5pct linearly interpolated as average of 2pct and 3pct cumulative depths.
    """
    def imb(b, a):
        total = b + a
        return (b - a) / total if total > 0 else None

    bid_1  = bands.get(-1)
    ask_1  = bands.get(1)
    bid_2  = bands.get(-2, 0.0)
    ask_2  = bands.get(2, 0.0)
    bid_3  = bands.get(-3, 0.0)
    ask_3  = bands.get(3, 0.0)
    bid_5  = bands.get(-5)
    ask_5  = bands.get(5)
    bid_25 = (bid_2 + bid_3) / 2 if bid_2 and bid_3 else None
    ask_25 = (ask_2 + ask_3) / 2 if ask_2 and ask_3 else None

    return {
        "mid_price":          mid_price,
        "best_bid":           None,
        "best_ask":           None,
        "spread_bps":         None,
        "depth_coverage_pct": 5.0 if bid_5 and ask_5 else None,
        "bid_qty_1pct":       bid_1,
        "ask_qty_1pct":       ask_1,
        "bid_levels_1pct":    None,
        "ask_levels_1pct":    None,
        "imbalance_1pct":     imb(bid_1, ask_1) if bid_1 and ask_1 else None,
        "bid_qty_2_5pct":     bid_25,
        "ask_qty_2_5pct":     ask_25,
        "bid_levels_2_5pct":  None,
        "ask_levels_2_5pct":  None,
        "imbalance_2_5pct":   imb(bid_25, ask_25) if bid_25 and ask_25 else None,
        "bid_qty_5pct":       bid_5,
        "ask_qty_5pct":       ask_5,
        "bid_levels_5pct":    None,
        "ask_levels_5pct":    None,
        "imbalance_5pct":     imb(bid_5, ask_5) if bid_5 and ask_5 else None,
        "bid_qty_10pct":      None,
        "ask_qty_10pct":      None,
        "bid_levels_10pct":   None,
        "ask_levels_10pct":   None,
        "imbalance_10pct":    None,
    }


def backfill_binance(
    conn,
    symbol: str,
    base_asset: str,
    start_date: Optional[date],
    end_date: date,
) -> None:
    if start_date is None:
        start_date = _first_available_date_binance(symbol)
        if start_date is None:
            log.info("binance %s: no data available, skipping", symbol)
            return
        log.info("binance %s: earliest date detected → %s", symbol, start_date)

    total_snaps = 0
    for day in date_range(start_date, end_date):
        depth_url  = f"{BINANCE_BASE}/{symbol}/{symbol}-bookDepth-{day}.zip"
        klines_url = f"{BINANCE_KLINES_BASE}/{symbol}/1m/{symbol}-1m-{day}.zip"

        depth_content  = _download(depth_url)
        klines_content = _download(klines_url)

        if depth_content is None:
            log.debug("binance: no bookDepth for %s %s", symbol, day)
            continue

        try:
            depth_snaps = _parse_binance_bookdepth(depth_content)
        except Exception as e:
            log.warning("binance bookDepth parse error %s %s: %s", symbol, day, e)
            continue

        klines = []
        if klines_content:
            try:
                klines = _parse_binance_klines_1m(klines_content)
            except Exception as e:
                log.warning("binance klines parse error %s %s: %s", symbol, day, e)

        rows = []
        for snap_h in BACKFILL_HOURS:
            boundary = datetime(day.year, day.month, day.day, snap_h, 0, 0, tzinfo=timezone.utc)

            snap = _nearest(depth_snaps, boundary, max_secs=1800)
            if snap is None:
                continue
            _, bands = snap

            mid_price = None
            if klines:
                kline = _nearest(klines, boundary, max_secs=300)
                if kline:
                    mid_price = kline[1]

            m = compute_metrics_from_bands(mid_price, bands)
            rows.append(_metrics_to_row(boundary, symbol, "binance", base_asset, "futures", m))

        if rows:
            upsert_snapshots(conn, rows)
            upsert_daily(conn, day, symbol, "binance", "futures")
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
    start_date: Optional[date],
    end_date: date,
) -> None:
    if start_date is None:
        start_date = date(2020, 1, 1)  # Bybit orderBook_200 historical start
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
                            rows.append(_metrics_to_row(snap_ts, symbol, "bybit", base_asset, "futures", m))
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
                    rows.append(_metrics_to_row(snap_ts, symbol, "bybit", base_asset, "futures", m))
                    snap_days.add(snap_ts.date())
            snap_idx += 1

        if rows:
            upsert_snapshots(conn, rows)
            for day in snap_days:
                upsert_daily(conn, day, symbol, "bybit", "futures")
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
    log.info("resolving exchange symbols for %d assets…", len(bases))
    return resolve_exchange_symbols(bases)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Historical order book backfill")
    parser.add_argument("--exchange", required=True, choices=["binance", "bybit"],
                        help="Exchange to backfill (okx: no free portal available)")
    parser.add_argument("--start", default=None,
                        help="Start date (YYYY-MM-DD). Omit to auto-detect earliest available data.")
    parser.add_argument("--end",   default=None,  help="End date (YYYY-MM-DD), defaults to yesterday")
    parser.add_argument("--symbols", default=None,
                        help="Comma-separated base assets (e.g. BTC,ETH). Defaults to all in DB.")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)

    start_date = date.fromisoformat(args.start) if args.start else None
    end_date   = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)
    symbols    = [s.strip().upper() for s in args.symbols.split(",")] if args.symbols else None

    if start_date and start_date > end_date:
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
