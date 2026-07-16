#!/usr/bin/env python3
"""
klines_15m_backfill.py — Historical 15m futures kline backfill.

For each (base_asset, exchange, symbol) already tracked by futures_ws_daemon.py,
walks backward in time from the earliest candle currently stored in
futures_klines_15m (or from now, if none stored yet) until the exchange's REST
API stops returning older data. This empirically discovers each exchange's real
historical depth — Binance/Bybit are expected to reach each symbol's listing
date; OKX's history-candles endpoint is known to have a much shorter retention
window for sub-1h bars, so it will simply stop early.

Idempotent: reuses futures_ws_daemon.write_klines_15m (ON CONFLICT DO UPDATE),
so re-running is safe and resumes automatically from wherever it left off.

Usage:
    python klines_15m_backfill.py --exchange all
    python klines_15m_backfill.py --exchange binance
    python klines_15m_backfill.py --exchange okx --bases BTC,ETH
    python klines_15m_backfill.py --exchange bybit --dry-run
"""

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Optional, Tuple

import psycopg2
from dotenv import load_dotenv

from realtime_daemon import (
    BYBIT_V5_API,
    OKX_V5_API,
    KLINE_INTERVAL_MS,
    UTC,
    RateLimiter,
    _limited_binance_get,
    _limited_get,
    _safe_float,
    load_top_assets,
    resolve_exchange_symbols,
)
from futures_ws_daemon import write_klines_15m

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("klines_backfill")

DEFAULT_EXCHANGES = ["binance", "bybit", "okx"]

# Safety floor so a pagination bug can't loop forever — well before any USDT
# perpetual listing (Binance Futures itself launched Sept 2019).
MIN_OPEN_MS = int(datetime(2017, 1, 1, tzinfo=UTC).timestamp() * 1000)

REQUEST_INTERVAL = {"binance": 0.15, "bybit": 0.12, "okx": 0.15}
DEFAULT_WORKERS = {"binance": 4, "bybit": 4, "okx": 3}

# Binance klines weight scales with `limit`: <=100 -> 1, <=500 -> 2, <=1000 -> 5,
# >1000 -> 10. The shared IP also serves the always-on realtime/ws daemons, so
# stay in the cheap weight-2 tier instead of maxing out limit=1500 (weight 10).
BINANCE_PAGE_LIMIT = 500


def _retry_fetch(description: str, fetch_fn, max_retries: int = 6, base_delay: float = 1.0):
    """Retry a raw API call (which returns None on failure) with backoff.

    Returns the raw payload, or None if still failing after max_retries — the
    caller must NOT treat that as "no more history", only as "try again later".
    """
    delay = base_delay
    for attempt in range(max_retries):
        data = fetch_fn()
        if data is not None:
            return data
        log.warning("%s: request failed (attempt %d/%d), retrying in %.1fs",
                    description, attempt + 1, max_retries, delay)
        time.sleep(delay)
        delay = min(delay * 2, 30)
    log.error("%s: giving up after %d retries", description, max_retries)
    return None


def _earliest_stored_ms(db_url: str, symbol: str, exchange: str) -> Optional[int]:
    conn = psycopg2.connect(db_url)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT EXTRACT(EPOCH FROM MIN(candle_open_at)) * 1000 "
            "FROM futures_klines_15m WHERE symbol = %s AND exchange = %s",
            (symbol, exchange),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None
    finally:
        conn.close()


def _row(base, symbol, exchange, open_ms, o, h, l, c, vol_base, vol_usd,
         txn_count=None, buy_vol=None) -> dict:
    sell_vol = (vol_base - buy_vol) if vol_base is not None and buy_vol is not None else None
    return {
        "candle_open_at": datetime.fromtimestamp(open_ms / 1000, UTC),
        "candle_close_at": datetime.fromtimestamp((open_ms + KLINE_INTERVAL_MS) / 1000, UTC),
        "symbol": symbol,
        "exchange": exchange,
        "base_asset": base,
        "price_open": o,
        "price_high": h,
        "price_low": l,
        "price_close": c,
        "volume_base": vol_base,
        "volume_usd": vol_usd,
        "buy_volume_base": buy_vol,
        "sell_volume_base": sell_vol,
        "volume_delta": (buy_vol - sell_vol) if buy_vol is not None and sell_vol is not None else None,
        "txn_count": txn_count,
        "polled_at": datetime.now(UTC),
        "source": "backfill",
        "ws_received_at": None,
        "rest_reconciled_at": None,
        "exchange_event_time": None,
    }


# ---------------------------------------------------------------------------
# Binance — GET /fapi/v1/klines, endTime-bounded, ascending, max 1500/page.
# ---------------------------------------------------------------------------

def backfill_pair_binance(db_url: str, limiter: RateLimiter, base: str, symbol: str,
                           dry_run: bool = False) -> int:
    cursor_ms = _earliest_stored_ms(db_url, symbol, "binance")
    cursor_ms = (cursor_ms - 1) if cursor_ms else int(time.time() * 1000)
    total = 0
    while cursor_ms > MIN_OPEN_MS:
        data = _retry_fetch(
            f"binance {symbol} endTime={cursor_ms}",
            lambda: _limited_binance_get(
                limiter, "/fapi/v1/klines",
                {"symbol": symbol, "interval": "15m", "endTime": cursor_ms, "limit": BINANCE_PAGE_LIMIT},
            ),
        )
        if data is None:
            log.error("binance %s: stopping after repeated failures (resumable on next run)", symbol)
            break
        candles = data if isinstance(data, list) else []
        if not candles:
            break
        rows = []
        for k in candles:
            vol_base = _safe_float(k[5])
            buy_vol = _safe_float(k[9]) if len(k) > 9 else None
            rows.append(_row(
                base, symbol, "binance", int(k[0]),
                _safe_float(k[1]), _safe_float(k[2]), _safe_float(k[3]), _safe_float(k[4]),
                vol_base, _safe_float(k[7]) if len(k) > 7 else None,
                int(k[8]) if len(k) > 8 and k[8] is not None else None, buy_vol,
            ))
        write_klines_15m(db_url, rows, dry_run)
        total += len(rows)
        oldest_ms = min(int(k[0]) for k in candles)
        if oldest_ms >= cursor_ms:
            break
        cursor_ms = oldest_ms - 1
    log.info("binance %-16s backfilled %d candles", symbol, total)
    return total


# ---------------------------------------------------------------------------
# Bybit v5 — GET /v5/market/kline, end-bounded, max 1000/page.
# ---------------------------------------------------------------------------

def backfill_pair_bybit(db_url: str, limiter: RateLimiter, base: str, symbol: str,
                         dry_run: bool = False) -> int:
    cursor_ms = _earliest_stored_ms(db_url, symbol, "bybit")
    cursor_ms = (cursor_ms - 1) if cursor_ms else int(time.time() * 1000)
    total = 0
    while cursor_ms > MIN_OPEN_MS:
        data = _retry_fetch(
            f"bybit {symbol} end={cursor_ms}",
            lambda: _limited_get(
                limiter, f"{BYBIT_V5_API}/market/kline",
                {"category": "linear", "symbol": symbol, "interval": "15", "end": cursor_ms, "limit": 1000},
            ),
        )
        if data is None:
            log.error("bybit %s: stopping after repeated failures (resumable on next run)", symbol)
            break
        candles = (data or {}).get("result", {}).get("list", []) if isinstance(data, dict) else []
        if not candles:
            break
        rows = [
            _row(base, symbol, "bybit", int(k[0]),
                 _safe_float(k[1]), _safe_float(k[2]), _safe_float(k[3]), _safe_float(k[4]),
                 _safe_float(k[5]), _safe_float(k[6]) if len(k) > 6 else None)
            for k in candles
        ]
        write_klines_15m(db_url, rows, dry_run)
        total += len(rows)
        oldest_ms = min(int(k[0]) for k in candles)
        if oldest_ms >= cursor_ms:
            break
        cursor_ms = oldest_ms - 1
    log.info("bybit   %-16s backfilled %d candles", symbol, total)
    return total


# ---------------------------------------------------------------------------
# OKX — GET /market/history-candles, after-bounded, max 100/page.
# Expected to hit its retention limit much sooner than Binance/Bybit.
# ---------------------------------------------------------------------------

def backfill_pair_okx(db_url: str, limiter: RateLimiter, base: str, inst_id: str,
                       dry_run: bool = False) -> int:
    symbol = f"{base}USDT"
    cursor_ms = _earliest_stored_ms(db_url, symbol, "okx")
    cursor_ms = (cursor_ms - 1) if cursor_ms else int(time.time() * 1000)
    total = 0
    while cursor_ms > MIN_OPEN_MS:
        data = _retry_fetch(
            f"okx {symbol} after={cursor_ms}",
            lambda: _limited_get(
                limiter, f"{OKX_V5_API}/market/history-candles",
                {"instId": inst_id, "bar": "15m", "after": cursor_ms, "limit": 100},
            ),
        )
        if data is None:
            log.error("okx %s: stopping after repeated failures (resumable on next run)", symbol)
            break
        candles = (data or {}).get("data", []) if isinstance(data, dict) else []
        if not candles:
            break
        rows = [
            _row(base, symbol, "okx", int(k[0]),
                 _safe_float(k[1]), _safe_float(k[2]), _safe_float(k[3]), _safe_float(k[4]),
                 _safe_float(k[5]), _safe_float(k[7]) if len(k) > 7 else None)
            for k in candles
        ]
        write_klines_15m(db_url, rows, dry_run)
        total += len(rows)
        oldest_ms = min(int(k[0]) for k in candles)
        if oldest_ms >= cursor_ms:
            break
        cursor_ms = oldest_ms - 1
    log.info("okx     %-16s backfilled %d candles", symbol, total)
    return total


BACKFILL_FN = {
    "binance": backfill_pair_binance,
    "bybit": backfill_pair_bybit,
    "okx": backfill_pair_okx,
}


def run_exchange(db_url: str, exchange: str, pairs: List[Tuple[str, str]],
                  workers: int, dry_run: bool) -> int:
    limiter = RateLimiter(REQUEST_INTERVAL[exchange])
    fn = BACKFILL_FN[exchange]
    total = 0
    with ThreadPoolExecutor(max_workers=min(workers, len(pairs) or 1)) as executor:
        futures = {
            executor.submit(fn, db_url, limiter, base, symbol, dry_run): (base, symbol)
            for base, symbol in pairs
        }
        for future in as_completed(futures):
            base, symbol = futures[future]
            try:
                total += future.result()
            except Exception as e:
                log.error("%s %s failed: %s", exchange, symbol, e)
    log.info("=== %s complete: %d candles written across %d symbols ===", exchange, total, len(pairs))
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Historical 15m futures kline backfill")
    parser.add_argument("--exchange", default="all", choices=["all", "binance", "bybit", "okx"])
    parser.add_argument("--bases", default=None,
                         help="Comma-separated base assets; default: full tracked universe (ever_in_top_50)")
    parser.add_argument("--workers", type=int, default=None,
                         help="Parallel symbol workers per exchange (default: per-exchange tuned value)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch but don't write to the DB")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)

    bases = [b.strip().upper() for b in args.bases.split(",") if b.strip()] if args.bases else load_top_assets(db_url, None, top_active=None)
    if not bases:
        print("No base assets resolved", file=sys.stderr)
        sys.exit(1)

    resolved = resolve_exchange_symbols(bases)
    exchanges = DEFAULT_EXCHANGES if args.exchange == "all" else [args.exchange]

    grand_total = 0
    for exchange in exchanges:
        pairs = resolved.get(exchange, [])
        if not pairs:
            log.warning("%s: no symbols resolved, skipping", exchange)
            continue
        workers = args.workers or DEFAULT_WORKERS[exchange]
        log.info("--- %s: backfilling %d symbols (workers=%d) ---", exchange, len(pairs), workers)
        grand_total += run_exchange(db_url, exchange, pairs, workers, args.dry_run)

    log.info("Backfill finished. %d candles written in total.", grand_total)


if __name__ == "__main__":
    main()
