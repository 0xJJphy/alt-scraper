#!/usr/bin/env python3
"""
realtime_daemon.py - Intraday snapshot daemon for futures_latest table.

Polls Binance, Bybit and OKX native REST APIs every POLL_INTERVAL seconds
and upserts the results into the `futures_latest` table in Supabase.

This solves the "today = 0" problem: the nightly batch scraper only runs
3 times a day, so current-day rows in futures_daily_metrics are incomplete.
The frontend uses futures_latest for live OI, L/S and funding values when
computing metrics like OI 7D delta or Vol/OI.

Usage:
    python realtime_daemon.py [--top N] [--exchanges E1,E2] [--interval SEC] [--once]

Environment variables (from .env):
    DATABASE_URL         PostgreSQL connection string (required)
    TELEGRAM_BOT_TOKEN   Telegram bot token for error alerts (optional)
    TELEGRAM_CHAT_ID     Telegram chat ID for error alerts (optional)
"""

import os
import sys
import time
import argparse
import logging
import requests
import psycopg2
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Union
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

UTC = timezone.utc

BINANCE_FUTURES_API = "https://fapi.binance.com"
BYBIT_V5_API        = "https://api.bybit.com/v5"
OKX_V5_API          = "https://www.okx.com/api/v5"

DEFAULT_POLL_INTERVAL = 900   # 15 minutes
DEFAULT_EXCHANGES     = ["binance", "bybit", "okx"]

HEADERS = {"User-Agent": "alt-scraper/realtime-daemon"}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")


def _telegram(msg: str) -> None:
    """Send a Telegram message. Silently no-ops if credentials are not set."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception:
        pass


# ==============================================================================
# Helpers
# ==============================================================================

def _get(url: str, params: dict = None, timeout: int = 10) -> Optional[Union[dict, list]]:
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.debug("GET %s failed: %s", url, e)
        return None


def _safe_float(val) -> Optional[float]:
    try:
        f = float(val)
        return None if (f != f or f == float("inf") or f == float("-inf")) else f
    except (TypeError, ValueError):
        return None


class RateLimiter:
    """Thread-safe minimum-spacing limiter for one exchange's REST calls."""
    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self._lock = threading.Lock()
        self._last_call = 0.0

    def wait(self) -> None:
        if self.min_interval <= 0:
            return
        with self._lock:
            now = time.monotonic()
            gap = self.min_interval - (now - self._last_call)
            if gap > 0:
                time.sleep(gap)
            self._last_call = time.monotonic()


def _limited_get(limiter: RateLimiter, url: str, params: dict = None, timeout: int = 10):
    limiter.wait()
    return _get(url, params=params, timeout=timeout)


def _chunks(items: List[Tuple[str, str]], size: int) -> List[List[Tuple[str, str]]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def resolve_exchange_symbols(base_assets: List[str]) -> Dict[str, List[Tuple[str, str]]]:
    """Resolve only currently-listed USDT perpetual symbols per exchange."""
    resolved = {"binance": [], "bybit": [], "okx": []}

    # Binance USDT-M futures: {BASE}USDT or 1000{BASE}USDT for some meme contracts.
    b_data = _get(f"{BINANCE_FUTURES_API}/fapi/v1/exchangeInfo", timeout=20)
    b_syms = {
        s["symbol"]
        for s in (b_data or {}).get("symbols", [])
        if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"
    }

    # Bybit linear contracts.
    by_data = _get(
        f"{BYBIT_V5_API}/market/instruments-info",
        {"category": "linear", "limit": 1000},
        timeout=20,
    )
    by_syms = {
        s["symbol"]
        for s in (by_data or {}).get("result", {}).get("list", [])
        if s.get("quoteCoin") == "USDT" and s.get("status") == "Trading"
    }

    # OKX USDT swaps. Map ctValCcy -> instId.
    okx_data = _get(
        f"{OKX_V5_API}/public/instruments",
        {"instType": "SWAP"},
        timeout=20,
    )
    okx_syms = {
        s["ctValCcy"]: s["instId"]
        for s in (okx_data or {}).get("data", [])
        if s.get("settleCcy") == "USDT" and s.get("state") == "live"
    }

    for base in base_assets:
        for pfx in ("", "1000"):
            sym = f"{pfx}{base}USDT"
            if sym in b_syms:
                resolved["binance"].append((base, sym))
                break
        for pfx in ("", "1000"):
            sym = f"{pfx}{base}USDT"
            if sym in by_syms:
                resolved["bybit"].append((base, sym))
                break
        inst = okx_syms.get(base)
        if inst:
            resolved["okx"].append((base, inst))

    return resolved


# ==============================================================================
# Exchange fetchers
# ==============================================================================

class BinanceFetcher:
    """Fetches OI, funding, L/S ratios and price for Binance USDT perpetuals."""

    EXCHANGE = "binance"
    CHUNK_SIZE = 10
    MAX_WORKERS = 6
    REQUEST_INTERVAL = 0.04

    def fetch(self, symbols: List[Tuple[str, str]]) -> List[dict]:
        limiter = RateLimiter(self.REQUEST_INTERVAL)

        def fetch_one(base: str, sym: str) -> dict:
            polled_at = datetime.now(UTC)
            row = {"symbol": sym, "exchange": self.EXCHANGE, "base_asset": base, "polled_at": polled_at}

            # Mark price + predicted funding (fetch first — needed for OI USD calc)
            pm = _limited_get(limiter, f"{BINANCE_FUTURES_API}/fapi/v1/premiumIndex", {"symbol": sym})
            if pm:
                row["price"]        = _safe_float(pm.get("markPrice"))
                row["pred_funding"] = _safe_float(pm.get("lastFundingRate"))

            # Open Interest — returned in base asset, convert to USD using mark price
            oi_data = _limited_get(limiter, f"{BINANCE_FUTURES_API}/fapi/v1/openInterest", {"symbol": sym})
            if oi_data:
                oi_base = _safe_float(oi_data.get("openInterest"))
                price   = row.get("price")
                if oi_base and price:
                    row["oi_usd"] = oi_base * price

            # Current funding rate (from fundingRate endpoint)
            fr = _limited_get(limiter, f"{BINANCE_FUTURES_API}/fapi/v1/fundingRate", {"symbol": sym, "limit": 1})
            if fr and isinstance(fr, list) and fr:
                row["funding"] = _safe_float(fr[0].get("fundingRate"))

            # L/S ratios
            r_gl = _limited_get(limiter, f"{BINANCE_FUTURES_API}/futures/data/globalLongShortAccountRatio",
                                {"symbol": sym, "period": "5m", "limit": 1})
            if r_gl and isinstance(r_gl, list) and r_gl:
                row["ls_acc_global"] = _safe_float(r_gl[0].get("longShortRatio"))

            r_ta = _limited_get(limiter, f"{BINANCE_FUTURES_API}/futures/data/topLongShortAccountRatio",
                                {"symbol": sym, "period": "5m", "limit": 1})
            if r_ta and isinstance(r_ta, list) and r_ta:
                row["ls_acc_top"] = _safe_float(r_ta[0].get("longShortRatio"))

            r_tp = _limited_get(limiter, f"{BINANCE_FUTURES_API}/futures/data/topLongShortPositionRatio",
                                {"symbol": sym, "period": "5m", "limit": 1})
            if r_tp and isinstance(r_tp, list) and r_tp:
                row["ls_pos_top"] = _safe_float(r_tp[0].get("longShortRatio"))

            return row

        return self._fetch_parallel(symbols, fetch_one)

    def _fetch_parallel(self, symbols, fetch_one):
        rows = []
        chunks = _chunks(symbols, self.CHUNK_SIZE)
        with ThreadPoolExecutor(max_workers=min(self.MAX_WORKERS, len(chunks) or 1)) as executor:
            futures = [executor.submit(lambda c: [fetch_one(base, sym) for base, sym in c], chunk) for chunk in chunks]
            for future in as_completed(futures):
                rows.extend(future.result())
        return rows


class BybitFetcher:
    """Fetches OI, funding and price for Bybit USDT linear perpetuals."""

    EXCHANGE = "bybit"
    CHUNK_SIZE = 8
    MAX_WORKERS = 5
    REQUEST_INTERVAL = 0.05

    def fetch(self, symbols: List[Tuple[str, str]]) -> List[dict]:
        limiter = RateLimiter(self.REQUEST_INTERVAL)

        def fetch_one(base: str, sym: str) -> dict:
            polled_at = datetime.now(UTC)
            row = {"symbol": sym, "exchange": self.EXCHANGE, "base_asset": base, "polled_at": polled_at}

            # Ticker (price + predicted funding)
            tickers = _limited_get(limiter, f"{BYBIT_V5_API}/market/tickers",
                                   {"category": "linear", "symbol": sym})
            if tickers:
                result = tickers.get("result", {}).get("list", [])
                if result:
                    t = result[0]
                    row["price"]        = _safe_float(t.get("markPrice"))
                    row["pred_funding"] = _safe_float(t.get("fundingRate"))
                    # OI in base asset — convert to USD
                    oi_base = _safe_float(t.get("openInterest"))
                    price   = row.get("price")
                    if oi_base and price:
                        row["oi_usd"] = oi_base * price

            # Latest funding rate paid
            funding_hist = _limited_get(limiter, f"{BYBIT_V5_API}/market/funding/history",
                                        {"category": "linear", "symbol": sym, "limit": 1})
            if funding_hist:
                flist = funding_hist.get("result", {}).get("list", [])
                if flist:
                    row["funding"] = _safe_float(flist[0].get("fundingRate"))

            # L/S ratio (account ratio)
            ls = _limited_get(limiter, f"{BYBIT_V5_API}/market/account-ratio",
                              {"category": "linear", "symbol": sym, "period": "5min", "limit": 1})
            if ls:
                llist = ls.get("result", {}).get("list", [])
                if llist:
                    buy  = _safe_float(llist[0].get("buyRatio"))
                    sell = _safe_float(llist[0].get("sellRatio"))
                    if buy and sell and sell > 0:
                        row["ls_acc_global"] = buy / sell

            return row

        return self._fetch_parallel(symbols, fetch_one)

    def _fetch_parallel(self, symbols, fetch_one):
        rows = []
        chunks = _chunks(symbols, self.CHUNK_SIZE)
        with ThreadPoolExecutor(max_workers=min(self.MAX_WORKERS, len(chunks) or 1)) as executor:
            futures = [executor.submit(lambda c: [fetch_one(base, sym) for base, sym in c], chunk) for chunk in chunks]
            for future in as_completed(futures):
                rows.extend(future.result())
        return rows


class OKXFetcher:
    """Fetches OI, funding and L/S ratios for OKX USDT swap perpetuals."""

    EXCHANGE = "okx"
    CHUNK_SIZE = 8
    MAX_WORKERS = 5
    REQUEST_INTERVAL = 0.06

    def fetch(self, symbols: List[Tuple[str, str]]) -> List[dict]:
        limiter = RateLimiter(self.REQUEST_INTERVAL)

        def fetch_one(base: str, inst: str) -> dict:
            polled_at = datetime.now(UTC)
            sym  = f"{base}USDT"
            row  = {"symbol": sym, "exchange": self.EXCHANGE, "base_asset": base, "polled_at": polled_at}

            # Ticker (price)
            ticker = _limited_get(limiter, f"{OKX_V5_API}/market/ticker", {"instId": inst})
            if ticker:
                data = ticker.get("data", [])
                if data:
                    row["price"] = _safe_float(data[0].get("markPx") or data[0].get("last"))

            # Open Interest (in USD — OKX returns in contracts, each = 1 USD for USDT swaps)
            oi = _limited_get(limiter, f"{OKX_V5_API}/public/open-interest", {"instId": inst})
            if oi:
                data = oi.get("data", [])
                if data:
                    # oiUsd is directly in USD value
                    row["oi_usd"] = _safe_float(data[0].get("oiUsd"))

            # Funding rate
            fr = _limited_get(limiter, f"{OKX_V5_API}/public/funding-rate", {"instId": inst})
            if fr:
                data = fr.get("data", [])
                if data:
                    row["funding"]      = _safe_float(data[0].get("fundingRate"))
                    row["pred_funding"] = _safe_float(data[0].get("nextFundingRate"))

            # L/S ratios (Rubik stats — by base currency, not instId)
            params = {"ccy": base.upper(), "period": "5m"}
            r_gl = _limited_get(limiter, f"{OKX_V5_API}/rubik/stat/contracts/long-short-account-ratio", params)
            if r_gl:
                d = r_gl.get("data", [])
                if d:
                    row["ls_acc_global"] = _safe_float(d[0][1])

            r_ta = _limited_get(limiter, f"{OKX_V5_API}/rubik/stat/contracts/top-traders-long-short-account-ratio", params)
            if r_ta:
                d = r_ta.get("data", [])
                if d:
                    row["ls_acc_top"] = _safe_float(d[0][1])

            r_tp = _limited_get(limiter, f"{OKX_V5_API}/rubik/stat/contracts/top-traders-long-short-position-ratio", params)
            if r_tp:
                d = r_tp.get("data", [])
                if d:
                    row["ls_pos_top"] = _safe_float(d[0][1])

            return row

        return self._fetch_parallel(symbols, fetch_one)

    def _fetch_parallel(self, symbols, fetch_one):
        rows = []
        chunks = _chunks(symbols, self.CHUNK_SIZE)
        with ThreadPoolExecutor(max_workers=min(self.MAX_WORKERS, len(chunks) or 1)) as executor:
            futures = [executor.submit(lambda c: [fetch_one(base, sym) for base, sym in c], chunk) for chunk in chunks]
            for future in as_completed(futures):
                rows.extend(future.result())
        return rows


# ==============================================================================
# Database
# ==============================================================================

UPSERT_SQL = """
INSERT INTO futures_latest (
    symbol, exchange, base_asset,
    oi_usd, funding, pred_funding,
    ls_acc_global, ls_acc_top, ls_pos_top,
    price, polled_at
)
VALUES %s
ON CONFLICT (symbol, exchange) DO UPDATE SET
    base_asset    = EXCLUDED.base_asset,
    oi_usd        = COALESCE(EXCLUDED.oi_usd,        futures_latest.oi_usd),
    funding       = COALESCE(EXCLUDED.funding,       futures_latest.funding),
    pred_funding  = COALESCE(EXCLUDED.pred_funding,  futures_latest.pred_funding),
    ls_acc_global = COALESCE(EXCLUDED.ls_acc_global, futures_latest.ls_acc_global),
    ls_acc_top    = COALESCE(EXCLUDED.ls_acc_top,    futures_latest.ls_acc_top),
    ls_pos_top    = COALESCE(EXCLUDED.ls_pos_top,    futures_latest.ls_pos_top),
    price         = COALESCE(EXCLUDED.price,         futures_latest.price),
    polled_at     = EXCLUDED.polled_at,
    updated_at    = NOW()
"""

UPSERT_TEMPLATE = "(%(symbol)s, %(exchange)s, %(base_asset)s, %(oi_usd)s, %(funding)s, %(pred_funding)s, %(ls_acc_global)s, %(ls_acc_top)s, %(ls_pos_top)s, %(price)s, %(polled_at)s)"

SNAPSHOT_SQL = """
INSERT INTO futures_snapshots (
    snapshot_at, symbol, exchange, base_asset,
    oi_usd, funding, ls_acc_global, ls_acc_top, ls_pos_top, price
)
VALUES %s
ON CONFLICT DO NOTHING
"""
SNAPSHOT_TEMPLATE = "(%(snapshot_at)s, %(symbol)s, %(exchange)s, %(base_asset)s, %(oi_usd)s, %(funding)s, %(ls_acc_global)s, %(ls_acc_top)s, %(ls_pos_top)s, %(price)s)"

SNAPSHOT_KEYS = ["snapshot_at", "symbol", "exchange", "base_asset",
                 "oi_usd", "funding", "ls_acc_global", "ls_acc_top", "ls_pos_top", "price"]

INTRADAY_SNAPSHOT_SQL = """
INSERT INTO futures_intraday_snapshots (
    snapshot_at, symbol, exchange, base_asset,
    oi_usd, funding, pred_funding, ls_acc_global, ls_acc_top, ls_pos_top,
    price, polled_at
)
VALUES %s
ON CONFLICT DO NOTHING
"""

INTRADAY_SNAPSHOT_TEMPLATE = "(%(snapshot_at)s, %(symbol)s, %(exchange)s, %(base_asset)s, %(oi_usd)s, %(funding)s, %(pred_funding)s, %(ls_acc_global)s, %(ls_acc_top)s, %(ls_pos_top)s, %(price)s, %(polled_at)s)"

INTRADAY_SNAPSHOT_KEYS = [
    "snapshot_at", "symbol", "exchange", "base_asset", "oi_usd",
    "funding", "pred_funding", "ls_acc_global", "ls_acc_top",
    "ls_pos_top", "price", "polled_at",
]

SNAPSHOT_HOURS = {0, 6, 12, 18}


def write_snapshots(db_url: str, rows: List[dict], snapshot_at: datetime):
    if not rows:
        return
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cur  = conn.cursor()
        records = [{**{k: r.get(k) for k in SNAPSHOT_KEYS}, "snapshot_at": snapshot_at} for r in rows]
        execute_values(cur, SNAPSHOT_SQL, records, template=SNAPSHOT_TEMPLATE, page_size=500)
        conn.commit()
        log.info("Wrote %d rows to futures_snapshots (slot %02d:00 UTC).", len(records), snapshot_at.hour)
    except Exception as e:
        log.error("Snapshot write failed: %s", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def write_intraday_snapshots(db_url: str, rows: List[dict], snapshot_at: datetime):
    """Persist every realtime poll for the last-48h Paper Live execution audit."""
    if not rows:
        return
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        records = [{**{k: r.get(k) for k in INTRADAY_SNAPSHOT_KEYS}, "snapshot_at": snapshot_at} for r in rows]
        execute_values(cur, INTRADAY_SNAPSHOT_SQL, records, template=INTRADAY_SNAPSHOT_TEMPLATE, page_size=500)
        cur.execute("DELETE FROM futures_intraday_snapshots WHERE snapshot_at < NOW() - INTERVAL '48 hours'")
        conn.commit()
        log.info("Wrote %d rows to futures_intraday_snapshots (48h retention).", len(records))
    except Exception as e:
        log.error("Intraday snapshot write failed: %s", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def upsert_rows(db_url: str, rows: List[dict]):
    if not rows:
        return
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cur  = conn.cursor()
        # Ensure every row has all keys (None for missing)
        keys = ["symbol", "exchange", "base_asset", "oi_usd", "funding",
                "pred_funding", "ls_acc_global", "ls_acc_top", "ls_pos_top",
                "price", "polled_at"]
        records = [{k: r.get(k) for k in keys} for r in rows]
        execute_values(cur, UPSERT_SQL, records, template=UPSERT_TEMPLATE, page_size=500)
        conn.commit()
        log.info("Upserted %d rows into futures_latest.", len(records))
    except Exception as e:
        log.error("DB upsert failed: %s", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


# ==============================================================================
# Asset loader
# ==============================================================================

def load_top_assets(db_url: str, top_n: Optional[int], top_active: Optional[int] = None) -> List[str]:
    """
    Return base_assets to poll, ordered by current market_cap_rank.

    By default (top_active=None) loads ALL assets that have EVER been in the
    top-N (ever_in_top_50=true) — this is the full tracked universe, ensuring
    no survivor bias in the realtime data collection.

    When --top-active N is set, restricts to only the top N assets by their
    current market_cap_rank. Useful to reduce load if the full universe causes
    rate-limit pressure on the exchanges.
    """
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cur  = conn.cursor()

        if top_active is not None:
            # Restricted mode: only top N currently active assets
            log.info("Asset load mode: TOP-ACTIVE %d (current rank only)", top_active)
            cur.execute("""
                SELECT symbol FROM asset_metadata
                WHERE (is_filtered = false OR is_filtered IS NULL)
                ORDER BY market_cap_rank ASC NULLS LAST
                LIMIT %s
            """, (top_active,))
        elif top_n is not None:
            # Legacy explicit limit
            log.info("Asset load mode: TOP %d (legacy limit)", top_n)
            cur.execute("""
                SELECT symbol FROM asset_metadata
                WHERE (is_filtered = false OR is_filtered IS NULL)
                ORDER BY market_cap_rank ASC NULLS LAST
                LIMIT %s
            """, (top_n,))
        else:
            # Default: full tracked universe — all assets that were ever in top-N
            log.info("Asset load mode: FULL UNIVERSE (ever_in_top_50 = true)")
            cur.execute("""
                SELECT symbol FROM asset_metadata
                WHERE (is_filtered = false OR is_filtered IS NULL)
                  AND (ever_in_top_50 = true OR market_cap_rank IS NOT NULL)
                ORDER BY market_cap_rank ASC NULLS LAST
            """)

        assets = [row[0] for row in cur.fetchall()]
        log.info("Loaded %d assets to poll.", len(assets))
        return assets
    except Exception as e:
        log.error("Failed to load assets: %s", e)
        return []
    finally:
        if conn:
            conn.close()


# ==============================================================================
# Main daemon loop
# ==============================================================================

FETCHER_MAP = {
    "binance": BinanceFetcher,
    "bybit":   BybitFetcher,
    "okx":     OKXFetcher,
}


def run_once(db_url: str, top_n: Optional[int], exchanges: List[str], top_active: Optional[int] = None):
    assets = load_top_assets(db_url, top_n, top_active=top_active)
    if not assets:
        log.warning("No assets loaded — skipping poll cycle.")
        _telegram("⚠️ *alt-scraper-realtime*: no assets loaded from metadata — poll skipped.")
        return

    resolved = resolve_exchange_symbols(assets)
    for ex in exchanges:
        log.info(
            "Resolved %s symbols: %d/%d assets supported",
            ex,
            len(resolved.get(ex, [])),
            len(assets),
        )

    now = datetime.now(UTC)
    failed_exchanges: List[str] = []

    def _fetch(ex: str) -> List[dict]:
        cls = FETCHER_MAP.get(ex)
        if not cls:
            log.warning("Unknown exchange: %s", ex)
            return []
        symbols = resolved.get(ex, [])
        if not symbols:
            log.warning("No supported symbols resolved for %s — skipping.", ex)
            return []
        log.info("Polling %s for %d supported symbols...", ex, len(symbols))
        rows = cls().fetch(symbols)
        log.info("  %s → %d rows fetched.", ex, len(rows))
        return rows

    all_rows = []
    with ThreadPoolExecutor(max_workers=len(exchanges)) as executor:
        futures = {executor.submit(_fetch, ex): ex for ex in exchanges}
        for future in as_completed(futures):
            ex = futures[future]
            try:
                rows = future.result()
                if not rows:
                    log.error("  %s returned 0 rows — exchange may be down.", ex)
                    failed_exchanges.append(ex)
                all_rows.extend(rows)
            except Exception as e:
                log.error("  %s fetch error: %s", ex, e)
                failed_exchanges.append(ex)

    if failed_exchanges:
        _telegram(
            f"⚠️ *alt-scraper-realtime*: exchange(s) returned 0 rows\n"
            f"*Failed:* {', '.join(failed_exchanges)}\n"
            f"*Time:* {now.strftime('%Y-%m-%d %H:%M UTC')}"
        )

    upsert_rows(db_url, all_rows)
    write_intraday_snapshots(db_url, all_rows, now.replace(second=0, microsecond=0))

    if now.hour in SNAPSHOT_HOURS:
        write_snapshots(db_url, all_rows, now.replace(minute=0, second=0, microsecond=0))


def main():
    parser = argparse.ArgumentParser(description="Futures latest snapshot daemon")
    parser.add_argument("--top",        type=int,   default=None,                help="(Legacy) Limit to top N assets by rank")
    parser.add_argument("--top-active",  type=int,   default=None,  dest="top_active",
                        help="Restrict polling to only the top N currently-active assets by market_cap_rank. "
                             "Default: off — polls the full tracked universe (all assets ever in top-N).")
    parser.add_argument("--exchanges",  type=str,   default=",".join(DEFAULT_EXCHANGES), help="Comma-separated exchange list")
    parser.add_argument("--interval",   type=int,   default=DEFAULT_POLL_INTERVAL, help="Poll interval in seconds (default 900)")
    parser.add_argument("--once",       action="store_true", help="Run one poll cycle then exit (useful for testing)")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL environment variable is not set.")
        sys.exit(1)

    exchanges = [e.strip().lower() for e in args.exchanges.split(",") if e.strip()]

    top_label = str(args.top) if args.top else (f"top-active-{args.top_active}" if args.top_active else "full-universe")
    log.info("Starting realtime_daemon — exchanges=%s, mode=%s, interval=%ds", exchanges, top_label, args.interval)

    if args.once:
        run_once(db_url, args.top, exchanges, top_active=args.top_active)
        return

    while True:
        start = time.monotonic()
        try:
            run_once(db_url, args.top, exchanges, top_active=args.top_active)
        except Exception as e:
            log.error("Unexpected error in poll cycle: %s", e)
            _telegram(f"🔴 *alt-scraper-realtime*: unexpected error in poll cycle\n`{e}`")
        elapsed = time.monotonic() - start
        sleep_for = max(0, args.interval - elapsed)
        log.info("Poll cycle done in %.1fs. Next in %.0fs.", elapsed, sleep_for)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
