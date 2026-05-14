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
from datetime import datetime, timedelta, timezone
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
KLINE_INTERVAL_MS     = 15 * 60 * 1000
KLINE_SETTLE_DELAY_MS = 5_000
KLINE_SETTLE_DELAY_S  = KLINE_SETTLE_DELAY_MS / 1000

HEADERS = {"User-Agent": "alt-scraper/realtime-daemon"}

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# Historical CMC symbols may differ from current exchange tickers after renames.
_SYMBOL_ALIASES = {
    "MATIC": ["POL"],
    "RNDR":  ["RENDER"],
}
# Normalize old names to canonical before storing base_asset.
_SYMBOL_CANONICAL = {
    "MATIC": "POL",
    "RNDR":  "RENDER",
}


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


def _closed_kline_by_open_time(candles: List[list], now_ms: int, open_idx: int = 0) -> Optional[list]:
    """Return the most recent candle whose 15m window has fully closed."""
    cutoff_ms = now_ms - KLINE_SETTLE_DELAY_MS
    closed = []
    for candle in candles:
        try:
            open_ms = int(candle[open_idx])
        except (TypeError, ValueError, IndexError):
            continue
        if open_ms + KLINE_INTERVAL_MS <= cutoff_ms:
            closed.append((open_ms, candle))
    if not closed:
        return None
    return max(closed, key=lambda item: item[0])[1]


def _next_aligned_poll_time(now: datetime) -> datetime:
    """Next UTC 15m boundary plus settle delay for closed exchange klines."""
    now = now.astimezone(UTC)
    minute = (now.minute // 15 + 1) * 15
    if minute == 60:
        boundary = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        boundary = now.replace(minute=minute, second=0, microsecond=0)
    target = boundary + timedelta(seconds=KLINE_SETTLE_DELAY_S)
    if now >= target:
        target += timedelta(minutes=15)
    return target


def _sleep_until_aligned_poll() -> None:
    target = _next_aligned_poll_time(datetime.now(UTC))
    sleep_for = max(0.0, (target - datetime.now(UTC)).total_seconds())
    log.info("Next aligned 15m poll at %s UTC (in %.1fs).", target.strftime("%Y-%m-%d %H:%M:%S"), sleep_for)
    time.sleep(sleep_for)


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
        store_base = _SYMBOL_CANONICAL.get(base, base)
        candidates = [base] + _SYMBOL_ALIASES.get(base, [])

        for pfx in ("", "1000"):
            for candidate in candidates:
                sym = f"{pfx}{candidate}USDT"
                if sym in b_syms:
                    resolved["binance"].append((store_base, sym))
                    break
            else:
                continue
            break

        for pfx in ("", "1000"):
            for candidate in candidates:
                sym = f"{pfx}{candidate}USDT"
                if sym in by_syms:
                    resolved["bybit"].append((store_base, sym))
                    break
            else:
                continue
            break

        for candidate in candidates:
            inst = okx_syms.get(candidate)
            if inst:
                resolved["okx"].append((store_base, inst))
                break

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

    def fetch_klines_15m(self, symbols: List[Tuple[str, str]]) -> List[dict]:
        limiter = RateLimiter(self.REQUEST_INTERVAL)

        def fetch_one(base: str, sym: str) -> Optional[dict]:
            now_ms = int(time.time() * 1000)
            data = _limited_get(
                limiter,
                f"{BINANCE_FUTURES_API}/fapi/v1/klines",
                {"symbol": sym, "interval": "15m", "limit": 2},
            )
            if not data or not isinstance(data, list):
                return None
            k = _closed_kline_by_open_time(data, now_ms)
            if not k:
                return None
            volume_base = _safe_float(k[5])
            buy_volume_base = _safe_float(k[9]) if len(k) > 9 else None
            sell_volume_base = (
                volume_base - buy_volume_base
                if volume_base is not None and buy_volume_base is not None
                else None
            )
            return {
                "candle_open_at": datetime.fromtimestamp(int(k[0]) / 1000, UTC),
                "candle_close_at": datetime.fromtimestamp((int(k[0]) + KLINE_INTERVAL_MS) / 1000, UTC),
                "symbol": sym,
                "exchange": self.EXCHANGE,
                "base_asset": base,
                "price_open": _safe_float(k[1]),
                "price_high": _safe_float(k[2]),
                "price_low": _safe_float(k[3]),
                "price_close": _safe_float(k[4]),
                "volume_base": volume_base,
                "volume_usd": _safe_float(k[7]) if len(k) > 7 else None,
                "txn_count": int(k[8]) if len(k) > 8 and k[8] is not None else None,
                "buy_volume_base": buy_volume_base,
                "sell_volume_base": sell_volume_base,
                "volume_delta": (
                    buy_volume_base - sell_volume_base
                    if buy_volume_base is not None and sell_volume_base is not None
                    else None
                ),
                "polled_at": datetime.now(UTC),
            }

        return [row for row in self._fetch_parallel(symbols, fetch_one) if row]

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

    def fetch_klines_15m(self, symbols: List[Tuple[str, str]]) -> List[dict]:
        limiter = RateLimiter(self.REQUEST_INTERVAL)

        def fetch_one(base: str, sym: str) -> Optional[dict]:
            now_ms = int(time.time() * 1000)
            data = _limited_get(
                limiter,
                f"{BYBIT_V5_API}/market/kline",
                {"category": "linear", "symbol": sym, "interval": "15", "limit": 2},
            )
            candles = (data or {}).get("result", {}).get("list", []) if isinstance(data, dict) else []
            k = _closed_kline_by_open_time(candles, now_ms)
            if not k:
                return None
            volume_base = _safe_float(k[5])
            volume_usd = _safe_float(k[6]) if len(k) > 6 else None
            return {
                "candle_open_at": datetime.fromtimestamp(int(k[0]) / 1000, UTC),
                "candle_close_at": datetime.fromtimestamp((int(k[0]) + KLINE_INTERVAL_MS) / 1000, UTC),
                "symbol": sym,
                "exchange": self.EXCHANGE,
                "base_asset": base,
                "price_open": _safe_float(k[1]),
                "price_high": _safe_float(k[2]),
                "price_low": _safe_float(k[3]),
                "price_close": _safe_float(k[4]),
                "volume_base": volume_base,
                "volume_usd": volume_usd,
                "txn_count": None,
                "buy_volume_base": None,
                "sell_volume_base": None,
                "volume_delta": None,
                "polled_at": datetime.now(UTC),
            }

        return [row for row in self._fetch_parallel(symbols, fetch_one) if row]

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

    def fetch_klines_15m(self, symbols: List[Tuple[str, str]]) -> List[dict]:
        limiter = RateLimiter(self.REQUEST_INTERVAL)

        def fetch_one(base: str, inst: str) -> Optional[dict]:
            data = _limited_get(
                limiter,
                f"{OKX_V5_API}/market/candles",
                {"instId": inst, "bar": "15m", "limit": 3},
            )
            candles = (data or {}).get("data", []) if isinstance(data, dict) else []
            k = None
            for candle in candles:
                if len(candle) > 8 and str(candle[8]) == "1":
                    k = candle
                    break
            if not k:
                k = _closed_kline_by_open_time(candles, int(time.time() * 1000))
            if not k:
                return None
            volume_base = _safe_float(k[5])
            volume_usd = _safe_float(k[7]) if len(k) > 7 else None
            return {
                "candle_open_at": datetime.fromtimestamp(int(k[0]) / 1000, UTC),
                "candle_close_at": datetime.fromtimestamp((int(k[0]) + KLINE_INTERVAL_MS) / 1000, UTC),
                "symbol": f"{base}USDT",
                "exchange": self.EXCHANGE,
                "base_asset": base,
                "price_open": _safe_float(k[1]),
                "price_high": _safe_float(k[2]),
                "price_low": _safe_float(k[3]),
                "price_close": _safe_float(k[4]),
                "volume_base": volume_base,
                "volume_usd": volume_usd,
                "txn_count": None,
                "buy_volume_base": None,
                "sell_volume_base": None,
                "volume_delta": None,
                "polled_at": datetime.now(UTC),
            }

        return [row for row in self._fetch_parallel(symbols, fetch_one) if row]

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

KLINE_15M_SQL = """
INSERT INTO futures_klines_15m (
    candle_open_at, candle_close_at, symbol, exchange, base_asset,
    price_open, price_high, price_low, price_close,
    volume_base, volume_usd, buy_volume_base, sell_volume_base,
    volume_delta, txn_count, polled_at
)
VALUES %s
ON CONFLICT (candle_open_at, symbol, exchange) DO UPDATE SET
    candle_close_at   = EXCLUDED.candle_close_at,
    base_asset        = EXCLUDED.base_asset,
    price_open        = COALESCE(EXCLUDED.price_open, futures_klines_15m.price_open),
    price_high        = COALESCE(EXCLUDED.price_high, futures_klines_15m.price_high),
    price_low         = COALESCE(EXCLUDED.price_low, futures_klines_15m.price_low),
    price_close       = COALESCE(EXCLUDED.price_close, futures_klines_15m.price_close),
    volume_base       = COALESCE(EXCLUDED.volume_base, futures_klines_15m.volume_base),
    volume_usd        = COALESCE(EXCLUDED.volume_usd, futures_klines_15m.volume_usd),
    buy_volume_base   = COALESCE(EXCLUDED.buy_volume_base, futures_klines_15m.buy_volume_base),
    sell_volume_base  = COALESCE(EXCLUDED.sell_volume_base, futures_klines_15m.sell_volume_base),
    volume_delta      = COALESCE(EXCLUDED.volume_delta, futures_klines_15m.volume_delta),
    txn_count         = COALESCE(EXCLUDED.txn_count, futures_klines_15m.txn_count),
    polled_at         = EXCLUDED.polled_at,
    updated_at        = NOW()
"""

KLINE_15M_TEMPLATE = "(%(candle_open_at)s, %(candle_close_at)s, %(symbol)s, %(exchange)s, %(base_asset)s, %(price_open)s, %(price_high)s, %(price_low)s, %(price_close)s, %(volume_base)s, %(volume_usd)s, %(buy_volume_base)s, %(sell_volume_base)s, %(volume_delta)s, %(txn_count)s, %(polled_at)s)"

KLINE_15M_KEYS = [
    "candle_open_at", "candle_close_at", "symbol", "exchange", "base_asset",
    "price_open", "price_high", "price_low", "price_close", "volume_base",
    "volume_usd", "buy_volume_base", "sell_volume_base", "volume_delta",
    "txn_count", "polled_at",
]

SNAPSHOT_HOURS = {0, 6, 12, 18}


def _non_null_count(row: dict) -> int:
    return sum(1 for value in row.values() if value is not None)


def _dedupe_rows(rows: List[dict], key_fields: Tuple[str, ...]) -> List[dict]:
    """Postgres cannot update the same ON CONFLICT target twice in one INSERT."""
    deduped: Dict[Tuple[object, ...], dict] = {}
    for row in rows:
        key = tuple(row.get(field) for field in key_fields)
        if any(value is None for value in key):
            continue
        existing = deduped.get(key)
        if existing is None or _non_null_count(row) >= _non_null_count(existing):
            deduped[key] = row
    skipped = len(rows) - len(deduped)
    if skipped:
        log.warning("Deduped %d rows for conflict key %s.", skipped, ",".join(key_fields))
    return list(deduped.values())


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
    rows = _dedupe_rows(rows, ("symbol", "exchange"))
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


def upsert_klines_15m(db_url: str, rows: List[dict]):
    if not rows:
        return
    rows = _dedupe_rows(rows, ("candle_open_at", "symbol", "exchange"))
    if not rows:
        return
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        records = [{k: r.get(k) for k in KLINE_15M_KEYS} for r in rows]
        execute_values(cur, KLINE_15M_SQL, records, template=KLINE_15M_TEMPLATE, page_size=500)
        conn.commit()
        log.info("Upserted %d rows into futures_klines_15m.", len(records))
    except Exception as e:
        log.error("15m kline upsert failed: %s", e)
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

    def _fetch_klines(ex: str) -> List[dict]:
        cls = FETCHER_MAP.get(ex)
        if not cls:
            return []
        symbols = resolved.get(ex, [])
        if not symbols:
            return []
        log.info("Polling %s 15m klines for %d supported symbols...", ex, len(symbols))
        rows = cls().fetch_klines_15m(symbols)
        log.info("  %s 15m klines → %d rows fetched.", ex, len(rows))
        return rows

    all_rows = []
    kline_rows = []
    with ThreadPoolExecutor(max_workers=len(exchanges)) as metrics_executor, \
            ThreadPoolExecutor(max_workers=len(exchanges)) as kline_executor:
        metric_futures = {metrics_executor.submit(_fetch, ex): ex for ex in exchanges}
        kline_futures = {kline_executor.submit(_fetch_klines, ex): ex for ex in exchanges}

        for future in as_completed(kline_futures):
            ex = kline_futures[future]
            try:
                rows = future.result()
                if not rows:
                    log.warning("  %s returned 0 closed 15m klines.", ex)
                kline_rows.extend(rows)
            except Exception as e:
                log.error("  %s 15m kline fetch error: %s", ex, e)
        upsert_klines_15m(db_url, kline_rows)

        for future in as_completed(metric_futures):
            ex = metric_futures[future]
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
    parser.add_argument("--no-align-15m", action="store_true",
                        help="Disable default alignment to UTC 15m candle closes. Only relevant when --interval is 900.")
    parser.add_argument("--once",       action="store_true", help="Run one poll cycle then exit (useful for testing)")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL environment variable is not set.")
        sys.exit(1)

    exchanges = [e.strip().lower() for e in args.exchanges.split(",") if e.strip()]

    top_label = str(args.top) if args.top else (f"top-active-{args.top_active}" if args.top_active else "full-universe")
    align_15m = args.interval == DEFAULT_POLL_INTERVAL and not args.no_align_15m
    log.info(
        "Starting realtime_daemon — exchanges=%s, mode=%s, interval=%ds, align_15m=%s",
        exchanges,
        top_label,
        args.interval,
        align_15m,
    )

    if args.once:
        run_once(db_url, args.top, exchanges, top_active=args.top_active)
        return

    while True:
        if align_15m:
            _sleep_until_aligned_poll()
        start = time.monotonic()
        try:
            run_once(db_url, args.top, exchanges, top_active=args.top_active)
        except Exception as e:
            log.error("Unexpected error in poll cycle: %s", e)
            _telegram(f"🔴 *alt-scraper-realtime*: unexpected error in poll cycle\n`{e}`")
        elapsed = time.monotonic() - start
        if align_15m:
            log.info("Poll cycle done in %.1fs.", elapsed)
        else:
            sleep_for = max(0, args.interval - elapsed)
            log.info("Poll cycle done in %.1fs. Next in %.0fs.", elapsed, sleep_for)
            time.sleep(sleep_for)


if __name__ == "__main__":
    main()
