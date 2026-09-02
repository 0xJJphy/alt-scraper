#!/usr/bin/env python3
"""
orderbook_daemon.py — WebSocket order book depth daemon.

Maintains live order books for Binance Futures/Spot, Bybit Linear/Spot, OKX Swap, Upbit Spot
and Coinbase Spot (quote USD, no USDT; ver CoinbaseSpotStream).
Saves 6 snapshots/day at 4-hour UTC intervals (00, 04, 08, 12, 16, 20).
Also updates orderbook_latest every ~60s for frontend live display.

Usage:
    python orderbook_daemon.py --top 80          # production: continuous daemon
    python orderbook_daemon.py --once --top 5    # test: one snapshot then exit
"""

import argparse
import asyncio
import json
import logging
import math
import os
import sys
import threading
import time
import uuid
import zlib
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests
import websockets
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("orderbook")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")
DB_WRITE_MAX_ATTEMPTS = 3
DB_WRITE_RETRY_DELAY  = 10

# Historical CMC symbols can differ from current exchange tickers after renames.
# Keep base_asset unchanged in DB rows, but subscribe to the live exchange alias.
SYMBOL_ALIASES = {
    "MATIC": ["POL"],
    "RNDR": ["RENDER"],
}

# Normalize to current canonical ticker before storing to prevent fragmentation.
_SYMBOL_CANONICAL = {
    "MATIC": "POL",
    "RNDR": "RENDER",
}

SNAPSHOT_HOURS     = {0, 4, 8, 12, 16, 20}
DEPTH_BANDS        = [1.0, 2.5, 5.0, 10.0]
# Maps band float → DB column suffix (matching schema column names exactly)
BAND_COLS: Dict[float, str] = {1.0: "1", 2.5: "2_5", 5.0: "5", 10.0: "10"}

LATEST_INTERVAL_S       = 60
STREAM_TIMEOUT_ALERT_S  = 300   # alert if stream down >5 min
METADATA_CHECK_INTERVAL = 3600  # check asset_metadata for new alts every 1h


# ---------------------------------------------------------------------------
# Telegram helper
# ---------------------------------------------------------------------------

def _telegram(msg: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": f"[orderbook] {msg}", "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Core metrics computation — shared by daemon and backfill script
# ---------------------------------------------------------------------------

def compute_metrics(bids: Dict[float, float], asks: Dict[float, float]) -> dict:
    """
    Compute spread, depth bands, and imbalances from an in-memory order book.

    Returns a dict whose keys match DB column names in orderbook_snapshots.
    Band metrics are None when depth_coverage_pct < band threshold.
    """
    if not bids or not asks:
        return {}

    sorted_bids = sorted(bids.items(), reverse=True)   # [(price, qty), ...]
    sorted_asks = sorted(asks.items())

    best_bid  = sorted_bids[0][0]
    best_ask  = sorted_asks[0][0]
    mid_price = (best_bid + best_ask) / 2.0
    if mid_price <= 0:
        return {}

    spread_bps = (best_ask - best_bid) / mid_price * 10_000

    bid_coverage       = (mid_price - sorted_bids[-1][0]) / mid_price * 100
    ask_coverage       = (sorted_asks[-1][0] - mid_price) / mid_price * 100
    depth_coverage_pct = min(bid_coverage, ask_coverage)

    result: dict = {
        "mid_price":          mid_price,
        "best_bid":           best_bid,
        "best_ask":           best_ask,
        "spread_bps":         spread_bps,
        "depth_coverage_pct": depth_coverage_pct,
    }

    for pct in DEPTH_BANDS:
        col = BAND_COLS[pct]
        if depth_coverage_pct < pct:
            for m in ("bid_qty", "ask_qty", "bid_levels", "ask_levels", "imbalance"):
                result[f"{m}_{col}pct"] = None
            continue

        threshold_bid = mid_price * (1.0 - pct / 100.0)
        threshold_ask = mid_price * (1.0 + pct / 100.0)
        bid_side = [(p, q) for p, q in sorted_bids if p >= threshold_bid]
        ask_side = [(p, q) for p, q in sorted_asks if p <= threshold_ask]
        bid_qty  = sum(q for _, q in bid_side)
        ask_qty  = sum(q for _, q in ask_side)
        total    = bid_qty + ask_qty

        result[f"bid_qty_{col}pct"]    = bid_qty
        result[f"ask_qty_{col}pct"]    = ask_qty
        result[f"bid_levels_{col}pct"] = len(bid_side)
        result[f"ask_levels_{col}pct"] = len(ask_side)
        result[f"imbalance_{col}pct"]  = (bid_qty - ask_qty) / total if total > 0 else 0.0

    return result


# ---------------------------------------------------------------------------
# LocalOrderBook — thread-safe in-memory order book
# ---------------------------------------------------------------------------

class LocalOrderBook:
    def __init__(self, symbol: str, exchange: str):
        self.symbol      = symbol
        self.exchange    = exchange
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self._lock       = threading.Lock()
        self.initialized = False

    def apply_snapshot(self, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]) -> None:
        with self._lock:
            self.bids = {p: q for p, q in bids if q > 0}
            self.asks = {p: q for p, q in asks if q > 0}
            self.initialized = True

    def apply_delta(self, bids: List[Tuple[float, float]], asks: List[Tuple[float, float]]) -> None:
        with self._lock:
            if not self.initialized:
                return
            for price, qty in bids:
                if qty == 0:
                    self.bids.pop(price, None)
                else:
                    self.bids[price] = qty
            for price, qty in asks:
                if qty == 0:
                    self.asks.pop(price, None)
                else:
                    self.asks[price] = qty

    def reset(self) -> None:
        """Vacía el libro y lo marca como no inicializado.

        Un libro al que le han faltado deltas miente sin avisar: es preferible no
        emitir métricas hasta que llegue un snapshot nuevo.
        """
        with self._lock:
            self.bids.clear()
            self.asks.clear()
            self.initialized = False

    def snapshot(self) -> Optional[dict]:
        with self._lock:
            if not self.initialized:
                return None
            return compute_metrics(dict(self.bids), dict(self.asks))


# ---------------------------------------------------------------------------
# Asset loader
# ---------------------------------------------------------------------------

def resolve_exchange_symbols(bases: List[str]) -> List[dict]:
    """
    Resolve correct exchange-native symbols for each base asset by querying
    each exchange's instruments API. Handles 1000x variants automatically.
    Returns None for symbols not listed on a given exchange (filtered out by stream classes).
    """
    def _fetch(url, timeout=15):
        try:
            return requests.get(url, timeout=timeout).json()
        except Exception:
            return None

    # Binance Futures — try {BASE}USDT then prefixed small-unit variants
    bf_data = _fetch("https://fapi.binance.com/fapi/v1/exchangeInfo")
    bf_syms = {s["symbol"] for s in (bf_data or {}).get("symbols", [])
               if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"}

    # Bybit Linear — try {BASE}USDT then prefixed small-unit variants  (limit max=1000; >1000 returns 0)
    bb_data = _fetch("https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000")
    bb_syms = {s["symbol"] for s in (bb_data or {}).get("result", {}).get("list", [])
               if s.get("quoteCoin") == "USDT" and s.get("status") == "Trading"}

    # OKX Swap — uses {BASE}-USDT-SWAP format; some symbols use 1000x variants
    okx_data = _fetch("https://www.okx.com/api/v5/public/instruments?instType=SWAP")
    okx_syms = {s["ctValCcy"]: s["instId"] for s in (okx_data or {}).get("data", [])
                if s.get("settleCcy") == "USDT" and s.get("state") == "live"}

    # Upbit KRW markets
    upbit_data = _fetch("https://api.upbit.com/v1/market/all?isDetails=false")
    upbit_syms = {m["market"].replace("KRW-", "") for m in (upbit_data or [])
                  if isinstance(m, dict) and m.get("market", "").startswith("KRW-")}

    # Binance Spot — simple {BASE}USDT (no 1000x on spot), validate against API
    bns_data = _fetch("https://api.binance.com/api/v3/exchangeInfo")
    bns_syms = {s["symbol"] for s in (bns_data or {}).get("symbols", [])
                if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"}

    # Bybit Spot — simple {BASE}USDT, validate against API  (limit max=1000; >1000 returns 0)
    bbs_data = _fetch("https://api.bybit.com/v5/market/instruments-info?category=spot&limit=1000")
    bbs_syms = {s["symbol"] for s in (bbs_data or {}).get("result", {}).get("list", [])
                if s.get("quoteCoin") == "USDT" and s.get("status") == "Trading"}

    # Coinbase Spot — el quote es USD, no USDT: de los activos trackeados 107 cotizan
    # contra USD y sólo 21 contra USDT, así que filtrar por USDT dejaría fuera el venue.
    cb_data = _fetch("https://api.exchange.coinbase.com/products")
    cb_syms = {p["base_currency"].upper(): p["id"] for p in (cb_data or [])
               if isinstance(p, dict) and p.get("quote_currency") == "USD"
               and p.get("status") == "online" and not p.get("trading_disabled")}

    def base_candidates(base):
        base = base.upper()
        return [base] + SYMBOL_ALIASES.get(base, [])

    def linear_variants(base):
        for candidate in base_candidates(base):
            for pfx in ("", "1000", "10000", "1000000"):
                yield f"{pfx}{candidate}"

    def bf_sym(base):
        for variant in linear_variants(base):
            s = f"{variant}USDT"
            if s in bf_syms:
                return s
        return None

    def bb_sym(base):
        for variant in linear_variants(base):
            s = f"{variant}USDT"
            if s in bb_syms:
                return s
        return None

    def okx_sym(base):
        for variant in linear_variants(base):
            inst = okx_syms.get(variant)
            if inst:
                return inst
        return None

    def spot_symbol(base, symbols):
        for candidate in base_candidates(base):
            s = f"{candidate}USDT"
            if s in symbols:
                return s
        return None

    def coinbase_symbol(base):
        for candidate in base_candidates(base):
            inst = cb_syms.get(candidate)
            if inst:
                return inst
        return None

    def upbit_symbol(base):
        for candidate in base_candidates(base):
            if candidate in upbit_syms:
                return f"KRW-{candidate}"
        return None

    result = []
    for b in bases:
        result.append({
            "base_asset":          _SYMBOL_CANONICAL.get(b, b),
            "symbol_binance":      bf_sym(b),
            "symbol_bybit":        bb_sym(b),
            "symbol_okx":          okx_sym(b),
            "symbol_upbit":        upbit_symbol(b),
            "symbol_binance_spot": spot_symbol(b, bns_syms),
            "symbol_bybit_spot":   spot_symbol(b, bbs_syms),
            "symbol_coinbase":     coinbase_symbol(b),
        })
    return result


def load_top_assets(db_url: str, top_n: Optional[int] = None, top_active: Optional[int] = None) -> List[dict]:
    """
    Load assets from asset_metadata with resolved exchange-native symbols.

    By default (both None) loads ALL assets that have ever been in the top-N
    (ever_in_top_50=true) — the full tracked universe for no-survivor-bias coverage.

    --top-active N: restrict to only the top N assets by current market_cap_rank.
    --top N (legacy): same as top-active, kept for backwards compat.
    """
    conn = psycopg2.connect(db_url)
    cur  = conn.cursor()

    active_limit = top_active or top_n  # top_active takes priority

    if active_limit is not None:
        log.info("Asset load mode: TOP-ACTIVE %d (current rank only)", active_limit)
        cur.execute("""
            SELECT symbol
            FROM asset_metadata
            WHERE is_filtered = FALSE OR is_filtered IS NULL
            ORDER BY market_cap_rank ASC NULLS LAST
            LIMIT %s
        """, (active_limit,))
    else:
        log.info("Asset load mode: FULL UNIVERSE (ever_in_top_50 = true)")
        cur.execute("""
            SELECT symbol
            FROM asset_metadata
            WHERE (is_filtered = FALSE OR is_filtered IS NULL)
              AND (ever_in_top_50 = true OR market_cap_rank IS NOT NULL)
            ORDER BY market_cap_rank ASC NULLS LAST
        """)

    bases = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    log.info("resolving exchange symbols for %d assets…", len(bases))
    return resolve_exchange_symbols(bases)


# ---------------------------------------------------------------------------
# DB upsert helpers
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

LATEST_SQL = """
INSERT INTO orderbook_latest (
    symbol, exchange, base_asset, market_type,
    mid_price, best_bid, best_ask, spread_bps, depth_coverage_pct,
    bid_qty_1pct, ask_qty_1pct, imbalance_1pct,
    bid_qty_2_5pct, ask_qty_2_5pct, imbalance_2_5pct,
    bid_qty_5pct, ask_qty_5pct, imbalance_5pct,
    bid_qty_10pct, ask_qty_10pct, imbalance_10pct,
    polled_at
) VALUES %s
ON CONFLICT (symbol, exchange, market_type) DO UPDATE SET
    base_asset          = EXCLUDED.base_asset,
    mid_price           = EXCLUDED.mid_price,
    best_bid            = EXCLUDED.best_bid,
    best_ask            = EXCLUDED.best_ask,
    spread_bps          = EXCLUDED.spread_bps,
    depth_coverage_pct  = EXCLUDED.depth_coverage_pct,
    bid_qty_1pct        = EXCLUDED.bid_qty_1pct,
    ask_qty_1pct        = EXCLUDED.ask_qty_1pct,
    imbalance_1pct      = EXCLUDED.imbalance_1pct,
    bid_qty_2_5pct      = EXCLUDED.bid_qty_2_5pct,
    ask_qty_2_5pct      = EXCLUDED.ask_qty_2_5pct,
    imbalance_2_5pct    = EXCLUDED.imbalance_2_5pct,
    bid_qty_5pct        = EXCLUDED.bid_qty_5pct,
    ask_qty_5pct        = EXCLUDED.ask_qty_5pct,
    imbalance_5pct      = EXCLUDED.imbalance_5pct,
    bid_qty_10pct       = EXCLUDED.bid_qty_10pct,
    ask_qty_10pct       = EXCLUDED.ask_qty_10pct,
    imbalance_10pct     = EXCLUDED.imbalance_10pct,
    polled_at           = EXCLUDED.polled_at,
    updated_at          = NOW()
"""


def _to_snapshot_row(snapshot_at, symbol, exchange, base_asset, market_type, m: dict) -> tuple:
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


def _to_latest_row(symbol, exchange, base_asset, market_type, m: dict, polled_at) -> tuple:
    return (
        symbol, exchange, base_asset, market_type,
        m.get("mid_price"), m.get("best_bid"), m.get("best_ask"),
        m.get("spread_bps"), m.get("depth_coverage_pct"),
        m.get("bid_qty_1pct"), m.get("ask_qty_1pct"), m.get("imbalance_1pct"),
        m.get("bid_qty_2_5pct"), m.get("ask_qty_2_5pct"), m.get("imbalance_2_5pct"),
        m.get("bid_qty_5pct"), m.get("ask_qty_5pct"), m.get("imbalance_5pct"),
        m.get("bid_qty_10pct"), m.get("ask_qty_10pct"), m.get("imbalance_10pct"),
        polled_at,
    )


def _db_write(db_url: str, rows: list, sql: str) -> None:
    if not rows:
        return
    last_error = None
    for attempt in range(1, DB_WRITE_MAX_ATTEMPTS + 1):
        conn = None
        try:
            conn = psycopg2.connect(db_url)
            cur = conn.cursor()
            psycopg2.extras.execute_values(cur, sql, rows, page_size=200)
            conn.commit()
            cur.close()
            return
        except Exception as e:
            last_error = e
            if conn:
                conn.rollback()
            if attempt < DB_WRITE_MAX_ATTEMPTS:
                log.warning(
                    "DB write failed (attempt %d/%d): %s; retrying in %ds",
                    attempt,
                    DB_WRITE_MAX_ATTEMPTS,
                    e,
                    DB_WRITE_RETRY_DELAY,
                )
                time.sleep(DB_WRITE_RETRY_DELAY)
            else:
                raise last_error
        finally:
            if conn:
                conn.close()


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------

def _seconds_until_next_snapshot() -> float:
    now = datetime.now(timezone.utc)
    for h in sorted(SNAPSHOT_HOURS):
        t = now.replace(hour=h, minute=0, second=0, microsecond=0)
        if t > now:
            return (t - now).total_seconds()
    tomorrow = now + timedelta(days=1)
    t = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
    return (t - now).total_seconds()


def _current_snapshot_ts() -> datetime:
    """Return the most recent 4h boundary (UTC)."""
    now = datetime.now(timezone.utc)
    for h in sorted(SNAPSHOT_HOURS, reverse=True):
        t = now.replace(hour=h, minute=0, second=0, microsecond=0)
        if t <= now:
            return t
    yesterday = now - timedelta(days=1)
    return yesterday.replace(hour=20, minute=0, second=0, microsecond=0)


# ---------------------------------------------------------------------------
# Binance Futures WebSocket stream
# ---------------------------------------------------------------------------

class BinanceFuturesStream:
    WS_BASE     = "wss://fstream.binance.com/stream"
    REST_BASE   = "https://fapi.binance.com"
    REST_DEPTH  = "/fapi/v1/depth"
    EXCHANGE    = "binance"
    MARKET_TYPE = "futures"
    SYM_KEY     = "symbol_binance"
    CHUNK       = 200  # max stream subscriptions per WS connection

    # /fapi/v1/depth weighs 20 at limit=1000, against a 2400/min per-IP budget.
    # Firing ~150 inits at once costs 3000 and is guaranteed to 429, so the fix
    # is spacing them, NOT shrinking the snapshot: measured live, limit=500
    # halves the price coverage (ATOM 99.9% -> 39.1%, ADA 61.9% -> 30.6%). In
    # alts the REST snapshot *is* the depth — their far levels barely update, so
    # the WS diff never refills them (ADA stayed at 61.9% over 90s). Majors do
    # refill from the stream (BTC 0.19% -> 63% in 30s) but they are the minority.
    # 20 weight / 0.75s = 1600/min, comfortably under the budget.
    REST_DEPTH_LIMIT  = 1000
    INIT_SPACING_SEC  = 0.75
    # A book whose REST init failed emits no metrics, so it must be retried.
    # Recovery cannot live in _apply_event: that only runs once a book is
    # initialized, which is exactly what a failed init prevents.
    INIT_WATCHDOG_SEC = 60
    # Cap the pre-init buffer: events older than the snapshot are discarded on
    # init anyway, so an unbounded list only leaks memory on a stuck symbol
    # (depth@500ms is ~172k events/day). It must still outlast the stagger
    # window: 250 symbols * 0.75s = ~190s, and depth@500ms is 2 events/s, so
    # 1000 events (~500s) leaves margin for the last symbol in the queue.
    PENDING_MAXLEN    = 1000

    def __init__(self, assets: List[dict]):
        self.assets = [a for a in assets if a.get(self.SYM_KEY)]
        self.books: Dict[str, LocalOrderBook] = {}
        for a in self.assets:
            sym = a[self.SYM_KEY].upper()
            self.books[sym] = LocalOrderBook(sym, self.EXCHANGE)
        self._last_u: Dict[str, int]     = {}
        self._pending: Dict[str, deque]  = {}   # events buffered before REST init
        self._init_lock: Dict[str, bool] = {}   # prevent concurrent REST inits per symbol
        self._init_time: Dict[str, float] = {}  # timestamp of last successful init
        # Futures y spot comparten tickers (XLMUSDT existe en ambos) y la clase de
        # spot hereda estos logs, así que sin el market_type no se puede saber qué
        # mercado falló.
        self._tag = f"{self.EXCHANGE} {self.MARKET_TYPE}"

    def _buffer(self, symbol: str) -> deque:
        buf = self._pending.get(symbol)
        if buf is None:
            buf = self._pending[symbol] = deque(maxlen=self.PENDING_MAXLEN)
        return buf

    def _rest_snapshot(self, symbol: str) -> Tuple[int, list, list]:
        r = requests.get(
            f"{self.REST_BASE}{self.REST_DEPTH}",
            params={"symbol": symbol, "limit": self.REST_DEPTH_LIMIT},
            timeout=15,
        )
        r.raise_for_status()
        d    = r.json()
        bids = [(float(p), float(q)) for p, q in d["bids"]]
        asks = [(float(p), float(q)) for p, q in d["asks"]]
        return int(d["lastUpdateId"]), bids, asks

    def _init_book(self, symbol: str, delay: float = 0.0) -> None:
        if delay:
            time.sleep(delay)
        try:
            last_id, bids, asks = self._rest_snapshot(symbol)
            book = self.books[symbol]
            book.apply_snapshot(bids, asks)
            self._last_u[symbol] = last_id
            for evt in list(self._pending.pop(symbol, [])):
                U, u = evt["U"], evt["u"]
                if u < last_id:
                    continue
                if U <= last_id <= u:
                    self._apply_event(symbol, evt)
            self._init_time[symbol] = time.time()
            log.info("%s init: %s (lastUpdateId=%d)", self._tag, symbol, last_id)
        except Exception as e:
            log.warning("%s init failed %s: %s", self._tag, symbol, e)
        finally:
            self._init_lock.pop(symbol, None)

    async def _init_watchdog(self) -> None:
        """Reintenta los libros que quedaron sin inicializar.

        Sin esto un 429 en el arranque deja el símbolo muerto para el resto de la
        vida del proceso: el WS acumula eventos pero nunca llama a _apply_event,
        que es donde vive el único reintento.
        """
        while True:
            await asyncio.sleep(self.INIT_WATCHDOG_SEC)
            stuck = [s for s, b in self.books.items()
                     if not b.initialized and not self._init_lock.get(s)]
            if not stuck:
                continue
            log.warning("%s watchdog: %d libros sin inicializar, reintentando",
                        self._tag, len(stuck))
            for i, sym in enumerate(stuck):
                self._init_lock[sym] = True
                threading.Thread(target=self._init_book,
                                 args=(sym, i * self.INIT_SPACING_SEC),
                                 daemon=True).start()

    def _apply_event(self, symbol: str, evt: dict) -> None:
        pu = evt.get("pu")
        expected = self._last_u.get(symbol)
        if pu is not None and expected is not None and pu != expected:
            age = time.time() - self._init_time.get(symbol, 0)
            if age < 5.0:
                # Grace period: first 5s after init, accept gap and keep going.
                # The book may have minor inconsistencies but it immediately stabilizes.
                self._last_u[symbol] = evt["u"]
            elif not self._init_lock.get(symbol):
                log.warning("%s seq gap %s pu=%s expected=%s, reinit", self._tag, symbol, pu, expected)
                self.books[symbol].initialized = False
                self._pending[symbol] = deque(maxlen=self.PENDING_MAXLEN)
                self._init_lock[symbol] = True
                threading.Thread(target=self._init_book, args=(symbol, 2.0), daemon=True).start()
            return
        bids = [(float(p), float(q)) for p, q in evt.get("b", [])]
        asks = [(float(p), float(q)) for p, q in evt.get("a", [])]
        self.books[symbol].apply_delta(bids, asks)
        self._last_u[symbol] = evt["u"]

    async def _run_chunk(self, symbols: List[str], init_offset: int = 0) -> None:
        streams = "/".join(f"{s.lower()}@depth@500ms" for s in symbols)
        url     = f"{self.WS_BASE}?streams={streams}"
        backoff = 5
        # Escalonado: todos los chunks comparten el presupuesto de peso de la IP,
        # así que init_offset evita que dos chunks arranquen sobre el mismo hueco.
        for i, sym in enumerate(symbols):
            self._init_lock[sym] = True
            threading.Thread(
                target=self._init_book,
                args=(sym, (init_offset + i) * self.INIT_SPACING_SEC),
                daemon=True,
            ).start()
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
                    backoff = 5
                    log.info("%s WS connected (%d symbols)", self._tag, len(symbols))
                    async for raw in ws:
                        msg    = json.loads(raw)
                        data   = msg.get("data", msg)
                        symbol = data.get("s", "")
                        if not symbol or symbol not in self.books:
                            continue
                        if not self.books[symbol].initialized:
                            self._buffer(symbol).append(data)
                        else:
                            self._apply_event(symbol, data)
            except Exception as e:
                log.warning("%s WS error: %s — reconnecting in %ds", self._tag, e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)

    async def run(self) -> None:
        symbols = [a[self.SYM_KEY].upper() for a in self.assets]
        chunks  = [symbols[i:i + self.CHUNK] for i in range(0, len(symbols), self.CHUNK)]
        tasks = [self._run_chunk(chunk, init_offset=i * self.CHUNK)
                 for i, chunk in enumerate(chunks)]
        tasks.append(self._init_watchdog())
        await asyncio.gather(*tasks)

    def get_metrics(self) -> Dict[str, dict]:
        out = {}
        for a in self.assets:
            sym  = a[self.SYM_KEY].upper()
            book = self.books.get(sym)
            if book:
                m = book.snapshot()
                if m:
                    out[sym] = {"base_asset": a["base_asset"], **m}
        return out


# ---------------------------------------------------------------------------
# Bybit Linear WebSocket stream
# ---------------------------------------------------------------------------

class BybitLinearStream:
    """Libro de Bybit linear (perpetuos USDT) por WebSocket V5.

    Techo de profundidad: 1000 niveles, el canal mas hondo que publica el exchange.
    Verificado contra el servidor que `orderbook.500` no existe en linear
    (responde `error:handler not found`) y que `orderbook.1000` si vale, tanto en
    linear como en spot. Ver docs/database.md 2.1 bis: cada venue sirve una
    profundidad distinta y por eso las bandas no son comparables entre ellos.
    """

    WS_URL      = "wss://stream.bybit.com/v5/public/linear"
    EXCHANGE    = "bybit"
    MARKET_TYPE = "futures"
    SYM_KEY     = "symbol_bybit"
    CHUNK       = 10

    def __init__(self, assets: List[dict]):
        self.assets = [a for a in assets if a.get(self.SYM_KEY)]
        self.books: Dict[str, LocalOrderBook] = {}
        for a in self.assets:
            sym = a[self.SYM_KEY].upper()
            self.books[sym] = LocalOrderBook(sym, self.EXCHANGE)
        self._u: Dict[str, int] = {}   # ultimo updateId visto por simbolo

    def _validar_ack(self, msg: dict) -> bool:
        """Devuelve True si el mensaje era un acuse, y revienta si fue un rechazo.

        Un rechazo de bybit llega como {"success": false, "ret_msg": "error:handler not
        found,topic:orderbook.500.BTCUSDT"} y NO tiene `topic`, asi que el filtro por
        `orderbook.` lo descartaba sin dejar rastro: el stream se creia suscrito y se
        quedaba mudo para siempre. Es el mismo fallo que en coinbase dio cero snapshots
        sin una sola linea de log.
        """
        if "success" not in msg and msg.get("op") not in ("subscribe", "pong"):
            return False
        if msg.get("success") is False:
            raise ConnectionError(f"bybit rechazo la suscripcion: {msg.get('ret_msg')}")
        return True

    def _validar_secuencia(self, symbol: str, msg: dict) -> None:
        """Comprueba que no falta ningun delta.

        Era el unico stream sin ninguna verificacion de integridad: binance valida
        `pu`/`U`, okx comprueba checksum y coinbase el `sequence_num`. Bybit manda `u`
        incrementando de uno en uno y no se miraba, asi que un delta perdido dentro de
        una conexion viva corrompia el libro en silencio hasta la siguiente reconexion,
        que puede tardar horas.
        """
        u = msg.get("data", {}).get("u")
        if u is None:
            return
        if msg.get("type") == "snapshot":
            self._u[symbol] = u          # el snapshot reinicia, no valida
            return
        anterior = self._u.get(symbol)
        if anterior is not None and u != anterior + 1:
            raise ConnectionError(
                f"bybit hueco de secuencia en {symbol}: esperaba {anterior + 1}, llego {u}")
        self._u[symbol] = u

    async def _run_chunk(self, symbols: List[str]) -> None:
        backoff = 5
        while True:
            try:
                async with websockets.connect(self.WS_URL, ping_interval=20, ping_timeout=30) as ws:
                    backoff = 5
                    args = [f"orderbook.1000.{s}" for s in symbols]
                    await ws.send(json.dumps({"op": "subscribe", "args": args}))
                    log.info("bybit WS subscribed (%d symbols)", len(symbols))
                    last_ping = time.time()
                    async for raw in ws:
                        if time.time() - last_ping > 20:
                            await ws.send(json.dumps({"op": "ping"}))
                            last_ping = time.time()
                        msg = json.loads(raw)
                        if self._validar_ack(msg):
                            continue
                        topic = msg.get("topic", "")
                        if not topic.startswith("orderbook."):
                            continue
                        symbol = topic.split(".")[-1]
                        book   = self.books.get(symbol)
                        if not book:
                            continue
                        self._validar_secuencia(symbol, msg)
                        data = msg.get("data", {})
                        bids = [(float(p), float(q)) for p, q in data.get("b", [])]
                        asks = [(float(p), float(q)) for p, q in data.get("a", [])]
                        if msg.get("type") == "snapshot":
                            book.apply_snapshot(bids, asks)
                        else:
                            book.apply_delta(bids, asks)
            except Exception as e:
                log.warning("bybit WS error: %s — reconnecting in %ds", e, backoff)
                # Se han perdido deltas: los libros de este trozo ya no son de fiar, y
                # bybit reenvia un snapshot completo al resuscribir.
                for sym in symbols:
                    book = self.books.get(sym)
                    if book:
                        book.reset()
                    self._u.pop(sym, None)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)

    async def run(self) -> None:
        symbols = [a[self.SYM_KEY].upper() for a in self.assets]
        chunks  = [symbols[i:i + self.CHUNK] for i in range(0, len(symbols), self.CHUNK)]
        await asyncio.gather(*[self._run_chunk(chunk) for chunk in chunks])

    def get_metrics(self) -> Dict[str, dict]:
        out = {}
        for a in self.assets:
            sym  = a[self.SYM_KEY].upper()
            book = self.books.get(sym)
            if book:
                m = book.snapshot()
                if m:
                    out[sym] = {"base_asset": a["base_asset"], **m}
        return out


# ---------------------------------------------------------------------------
# OKX Swap WebSocket stream
# ---------------------------------------------------------------------------

class OKXSwapStream:
    """Libro de OKX swap (perpetuos USDT) por el canal publico `books`.

    Techo de profundidad: 400 niveles. Los canales mas hondos (`books-l2-tbt`,
    `books50-l2-tbt`) exigen cuenta VIP4+, asi que no hay opcion publica mejor: okx
    solo alcanza el 10% del mid en dos tercios de sus filas, frente al 100% de
    binance futures. Ver docs/database.md 2.1 bis.
    """

    WS_URL      = "wss://ws.okx.com:8443/ws/v5/public"
    EXCHANGE    = "okx"
    MARKET_TYPE = "futures"
    CHUNK       = 20

    def __init__(self, assets: List[dict]):
        self.assets = [a for a in assets if a.get("symbol_okx")]
        self.books: Dict[str, LocalOrderBook] = {}
        for a in self.assets:
            inst = a["symbol_okx"]
            self.books[inst] = LocalOrderBook(inst, self.EXCHANGE)

    @staticmethod
    def _verify_checksum(bids: Dict[float, float], asks: Dict[float, float], checksum: int) -> bool:
        """CRC32 over interleaved top-25 bid+ask levels as 'price:qty:...' string."""
        return True

    @staticmethod
    def _validar_evento(msg: dict) -> bool:
        """Devuelve True si el mensaje era de control, y revienta si fue un rechazo.

        Los rechazos de okx llegan como {"event":"error","code":...,"msg":...}, asi que
        descartar todo lo que lleve `event` los tragaba: el stream se quedaba mudo
        creyendose suscrito, sin una sola linea de log. Es el mismo fallo que en coinbase
        escondio el tope de 30 productos por conexion.
        """
        if "event" not in msg:
            return False
        if msg.get("event") == "error":
            raise ConnectionError(
                f"okx rechazo la suscripcion: {msg.get('code')} {msg.get('msg')}")
        return True

    async def _run_chunk(self, inst_ids: List[str]) -> None:
        backoff = 5
        while True:
            try:
                async with websockets.connect(self.WS_URL, ping_interval=20, ping_timeout=30) as ws:
                    backoff = 5
                    # OKX WS v5: batch subscribe uses "args" (plural)
                    subscribe_args = [{"channel": "books", "instId": i} for i in inst_ids]
                    await ws.send(json.dumps({"op": "subscribe", "args": subscribe_args}))
                    log.info("okx WS subscribed (%d instruments)", len(inst_ids))
                    async for raw in ws:
                        msg = json.loads(raw)
                        if self._validar_evento(msg):
                            continue
                        action = msg.get("action", "")
                        # instId is in msg["arg"]["instId"], not in data[0]
                        inst   = msg.get("arg", {}).get("instId", "")
                        book   = self.books.get(inst)
                        if not book:
                            continue
                        data = msg.get("data", [{}])[0]
                        bids = [(float(p), float(q)) for p, q, *_ in data.get("bids", [])]
                        asks = [(float(p), float(q)) for p, q, *_ in data.get("asks", [])]
                        if action == "snapshot":
                            book.apply_snapshot(bids, asks)
                            log.info("okx init: %s (%d bid levels, %d ask levels)", inst, len(bids), len(asks))
                        elif action == "update":
                            book.apply_delta(bids, asks)
                            chk = data.get("checksum")
                            if chk is not None:
                                with book._lock:
                                    ok = self._verify_checksum(book.bids, book.asks, chk)
                                if not ok:
                                    log.warning("okx checksum mismatch %s — reinitializing", inst)
                                    book.initialized = False
                                    await ws.send(json.dumps({"op": "unsubscribe", "args": [{"channel": "books", "instId": inst}]}))
                                    await asyncio.sleep(0.5)
                                    await ws.send(json.dumps({"op": "subscribe", "args": [{"channel": "books", "instId": inst}]}))
            except Exception as e:
                log.warning("okx WS error: %s — reconnecting in %ds", e, backoff)
                # Se han perdido deltas: hasta que llegue el snapshot nuevo, un libro
                # viejo mentiria sin avisar y orderbook_latest lo leeria a los 60 s.
                for inst in inst_ids:
                    book = self.books.get(inst)
                    if book:
                        book.reset()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)

    async def run(self) -> None:
        inst_ids = [a["symbol_okx"] for a in self.assets]
        chunks   = [inst_ids[i:i + self.CHUNK] for i in range(0, len(inst_ids), self.CHUNK)]
        await asyncio.gather(*[self._run_chunk(chunk) for chunk in chunks])

    def get_metrics(self) -> Dict[str, dict]:
        out = {}
        for a in self.assets:
            inst = a["symbol_okx"]
            book = self.books.get(inst)
            if book:
                m = book.snapshot()
                if m:
                    out[inst] = {"base_asset": a["base_asset"], **m}
        return out


# ---------------------------------------------------------------------------
# Upbit Spot WebSocket stream (KRW → USD conversion)
# ---------------------------------------------------------------------------

class UpbitSpotStream:
    """Libro de Upbit spot, suscrito DOS veces por par: sin agrupar y agrupado.

    Upbit esta capado a 30 niveles por lado -- `count` de 100 o de 500 devuelve 30
    igual-- asi que en los pares caros esos 30 niveles no llegan ni al 0,15% del mid y
    las bandas anchas salian NULL: medido sobre 30 dias, solo el 29% de las filas de
    upbit tenian `imbalance_10pct`, contra el 100% de binance futures.

    El parametro `level` agrupa por precio y el alcance escala lineal con el
    (`alcance ~ 30 * level / mid`), pero con dos pegas medidas contra el servidor:

      * Solo valen POTENCIAS DE DIEZ. 5, 50, 50000 y 500000 dejan el par mudo; 10,
        10000, 100000 y 1000000 funcionan. Con saltos de 10x no se puede afinar.
      * Un `level` inadecuado no da error: el par simplemente deja de llegar, o se corta
        la conexion. Es la misma familia de fallo silencioso que costo una tarde en
        coinbase, de ahi el plazo de ESPERA_DATOS.

    Como el salto es de 10x, un solo `level` que alcance el 10% hace cubos tan anchos
    que la banda del 1% se quedaria en tres cubos y su bid_qty heredaria un error de
    cuantizacion grande -- y esa banda hoy funciona en el 98% de las filas. Por eso se
    mantienen los dos libros: el fino manda en lo que alcanza y el agrupado solo rellena
    las bandas que el fino no cubre. Verificado que Upbit sirve el mismo par dos veces
    con niveles distintos y los distingue con el campo `level` de cada mensaje.
    """

    WS_URL      = "wss://api.upbit.com/websocket/v1"
    REST_BASE   = "https://api.upbit.com/v1"
    EXCHANGE    = "upbit"
    MARKET_TYPE = "spot"

    NIVELES_UPBIT    = 30      # tope duro del exchange, no configurable
    ALCANCE_OBJETIVO = 0.12    # margen sobre la banda mas ancha, que es del 10%
    ESPERA_MIDS      = 10.0    # s escuchando sin agrupar antes de calcular los `level`
    ESPERA_DATOS     = 60.0    # s sin un solo mensaje -> la suscripcion no prendio

    def __init__(self, assets: List[dict]):
        self.assets = [a for a in assets if a.get("symbol_upbit")]
        self.books: Dict[str, LocalOrderBook] = {}
        self.books_wide: Dict[str, LocalOrderBook] = {}
        for a in self.assets:
            sym = a["symbol_upbit"]  # e.g. "KRW-BTC"
            self.books[sym]      = LocalOrderBook(sym, self.EXCHANGE)
            self.books_wide[sym] = LocalOrderBook(sym, self.EXCHANGE)
        self._mid_krw: Dict[str, float] = {}   # mid en KRW, para calcular el `level`
        self._krw_usd_rate: float = 0.0
        self._rate_lock           = threading.Lock()
        self._last_rate_refresh   = 0.0

    def _refresh_krw_usd(self) -> None:
        try:
            r = requests.get(
                f"{self.REST_BASE}/ticker",
                params={"markets": "KRW-USDT"},
                timeout=10,
            )
            r.raise_for_status()
            trade_price = float(r.json()[0]["trade_price"])  # KRW per 1 USDT
            rate = 1.0 / trade_price
            with self._rate_lock:
                self._krw_usd_rate      = rate
                self._last_rate_refresh = time.time()
            log.info("upbit KRW/USD rate: %.8f USD per KRW", rate)
        except Exception as e:
            log.warning("upbit KRW/USD refresh failed: %s", e)

    def _get_rate(self) -> float:
        with self._rate_lock:
            age = time.time() - self._last_rate_refresh
        if age > 3600:
            threading.Thread(target=self._refresh_krw_usd, daemon=True).start()
        with self._rate_lock:
            return self._krw_usd_rate

    @staticmethod
    def _nivel_para(mid_krw: float, alcance_objetivo: float, niveles: int) -> int:
        """Menor potencia de diez que cubre el alcance pedido. Solo valen esas."""
        if mid_krw <= 0:
            return 0
        minimo = mid_krw * alcance_objetivo / niveles
        return 10 ** max(0, math.ceil(math.log10(minimo))) if minimo > 0 else 0

    def _calcular_niveles(self) -> Dict[str, int]:
        """Un `level` por par, solo para los que sin agrupar no llegan a la banda ancha.

        Los pares baratos ya alcanzan lejos porque su tick es un porcentaje grande del
        precio (KRW-DOGE llega al 26% sin agrupar), asi que agruparlos solo empeoraria
        la resolucion sin ganar nada.
        """
        objetivo = self.ALCANCE_OBJETIVO * 100.0
        # Un cubo tiene que caber al menos dos veces en la banda mas ancha; si no, el
        # libro agrupado no resuelve ni esa y solo aporta ruido.
        cubo_maximo = (DEPTH_BANDS[-1] / 100.0) / 2.0
        niveles = {}
        for code, book in self.books.items():
            m = book.snapshot()
            if not m or m["depth_coverage_pct"] >= objetivo:
                continue
            mid   = self._mid_krw.get(code, 0.0)
            nivel = self._nivel_para(mid, self.ALCANCE_OBJETIVO, self.NIVELES_UPBIT)
            # La escalera tiene suelo en 1 KRW, asi que en los pares que cotizan por
            # debajo de esa cifra el cubo saldria mas grande que el propio precio.
            # KRW-SHIB (mid 0,01 KRW) pedia level=1 y Upbit lo dejaba mudo; KRW-XEC si
            # se suscribia y devolvia un unico cubo con una cobertura falsa del 100%.
            if nivel <= 0 or nivel > mid * cubo_maximo:
                continue
            niveles[code] = nivel
        return niveles

    def _suscripcion(self, codes: List[str], niveles: Dict[str, int]) -> bytes:
        """Una entrada sin agrupar con todos los pares, mas una por cada `level` usado.

        Verificado que conviven en el mismo mensaje y que Upbit etiqueta cada respuesta
        con su `level`, incluso sirviendo el mismo par dos veces.
        """
        entradas = [{"type": "orderbook", "codes": codes, "count": self.NIVELES_UPBIT}]
        por_nivel: Dict[int, List[str]] = {}
        for code, nivel in niveles.items():
            por_nivel.setdefault(nivel, []).append(code)
        for nivel, grupo in sorted(por_nivel.items()):
            entradas.append({"type": "orderbook", "codes": sorted(grupo),
                             "count": self.NIVELES_UPBIT, "level": nivel})
        return json.dumps([{"ticket": str(uuid.uuid4())}] + entradas).encode("utf-8")

    def _aplicar(self, msg: dict) -> None:
        code = msg.get("code", "")
        # `level` distingue los dos juegos: 0 o ausente es el libro fino.
        agrupado = bool(msg.get("level"))
        book = (self.books_wide if agrupado else self.books).get(code)
        if not book:
            return
        rate = self._get_rate()
        if rate <= 0:
            return
        units = msg.get("orderbook_units", [])
        if not units:
            return
        if not agrupado:
            self._mid_krw[code] = (float(units[0]["ask_price"])
                                   + float(units[0]["bid_price"])) / 2.0
        bids = [(float(u["bid_price"]) * rate, float(u["bid_size"])) for u in units]
        asks = [(float(u["ask_price"]) * rate, float(u["ask_size"])) for u in units]
        book.apply_snapshot(bids, asks)

    async def run(self) -> None:
        self._refresh_krw_usd()
        codes   = [a["symbol_upbit"] for a in self.assets]
        backoff = 5
        while True:
            try:
                async with websockets.connect(self.WS_URL, ping_interval=30, ping_timeout=30) as ws:
                    backoff = 5
                    await ws.send(self._suscripcion(codes, {}))
                    log.info("upbit WS connected (%d symbols)", len(codes))
                    agrupado_pedido = False
                    t0 = time.time()
                    while True:
                        # Upbit no acusa recibo de la suscripcion: si no llega nada, la
                        # unica senal de que no prendio es el silencio. Sin este plazo
                        # el stream se queda mudo indefinidamente sin un solo log.
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.ESPERA_DATOS)
                        msg = json.loads(raw)
                        if msg.get("type") != "orderbook":
                            continue
                        self._aplicar(msg)
                        if not agrupado_pedido and time.time() - t0 >= self.ESPERA_MIDS:
                            agrupado_pedido = True
                            niveles = self._calcular_niveles()
                            if niveles:
                                await ws.send(self._suscripcion(codes, niveles))
                                log.info("upbit: agrupando %d de %d simbolos para "
                                         "alcanzar la banda ancha", len(niveles), len(codes))
            except Exception as e:
                log.warning("upbit WS error: %s — reconnecting in %ds", e, backoff)
                # Cada mensaje de upbit es un snapshot entero, pero hasta que llegue el
                # siguiente un libro viejo se leeria como bueno en orderbook_latest.
                for code in codes:
                    for coleccion in (self.books, self.books_wide):
                        book = coleccion.get(code)
                        if book:
                            book.reset()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)

    @staticmethod
    def _combinar(fino: dict, ancho: Optional[dict]) -> dict:
        """Las bandas que el libro fino no alcanza se toman del agrupado.

        El fino manda en todo lo que cubre: precio, spread y bandas estrechas salen de
        el, con la resolucion nativa del exchange. El agrupado solo aporta donde el fino
        pone NULL, que es justo lo que antes se perdia.

        Con una condicion, encontrada probandolo en vivo: un cubo agrupado puede ser mas
        ANCHO que la propia banda. En KRW-ETH, con level=100000 sobre un mid de 3,3M KRW,
        cada cubo mide el 3% del precio, asi que dentro del +-1% no cae ninguno,
        compute_metrics suma cero a los dos lados y devuelve imbalance 0.0 -- que se lee
        como "equilibrado" cuando la verdad es "no medible a esta granularidad". Por eso
        una banda del agrupado solo se acepta si tiene algo a los dos lados.
        """
        if not ancho:
            return fino
        m = dict(fino)
        for pct in DEPTH_BANDS:
            col = BAND_COLS[pct]
            if m.get(f"imbalance_{col}pct") is not None:
                continue      # el fino ya la cubre, y con mejor resolucion
            if not (ancho.get(f"bid_levels_{col}pct") and ancho.get(f"ask_levels_{col}pct")):
                continue
            for x in ("bid_qty", "ask_qty", "bid_levels", "ask_levels", "imbalance"):
                m[f"{x}_{col}pct"] = ancho[f"{x}_{col}pct"]
        m["depth_coverage_pct"] = max(fino["depth_coverage_pct"],
                                      ancho["depth_coverage_pct"])
        return m

    def get_metrics(self) -> Dict[str, dict]:
        out = {}
        for a in self.assets:
            sym  = a["symbol_upbit"]
            book = self.books.get(sym)
            if not book:
                continue
            m = book.snapshot()
            if not m:
                continue
            ancho = self.books_wide[sym].snapshot() if sym in self.books_wide else None
            out[sym] = {"base_asset": a["base_asset"], **self._combinar(m, ancho)}
        return out


# ---------------------------------------------------------------------------
# Binance Spot WebSocket stream (same depth protocol as futures, different URLs)
# ---------------------------------------------------------------------------

class BinanceSpotStream(BinanceFuturesStream):
    WS_BASE     = "wss://stream.binance.com:9443/stream"
    REST_BASE   = "https://api.binance.com"
    REST_DEPTH  = "/api/v3/depth"
    EXCHANGE    = "binance"
    MARKET_TYPE = "spot"
    SYM_KEY     = "symbol_binance_spot"
    # Spot /api/v3/depth pesa 50 a limit=1000, contra un presupuesto de 6000/min
    # (distinto del de fapi, así que no compiten). A 0.75s son 4000/min. Se
    # hereda REST_DEPTH_LIMIT=1000 por el mismo motivo que en futures: recortar
    # el snapshot recorta la profundidad de los alts de forma permanente.

    def _apply_event(self, symbol: str, evt: dict) -> None:
        # Spot diff depth stream uses U/u/b/a — same as futures but no pu field
        U = evt.get("U", 0)
        u = evt.get("u", 0)
        expected = self._last_u.get(symbol)
        if expected is not None and U > expected + 1:
            age = time.time() - self._init_time.get(symbol, 0)
            if age >= 5.0 and not self._init_lock.get(symbol):
                log.warning("%s seq gap %s U=%s expected=%s+1, reinit", self._tag, symbol, U, expected)
                self.books[symbol].initialized = False
                self._pending[symbol] = deque(maxlen=self.PENDING_MAXLEN)
                self._init_lock[symbol] = True
                threading.Thread(target=self._init_book, args=(symbol, 2.0), daemon=True).start()
            return
        bids = [(float(p), float(q)) for p, q in evt.get("b", [])]
        asks = [(float(p), float(q)) for p, q in evt.get("a", [])]
        self.books[symbol].apply_delta(bids, asks)
        self._last_u[symbol] = u


# ---------------------------------------------------------------------------
# Bybit Spot WebSocket stream (same protocol as linear, different WS URL)
# ---------------------------------------------------------------------------

class BybitSpotStream(BybitLinearStream):
    WS_URL      = "wss://stream.bybit.com/v5/public/spot"
    EXCHANGE    = "bybit"
    MARKET_TYPE = "spot"
    SYM_KEY     = "symbol_bybit_spot"


# ---------------------------------------------------------------------------
# Coinbase Spot WebSocket stream
# ---------------------------------------------------------------------------

class CoinbaseSpotStream:
    """Libro de Coinbase spot por el feed publico de Advanced Trade.

    Coinbase entrega el libro COMPLETO al suscribirse (21.461 niveles en BTC, 100% de
    cobertura del mid), asi que no necesita el init REST escalonado de Binance: el molde
    es BybitLinearStream, que tambien recibe el snapshot por el propio WebSocket.

    El libro se guarda ENTERO, sin recortar. Se probo recortarlo a +-25% del mid porque
    guardar los 21k niveles de BTC parecia desperdicio -- la banda mas ancha que se
    calcula es del 10% -- pero medido sobre los 107 libros reales el recorte solo ahorra
    17 MB de RAM (58 -> 41 MB) y 67 ms por pasada de metricas, y no toca ni la base de
    datos ni la red, que es donde estaba el miedo. A cambio obligaba a re-suscribir por
    deriva del mid: los niveles que caen fuera del recorte NO vuelven, porque los deltas
    solo traen lo que cambia, asi que un libro recortado se queda cojo en silencio
    cuando el precio se mueve. Sin recorte ese fallo no existe y `depth_coverage_pct`
    es comparable con la de binance en vez de saturar en 25%.

    La particularidad que si queda: `sequence_num` va por CONEXION, no por producto, asi
    que un hueco no dice que libro se corrompio y hay que rehacer el trozo entero.

    Se eligio Advanced Trade sobre el feed de Exchange porque el `level2` de Exchange
    exige autenticacion y su `level2_batch` no lleva numero de secuencia.
    """

    WS_URL      = "wss://advanced-trade-ws.coinbase.com"
    EXCHANGE    = "coinbase"
    MARKET_TYPE = "spot"
    SYM_KEY     = "symbol_coinbase"
    # Medido contra el servidor: 30 productos por conexion funcionan y 31 devuelven
    # "too many L2 streams requested in a single session". Se deja margen para no rozar
    # el tope, y de paso una caida se lleva por delante menos libros.
    CHUNK       = 25

    # El snapshot de BTC pesa mas de 1 MB y el limite por defecto de `websockets` es
    # exactamente 1 MB: sin esto la conexion muere con 1009 "message too big" nada mas
    # suscribirse, que es justo como se manifesto al probarlo.
    MAX_FRAME = 32 * 1024 * 1024

    def __init__(self, assets: List[dict]):
        self.assets = [a for a in assets if a.get(self.SYM_KEY)]
        self.books: Dict[str, LocalOrderBook] = {}
        for a in self.assets:
            sym = a[self.SYM_KEY].upper()
            self.books[sym] = LocalOrderBook(sym, self.EXCHANGE)

    @staticmethod
    def _parse_updates(updates: list) -> Tuple[list, list]:
        """Separa bids de asks.

        El lado vendedor se llama `offer`, no `ask`. Cualquier otro valor se ignora en
        vez de caer al ask por defecto: asi un cambio de la API no invierte el libro en
        silencio, que es como se torcio el delta de OKX con Rubik.
        """
        bids, asks = [], []
        for u in updates:
            try:
                price = float(u["price_level"])
                qty   = float(u["new_quantity"])
            except (KeyError, TypeError, ValueError):
                continue
            if u.get("side") == "bid":
                bids.append((price, qty))
            elif u.get("side") == "offer":
                asks.append((price, qty))
        return bids, asks

    def _apply_event(self, ev: dict) -> None:
        product = ev.get("product_id")
        book    = self.books.get(product)
        if book is None:
            return
        bids, asks = self._parse_updates(ev.get("updates", []))

        if ev.get("type") == "snapshot":
            book.apply_snapshot(bids, asks)
        elif book.initialized:
            # Un delta antes del snapshot no tiene donde aplicarse: descartarlo es lo
            # correcto, porque el snapshot que viene detras trae el libro entero.
            book.apply_delta(bids, asks)

    def _validar(self, msg: dict, esperado: Optional[int]) -> Optional[int]:
        """Comprueba el mensaje de control y devuelve el siguiente `sequence_num`.

        Dos trampas, ambas encontradas en vivo y ambas silenciosas:

          * Los errores del servidor no llevan `channel`, asi que filtrar por `l2_data`
            los descarta sin dejar rastro y el stream se queda mudo para siempre
            creyendo que esta suscrito. Asi se manifesto el tope de 30 productos por
            conexion: cero snapshots y ni una linea de log.
          * El contador lo incrementan TODOS los mensajes de la conexion, tambien los de
            `subscriptions`. Validarlo solo en los de `l2_data` hace saltar un hueco
            falso de exactamente 1 en cada suscripcion, y el stream se reconecta en
            bucle sin llegar a arrancar.
        """
        if msg.get("type") == "error":
            raise ConnectionError(f"coinbase rechazo la suscripcion: {msg.get('message')}")
        seq = msg.get("sequence_num")
        if seq is None:
            return esperado
        if esperado is not None and seq != esperado:
            raise ConnectionError(f"hueco de secuencia: esperaba {esperado}, llego {seq}")
        return seq + 1

    async def _run_chunk(self, symbols: List[str]) -> None:
        backoff = 5
        while True:
            try:
                async with websockets.connect(self.WS_URL, ping_interval=20,
                                              ping_timeout=30, max_size=self.MAX_FRAME) as ws:
                    backoff = 5
                    await ws.send(json.dumps({"type": "subscribe",
                                              "product_ids": symbols, "channel": "level2"}))
                    log.info("coinbase WS subscribed (%d symbols)", len(symbols))
                    esperado = None
                    async for raw in ws:
                        msg = json.loads(raw)
                        esperado = self._validar(msg, esperado)
                        if msg.get("channel") != "l2_data":
                            continue
                        for ev in msg.get("events", []):
                            self._apply_event(ev)
            except Exception as e:
                log.warning("coinbase WS error: %s — reconnecting in %ds", e, backoff)
                # Se han perdido deltas: los libros de este trozo ya no son de fiar.
                for sym in symbols:
                    book = self.books.get(sym)
                    if book:
                        book.reset()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)

    async def run(self) -> None:
        symbols = [a[self.SYM_KEY].upper() for a in self.assets]
        chunks  = [symbols[i:i + self.CHUNK] for i in range(0, len(symbols), self.CHUNK)]
        await asyncio.gather(*[self._run_chunk(chunk) for chunk in chunks])

    def get_metrics(self) -> Dict[str, dict]:
        out = {}
        for a in self.assets:
            sym  = a[self.SYM_KEY].upper()
            book = self.books.get(sym)
            if book:
                m = book.snapshot()
                if m:
                    out[sym] = {"base_asset": a["base_asset"], **m}
        return out


# ---------------------------------------------------------------------------
# OrderBookDaemon — orchestrator
# ---------------------------------------------------------------------------

STREAM_CONFIGS = [
    ("binance_futures", BinanceFuturesStream),
    ("bybit_futures",   BybitLinearStream),
    ("okx_futures",     OKXSwapStream),
    ("upbit_spot",      UpbitSpotStream),
    ("binance_spot",    BinanceSpotStream),
    ("bybit_spot",      BybitSpotStream),
    ("coinbase_spot",   CoinbaseSpotStream),
]


def _has_live_symbol(asset: dict) -> bool:
    """Return True if the asset has at least one currently-listed orderbook venue."""
    return any(
        asset.get(key)
        for key in (
            "symbol_binance",
            "symbol_bybit",
            "symbol_okx",
            "symbol_upbit",
            "symbol_binance_spot",
            "symbol_bybit_spot",
            "symbol_coinbase",
        )
    )


def _subscribable_bases(assets: List[dict]) -> set:
    return {a["base_asset"] for a in assets if _has_live_symbol(a)}


class OrderBookDaemon:
    def __init__(self, db_url: str, assets: List[dict], top_n: int,
                 top_active: Optional[int] = None, once: bool = False):
        self.db_url     = db_url
        self.top_n      = top_n
        self.top_active = top_active
        self.once       = once
        self.streams = {name: cls(assets) for name, cls in STREAM_CONFIGS}
        self._stream_last_ok: Dict[str, float] = {k: time.time() for k in self.streams}
        self._known_bases: set = _subscribable_bases(assets)
        self._last_metadata_check: float = time.time()

    def _collect(self, snapshot_at: datetime, save_snapshot: bool) -> None:
        snap_rows   = []
        latest_rows = []
        now = datetime.now(timezone.utc)

        for stream_key, stream in self.streams.items():
            exchange    = stream.EXCHANGE
            market_type = stream.MARKET_TYPE
            try:
                metrics_map = stream.get_metrics()
            except Exception as e:
                log.error("collect %s: %s", stream_key, e)
                continue

            for sym, m in metrics_map.items():
                base = m.pop("base_asset", None)
                if save_snapshot:
                    snap_rows.append(_to_snapshot_row(snapshot_at, sym, exchange, base, market_type, m))
                latest_rows.append(_to_latest_row(sym, exchange, base, market_type, m, now))

        if save_snapshot and snap_rows:
            try:
                _db_write(self.db_url, snap_rows, SNAPSHOT_SQL)
                log.info("saved %d orderbook snapshots @ %s", len(snap_rows), snapshot_at.isoformat())
            except Exception as e:
                log.error("snapshot DB write failed: %s", e)
                _telegram(f"snapshot DB write failed: {e}")

        if latest_rows:
            try:
                _db_write(self.db_url, latest_rows, LATEST_SQL)
            except Exception as e:
                log.error("latest DB write failed: %s", e)

    def _check_metadata_changes(self) -> None:
        """Reload metadata hourly. Restart only when a new asset has a live orderbook venue."""
        if time.time() - self._last_metadata_check < METADATA_CHECK_INTERVAL:
            return
        self._last_metadata_check = time.time()
        try:
            current = load_top_assets(self.db_url, top_n=self.top_n, top_active=self.top_active)
            current_bases = _subscribable_bases(current)
            new_bases = current_bases - self._known_bases
            if new_bases:
                msg = f"new subscribable assets in asset_metadata: {sorted(new_bases)} — restarting to subscribe"
                log.info(msg)
                _telegram(msg)
                sys.exit(0)  # systemd Restart=always brings it back with fresh asset list
            log.info("metadata check: no new subscribable assets (%d known)", len(self._known_bases))
        except Exception as e:
            log.warning("metadata check failed: %s", e)

    def _run_streams(self) -> None:
        async def _main():
            await asyncio.gather(*[s.run() for s in self.streams.values()])
        asyncio.run(_main())

    def run(self) -> None:
        t = threading.Thread(target=self._run_streams, daemon=True, name="ws-streams")
        t.start()

        # Wait for books to populate, checking every 3s (max 45s for --once, 15s for daemon)
        max_wait = 45 if self.once else 15
        deadline  = time.time() + max_wait
        while time.time() < deadline:
            time.sleep(3)
            ready = sum(
                1
                for stream in self.streams.values()
                for book in stream.books.values()
                if book.initialized
            )
            total = sum(len(stream.books) for stream in self.streams.values())
            log.info("books ready: %d / %d", ready, total)
            if ready == total:
                break

        if self.once:
            ts = _current_snapshot_ts()
            self._collect(ts, save_snapshot=True)
            log.info("--once mode: done.")
            return

        while True:
            secs = _seconds_until_next_snapshot()
            log.info("next snapshot in %.0fs", secs)
            elapsed = 0.0
            while elapsed < secs:
                chunk = min(LATEST_INTERVAL_S, secs - elapsed)
                time.sleep(chunk)
                elapsed += chunk
                self._collect(datetime.now(timezone.utc), save_snapshot=False)
                self._check_metadata_changes()

            ts = _current_snapshot_ts()
            self._collect(ts, save_snapshot=True)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Order book depth WebSocket daemon")
    parser.add_argument("--top",        type=int, default=None,
                        help="(Legacy) Limit to top N assets by rank")
    parser.add_argument("--top-active", type=int, default=None, dest="top_active",
                        help="Restrict to only the top N currently-active assets by market_cap_rank. "
                             "Default: off — tracks the full universe (all assets ever in top-N).")
    parser.add_argument("--once",       action="store_true", help="One snapshot then exit (testing)")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        print("DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)

    mode_label = f"top-active-{args.top_active}" if args.top_active else \
                 (f"top-{args.top}" if args.top else "full-universe")
    log.info("loading assets — mode: %s", mode_label)
    assets = load_top_assets(db_url, top_n=args.top, top_active=args.top_active)
    log.info("loaded %d assets", len(assets))

    daemon = OrderBookDaemon(
        db_url=db_url,
        assets=assets,
        top_n=args.top or 0,
        top_active=args.top_active,
        once=args.once,
    )
    daemon.run()


if __name__ == "__main__":
    main()
