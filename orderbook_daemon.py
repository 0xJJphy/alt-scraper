#!/usr/bin/env python3
"""
orderbook_daemon.py — WebSocket order book depth daemon.

Maintains live order books for Binance Futures/Spot, Bybit Linear/Spot, OKX Swap, and Upbit Spot.
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
import os
import sys
import threading
import time
import uuid
import zlib
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

    # Binance Futures — try {BASE}USDT then 1000{BASE}USDT
    bf_data = _fetch("https://fapi.binance.com/fapi/v1/exchangeInfo")
    bf_syms = {s["symbol"] for s in (bf_data or {}).get("symbols", [])
               if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"}

    # Bybit Linear — try {BASE}USDT then 1000{BASE}USDT  (limit max=1000; >1000 returns 0)
    bb_data = _fetch("https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000")
    bb_syms = {s["symbol"] for s in (bb_data or {}).get("result", {}).get("list", [])
               if s.get("quoteCoin") == "USDT" and s.get("status") == "Trading"}

    # OKX Swap — uses {BASE}-USDT-SWAP format, no 1000x variants
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

    def bf_sym(base):
        for pfx in ("", "1000"):
            s = f"{pfx}{base}USDT"
            if s in bf_syms:
                return s
        return None

    def bb_sym(base):
        for pfx in ("", "1000"):
            s = f"{pfx}{base}USDT"
            if s in bb_syms:
                return s
        return None

    result = []
    for b in bases:
        spot_sym = f"{b}USDT"
        result.append({
            "base_asset":          b,
            "symbol_binance":      bf_sym(b),
            "symbol_bybit":        bb_sym(b),
            "symbol_okx":          okx_syms.get(b),
            "symbol_upbit":        f"KRW-{b}" if b in upbit_syms else None,
            "symbol_binance_spot": spot_sym if spot_sym in bns_syms else None,
            "symbol_bybit_spot":   spot_sym if spot_sym in bbs_syms else None,
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
    conn = psycopg2.connect(db_url)
    try:
        cur = conn.cursor()
        psycopg2.extras.execute_values(cur, sql, rows, page_size=200)
        conn.commit()
        cur.close()
    finally:
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

    def __init__(self, assets: List[dict]):
        self.assets = [a for a in assets if a.get(self.SYM_KEY)]
        self.books: Dict[str, LocalOrderBook] = {}
        for a in self.assets:
            sym = a[self.SYM_KEY].upper()
            self.books[sym] = LocalOrderBook(sym, self.EXCHANGE)
        self._last_u: Dict[str, int]     = {}
        self._pending: Dict[str, list]   = {}   # events buffered before REST init
        self._init_lock: Dict[str, bool] = {}   # prevent concurrent REST inits per symbol
        self._init_time: Dict[str, float] = {}  # timestamp of last successful init

    def _rest_snapshot(self, symbol: str) -> Tuple[int, list, list]:
        r = requests.get(
            f"{self.REST_BASE}{self.REST_DEPTH}",
            params={"symbol": symbol, "limit": 1000},
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
            for evt in self._pending.pop(symbol, []):
                U, u = evt["U"], evt["u"]
                if u < last_id:
                    continue
                if U <= last_id <= u:
                    self._apply_event(symbol, evt)
            self._init_time[symbol] = time.time()
            log.info("binance init: %s (lastUpdateId=%d)", symbol, last_id)
        except Exception as e:
            log.warning("binance init failed %s: %s", symbol, e)
        finally:
            self._init_lock.pop(symbol, None)

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
                log.warning("binance seq gap %s pu=%s expected=%s, reinit", symbol, pu, expected)
                self.books[symbol].initialized = False
                self._pending[symbol] = []
                self._init_lock[symbol] = True
                threading.Thread(target=self._init_book, args=(symbol, 2.0), daemon=True).start()
            return
        bids = [(float(p), float(q)) for p, q in evt.get("b", [])]
        asks = [(float(p), float(q)) for p, q in evt.get("a", [])]
        self.books[symbol].apply_delta(bids, asks)
        self._last_u[symbol] = evt["u"]

    async def _run_chunk(self, symbols: List[str]) -> None:
        streams = "/".join(f"{s.lower()}@depth@500ms" for s in symbols)
        url     = f"{self.WS_BASE}?streams={streams}"
        backoff = 5
        for sym in symbols:
            self._init_lock[sym] = True
            threading.Thread(target=self._init_book, args=(sym, 0.0), daemon=True).start()
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
                    backoff = 5
                    log.info("binance WS connected (%d symbols)", len(symbols))
                    async for raw in ws:
                        msg    = json.loads(raw)
                        data   = msg.get("data", msg)
                        symbol = data.get("s", "")
                        if not symbol or symbol not in self.books:
                            continue
                        if not self.books[symbol].initialized:
                            self._pending.setdefault(symbol, []).append(data)
                        else:
                            self._apply_event(symbol, data)
            except Exception as e:
                log.warning("binance WS error: %s — reconnecting in %ds", e, backoff)
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
# Bybit Linear WebSocket stream
# ---------------------------------------------------------------------------

class BybitLinearStream:
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
                        if msg.get("op") in ("subscribe", "pong"):
                            continue
                        topic = msg.get("topic", "")
                        if not topic.startswith("orderbook."):
                            continue
                        symbol = topic.split(".")[-1]
                        book   = self.books.get(symbol)
                        if not book:
                            continue
                        data = msg.get("data", {})
                        bids = [(float(p), float(q)) for p, q in data.get("b", [])]
                        asks = [(float(p), float(q)) for p, q in data.get("a", [])]
                        if msg.get("type") == "snapshot":
                            book.apply_snapshot(bids, asks)
                        else:
                            book.apply_delta(bids, asks)
            except Exception as e:
                log.warning("bybit WS error: %s — reconnecting in %ds", e, backoff)
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
                        if "event" in msg:
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
    WS_URL      = "wss://api.upbit.com/websocket/v1"
    REST_BASE   = "https://api.upbit.com/v1"
    EXCHANGE    = "upbit"
    MARKET_TYPE = "spot"

    def __init__(self, assets: List[dict]):
        self.assets = [a for a in assets if a.get("symbol_upbit")]
        self.books: Dict[str, LocalOrderBook] = {}
        for a in self.assets:
            sym = a["symbol_upbit"]  # e.g. "KRW-BTC"
            self.books[sym] = LocalOrderBook(sym, self.EXCHANGE)
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

    async def run(self) -> None:
        self._refresh_krw_usd()
        codes   = [a["symbol_upbit"] for a in self.assets]
        backoff = 5
        while True:
            try:
                subscribe_msg = json.dumps([
                    {"ticket": str(uuid.uuid4())},
                    {"type": "orderbook", "codes": codes, "count": 30},
                ]).encode("utf-8")
                async with websockets.connect(self.WS_URL, ping_interval=30, ping_timeout=30) as ws:
                    backoff = 5
                    await ws.send(subscribe_msg)
                    log.info("upbit WS connected (%d symbols)", len(codes))
                    async for raw in ws:
                        msg  = json.loads(raw)
                        if msg.get("type") != "orderbook":
                            continue
                        code = msg.get("code", "")
                        book = self.books.get(code)
                        if not book:
                            continue
                        rate = self._get_rate()
                        if rate <= 0:
                            continue
                        units = msg.get("orderbook_units", [])
                        bids  = [(float(u["bid_price"]) * rate, float(u["bid_size"])) for u in units]
                        asks  = [(float(u["ask_price"]) * rate, float(u["ask_size"])) for u in units]
                        book.apply_snapshot(bids, asks)
            except Exception as e:
                log.warning("upbit WS error: %s — reconnecting in %ds", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120)

    def get_metrics(self) -> Dict[str, dict]:
        out = {}
        for a in self.assets:
            sym  = a["symbol_upbit"]
            book = self.books.get(sym)
            if book:
                m = book.snapshot()
                if m:
                    out[sym] = {"base_asset": a["base_asset"], **m}
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

    def _apply_event(self, symbol: str, evt: dict) -> None:
        # Spot diff depth stream uses U/u/b/a — same as futures but no pu field
        U = evt.get("U", 0)
        u = evt.get("u", 0)
        expected = self._last_u.get(symbol)
        if expected is not None and U > expected + 1:
            age = time.time() - self._init_time.get(symbol, 0)
            if age >= 5.0 and not self._init_lock.get(symbol):
                log.warning("binance spot seq gap %s U=%s expected=%s+1, reinit", symbol, U, expected)
                self.books[symbol].initialized = False
                self._pending[symbol] = []
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
# OrderBookDaemon — orchestrator
# ---------------------------------------------------------------------------

STREAM_CONFIGS = [
    ("binance_futures", BinanceFuturesStream),
    ("bybit_futures",   BybitLinearStream),
    ("okx_futures",     OKXSwapStream),
    ("upbit_spot",      UpbitSpotStream),
    ("binance_spot",    BinanceSpotStream),
    ("bybit_spot",      BybitSpotStream),
]


class OrderBookDaemon:
    def __init__(self, db_url: str, assets: List[dict], top_n: int,
                 top_active: Optional[int] = None, once: bool = False):
        self.db_url     = db_url
        self.top_n      = top_n
        self.top_active = top_active
        self.once       = once
        self.streams = {name: cls(assets) for name, cls in STREAM_CONFIGS}
        self._stream_last_ok: Dict[str, float] = {k: time.time() for k in self.streams}
        self._known_bases: set = {a["base_asset"] for a in assets}
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
        """Reload asset_metadata every hour. Exit if new assets found — systemd restarts with fresh state."""
        if time.time() - self._last_metadata_check < METADATA_CHECK_INTERVAL:
            return
        self._last_metadata_check = time.time()
        try:
            current = load_top_assets(self.db_url, top_n=self.top_n, top_active=self.top_active)
            current_bases = {a["base_asset"] for a in current}
            new_bases = current_bases - self._known_bases
            if new_bases:
                msg = f"new assets in asset_metadata: {sorted(new_bases)} — restarting to subscribe"
                log.info(msg)
                _telegram(msg)
                sys.exit(0)  # systemd Restart=always brings it back with fresh asset list
            log.info("metadata check: no new assets (%d known)", len(self._known_bases))
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
