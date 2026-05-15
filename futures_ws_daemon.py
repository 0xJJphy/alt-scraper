#!/usr/bin/env python3
"""
futures_ws_daemon.py - 15m futures kline WebSocket daemon.

Uses exchange-native closed 15 minute kline events as the primary clock for
Paper Live. REST reconciliation runs after each closed candle and upserts by
the exchange candle open time, never by receive/poll time.

Usage:
    python futures_ws_daemon.py --top-active 20 --once
    python futures_ws_daemon.py --exchanges binance,bybit,okx
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

import psycopg2
import websockets
from dotenv import load_dotenv
from psycopg2.extras import execute_values

from realtime_daemon import (
    BYBIT_V5_API,
    KLINE_INTERVAL_MS,
    OKX_V5_API,
    UTC,
    _binance_get,
    _chunks,
    _get,
    _safe_float,
    load_top_assets,
    resolve_exchange_symbols,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DEFAULT_EXCHANGES = ["binance", "bybit", "okx"]
DEFAULT_RECONCILE_DELAY_S = 20
DEFAULT_MAX_STREAMS_PER_CONN = 80
RECONNECT_DELAY_S = 5


KLINE_15M_SQL = """
INSERT INTO futures_klines_15m (
    candle_open_at, candle_close_at, symbol, exchange, base_asset,
    price_open, price_high, price_low, price_close,
    volume_base, volume_usd, buy_volume_base, sell_volume_base,
    volume_delta, txn_count, polled_at,
    source, ws_received_at, rest_reconciled_at, exchange_event_time
)
VALUES %s
ON CONFLICT (candle_open_at, symbol, exchange) DO UPDATE SET
    candle_close_at     = EXCLUDED.candle_close_at,
    base_asset          = EXCLUDED.base_asset,
    price_open          = COALESCE(EXCLUDED.price_open, futures_klines_15m.price_open),
    price_high          = COALESCE(EXCLUDED.price_high, futures_klines_15m.price_high),
    price_low           = COALESCE(EXCLUDED.price_low, futures_klines_15m.price_low),
    price_close         = COALESCE(EXCLUDED.price_close, futures_klines_15m.price_close),
    volume_base         = COALESCE(EXCLUDED.volume_base, futures_klines_15m.volume_base),
    volume_usd          = COALESCE(EXCLUDED.volume_usd, futures_klines_15m.volume_usd),
    buy_volume_base     = COALESCE(EXCLUDED.buy_volume_base, futures_klines_15m.buy_volume_base),
    sell_volume_base    = COALESCE(EXCLUDED.sell_volume_base, futures_klines_15m.sell_volume_base),
    volume_delta        = COALESCE(EXCLUDED.volume_delta, futures_klines_15m.volume_delta),
    txn_count           = COALESCE(EXCLUDED.txn_count, futures_klines_15m.txn_count),
    polled_at           = EXCLUDED.polled_at,
    source              = EXCLUDED.source,
    ws_received_at      = COALESCE(EXCLUDED.ws_received_at, futures_klines_15m.ws_received_at),
    rest_reconciled_at  = COALESCE(EXCLUDED.rest_reconciled_at, futures_klines_15m.rest_reconciled_at),
    exchange_event_time = COALESCE(EXCLUDED.exchange_event_time, futures_klines_15m.exchange_event_time),
    updated_at          = NOW()
"""

KLINE_15M_TEMPLATE = (
    "(%(candle_open_at)s, %(candle_close_at)s, %(symbol)s, %(exchange)s, "
    "%(base_asset)s, %(price_open)s, %(price_high)s, %(price_low)s, "
    "%(price_close)s, %(volume_base)s, %(volume_usd)s, %(buy_volume_base)s, "
    "%(sell_volume_base)s, %(volume_delta)s, %(txn_count)s, %(polled_at)s, "
    "%(source)s, %(ws_received_at)s, %(rest_reconciled_at)s, %(exchange_event_time)s)"
)

KLINE_15M_KEYS = [
    "candle_open_at",
    "candle_close_at",
    "symbol",
    "exchange",
    "base_asset",
    "price_open",
    "price_high",
    "price_low",
    "price_close",
    "volume_base",
    "volume_usd",
    "buy_volume_base",
    "sell_volume_base",
    "volume_delta",
    "txn_count",
    "polled_at",
    "source",
    "ws_received_at",
    "rest_reconciled_at",
    "exchange_event_time",
]


def _dt_from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC)


def _ms_from_dt(ts: datetime) -> int:
    return int(ts.astimezone(UTC).timestamp() * 1000)


def _is_aligned_15m(ts: datetime) -> bool:
    return _ms_from_dt(ts) % KLINE_INTERVAL_MS == 0


def _close_time(open_at: datetime) -> datetime:
    return _dt_from_ms(_ms_from_dt(open_at) + KLINE_INTERVAL_MS)


def most_recent_closed_open_at(now: Optional[datetime] = None, settle_delay_s: int = 20) -> datetime:
    now = now or datetime.now(UTC)
    cutoff_ms = _ms_from_dt(now) - settle_delay_s * 1000
    close_boundary_ms = (cutoff_ms // KLINE_INTERVAL_MS) * KLINE_INTERVAL_MS
    return _dt_from_ms(close_boundary_ms - KLINE_INTERVAL_MS)


def _base_row(
    *,
    candle_open_at: datetime,
    symbol: str,
    exchange: str,
    base_asset: str,
    source: str,
    polled_at: Optional[datetime] = None,
    ws_received_at: Optional[datetime] = None,
    rest_reconciled_at: Optional[datetime] = None,
    exchange_event_time: Optional[datetime] = None,
) -> Optional[dict]:
    if not _is_aligned_15m(candle_open_at):
        log.warning(
            "Rejecting unaligned 15m candle: exchange=%s symbol=%s open=%s",
            exchange,
            symbol,
            candle_open_at.isoformat(),
        )
        return None
    return {
        "candle_open_at": candle_open_at,
        "candle_close_at": _close_time(candle_open_at),
        "symbol": symbol,
        "exchange": exchange,
        "base_asset": base_asset,
        "polled_at": polled_at or datetime.now(UTC),
        "source": source,
        "ws_received_at": ws_received_at,
        "rest_reconciled_at": rest_reconciled_at,
        "exchange_event_time": exchange_event_time,
    }


def normalize_binance_ws(payload: dict, symbol_to_base: Dict[str, str]) -> Optional[dict]:
    data = payload.get("data", payload)
    k = data.get("k") if isinstance(data, dict) else None
    if not k or not k.get("x"):
        return None
    symbol = k.get("s")
    if symbol not in symbol_to_base:
        return None
    received_at = datetime.now(UTC)
    row = _base_row(
        candle_open_at=_dt_from_ms(int(k["t"])),
        symbol=symbol,
        exchange="binance",
        base_asset=symbol_to_base[symbol],
        source="ws",
        ws_received_at=received_at,
        exchange_event_time=_dt_from_ms(int(data.get("E"))) if data.get("E") else None,
    )
    if not row:
        return None
    volume_base = _safe_float(k.get("v"))
    buy_volume_base = _safe_float(k.get("V"))
    sell_volume_base = (
        volume_base - buy_volume_base
        if volume_base is not None and buy_volume_base is not None
        else None
    )
    row.update(
        {
            "price_open": _safe_float(k.get("o")),
            "price_high": _safe_float(k.get("h")),
            "price_low": _safe_float(k.get("l")),
            "price_close": _safe_float(k.get("c")),
            "volume_base": volume_base,
            "volume_usd": _safe_float(k.get("q")),
            "txn_count": int(k["n"]) if k.get("n") is not None else None,
            "buy_volume_base": buy_volume_base,
            "sell_volume_base": sell_volume_base,
            "volume_delta": (
                buy_volume_base - sell_volume_base
                if buy_volume_base is not None and sell_volume_base is not None
                else None
            ),
        }
    )
    return row


def normalize_bybit_ws(payload: dict, symbol_to_base: Dict[str, str]) -> Optional[dict]:
    topic = payload.get("topic", "")
    parts = topic.split(".")
    symbol = parts[-1] if parts else None
    if symbol not in symbol_to_base:
        return None
    candles = payload.get("data") or []
    if not candles:
        return None
    candle = candles[0]
    if not candle.get("confirm"):
        return None
    received_at = datetime.now(UTC)
    row = _base_row(
        candle_open_at=_dt_from_ms(int(candle["start"])),
        symbol=symbol,
        exchange="bybit",
        base_asset=symbol_to_base[symbol],
        source="ws",
        ws_received_at=received_at,
        exchange_event_time=_dt_from_ms(int(candle.get("timestamp") or payload.get("ts")))
        if candle.get("timestamp") or payload.get("ts")
        else None,
    )
    if not row:
        return None
    row.update(
        {
            "price_open": _safe_float(candle.get("open")),
            "price_high": _safe_float(candle.get("high")),
            "price_low": _safe_float(candle.get("low")),
            "price_close": _safe_float(candle.get("close")),
            "volume_base": _safe_float(candle.get("volume")),
            "volume_usd": _safe_float(candle.get("turnover")),
            "txn_count": None,
            "buy_volume_base": None,
            "sell_volume_base": None,
            "volume_delta": None,
        }
    )
    return row


def normalize_okx_ws(payload: dict, inst_to_meta: Dict[str, Tuple[str, str]]) -> Optional[dict]:
    arg = payload.get("arg") or {}
    inst_id = arg.get("instId")
    if inst_id not in inst_to_meta:
        return None
    data = payload.get("data") or []
    if not data:
        return None
    candle = data[0]
    if len(candle) > 8 and str(candle[8]) != "1":
        return None
    base_asset, store_symbol = inst_to_meta[inst_id]
    received_at = datetime.now(UTC)
    row = _base_row(
        candle_open_at=_dt_from_ms(int(candle[0])),
        symbol=store_symbol,
        exchange="okx",
        base_asset=base_asset,
        source="ws",
        ws_received_at=received_at,
        exchange_event_time=received_at,
    )
    if not row:
        return None
    row.update(
        {
            "price_open": _safe_float(candle[1]),
            "price_high": _safe_float(candle[2]),
            "price_low": _safe_float(candle[3]),
            "price_close": _safe_float(candle[4]),
            "volume_base": _safe_float(candle[5]),
            "volume_usd": _safe_float(candle[7]) if len(candle) > 7 else None,
            "txn_count": None,
            "buy_volume_base": None,
            "sell_volume_base": None,
            "volume_delta": None,
        }
    )
    return row


def _dedupe_rows(rows: Sequence[dict]) -> List[dict]:
    deduped: Dict[Tuple[object, object, object], dict] = {}
    for row in rows:
        key = (row.get("candle_open_at"), row.get("symbol"), row.get("exchange"))
        if any(v is None for v in key):
            continue
        deduped[key] = row
    return list(deduped.values())


def upsert_klines_15m(db_url: str, rows: Sequence[dict]) -> None:
    rows = _dedupe_rows(rows)
    if not rows:
        return
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        records = [{k: row.get(k) for k in KLINE_15M_KEYS} for row in rows]
        execute_values(cur, KLINE_15M_SQL, records, template=KLINE_15M_TEMPLATE, page_size=500)
        conn.commit()
        log.info("Upserted %d 15m kline rows.", len(records))
    except Exception as e:
        log.error("15m kline upsert failed: %s", e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def write_klines_15m(db_url: str, rows: Sequence[dict], dry_run: bool = False) -> None:
    rows = _dedupe_rows(rows)
    if not rows:
        return
    if dry_run:
        for row in rows:
            log.info(
                "[DRY RUN] %s %s %s source=%s close=%s price_close=%s volume_base=%s",
                row.get("exchange"),
                row.get("symbol"),
                row.get("candle_open_at").isoformat() if row.get("candle_open_at") else None,
                row.get("source"),
                row.get("candle_close_at").isoformat() if row.get("candle_close_at") else None,
                row.get("price_close"),
                row.get("volume_base"),
            )
        return
    upsert_klines_15m(db_url, rows)


def fetch_binance_rest_kline(base: str, symbol: str, candle_open_at: datetime) -> Optional[dict]:
    open_ms = _ms_from_dt(candle_open_at)
    data = _binance_get(
        "/fapi/v1/klines",
        {"symbol": symbol, "interval": "15m", "startTime": open_ms, "limit": 1},
    )
    if not data or not isinstance(data, list):
        return None
    k = data[0]
    if int(k[0]) != open_ms:
        return None
    volume_base = _safe_float(k[5])
    buy_volume_base = _safe_float(k[9]) if len(k) > 9 else None
    sell_volume_base = (
        volume_base - buy_volume_base
        if volume_base is not None and buy_volume_base is not None
        else None
    )
    row = _base_row(
        candle_open_at=candle_open_at,
        symbol=symbol,
        exchange="binance",
        base_asset=base,
        source="rest_reconcile",
        rest_reconciled_at=datetime.now(UTC),
    )
    if not row:
        return None
    row.update(
        {
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
        }
    )
    return row


def fetch_bybit_rest_kline(base: str, symbol: str, candle_open_at: datetime) -> Optional[dict]:
    open_ms = _ms_from_dt(candle_open_at)
    data = _get(
        f"{BYBIT_V5_API}/market/kline",
        {
            "category": "linear",
            "symbol": symbol,
            "interval": "15",
            "start": open_ms,
            "end": open_ms + KLINE_INTERVAL_MS - 1,
            "limit": 1,
        },
    )
    candles = (data or {}).get("result", {}).get("list", []) if isinstance(data, dict) else []
    if not candles:
        return None
    k = candles[0]
    if int(k[0]) != open_ms:
        return None
    row = _base_row(
        candle_open_at=candle_open_at,
        symbol=symbol,
        exchange="bybit",
        base_asset=base,
        source="rest_reconcile",
        rest_reconciled_at=datetime.now(UTC),
    )
    if not row:
        return None
    row.update(
        {
            "price_open": _safe_float(k[1]),
            "price_high": _safe_float(k[2]),
            "price_low": _safe_float(k[3]),
            "price_close": _safe_float(k[4]),
            "volume_base": _safe_float(k[5]),
            "volume_usd": _safe_float(k[6]) if len(k) > 6 else None,
            "txn_count": None,
            "buy_volume_base": None,
            "sell_volume_base": None,
            "volume_delta": None,
        }
    )
    return row


def fetch_okx_rest_kline(base: str, inst_id: str, candle_open_at: datetime) -> Optional[dict]:
    open_ms = _ms_from_dt(candle_open_at)
    data = _get(
        f"{OKX_V5_API}/market/candles",
        {"instId": inst_id, "bar": "15m", "limit": 4},
    )
    candles = (data or {}).get("data", []) if isinstance(data, dict) else []
    k = next((c for c in candles if c and int(c[0]) == open_ms), None)
    if not k:
        return None
    row = _base_row(
        candle_open_at=candle_open_at,
        symbol=f"{base}USDT",
        exchange="okx",
        base_asset=base,
        source="rest_reconcile",
        rest_reconciled_at=datetime.now(UTC),
    )
    if not row:
        return None
    row.update(
        {
            "price_open": _safe_float(k[1]),
            "price_high": _safe_float(k[2]),
            "price_low": _safe_float(k[3]),
            "price_close": _safe_float(k[4]),
            "volume_base": _safe_float(k[5]),
            "volume_usd": _safe_float(k[7]) if len(k) > 7 else None,
            "txn_count": None,
            "buy_volume_base": None,
            "sell_volume_base": None,
            "volume_delta": None,
        }
    )
    return row


def fetch_rest_reconcile_row(row: dict, symbol_maps: Dict[str, Dict[str, str]]) -> Optional[dict]:
    exchange = row["exchange"]
    symbol = row["symbol"]
    base = row["base_asset"]
    candle_open_at = row["candle_open_at"]
    if exchange == "binance":
        return fetch_binance_rest_kline(base, symbol, candle_open_at)
    if exchange == "bybit":
        return fetch_bybit_rest_kline(base, symbol, candle_open_at)
    if exchange == "okx":
        inst_id = symbol_maps["okx_symbol_to_inst"].get(symbol)
        if not inst_id:
            return None
        return fetch_okx_rest_kline(base, inst_id, candle_open_at)
    return None


def _diff_fields(ws_row: dict, rest_row: dict) -> List[str]:
    fields = [
        "price_open",
        "price_high",
        "price_low",
        "price_close",
        "volume_base",
        "volume_usd",
        "txn_count",
        "buy_volume_base",
        "sell_volume_base",
        "volume_delta",
    ]
    changed = []
    for field in fields:
        if ws_row.get(field) is None or rest_row.get(field) is None:
            continue
        if str(ws_row.get(field)) != str(rest_row.get(field)):
            changed.append(field)
    return changed


async def reconcile_after_delay(
    db_url: str,
    ws_row: dict,
    symbol_maps: Dict[str, Dict[str, str]],
    delay_s: int,
    dry_run: bool = False,
) -> None:
    await asyncio.sleep(delay_s)
    rest_row = await asyncio.to_thread(fetch_rest_reconcile_row, ws_row, symbol_maps)
    if not rest_row:
        log.warning(
            "REST reconcile missing: %s %s %s",
            ws_row["exchange"],
            ws_row["symbol"],
            ws_row["candle_open_at"].isoformat(),
        )
        return
    changed = _diff_fields(ws_row, rest_row)
    if changed:
        log.warning(
            "WS/REST kline divergence: %s %s %s fields=%s",
            ws_row["exchange"],
            ws_row["symbol"],
            ws_row["candle_open_at"].isoformat(),
            ",".join(changed),
        )
    await asyncio.to_thread(write_klines_15m, db_url, [rest_row], dry_run)


async def persist_closed_kline(
    db_url: str,
    row: dict,
    symbol_maps: Dict[str, Dict[str, str]],
    reconcile_delay_s: int,
    dry_run: bool = False,
    once_event: Optional[asyncio.Event] = None,
) -> None:
    latency = (datetime.now(UTC) - row["candle_close_at"]).total_seconds()
    log.info(
        "Closed 15m kline: %s %s %s latency=%.2fs",
        row["exchange"],
        row["symbol"],
        row["candle_open_at"].isoformat(),
        latency,
    )
    await asyncio.to_thread(write_klines_15m, db_url, [row], dry_run)
    asyncio.create_task(reconcile_after_delay(db_url, row, symbol_maps, reconcile_delay_s, dry_run))
    if once_event:
        once_event.set()


async def rest_gap_backstop(
    db_url: str,
    resolved: Dict[str, List[Tuple[str, str]]],
    exchanges: Sequence[str],
    settle_delay_s: int,
    dry_run: bool,
    once_event: Optional[asyncio.Event],
) -> None:
    """Periodically backfill the latest closed candle by official REST open time."""
    while True:
        now = datetime.now(UTC)
        next_close_ms = ((_ms_from_dt(now) // KLINE_INTERVAL_MS) + 1) * KLINE_INTERVAL_MS
        wake_at_ms = next_close_ms + settle_delay_s * 1000
        await asyncio.sleep(max(1.0, (wake_at_ms - _ms_from_dt(now)) / 1000))
        candle_open_at = most_recent_closed_open_at(settle_delay_s=settle_delay_s)
        rows = []
        for exchange in exchanges:
            for base, symbol in resolved.get(exchange, []):
                if exchange == "binance":
                    row = await asyncio.to_thread(fetch_binance_rest_kline, base, symbol, candle_open_at)
                elif exchange == "bybit":
                    row = await asyncio.to_thread(fetch_bybit_rest_kline, base, symbol, candle_open_at)
                elif exchange == "okx":
                    row = await asyncio.to_thread(fetch_okx_rest_kline, base, symbol, candle_open_at)
                else:
                    row = None
                if row:
                    rows.append(row)
        if rows:
            await asyncio.to_thread(write_klines_15m, db_url, rows, dry_run)
            log.info(
                "REST gap backstop wrote %d rows for closed candle %s.",
                len(rows),
                candle_open_at.isoformat(),
            )
        else:
            log.warning("REST gap backstop found no rows for %s.", candle_open_at.isoformat())
        if once_event and once_event.is_set():
            return


async def run_binance_stream(
    db_url: str,
    symbols: List[Tuple[str, str]],
    symbol_maps: Dict[str, Dict[str, str]],
    reconcile_delay_s: int,
    max_streams_per_conn: int,
    dry_run: bool,
    log_first_event: bool,
    once_event: Optional[asyncio.Event],
) -> None:
    symbol_to_base = {sym: base for base, sym in symbols}
    tasks = []
    for batch in _chunks(symbols, max_streams_per_conn):
        streams = "/".join(f"{sym.lower()}@kline_15m" for _, sym in batch)
        url = f"wss://fstream.binance.com/market/stream?streams={streams}"
        tasks.append(
            asyncio.create_task(
                _run_binance_connection(
                    db_url,
                    url,
                    symbol_to_base,
                    symbol_maps,
                    reconcile_delay_s,
                    dry_run,
                    log_first_event,
                    once_event,
                )
            )
        )
    await asyncio.gather(*tasks)


async def _run_binance_connection(
    db_url: str,
    url: str,
    symbol_to_base: Dict[str, str],
    symbol_maps: Dict[str, Dict[str, str]],
    reconcile_delay_s: int,
    dry_run: bool,
    log_first_event: bool,
    once_event: Optional[asyncio.Event],
) -> None:
    logged_first_event = False
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
                async for raw in ws:
                    msg = json.loads(raw)
                    if log_first_event and not logged_first_event:
                        data = msg.get("data", msg)
                        k = data.get("k") if isinstance(data, dict) else None
                        if k:
                            log.info(
                                "[PROBE] Binance event received: symbol=%s open=%s closed=%s",
                                k.get("s"),
                                _dt_from_ms(int(k["t"])).isoformat() if k.get("t") else None,
                                k.get("x"),
                            )
                            logged_first_event = True
                    row = normalize_binance_ws(msg, symbol_to_base)
                    if row:
                        await persist_closed_kline(db_url, row, symbol_maps, reconcile_delay_s, dry_run, once_event)
                    if once_event and once_event.is_set():
                        return
        except Exception as e:
            log.error("Binance kline WS error: %s", e)
            if once_event:
                return
            await asyncio.sleep(RECONNECT_DELAY_S)


async def run_bybit_stream(
    db_url: str,
    symbols: List[Tuple[str, str]],
    symbol_maps: Dict[str, Dict[str, str]],
    reconcile_delay_s: int,
    max_streams_per_conn: int,
    dry_run: bool,
    log_first_event: bool,
    once_event: Optional[asyncio.Event],
) -> None:
    symbol_to_base = {sym: base for base, sym in symbols}
    tasks = []
    for batch in _chunks(symbols, max_streams_per_conn):
        args = [f"kline.15.{sym}" for _, sym in batch]
        tasks.append(
            asyncio.create_task(
                _run_bybit_connection(
                    db_url,
                    args,
                    symbol_to_base,
                    symbol_maps,
                    reconcile_delay_s,
                    dry_run,
                    log_first_event,
                    once_event,
                )
            )
        )
    await asyncio.gather(*tasks)


async def _run_bybit_connection(
    db_url: str,
    args: List[str],
    symbol_to_base: Dict[str, str],
    symbol_maps: Dict[str, Dict[str, str]],
    reconcile_delay_s: int,
    dry_run: bool,
    log_first_event: bool,
    once_event: Optional[asyncio.Event],
) -> None:
    logged_first_event = False
    while True:
        try:
            async with websockets.connect(
                "wss://stream.bybit.com/v5/public/linear",
                ping_interval=20,
                ping_timeout=30,
            ) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                async for raw in ws:
                    msg = json.loads(raw)
                    if msg.get("op") == "ping":
                        await ws.send(json.dumps({"op": "pong"}))
                        continue
                    if log_first_event and not logged_first_event and msg.get("data"):
                        candle = msg["data"][0]
                        log.info(
                            "[PROBE] Bybit event received: topic=%s open=%s closed=%s",
                            msg.get("topic"),
                            _dt_from_ms(int(candle["start"])).isoformat() if candle.get("start") else None,
                            candle.get("confirm"),
                        )
                        logged_first_event = True
                    row = normalize_bybit_ws(msg, symbol_to_base)
                    if row:
                        await persist_closed_kline(db_url, row, symbol_maps, reconcile_delay_s, dry_run, once_event)
                    if once_event and once_event.is_set():
                        return
        except Exception as e:
            log.error("Bybit kline WS error: %s", e)
            if once_event:
                return
            await asyncio.sleep(RECONNECT_DELAY_S)


async def run_okx_stream(
    db_url: str,
    symbols: List[Tuple[str, str]],
    symbol_maps: Dict[str, Dict[str, str]],
    reconcile_delay_s: int,
    max_streams_per_conn: int,
    dry_run: bool,
    log_first_event: bool,
    once_event: Optional[asyncio.Event],
) -> None:
    inst_to_meta = {inst: (base, f"{base}USDT") for base, inst in symbols}
    tasks = []
    for batch in _chunks(symbols, max_streams_per_conn):
        args = [{"channel": "candle15m", "instId": inst} for _, inst in batch]
        tasks.append(
            asyncio.create_task(
                _run_okx_connection(
                    db_url,
                    args,
                    inst_to_meta,
                    symbol_maps,
                    reconcile_delay_s,
                    dry_run,
                    log_first_event,
                    once_event,
                )
            )
        )
    await asyncio.gather(*tasks)


async def _run_okx_connection(
    db_url: str,
    args: List[dict],
    inst_to_meta: Dict[str, Tuple[str, str]],
    symbol_maps: Dict[str, Dict[str, str]],
    reconcile_delay_s: int,
    dry_run: bool,
    log_first_event: bool,
    once_event: Optional[asyncio.Event],
) -> None:
    logged_first_event = False
    while True:
        try:
            async with websockets.connect(
                "wss://ws.okx.com:8443/ws/v5/business",
                ping_interval=20,
                ping_timeout=30,
            ) as ws:
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                async for raw in ws:
                    msg = json.loads(raw)
                    if log_first_event and not logged_first_event and msg.get("data"):
                        candle = msg["data"][0]
                        log.info(
                            "[PROBE] OKX event received: instId=%s open=%s closed=%s",
                            (msg.get("arg") or {}).get("instId"),
                            _dt_from_ms(int(candle[0])).isoformat() if candle else None,
                            candle[8] if len(candle) > 8 else None,
                        )
                        logged_first_event = True
                    row = normalize_okx_ws(msg, inst_to_meta)
                    if row:
                        await persist_closed_kline(db_url, row, symbol_maps, reconcile_delay_s, dry_run, once_event)
                    if once_event and once_event.is_set():
                        return
        except Exception as e:
            log.error("OKX kline WS error: %s", e)
            if once_event:
                return
            await asyncio.sleep(RECONNECT_DELAY_S)


def _build_symbol_maps(resolved: Dict[str, List[Tuple[str, str]]]) -> Dict[str, Dict[str, str]]:
    return {
        "okx_symbol_to_inst": {f"{base}USDT": inst for base, inst in resolved.get("okx", [])},
    }


async def run_daemon(
    db_url: str,
    top_n: Optional[int],
    top_active: Optional[int],
    bases: Optional[Sequence[str]],
    exchanges: Sequence[str],
    reconcile_delay_s: int,
    max_streams_per_conn: int,
    dry_run: bool,
    log_first_event: bool,
    probe_seconds: Optional[int],
    once: bool,
) -> None:
    assets = [base.strip().upper() for base in bases if base.strip()] if bases else load_top_assets(db_url, top_n, top_active=top_active)
    if not assets:
        log.error("No assets loaded; exiting.")
        return
    resolved = resolve_exchange_symbols(assets)
    symbol_maps = _build_symbol_maps(resolved)
    once_event = asyncio.Event() if once else None

    tasks = []
    for exchange in exchanges:
        symbols = resolved.get(exchange, [])
        log.info("Resolved %s kline symbols: %d/%d assets supported", exchange, len(symbols), len(assets))
        if not symbols:
            continue
        if exchange == "binance":
            tasks.append(
                asyncio.create_task(
                    run_binance_stream(
                        db_url,
                        symbols,
                        symbol_maps,
                        reconcile_delay_s,
                        max_streams_per_conn,
                        dry_run,
                        log_first_event,
                        once_event,
                    )
                )
            )
        elif exchange == "bybit":
            tasks.append(
                asyncio.create_task(
                    run_bybit_stream(
                        db_url,
                        symbols,
                        symbol_maps,
                        reconcile_delay_s,
                        max_streams_per_conn,
                        dry_run,
                        log_first_event,
                        once_event,
                    )
                )
            )
        elif exchange == "okx":
            tasks.append(
                asyncio.create_task(
                    run_okx_stream(
                        db_url,
                        symbols,
                        symbol_maps,
                        reconcile_delay_s,
                        max_streams_per_conn,
                        dry_run,
                        log_first_event,
                        once_event,
                    )
                )
            )
        else:
            log.warning("Unknown exchange: %s", exchange)

    if not tasks:
        log.error("No websocket tasks started; exiting.")
        return

    tasks.append(
        asyncio.create_task(rest_gap_backstop(db_url, resolved, exchanges, reconcile_delay_s, dry_run, once_event))
    )

    if once_event:
        await once_event.wait()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        return
    if probe_seconds:
        await asyncio.sleep(probe_seconds)
        log.info("Probe window elapsed after %ds; stopping daemon.", probe_seconds)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        return
    await asyncio.gather(*tasks)


def main() -> None:
    parser = argparse.ArgumentParser(description="Futures 15m kline WebSocket daemon")
    parser.add_argument("--top", type=int, default=None, help="(Legacy) limit assets by current rank")
    parser.add_argument("--top-active", type=int, default=None, dest="top_active", help="Restrict to top N active assets")
    parser.add_argument("--bases", type=str, default="", help="Comma-separated base assets; bypasses DB asset loading")
    parser.add_argument("--exchanges", type=str, default=",".join(DEFAULT_EXCHANGES), help="Comma-separated exchange list")
    parser.add_argument("--reconcile-delay", type=int, default=DEFAULT_RECONCILE_DELAY_S, help="REST reconcile delay after WS close")
    parser.add_argument("--max-streams-per-conn", type=int, default=DEFAULT_MAX_STREAMS_PER_CONN, help="Subscription batch size")
    parser.add_argument("--dry-run", action="store_true", help="Log normalized rows without writing to the database")
    parser.add_argument("--log-first-event", action="store_true", help="Log the first open/closed WebSocket kline event per connection")
    parser.add_argument("--probe-seconds", type=int, default=None, help="Stop after N seconds; useful with --dry-run")
    parser.add_argument("--once", action="store_true", help="Exit after first closed WS kline")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        if args.dry_run:
            db_url = "dry-run"
        else:
            log.error("DATABASE_URL environment variable is not set.")
            sys.exit(1)

    exchanges = [e.strip().lower() for e in args.exchanges.split(",") if e.strip()]
    bases = [b.strip().upper() for b in args.bases.split(",") if b.strip()] if args.bases else None
    log.info(
        "Starting futures_ws_daemon — exchanges=%s, bases=%s, top=%s, top_active=%s, reconcile_delay=%ds, dry_run=%s",
        exchanges,
        bases,
        args.top,
        args.top_active,
        args.reconcile_delay,
        args.dry_run,
    )
    asyncio.run(
        run_daemon(
            db_url=db_url,
            top_n=args.top,
            top_active=args.top_active,
            bases=bases,
            exchanges=exchanges,
            reconcile_delay_s=args.reconcile_delay,
            max_streams_per_conn=args.max_streams_per_conn,
            dry_run=args.dry_run,
            log_first_event=args.log_first_event,
            probe_seconds=args.probe_seconds,
            once=args.once,
        )
    )


if __name__ == "__main__":
    main()
