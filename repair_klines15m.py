"""Detecta y repara barras 15m ausentes en `futures_klines_15m` (pool alts).

Para qué: los colectores (ws + rest_reconcile) dejan huecos cuando estan parados
(2026-05-14 binance 16:45->19:45; 2026-09-22..25; bybit/okx con agujeros de
30-165 min). Aqui se detectan los huecos INTERIORES (barras consecutivas
existentes con paso > 15 min; nunca se inventa nada antes de la primera barra ni
despues de la ultima) y se rellenan con datos reales del propio exchange.

Port del reparador de GLI-CLI-Estimation (backend/backfills/repair_klines15m.py)
sin el corpus 1m de GLI: aqui todas las fuentes son REST publicas.

Fuentes:
  binance : `/fapi/v1/klines` 15m.
  bybit   : `/v5/market/kline?category=linear&interval=15` (volume=base,
            turnover=USD).
  okx     : `/api/v5/market/history-candles` bar=15m, instId `{BASE}-USDT-SWAP`
            (volCcy = base, volCcyQuote = USD; `vol` son contratos). Las filas OKX existentes
            guardan volume_base = `vol` (contratos) y NO base cuando ctVal != 1
            (HBAR ctVal=100, COMP 0.1, MOVE 10...): la validacion prueba volCcy y,
            si no reproduce las filas, `vol`, y se inserta la variante que cuadra.
Solo GET publicos, sin claves, <= 5 req/s, reintentos con backoff en 429/5xx.

Validacion semantica OBLIGATORIA antes de insertar: para cada hueco se comparan
hasta 8 barras vecinas a cada lado QUE YA EXISTEN en la DB contra la misma
fuente (OHLC rel 1e-6; volume_base / volume_usd / buy_volume_base rel 1 %).
Si la fuente no reproduce al menos el 90 % de ellas (minimo 2 comparadas) el
hueco se omite y se informa del desajuste; asi se confirman las unidades en vez
de suponerlas. Una barra que el exchange no devuelve (mantenimiento, deslistado)
o que es incoherente (no finita, h<max(o,c), ...) queda como irrecuperable:
nunca se rellena hacia delante ni se fabrica.

Insercion (solo con --apply): INSERT ... ON CONFLICT DO NOTHING, una transaccion
por (exchange, lote). source = 'gap_repair_rest';
polled_at = created_at = updated_at = now() (PIT honesto: ninguna captura previa
las vio); ws_received_at / rest_reconciled_at / exchange_event_time NULL;
base_asset copiado de la fila vecina. bybit/okx mantienen buy/sell/delta/txn en
NULL. Cada --apply escribe un manifiesto JSON (lote a lote) y `--rollback
<manifiesto>` borra exactamente esas claves Y source LIKE 'gap_repair_%'.
Nunca actualiza ni borra otras filas.

Modo diario (--recent-days N, lo usa run_pipeline.py): solo huecos cuya barra
siguiente existente esta en los ultimos N dias. La barra anterior se busca hasta
RECENT_PREV_MARGIN_DAYS antes, asi un corte largo del demonio tambien se detecta
en cuanto el demonio vuelve a escribir. Un hueco que llega hasta ahora (sin barra
posterior) no es interior y se repara en la corrida siguiente a la reanudacion.

Uso (desde la raiz del repo; DATABASE_URL de .env):
  python repair_klines15m.py                       # --detect
  python repair_klines15m.py --detect --exchange okx --since 2026-05-01
  python repair_klines15m.py --repair              # dry-run
  python repair_klines15m.py --repair --apply
  python repair_klines15m.py --repair --apply --recent-days 3 --max-gaps 300   # paso diario
  python repair_klines15m.py --rollback <manifest.json> [--apply]

Codigos de salida: 0 = ejecucion completada (deteccion, dry-run o apply sin
huecos omitidos); 1 = error fatal (DB, E/S); 2 = uso incorrecto (argparse);
3 = completada pero quedan huecos omitidos / irrecuperables / sin validar.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

_HERE = os.path.dirname(os.path.abspath(__file__))

logger = logging.getLogger("repair_klines15m")

UTC = timezone.utc
TABLE = "futures_klines_15m"
EXCHANGES = ("binance", "bybit", "okx")
STEP_MS = 900_000
MS_MINUTE = 60_000
NEIGHBOR_BARS = 8
MIN_COMPARED = 2
MIN_MATCH_RATIO = 0.9
PRICE_RTOL = 1e-6
VOLUME_RTOL = 0.01
MAX_RPS = 5.0
SRC_REST = "gap_repair_rest"
DEFAULT_OUT_DIR = Path(_HERE) / "logs" / "repair_klines15m"  # logs/ esta en .gitignore
RECENT_PREV_MARGIN_DAYS = 30  # --recent-days: hasta donde se busca la barra anterior al hueco
INSERT_BATCH = 1000

EXIT_OK, EXIT_FATAL, EXIT_PARTIAL = 0, 1, 3


# --------------------------------------------------------------------------- utilidades

def ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


def dt_to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000)


def iso(ms: int) -> str:
    return ms_to_dt(ms).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_when(text: str) -> datetime:
    """`YYYY-MM-DD` o ISO completo, siempre UTC."""
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def _dec(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return d if d.is_finite() else None


def _fdec(x: float, places: int) -> Decimal | None:
    if x is None or not math.isfinite(x):
        return None
    return Decimal(f"{x:.{places}f}")


# --------------------------------------------------------------------------- barras y parseo

@dataclass(frozen=True)
class Bar:
    t: int  # open time ms
    o: Decimal
    h: Decimal
    l: Decimal
    c: Decimal
    vb: Decimal  # volume_base
    vq: Decimal  # volume_usd
    buy: Decimal | None = None
    trades: int | None = None
    vb_alt: Decimal | None = None  # volumen alternativo (OKX: `vol` en contratos); ver plan_symbol


def bar_is_coherent(b: Bar) -> bool:
    """Finita, positiva y con OHLC consistente; volumenes >= 0."""
    try:
        vals = [b.o, b.h, b.l, b.c, b.vb, b.vq]
        if any(v is None or not v.is_finite() for v in vals):
            return False
        if b.buy is not None and (not b.buy.is_finite() or b.buy < 0 or b.buy > b.vb * Decimal("1.0001") + Decimal("1e-8")):
            return False
        if b.trades is not None and b.trades < 0:
            return False
        return (b.t % STEP_MS == 0 and min(b.o, b.h, b.l, b.c) > 0 and b.h >= max(b.o, b.c, b.l)
                and b.l <= min(b.o, b.c, b.h) and b.vb >= 0 and b.vq >= 0)
    except (InvalidOperation, TypeError):
        return False


def _complete(t: int, now_ms: int | None) -> bool:
    return now_ms is None or t + STEP_MS <= now_ms


def parse_binance_klines(raw: Sequence[Sequence[Any]], now_ms: int | None = None) -> list[Bar]:
    """[openTime,o,h,l,c,vol,closeTime,quoteVol,count,takerBuyBase,takerBuyQuote,_]."""
    out = []
    for r in raw:
        t = int(r[0])
        if not _complete(t, now_ms):
            continue
        o, h, l, c, vb, vq, buy = (_dec(r[i]) for i in (1, 2, 3, 4, 5, 7, 9))
        if None in (o, h, l, c, vb, vq):
            continue
        out.append(Bar(t, o, h, l, c, vb, vq, buy, int(r[8])))
    return out


def parse_bybit_klines(payload: Mapping[str, Any], now_ms: int | None = None) -> list[Bar]:
    """result.list: [start,o,h,l,c,volume(base),turnover(USD)] en orden descendente."""
    out = []
    for r in (payload.get("result") or {}).get("list") or []:
        t = int(r[0])
        if not _complete(t, now_ms):
            continue
        o, h, l, c, vb, vq = (_dec(r[i]) for i in range(1, 7))
        if None in (o, h, l, c, vb, vq):
            continue
        out.append(Bar(t, o, h, l, c, vb, vq))
    return out


def parse_okx_candles(payload: Mapping[str, Any], now_ms: int | None = None) -> list[Bar]:
    """data: [ts,o,h,l,c,vol(contratos),volCcy(base),volCcyQuote(USD),confirm]; solo confirm == '1'."""
    out = []
    for r in payload.get("data") or []:
        t = int(r[0])
        if len(r) > 8 and str(r[8]) != "1":
            continue
        if not _complete(t, now_ms):
            continue
        o, h, l, c, vb, vq = _dec(r[1]), _dec(r[2]), _dec(r[3]), _dec(r[4]), _dec(r[6]), _dec(r[7])
        if None in (o, h, l, c, vb, vq):
            continue
        out.append(Bar(t, o, h, l, c, vb, vq, None, None, _dec(r[5])))
    return out


# --------------------------------------------------------------------------- huecos

@dataclass(frozen=True)
class Gap:
    exchange: str
    symbol: str
    prev_ms: int  # ultima barra existente antes del hueco
    next_ms: int  # primera barra existente despues

    def missing_times(self) -> list[int]:
        return list(range(self.prev_ms + STEP_MS, self.next_ms, STEP_MS))

    @property
    def missing(self) -> int:
        return max(0, (self.next_ms - self.prev_ms) // STEP_MS - 1)


def find_gaps_in_series(exchange: str, symbol: str, times_ms: Iterable[int]) -> list[Gap]:
    """Huecos interiores: pares consecutivos de barras existentes con paso > 15 min."""
    ts = sorted(set(times_ms))
    return [Gap(exchange, symbol, a, b) for a, b in zip(ts, ts[1:]) if b - a > STEP_MS]


_DETECT_SQL = """
SELECT symbol, prev_at, candle_open_at FROM (
    SELECT symbol, candle_open_at,
           lag(candle_open_at) OVER (PARTITION BY symbol ORDER BY candle_open_at) AS prev_at
    FROM futures_klines_15m
    WHERE exchange = %s AND candle_open_at >= %s AND candle_open_at < %s {sym}
) t
WHERE candle_open_at - prev_at > interval '15 minutes' AND candle_open_at >= %s
ORDER BY symbol, candle_open_at
"""


def detect_gaps(cur: Any, exchange: str, since: datetime, until: datetime,
                symbols: Sequence[str] | None = None, prev_margin: timedelta = timedelta(0)) -> list[Gap]:
    """Huecos cuya barra siguiente cae en [since, until); la anterior se busca desde since - prev_margin."""
    params: list[Any] = [exchange, since - prev_margin, until]
    sym = ""
    if symbols:
        sym = "AND symbol = ANY(%s)"
        params.append(list(symbols))
    params.append(since)
    cur.execute(_DETECT_SQL.format(sym=sym), params)
    return [Gap(exchange, s, dt_to_ms(a), dt_to_ms(b)) for s, a, b in cur.fetchall()]


def summarize_gaps(gaps: Sequence[Gap]) -> dict[str, Any]:
    by_month: dict[str, int] = defaultdict(int)
    for g in gaps:
        for t in g.missing_times():
            by_month[ms_to_dt(t).strftime("%Y-%m")] += 1
    return {"gaps": len(gaps), "missing_bars": sum(g.missing for g in gaps),
            "symbols": len({g.symbol for g in gaps}), "by_month": dict(sorted(by_month.items()))}


# --------------------------------------------------------------------------- validacion

_PRICE_FIELDS = (("price_open", "o"), ("price_high", "h"), ("price_low", "l"), ("price_close", "c"))


def _rel_close(a: Any, b: Any, rtol: float, atol: float = 1e-12) -> bool:
    a, b = float(a), float(b)
    return abs(a - b) <= rtol * max(abs(a), abs(b)) + atol


# Escala de las columnas numeric(24,s): la DB redondea al insertar (p. ej. PEPE 0.000004335 -> 0.00000434),
# asi que una diferencia de media unidad de escala NO es un desajuste.
_ATOL_PRICE = 0.6e-8
_ATOL_VOL_BASE = 0.6e-8
_ATOL_VOL_USD = 0.6e-4


def _q(x: Decimal | None, places: int) -> Decimal | None:
    return None if x is None else x.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def compare_bar(db_row: Mapping[str, Any], bar: Bar, exchange: str) -> list[str]:
    """Campos que NO coinciden entre una fila DB existente y la barra de la fuente."""
    bad = []
    for col, attr in _PRICE_FIELDS:
        if db_row.get(col) is None or not _rel_close(db_row[col], getattr(bar, attr), PRICE_RTOL, _ATOL_PRICE):
            bad.append(col)
    for col, attr, atol in (("volume_base", "vb", _ATOL_VOL_BASE), ("volume_usd", "vq", _ATOL_VOL_USD)):
        if db_row.get(col) is None or not _rel_close(db_row[col], getattr(bar, attr), VOLUME_RTOL, atol):
            bad.append(col)
    if exchange == "binance" and db_row.get("buy_volume_base") is not None and bar.buy is not None:
        if not _rel_close(db_row["buy_volume_base"], bar.buy, VOLUME_RTOL, _ATOL_VOL_BASE):
            bad.append("buy_volume_base")
    return bad


@dataclass
class Validation:
    ok: bool
    compared: int
    matched: int
    reason: str = ""
    mismatches: tuple = ()


def validate_gap(gap: Gap, db_rows: Mapping[int, Mapping[str, Any]], src: Mapping[int, Bar],
                 n: int = NEIGHBOR_BARS) -> Validation:
    """Compara las <= n barras DB existentes a cada lado del hueco con la fuente."""
    times = [gap.prev_ms - i * STEP_MS for i in range(n)] + [gap.next_ms + i * STEP_MS for i in range(n)]
    compared = matched = 0
    mism: list[Any] = []
    absent = 0
    for t in times:
        row = db_rows.get(t)
        if row is None:
            continue
        bar = src.get(t)
        if bar is None:
            absent += 1
            continue
        compared += 1
        bad = compare_bar(row, bar, gap.exchange)
        if bad:
            mism.append((iso(t), bad))
        else:
            matched += 1
    if compared < MIN_COMPARED:
        return Validation(False, compared, matched, f"insufficient_validation_bars(compared={compared},absent_in_source={absent})")
    if matched / compared < MIN_MATCH_RATIO:
        return Validation(False, compared, matched, f"source_mismatch({matched}/{compared})", tuple(mism[:4]))
    return Validation(True, compared, matched, "", tuple(mism[:4]))


# --------------------------------------------------------------------------- HTTP / proveedores

class SourceUnavailable(Exception):
    """El exchange no sirve ese simbolo/rango (instrumento inexistente, error persistente)."""


class RateLimiter:
    def __init__(self, rps: float = MAX_RPS, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep):
        self.interval, self._clock, self._sleep = 1.0 / rps, clock, sleep
        self._next = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = self._clock()
            delay = self._next - now
            self._next = max(now, self._next) + self.interval
        if delay > 0:
            self._sleep(delay)


class Http:
    """GET JSON con limite de ritmo y backoff en 429/418/5xx/red."""

    def __init__(self, limiter: RateLimiter | None = None, retries: int = 6, sleep: Callable[[float], None] = time.sleep):
        self.limiter, self.retries, self._sleep = limiter or RateLimiter(), retries, sleep
        self.requests = 0

    def get(self, url: str, params: Mapping[str, Any]) -> Any:
        full = f"{url}?{urllib.parse.urlencode(params)}"
        delay = 1.0
        for attempt in range(self.retries):
            self.limiter.wait()
            self.requests += 1
            try:
                req = urllib.request.Request(full, headers={"User-Agent": "gli-repair-klines15m/1.0"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                if exc.code in (429, 418) or exc.code >= 500:
                    self._sleep(delay)
                    delay = min(delay * 2, 30)
                    continue
                body = ""
                try:
                    body = exc.read().decode("utf-8", "replace")[:200]
                except Exception:  # noqa: BLE001
                    pass
                raise SourceUnavailable(f"http_{exc.code}:{body}") from exc
            except (urllib.error.URLError, TimeoutError, ConnectionError, json.JSONDecodeError):
                self._sleep(delay)
                delay = min(delay * 2, 30)
        raise SourceUnavailable("retries_exhausted")


class RestProvider:
    label = SRC_REST

    def __init__(self, exchange: str, http: Http, now_ms: Callable[[], int] | None = None):
        self.exchange, self.http = exchange, http
        self._now = now_ms or (lambda: int(time.time() * 1000))

    def source_symbol(self, symbol: str, base: str | None) -> str:
        if self.exchange == "okx":
            if not base:
                raise SourceUnavailable("okx_base_asset_unknown")
            return f"{base}-USDT-SWAP"
        return symbol  # binance y bybit usan el mismo ticker que la DB (incl. 1000PEPEUSDT)

    def fetch(self, symbol: str, base: str | None, lo_ms: int, hi_ms: int) -> dict[int, Bar]:
        """Barras con apertura en [lo_ms, hi_ms] (ambos inclusive)."""
        sym = self.source_symbol(symbol, base)
        now = self._now()
        out: dict[int, Bar] = {}
        if self.exchange == "binance":
            cursor = lo_ms
            while cursor <= hi_ms:
                raw = self.http.get("https://fapi.binance.com/fapi/v1/klines", {
                    "symbol": sym, "interval": "15m", "startTime": cursor, "endTime": hi_ms + STEP_MS - 1, "limit": 1500})
                if isinstance(raw, dict):
                    raise SourceUnavailable(f"binance_error:{raw.get('code')}:{raw.get('msg')}")
                bars = parse_binance_klines(raw, now)
                for b in bars:
                    out[b.t] = b
                if not raw or len(raw) < 1500:
                    break
                cursor = int(raw[-1][0]) + STEP_MS
        elif self.exchange == "bybit":
            end = hi_ms
            while end >= lo_ms:
                for attempt in range(4):  # 10016/10006: error transitorio del servicio / limite
                    raw = self.http.get("https://api.bybit.com/v5/market/kline", {
                        "category": "linear", "symbol": sym, "interval": "15", "start": lo_ms, "end": end, "limit": 1000})
                    if raw.get("retCode") in (10016, 10006) and attempt < 3:
                        self.http._sleep(2.0 * (attempt + 1))
                        continue
                    break
                if raw.get("retCode") != 0:
                    raise SourceUnavailable(f"bybit_error:{raw.get('retCode')}:{raw.get('retMsg')}")
                lst = (raw.get("result") or {}).get("list") or []
                for b in parse_bybit_klines(raw, now):
                    out[b.t] = b
                if len(lst) < 1000:
                    break
                end = min(int(r[0]) for r in lst) - STEP_MS
        elif self.exchange == "okx":
            after = hi_ms + STEP_MS  # `after`: registros con ts < after
            while after > lo_ms:
                raw = self.http.get("https://www.okx.com/api/v5/market/history-candles", {
                    "instId": sym, "bar": "15m", "after": after, "limit": 100})
                if str(raw.get("code")) != "0":
                    raise SourceUnavailable(f"okx_error:{raw.get('code')}:{raw.get('msg')}")
                data = raw.get("data") or []
                for b in parse_okx_candles(raw, now):
                    if b.t >= lo_ms:
                        out[b.t] = b
                if not data:
                    break
                oldest = min(int(r[0]) for r in data)
                if oldest >= after or oldest <= lo_ms:
                    break
                after = oldest
        else:
            raise SourceUnavailable(f"unknown_exchange:{self.exchange}")
        return out


# --------------------------------------------------------------------------- planificacion

_DB_COLS = ("candle_open_at, symbol, exchange, base_asset, price_open, price_high, price_low, price_close, "
            "volume_base, volume_usd, buy_volume_base, txn_count, source")
_DB_KEYS = [c.strip() for c in _DB_COLS.split(",")]


def fetch_db_window(cur: Any, exchange: str, symbol: str, lo_ms: int, hi_ms: int) -> dict[int, dict[str, Any]]:
    cur.execute(f"SELECT {_DB_COLS} FROM {TABLE} WHERE exchange = %s AND symbol = %s "
                "AND candle_open_at >= %s AND candle_open_at <= %s",
                (exchange, symbol, ms_to_dt(lo_ms), ms_to_dt(hi_ms)))
    out = {}
    for r in cur.fetchall():
        row = dict(zip(_DB_KEYS, r))
        out[dt_to_ms(row["candle_open_at"])] = row
    return out


def merge_windows(gaps: Sequence[Gap], n: int = NEIGHBOR_BARS) -> list[tuple[int, int, list[Gap]]]:
    """Ventanas [prev-n, next+n] fusionadas cuando solapan; devuelve (lo, hi, gaps)."""
    wins: list[list[Any]] = []
    for g in sorted(gaps, key=lambda g: g.prev_ms):
        lo, hi = g.prev_ms - n * STEP_MS, g.next_ms + n * STEP_MS
        if wins and lo <= wins[-1][1]:
            wins[-1][1] = max(wins[-1][1], hi)
            wins[-1][2].append(g)
        else:
            wins.append([lo, hi, [g]])
    return [(w[0], w[1], w[2]) for w in wins]


def build_row(exchange: str, symbol: str, base: str | None, bar: Bar, source: str) -> dict[str, Any]:
    row = {"candle_open_at": ms_to_dt(bar.t), "candle_close_at": ms_to_dt(bar.t + STEP_MS), "symbol": symbol,
           "exchange": exchange, "base_asset": base, "price_open": _q(bar.o, 8), "price_high": _q(bar.h, 8),
           "price_low": _q(bar.l, 8), "price_close": _q(bar.c, 8), "volume_base": _q(bar.vb, 8),
           "volume_usd": _q(bar.vq, 4), "buy_volume_base": None,
           "sell_volume_base": None, "volume_delta": None, "txn_count": None, "source": source}
    if exchange == "binance" and bar.buy is not None:
        row["buy_volume_base"] = _q(bar.buy, 8)
        row["sell_volume_base"] = _q(bar.vb - bar.buy, 8)
        row["volume_delta"] = _q(2 * bar.buy - bar.vb, 8)
        row["txn_count"] = bar.trades
    return row


def plan_symbol(exchange: str, symbol: str, gaps: Sequence[Gap], db_window: Callable[[int, int], dict[int, dict[str, Any]]],
                providers: Sequence[Any]) -> dict[str, Any]:
    """Plan de reparacion de un simbolo. Devuelve filas a insertar y resultado por hueco."""
    rows: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    for lo, hi, wgaps in merge_windows(gaps):
        db_rows = db_window(lo, hi)
        base = next((r["base_asset"] for r in db_rows.values() if r.get("base_asset")), None)
        src_cache: dict[int, dict[int, Bar] | str] = {}

        def get_src(pi: int):
            if pi not in src_cache:
                try:
                    src_cache[pi] = providers[pi].fetch(symbol, base, lo, hi)
                except SourceUnavailable as exc:
                    src_cache[pi] = f"source_unavailable:{exc}"
            return src_cache[pi]

        for g in wgaps:
            pending = set(g.missing_times())
            accepted: dict[int, tuple[Bar, str]] = {}
            reasons: list[str] = []
            units: dict[int, str] = {}
            for pi, prov in enumerate(providers):
                if not pending:
                    break
                src = get_src(pi)
                if isinstance(src, str):
                    reasons.append(f"{prov.label}:{src}")
                    continue
                v = validate_gap(g, db_rows, src)
                unit = "base"
                if not v.ok and any(b.vb_alt is not None for b in src.values()):
                    alt = {k: replace(b, vb=b.vb_alt) for k, b in src.items() if b.vb_alt is not None}
                    v2 = validate_gap(g, db_rows, alt)
                    if v2.ok:  # las filas existentes usan el volumen alternativo (contratos OKX)
                        src, v, unit = alt, v2, "contracts"
                if not v.ok:
                    reasons.append(f"{prov.label}:{v.reason}" + (f" {list(v.mismatches)}" if v.mismatches else ""))
                    continue
                missing_here = 0
                for t in sorted(pending):
                    b = src.get(t)
                    if b is None:
                        missing_here += 1
                        continue
                    if not bar_is_coherent(b):
                        reasons.append(f"{prov.label}:incoherent_bar@{iso(t)}")
                        continue
                    accepted[t] = (b, prov.label)
                    units[t] = unit
                if missing_here:
                    reasons.append(f"{prov.label}:bar_not_returned x{missing_here}")
                pending -= set(accepted)
            for t, (b, label) in accepted.items():
                rows.append(build_row(exchange, symbol, base, b, label))
            reason = ("; ".join(reasons) or "not_returned_by_source") if pending else ""
            results.append({"symbol": symbol, "prev": iso(g.prev_ms), "next": iso(g.next_ms), "missing": g.missing,
                            "repaired": len(accepted), "units": sorted(set(units.values())), "unrecoverable": len(pending), "reason": reason,
                            "unrecoverable_times": [iso(t) for t in sorted(pending)][:20]})
    return {"rows": rows, "results": results}


def source_label_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = defaultdict(int)
    for r in rows:
        out[r["source"]] += 1
    return dict(out)


# --------------------------------------------------------------------------- escritura

_INSERT_SQL = (
    f"INSERT INTO {TABLE} (candle_open_at, candle_close_at, symbol, exchange, base_asset, price_open, price_high, "
    "price_low, price_close, volume_base, volume_usd, buy_volume_base, sell_volume_base, volume_delta, txn_count, "
    "polled_at, created_at, updated_at, source, ws_received_at, rest_reconciled_at, exchange_event_time) VALUES %s "
    "ON CONFLICT (candle_open_at, symbol, exchange) DO NOTHING "
    "RETURNING candle_open_at, symbol, exchange")
_INSERT_TEMPLATE = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now(),now(),%s,NULL,NULL,NULL)"
_ROW_ORDER = ("candle_open_at", "candle_close_at", "symbol", "exchange", "base_asset", "price_open", "price_high",
              "price_low", "price_close", "volume_base", "volume_usd", "buy_volume_base", "sell_volume_base",
              "volume_delta", "txn_count", "source")


def _values_execute(cur: Any, sql: str, rows: Sequence[Sequence[Any]], template: str) -> list[Any]:
    from psycopg2.extras import execute_values
    return execute_values(cur, sql, rows, template=template, fetch=True)


def insert_batch(cur: Any, rows: Sequence[Mapping[str, Any]],
                 executor: Callable[..., list[Any]] = _values_execute) -> list[tuple[int, str, str]]:
    """Inserta con ON CONFLICT DO NOTHING; devuelve las claves realmente insertadas."""
    if not rows:
        return []
    tuples = [tuple(r[k] for k in _ROW_ORDER) for r in rows]
    got = executor(cur, _INSERT_SQL, tuples, _INSERT_TEMPLATE)
    return [(dt_to_ms(a), s, e) for a, s, e in got]


def manifest_keys_for_rollback(manifest: Mapping[str, Any]) -> list[tuple[datetime, str, str]]:
    """Claves (candle_open_at, symbol, exchange) del manifiesto, sin duplicados."""
    seen, out = set(), []
    for b in manifest.get("batches", []):
        for t_ms, symbol, exchange in b.get("keys", []):
            k = (int(t_ms), symbol, exchange)
            if k not in seen:
                seen.add(k)
                out.append((ms_to_dt(int(t_ms)), symbol, exchange))
    return out


def rollback_keys(cur: Any, keys: Sequence[tuple[datetime, str, str]],
                  executor: Callable[..., list[Any]] | None = None) -> int:
    """Borra EXACTAMENTE esas claves y solo si source LIKE 'gap_repair_%'."""
    deleted = 0
    for i in range(0, len(keys), INSERT_BATCH):
        chunk = keys[i:i + INSERT_BATCH]
        sql = (f"DELETE FROM {TABLE} t USING (VALUES %s) AS k(candle_open_at, symbol, exchange) "
               "WHERE t.candle_open_at = k.candle_open_at::timestamptz AND t.symbol = k.symbol AND t.exchange = k.exchange "
               "AND left(t.source, 11) = 'gap_repair_' RETURNING 1")
        got = (executor or _values_execute)(cur, sql, chunk, "(%s,%s,%s)")
        deleted += len(got)
    return deleted


class _DB:
    def __init__(self) -> None:
        import psycopg2
        url = os.getenv("DATABASE_URL")
        if not url:
            raise RuntimeError("DATABASE_URL no definida")
        self.conn = psycopg2.connect(url)

    def close(self) -> None:
        try:
            self.conn.rollback()
        finally:
            self.conn.close()


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(_HERE, ".env"))
    except ImportError:
        pass


# --------------------------------------------------------------------------- orquestacion

def run_detect(db_cur: Any, exchanges: Sequence[str], since: datetime, until: datetime,
               symbols: Sequence[str] | None, prev_margin: timedelta = timedelta(0)) -> dict[str, list[Gap]]:
    return {ex: detect_gaps(db_cur, ex, since, until, symbols, prev_margin) for ex in exchanges}


def print_summary(title: str, gaps_by_ex: Mapping[str, Sequence[Gap]]) -> dict[str, Any]:
    out = {}
    print(f"== {title}")
    for ex, gaps in gaps_by_ex.items():
        s = summarize_gaps(gaps)
        out[ex] = s
        print(f"[{ex}] gaps={s['gaps']} missing_bars={s['missing_bars']} symbols={s['symbols']}")
        months = s["by_month"]
        top = sorted(months.items(), key=lambda kv: -kv[1])[:12]
        print("   by_month (top 12 by bars): " + ", ".join(f"{m}={n}" for m, n in sorted(top)))
    return out


def gaps_to_json(gaps: Sequence[Gap]) -> list[list[Any]]:
    return [[g.symbol, iso(g.prev_ms), iso(g.next_ms), g.missing] for g in gaps]


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, indent=1, default=str))
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def run_repair(db: Any, gaps_by_ex: Mapping[str, Sequence[Gap]], *, apply: bool, run_id: str, out_dir: Path,
               http: Http | None = None, max_gaps: int | None = None) -> dict[str, Any]:
    http = http or Http()
    report: dict[str, Any] = {"run_id": run_id, "apply": apply, "exchanges": {}}
    manifest_path = out_dir / f"manifest_{run_id}.json"
    manifest = {"run_id": run_id, "created_at": datetime.now(UTC).isoformat(), "table": TABLE, "batches": []}
    cur = db.conn.cursor()
    cur.execute("SET statement_timeout = 0")
    for ex, gaps in gaps_by_ex.items():
        omitted_gaps: list[Gap] = []
        if max_gaps is not None:
            omitted_gaps = list(gaps)[max_gaps:]
            gaps = list(gaps)[:max_gaps]
        providers: list[Any] = [RestProvider(ex, http)]
        by_symbol: dict[str, list[Gap]] = defaultdict(list)
        for g in gaps:
            by_symbol[g.symbol].append(g)
        all_rows: list[dict[str, Any]] = []
        results: list[dict[str, Any]] = []
        for i, (symbol, sg) in enumerate(sorted(by_symbol.items()), 1):
            plan = plan_symbol(ex, symbol, sg, lambda lo, hi, s=symbol, e=ex: fetch_db_window(cur, e, s, lo, hi), providers)
            all_rows.extend(plan["rows"])
            results.extend(plan["results"])
            if i % 20 == 0:
                print(f"  [{ex}] {i}/{len(by_symbol)} symbols planned, rows={len(all_rows)}, http={http.requests}", flush=True)
        db.conn.rollback()
        rep = {"gaps": len(gaps), "missing_bars": sum(g.missing for g in gaps), "planned_rows": len(all_rows),
               "by_source": source_label_counts(all_rows),
               "unrecoverable_bars": sum(r["unrecoverable"] for r in results),
               "gaps_with_unrecoverable": sum(1 for r in results if r["unrecoverable"]),
               "unrecoverable_reasons": _reason_histogram(results), "inserted": 0, "results": results,
               "omitted_gaps": len(omitted_gaps), "omitted_bars": sum(g.missing for g in omitted_gaps)}
        if apply and all_rows:
            for j in range(0, len(all_rows), INSERT_BATCH):
                batch = all_rows[j:j + INSERT_BATCH]
                try:
                    keys = insert_batch(cur, batch)
                    srcs = defaultdict(int)
                    for r in batch:
                        srcs[r["source"]] += 1
                    entry = {"exchange": ex, "n_planned": len(batch), "n_inserted": len(keys), "status": "pending",
                             "sources": dict(srcs), "keys": [[k[0], k[1], k[2]] for k in keys]}
                    manifest["batches"].append(entry)
                    write_json(manifest_path, manifest)  # durable ANTES del commit
                    db.conn.commit()
                except Exception:
                    db.conn.rollback()
                    if manifest["batches"] and manifest["batches"][-1].get("status") == "pending":
                        manifest["batches"].pop()  # no commiteado; el pending en disco es inocuo en rollback
                    raise
                entry["status"] = "committed"
                write_json(manifest_path, manifest)
                rep["inserted"] += len(keys)
        report["exchanges"][ex] = rep
        print(f"[{ex}] gaps={rep['gaps']} missing={rep['missing_bars']} planned={rep['planned_rows']} "
              f"inserted={rep['inserted']} unrecoverable_bars={rep['unrecoverable_bars']} by_source={rep['by_source']}")
        for reason, n in sorted(rep["unrecoverable_reasons"].items(), key=lambda kv: -kv[1])[:8]:
            print(f"    unrecoverable x{n}: {reason}")
    if apply:
        report["manifest"] = str(manifest_path)
    return report


def _reason_histogram(results: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    h: dict[str, int] = defaultdict(int)
    for r in results:
        if r["unrecoverable"]:
            key = (r["reason"] or "unknown")
            key = key.split(" [")[0][:160]
            h[key] += r["unrecoverable"]
    return dict(h)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Detecta y repara barras 15m ausentes en futures_klines_15m.")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--detect", action="store_true", help="solo lectura (por defecto)")
    mode.add_argument("--repair", action="store_true", help="planifica la reparacion (dry-run salvo --apply)")
    mode.add_argument("--rollback", metavar="MANIFEST", help="borra las claves del manifiesto (con --apply)")
    p.add_argument("--apply", action="store_true", help="escribe en la DB (requiere --repair o --rollback)")
    p.add_argument("--exchange", choices=EXCHANGES, action="append", help="repetible; por defecto los tres")
    p.add_argument("--symbols", help="lista separada por comas (simbolos tal como estan en la DB)")
    p.add_argument("--since", help="UTC, YYYY-MM-DD o ISO (por defecto: toda la historia)")
    p.add_argument("--until", help="UTC exclusivo (por defecto: toda la historia)")
    p.add_argument("--max-gaps", type=int, help="limite de huecos a reparar por exchange")
    p.add_argument("--report", help="ruta del informe JSON")
    p.add_argument("--recent-days", type=int,
                   help="solo huecos cuya barra siguiente esta en los ultimos N dias (incompatible con --since/--until)")
    p.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR))
    return p


def main(argv: Sequence[str] | None = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:  # noqa: BLE001
            pass
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.apply and not (args.repair or args.rollback):
        parser.error("--apply requiere --repair o --rollback (--detect es solo lectura)")
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    _load_env()
    out_dir = Path(args.out_dir)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:6]
    exchanges = tuple(args.exchange or EXCHANGES)
    if args.recent_days is not None and (args.since or args.until or args.recent_days < 1):
        parser.error("--recent-days N (N >= 1) no se combina con --since/--until")
    prev_margin = timedelta(0)
    if args.recent_days is not None:
        since = datetime.now(UTC) - timedelta(days=args.recent_days)
        until = datetime(2100, 1, 1, tzinfo=UTC)
        prev_margin = timedelta(days=RECENT_PREV_MARGIN_DAYS)
    else:
        since = parse_when(args.since) if args.since else datetime(2015, 1, 1, tzinfo=UTC)
        until = parse_when(args.until) if args.until else datetime(2100, 1, 1, tzinfo=UTC)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] if args.symbols else None
    try:
        db = _DB()
    except Exception as exc:  # noqa: BLE001
        print(f"fatal: DB unavailable ({type(exc).__name__}: {exc})", file=sys.stderr)
        return EXIT_FATAL
    try:
        if args.rollback:
            manifest = json.loads(Path(args.rollback).read_text(encoding="utf-8"))
            keys = manifest_keys_for_rollback(manifest)
            print(f"rollback manifest={args.rollback} keys={len(keys)} apply={args.apply}")
            if not args.apply:
                print("dry-run: nada borrado (anade --apply)")
                return EXIT_OK
            cur = db.conn.cursor()
            n = rollback_keys(cur, keys)
            db.conn.commit()
            print(f"deleted={n}")
            return EXIT_OK
        cur = db.conn.cursor()
        cur.execute("SET statement_timeout = 0")
        t0 = time.time()
        gaps_by_ex = run_detect(cur, exchanges, since, until, symbols, prev_margin)
        db.conn.rollback()
        summary = print_summary("detect", gaps_by_ex)
        print(f"detect took {time.time() - t0:.0f}s")
        report: dict[str, Any] = {"run_id": run_id, "since": iso(dt_to_ms(since)), "until": iso(dt_to_ms(until)),
                                  "detect": summary, "gaps": {ex: gaps_to_json(g) for ex, g in gaps_by_ex.items()}}
        code = EXIT_OK
        if args.repair:
            rep = run_repair(db, gaps_by_ex, apply=args.apply, run_id=run_id, out_dir=out_dir,
                             max_gaps=args.max_gaps)
            report["repair"] = rep
            if any(e["unrecoverable_bars"] or (e["planned_rows"] < e["missing_bars"]
                   or e.get("omitted_gaps")) for e in rep["exchanges"].values()):
                code = EXIT_PARTIAL
        path = Path(args.report) if args.report else out_dir / f"report_{run_id}.json"
        write_json(path, report)
        print(f"report: {path}")
        return code
    except Exception as exc:  # noqa: BLE001
        logger.exception("fatal: %s", exc)
        return EXIT_FATAL
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
