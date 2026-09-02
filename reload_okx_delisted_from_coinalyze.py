#!/usr/bin/env python3
"""Realinea a UTC los pares de OKX que ya estan deslistados, via Coinalyze.

CONTEXTO
--------
reload_okx_spot_1dutc.py (migracion 006) recargo los 132 simbolos de OKX con velas `1Dutc`,
pero por diseno no borra un simbolo cuyo fetch vuelve vacio. Dos pares estan DESLISTADOS de
OKX -- `/public/instruments?instType=SPOT` no los lista y `/market/history-candles` devuelve
code 51001 en cualquier `bar` -- asi que se quedaron con sus velas viejas en UTC+8:

    TON-USDT   1511 filas   2022-04-28 .. 2026-06-16
    IP-USDT     506 filas   2025-02-12 .. 2026-07-02

NO se borran: son los dos unicos activos muertos que tiene OKX en la tabla, y quitarlos
meteria sesgo de supervivencia en cualquier backtest que use ese universo. Pero tampoco
pueden quedarse como estan: describen otras 24h que las filas UTC con la misma `date`, y se
agregan en silencio.

LA SALIDA
---------
Coinalyze SI conserva el historico de esos mercados (`TONUSDT.3`, `IPUSDT.3`) aunque ya no
los liste en `/v1/spot-markets`, y sus velas diarias son UTC. Medido contra el mismo activo
en binance/bybit, que si son UTC:

    TON   guardado UTC+8  212.2 bps      Coinalyze UTC  6.5 bps
    IP    guardado UTC+8  405.2 bps      Coinalyze UTC  6.8 bps

(el resto de OKX, ya recargado, esta en 11.0 bps contra binance)

QUE SE CONSERVA Y QUE SE PIERDE
-------------------------------
La FECHA DE DESLISTADO se conserva exacta en los dos (2026-06-16 y 2026-07-02): el evento
que de verdad importa para no sesgar un backtest sigue ahi. Lo que se recorta son 285 dias
del ARRANQUE de las series (TON empieza 2022-10-27 en vez de 2022-04-28; IP, un dia mas
tarde), porque Coinalyze no llega tan atras. Es el mismo recorte de inicio que la 006 ya
acepto para los otros 130 simbolos, y un inicio mas tardio no es sesgo de supervivencia:
el activo sigue en la tabla y su muerte sigue siendo visible.

Las filas viejas siguen intactas en spot_daily_ohlcv_okx_pre1dutc (backup de la 006).

USO
---
    python reload_okx_delisted_from_coinalyze.py --dry-run
    python reload_okx_delisted_from_coinalyze.py
    python audit_spot_delta.py --source db --exchange okx
"""
import argparse
import os
import sys
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv

from spot_scraper import (
    CoinalyzeClient,
    DatabaseManager,
    patch_missing_metrics,
    spot_symbol_for,
    to_unix_ms,
)

load_dotenv()

# base -> simbolo spot de Coinalyze para OKX (sufijo .3).
DELISTED = {"TON": "TONUSDT.3", "IP": "IPUSDT.3"}

FINAL_COLS = [
    "date", "price_open", "price_high", "price_low", "price_close",
    "volume_base", "volume_usd", "buy_volume_base", "sell_volume_base",
    "volume_delta", "txn_count", "buy_txn_count", "sell_txn_count",
    "symbol", "exchange",
]


def stored_range(db_url: str, symbol: str):
    with psycopg2.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), min(date), max(date) FROM spot_daily_ohlcv "
            "WHERE exchange = 'okx' AND symbol = %s",
            (symbol,),
        )
        return cur.fetchone()


def backed_up(db_url: str, table: str, symbol: str) -> int:
    """Filas de ese simbolo en la tabla de backup de la 006. 0 si la tabla no existe."""
    with psycopg2.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (table,))
        if cur.fetchone()[0] is None:
            return 0
        cur.execute(f"SELECT count(*) FROM {table} WHERE symbol = %s", (symbol,))
        return cur.fetchone()[0]


def replace_symbol(db: DatabaseManager, db_url: str, symbol: str, df) -> int:
    """Borra las filas UTC+8 del simbolo e inserta las UTC, en ese orden.

    El upsert usa COALESCE(EXCLUDED.x, existente.x), asi que sin el DELETE previo las
    columnas viejas de buy/sell/delta sobrevivirian pegadas a precios ya realineados.
    """
    with psycopg2.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM spot_daily_ohlcv WHERE exchange = 'okx' AND symbol = %s",
            (symbol,),
        )
        deleted = cur.rowcount
        conn.commit()
    db.upsert_spot_ohlcv(df)
    return deleted


def main():
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--db-url", default=None, help="Sobrescribe DATABASE_URL")
    ap.add_argument("--backup-table", default="spot_daily_ohlcv_okx_pre1dutc",
                    help="Tabla donde reload_okx_spot_1dutc.py dejo las filas okx originales. "
                         "Un simbolo no se toca si no esta respaldado ahi.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Descarga y compara, sin escribir en la tabla")
    args = ap.parse_args()

    db_url = args.db_url or os.getenv("DATABASE_URL")
    if not db_url:
        sys.exit("[ERROR] DATABASE_URL no definida y no se paso --db-url.")

    api_key = os.getenv("COINALYZE_API_KEY_SPOT") or os.getenv("COINALYZE_API_KEY")
    if not api_key:
        sys.exit("[ERROR] Falta COINALYZE_API_KEY_SPOT (o COINALYZE_API_KEY).")

    client = CoinalyzeClient(api_key)
    db = DatabaseManager(db_url) if args.db_url else DatabaseManager()
    start_ts = to_unix_ms(datetime(2017, 1, 1, tzinfo=timezone.utc))
    end_ts = to_unix_ms(datetime.now(timezone.utc))

    for base, cz_symbol in DELISTED.items():
        symbol = spot_symbol_for("okx", base)
        n_old, min_old, max_old = stored_range(db_url, symbol)
        n_backup = backed_up(db_url, args.backup_table, symbol)
        print(f"\n=== {symbol} ===")
        print(f"  guardado (UTC+8): {n_old} filas  {min_old} .. {max_old}"
              f"   (respaldadas en {args.backup_table}: {n_backup})")

        if n_old and n_backup < n_old:
            print(f"  [SKIP] backup incompleto ({n_backup} < {n_old}). No se toca.")
            continue

        df = client.fetch_ohlcv(cz_symbol, start_ts, end_ts)
        if df.empty:
            print(f"  [SKIP] Coinalyze no devolvio nada para {cz_symbol}. "
                  f"Las filas viejas se quedan como estan.")
            continue

        df["exchange"], df["symbol"] = "okx", symbol
        df = patch_missing_metrics(df, base, "okx", symbol)
        df = df[[c for c in FINAL_COLS if c in df.columns]]
        print(f"  Coinalyze (UTC):  {len(df)} filas  {df['date'].min()} .. {df['date'].max()}")

        if str(df["date"].max()) < str(max_old):
            print(f"  [SKIP] Coinalyze acaba antes que lo guardado: perderiamos la fecha de "
                  f"deslistado ({max_old}), que es justo lo que no se puede perder.")
            continue

        if args.dry_run:
            print("  [DRY-RUN] no se escribe nada.")
            continue

        deleted = replace_symbol(db, db_url, symbol, df)
        n_new, min_new, max_new = stored_range(db_url, symbol)
        print(f"  reemplazado: -{deleted} filas UTC+8, +{n_new} filas UTC  "
              f"{min_new} .. {max_new}")

    print("\nVerificar con:  python audit_spot_delta.py --source db --exchange okx")


if __name__ == "__main__":
    main()
