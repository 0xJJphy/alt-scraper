#!/usr/bin/env python3
"""Recarga el historico de OKX en spot_daily_ohlcv con velas alineadas a UTC.

Hasta ahora spot_scraper.py pedia a OKX `bar="1D"`, que cierra en el corte de UTC+8
(16:00 UTC), no a medianoche UTC. Cada fila okx describia por tanto OTRAS 24h que la fila
de binance/bybit con la MISMA `date`: comparando price_close por (date, activo) okx se
desviaba 308 bps de binance mientras bybit se quedaba en 23 bps. Eso rompe cualquier
agregacion cross-exchange y descorrelaciona el volume_delta del retorno del dia.

El arreglo en el scraper es `bar="1Dutc"`. Pero no basta con re-ejecutarlo:

  * `upsert_spot_ohlcv` hace ON CONFLICT ... COALESCE(EXCLUDED.x, existente.x), asi que un
    NULL de la recarga NO limpia el valor viejo: las columnas buy/sell/delta contaminadas
    sobrevivirian mezcladas con precios ya en UTC.
  * El historico `1Dutc` no cubre exactamente las mismas fechas que `1D` (para BTC empieza
    ~3 meses mas tarde), asi que quedarian fechas huerfanas con datos UTC+8.

Por eso, para cada simbolo, este script BORRA sus filas okx y escribe las nuevas dentro de
la misma transaccion. Si el fetch de un simbolo vuelve vacio no se borra nada: se reporta al
final y sus filas viejas quedan intactas para revisarlas a mano.

Uso:
    python reload_okx_spot_1dutc.py --dry-run
    python reload_okx_spot_1dutc.py --backup-table spot_daily_ohlcv_okx_pre1dutc
    python reload_okx_spot_1dutc.py --symbols BTC,ETH

Verificar despues:
    python audit_spot_delta.py --source db
"""
import argparse
import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv

from spot_scraper import DatabaseManager, SpotScraper, to_unix_ms

load_dotenv()

UTC = timezone.utc


def okx_bases(db_url: str) -> list:
    """Activos con historico okx ya en la tabla. La recarga cubre exactamente esos, sin
    pasar por CoinGecko: no queremos que la deriva del universo actual decida que se
    recarga y que se queda con velas UTC+8."""
    with psycopg2.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT replace(symbol, '-USDT', '') AS base
            FROM spot_daily_ohlcv WHERE exchange = 'okx' ORDER BY 1
        """)
        return [r[0] for r in cur.fetchall()]


def make_backup(db_url: str, table: str) -> int:
    with psycopg2.connect(db_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (table,))
        if cur.fetchone()[0] is not None:
            sys.exit(f"[ERROR] La tabla de backup {table} ya existe. Borrala o pasa otro nombre.")
        cur.execute(
            f"CREATE TABLE {table} AS SELECT * FROM spot_daily_ohlcv WHERE exchange = 'okx'"
        )
        rows = cur.rowcount
        conn.commit()
    return rows


def replace_symbol(db: DatabaseManager, symbol: str, df) -> int:
    """Borra las filas okx del simbolo e inserta las nuevas en una sola transaccion."""
    with psycopg2.connect(db.db_url) as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM spot_daily_ohlcv WHERE exchange = 'okx' AND symbol = %s", (symbol,)
        )
        deleted = cur.rowcount
        conn.commit()
    db.upsert_spot_ohlcv(df)
    return deleted


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db-url", default=None, help="Sobrescribe DATABASE_URL")
    ap.add_argument("--symbols", default=None, help="Solo estos activos (BTC,ETH). Por defecto, todos los okx de la tabla")
    ap.add_argument("--start", default="2017-01-01", help="Fecha inicial del refetch")
    ap.add_argument("--backup-table", default=None, help="Copia las filas okx a esta tabla antes de tocar nada")
    ap.add_argument("--dry-run", action="store_true", help="Lista los activos y sale sin tocar la DB")
    args = ap.parse_args()

    db_url = args.db_url or os.getenv("DATABASE_URL")
    if not db_url:
        sys.exit("[ERROR] DATABASE_URL no esta definida y no se paso --db-url.")

    bases = ([s.strip().upper() for s in args.symbols.split(",")] if args.symbols
             else okx_bases(db_url))
    if not bases:
        sys.exit("[ERROR] Sin activos okx que recargar.")

    print(f"Recarga OKX -> 1Dutc: {len(bases)} activos desde {args.start}")
    if args.dry_run:
        print(", ".join(bases))
        print("[INFO] Dry run. No se ha tocado la DB.")
        return

    if args.backup_table:
        n = make_backup(db_url, args.backup_table)
        print(f"[BACKUP] {n} filas okx copiadas a {args.backup_table}")

    start_ts = to_unix_ms(datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=UTC))
    end_ts = to_unix_ms(datetime.now(UTC))

    scraper = SpotScraper()
    db = DatabaseManager(db_url)

    replaced, empty = 0, []
    for i, base in enumerate(bases, 1):
        symbol = f"{base}-USDT"
        print(f"\n[{i}/{len(bases)}] {symbol}")
        try:
            df = scraper.fetch_okx(base, start_ts, end_ts)
        except Exception as e:
            print(f"    [FAILED] {base}: {e}")
            empty.append(base)
            continue

        if df is None or df.empty:
            print(f"    [SKIP] sin datos — se dejan las filas viejas de {symbol} sin tocar")
            empty.append(base)
            continue

        deleted = replace_symbol(db, symbol, df)
        replaced += 1
        print(f"    [DB] {deleted} filas UTC+8 borradas -> {len(df)} filas UTC "
              f"({df['date'].min()} .. {df['date'].max()})")
        time.sleep(0.1)

    print(f"\n{'=' * 60}\nRecargados {replaced}/{len(bases)} activos.")
    if empty:
        print(f"SIN RECARGAR ({len(empty)}) — siguen con velas UTC+8: {', '.join(empty)}")
    print("Verificar con: python audit_spot_delta.py --source db")


if __name__ == "__main__":
    main()
