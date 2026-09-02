#!/usr/bin/env python3
"""Auditoría read-only del volume_delta de spot.

Comprueba el invariante que el scraper debe cumplir siempre:

    buy_volume_base + sell_volume_base == volume_base
    volume_delta                       == buy_volume_base - sell_volume_base
    0 <= buy_txn_count <= txn_count

Cuando falla, el delta no mide presión compradora sino la diferencia de escala entre dos
mercados distintos (era el caso de binance, que mezclaba el buy de las klines del par USDT
con el sell de Coinalyze de otro par).

La columna `med btxn/txn` marca con `!` los exchanges cuya mediana de buy_txn_count/txn_count
se aleja de 0.5: ahí los dos contadores no describen el mismo mercado. Un día suelto fuera de
la banda no cuenta como violación; un sesgo sistemático sí.

Además calcula la correlación entre sign(volume_delta) y el retorno del día por exchange.
Un valor NEGATIVO es la firma de columnas buy/sell invertidas — así se destapó que OKX leía
la respuesta de Rubik al revés.

Uso:
    python audit_spot_delta.py --source db
    python audit_spot_delta.py --source csv --data-dir data/spot
    python audit_spot_delta.py --source db --exchange okx --detail

No escribe nada. Ejecutar antes y después de migrations/005_fix_spot_volume_delta.sql.
"""
import argparse
import glob
import os
import sys

import warnings

import pandas as pd
from dotenv import load_dotenv

warnings.filterwarnings("ignore", ".*pandas only supports SQLAlchemy connectable.*")

load_dotenv()

TOL = 0.02
# Un día suelto con buy_txn/txn fuera de esta banda es normal (días de volumen mínimo o
# de flujo muy unidireccional): bybit, que es correcto, tiene 1357 así sobre 176k filas, y
# coinbase 1 sobre 8965. Lo que NO es normal es que la MEDIANA del exchange se desplace:
# ahí los dos contadores vienen de mercados distintos (binance daba mediana 0.012 mezclando
# el txn_count de las klines del par USDT con el buy_txn_count de Coinalyze de otro par).
# Por eso la banda solo cuenta como violación cuando la mediana está sesgada.
TXN_BAND = (0.2, 0.8)
MEDIAN_BAND = (0.35, 0.65)

COLS = [
    "date", "symbol", "exchange", "price_close", "volume_base",
    "buy_volume_base", "sell_volume_base", "volume_delta",
    "txn_count", "buy_txn_count", "sell_txn_count",
]


def load_from_db(exchange=None, symbol=None, db_url=None) -> pd.DataFrame:
    import psycopg2

    db_url = db_url or os.getenv("DATABASE_URL")
    if not db_url:
        sys.exit("[ERROR] DATABASE_URL no está definida y no se pasó --db-url.")

    where, params = [], []
    if exchange:
        where.append("exchange = %s")
        params.append(exchange)
    if symbol:
        where.append("symbol = %s")
        params.append(symbol)
    clause = ("WHERE " + " AND ".join(where)) if where else ""

    sql = f"SELECT {', '.join(COLS)} FROM spot_daily_ohlcv {clause} ORDER BY exchange, symbol, date"
    with psycopg2.connect(db_url) as conn:
        df = pd.read_sql(sql, conn, params=params or None)
    return df


def load_from_csv(data_dir: str, exchange=None, symbol=None) -> pd.DataFrame:
    frames = []
    pattern = os.path.join(data_dir, exchange or "*", "*.csv")
    for path in sorted(glob.glob(pattern)):
        df = pd.read_csv(path)
        if "exchange" not in df.columns:
            df["exchange"] = os.path.basename(os.path.dirname(path))
        if "symbol" not in df.columns:
            df["symbol"] = os.path.basename(path).split("_")[0]
        frames.append(df.reindex(columns=COLS))
    if not frames:
        sys.exit(f"[ERROR] Sin CSV en {pattern}")
    df = pd.concat(frames, ignore_index=True)
    if symbol:
        df = df[df["symbol"] == symbol]
    return df.sort_values(["exchange", "symbol", "date"])


def analyze(df: pd.DataFrame) -> pd.DataFrame:
    for col in COLS:
        if col not in ("date", "symbol", "exchange"):
            df[col] = pd.to_numeric(df[col], errors="coerce")

    vol = df["volume_base"]
    buy, sell, delta = df["buy_volume_base"], df["sell_volume_base"], df["volume_delta"]
    txn, btxn, stxn = df["txn_count"], df["buy_txn_count"], df["sell_txn_count"]

    df["_has_delta"] = buy.notna() & sell.notna()
    df["_bad_invariant"] = df["_has_delta"] & (
        ((buy + sell - vol).abs() / vol.where(vol > 0)) > TOL
    )
    df["_bad_delta_calc"] = df["_has_delta"] & delta.notna() & (
        (delta - (buy - sell)).abs() > (vol.abs() * 1e-6 + 1e-6)
    )
    df["_neg_volume"] = (buy < 0) | (sell < 0)
    # Imposible: no hay lectura del mundo real que lo produzca.
    df["_txn_impossible"] = (btxn > txn) | (stxn < 0) | (btxn < 0)
    df["_txn_ratio"] = btxn / txn.where(txn > 0)
    df["_txn_out_of_band"] = df["_txn_ratio"].notna() & ~df["_txn_ratio"].between(*TXN_BAND)

    df["_ret"] = df.groupby(["exchange", "symbol"])["price_close"].pct_change()
    return df


def report(df: pd.DataFrame, detail: bool) -> int:
    print("=" * 104)
    print(f"{'exchange':10s} {'filas':>8s} {'con delta':>10s} {'buy+sell!=vol':>14s} "
          f"{'delta!=b-s':>11s} {'neg':>5s} {'txn malos':>10s} {'med btxn/txn':>13s} "
          f"{'corr signo':>11s}")
    print("-" * 104)

    violations = 0
    for exchange, g in df.groupby("exchange"):
        m = g["_has_delta"] & g["_ret"].notna() & g["volume_delta"].notna()
        if m.sum() > 30:
            import numpy as np
            corr = np.corrcoef(np.sign(g.loc[m, "volume_delta"]), np.sign(g.loc[m, "_ret"]))[0, 1]
            corr_s = f"{corr:+.3f}"
            flag = "  <-- INVERTIDO" if corr < 0 else ""
        else:
            corr_s, flag = "n/a", ""

        bad_inv = int(g["_bad_invariant"].sum())
        bad_calc = int(g["_bad_delta_calc"].sum())
        neg = int(g["_neg_volume"].sum())

        med = g["_txn_ratio"].median()
        skewed = g["_txn_ratio"].notna().sum() > 30 and pd.notna(med) and not (
            MEDIAN_BAND[0] <= med <= MEDIAN_BAND[1]
        )
        # La cola fuera de banda solo es un problema si la mediana está sesgada.
        bad_txn = int((g["_txn_impossible"] | (g["_txn_out_of_band"] if skewed else False)).sum())
        med_s = "n/a" if pd.isna(med) else f"{med:.3f}{' !' if skewed else ''}"
        violations += bad_inv + bad_calc + neg + bad_txn

        print(f"{exchange:10s} {len(g):8d} {int(g['_has_delta'].sum()):10d} {bad_inv:14d} "
              f"{bad_calc:11d} {neg:5d} {bad_txn:10d} {med_s:>13s} {corr_s:>11s}{flag}")

    print("=" * 104)

    if detail:
        bad = df[df["_bad_invariant"] | df["_neg_volume"] | df["_txn_impossible"]]
        if not bad.empty:
            print("\nRango afectado por (exchange, symbol):")
            g = bad.groupby(["exchange", "symbol"]).agg(
                filas=("date", "size"), desde=("date", "min"), hasta=("date", "max")
            )
            print(g.to_string())

    if violations:
        print(f"\n[FAIL] {violations} violaciones. Ver migrations/005_fix_spot_volume_delta.sql")
    else:
        print("\n[OK] Sin violaciones del invariante.")
    return violations


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", choices=["db", "csv"], default="db")
    ap.add_argument("--db-url", default=None, help="Sobrescribe DATABASE_URL (p.ej. una copia dev)")
    ap.add_argument("--data-dir", default="data/spot", help="Solo para --source csv")
    ap.add_argument("--exchange", default=None)
    ap.add_argument("--symbol", default=None)
    ap.add_argument("--detail", action="store_true", help="Desglose por símbolo y rango de fechas")
    args = ap.parse_args()

    if args.source == "db":
        df = load_from_db(args.exchange, args.symbol, args.db_url)
    else:
        df = load_from_csv(args.data_dir, args.exchange, args.symbol)

    if df.empty:
        sys.exit("[ERROR] Sin filas que auditar.")

    print(f"Auditando {len(df)} filas ({args.source}) — tolerancia {TOL:.0%}\n")
    violations = report(analyze(df), args.detail)
    sys.exit(1 if violations else 0)


if __name__ == "__main__":
    main()
