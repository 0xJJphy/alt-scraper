#!/usr/bin/env python3
"""
discover_universe.py — Descubre qué activos han podido estar en el top 50 desde 2020.

Este script es rápido (~2 min) y NO descarga históricos. Solo construye la lista
de candidatos que luego se usa en backfill_market_cap_history.py.

Estrategia:
  - Descarga los top N activos por market cap actual de CoinGecko.
  - Añade los activos ya en asset_metadata (para no perder nada que ya trackeamos).
  - Filtra stablecoins y wrapped tokens.
  - Guarda el resultado en data/universe_candidates.json.

El usuario puede revisar y editar el JSON antes de lanzar el backfill.

Uso:
    python discover_universe.py [--size 400]   # cuántos descargar de CoinGecko
    python discover_universe.py --preview       # solo muestra, no guarda
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import psycopg2
import requests
from dotenv import load_dotenv

load_dotenv()

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
UTC = timezone.utc
OUTPUT_FILE = "data/universe_candidates.json"

CG_API_KEY = os.getenv("COINGECKO_API_KEY", "")
CG_DELAY   = 0.5 if CG_API_KEY else 2.5

# Stablecoins y wrapped tokens — se excluyen del universo
KNOWN_FILTERED = {
    "USDT", "USDC", "DAI", "BUSD", "TUSD", "USDP", "GUSD", "FRAX", "LUSD", "USDD",
    "PYUSD", "FDUSD", "EURC", "EURT", "XAUT", "PAXG", "GHO", "CRVUSD", "MKUSD",
    "USDE", "USDX", "USD0", "USDY", "SUSD", "RAI", "FEI", "MIM", "DOLA", "ALUSD",
    "WBTC", "WETH", "WBNB", "STETH", "WSTETH", "RETH", "CBETH", "FRXETH", "SFRXETH",
    "MSOL", "BNSOL", "EZETH", "WEETH", "RSETH", "METH", "SWETH",
    "TBTC", "HBTC", "RENBTC", "SBTC", "OBTC", "PBTC",
    "STMATIC", "STSOL",
}


def _cg_get(endpoint: str, params: dict = None) -> Optional[list]:
    url     = f"{COINGECKO_BASE}{endpoint}"
    headers = {"User-Agent": "alt-scraper/discover-universe"}
    if CG_API_KEY:
        headers["x-cg-demo-api-key"] = CG_API_KEY
    for attempt in range(3):
        time.sleep(CG_DELAY)
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 60))
                print(f"  [CG] Rate limit — esperando {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            print(f"  [CG] Error ({attempt+1}/3): {e}")
            time.sleep(5)
    return None


def fetch_coingecko_top(size: int) -> List[Dict]:
    """Descarga los top `size` activos de CoinGecko filtrando stables/wrapped."""
    print(f"[CoinGecko] Descargando top {size} por market cap...")
    candidates = []
    page = 1
    per_page = 250

    while len(candidates) < size:
        data = _cg_get("/coins/markets", {
            "vs_currency": "usd",
            "order":       "market_cap_desc",
            "per_page":    per_page,
            "page":        page,
            "sparkline":   "false",
        })
        if not data:
            break

        for coin in data:
            sym  = coin.get("symbol", "").upper()
            if sym in KNOWN_FILTERED:
                continue
            candidates.append({
                "symbol":          sym,
                "coingecko_id":    coin.get("id"),
                "name":            coin.get("name"),
                "market_cap_usd":  coin.get("market_cap"),
                "market_cap_rank": coin.get("market_cap_rank"),
                "source":          "coingecko_current",
            })
            if len(candidates) >= size:
                break

        print(f"  página {page}: {len(candidates)} candidatos acumulados")
        if len(data) < per_page:
            break
        page += 1

    return candidates


def load_db_assets(db_url: str) -> List[Dict]:
    """Carga activos de asset_metadata que no estén ya en la lista."""
    try:
        conn = psycopg2.connect(db_url)
        cur  = conn.cursor()
        cur.execute("""
            SELECT symbol, market_cap, market_cap_rank, narrative
            FROM asset_metadata
            WHERE is_filtered = false OR is_filtered IS NULL
            ORDER BY market_cap_rank ASC NULLS LAST
        """)
        rows = [{
            "symbol":          r[0],
            "coingecko_id":    None,   # se resolverá si es necesario
            "name":            r[0],
            "market_cap_usd":  float(r[1]) if r[1] else None,
            "market_cap_rank": r[2],
            "source":          "asset_metadata_db",
        } for r in cur.fetchall()]
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"[WARN] No se pudo leer asset_metadata: {e}")
        return []


def resolve_missing_cg_ids(candidates: List[Dict]) -> List[Dict]:
    """Para activos sin coingecko_id, intenta resolverlos paginando CoinGecko."""
    missing = [c for c in candidates if not c.get("coingecko_id")]
    if not missing:
        return candidates

    missing_syms = {c["symbol"] for c in missing}
    print(f"[CoinGecko] Resolviendo IDs para {len(missing_syms)} activos extra...")

    sym_to_id: Dict[str, str] = {}
    for page in range(1, 6):
        if not missing_syms:
            break
        data = _cg_get("/coins/markets", {
            "vs_currency": "usd",
            "order":       "market_cap_desc",
            "per_page":    250,
            "page":        page,
            "sparkline":   "false",
        })
        if not data:
            break
        for coin in data:
            sym = coin.get("symbol", "").upper()
            if sym in missing_syms:
                sym_to_id[sym] = coin.get("id")
                missing_syms.discard(sym)

    # Actualizar candidatos con los IDs encontrados
    for c in candidates:
        if not c.get("coingecko_id") and c["symbol"] in sym_to_id:
            c["coingecko_id"] = sym_to_id[c["symbol"]]

    if missing_syms:
        print(f"  [WARN] Sin ID para: {sorted(missing_syms)}")

    return candidates


def main():
    parser = argparse.ArgumentParser(
        description="Descubre el universo histórico de activos top-50 (candidatos para backfill)"
    )
    parser.add_argument("--size",    type=int, default=400,
                        help="Cuántos activos descargar de CoinGecko (default: 400)")
    parser.add_argument("--preview", action="store_true",
                        help="Solo muestra el resumen, no guarda el fichero")
    parser.add_argument("--output",  type=str, default=OUTPUT_FILE,
                        help=f"Fichero de salida (default: {OUTPUT_FILE})")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")

    print("=" * 60)
    print("Discover Universe — Candidatos históricos top-50")
    print("=" * 60)
    key_status = f"✅ ({CG_API_KEY[:8]}...)" if CG_API_KEY else "⚠️  Sin key (free tier)"
    print(f"  CG API Key  : {key_status}")
    print(f"  Universo CG : top {args.size}")
    print(f"  Salida      : {args.output}")
    print("=" * 60)
    print()

    # 1. Top N de CoinGecko
    cg_candidates = fetch_coingecko_top(args.size)
    cg_symbols     = {c["symbol"] for c in cg_candidates}

    # 2. Activos extra de la DB
    if db_url:
        db_assets  = load_db_assets(db_url)
        extra      = [a for a in db_assets if a["symbol"] not in cg_symbols]
        if extra:
            print(f"[DB] Añadiendo {len(extra)} activos extra de asset_metadata")
        all_candidates = cg_candidates + extra
    else:
        print("[WARN] DATABASE_URL no definido — solo se usa universo CoinGecko")
        all_candidates = cg_candidates

    # 3. Deduplicar por símbolo (CG tiene prioridad)
    seen: Dict[str, Dict] = {}
    for c in all_candidates:
        sym = c["symbol"]
        if sym not in seen:
            seen[sym] = c

    final = list(seen.values())

    # 4. Resolver IDs que falten
    final = resolve_missing_cg_ids(final)

    # Separar los que tienen ID (backfilleables) de los que no
    with_id    = [c for c in final if c.get("coingecko_id")]
    without_id = [c for c in final if not c.get("coingecko_id")]

    # 5. Resumen
    print()
    print("=" * 60)
    print(f"  Total candidatos    : {len(final)}")
    print(f"  Con CoinGecko ID    : {len(with_id)}  ← estos se backfillarán")
    print(f"  Sin CoinGecko ID    : {len(without_id)} ← sin datos históricos CG")
    print(f"  Rango de rank actual: #{min((c['market_cap_rank'] or 9999) for c in final)} "
          f"— #{max((c['market_cap_rank'] or 0) for c in final)}")
    print("=" * 60)
    print()
    print("Top 30 del universo:")
    for c in sorted(with_id, key=lambda x: x.get("market_cap_rank") or 9999)[:30]:
        mcap = c.get("market_cap_usd")
        mcap_str = f"${mcap/1e9:.1f}B" if mcap and mcap > 1e9 else (f"${mcap/1e6:.0f}M" if mcap else "?")
        print(f"  #{c.get('market_cap_rank', '?'):>4}  {c['symbol']:<12} {mcap_str:>8}  ({c['coingecko_id']})")

    if args.preview:
        print("\n[preview] Fichero no guardado (--preview activo).")
        return

    # 6. Guardar JSON
    output = {
        "generated_at":  datetime.now(UTC).isoformat(),
        "universe_size": args.size,
        "total":         len(final),
        "with_cg_id":    len(with_id),
        "candidates":    final,
    }
    os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else ".", exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n✅ Universo guardado en: {args.output}")
    print(f"   Edita el fichero si quieres excluir algún activo, luego lanza:")
    print(f"   python backfill_market_cap_history.py --from-file {args.output} --days 1095")


if __name__ == "__main__":
    main()
