#!/usr/bin/env python3
"""
backfill_market_cap_history.py

Builds a point-in-time market-cap universe for survivor-bias-free backtests.

Default mode uses CoinMarketCap historical snapshots. This is the preferred
source for the universe because each snapshot already answers the question
"what was in the top N on this date?" without reconstructing ranks from a
limited CoinGecko candidate set.

CoinGecko remains available as an explicit fallback/enrichment source, with
local ID/history caches and DB-aware skipping to avoid burning API quota.
"""

import argparse
import html
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from json import JSONDecodeError
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

UTC = timezone.utc
REPO_DIR = Path(__file__).resolve().parent
DEFAULT_UNIVERSE_FILE = REPO_DIR / "data" / "universe_cmc.json"
DEFAULT_CACHE_DIR = REPO_DIR / "data" / "cache" / "market_cap_history"

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
COINMARKETCAP_HISTORICAL = "https://coinmarketcap.com/historical/{yyyymmdd}/"

CG_API_KEY = os.getenv("COINGECKO_API_KEY", "").strip().strip(".")
CG_DELAY = float(os.getenv("COINGECKO_DELAY_SECONDS", "2.2"))
CMC_DELAY = float(os.getenv("CMC_DELAY_SECONDS", "1.25"))

# Stablecoins, wrapped/liquid-staking tokens, and synthetic assets. The default
# universe is intended for tradable alts, not dollar proxies or wrappers.
FILTERED_SYMBOLS = {
    "USDT", "USDC", "DAI", "BUSD", "TUSD", "USDP", "GUSD", "FRAX", "LUSD",
    "USDD", "PYUSD", "FDUSD", "EURC", "EURT", "XAUT", "PAXG", "GHO",
    "CRVUSD", "MKUSD", "USDE", "USDX", "USD0", "USDY", "SUSD", "RAI",
    "FEI", "MIM", "DOLA", "ALUSD", "UST", "USTC", "PAX",
    "WBTC", "WETH", "WBNB", "WHBAR", "STETH", "WSTETH", "RETH", "CBETH",
    "FRXETH", "SFRXETH", "MSOL", "BNSOL", "EZETH", "WEETH", "RSETH",
    "METH", "SWETH", "TBTC", "HBTC", "RENBTC", "SBTC", "OBTC", "PBTC",
    "STMATIC", "STSOL",
}


class QuotaExhausted(RuntimeError):
    """Raised when a provider asks us to stop rather than retry aggressively."""


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def iter_dates(start: date, end: date, frequency: str) -> Iterable[date]:
    step = {"daily": 1, "weekly": 7, "monthly": 0}[frequency]
    cur = start
    while cur <= end:
        yield cur
        if frequency == "monthly":
            year = cur.year + (1 if cur.month == 12 else 0)
            month = 1 if cur.month == 12 else cur.month + 1
            cur = date(year, month, 1)
        else:
            cur += timedelta(days=step)


def load_universe_symbols(path: Optional[Path]) -> List[str]:
    if not path or not path.exists():
        return []
    with path.open("r") as f:
        data = json.load(f)
    raw = data.get("symbols") if isinstance(data, dict) else data
    if raw is None and isinstance(data, dict):
        raw = data.get("candidates", [])

    symbols = []
    for item in raw:
        sym = item.get("symbol") if isinstance(item, dict) else item
        if sym:
            symbols.append(str(sym).upper())
    return list(dict.fromkeys(symbols))


def read_json(path: Path, default):
    if not path.exists():
        return default
    with path.open("r") as f:
        return json.load(f)


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    tmp.replace(path)


def sanitize_float(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace("$", "").replace(",", "").strip()
        if not value or value in {"--", "N/A"}:
            return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out or out in (float("inf"), float("-inf")):
        return None
    return out


def sanitize_int(value) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace("#", "").replace(",", "").strip()
        if not value:
            return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def get_database_url(required: bool = True) -> Optional[str]:
    db_url = os.getenv("DATABASE_URL")
    if required and not db_url:
        print("[ERROR] DATABASE_URL is required to write market_cap_history.")
        sys.exit(1)
    return db_url


def existing_cmc_dates(conn, start: date, end: date) -> set:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT date
            FROM market_cap_history
            WHERE date BETWEEN %s AND %s
              AND source = 'coinmarketcap'
            """,
            (start, end),
        )
        return {row[0] for row in cur.fetchall()}


def existing_symbol_counts(conn, start: date, end: date, symbols: Sequence[str]) -> Dict[str, int]:
    if not symbols:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol, COUNT(*)::int
            FROM market_cap_history
            WHERE date BETWEEN %s AND %s
              AND symbol = ANY(%s)
              AND market_cap_usd IS NOT NULL
            GROUP BY symbol
            """,
            (start, end, list(symbols)),
        )
        return {row[0]: row[1] for row in cur.fetchall()}


def upsert_market_cap_rows(db_url: str, records: Sequence[Tuple]) -> None:
    if not records:
        return

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO market_cap_history
                    (date, symbol, market_cap_rank, market_cap_usd,
                     in_top_50, ever_in_top_50, source)
                VALUES %s
                ON CONFLICT (date, symbol) DO UPDATE SET
                    market_cap_rank = EXCLUDED.market_cap_rank,
                    market_cap_usd = COALESCE(EXCLUDED.market_cap_usd, market_cap_history.market_cap_usd),
                    in_top_50 = EXCLUDED.in_top_50,
                    ever_in_top_50 = market_cap_history.ever_in_top_50 OR EXCLUDED.ever_in_top_50,
                    source = EXCLUDED.source
                """,
                records,
                template="(%s,%s,%s,%s,%s,%s,%s)",
                page_size=1000,
            )

            ever_top = sorted({r[1] for r in records if r[5]})
            if ever_top:
                cur.execute(
                    """
                    UPDATE asset_metadata
                    SET ever_in_top_50 = true
                    WHERE symbol = ANY(%s)
                      AND (ever_in_top_50 IS NULL OR ever_in_top_50 = false)
                    """,
                    (ever_top,),
                )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def sync_asset_metadata_from_market_cap_history(db_url: str) -> int:
    """
    Ensure every historical ever-top asset exists in asset_metadata.

    Daemons load their polling universe from asset_metadata for speed, while
    backtests load point-in-time membership from market_cap_history. This sync
    keeps those two views consistent after a CMC historical backfill.
    """
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (symbol)
                        symbol,
                        market_cap_usd,
                        market_cap_rank
                    FROM market_cap_history
                    WHERE ever_in_top_50 = true
                    ORDER BY symbol, date DESC
                ),
                upserted AS (
                    INSERT INTO asset_metadata
                        (symbol, narrative, is_filtered, market_cap, market_cap_rank,
                         ever_in_top_50, updated_at)
                    SELECT
                        symbol,
                        'Historical Top 50',
                        false,
                        market_cap_usd,
                        market_cap_rank,
                        true,
                        NOW()
                    FROM latest
                    ON CONFLICT (symbol) DO UPDATE SET
                        ever_in_top_50 = true,
                        is_filtered = COALESCE(asset_metadata.is_filtered, false),
                        narrative = COALESCE(asset_metadata.narrative, EXCLUDED.narrative),
                        market_cap = COALESCE(asset_metadata.market_cap, EXCLUDED.market_cap),
                        market_cap_rank = COALESCE(asset_metadata.market_cap_rank, EXCLUDED.market_cap_rank),
                        updated_at = NOW()
                    RETURNING symbol
                )
                SELECT COUNT(*) FROM upserted
                """
            )
            count = cur.fetchone()[0]
        conn.commit()
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def extract_balanced_json_after(html_text: str, marker: str):
    idx = html_text.find(marker)
    if idx < 0:
        return None
    start = html_text.find("[", idx)
    if start < 0:
        return None
    decoder = json.JSONDecoder()
    try:
        value, _ = decoder.raw_decode(html_text[start:])
        return value
    except JSONDecodeError:
        return None


def find_crypto_currency_list(payload):
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key == "cryptoCurrencyList" and isinstance(value, list):
                return value
            if key == "listingHistorical" and isinstance(value, dict):
                data = value.get("data")
                if isinstance(data, list):
                    return data
            if isinstance(value, str) and key in {"initialState", "__APOLLO_STATE__"}:
                try:
                    decoded = json.loads(value)
                except JSONDecodeError:
                    decoded = None
                if decoded is not None:
                    found = find_crypto_currency_list(decoded)
                    if found is not None:
                        return found
            found = find_crypto_currency_list(value)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for value in payload:
            found = find_crypto_currency_list(value)
            if found is not None:
                return found
    return None


def parse_cmc_snapshot_html(html_text: str) -> List[dict]:
    text = html.unescape(html_text)

    direct = extract_balanced_json_after(text, '"cryptoCurrencyList"')
    if isinstance(direct, list):
        return direct

    scripts = re.findall(
        r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        text,
        flags=re.DOTALL,
    )
    for script in scripts:
        try:
            payload = json.loads(script)
        except JSONDecodeError:
            continue
        found = find_crypto_currency_list(payload)
        if found:
            return found

    raise ValueError("Could not find cryptoCurrencyList in CMC historical page")


def normalize_cmc_item(item: dict) -> Optional[dict]:
    symbol = str(item.get("symbol") or item.get("ticker") or "").upper().strip()
    if not symbol:
        return None

    quote = item.get("quote") or item.get("quotes") or {}
    usd_quote = quote.get("USD") if isinstance(quote, dict) else None
    if not isinstance(usd_quote, dict):
        usd_quote = {}

    raw_rank = sanitize_int(
        item.get("cmcRank")
        or item.get("rank")
        or item.get("marketCapRank")
        or item.get("market_cap_rank")
    )
    market_cap = sanitize_float(
        usd_quote.get("marketCap")
        or usd_quote.get("market_cap")
        or item.get("marketCap")
        or item.get("market_cap")
    )

    return {
        "symbol": symbol,
        "raw_rank": raw_rank,
        "market_cap_usd": market_cap,
    }


def fetch_cmc_snapshot(snapshot_date: date, cache_dir: Path, retries: int = 3) -> List[dict]:
    cache_path = cache_dir / "cmc_snapshots" / f"{snapshot_date:%Y-%m-%d}.json"
    cached = read_json(cache_path, None)
    if cached is not None:
        return cached.get("assets", cached if isinstance(cached, list) else [])

    url = COINMARKETCAP_HISTORICAL.format(yyyymmdd=snapshot_date.strftime("%Y%m%d"))
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "User-Agent": "Mozilla/5.0 (compatible; alt-scraper/1.0; +https://coinmarketcap.com/)",
    }

    last_error = None
    for attempt in range(retries):
        time.sleep(CMC_DELAY)
        try:
            resp = requests.get(url, headers=headers, timeout=45)
            if resp.status_code == 429:
                retry_after = int(float(resp.headers.get("Retry-After", "60")))
                raise QuotaExhausted(f"CMC returned 429; retry after {retry_after}s")
            if resp.status_code == 404:
                assets: List[dict] = []
            else:
                resp.raise_for_status()
                assets = [
                    row for row in (normalize_cmc_item(i) for i in parse_cmc_snapshot_html(resp.text))
                    if row
                ]
            write_json(
                cache_path,
                {
                    "date": snapshot_date.isoformat(),
                    "source_url": url,
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "assets": assets,
                },
            )
            return assets
        except QuotaExhausted:
            raise
        except Exception as exc:
            last_error = exc
            print(f"    [CMC] {snapshot_date} attempt {attempt + 1}/{retries} failed: {exc}")
            time.sleep(5 * (attempt + 1))

    raise RuntimeError(f"CMC snapshot failed for {snapshot_date}: {last_error}")


def build_cmc_records(
    snapshot_date: date,
    assets: List[dict],
    top_n: int,
    snapshot_size: int,
    exclude_filtered: bool,
) -> List[Tuple]:
    ranked = [a for a in assets if a.get("raw_rank")]
    ranked.sort(key=lambda a: a["raw_rank"])
    ranked = ranked[:snapshot_size]

    eligible = []
    seen_symbols = set()
    for asset in ranked:
        if exclude_filtered and asset["symbol"] in FILTERED_SYMBOLS:
            continue
        if asset["symbol"] in seen_symbols:
            continue
        seen_symbols.add(asset["symbol"])
        eligible.append(asset)

    records = []
    for clean_rank, asset in enumerate(eligible, start=1):
        in_top = clean_rank <= top_n
        records.append(
            (
                snapshot_date,
                asset["symbol"],
                clean_rank,
                asset.get("market_cap_usd"),
                in_top,
                in_top,
                "coinmarketcap",
            )
        )
    return records


def run_cmc_backfill(args) -> None:
    db_url = get_database_url(required=not args.dry_run)
    start = parse_date(args.start)
    end = parse_date(args.end)
    cache_dir = Path(args.cache_dir)

    candidate_symbols = load_universe_symbols(Path(args.from_file)) if args.from_file else []
    if candidate_symbols:
        print(f"[INFO] Loaded {len(candidate_symbols)} historical candidate symbols from {args.from_file}")
    print(
        f"[INFO] CMC backfill {start} -> {end}, frequency={args.frequency}, "
        f"top_n={args.top_n}, snapshot_size={args.snapshot_size}, "
        f"exclude_filtered={not args.include_filtered}"
    )

    skip_dates = set()
    if db_url and not args.dry_run and not args.force:
        conn = psycopg2.connect(db_url)
        try:
            skip_dates = existing_cmc_dates(conn, start, end)
        finally:
            conn.close()
        if skip_dates:
            print(f"[INFO] Skipping {len(skip_dates)} dates already present in market_cap_history.")

    total_rows = 0
    processed_dates = 0
    for snapshot_date in iter_dates(start, end, args.frequency):
        if snapshot_date in skip_dates:
            continue

        print(f"[CMC] {snapshot_date} ... ", end="", flush=True)
        try:
            assets = fetch_cmc_snapshot(snapshot_date, cache_dir, retries=args.retries)
        except QuotaExhausted as exc:
            print(f"\n[STOP] {exc}. Progress is cached; rerun later to resume.")
            break

        records = build_cmc_records(
            snapshot_date,
            assets,
            top_n=args.top_n,
            snapshot_size=args.snapshot_size,
            exclude_filtered=not args.include_filtered,
        )
        if args.dry_run:
            print(f"{len(records)} rows (dry-run)")
        else:
            upsert_market_cap_rows(db_url, records)
            in_top = sum(1 for r in records if r[4])
            print(f"{len(records)} rows, {in_top} in top-{args.top_n}")
        total_rows += len(records)
        processed_dates += 1

    print(f"[DONE] CMC backfill processed {processed_dates} dates and {total_rows} rows.")
    if not args.dry_run:
        synced = sync_asset_metadata_from_market_cap_history(db_url)
        print(f"[INFO] Synced {synced} ever-top assets into asset_metadata.")
        print_validation_summary(db_url, start, end, args.top_n)


def cg_get(endpoint: str, params: Optional[dict] = None, retries: int = 5):
    if not CG_API_KEY:
        raise RuntimeError("COINGECKO_API_KEY is required for --source coingecko")

    url = f"{COINGECKO_BASE}{endpoint}"
    params = dict(params or {})
    params["x_cg_demo_api_key"] = CG_API_KEY

    for attempt in range(retries):
        time.sleep(CG_DELAY)
        resp = requests.get(url, params=params, timeout=45)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:
            retry_after = int(float(resp.headers.get("Retry-After", "60")))
            if attempt == retries - 1:
                raise QuotaExhausted(f"CoinGecko returned 429; retry after {retry_after}s")
            print(f"    [CG] 429; waiting {retry_after}s...")
            time.sleep(retry_after)
            continue
        if resp.status_code in (401, 403):
            raise RuntimeError(f"CoinGecko auth error HTTP {resp.status_code}; check API key/plan")
        resp.raise_for_status()
    return None


def resolve_coingecko_ids(symbols: Sequence[str], cache_dir: Path) -> Dict[str, str]:
    cache_path = cache_dir / "coingecko_id_cache.json"
    cached = read_json(cache_path, {})
    result = {str(k).upper(): v for k, v in cached.items() if v}
    remaining = [s for s in symbols if s not in result]
    if not remaining:
        return result

    print(f"[CG] Resolving IDs for {len(remaining)} uncached symbols...")
    remaining_set = set(remaining)
    for page in range(1, 11):
        if not remaining_set:
            break
        data = cg_get(
            "/coins/markets",
            {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 250,
                "page": page,
                "sparkline": "false",
            },
        )
        if not data:
            break
        for coin in data:
            sym = str(coin.get("symbol", "")).upper()
            if sym in remaining_set:
                result[sym] = coin.get("id")
                remaining_set.discard(sym)
        write_json(cache_path, result)

    if remaining_set:
        print(f"[WARN] No CoinGecko ID for: {', '.join(sorted(remaining_set))}")
    write_json(cache_path, result)
    return result


def fetch_coingecko_market_cap_history(cg_id: str, start: date, end: date, cache_dir: Path) -> List[Tuple[str, float]]:
    cache_path = cache_dir / "coingecko_history" / f"{cg_id}.json"
    cached = read_json(cache_path, None)
    if cached:
        cached_start = parse_date(cached["start"])
        cached_end = parse_date(cached["end"])
        if cached_start <= start and cached_end >= end:
            return [(d, float(v)) for d, v in cached.get("history", [])]

    start_dt = datetime.combine(start, datetime.min.time(), tzinfo=UTC)
    end_dt = datetime.combine(end + timedelta(days=1), datetime.min.time(), tzinfo=UTC)
    data = cg_get(
        f"/coins/{cg_id}/market_chart/range",
        {
            "vs_currency": "usd",
            "from": int(start_dt.timestamp()),
            "to": int(end_dt.timestamp()),
        },
    )

    seen: Dict[str, float] = {}
    for ts_ms, mcap in (data or {}).get("market_caps", []):
        if mcap is None:
            continue
        day = datetime.fromtimestamp(ts_ms / 1000, tz=UTC).strftime("%Y-%m-%d")
        if start.isoformat() <= day <= end.isoformat():
            seen[day] = float(mcap)

    history = sorted(seen.items())
    write_json(
        cache_path,
        {
            "coingecko_id": cg_id,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "fetched_at": datetime.now(UTC).isoformat(),
            "history": history,
        },
    )
    return history


def run_coingecko_backfill(args) -> None:
    db_url = get_database_url(required=not args.dry_run)
    start = parse_date(args.start)
    end = parse_date(args.end)
    cache_dir = Path(args.cache_dir)
    symbols = load_universe_symbols(Path(args.from_file))
    if not args.include_filtered:
        symbols = [s for s in symbols if s not in FILTERED_SYMBOLS]
    if not symbols:
        print("[ERROR] No symbols found for CoinGecko fallback.")
        sys.exit(1)

    min_days = max(1, int((end - start).days * args.skip_existing_coverage))
    if db_url and not args.dry_run and not args.force:
        conn = psycopg2.connect(db_url)
        try:
            counts = existing_symbol_counts(conn, start, end, symbols)
        finally:
            conn.close()
        before = len(symbols)
        symbols = [s for s in symbols if counts.get(s, 0) < min_days]
        print(f"[CG] Skipping {before - len(symbols)} symbols with sufficient DB coverage.")

    sym_to_id = resolve_coingecko_ids(symbols, cache_dir)
    histories: Dict[str, Dict[str, float]] = {}
    for idx, sym in enumerate(symbols, start=1):
        cg_id = sym_to_id.get(sym)
        if not cg_id:
            continue
        print(f"[CG] {idx:3d}/{len(symbols)} {sym:12s} ... ", end="", flush=True)
        try:
            history = fetch_coingecko_market_cap_history(cg_id, start, end, cache_dir)
        except QuotaExhausted as exc:
            print(f"\n[STOP] {exc}. Progress is cached; rerun later to resume.")
            break
        histories[sym] = dict(history)
        print(f"{len(history)} days")

    records = build_relative_rank_records(histories, args.top_n, "coingecko")
    if args.dry_run:
        print(f"[DRY-RUN] Would upsert {len(records)} CoinGecko rows.")
    else:
        upsert_market_cap_rows(db_url, records)
        synced = sync_asset_metadata_from_market_cap_history(db_url)
        print(f"[DONE] Upserted {len(records)} CoinGecko fallback rows.")
        print(f"[INFO] Synced {synced} ever-top assets into asset_metadata.")
        print_validation_summary(db_url, start, end, args.top_n)


def build_relative_rank_records(histories: Dict[str, Dict[str, float]], top_n: int, source: str) -> List[Tuple]:
    by_date: Dict[str, Dict[str, float]] = {}
    for symbol, history in histories.items():
        for day, mcap in history.items():
            by_date.setdefault(day, {})[symbol] = mcap

    ever_top = {symbol: False for symbol in histories}
    daily_ranks: Dict[str, Dict[str, int]] = {}
    for day, mcaps in by_date.items():
        ranked = sorted(mcaps.items(), key=lambda x: x[1], reverse=True)
        ranks = {sym: rank for rank, (sym, _) in enumerate(ranked, start=1)}
        daily_ranks[day] = ranks
        for sym, rank in ranks.items():
            if rank <= top_n:
                ever_top[sym] = True

    records = []
    for day, ranks in sorted(daily_ranks.items()):
        for sym, rank in ranks.items():
            in_top = rank <= top_n
            records.append((day, sym, rank, by_date[day].get(sym), in_top, ever_top.get(sym, False), source))
    return records


def print_validation_summary(db_url: str, start: date, end: date, top_n: int) -> None:
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(DISTINCT date), COUNT(*), MIN(date), MAX(date)
                FROM market_cap_history
                WHERE date BETWEEN %s AND %s
                """,
                (start, end),
            )
            dates, rows, min_date, max_date = cur.fetchone()
            cur.execute(
                """
                SELECT date, COUNT(*)::int
                FROM market_cap_history
                WHERE date BETWEEN %s AND %s
                  AND in_top_50 = true
                GROUP BY date
                HAVING COUNT(*) <> %s
                ORDER BY date
                LIMIT 10
                """,
                (start, end, top_n),
            )
            bad_counts = cur.fetchall()
            cur.execute(
                """
                SELECT COUNT(DISTINCT symbol)
                FROM market_cap_history
                WHERE ever_in_top_50 = true
                  AND date BETWEEN %s AND %s
                """,
                (start, end),
            )
            ever_symbols = cur.fetchone()[0]
        print("[VALIDATION]")
        print(f"  Coverage dates : {dates} ({min_date} -> {max_date})")
        print(f"  Rows           : {rows}")
        print(f"  Ever top symbols: {ever_symbols}")
        if bad_counts:
            print(f"  Dates not equal to top-{top_n}: {bad_counts}")
        else:
            print(f"  All populated dates have exactly {top_n} top flags.")
    finally:
        conn.close()


def main():
    today = datetime.now(UTC).date()
    default_end = today - timedelta(days=1)
    parser = argparse.ArgumentParser(
        description="Backfill market_cap_history without survivor bias."
    )
    parser.add_argument("--source", choices=["cmc", "coingecko"], default="cmc")
    parser.add_argument("--from-file", default=str(DEFAULT_UNIVERSE_FILE))
    parser.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR))
    parser.add_argument("--start", default="2020-01-01")
    parser.add_argument("--end", default=default_end.isoformat())
    parser.add_argument("--frequency", choices=["daily", "weekly", "monthly"], default="daily")
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--snapshot-size", type=int, default=100)
    parser.add_argument("--include-filtered", action="store_true")
    parser.add_argument("--force", action="store_true", help="Re-upsert dates/symbols even if DB already has coverage.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument(
        "--skip-existing-coverage",
        type=float,
        default=0.7,
        help="CoinGecko only: skip symbols with at least this fraction of date coverage.",
    )
    # Backward-compatible option. It is translated into --start if supplied.
    parser.add_argument("--days", type=int, default=None)
    args = parser.parse_args()

    if args.days is not None:
        args.start = (default_end - timedelta(days=args.days)).isoformat()

    if parse_date(args.start) > parse_date(args.end):
        print("[ERROR] --start must be <= --end")
        sys.exit(1)

    if args.source == "cmc":
        run_cmc_backfill(args)
    else:
        run_coingecko_backfill(args)


if __name__ == "__main__":
    main()
