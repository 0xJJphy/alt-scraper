#!/usr/bin/env python3
"""
backfill_mcap_gaps.py

Fills missing days of CoinGecko market cap in market_cap_history for the listed
futures universe (binance/bybit base assets seen in the last 7 days).

Why: the daily snapshot in alt_scraper.py only stored the top 250, so smaller
listed alts never got a row and the GLI screener could not validate their rank.

Rules:
- Rows are inserted only for (date, symbol) pairs that do not exist yet
  (ON CONFLICT DO NOTHING); existing CMC/CoinGecko rows are never touched.
- market_cap_rank is left NULL: CoinGecko has no historical global rank, and a
  rank computed inside the fetched set would be wrong. The daily snapshot
  supplies the rank from its next run on.
- A row dated T is the 00:00 UTC CoinGecko point of T (= close of T-1), the same
  convention as the 00:15 daily snapshot. Only points exactly at midnight are kept;
  the range always spans > 90 days so CoinGecko returns daily granularity.
- Ticker -> CoinGecko id: candidates sharing the ticker (also without a leading
  1000/1M scale prefix) are tried in market-cap order. An id is accepted only if
  its daily log market-cap change matches the futures close change
  (>= 20 pairs; on the days with |d| <= 0.05, median |d| <= 0.01 and >= 80% of
  |d| <= 0.02), the thresholds of the screener's mcap-check-v2.1. Days with
  |d| > 0.05 (supply updates) are counted in the report, not used to reject the
  id. Failures are reported and nothing is written.
- okx closes are never used. Default is a dry run; --apply writes.

Usage:
    python backfill_mcap_gaps.py                      # dry run, report only
    python backfill_mcap_gaps.py --apply              # write the gaps
    python backfill_mcap_gaps.py --symbols BAND,ENJ --days 365 --apply
    python backfill_mcap_gaps.py --ids US=some-coingecko-id --apply
    python backfill_mcap_gaps.py --auto --apply       # daily pipeline step (run_pipeline.py)

--auto (detection for the daily run): a base is a gap only if it has no row in the
whole window (new listing / outside the CoinGecko top 1000) or misses a day in the
last --recent-days days (failed snapshot, rank fell out of the snapshot pages).
Older holes alone do not trigger a fetch, so coins whose CoinGecko history starts
late are not re-downloaded every day. At most --max-bases bases per run, those
with the most missing recent days first.
"""

import argparse
import json
import math
import os
import statistics
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8")

UTC = timezone.utc
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
CG_API_KEY = os.getenv("COINGECKO_API_KEY", "").strip().strip(".")
CG_DELAY = float(os.getenv("COINGECKO_DELAY_SECONDS", "2.2"))
REPORT_DIR = Path(__file__).resolve().parent / "logs"

MIN_PAIRS = 20
MEDIAN_ABS_MAX = 0.01
WITHIN = 0.02
WITHIN_SHARE_MIN = 0.80
JUMP_ABS = 0.05
MAX_CANDIDATES = 3
SCALE_PREFIXES = ("1000000", "1000", "1M")


def cg_get(endpoint: str, params: Optional[dict] = None, retries: int = 5):
    params = dict(params or {})
    if CG_API_KEY:
        params["x_cg_demo_api_key"] = CG_API_KEY
    for attempt in range(retries):
        time.sleep(CG_DELAY)
        resp = requests.get(f"{COINGECKO_BASE}{endpoint}", params=params, timeout=45)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429:
            wait = int(float(resp.headers.get("Retry-After", "60")))
            print(f"    [CG] 429; waiting {wait}s...")
            time.sleep(wait)
            continue
        if resp.status_code in (401, 403):
            raise RuntimeError(f"CoinGecko HTTP {resp.status_code} on {endpoint}: {resp.text[:200]}")
        if attempt == retries - 1:
            resp.raise_for_status()
    return None


def target_bases(conn, symbols: Optional[List[str]]) -> Dict[str, Tuple[str, str]]:
    """base -> (exchange, symbol) of the futures series used for validation (binance preferred)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (base_asset) base_asset, exchange, symbol
            FROM futures_daily_metrics
            WHERE exchange IN ('binance', 'bybit') AND date >= CURRENT_DATE - 7 AND base_asset IS NOT NULL
            ORDER BY base_asset, (exchange = 'binance') DESC, symbol
            """
        )
        out = {b.upper(): (ex, sym) for b, ex, sym in cur.fetchall()}
    if symbols:
        missing = [s for s in symbols if s not in out]
        if missing:
            print(f"[WARN] no binance/bybit futures in the last 7 days for: {', '.join(missing)}")
        out = {s: out[s] for s in symbols if s in out}
    return out


def existing_dates(conn, bases: Sequence[str], start: date, end: date) -> Dict[str, set]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT symbol, date FROM market_cap_history WHERE symbol = ANY(%s) AND date >= %s AND date < %s",
            (list(bases), start, end),
        )
        out: Dict[str, set] = {}
        for sym, d in cur.fetchall():
            out.setdefault(sym, set()).add(d)
    return out


def futures_closes(conn, exchange: str, symbol: str, start: date) -> Dict[date, float]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT date, price_close::float8 FROM futures_daily_metrics "
            "WHERE exchange = %s AND symbol = %s AND date >= %s AND price_close > 0",
            (exchange, symbol, start - timedelta(days=2)),
        )
        return {d: c for d, c in cur.fetchall()}


def candidate_ids(coins: List[dict], base: str) -> List[str]:
    """CoinGecko ids sharing the ticker, highest market cap first (one /coins/markets call)."""
    tickers = {base.lower()}
    for prefix in SCALE_PREFIXES:
        if base.startswith(prefix) and len(base) > len(prefix):
            tickers.add(base[len(prefix):].lower())
    ids = [c["id"] for c in coins if (c.get("symbol") or "").lower() in tickers]
    if not ids:
        return []
    ranked = []
    for i in range(0, len(ids), 200):
        data = cg_get("/coins/markets", {"vs_currency": "usd", "ids": ",".join(ids[i:i + 200]), "per_page": 250})
        ranked += [(c.get("market_cap") or 0, c["id"]) for c in data or []]
    ranked.sort(reverse=True)
    return [cid for cap, cid in ranked if cap > 0][:MAX_CANDIDATES]


def mcap_history(cg_id: str, start: date, end: date) -> Tuple[Dict[date, float], int]:
    """Daily 00:00 UTC market caps in [start, end); also returns how many points were dropped as non-midnight."""
    frm = datetime.combine(min(start, end - timedelta(days=91)), datetime.min.time(), tzinfo=UTC)
    to = datetime.combine(end, datetime.min.time(), tzinfo=UTC)
    data = cg_get(f"/coins/{cg_id}/market_chart/range",
                  {"vs_currency": "usd", "from": int(frm.timestamp()), "to": int(to.timestamp())})
    out: Dict[date, float] = {}
    dropped = 0
    for ts_ms, cap in (data or {}).get("market_caps", []):
        if ts_ms % 86_400_000 != 0:
            dropped += 1
            continue
        d = datetime.fromtimestamp(ts_ms / 1000, tz=UTC).date()
        if start <= d < end and cap is not None and cap > 0:
            out[d] = float(cap)
    return out, dropped


def check(caps: Dict[date, float], closes: Dict[date, float]) -> dict:
    """d_T = dln mcap(T) - dln close(T-1): mcap row T is the close of T-1."""
    diffs = []
    for t in sorted(caps):
        p, c1, c2 = caps.get(t - timedelta(days=1)), closes.get(t - timedelta(days=1)), closes.get(t - timedelta(days=2))
        if p and c1 and c2:
            d = math.log(caps[t] / p) - math.log(c1 / c2)
            if math.isfinite(d):
                diffs.append(d)
    abs_d = [abs(x) for x in diffs]
    out = {"pairs": len(diffs), "jump_days": sum(x > JUMP_ABS for x in abs_d)}
    if len(diffs) < MIN_PAIRS:
        return {**out, "ok": False, "reason": "insufficient_pairs"}
    judged = [x for x in abs_d if x <= JUMP_ABS]
    out["median_abs_diff"] = round(statistics.median(judged), 5) if judged else None
    out["share_within_2pct"] = round(sum(x <= WITHIN for x in judged) / len(judged), 3) if judged else 0.0
    # Identity test only: jump days (supply updates in CoinGecko's series) are reported, not judged here; the
    # screener's own 30-day check decides whether the rank is publishable in each window.
    ok = bool(judged) and out["median_abs_diff"] <= MEDIAN_ABS_MAX and out["share_within_2pct"] >= WITHIN_SHARE_MIN
    return {**out, "ok": ok, "reason": None if ok else "check_failed"}


def insert_rows(conn, rows: List[Tuple]) -> int:
    # RETURNING + fetch counts every page (cur.rowcount only reflects the last page of execute_values)
    with conn.cursor() as cur:
        inserted = execute_values(
            cur,
            """
            INSERT INTO market_cap_history (date, symbol, market_cap_rank, market_cap_usd, in_top_50, ever_in_top_50, source)
            VALUES %s
            ON CONFLICT (date, symbol) DO NOTHING
            RETURNING 1
            """,
            rows,
            template="(%s, %s, NULL, %s, false, false, 'coingecko')",
            page_size=1000,
            fetch=True,
        )
    conn.commit()
    return len(inserted)


def select_todo(bases: Sequence[str], have: Dict[str, set], start: date, end: date,
                auto: bool, recent_days: int, min_missing: int, max_bases: int) -> List[str]:
    """Bases to process. Manual mode: >= min_missing missing days in [start, end).
    Auto mode: no row at all in [start, end), or a missing day in [end - recent_days, end)."""
    expected = (end - start).days
    recent = {end - timedelta(days=k) for k in range(1, recent_days + 1)}
    scored = []
    for b in bases:
        got = have.get(b, set())
        if auto:
            miss_recent = len(recent - got)
            if not got or miss_recent:
                scored.append((-(recent_days + 1) if not got else -miss_recent, b))
        elif expected - len(got) >= min_missing:
            scored.append((0, b))
    scored.sort()
    out = [b for _, b in scored]
    return out[:max_bases] if max_bases > 0 else out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=365, help="history depth (Demo key serves at most 365 days)")
    ap.add_argument("--symbols", default="", help="comma list of base assets (default: listed futures universe)")
    ap.add_argument("--ids", default="", help="manual overrides SYMBOL=coingecko-id,... (still validated)")
    ap.add_argument("--min-missing", type=int, default=1, help="skip bases with fewer missing days")
    ap.add_argument("--apply", action="store_true", help="write to market_cap_history (default: dry run)")
    ap.add_argument("--auto", action="store_true", help="daily detection: no rows at all or a recent missing day")
    ap.add_argument("--recent-days", type=int, default=7, help="--auto: recent window checked for missing days")
    ap.add_argument("--max-bases", type=int, default=None,
                    help="cap of bases per run (default: 40 with --auto, unlimited otherwise)")
    args = ap.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("[ERROR] DATABASE_URL is required")
    end = datetime.now(UTC).date()          # today's row belongs to the daily snapshot
    start = end - timedelta(days=args.days)
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()] or None
    overrides = dict(kv.split("=", 1) for kv in args.ids.split(",") if "=" in kv)
    overrides = {k.strip().upper(): v.strip() for k, v in overrides.items()}

    conn = psycopg2.connect(db_url)
    try:
        bases = target_bases(conn, symbols)
        have = existing_dates(conn, list(bases), start, end)
        expected = (end - start).days
        max_bases = args.max_bases if args.max_bases is not None else (40 if args.auto else 0)
        picked = select_todo(list(bases), have, start, end, args.auto, args.recent_days, args.min_missing, max_bases)
        todo = {b: bases[b] for b in picked}
        rule = (f"no rows or a missing day in the last {args.recent_days}d" if args.auto
                else f">= {args.min_missing} missing days in [{start}, {end})")
        print(f"[INFO] {len(bases)} bases, {len(todo)} to process ({rule}; cap {max_bases or 'none'})"
              f" | mode={'APPLY' if args.apply else 'DRY RUN'}")
        if not todo:
            print("[DONE] no gaps detected")
            return
        coins = cg_get("/coins/list") or []
        report, total_new = [], 0
        for i, (base, (exchange, fsym)) in enumerate(todo.items(), 1):
            entry = {"base": base, "futures": f"{exchange}:{fsym}", "missing_days": expected - len(have.get(base, ()))}
            try:
                closes = futures_closes(conn, exchange, fsym, start)
                cands = [overrides[base]] if base in overrides else candidate_ids(coins, base)
                entry["candidates"] = cands
                chosen = None
                for cid in cands:
                    caps, dropped = mcap_history(cid, start, end)
                    res = check(caps, closes)
                    entry.setdefault("checks", {})[cid] = {**res, "points": len(caps), "non_midnight": dropped}
                    if res["ok"]:
                        chosen = (cid, caps)
                        break
                if chosen is None:
                    entry["status"] = "no_valid_id" if cands else "no_candidate"
                else:
                    cid, caps = chosen
                    rows = [(d, base, caps[d]) for d in sorted(caps) if d not in have.get(base, set())]
                    entry.update(status="ok", coingecko_id=cid, new_rows=len(rows),
                                 first=rows[0][0].isoformat() if rows else None)
                    if args.apply and rows:
                        entry["inserted"] = insert_rows(conn, rows)
                    total_new += len(rows)
            except Exception as exc:  # one asset failing must not stop the rest
                conn.rollback()
                entry.update(status="error", error=str(exc)[:300])
            report.append(entry)
            print(f"  [{i}/{len(todo)}] {base:<10} {entry['status']:<12} {entry.get('coingecko_id', '')} "
                  f"new={entry.get('new_rows', 0)} " + json.dumps(entry.get("checks", {}).get(entry.get("coingecko_id"), {})))
    finally:
        conn.close()

    REPORT_DIR.mkdir(exist_ok=True)
    out = REPORT_DIR / f"mcap_gaps_{datetime.now(UTC):%Y%m%dT%H%MZ}{'' if args.apply else '_dryrun'}.json"
    out.write_text(json.dumps(report, indent=1, default=str), encoding="utf-8")
    by_status: Dict[str, int] = {}
    for e in report:
        by_status[e["status"]] = by_status.get(e["status"], 0) + 1
    print(f"[DONE] {by_status} | rows {'inserted' if args.apply else 'to insert'}: {total_new} | report: {out}")


if __name__ == "__main__":
    main()
