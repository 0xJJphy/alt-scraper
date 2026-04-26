#!/usr/bin/env python3
"""
backfill_ls_ranges.py - Backfill of L/S daily high/low ranges.

Fetches hourly L/S ratio history from exchange native APIs and computes
daily MAX/MIN to populate ls_acc_global_high/low, ls_acc_top_high/low,
ls_pos_top_high/low columns in futures_daily_metrics.

Important API limitation discovered:
  Binance /futures/data/*LongShortRatio does NOT support startTime/endTime.
  Only `limit` is accepted (max 500 = last ~21 days of hourly data).
  Historical backfill beyond 21 days is not possible via Binance API.
  Going forward, realtime_daemon.py captures 4 snapshots/day.

Coverage:
  Binance — last 500 hourly points (~21 days). Global + top account + top position.
  Bybit   — last 200 hourly points (~8 days). Account ratio (global only).
  OKX     — last 100 hourly points (~4 days). Global + top account + top position.

Usage:
    python backfill_ls_ranges.py --exchange binance [--top 50] [--dry-run]
    python backfill_ls_ranges.py --exchange bybit   [--top 50] [--dry-run]
    python backfill_ls_ranges.py --exchange okx     [--top 50] [--dry-run]
    python backfill_ls_ranges.py --exchange all     [--top 50] [--dry-run]
"""

import os
import sys
import time
import argparse
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict, Tuple

import requests
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

UTC = timezone.utc
HEADERS = {"User-Agent": "alt-scraper/backfill"}

BINANCE_FUTURES_API = "https://fapi.binance.com"
BYBIT_V5_API        = "https://api.bybit.com/v5"
OKX_V5_API          = "https://www.okx.com/api/v5"


# ==============================================================================
# Helpers
# ==============================================================================

def _get(url: str, params: dict = None, timeout: int = 15) -> Optional[object]:
    try:
        r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.debug("GET %s %s — %s", url, params, e)
        return None


def _ts_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _from_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=UTC)


# ==============================================================================
# Binance backfiller
# ==============================================================================

BINANCE_LS_ENDPOINTS = {
    "ls_acc_global": "/futures/data/globalLongShortAccountRatio",
    "ls_acc_top":    "/futures/data/topLongShortAccountRatio",
    "ls_pos_top":    "/futures/data/topLongShortPositionRatio",
}


class BinanceLSBackfiller:
    EXCHANGE = "binance"
    PAGE     = 500   # max points per request
    PERIOD   = "1h"

    def _perp_symbol(self, base: str) -> str:
        return f"{base}USDT"

    def fetch_hourly(self, base: str, col: str, start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch last PAGE hourly L/S points (startTime not supported by Binance on this endpoint)."""
        endpoint = BINANCE_LS_ENDPOINTS[col]
        sym = self._perp_symbol(base)
        data = _get(
            f"{BINANCE_FUTURES_API}{endpoint}",
            {"symbol": sym, "period": self.PERIOD, "limit": self.PAGE},
        )
        if not data or not isinstance(data, list):
            return pd.DataFrame(columns=["ts", "value"])
        rows = [{"ts": _from_ms(item["timestamp"]), "value": float(item["longShortRatio"])}
                for item in data]
        df = pd.DataFrame(rows)
        df["date"] = df["ts"].dt.date
        # Filter to the requested date range
        df = df[(df["ts"] >= start) & (df["ts"] <= end)]
        return df

    def daily_ranges(self, base: str, start: datetime, end: datetime) -> pd.DataFrame:
        """Return DataFrame with columns: date, ls_acc_global_high/low, ls_acc_top_high/low, ls_pos_top_high/low."""
        frames = []
        for col, ep in BINANCE_LS_ENDPOINTS.items():
            log.info("    [binance] %s %s ...", base, col)
            df = self.fetch_hourly(base, col, start, end)
            if df.empty:
                continue
            agg = df.groupby("date")["value"].agg(
                **{f"{col}_high": "max", f"{col}_low": "min"}
            ).reset_index()
            frames.append(agg)

        if not frames:
            return pd.DataFrame()
        result = frames[0]
        for f in frames[1:]:
            result = result.merge(f, on="date", how="outer")
        return result


# ==============================================================================
# Bybit backfiller
# ==============================================================================

class BybitLSBackfiller:
    EXCHANGE = "bybit"
    PAGE     = 200
    PERIOD   = "1h"

    def _perp_symbol(self, base: str) -> str:
        return f"{base}USDT"

    def fetch_hourly(self, base: str, start: datetime, end: datetime) -> pd.DataFrame:
        """Fetch last PAGE hourly L/S points from Bybit."""
        sym = self._perp_symbol(base)
        data = _get(
            f"{BYBIT_V5_API}/market/account-ratio",
            {"category": "linear", "symbol": sym, "period": self.PERIOD, "limit": self.PAGE},
        )
        if not data:
            return pd.DataFrame(columns=["ts", "value"])
        items = data.get("result", {}).get("list", [])
        rows = []
        for item in items:
            ts   = _from_ms(int(item["timestamp"]))
            buy  = float(item.get("buyRatio", 0))
            sell = float(item.get("sellRatio", 1))
            if sell > 0:
                rows.append({"ts": ts, "value": buy / sell})
        if not rows:
            return pd.DataFrame(columns=["ts", "value"])
        df = pd.DataFrame(rows)
        df["date"] = df["ts"].dt.date
        df = df[(df["ts"] >= start) & (df["ts"] <= end)]
        return df

    def daily_ranges(self, base: str, start: datetime, end: datetime) -> pd.DataFrame:
        log.info("    [bybit] %s ls_acc_global ...", base)
        df = self.fetch_hourly(base, start, end)
        if df.empty:
            return pd.DataFrame()
        return df.groupby("date")["value"].agg(
            ls_acc_global_high="max", ls_acc_global_low="min"
        ).reset_index()


# ==============================================================================
# OKX backfiller
# ==============================================================================

OKX_LS_ENDPOINTS = {
    "ls_acc_global": "/rubik/stat/contracts/long-short-account-ratio",
    "ls_acc_top":    "/rubik/stat/contracts/top-traders-long-short-account-ratio",
    "ls_pos_top":    "/rubik/stat/contracts/top-traders-long-short-position-ratio",
}


class OKXLSBackfiller:
    EXCHANGE = "okx"
    PAGE     = 100
    PERIOD   = "1H"

    def fetch_hourly(self, base: str, col: str) -> pd.DataFrame:
        """OKX only supports limit, no startTime — returns last PAGE points."""
        data = _get(
            f"{OKX_V5_API}{OKX_LS_ENDPOINTS[col]}",
            {"ccy": base.upper(), "period": self.PERIOD, "limit": self.PAGE},
        )
        if not data:
            return pd.DataFrame(columns=["ts", "value"])
        rows = []
        for item in data.get("data", []):
            ts = _from_ms(int(item[0]))
            try:
                rows.append({"ts": ts, "value": float(item[1])})
            except (ValueError, IndexError):
                pass
        if not rows:
            return pd.DataFrame(columns=["ts", "value"])
        df = pd.DataFrame(rows)
        df["date"] = df["ts"].dt.date
        return df

    def daily_ranges(self, base: str, start: datetime, end: datetime) -> pd.DataFrame:
        frames = []
        for col in OKX_LS_ENDPOINTS:
            log.info("    [okx] %s %s ...", base, col)
            df = self.fetch_hourly(base, col)
            if df.empty:
                continue
            df = df[df["ts"] >= start]
            agg = df.groupby("date")["value"].agg(
                **{f"{col}_high": "max", f"{col}_low": "min"}
            ).reset_index()
            frames.append(agg)
            time.sleep(0.2)

        if not frames:
            return pd.DataFrame()
        result = frames[0]
        for f in frames[1:]:
            result = result.merge(f, on="date", how="outer")
        return result


# ==============================================================================
# Database helpers
# ==============================================================================

def load_assets(db_url: str, top_n: int) -> List[Dict]:
    """Load top_n non-filtered assets from asset_metadata."""
    conn = psycopg2.connect(db_url)
    cur  = conn.cursor()
    cur.execute("""
        SELECT symbol AS base_asset
        FROM asset_metadata
        WHERE is_filtered = false OR is_filtered IS NULL
        ORDER BY market_cap_rank ASC NULLS LAST
        LIMIT %s
    """, (top_n,))
    assets = [{"base_asset": row[0]} for row in cur.fetchall()]
    cur.close()
    conn.close()
    log.info("Loaded %d assets from asset_metadata.", len(assets))
    return assets


def load_date_range(db_url: str, base_asset: str, exchange: str) -> Optional[Tuple[datetime, datetime]]:
    """Return (min_date, max_date) for this asset+exchange in futures_daily_metrics."""
    conn = psycopg2.connect(db_url)
    cur  = conn.cursor()
    cur.execute("""
        SELECT min(date), max(date)
        FROM futures_daily_metrics
        WHERE base_asset = %s AND exchange = %s
    """, (base_asset, exchange))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row or row[0] is None:
        return None
    start = datetime.combine(row[0], datetime.min.time(), tzinfo=UTC)
    end   = datetime.combine(row[1], datetime.max.time(), tzinfo=UTC)
    return start, end


LS_COLS = [
    "ls_acc_global_high", "ls_acc_global_low",
    "ls_acc_top_high",    "ls_acc_top_low",
    "ls_pos_top_high",    "ls_pos_top_low",
]

UPDATE_SQL = """
UPDATE futures_daily_metrics fdm
SET
    ls_acc_global_high = COALESCE(v.ls_acc_global_high, fdm.ls_acc_global_high),
    ls_acc_global_low  = COALESCE(v.ls_acc_global_low,  fdm.ls_acc_global_low),
    ls_acc_top_high    = COALESCE(v.ls_acc_top_high,    fdm.ls_acc_top_high),
    ls_acc_top_low     = COALESCE(v.ls_acc_top_low,     fdm.ls_acc_top_low),
    ls_pos_top_high    = COALESCE(v.ls_pos_top_high,    fdm.ls_pos_top_high),
    ls_pos_top_low     = COALESCE(v.ls_pos_top_low,     fdm.ls_pos_top_low)
FROM (VALUES %s) AS v(
    date, symbol, exchange,
    ls_acc_global_high, ls_acc_global_low,
    ls_acc_top_high,    ls_acc_top_low,
    ls_pos_top_high,    ls_pos_top_low
)
WHERE fdm.date    = v.date::date
  AND fdm.exchange = v.exchange
  AND fdm.base_asset = v.symbol
"""

UPDATE_TEMPLATE = "(%s::date, %s::text, %s::text, %s::numeric, %s::numeric, %s::numeric, %s::numeric, %s::numeric, %s::numeric)"


def apply_ranges(db_url: str, base_asset: str, exchange: str, ranges_df: pd.DataFrame, dry_run: bool):
    if ranges_df.empty:
        log.info("    No data to write for %s/%s.", base_asset, exchange)
        return

    # Ensure all LS columns exist (fill missing with None)
    for col in LS_COLS:
        if col not in ranges_df.columns:
            ranges_df[col] = None

    records = [
        (
            str(row["date"]), base_asset, exchange,
            row.get("ls_acc_global_high"), row.get("ls_acc_global_low"),
            row.get("ls_acc_top_high"),    row.get("ls_acc_top_low"),
            row.get("ls_pos_top_high"),    row.get("ls_pos_top_low"),
        )
        for _, row in ranges_df.iterrows()
    ]

    if dry_run:
        log.info("    [DRY RUN] Would update %d dates for %s/%s.", len(records), base_asset, exchange)
        if records:
            log.info("    Sample: %s", records[0])
        return

    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cur  = conn.cursor()
        execute_values(cur, UPDATE_SQL, records, template=UPDATE_TEMPLATE, page_size=500)
        updated = cur.rowcount
        conn.commit()
        log.info("    Updated %d/%d rows for %s/%s.", updated, len(records), base_asset, exchange)
    except Exception as e:
        log.error("    DB update failed for %s/%s: %s", base_asset, exchange, e)
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


# ==============================================================================
# Main
# ==============================================================================

BACKFILLERS = {
    "binance": BinanceLSBackfiller,
    "bybit":   BybitLSBackfiller,
    "okx":     OKXLSBackfiller,
}


def main():
    parser = argparse.ArgumentParser(description="Backfill L/S daily high/low ranges")
    parser.add_argument("--exchange", required=True, choices=["binance", "bybit", "okx", "all"])
    parser.add_argument("--top",      type=int, default=50)
    parser.add_argument("--dry-run",  action="store_true", help="Print what would be updated without writing")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        log.error("DATABASE_URL not set.")
        sys.exit(1)

    assets    = load_assets(db_url, args.top)
    exchanges = list(BACKFILLERS.keys()) if args.exchange == "all" else [args.exchange]

    for exchange in exchanges:
        backfill = BACKFILLERS[exchange]()
        log.info("Starting backfill — exchange=%s, assets=%d, dry_run=%s",
                 exchange, len(assets), args.dry_run)

        for i, asset in enumerate(assets, 1):
            base = asset["base_asset"]
            log.info("[%d/%d] %s / %s", i, len(assets), base, exchange)

            date_range = load_date_range(db_url, base, exchange)
            if not date_range:
                log.info("  No rows in futures_daily_metrics — skipping.")
                continue

            start, end = date_range
            log.info("  Date range: %s → %s", start.date(), end.date())

            try:
                ranges_df = backfill.daily_ranges(base, start, end)
            except Exception as e:
                log.error("  Fetch failed for %s: %s", base, e)
                continue

            apply_ranges(db_url, base, exchange, ranges_df, args.dry_run)
            time.sleep(0.5)

    log.info("Backfill complete.")


if __name__ == "__main__":
    main()
