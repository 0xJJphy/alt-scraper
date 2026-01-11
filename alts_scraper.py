#!/usr/bin/env python3
"""
alts_scraper.py - Coinalyze Historical Data Backfill

This script fetches historical futures/perpetual contract data for the top N crypto tokens
(excluding stablecoins) from the Coinalyze API. It collects all available metrics:

- Open Interest (OHLC in USD)
- Funding Rate (OHLC)
- Predicted Funding Rate (OHLC)
- Long/Short Ratio (ratio, longs, shorts)
- Liquidations (long liquidations, short liquidations)
- OHLCV (price OHLC, volume, transactions)

Data is saved at daily timeframe for maximum historical coverage.

Author: Quantitative Trading Systems
Date: 2026-01-10
"""

import os
import time
import argparse
import requests
import pandas as pd
import psycopg2
import warnings
import concurrent.futures
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, timezone, time as dtime

# Suppress pandas warning about raw DB connections
warnings.filterwarnings("ignore", ".*pandas only supports SQLAlchemy connectable.*")
from typing import Dict, List, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ==============================================================================
# API Configuration
# ==============================================================================
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
COINALYZE_BASE = "https://api.coinalyze.net/v1"

# Coinalyze API Endpoints
COINALYZE_ENDPOINTS = {
    "future_markets": f"{COINALYZE_BASE}/future-markets",
    "spot_markets": f"{COINALYZE_BASE}/spot-markets",
    "exchanges": f"{COINALYZE_BASE}/exchanges",
    "open_interest_history": f"{COINALYZE_BASE}/open-interest-history",
    "funding_rate_history": f"{COINALYZE_BASE}/funding-rate-history",
    "predicted_funding_rate_history": f"{COINALYZE_BASE}/predicted-funding-rate-history",
    "long_short_ratio_history": f"{COINALYZE_BASE}/long-short-ratio-history",
    "liquidation_history": f"{COINALYZE_BASE}/liquidation-history",
    "ohlcv_history": f"{COINALYZE_BASE}/ohlcv-history",
}

# Native Exchange API Endpoints
BINANCE_FUTURES_API = "https://fapi.binance.com"
BYBIT_V5_API = "https://api.bybit.com/v5"
OKX_V5_API = "https://www.okx.com/api/v5"

UTC = timezone.utc

STABLES_CATEGORIES = [
    "stablecoins", "usd-stablecoin", "wrapped-tokens", "liquid-staking-tokens",
    "tokenized-btc", "asset-backed-tokens", "synths", "bridged-tokens"
]

# Known stablecoins and wrapped tokens (to filter without slow API calls)
KNOWN_FILTERED_SYMBOLS = {
    # Stablecoins
    "USDT", "USDC", "DAI", "BUSD", "TUSD", "USDP", "GUSD", "FRAX", "LUSD", "USDD",
    "PYUSD", "FDUSD", "EURC", "EURT", "XAUT", "PAXG", "GHO", "CRVUSD", "MKUSD",
    "USDE", "USDX", "USD0", "USDY", "SUSD", "RAI", "FEI", "MIM", "DOLA", "ALUSD",
    # Wrapped tokens
    "WBTC", "WETH", "WBNB", "STETH", "WSTETH", "RETH", "CBETH", "FRXETH", "SFRXETH",
    "MSOL", "BNSOL", "JITOETH", "EZETH", "WEETH", "RSETH", "METH", "SWETH",
    "TBTC", "HBTC", "RENBTC", "SBTC", "OBTC", "PBTC",
    # Liquid staking derivatives
    "STMATIC", "STSOL", "STETHR", "STANEAR",
}

def _get_default_narrative(market_cap_rank: Optional[int]) -> str:
    """Assign a default narrative based on market cap rank."""
    if market_cap_rank is None:
        return "Cryptocurrency"
    if market_cap_rank <= 10:
        return "Blue Chip"
    elif market_cap_rank <= 50:
        return "Large Cap"
    elif market_cap_rank <= 100:
        return "Mid Cap"
    else:
        return "Small Cap"

# ==============================================================================
# Exchange Configuration
# ==============================================================================
# Each exchange has a different symbol format for perpetual contracts
# Format: (exchange_code, symbol_pattern)
# Verified from Coinalyze API /v1/future-markets endpoint:
#   .0 = KuCoin       .6 = Bybit        .4 = Bitget       .3 = OKX
#   .7 = BitMEX       .8 = Deribit      .A = Binance      .F = CoinEx
#   .H = Huobi        .K = Kraken       .V = MEXC         .W = Phemex
#   .Y = Gate.io
EXCHANGE_FORMATS = {
    "binance": ("A", "{BASE}USDT_PERP.A"),             # BTCUSDT_PERP.A
    "bybit": ("6", "{BASE}USDT.6"),                    # BTCUSDT.6
    "okx": ("3", "{BASE}USDT_PERP.3"),                 # BTCUSDT_PERP.3
    "deribit": ("8", "{BASE}-USD.8"),                  # BTC-USD.8
    "bitget": ("4", "{BASE}USDT_PERP.4"),              # BTCUSDT_PERP.4
    "gate": ("Y", "{BASE}_USDT.Y"),                    # BTC_USDT.Y
    "huobi": ("H", "{BASE}.H"),                        # BTC.H
    "kraken": ("K", "{BASE}USD_PERP.K"),               # BTCUSD_PERP.K
    "bitmex": ("7", "{BASE}USD.7"),                    # BTCUSD.7 (only BTC available)
    "kucoin": ("0", "{BASE}USDT_PERP.0"),              # BTCUSDT_PERP.0
    "mexc": ("V", "{BASE}-PERP.V"),                    # BTC-PERP.V
    "phemex": ("W", "PERP_{BASE}_USDT.W"),             # PERP_BTC_USDT.W
    "coinex": ("F", "{BASE}USDT_PERP.F"),              # BTCUSDT_PERP.F
}

# Legacy EXCHANGE_CODES for backwards compatibility
EXCHANGE_CODES = {k: v[0] for k, v in EXCHANGE_FORMATS.items()}

# Default exchanges to fetch data from (can be overridden via CLI)
DEFAULT_EXCHANGES = ["binance", "bybit", "okx"]


# ==============================================================================
# Utility Functions
# ==============================================================================
def ensure_dir(path: str) -> None:
    """Create directory if it doesn't exist."""
    os.makedirs(path, exist_ok=True)


def to_unix_seconds(dt: datetime) -> int:
    """Convert datetime to Unix timestamp in seconds."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp())


class DatabaseManager:
    """Handles communication with Supabase (PostgreSQL)."""
    def __init__(self, db_url: Optional[str] = None):
        if not db_url:
            db_url = os.getenv("DATABASE_URL")
        self.db_url = db_url
        self.enabled = bool(db_url)
        if self.enabled:
            print("[DB] Supabase Integration Enabled.")
        else:
            print("[DB] Supabase Integration Disabled (DATABASE_URL missing).")

    def _to_python(self, val):
        """Convert numpy/pandas types to native Python types for psycopg2."""
        if val is None or pd.isna(val):
            return None
        # Handle numpy types
        if hasattr(val, 'item'):  # numpy scalar
            val = val.item()
        # Handle infinity
        if isinstance(val, float) and (val == float('inf') or val == float('-inf')):
            return None
        return val

    def _sanitize_float(self, val) -> Optional[float]:
        """Convert to float, handling NaN/None/Inf."""
        val = self._to_python(val)
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _sanitize_int(self, val) -> Optional[int]:
        """Convert to int, handling NaN/None/Inf/Overflow."""
        val = self._to_python(val)
        if val is None:
            return None
        try:
            val = int(float(val))
            # Postgres BIGINT range
            if val > 9223372036854775807 or val < -9223372036854775808:
                return None
            return val
        except (ValueError, TypeError, OverflowError):
            return None

    def upsert_futures_metrics(self, df: pd.DataFrame):
        """Batch upsert futures metrics using execute_values (50-100x faster)."""
        if not self.enabled or df.empty:
            return

        conn = None
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()

            # Prepare records as list of tuples with proper type conversion
            records = []
            for _, row in df.iterrows():
                records.append((
                    self._to_python(row.get('date')),
                    self._to_python(row.get('symbol')),
                    self._to_python(row.get('exchange')),
                    self._to_python(row.get('base_asset')),
                    self._sanitize_float(row.get('oi_usd_open')),
                    self._sanitize_float(row.get('oi_usd_high')),
                    self._sanitize_float(row.get('oi_usd_low')),
                    self._sanitize_float(row.get('oi_usd_close')),
                    self._sanitize_float(row.get('funding_open')),
                    self._sanitize_float(row.get('funding_high')),
                    self._sanitize_float(row.get('funding_low')),
                    self._sanitize_float(row.get('funding_close')),
                    self._sanitize_float(row.get('pred_funding_open')),
                    self._sanitize_float(row.get('pred_funding_high')),
                    self._sanitize_float(row.get('pred_funding_low')),
                    self._sanitize_float(row.get('pred_funding_close')),
                    self._sanitize_float(row.get('ls_ratio')),
                    self._sanitize_float(row.get('longs_qty')),
                    self._sanitize_float(row.get('shorts_qty')),
                    self._sanitize_float(row.get('ls_acc_global')),
                    self._sanitize_float(row.get('ls_acc_top')),
                    self._sanitize_float(row.get('ls_pos_top')),
                    self._sanitize_float(row.get('liq_longs')),
                    self._sanitize_float(row.get('liq_shorts')),
                    self._sanitize_float(row.get('liq_total')),
                    self._sanitize_float(row.get('price_open')),
                    self._sanitize_float(row.get('price_high')),
                    self._sanitize_float(row.get('price_low')),
                    self._sanitize_float(row.get('price_close')),
                    self._sanitize_float(row.get('volume_usd')),
                    self._sanitize_float(row.get('volume_base')),
                    self._sanitize_float(row.get('buy_volume_base')),
                    self._sanitize_float(row.get('sell_volume_base')),
                    self._sanitize_float(row.get('volume_delta')),
                    self._sanitize_int(row.get('txn_count')),
                    self._sanitize_int(row.get('buy_txn_count')),
                    self._sanitize_int(row.get('sell_txn_count'))
                ))

            # Batch INSERT with ON CONFLICT (upsert)
            sql = """
                INSERT INTO futures_daily_metrics (
                    date, symbol, exchange, base_asset,
                    oi_usd_open, oi_usd_high, oi_usd_low, oi_usd_close,
                    funding_open, funding_high, funding_low, funding_close,
                    pred_funding_open, pred_funding_high, pred_funding_low, pred_funding_close,
                    ls_ratio, longs_qty, shorts_qty,
                    ls_acc_global, ls_acc_top, ls_pos_top,
                    liq_longs, liq_shorts, liq_total,
                    price_open, price_high, price_low, price_close,
                    volume_usd, volume_base, buy_volume_base, sell_volume_base, volume_delta,
                    txn_count, buy_txn_count, sell_txn_count,
                    updated_at
                ) VALUES %s
                ON CONFLICT (date, symbol, exchange) DO UPDATE SET
                    base_asset = COALESCE(EXCLUDED.base_asset, futures_daily_metrics.base_asset),
                    oi_usd_open = COALESCE(EXCLUDED.oi_usd_open, futures_daily_metrics.oi_usd_open),
                    oi_usd_high = COALESCE(EXCLUDED.oi_usd_high, futures_daily_metrics.oi_usd_high),
                    oi_usd_low = COALESCE(EXCLUDED.oi_usd_low, futures_daily_metrics.oi_usd_low),
                    oi_usd_close = COALESCE(EXCLUDED.oi_usd_close, futures_daily_metrics.oi_usd_close),
                    funding_open = COALESCE(EXCLUDED.funding_open, futures_daily_metrics.funding_open),
                    funding_high = COALESCE(EXCLUDED.funding_high, futures_daily_metrics.funding_high),
                    funding_low = COALESCE(EXCLUDED.funding_low, futures_daily_metrics.funding_low),
                    funding_close = COALESCE(EXCLUDED.funding_close, futures_daily_metrics.funding_close),
                    pred_funding_open = COALESCE(EXCLUDED.pred_funding_open, futures_daily_metrics.pred_funding_open),
                    pred_funding_high = COALESCE(EXCLUDED.pred_funding_high, futures_daily_metrics.pred_funding_high),
                    pred_funding_low = COALESCE(EXCLUDED.pred_funding_low, futures_daily_metrics.pred_funding_low),
                    pred_funding_close = COALESCE(EXCLUDED.pred_funding_close, futures_daily_metrics.pred_funding_close),
                    ls_ratio = COALESCE(EXCLUDED.ls_ratio, futures_daily_metrics.ls_ratio),
                    longs_qty = COALESCE(EXCLUDED.longs_qty, futures_daily_metrics.longs_qty),
                    shorts_qty = COALESCE(EXCLUDED.shorts_qty, futures_daily_metrics.shorts_qty),
                    ls_acc_global = COALESCE(EXCLUDED.ls_acc_global, futures_daily_metrics.ls_acc_global),
                    ls_acc_top = COALESCE(EXCLUDED.ls_acc_top, futures_daily_metrics.ls_acc_top),
                    ls_pos_top = COALESCE(EXCLUDED.ls_pos_top, futures_daily_metrics.ls_pos_top),
                    liq_longs = COALESCE(EXCLUDED.liq_longs, futures_daily_metrics.liq_longs),
                    liq_shorts = COALESCE(EXCLUDED.liq_shorts, futures_daily_metrics.liq_shorts),
                    liq_total = COALESCE(EXCLUDED.liq_total, futures_daily_metrics.liq_total),
                    price_open = COALESCE(EXCLUDED.price_open, futures_daily_metrics.price_open),
                    price_high = COALESCE(EXCLUDED.price_high, futures_daily_metrics.price_high),
                    price_low = COALESCE(EXCLUDED.price_low, futures_daily_metrics.price_low),
                    price_close = COALESCE(EXCLUDED.price_close, futures_daily_metrics.price_close),
                    volume_usd = COALESCE(EXCLUDED.volume_usd, futures_daily_metrics.volume_usd),
                    volume_base = COALESCE(EXCLUDED.volume_base, futures_daily_metrics.volume_base),
                    buy_volume_base = COALESCE(EXCLUDED.buy_volume_base, futures_daily_metrics.buy_volume_base),
                    sell_volume_base = COALESCE(EXCLUDED.sell_volume_base, futures_daily_metrics.sell_volume_base),
                    volume_delta = COALESCE(EXCLUDED.volume_delta, futures_daily_metrics.volume_delta),
                    txn_count = COALESCE(EXCLUDED.txn_count, futures_daily_metrics.txn_count),
                    buy_txn_count = COALESCE(EXCLUDED.buy_txn_count, futures_daily_metrics.buy_txn_count),
                    sell_txn_count = COALESCE(EXCLUDED.sell_txn_count, futures_daily_metrics.sell_txn_count),
                    updated_at = NOW()
            """

            # Template: 37 columns + NOW()
            template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"

            execute_values(cur, sql, records, template=template, page_size=1000)
            conn.commit()

            cur.close()
            print(f"    [DB] Batch upserted {len(records)} rows.")

        except Exception as e:
            print(f"    [DB ERROR] Batch insert failed: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    def get_last_data_date(self, symbol: str, exchange: str) -> Optional[datetime]:
        """Get the last stored date for a symbol/exchange in the DB."""
        if not self.enabled: return None
        conn = None
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            cur.execute("SELECT MAX(date) FROM futures_daily_metrics WHERE symbol = %s AND exchange = %s", (symbol, exchange))
            res = cur.fetchone()
            if res and res[0]: return res[0]
            return None
        except Exception as e:
            print(f"    [DB INFO] Could not fetch last date for {symbol}: {e}")
            return None
        finally:
             if conn: conn.close()
             
    def get_all_asset_metadata(self) -> pd.DataFrame:
        """Fetch all asset metadata from DB."""
        if not self.enabled: return pd.DataFrame()
        conn = None
        try:
            conn = psycopg2.connect(self.db_url)
            query = "SELECT symbol, narrative, is_filtered, market_cap, market_cap_rank FROM asset_metadata"
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            print(f"    [DB INFO] Could not fetch metadata: {e}")
            return pd.DataFrame()
        finally:
            if conn: conn.close()

    def upsert_asset_metadata(self, symbol: str, narrative: str, is_filtered: int, market_cap: Optional[float] = None, market_cap_rank: Optional[int] = None):
        """Upsert asset metadata into asset_metadata table."""
        if not self.enabled: return
        conn = None
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            cur.execute(
                "SELECT upsert_asset_metadata(%s::VARCHAR, %s::VARCHAR, %s::BOOLEAN, %s::DECIMAL, %s::INTEGER)",
                (symbol, narrative, bool(is_filtered), self._sanitize_float(market_cap), self._sanitize_int(market_cap_rank))
            )
            conn.commit()
        except Exception as e:
            print(f"    [DB ERROR] Metadata upsert failed for {symbol}: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()


def get_incremental_start(path: str, default_start_sec: int, symbol: str, exchange: str, db_manager: Optional[DatabaseManager] = None) -> int:
    """
    Find start timestamp from:
    1. Database last record (if available)
    2. Local CSV file (if available)
    3. Default value
    """
    last_date = None
    
    # 1. Check Database
    if db_manager and db_manager.enabled:
        last_date_db = db_manager.get_last_data_date(symbol, exchange)
        if last_date_db:
             last_date = datetime.combine(last_date_db, datetime.min.time(), tzinfo=timezone.utc)
             print(f"    [Start] Found DB record: {last_date.date()}")

    # 2. Check CSV if no DB record found
    if not last_date and os.path.exists(path):
        try:
            df = pd.read_csv(path)
            if not df.empty and 'date' in df.columns:
                 last_date_str = df['date'].max()
                 last_date = datetime.strptime(last_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                 print(f"    [Start] Found CSV record: {last_date.date()}")
        except Exception as e:
             print(f"    [INFO] Could not read existing file: {e}")

    # Return logic
    if last_date:
        # Re-fetch the last 14 days to ensure completeness (Coinalyze lag fix)
        fast_forward_sec = to_unix_seconds(last_date - timedelta(days=14))
        
        # If the user didn't specify a start date (it's the 2017 default), 
        # or if the requested start is newer than our fast_forward, we use incremental.
        if default_start_sec <= 1483228800:
            print(f"    [Start] Incremental mode (14d overlap): {datetime.fromtimestamp(fast_forward_sec, tz=UTC).date()}")
            return fast_forward_sec
            
        # If user provided a custom start date newer than 2017, respect it
        return default_start_sec
        
    return default_start_sec


class AssetMetadataManager:
    def __init__(self, file_path: str = "data/asset_metadata.csv", db_manager: Optional[DatabaseManager] = None, allow_csv: bool = True):
        self.file_path = file_path
        self.db_manager = db_manager
        self.allow_csv = allow_csv
        
        # Load from DB first if enabled
        db_loaded = False
        if self.db_manager and self.db_manager.enabled:
            print("  [Meta] Loading from Database...")
            db_df = self.db_manager.get_all_asset_metadata()
            if not db_df.empty:
                self.df = db_df
                db_loaded = True
                print(f"  [Meta] Loaded {len(self.df)} assets from DB")

        # Fallback to CSV or create new
        if not db_loaded:
             if os.path.exists(file_path):
                try:
                    self.df = pd.read_csv(file_path)
                    if 'is_filtered' in self.df.columns:
                        self.df['is_filtered'] = self.df['is_filtered'].astype(int)
                except:
                     self.df = pd.DataFrame(columns=['symbol', 'narrative', 'is_filtered'])
             else:
                self.df = pd.DataFrame(columns=['symbol', 'narrative', 'is_filtered'])

        # Create/Touch CSV if allowed
        if self.allow_csv:
             os.makedirs(os.path.dirname(file_path), exist_ok=True)
             if not os.path.exists(file_path) or not db_loaded: # Only write if new or not from DB
                 self.df.to_csv(file_path, index=False)

    def _select_best_narrative(self, categories: List[str]) -> str:
        """Pick the most significant narrative from a list of categories."""
        if not categories: return "Unknown"
        generic_terms = ["Ecosystem", "Standard", "Portfolio", "Asset-Backed", "Wrapped", "Index", "SEC Securities", "Alleged", "FTX Holdings", "Multicoin Capital", "Alameda Research", "GMCI", "Proof of", "Made in", "CoinList", "Launchpad", "Research", "Ventures", "Capital"]
        specific = [c for c in categories if not any(x in c for x in generic_terms)]
        if specific:
            detailed = [c for c in specific if c not in ["Layer 1 (L1)", "Layer 2 (L2)", "Smart Contract Platform"]]
            return detailed[0] if detailed else specific[0]
        return categories[0]

    def get_metadata(self, symbol: str, coin_id: str, market_cap: Optional[float] = None, market_cap_rank: Optional[int] = None) -> Dict:
        """Get narrative and filter status, checking cache first.

        Optimized to avoid slow CoinGecko API calls:
        1. Check cache (DB/CSV) first
        2. Use KNOWN_FILTERED_SYMBOLS for instant stablecoin/wrapped detection
        3. Assign default narrative based on market_cap_rank if not in cache
        """
        symbol = symbol.upper()
        cache_row = self.df[self.df['symbol'] == symbol]

        # 1. Check cache first
        if not cache_row.empty:
            row = cache_row.iloc[0]
            if market_cap is not None:
                self.df.loc[self.df['symbol'] == symbol, 'market_cap'] = market_cap
                self.df.loc[self.df['symbol'] == symbol, 'market_cap_rank'] = market_cap_rank
                if self.db_manager and self.db_manager.enabled:
                    self.db_manager.upsert_asset_metadata(symbol, row['narrative'], int(row['is_filtered']), market_cap, market_cap_rank)
            return {"narrative": row['narrative'], "is_filtered": int(row['is_filtered'])}

        # 2. Check known filtered symbols (instant, no API call)
        if symbol in KNOWN_FILTERED_SYMBOLS:
            narrative = "Stablecoin/Wrapped"
            is_filtered = 1
            self._save_metadata(symbol, narrative, is_filtered, market_cap, market_cap_rank)
            return {"narrative": narrative, "is_filtered": is_filtered}

        # 3. For unknown tokens, assign default narrative based on rank (no API call)
        narrative = _get_default_narrative(market_cap_rank)
        is_filtered = 0
        self._save_metadata(symbol, narrative, is_filtered, market_cap, market_cap_rank)
        return {"narrative": narrative, "is_filtered": is_filtered}

    def _save_metadata(self, symbol: str, narrative: str, is_filtered: int, market_cap: Optional[float], market_cap_rank: Optional[int]):
        """Helper to save metadata to cache and DB."""
        new_row = pd.DataFrame([{
            'symbol': symbol,
            'narrative': narrative,
            'is_filtered': is_filtered,
            'market_cap': market_cap,
            'market_cap_rank': market_cap_rank
        }])
        self.df = pd.concat([self.df, new_row], ignore_index=True).drop_duplicates('symbol')

        if self.db_manager and self.db_manager.enabled:
            self.db_manager.upsert_asset_metadata(symbol, narrative, is_filtered, market_cap, market_cap_rank)

        if self.allow_csv:
            self.df.to_csv(self.file_path, index=False)

def coingecko_get_top_candidates(n: int = 50, specific_symbols: Optional[List[str]] = None, max_retries: int = 3) -> List[Dict]:
    """Fetch top tokens from CoinGecko markets with retry for null market_cap."""
    print(f"[INFO] Fetching market data from CoinGecko (specific={bool(specific_symbols)})...")

    url = f"{COINGECKO_BASE}/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 250, "page": 1, "sparkline": "false"}
    if specific_symbols:
        params["symbols"] = ",".join(specific_symbols).lower()
        params["per_page"] = 100

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }

    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)
            if resp.status_code == 429:
                wait_time = int(resp.headers.get("Retry-After", 60))
                print(f"[CG] Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
                continue

            resp.raise_for_status()
            data = resp.json()

            out = []
            null_count = 0
            for coin in data:
                mc = coin.get("market_cap")
                if mc is None:
                    null_count += 1
                out.append({
                    "symbol": coin.get("symbol", "").upper(),
                    "id": coin.get("id"),
                    "market_cap": mc,
                    "market_cap_rank": coin.get("market_cap_rank")
                })

            # If more than 20% of market caps are null, retry after delay
            if len(out) > 0 and null_count / len(out) > 0.2:
                print(f"[CG] Warning: {null_count}/{len(out)} tokens have null market_cap, retrying in 5s...")
                time.sleep(5)
                continue

            if null_count > 0:
                print(f"[CG] Note: {null_count} tokens have null market_cap")

            return out

        except requests.exceptions.Timeout:
            print(f"[CG] Timeout, attempt {attempt+1}/{max_retries}")
            time.sleep(2 ** attempt)
        except Exception as e:
            print(f"[ERROR] CG Markets API failed: {e}")
            time.sleep(2 ** attempt)

    print("[CG] All retries failed, returning empty list")
    return []


# ==============================================================================
# Coinalyze API Client
# ==============================================================================
class CoinalyzeClient:
    """
    Client for Coinalyze REST API with rate limiting and retry logic.
    
    API Rate Limit: 40 calls per minute
    See: https://coinalyze.net/api-docs/
    """
    
    def __init__(self, api_key: str, rate_delay: float = 1.6):
        """
        Initialize Coinalyze client.
        
        Args:
            api_key: Coinalyze API key
            rate_delay: Delay between API calls in seconds (default 1.6s = ~37 calls/min)
        """
        self.api_key = api_key
        self.rate_delay = rate_delay
        self._future_symbols_cache: Optional[set] = None
        
    def _get(self, url: str, params: dict) -> Optional[dict]:
        """
        Make GET request to Coinalyze API with retry logic.
        
        Args:
            url: API endpoint URL
            params: Query parameters
            
        Returns:
            JSON response or None if request failed
        """
        if not self.api_key:
            print("[ERROR] No API key provided")
            return None
            
        params = dict(params or {})
        params["api_key"] = self.api_key
        
        for attempt in range(5):
            time.sleep(self.rate_delay)
            try:
                resp = requests.get(url, params=params, timeout=60)
                
                if resp.status_code == 200:
                    return resp.json()
                    
                if resp.status_code == 429:
                    # Rate limited - wait for Retry-After header
                    retry_after = int(resp.headers.get("Retry-After", "30"))
                    print(f"[WARN] Rate limited, waiting {retry_after}s...")
                    time.sleep(retry_after + 1)
                    continue
                    
                if resp.status_code in (400, 401, 403, 404):
                    # Client error - symbol not supported or invalid request
                    return None
                    
                # Server error - exponential backoff
                time.sleep(2 ** attempt)
                
            except requests.RequestException as e:
                print(f"[ERROR] Request failed: {e}")
                time.sleep(2 ** attempt)
                
        return None
    
    def load_future_symbols(self) -> dict:
        """
        Load all available futures symbols from Coinalyze and organize by exchange.
        
        Returns:
            Dict mapping exchange codes to sets of symbols
        """
        if self._future_symbols_cache is not None:
            return self._future_symbols_cache
            
        print("[INFO] Loading Coinalyze futures market list...")
        data = self._get(COINALYZE_ENDPOINTS["future_markets"], params={})
        
        # Organize symbols by exchange code
        symbols_by_exchange = {}
        all_symbols = set()
        
        if isinstance(data, list):
            for market in data:
                symbol = market.get("symbol", "")
                if not symbol:
                    continue
                    
                all_symbols.add(symbol)
                
                # Extract exchange code from symbol (e.g., 'BTCUSDT_PERP.A' -> 'A')
                if '.' in symbol:
                    exchange_code = symbol.split('.')[-1]
                else:
                    exchange_code = None  # Aggregated
                    
                if exchange_code not in symbols_by_exchange:
                    symbols_by_exchange[exchange_code] = set()
                symbols_by_exchange[exchange_code].add(symbol)
        
        self._future_symbols_cache = {
            'all': all_symbols,
            'by_exchange': symbols_by_exchange
        }
        
        print(f"[INFO] Found {len(all_symbols)} futures markets across {len(symbols_by_exchange)} exchange codes")
        return self._future_symbols_cache
    
    def find_symbol_for_base(self, base: str, exchange: str) -> str:
        """
        Find the actual Coinalyze symbol for a base asset on a specific exchange.
        
        Uses EXACT matching patterns for each exchange to avoid incorrect matches
        like BTCDOMUSDT instead of BTCUSDT.
        
        Args:
            base: Base asset symbol (e.g., 'BTC', 'ETH')
            exchange: Exchange name (e.g., 'binance', 'bybit', 'okx')
            
        Returns:
            The matching Coinalyze symbol, or None if not found
        """
        cache = self.load_future_symbols()
        exchange_lower = exchange.lower()
        
        if exchange_lower not in EXCHANGE_FORMATS:
            return None
            
        exchange_code, _ = EXCHANGE_FORMATS[exchange_lower]
        base_upper = base.upper()
        
        # Get symbols for this exchange code
        symbols_by_exchange = cache.get('by_exchange', {})
        exchange_symbols = symbols_by_exchange.get(exchange_code, set())
        
        # Define EXACT patterns for each exchange (in priority order)
        # Verified from Coinalyze API /v1/future-markets endpoint
        exact_patterns = {
            'binance': [
                f"{base_upper}USDT_PERP.A",      # BTCUSDT_PERP.A
                f"{base_upper}USD_PERP.A",       # Coin-margined
            ],
            'bybit': [
                f"{base_upper}USDT.6",           # BTCUSDT.6
                f"{base_upper}USD.6",
            ],
            'okx': [
                f"{base_upper}USDT_PERP.3",      # BTCUSDT_PERP.3
                f"{base_upper}USD_PERP.3",       # Coin-margined
            ],
            'deribit': [
                f"{base_upper}-USD.8",           # BTC-USD.8
            ],
            'bitget': [
                f"{base_upper}USDT_PERP.4",      # BTCUSDT_PERP.4
                f"{base_upper}USD_PERP.4",
            ],
            'gate': [
                f"{base_upper}_USDT.Y",          # BTC_USDT.Y
            ],
            'huobi': [
                f"{base_upper}.H",               # BTC.H
            ],
            'kraken': [
                f"{base_upper}USD_PERP.K",       # BTCUSD_PERP.K
            ],
            'bitmex': [
                f"{base_upper}USD.7",            # BTCUSD.7
            ],
            'mexc': [
                f"{base_upper}-PERP.V",          # BTC-PERP.V
            ],
            'kucoin': [
                f"{base_upper}USDT_PERP.0",      # BTCUSDT_PERP.0
                f"{base_upper}USD_PERP.0",
            ],
            'phemex': [
                f"PERP_{base_upper}_USDT.W",     # PERP_BTC_USDT.W
            ],
            'coinex': [
                f"{base_upper}USDT_PERP.F",      # BTCUSDT_PERP.F
            ],
        }
        
        # Get patterns for this exchange
        patterns = exact_patterns.get(exchange_lower, [])
        
        # Try each pattern in priority order
        for pattern in patterns:
            if pattern in exchange_symbols:
                return pattern
        
        # If no exact match, return None (don't guess)
        return None
        
    @staticmethod
    def perp_symbol(base: str, exchange: str = "binance") -> str:
        """
        Convert base asset to perpetual symbol format for a given exchange.
        
        Args:
            base: Base asset symbol (e.g., 'BTC')
            exchange: Exchange name (e.g., 'binance', 'bybit', 'okx', 'aggregated')
            
        Returns:
            Coinalyze symbol format specific to each exchange:
            - 'BTCUSDT_PERP.A' for Binance
            - 'BTCUSDT.2' for Bybit
            - 'BTC-USDT-SWAP.6' for OKX
            - 'BTCUSDT_PERP' for aggregated
        """
        exchange_lower = exchange.lower()
        if exchange_lower not in EXCHANGE_FORMATS:
            raise ValueError(f"Unknown exchange: {exchange}. Available: {list(EXCHANGE_FORMATS.keys())}")
        
        _, pattern = EXCHANGE_FORMATS[exchange_lower]
        return pattern.format(BASE=base)
    
    @staticmethod
    def binance_perp_symbol(base: str) -> str:
        """Legacy method - use perp_symbol() instead."""
        return CoinalyzeClient.perp_symbol(base, "binance")
        
    # ==========================================================================
    # Historical Data Methods
    # ==========================================================================
    
    def open_interest_daily(
        self, 
        symbol: str, 
        start_sec: int, 
        end_sec: int, 
        convert_to_usd: bool = True
    ) -> pd.DataFrame:
        """
        Fetch daily Open Interest history.
        
        API Response Fields:
            - t: timestamp (UNIX seconds)
            - o: OI at open
            - h: OI high
            - l: OI low  
            - c: OI at close
            
        Args:
            symbol: Coinalyze futures symbol
            start_sec: Start timestamp (UNIX seconds)
            end_sec: End timestamp (UNIX seconds)
            convert_to_usd: Convert OI to USD value (default True)
            
        Returns:
            DataFrame with columns: date, oi_usd_open, oi_usd_high, oi_usd_low, oi_usd_close
        """
        params = {
            "symbols": symbol,
            "interval": "daily",
            "from": start_sec,
            "to": end_sec,
            "convert_to_usd": "true" if convert_to_usd else "false",
        }
        data = self._get(COINALYZE_ENDPOINTS["open_interest_history"], params)
        
        if not data or not isinstance(data, list) or not data[0].get("history"):
            return pd.DataFrame()
            
        df = pd.DataFrame(data[0]["history"])
        df = df.rename(columns={
            "t": "timestamp",
            "o": "oi_usd_open",
            "h": "oi_usd_high",
            "l": "oi_usd_low",
            "c": "oi_usd_close",
        })
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        
        for col in ["oi_usd_open", "oi_usd_high", "oi_usd_low", "oi_usd_close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                
        return df[["date", "oi_usd_open", "oi_usd_high", "oi_usd_low", "oi_usd_close"]]\
            .drop_duplicates("date").sort_values("date")
            
    def funding_rate_daily(self, symbol: str, start_sec: int, end_sec: int) -> pd.DataFrame:
        """
        Fetch daily Funding Rate history.
        
        API Response Fields:
            - t: timestamp (UNIX seconds)
            - o: Funding rate at open
            - h: Funding rate high
            - l: Funding rate low
            - c: Funding rate at close
            
        Returns:
            DataFrame with columns: date, funding_open, funding_high, funding_low, funding_close
        """
        params = {
            "symbols": symbol,
            "interval": "daily",
            "from": start_sec,
            "to": end_sec,
        }
        data = self._get(COINALYZE_ENDPOINTS["funding_rate_history"], params)
        
        if not data or not isinstance(data, list) or not data[0].get("history"):
            return pd.DataFrame()
            
        df = pd.DataFrame(data[0]["history"])
        df = df.rename(columns={
            "t": "timestamp",
            "o": "funding_open",
            "h": "funding_high",
            "l": "funding_low",
            "c": "funding_close",
        })
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        
        for col in ["funding_open", "funding_high", "funding_low", "funding_close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                
        return df[["date", "funding_open", "funding_high", "funding_low", "funding_close"]]\
            .drop_duplicates("date").sort_values("date")
            
    def predicted_funding_rate_daily(
        self, 
        symbol: str, 
        start_sec: int, 
        end_sec: int
    ) -> pd.DataFrame:
        """
        Fetch daily Predicted Funding Rate history.
        
        The predicted funding rate is the estimated next funding rate based on
        current market conditions before the funding interval settles.
        
        Returns:
            DataFrame with columns: date, pred_funding_open, pred_funding_high, 
                                   pred_funding_low, pred_funding_close
        """
        params = {
            "symbols": symbol,
            "interval": "daily",
            "from": start_sec,
            "to": end_sec,
        }
        data = self._get(COINALYZE_ENDPOINTS["predicted_funding_rate_history"], params)
        
        if not data or not isinstance(data, list) or not data[0].get("history"):
            return pd.DataFrame()
            
        df = pd.DataFrame(data[0]["history"])
        df = df.rename(columns={
            "t": "timestamp",
            "o": "pred_funding_open",
            "h": "pred_funding_high",
            "l": "pred_funding_low",
            "c": "pred_funding_close",
        })
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        
        for col in ["pred_funding_open", "pred_funding_high", "pred_funding_low", "pred_funding_close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                
        return df[["date", "pred_funding_open", "pred_funding_high", "pred_funding_low", "pred_funding_close"]]\
            .drop_duplicates("date").sort_values("date")
            
    def long_short_ratio_daily(
        self, 
        symbol: str, 
        start_sec: int, 
        end_sec: int
    ) -> pd.DataFrame:
        """
        Fetch daily Long/Short Ratio history.
        
        API Response Fields:
            - t: timestamp (UNIX seconds)
            - r: Long/Short ratio (longs / shorts)
            - l: Quantity of long positions
            - s: Quantity of short positions
            
        Returns:
            DataFrame with columns: date, ls_ratio, longs_qty, shorts_qty
        """
        params = {
            "symbols": symbol,
            "interval": "daily",
            "from": start_sec,
            "to": end_sec,
        }
        data = self._get(COINALYZE_ENDPOINTS["long_short_ratio_history"], params)
        
        if not data or not isinstance(data, list) or not data[0].get("history"):
            return pd.DataFrame()
            
        df = pd.DataFrame(data[0]["history"])
        df = df.rename(columns={
            "t": "timestamp",
            "r": "ls_ratio",
            "l": "longs_qty",
            "s": "shorts_qty",
        })
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        
        for col in ["ls_ratio", "longs_qty", "shorts_qty"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                
        return df[["date", "ls_ratio", "longs_qty", "shorts_qty"]]\
            .drop_duplicates("date").sort_values("date")
            
    def liquidation_daily(self, symbol: str, start_sec: int, end_sec: int) -> pd.DataFrame:
        """
        Fetch daily Liquidation history.
        
        API Response Fields:
            - t: timestamp (UNIX seconds)
            - l: Volume of long liquidations (USD)
            - s: Volume of short liquidations (USD)
            
        Returns:
            DataFrame with columns: date, liq_longs, liq_shorts, liq_total
        """
        params = {
            "symbols": symbol,
            "interval": "daily",
            "from": start_sec,
            "to": end_sec,
        }
        data = self._get(COINALYZE_ENDPOINTS["liquidation_history"], params)
        
        if not data or not isinstance(data, list) or not data[0].get("history"):
            return pd.DataFrame()
            
        df = pd.DataFrame(data[0]["history"])
        df = df.rename(columns={
            "t": "timestamp",
            "l": "liq_longs",
            "s": "liq_shorts",
        })
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        
        for col in ["liq_longs", "liq_shorts"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
                
        # Calculate total liquidations
        df["liq_total"] = df["liq_longs"].fillna(0) + df["liq_shorts"].fillna(0)
        
        return df[["date", "liq_longs", "liq_shorts", "liq_total"]]\
            .drop_duplicates("date").sort_values("date")
            
    def ohlcv_daily(self, symbol: str, start_sec: int, end_sec: int) -> pd.DataFrame:
        """
        Fetch daily OHLCV history.
        
        API Response Fields:
            - t: timestamp (UNIX seconds)
            - o: Opening price
            - h: Highest price
            - l: Lowest price
            - c: Closing price
            - v: Total volume (Base Asset)
            - bv: Buy volume (Base Asset)
            - tx: Total transactions
            - btx: Buy transactions
            
        Returns:
            DataFrame with columns: date, price_open, price_high, price_low, price_close,
                                   volume_base, buy_volume_base, sell_volume_base, 
                                   volume_delta, txn_count, buy_txn_count
        """
        params = {
            "symbols": symbol,
            "interval": "daily",
            "from": start_sec,
            "to": end_sec,
        }
        data = self._get(COINALYZE_ENDPOINTS["ohlcv_history"], params)
        
        if not data or not isinstance(data, list) or not data[0].get("history"):
            return pd.DataFrame()
            
        df = pd.DataFrame(data[0]["history"])
        df = df.rename(columns={
            "t": "timestamp",
            "o": "price_open",
            "h": "price_high",
            "l": "price_low",
            "c": "price_close",
            "v": "volume_base",
            "bv": "buy_volume_base",
            "tx": "txn_count",
            "btx": "buy_txn_count",
        })
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        
        for col in ["price_open", "price_high", "price_low", "price_close",
                       "volume_base", "buy_volume_base", "txn_count", "buy_txn_count"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Calculate sell volume and delta
        if "volume_base" in df.columns and "buy_volume_base" in df.columns:
            df["sell_volume_base"] = df["volume_base"] - df["buy_volume_base"]
            df["volume_delta"] = df["buy_volume_base"] - df["sell_volume_base"]
            
        # Calculate sell transactions
        if "txn_count" in df.columns and "buy_txn_count" in df.columns:
            df["sell_txn_count"] = df["txn_count"] - df["buy_txn_count"]
            
        # Calculate USD volumes (estimate using price_close)
        if "volume_base" in df.columns and "price_close" in df.columns:
            df["volume_usd"] = df["volume_base"] * df["price_close"]
            
        final_cols = ["date", "price_open", "price_high", "price_low", "price_close",
                     "volume_base", "buy_volume_base", "sell_volume_base", "volume_delta",
                     "volume_usd", "txn_count", "buy_txn_count", "sell_txn_count"]
        
        output_cols = [c for c in final_cols if c in df.columns]
        return df[output_cols].drop_duplicates("date").sort_values("date")


# ==============================================================================
# Native Exchange Fetchers
# ==============================================================================

class BinanceFuturesFetcher:
    """Fetcher for Binance Futures API."""
    def __init__(self):
        self.base_url = BINANCE_FUTURES_API

    def fetch_current_day_data(self, symbol: str) -> Optional[Dict]:
        """Fetch today's open candle data."""
        sym = symbol.split('.')[0].replace('_PERP', '')
        try:
            params = {"symbol": sym, "interval": "1d", "limit": 1}
            resp = requests.get(f"{self.base_url}/fapi/v1/klines", params=params)
            data = resp.json()
            if not data: return None
            k = data[0]
            return {
                "price_open": float(k[1]), "price_high": float(k[2]), "price_low": float(k[3]), "price_close": float(k[4]),
                "volume_base": float(k[5]), "volume_usd": float(k[7]), "txn_count": int(k[8]),
                "buy_volume_base": float(k[9])
            }
        except: return None

    def fetch_ls_ratios(self, symbol: str) -> Dict:
        """Fetch latest L/S ratios (Global, Top Account, Top Position)."""
        sym = symbol.split('.')[0].replace('_PERP', '')
        res = {}
        try:
            # Global
            r1 = requests.get(f"{self.base_url}/futures/data/globalLongShortAccountRatio", params={"symbol": sym, "period": "1d", "limit": 1}).json()
            if r1: res['ls_acc_global'] = float(r1[0]['longShortRatio'])
            # Top Account
            r2 = requests.get(f"{self.base_url}/futures/data/topLongShortAccountRatio", params={"symbol": sym, "period": "1d", "limit": 1}).json()
            if r2: res['ls_acc_top'] = float(r2[0]['longShortRatio'])
            # Top Position
            r3 = requests.get(f"{self.base_url}/futures/data/topLongShortPositionRatio", params={"symbol": sym, "period": "1d", "limit": 1}).json()
            if r3: res['ls_pos_top'] = float(r3[0]['longShortRatio'])
        except: pass
        return res

class BybitFuturesFetcher:
    """Fetcher for Bybit V5 API."""
    def __init__(self):
        self.base_url = BYBIT_V5_API

    def fetch_current_day_data(self, symbol: str) -> Optional[Dict]:
        sym = symbol.split('.')[0]
        try:
            params = {"category": "linear", "symbol": sym, "interval": "D", "limit": 1}
            resp = requests.get(f"{self.base_url}/market/kline", params=params).json()
            data = resp.get("result", {}).get("list", [])
            if not data: return None
            k = data[0]
            return {
                "price_open": float(k[1]), "price_high": float(k[2]), "price_low": float(k[3]), "price_close": float(k[4]),
                "volume_base": float(k[5]), "volume_usd": float(k[6])
            }
        except: return None

    def fetch_ls_ratios(self, symbol: str) -> Dict:
        sym = symbol.split('.')[0]
        res = {}
        try:
            r = requests.get(f"{self.base_url}/market/account-ratio", params={"category": "linear", "symbol": sym, "period": "1d", "limit": 1}).json()
            data = r.get("result", {}).get("list", [])
            if data:
                val = float(data[0].get('buyRatio', 0)) / float(data[0].get('sellRatio', 1))
                res['ls_acc_global'] = val
        except: pass
        return res

class OKXFuturesFetcher:
    """Fetcher for OKX V5 API."""
    def __init__(self):
        self.base_url = OKX_V5_API

    def fetch_current_day_data(self, symbol: str) -> Optional[Dict]:
        sym = symbol.split('.')[0].replace('USDT_PERP', '-USDT-SWAP')
        try:
            params = {"instId": sym, "bar": "1D", "limit": 1}
            resp = requests.get(f"{self.base_url}/market/candles", params=params).json()
            data = resp.get("data", [])
            if not data: return None
            k = data[0]
            return {
                "price_open": float(k[1]), "price_high": float(k[2]), "price_low": float(k[3]), "price_close": float(k[4]),
                "volume_base": float(k[5]), "volume_usd": float(k[7])
            }
        except: return None

    def fetch_ls_ratios(self, symbol: str) -> Dict:
        # OKX uses base asset for Rubik stats
        base = symbol.split('USDT')[0]
        res = {}
        try:
            params = {"ccy": base.upper(), "period": "1D"}
            r1 = requests.get(f"{self.base_url}/rubik/stat/contracts/long-short-account-ratio", params=params).json().get("data", [])
            if r1: res['ls_acc_global'] = float(r1[0][1])
            r2 = requests.get(f"{self.base_url}/rubik/stat/contracts/top-traders-long-short-account-ratio", params=params).json().get("data", [])
            if r2: res['ls_acc_top'] = float(r2[0][1])
            r3 = requests.get(f"{self.base_url}/rubik/stat/contracts/top-traders-long-short-position-ratio", params=params).json().get("data", [])
            if r3: res['ls_pos_top'] = float(r3[0][1])
        except: pass
        return res

# ==============================================================================
# Data Merge Utilities
# ==============================================================================
def merge_dataframes(dataframes: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Merge multiple DataFrames on 'date' column.
    
    Args:
        dataframes: List of DataFrames with 'date' column
        
    Returns:
        Merged DataFrame
    """
    # Filter out empty DataFrames
    valid_dfs = [df for df in dataframes if not df.empty and "date" in df.columns]
    
    if not valid_dfs:
        return pd.DataFrame()
        
    if len(valid_dfs) == 1:
        return valid_dfs[0]
        
    # Start with all unique dates
    all_dates = set()
    for df in valid_dfs:
        all_dates.update(df["date"].tolist())
        
    result = pd.DataFrame({"date": sorted(all_dates)})
    
    for df in valid_dfs:
        # Remove 'date' duplicates in source
        df = df.drop_duplicates("date")
        # Merge on date
        cols_to_merge = [c for c in df.columns if c != "date"]
        result = result.merge(df[["date"] + cols_to_merge], on="date", how="left")
        
    return result.sort_values("date")


def merge_on_date(perp_csv_path: str, metrics: pd.DataFrame) -> None:
    """
    Merge metrics DataFrame into existing perpetual CSV file.
    
    Args:
        perp_csv_path: Path to existing CSV file
        metrics: DataFrame with metrics to merge
    """
    if not os.path.exists(perp_csv_path):
        print(f"[SKIP] Perp CSV does not exist: {perp_csv_path}")
        return
        
    df = pd.read_csv(perp_csv_path)
    
    # Derive date column if not present
    if "date" not in df.columns:
        if "open_time" in df.columns:
            ts = pd.to_datetime(df["open_time"], unit="ms", utc=True)
            df["date"] = ts.dt.strftime("%Y-%m-%d")
        elif "timestamp" in df.columns:
            ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            df["date"] = ts.dt.strftime("%Y-%m-%d")
        else:
            print(f"[ERROR] Cannot find date column in {perp_csv_path}")
            return
            
    df["date"] = df["date"].astype(str)
    metrics["date"] = metrics["date"].astype(str)
    
    # Merge and deduplicate
    out = df.merge(metrics, on="date", how="left", suffixes=("", "_coinalyze"))
    out = out.sort_values("date").drop_duplicates("date", keep="last")
    out.to_csv(perp_csv_path, index=False)
    
    new_cols = [c for c in metrics.columns if c != "date"]
    print(f"[OK] Merged into {perp_csv_path} (+{new_cols})")


# ==============================================================================
# Exchange Direct APIs (Hybrid Patching)
# ==============================================================================

class BinanceFuturesFetcher:
    """Fetcher for Binance USD-M Futures Direct API."""
    BASE_URL = "https://fapi.binance.com"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }

    def __init__(self, timeout: int = 30, max_retries: int = 3):
        self.timeout = timeout
        self.max_retries = max_retries

    def _get(self, endpoint: str, params: dict) -> Optional[dict]:
        for attempt in range(self.max_retries):
            try:
                resp = requests.get(
                    f"{self.BASE_URL}{endpoint}",
                    params=params,
                    headers=self.HEADERS,
                    timeout=self.timeout
                )
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 429:
                    wait_time = int(resp.headers.get("Retry-After", 2 ** attempt))
                    print(f"    [Binance] Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                elif resp.status_code in (403, 418, 451):
                    wait_time = 5 ** attempt + 5
                    print(f"    [Binance] IP blocked (HTTP {resp.status_code}), retrying in {wait_time}s... (attempt {attempt+1}/{self.max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"    [Binance] HTTP {resp.status_code} for {endpoint}")
                    return None
            except requests.exceptions.Timeout:
                print(f"    [Binance] Timeout, attempt {attempt+1}/{self.max_retries}")
                time.sleep(2 ** attempt)
            except Exception as e:
                print(f"    [Binance API Error] {e}")
                return None
        return None

    def fetch_ls_ratios(self, symbol: str, period: str = "1d", limit: int = 30) -> pd.DataFrame:
        """Fetch historical Long/Short ratios in bulk (last 30 days max for Binance)."""
        clean_symbol = symbol.split('.')[0].replace('_PERP', '') 
        
        # 1. Global L/S Account Ratio
        g_data = self._get("/futures/data/globalLongShortAccountRatio", {"symbol": clean_symbol, "period": period, "limit": limit})
        # 2. Top Trader L/S Account Ratio
        a_data = self._get("/futures/data/topLongShortAccountRatio", {"symbol": clean_symbol, "period": period, "limit": limit})
        # 3. Top Trader L/S Position Ratio
        p_data = self._get("/futures/data/topLongShortPositionRatio", {"symbol": clean_symbol, "period": period, "limit": limit})

        def to_df(data, col_name):
            if not data: return pd.DataFrame()
            df = pd.DataFrame(data)
            df['date'] = pd.to_datetime(df['timestamp'].astype('int64'), unit='ms', utc=True).dt.strftime('%Y-%m-%d')
            df['longShortRatio'] = pd.to_numeric(df['longShortRatio'], errors='coerce')
            return df[['date', 'longShortRatio']].rename(columns={'longShortRatio': col_name})

        df_g = to_df(g_data, 'ls_acc_global')
        df_a = to_df(a_data, 'ls_acc_top')
        df_p = to_df(p_data, 'ls_pos_top')

        # Merge them
        res = pd.DataFrame()
        for df in [df_g, df_a, df_p]:
            if df.empty: continue
            if res.empty: res = df
            else: res = res.merge(df, on='date', how='outer')
        return res

    def fetch_current_day_data(self, symbol: str) -> Optional[Dict]:
        """Fetch today's open candle data + living metrics (Funding, OI)."""
        clean_symbol = symbol.split('.')[0].replace('_PERP', '')
        # 1. OHLCV
        k_data = self._get("/fapi/v1/klines", {"symbol": clean_symbol, "interval": "1d", "limit": 1})
        # 2. Funding
        f_data = self._get("/fapi/v1/premiumIndex", {"symbol": clean_symbol})
        # 3. Open Interest
        o_data = self._get("/fapi/v1/openInterest", {"symbol": clean_symbol})

        if not k_data: return None
        k = k_data[0]
        res = {
            "price_open": float(k[1]), "price_high": float(k[2]), "price_low": float(k[3]), "price_close": float(k[4]),
            "volume_base": float(k[5]), "volume_usd": float(k[7]), "txn_count": int(k[8]),
            "buy_volume_base": float(k[9])
        }
        if f_data:
            frate = f_data.get("lastFundingRate")
            val = float(frate) if frate and frate != "" else 0.0
            for suffix in ['open', 'high', 'low', 'close']:
                res[f"funding_{suffix}"] = val
            for suffix in ['open', 'high', 'low', 'close']:
                res[f"pred_funding_{suffix}"] = val
                
        if o_data:
            oi_val = o_data.get("openInterest")
            val = float(oi_val) if oi_val and oi_val != "" else 0.0
            for suffix in ['open', 'high', 'low', 'close']:
                res[f"oi_usd_{suffix}"] = val
        return res

class BybitFuturesFetcher:
    """Fetcher for Bybit V5 Futures Direct API."""
    BASE_URL = "https://api.bybit.com"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }

    def __init__(self, timeout: int = 15, max_retries: int = 3):
        self.timeout = timeout
        self.max_retries = max_retries

    def _get(self, endpoint: str, params: dict) -> Optional[dict]:
        for attempt in range(self.max_retries):
            try:
                resp = requests.get(
                    f"{self.BASE_URL}{endpoint}",
                    params=params,
                    headers=self.HEADERS,
                    timeout=self.timeout
                )
                if resp.status_code == 200:
                    return resp.json()
                elif resp.status_code == 429:
                    wait_time = int(resp.headers.get("Retry-After", 2 ** attempt))
                    print(f"    [Bybit] Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                elif resp.status_code in (403, 418):
                    print(f"    [Bybit] IP blocked (HTTP {resp.status_code}), attempt {attempt+1}/{self.max_retries}")
                    time.sleep(2 ** attempt)
                    continue
                else:
                    print(f"    [Bybit] HTTP {resp.status_code} for {endpoint}")
                    return None
            except requests.exceptions.Timeout:
                print(f"    [Bybit] Timeout, attempt {attempt+1}/{self.max_retries}")
                time.sleep(2 ** attempt)
            except Exception as e:
                print(f"    [Bybit API Error] {e}")
                return None
        return None

    def fetch_ls_ratios(self, symbol: str, limit: int = 500) -> pd.DataFrame:
        """Fetch historical Long/Short ratio in bulk."""
        clean_symbol = symbol.split('.')[0]
        data = self._get("/v5/market/account-ratio", {"category": "linear", "symbol": clean_symbol, "period": "1d", "limit": limit})
        if not data or data.get("retCode") != 0: return pd.DataFrame()
        
        list_data = data.get("result", {}).get("list", [])
        if not list_data: return pd.DataFrame()
        
        df = pd.DataFrame(list_data)
        df['date'] = pd.to_datetime(df['timestamp'].astype('int64'), unit='ms', utc=True).dt.strftime('%Y-%m-%d')
        # Calculate ratio from buy/sell components
        df['ls_acc_global'] = pd.to_numeric(df['buyRatio']) / pd.to_numeric(df['sellRatio'])
        return df[['date', 'ls_acc_global']]

    def fetch_current_day_data(self, symbol: str) -> Optional[Dict]:
        """Fetch today's data + living metrics (Funding, OI)."""
        clean_symbol = symbol.split('.')[0]
        # 1. OHLCV
        k_data = self._get("/v5/market/kline", {"category": "linear", "symbol": clean_symbol, "interval": "D", "limit": 1})
        # 2. Tickers (has Funding/OI)
        t_data = self._get("/v5/market/tickers", {"category": "linear", "symbol": clean_symbol})

        if not k_data or k_data.get("retCode") != 0: return None
        list_k = k_data.get("result", {}).get("list", [])
        if not list_k: return None
        k = list_k[0]
        
        res = {
            "price_open": float(k[1]), "price_high": float(k[2]), "price_low": float(k[3]), "price_close": float(k[4]),
            "volume_base": float(k[5]), "volume_usd": float(k[6])
        }
        
        if t_data and t_data.get("retCode") == 0:
            tick = t_data.get("result", {}).get("list", [])[0]
            fr = tick.get("fundingRate")
            nfr = tick.get("nextFundingRate")
            oi = tick.get("openInterest")
            
            f_val = float(fr) if fr and fr != "" else 0.0
            nf_val = float(nfr) if nfr and nfr != "" else 0.0
            oi_val = float(oi) if oi and oi != "" else 0.0
            
            for suffix in ['open', 'high', 'low', 'close']:
                res[f"funding_{suffix}"] = f_val
                res[f"pred_funding_{suffix}"] = nf_val
                res[f"oi_usd_{suffix}"] = oi_val
            
        return res

class OKXFuturesFetcher:
    """Fetcher for OKX V5 Futures Direct API."""
    BASE_URL = "https://www.okx.com"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }

    def __init__(self, timeout: int = 15, max_retries: int = 3):
        self.timeout = timeout
        self.max_retries = max_retries

    def _get(self, endpoint: str, params: dict) -> Optional[dict]:
        for attempt in range(self.max_retries):
            try:
                resp = requests.get(
                    f"{self.BASE_URL}{endpoint}",
                    params=params,
                    headers=self.HEADERS,
                    timeout=self.timeout
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("code") == "0": return data
                    print(f"    [OKX] API error code: {data.get('code')}, msg: {data.get('msg')}")
                    return None
                elif resp.status_code == 429:
                    wait_time = int(resp.headers.get("Retry-After", 2 ** attempt))
                    print(f"    [OKX] Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                elif resp.status_code in (403, 418, 451):
                    print(f"    [OKX] IP blocked (HTTP {resp.status_code}), attempt {attempt+1}/{self.max_retries}")
                    time.sleep(2 ** attempt)
                    continue
                else:
                    print(f"    [OKX] HTTP {resp.status_code} for {endpoint}")
                    return None
            except requests.exceptions.Timeout:
                print(f"    [OKX] Timeout, attempt {attempt+1}/{self.max_retries}")
                time.sleep(2 ** attempt)
            except Exception as e:
                print(f"    [OKX API Error] {e}")
                return None
        return None

    def fetch_ls_ratios(self, symbol: str, limit: int = 180) -> pd.DataFrame:
        """Fetch historical Long/Short ratios in bulk."""
        base = symbol.split('USDT')[0].split('.')[0]
        clean_symbol = f"{base}-USDT-SWAP"
        
        # 1. Global
        g_data = self._get("/api/v5/rubik/stat/contracts/long-short-account-ratio", {"ccy": base.upper(), "period": "1D"})
        # 2. Top Account
        a_data = self._get("/api/v5/rubik/stat/contracts/top-traders-long-short-account-ratio", {"ccy": base.upper(), "period": "1D"})
        # 3. Top Position
        p_data = self._get("/api/v5/rubik/stat/contracts/top-traders-long-short-position-ratio", {"ccy": base.upper(), "period": "1D"})

        def to_df(data, col_name):
            if not data or not data.get("data"): return pd.DataFrame()
            df = pd.DataFrame(data["data"], columns=['ts', 'ratio'])
            df['date'] = pd.to_datetime(df['ts'].astype('int64'), unit='ms', utc=True).dt.strftime('%Y-%m-%d')
            df['ratio'] = pd.to_numeric(df['ratio'], errors='coerce')
            return df[['date', 'ratio']].rename(columns={'ratio': col_name})

        df_g = to_df(g_data, 'ls_acc_global')
        df_a = to_df(a_data, 'ls_acc_top')
        df_p = to_df(p_data, 'ls_pos_top')

        res = pd.DataFrame()
        for df in [df_g, df_a, df_p]:
            if df.empty: continue
            if res.empty: res = df
            else: res = res.merge(df, on='date', how='outer')
        return res

    def fetch_current_day_data(self, symbol: str) -> Optional[Dict]:
        """Fetch today's data + living metrics (Funding, OI)."""
        base_asset = symbol.split('USDT')[0].split('.')[0]
        clean_symbol = f"{base_asset}-USDT-SWAP"
        
        # 1. OHLCV
        k_data = self._get("/api/v5/market/candles", {"instId": clean_symbol, "bar": "1D", "limit": 1})
        # 2. Funding
        f_data = self._get("/api/v5/public/funding-rate", {"instId": clean_symbol})
        # 3. Open Interest
        o_data = self._get("/api/v5/public/open-interest", {"instId": clean_symbol})

        if not k_data: return None
        list_k = k_data.get("data", [])
        if not list_k: return None
        k = list_k[0]
        
        res = {
            "price_open": float(k[1]), "price_high": float(k[2]), "price_low": float(k[3]), "price_close": float(k[4]),
            "volume_base": float(k[5]), "volume_usd": float(k[7])
        }
        
        if f_data and f_data.get("code") == "0":
            fdata = f_data["data"][0]
            fr = fdata.get("fundingRate")
            nfr = fdata.get("nextFundingRate")
            f_val = float(fr) if fr and fr != "" else 0.0
            nf_val = float(nfr) if nfr and nfr != "" else 0.0
            for suffix in ['open', 'high', 'low', 'close']:
                res[f"funding_{suffix}"] = f_val
                res[f"pred_funding_{suffix}"] = nf_val
            
        if o_data and o_data.get("code") == "0":
            odata = o_data["data"][0]
            oi = odata.get("oi")
            oi_val = float(oi) if oi and oi != "" else 0.0
            for suffix in ['open', 'high', 'low', 'close']:
                res[f"oi_usd_{suffix}"] = oi_val
            
        return res

# ==============================================================================
# Hybrid Sourcing Manager
# ==============================================================================

# Global instances for fetchers to reuse connections
GLOBAL_FETCHERS = {
    "binance": BinanceFuturesFetcher(),
    "bybit": BybitFuturesFetcher(),
    "okx": OKXFuturesFetcher()
}

def patch_missing_metrics(df: pd.DataFrame, base: str, exchange: str, symbol: str) -> pd.DataFrame:
    """Bulk update missing columns using direct exchange APIs."""
    if exchange.lower() not in GLOBAL_FETCHERS: return df
    fetcher = GLOBAL_FETCHERS[exchange.lower()]
    
    # 1. Initialize metric columns if they don't exist
    target_cols = ['ls_acc_global', 'ls_acc_top', 'ls_pos_top', 
                   'buy_volume_base', 'sell_volume_base', 'volume_delta', 
                   'txn_count', 'buy_txn_count', 'sell_txn_count']
    for col in target_cols:
        if col not in df.columns:
            df[col] = None

    # 2. Ensure Today's Data
    today_str = datetime.now(UTC).strftime('%Y-%m-%d')
    if df.empty or not (df['date'] == today_str).any():
        print(f"    [Hybrid] Fetching current day open candle from {exchange.upper()}...")
        today_data = fetcher.fetch_current_day_data(symbol)
        if today_data:
            today_row = pd.DataFrame([today_data])
            today_row['date'] = today_str
            today_row['symbol'] = symbol
            today_row['exchange'] = exchange
            today_row['base_asset'] = base
            df = pd.concat([df, today_row], ignore_index=True)
            
    if df.empty: return df
    
    # 3. Bulk Patch Missing Metrics (L/S Ratios)
    ls_cols = ['ls_acc_global', 'ls_acc_top', 'ls_pos_top']
    missing_any = any(col in df.columns and pd.isna(df[col]).any() for col in ls_cols) or \
                  any(col not in df.columns for col in ls_cols)
    
    if missing_any:
        print(f"    [Hybrid] Fetching historical L/S metrics in bulk from {exchange.upper()}...")
        limit = 30 if exchange.lower() == "binance" else 180 if exchange.lower() == "okx" else 500
        df_history = fetcher.fetch_ls_ratios(symbol, limit=limit)
        
        if not df_history.empty:
            print(f"    [Hybrid] Found {len(df_history)} L/S history records.")
            df = df.merge(df_history, on='date', how='left', suffixes=('', '_new'))
            patched_count = 0
            for col in ls_cols:
                new_col = f"{col}_new"
                if new_col in df.columns:
                    patched_rows = df[new_col].notna().sum()
                    df[col] = df[col].fillna(df[new_col])
                    df.drop(columns=[new_col], inplace=True)
                    patched_count = max(patched_count, patched_rows)
            print(f"    [Hybrid] Successfully patched L/S metrics for ~{patched_count} days.")
        else:
            print(f"    [Hybrid WARNING] No L/S history returned from native API.")
    
    # 4. Final Consistency Fix (Calculate Sell/Delta if components exist)
    if 'volume_base' in df.columns and 'buy_volume_base' in df.columns:
        # Fill sell_volume if missing
        df['sell_volume_base'] = df['sell_volume_base'].fillna(df['volume_base'] - df['buy_volume_base'])
        # Always recalculate volume_delta if we have both buy/sell
        df['volume_delta'] = df['buy_volume_base'] - df['sell_volume_base']
        
    if 'txn_count' in df.columns and 'buy_txn_count' in df.columns:
        # Calculate sell_txn_count
        df['sell_txn_count'] = df['txn_count'] - df['buy_txn_count']

    # 5. Final Cleanup
    if not df.empty:
        df.drop_duplicates(subset=['date'], keep='last', inplace=True)
        
    return df

def fetch_hybrid_futures_data(client: CoinalyzeClient,
                             token: str, exchange: str, symbol: str,
                             start_sec: int, end_sec: int) -> pd.DataFrame:
    """Manager to fetch data from native APIs or Coinalyze based on Smart Sourcing."""
    print(f"    [Source] Coinalyze + Native Patches")
    
    dfs = []
    # 1. Fetch bulk history from Coinalyze
    df_liq = client.liquidation_daily(symbol, start_sec, end_sec)
    if not df_liq.empty: dfs.append(df_liq)
    
    df_pred = client.predicted_funding_rate_daily(symbol, start_sec, end_sec)
    if not df_pred.empty: dfs.append(df_pred)

    df_oi = client.open_interest_daily(symbol, start_sec, end_sec)
    if not df_oi.empty: dfs.append(df_oi)
    
    df_f = client.funding_rate_daily(symbol, start_sec, end_sec)
    if not df_f.empty: dfs.append(df_f)
    
    df_ls = client.long_short_ratio_daily(symbol, start_sec, end_sec)
    if not df_ls.empty: dfs.append(df_ls)
    
    df_ohlcv = client.ohlcv_daily(symbol, start_sec, end_sec)
    if not df_ohlcv.empty: dfs.append(df_ohlcv)

    if not dfs: return pd.DataFrame()
    
    # 2. Merge all sources
    df_final = merge_dataframes(dfs)
    if not df_final.empty:
        df_final['symbol'], df_final['exchange'], df_final['base_asset'] = symbol, exchange, token
        # 3. Patch missing metrics (L/S ratios, today's candle)
        df_final = patch_missing_metrics(df_final, token, exchange, symbol)
        
    return df_final


def main():
    """Main entry point for the Coinalyze data backfill script."""
    parser = argparse.ArgumentParser(
        description="Backfill historical crypto futures data from Coinalyze API"
    )
    parser.add_argument("--top", "--limit", type=int, default=50, dest="top",
                       help="Number of top tokens to fetch (default: 50)")
    parser.add_argument("--top-range", type=str, default=None,
                       help="Range of top tokens to fetch (e.g., 10-50). Overrides --top.")
    parser.add_argument("--symbols", type=str, default=None,
                       help="Comma-separated list of symbols (e.g., BTC,ETH). Overrides --top and --top-range.")
    parser.add_argument("--output-dir", type=str, default="data",
                       help="Output directory for data files (default: data)")
    parser.add_argument("--perp-dir", type=str, default="data/binance/perp",
                       help="Directory with existing perp CSV files to merge")
    parser.add_argument("--start", type=str, default="2017-01-01",
                       help="Start date YYYY-MM-DD UTC (default: 2017-01-01)")
    parser.add_argument("--end-days-ago", type=int, default=1,
                       help="End date as N days ago (default: 1 = yesterday)")
    parser.add_argument("--coinalyze-key", type=str,
                       default=os.environ.get("COINALYZE_API_KEY", ""),
                       help="Coinalyze API key (or set COINALYZE_API_KEY env var)")
    parser.add_argument("--skip-ohlcv", action="store_true",
                       help="Skip OHLCV data (useful if you have price data from elsewhere)")
    parser.add_argument("--skip-merge", action="store_true", help="Skip merging metrics into OHLCV files")
    parser.add_argument("--csv", action="store_true", help="Save results to local CSV files (default: False)")
    parser.add_argument("--exchanges", type=str, default="binance,bybit,okx",
                       help=f"Comma-separated list of exchanges, or 'all' for all (default: binance,bybit,okx). Available: {','.join(EXCHANGE_CODES.keys())}")
    parser.add_argument("--metadata-only", action="store_true", help="Only sync metadata and exit")
    args = parser.parse_args()
    
    # Initialize API Clients
    client = CoinalyzeClient(args.coinalyze_key, rate_delay=1.6)
    db_manager = DatabaseManager()
    
    # Validate API key
    if not args.coinalyze_key:
        raise SystemExit(
            "[ERROR] Missing COINALYZE_API_KEY environment variable or --coinalyze-key argument.\n"
            "Get your free API key at: https://coinalyze.net/api/"
        )
    
    # Parse exchanges list
    if args.exchanges.lower() == "all":
        # Use all available exchanges
        exchanges = list(EXCHANGE_CODES.keys())
    else:
        exchanges = [e.strip().lower() for e in args.exchanges.split(",")]
        invalid_exchanges = [e for e in exchanges if e not in EXCHANGE_CODES]
        if invalid_exchanges:
            raise SystemExit(
                f"[ERROR] Invalid exchange(s): {invalid_exchanges}\n"
                f"Available exchanges: {list(EXCHANGE_CODES.keys())}"
            )
        
    # Calculate time range
    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=UTC)
    end_date = datetime.now(tz=UTC).date() - timedelta(days=args.end_days_ago)
    end_dt = datetime.combine(end_date, dtime(23, 59, 59), tzinfo=UTC)
    default_start_sec = to_unix_seconds(start_dt)
    end_sec = to_unix_seconds(end_dt)
    
    # Initialize Metadata Manager with DB support
    meta = AssetMetadataManager(db_manager=db_manager, allow_csv=args.csv)
    
    target_bases = []
    
    if args.symbols:
        raw_symbols = [s.strip().upper() for s in args.symbols.split(",")]
        candidates = coingecko_get_top_candidates(specific_symbols=raw_symbols)
        for c in candidates:
            res = meta.get_metadata(c['symbol'], c['id'], c.get('market_cap'), c.get('market_cap_rank'))
            if res.get("is_filtered") == 0:
                target_bases.append(c['symbol'])
        selection_desc = f"Specific Symbols: {len(target_bases)}"
    else:
        limit = 50
        if args.top_range:
            try:
                start_rank, end_rank = map(int, args.top_range.split("-"))
                limit = end_rank
            except ValueError:
                limit = args.top
        else:
            limit = args.top

        candidates = coingecko_get_top_candidates(n=limit)
        valid_candidates = []
        for c in candidates:
            res = meta.get_metadata(c['symbol'], c['id'], c.get('market_cap'), c.get('market_cap_rank'))
            if res['is_filtered'] == 0:
                valid_candidates.append(c['symbol'])
            if len(valid_candidates) >= limit: break
            
        if args.top_range:
            try:
                start_rank, end_rank = map(int, args.top_range.split("-"))
                target_bases = valid_candidates[start_rank-1:end_rank]
                selection_desc = f"Top Range: {start_rank}-{start_rank + len(target_bases) - 1}"
            except Exception as e:
                target_bases = valid_candidates[:args.top]
                selection_desc = f"Top Tokens: {len(target_bases)}"
            target_bases = valid_candidates[:args.top]
            selection_desc = f"Top Tokens: {len(target_bases)}"

    if args.metadata_only:
        # 1. Update existing metadata first
        print(f"[DB] Syncing {len(meta.df)} cached assets metadata...")
        for _, row in meta.df.iterrows():
            db_manager.upsert_asset_metadata(row['symbol'], row['narrative'], int(row['is_filtered']), row.get('market_cap'), row.get('market_cap_rank'))
        print("[INFO] Metadata sync complete. Exiting (--metadata-only).")
        return

    bases = target_bases

    print("=" * 60)
    print(f"Coinalyze Historical Data Backfill (Cache: {len(meta.df)} assets)")
    print("=" * 60)
    print(f"Date Range: {start_dt.date()} to {end_date}")
    print(f"Selection:  {selection_desc}")
    print(f"Exchanges:  {exchanges}")
    print(f"Output Dir: {args.output_dir}")
    print("=" * 60)
    
    print(f"\nTarget tokens ({len(bases)}): {bases[:10]} ...")
    
    # Load supported Coinalyze symbols
    supported = client.load_future_symbols()
    
    # Process each exchange
    for exchange in exchanges:
        print(f"\n{'='*60}")
        print(f"EXCHANGE: {exchange.upper()}")
        print(f"{'='*60}")
        
        # Create output directories per exchange
        metrics_root = os.path.join(args.output_dir, "coinalyze", exchange)
        ensure_dir(metrics_root)
        
        # Process each token for this exchange
        success_count = 0
        skip_count = 0
        
        for i, base in enumerate(bases, 1):
            # Find the actual symbol for this base asset on this exchange
            symbol = client.find_symbol_for_base(base, exchange)
            
            if not symbol:
                print(f"\n[{i}/{len(bases)}] {base}: SKIP (not found on {exchange})")
                skip_count += 1
                continue
                
            print(f"\n[{i}/{len(bases)}] {base} ({symbol})")
            print("-" * 40)
            
            # Save metrics file path
            out_path = os.path.join(metrics_root, f"{symbol}_1d_metrics.csv")
            
            # Determine start time based on existing data (File or DB)
            start_sec = get_incremental_start(out_path, default_start_sec, symbol, exchange, db_manager)
            
            # Fetch Hybrid Data (Smart Sourcing)
            metrics = fetch_hybrid_futures_data(
                client, 
                base, exchange, symbol, 
                start_sec, end_sec
            )
            
            if metrics.empty:
                print(f"  [SKIP] No data available for {symbol}")
                skip_count += 1
                continue
            
            print(f"    -> {len(metrics)} rows collected")
            
            # Save to Database (Supabase)
            if db_manager.enabled:
                db_manager.upsert_futures_metrics(metrics)
            
            # Save metrics file (Optional CSV)
            if args.csv:
                if os.path.exists(out_path):
                    df_old = pd.read_csv(out_path)
                    metrics_csv = pd.concat([df_old, metrics], ignore_index=True)
                    metrics_csv.drop_duplicates(subset=['date'], keep='last', inplace=True)
                else:
                    metrics_csv = metrics
                
                metrics_csv.sort_values("date").to_csv(out_path, index=False)
                print(f"  [CSV] Saved {out_path} ({len(metrics_csv)} total rows)")
            else:
                print(f"  [CSV] Skipping local save (use --csv to enable)")
            
            if not args.skip_merge:
                perp_csv = os.path.join(args.perp_dir, exchange, f"{base}USDT_1d.csv")
                if os.path.exists(perp_csv):
                    merge_on_date(perp_csv, metrics.drop(columns=["symbol", "exchange"]))
            
            # Additional safety delay for Binance
            if exchange.lower() == 'binance':
                time.sleep(1.0)
                    
            success_count += 1
            
        # Summary for this exchange
        print(f"\n[{exchange.upper()}] Processed: {success_count}, Skipped: {skip_count}")
    
    # Final Summary
    print("\n" + "=" * 60)
    print("COMPLETE")
    print("=" * 60)
    print(f"Exchanges: {exchanges}")
    print(f"Output: {args.output_dir}/coinalyze/")
    print("=" * 60)


if __name__ == "__main__":
    main()
