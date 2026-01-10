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
from datetime import datetime, timedelta, timezone, time as dtime
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


def get_incremental_start(path: str, default_start_sec: int) -> int:
    """Read existing CSV to find the last date and return start_sec - 2 days."""
    if not os.path.exists(path):
        return default_start_sec
    try:
        df = pd.read_csv(path)
        if df.empty or 'date' not in df.columns:
            return default_start_sec
        last_date_str = df['date'].max()
        last_date = datetime.strptime(last_date_str, "%Y-%m-%d").replace(tzinfo=UTC)
        # Fetch from 2 days before the last date to catch incomplete daily closes
        start_dt = last_date - timedelta(days=2)
        return max(default_start_sec, to_unix_seconds(start_dt))
    except Exception as e:
        print(f"    [INFO] Could not read existing file for incremental start: {e}")
        return default_start_sec


class AssetMetadataManager:
    def __init__(self, file_path: str = "data/asset_metadata.csv"):
        self.file_path = file_path
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        if os.path.exists(file_path):
            try:
                self.df = pd.read_csv(file_path)
                if 'is_filtered' in self.df.columns:
                    self.df['is_filtered'] = self.df['is_filtered'].astype(int)
            except:
                self.df = pd.DataFrame(columns=['symbol', 'narrative', 'is_filtered'])
        else:
            self.df = pd.DataFrame(columns=['symbol', 'narrative', 'is_filtered'])
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

    def get_metadata(self, symbol: str, coin_id: str) -> Dict:
        """Get narrative and filter status, checking cache first."""
        symbol = symbol.upper()
        cache_row = self.df[self.df['symbol'] == symbol]
        if not cache_row.empty:
            row = cache_row.iloc[0]
            return {"narrative": row['narrative'], "is_filtered": int(row['is_filtered'])}
            
        print(f"  [CG] Fetching detail for {symbol} ({coin_id})...")
        url = f"{COINGECKO_BASE}/coins/{coin_id}"
        try:
            resp = requests.get(url, params={"localization": "false", "tickers": "false", "market_data": "false", "community_data": "false", "developer_data": "false", "sparkline": "false"})
            if resp.status_code == 429:
                print("    Rate limit. Waiting 60s..."); time.sleep(60)
                return self.get_metadata(symbol, coin_id)
            resp.raise_for_status()
            detail = resp.json()
            categories = detail.get("categories", [])
            cat_ids = [c.lower().replace(" ", "-") for c in categories]
            
            is_filtered = 0
            narrative = "Unknown"
            excluded_cats_indices = [i for i, cid in enumerate(cat_ids) if any(s_cat in cid for s_cat in STABLES_CATEGORIES)]
            if excluded_cats_indices:
                is_filtered = 1
                narrative = categories[excluded_cats_indices[0]]
            else:
                narrative = self._select_best_narrative(categories)

            new_row = pd.DataFrame([{'symbol': symbol, 'narrative': narrative, 'is_filtered': is_filtered}])
            self.df = pd.concat([self.df, new_row], ignore_index=True).drop_duplicates('symbol')
            self.df.to_csv(self.file_path, index=False)
            return {"narrative": narrative, "is_filtered": is_filtered}
        except Exception as e:
            print(f"    [ERROR] CG Fetch failed for {symbol}: {e}")
            return {"narrative": "Unknown", "is_filtered": 0}

def coingecko_get_top_candidates(n: int = 50, specific_symbols: Optional[List[str]] = None) -> List[Dict]:
    """Fetch top N market candidates."""
    print(f"[INFO] Fetching market data from CoinGecko (specific={bool(specific_symbols)})...")
    out = []
    url = f"{COINGECKO_BASE}/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 250, "page": 1, "sparkline": "false"}
    if specific_symbols:
        params["symbols"] = ",".join(specific_symbols).lower()
        params["per_page"] = 100
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for coin in data:
            out.append({"symbol": coin.get("symbol", "").upper(), "id": coin.get("id")})
    except Exception as e:
        print(f"[ERROR] CG Markets API failed: {e}")
    return out


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
        
        numeric_cols = ["price_open", "price_high", "price_low", "price_close",
                       "volume_base", "buy_volume_base", "txn_count", "buy_txn_count"]
        for col in numeric_cols:
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

class NativeFuturesFetcher:
    """Fetcher for native exchange APIs (Binance, Bybit, OKX)."""

    @staticmethod
    def fetch_binance_core(symbol: str, start_sec: int, end_sec: int) -> pd.DataFrame:
        """Fetch Price, Volume, OI, and Funding from Binance."""
        base_url = BINANCE_FUTURES_API
        sym = symbol.split('.')[0].replace('_PERP', '') # BTCUSDT_PERP -> BTCUSDT
        try:
            params = {
                "symbol": sym,
                "interval": "1d",
                "startTime": start_sec * 1000,
                "endTime": end_sec * 1000,
                "limit": 1000
            }
            resp = requests.get(f"{base_url}/fapi/v1/klines", params=params)
            resp.raise_for_status()
            data = resp.json()
            if not data: return pd.DataFrame()
            
            df = pd.DataFrame(data, columns=['t', 'o', 'h', 'l', 'c', 'v', 'ct', 'q', 'n', 'bv', 'bq', 'i'])
            df['date'] = pd.to_datetime(df['t'], unit='ms', utc=True).dt.strftime('%Y-%m-%d')
            df.rename(columns={'o':'price_open', 'h':'price_high', 'l':'price_low', 'c':'price_close', 
                             'v':'volume_base', 'bv':'buy_volume_base', 'q':'volume_usd', 'n':'txn_count'}, inplace=True)
            
            # Numeric conversion
            for col in ['price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'buy_volume_base', 'volume_usd']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return df[['date', 'price_open', 'price_high', 'price_low', 'price_close', 
                      'volume_base', 'buy_volume_base', 'volume_usd', 'txn_count']]
        except Exception as e:
            print(f"    [Binance Native Error] {e}")
            return pd.DataFrame()

    @staticmethod
    def fetch_binance_oi_funding_ls(symbol: str, start_sec: int, end_sec: int) -> pd.DataFrame:
        """Fetch OI, Funding, and L/S ratio from Binance."""
        base_url = BINANCE_FUTURES_API
        sym = symbol.split('.')[0].replace('_PERP', '')
        try:
            # OI History
            oi_resp = requests.get(f"{base_url}/fapi/v1/openInterestHist", 
                                  params={"symbol": sym, "period": "1d", "startTime": start_sec * 1000, "endTime": end_sec * 1000, "limit": 500})
            oi_data = oi_resp.json()
            df_oi = pd.DataFrame(oi_data)
            if not df_oi.empty:
                df_oi['date'] = pd.to_datetime(df_oi['timestamp'], unit='ms', utc=True).dt.strftime('%Y-%m-%d')
                df_oi.rename(columns={'sumOpenInterest':'oi_usd_close'}, inplace=True) # Usually it is sumOpenInterestValue for USD
                # Binance has sumOpenInterest (base) and sumOpenInterestValue (quote)
                if 'sumOpenInterestValue' in df_oi.columns:
                    df_oi.rename(columns={'sumOpenInterestValue':'oi_usd_close'}, inplace=True)
                df_oi = df_oi[['date', 'oi_usd_close']]

            # Funding History
            f_resp = requests.get(f"{base_url}/fapi/v1/fundingRate", 
                                 params={"symbol": sym, "startTime": start_sec * 1000, "endTime": end_sec * 1000, "limit": 1000})
            f_data = f_resp.json()
            df_f = pd.DataFrame(f_data)
            if not df_f.empty:
                df_f['date'] = pd.to_datetime(df_f['fundingTime'], unit='ms', utc=True).dt.strftime('%Y-%m-%d')
                df_f.rename(columns={'fundingRate':'funding_close'}, inplace=True)
                # Resample or take last per day
                df_f = df_f.groupby('date').last().reset_index()[['date', 'funding_close']]

            # Long/Short Global Ratio
            ls_resp = requests.get(f"{base_url}/futures/data/globalLongShortAccountRatio", 
                                  params={"symbol": sym, "period": "1d", "startTime": start_sec * 1000, "endTime": end_sec * 1000, "limit": 500})
            ls_data = ls_resp.json()
            df_ls = pd.DataFrame(ls_data)
            if not df_ls.empty:
                df_ls['date'] = pd.to_datetime(df_ls['timestamp'], unit='ms', utc=True).dt.strftime('%Y-%m-%d')
                df_ls.rename(columns={'longShortRatio':'ls_ratio'}, inplace=True)
                df_ls = df_ls[['date', 'ls_ratio']]
            
            # Merge all
            dfs = [d for d in [df_oi if 'df_oi' in locals() else None, 
                               df_f if 'df_f' in locals() else None, 
                               df_ls if 'df_ls' in locals() else None] if d is not None]
            if not dfs: return pd.DataFrame()
            
            res = dfs[0]
            for d in dfs[1:]: res = res.merge(d, on='date', how='outer')
            return res
        except Exception as e:
            print(f"    [Binance Metrics Error] {e}")
            return pd.DataFrame()

    @staticmethod
    def fetch_bybit_metrics(symbol: str, start_sec: int, end_sec: int) -> pd.DataFrame:
        """Fetch OI, Funding, and L/S ratio from Bybit V5."""
        sym = symbol.split('.')[0]
        try:
            # OI
            oi_resp = requests.get(f"{BYBIT_V5_API}/market/open-interest", 
                                   params={"category": "linear", "symbol": sym, "intervalTime": "1d", "startTime": start_sec * 1000, "endTime": end_sec * 1000})
            oi_data = oi_resp.json().get("result", {}).get("list", [])
            df_oi = pd.DataFrame(oi_data)
            if not df_oi.empty:
                df_oi['date'] = pd.to_datetime(pd.to_numeric(df_oi['timestamp']), unit='ms', utc=True).dt.strftime('%Y-%m-%d')
                df_oi.rename(columns={'openInterest':'oi_usd_close'}, inplace=True)
                df_oi = df_oi[['date', 'oi_usd_close']]

            # Funding
            f_resp = requests.get(f"{BYBIT_V5_API}/market/funding/history", 
                                 params={"category": "linear", "symbol": sym, "startTime": start_sec * 1000, "endTime": end_sec * 1000})
            f_data = f_resp.json().get("result", {}).get("list", [])
            df_f = pd.DataFrame(f_data)
            if not df_f.empty:
                df_f['date'] = pd.to_datetime(pd.to_numeric(df_f['fundingRateTimestamp']), unit='ms', utc=True).dt.strftime('%Y-%m-%d')
                df_f.rename(columns={'fundingRate':'funding_close'}, inplace=True)
                df_f = df_f.groupby('date').last().reset_index()[['date', 'funding_close']]

            # Long/Short Account Ratio
            ls_resp = requests.get(f"{BYBIT_V5_API}/market/account-ratio", 
                                  params={"category": "linear", "symbol": sym, "period": "1d", "startTime": start_sec * 1000, "endTime": end_sec * 1000})
            ls_data = ls_resp.json().get("result", {}).get("list", [])
            df_ls = pd.DataFrame(ls_data)
            if not df_ls.empty:
                df_ls['date'] = pd.to_datetime(pd.to_numeric(df_ls['timestamp']), unit='ms', utc=True).dt.strftime('%Y-%m-%d')
                if 'buyRatio' in df_ls.columns and 'sellRatio' in df_ls.columns:
                    df_ls['ls_ratio'] = pd.to_numeric(df_ls['buyRatio']) / pd.to_numeric(df_ls['sellRatio'])
                elif 'accountRatio' in df_ls.columns:
                    df_ls.rename(columns={'accountRatio':'ls_ratio'}, inplace=True)
                
                if 'ls_ratio' in df_ls.columns:
                    df_ls = df_ls[['date', 'ls_ratio']]
                else:
                    df_ls = pd.DataFrame()

            dfs = [d for d in [df_oi if 'df_oi' in locals() else None, 
                               df_f if 'df_f' in locals() else None, 
                               df_ls if 'df_ls' in locals() else None] if d is not None]
            if not dfs: return pd.DataFrame()
            res = dfs[0]
            for d in dfs[1:]: res = res.merge(d, on='date', how='outer')
            return res
        except Exception as e:
            print(f"    [Bybit Metrics Error] {e}")
            return pd.DataFrame()

    @staticmethod
    def fetch_okx_metrics(symbol: str, start_sec: int, end_sec: int) -> pd.DataFrame:
        """Fetch OI, Funding from OKX V5."""
        sym = symbol.split('.')[0].replace('USDT_PERP', '-USDT-SWAP')
        try:
            # OI
            oi_resp = requests.get(f"{OKX_V5_API}/market/open-interest", params={"instId": sym})
            # OKX OI history is in public statistical data, but let's use the current one if recent
            # For brevity and consistency, OKX is better handled hybridly
            return pd.DataFrame()
        except Exception as e:
            return pd.DataFrame()

    @staticmethod
    def fetch_bybit_core(symbol: str, start_sec: int, end_sec: int) -> pd.DataFrame:
        """Fetch Price, Volume from Bybit V5."""
        try:
            params = {
                "category": "linear",
                "symbol": symbol.split('.')[0],
                "interval": "D",
                "start": start_sec * 1000,
                "end": end_sec * 1000,
                "limit": 1000
            }
            resp = requests.get(f"{BYBIT_V5_API}/market/kline", params=params)
            resp.raise_for_status()
            result = resp.json().get("result", {})
            data = result.get("list", [])
            if not data: return pd.DataFrame()
            
            df = pd.DataFrame(data, columns=['t', 'o', 'h', 'l', 'c', 'v', 'q'])
            df['date'] = pd.to_datetime(pd.to_numeric(df['t']), unit='ms', utc=True).dt.strftime('%Y-%m-%d')
            df.rename(columns={'o':'price_open', 'h':'price_high', 'l':'price_low', 'c':'price_close', 
                             'v':'volume_base', 'q':'volume_usd'}, inplace=True)
            return df[['date', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'volume_usd']]
        except Exception as e:
            print(f"    [Bybit Native Error] {e}")
            return pd.DataFrame()

    @staticmethod
    def fetch_okx_core(symbol: str, start_sec: int, end_sec: int) -> pd.DataFrame:
        """Fetch Price, Volume from OKX V5."""
        try:
            # OKX symbol on exchange: symbol_on_exchange (usually BASE-USDT-SWAP)
            # For simplicity, we assume symbol mapping exists in CoinalyzeClient
            # But here we need the native symbol.
            native_symbol = symbol.split('.')[0].replace('USDT_PERP', '-USDT-SWAP')
            # In OKX v5, 'after' means fetch data older than after (TS)
            # 'before' means fetch data newer than before (TS)
            # To get range [start, end]:
            # after = end_ts + 1 day, before = start_ts - 1 day
            params = {
                "instId": native_symbol,
                "bar": "1D",
                "after": (start_sec - 86400) * 1000, 
                "before": (end_sec + 86400) * 1000,
                "limit": 100
            }
            resp = requests.get(f"{OKX_V5_API}/market/history-candles", params=params)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            if not data: return pd.DataFrame()
            
            df = pd.DataFrame(data, columns=['t', 'o', 'h', 'l', 'c', 'vol', 'volCcy', 'volCcyQuote', 'confirm'])
            df['date'] = pd.to_datetime(pd.to_numeric(df['t']), unit='ms', utc=True).dt.strftime('%Y-%m-%d')
            df.rename(columns={'o':'price_open', 'h':'price_high', 'l':'price_low', 'c':'price_close', 
                             'vol':'volume_base', 'volCcyQuote':'volume_usd'}, inplace=True)
            return df[['date', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'volume_usd']]
        except Exception as e:
            print(f"    [OKX Native Error] {e}")
            return pd.DataFrame()

    @staticmethod
    def fetch_okx_ls_rubik(base: str) -> pd.DataFrame:
        """Fetch Long/Short Ratio from OKX Rubik API."""
        try:
            # Rubik L/S ratio for perp: /api/v5/rubik/stat/contracts/long-short-account-ratio
            # instId should be base-USDT
            params = {"ccy": base.upper(), "period": "1D"}
            resp = requests.get(f"{OKX_V5_API}/rubik/stat/contracts/long-short-account-ratio", params=params)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            if not data: return pd.DataFrame()
            
            # Returns [ts, ratio]
            df = pd.DataFrame(data, columns=['t', 'ls_ratio'])
            df['date'] = pd.to_datetime(pd.to_numeric(df['t']), unit='ms', utc=True).dt.strftime('%Y-%m-%d')
            df['ls_ratio'] = pd.to_numeric(df['ls_ratio'])
            return df[['date', 'ls_ratio']]
        except Exception as e:
            print(f"    [OKX Rubik Error] {e}")
            return pd.DataFrame()


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
# Hybrid Sourcing Manager
# ==============================================================================

def fetch_hybrid_futures_data(client: CoinalyzeClient, native: NativeFuturesFetcher, 
                             token: str, exchange: str, symbol: str,
                             start_sec: int, end_sec: int) -> pd.DataFrame:
    """
    Manager to fetch data from native APIs or Coinalyze based on Smart Sourcing logic.
    """
    days_requested = (end_sec - start_sec) / 86400
    use_native = days_requested <= 35 and exchange.lower() in ["binance", "bybit", "okx"]
    
    # Always fetch Specialized metrics from Coinalyze (except OKX L/S)
    # Metrics: Liquidations, Predicted Funding, Long/Short Ratio (for history > 30d)
    
    print(f"    [Source] {'Native+Coinalyze' if use_native else 'Coinalyze'}")
    
    # 1. Fetch Coinalyze metrics (historical/specialized)
    # We use a subset of METRIC_CALLS to avoid redundant fetching if native is used
    dfs = []
    
    # Always Coinalyze: Liquidations
    df_liq = client.liquidation_daily(symbol, start_sec, end_sec)
    if not df_liq.empty: dfs.append(df_liq)
    
    # Specialized: Pred Funding
    df_pred = client.predicted_funding_rate_daily(symbol, start_sec, end_sec)
    if not df_pred.empty: dfs.append(df_pred)

    if use_native:
        # Native Path
        if exchange.lower() == "binance":
            df_core = native.fetch_binance_core(symbol, start_sec, end_sec)
            df_metrics = native.fetch_binance_oi_funding_ls(symbol, start_sec, end_sec)
            if not df_core.empty: dfs.append(df_core)
            if not df_metrics.empty: dfs.append(df_metrics)
        elif exchange.lower() == "bybit":
            df_core = native.fetch_bybit_core(symbol, start_sec, end_sec)
            df_metrics = native.fetch_bybit_metrics(symbol, start_sec, end_sec)
            if not df_core.empty: dfs.append(df_core)
            if not df_metrics.empty: dfs.append(df_metrics)
        elif exchange.lower() == "okx":
            df_core = native.fetch_okx_core(symbol, start_sec, end_sec)
            df_ls = native.fetch_okx_ls_rubik(token)
            if not df_core.empty: dfs.append(df_core)
            if not df_ls.empty: dfs.append(df_ls)
            
            # OKX OI/Funding from Coinalyze for now as they are harder natively
            df_oi = client.open_interest_daily(symbol, start_sec, end_sec)
            df_f = client.funding_rate_daily(symbol, start_sec, end_sec)
            if not df_oi.empty: dfs.append(df_oi)
            if not df_f.empty: dfs.append(df_f)
    else:
        # Full Coinalyze Path (Backfill)
        df_oi = client.open_interest_daily(symbol, start_sec, end_sec)
        df_f = client.funding_rate_daily(symbol, start_sec, end_sec)
        df_ls = client.long_short_ratio_daily(symbol, start_sec, end_sec)
        df_ohlcv = client.ohlcv_daily(symbol, start_sec, end_sec)
        
        if not df_oi.empty: dfs.append(df_oi)
        if not df_f.empty: dfs.append(df_f)
        if not df_ls.empty: dfs.append(df_ls)
        if not df_ohlcv.empty: dfs.append(df_ohlcv)
        
        # Patch OKX L/S from Rubik always as Coinalyze doesn't have it
        if exchange.lower() == "okx":
            df_rubik = native.fetch_okx_ls_rubik(token)
            if not df_rubik.empty: dfs.append(df_rubik)

    if not dfs: return pd.DataFrame()
    
    # Merge and ensure symbol/exchange
    df_final = merge_dataframes(dfs)
    if not df_final.empty:
        df_final['symbol'] = symbol
        df_final['exchange'] = exchange
    return df_final


# ==============================================================================
# Main Execution
# ==============================================================================
def main():
    """Main entry point for the Coinalyze data backfill script."""
    parser = argparse.ArgumentParser(
        description="Backfill historical crypto futures data from Coinalyze API"
    )
    parser.add_argument("--top", type=int, default=50,
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
    parser.add_argument("--skip-merge", action="store_true",
                       help="Skip merging into existing perp CSV files")
    parser.add_argument("--exchanges", type=str, default="binance,bybit,okx",
                       help=f"Comma-separated list of exchanges, or 'all' for all (default: binance,bybit,okx). Available: {','.join(EXCHANGE_CODES.keys())}")
    args = parser.parse_args()
    
    # Initialize Clients
    client = CoinalyzeClient(args.coinalyze_key, rate_delay=1.6)
    native_fetcher = NativeFuturesFetcher()
    
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
    start_sec = to_unix_seconds(start_dt)
    end_sec = to_unix_seconds(end_dt)
    
    # Determine target tokens (base assets)
    meta = AssetMetadataManager()
    target_bases = []
    
    if args.symbols:
        raw_symbols = [s.strip().upper() for s in args.symbols.split(",")]
        candidates = coingecko_get_top_candidates(specific_symbols=raw_symbols)
        for c in candidates:
            res = meta.get_metadata(c['symbol'], c['id'])
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
            res = meta.get_metadata(c['symbol'], c['id'])
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
        else:
            target_bases = valid_candidates[:args.top]
            selection_desc = f"Top Tokens: {len(target_bases)}"

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
            
            # Dynamic start for incremental fetch
            dynamic_start_sec = get_incremental_start(out_path, start_sec)
            
            # Fetch Hybrid Data (Smart Sourcing)
            metrics = fetch_hybrid_futures_data(
                client, native_fetcher, 
                base, exchange, symbol, 
                dynamic_start_sec, end_sec
            )
            
            if metrics.empty:
                print(f"  [SKIP] No data available for {symbol}")
                skip_count += 1
                continue
            
            print(f"    -> {len(metrics)} rows collected")
            
            # Save metrics file
            if os.path.exists(out_path):
                df_old = pd.read_csv(out_path)
                metrics = pd.concat([df_old, metrics], ignore_index=True)
                metrics.drop_duplicates(subset=['date'], keep='last', inplace=True)
            
            metrics.sort_values("date").to_csv(out_path, index=False)
            print(f"  [SAVED] {out_path} ({len(metrics)} total rows)")
            
            # Optionally merge into existing perp file
            if not args.skip_merge:
                perp_csv = os.path.join(args.perp_dir, exchange, f"{base}USDT_1d.csv")
                if os.path.exists(perp_csv):
                    merge_on_date(perp_csv, metrics.drop(columns=["symbol", "exchange"]))
                    
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
