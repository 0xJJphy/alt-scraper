import os
import time
import requests
import pandas as pd
import psycopg2
import warnings
from psycopg2.extras import execute_values

# Suppress pandas warning about raw DB connections
warnings.filterwarnings("ignore", ".*pandas only supports SQLAlchemy connectable.*")
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional
import argparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Constants
COINGECKO_BASE = "https://api.coingecko.com/api/v3"
STABLES_CATEGORIES = [
    "stablecoins", "usd-stablecoin", "wrapped-tokens", "liquid-staking-tokens", 
    "tokenized-btc", "asset-backed-tokens", "synths", "bridged-tokens"
]

# API Endpoints
BINANCE_SPOT_API = "https://api.binance.com/api/v3/klines"
BYBIT_SPOT_API = "https://api.bybit.com/v5/market/kline"
OKX_SPOT_API = "https://www.okx.com/api/v5/market/history-candles"
OKX_RUBIK_API = "https://www.okx.com/api/v5/rubik/stat/taker-volume"
COINALYZE_BASE = "https://api.coinalyze.net/v1"

def to_unix_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

# ==============================================================================
# Database Management
# ==============================================================================
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

    def upsert_spot_ohlcv(self, df: pd.DataFrame):
        """Upsert a dataframe into spot_daily_ohlcv using the schema function."""
        if not self.enabled or df.empty:
            return

        conn = None
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            
            for _, row in df.iterrows():
                cur.execute("""
                    SELECT upsert_spot_daily_ohlcv(
                        %s::DATE, %s::VARCHAR, %s::VARCHAR, 
                        %s::DECIMAL, %s::DECIMAL, %s::DECIMAL, %s::DECIMAL, 
                        %s::DECIMAL, %s::DECIMAL, 
                        %s::DECIMAL, %s::DECIMAL, %s::DECIMAL, 
                        %s::BIGINT, %s::BIGINT, %s::BIGINT
                    )
                """, (
                    row.get('date'), row.get('symbol'), row.get('exchange'),
                    row.get('price_open'), row.get('price_high'), row.get('price_low'), row.get('price_close'),
                    row.get('volume_base'), row.get('volume_usd'),
                    row.get('buy_volume_base'), row.get('sell_volume_base'), row.get('volume_delta'),
                    row.get('txn_count'), row.get('buy_txn_count'), row.get('sell_txn_count')
                ))
            
            conn.commit()
            cur.close()
            print(f"    [DB] Upserted {len(df)} rows to Supabase.")
        except Exception as e:
            print(f"    [DB ERROR] Upsert failed: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    def upsert_asset_metadata(self, symbol: str, narrative: str, is_filtered: int):
        """Upsert asset metadata into asset_metadata table."""
        if not self.enabled: return
        
        conn = None
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            cur.execute("SELECT upsert_asset_metadata(%s::VARCHAR, %s::VARCHAR, %s::BOOLEAN)", (symbol, narrative, bool(is_filtered)))
            conn.commit()
            cur.close()
        except Exception as e:
            print(f"    [DB ERROR] Metadata upsert failed for {symbol}: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    def get_last_data_date(self, symbol: str, exchange: str) -> Optional[datetime]:
        """Get the last stored date for a symbol/exchange in the DB."""
        if not self.enabled: return None
        conn = None
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            cur.execute("""
                SELECT MAX(date) FROM spot_daily_ohlcv 
                WHERE symbol = %s AND exchange = %s
            """, (symbol, exchange))
            res = cur.fetchone()
            if res and res[0]:
                return res[0] # Returns a date object
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
            query = "SELECT symbol, narrative, is_filtered FROM asset_metadata"
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            print(f"    [DB INFO] Could not fetch metadata: {e}")
            return pd.DataFrame()
        finally:
            if conn: conn.close()

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
        if not categories:
            return "Unknown"
            
        # Preference: Specific sectors > Generic L1/L2 > Ecosystems
        # 1. Look for specific sectors (excluding generic terms)
        generic_terms = ["Ecosystem", "Standard", "Portfolio", "Asset-Backed", "Wrapped", "Index", "SEC Securities", "Alleged", "FTX Holdings", "Multicoin Capital", "Alameda Research", "GMCI", "Proof of", "Made in", "CoinList", "Launchpad", "Research", "Ventures", "Capital"]
        specific = [c for c in categories if not any(x in c for x in generic_terms)]
        
        if specific:
            # Prefer sectors that are not just "Layer 1" or "Smart Contract Platform" if others exist
            detailed = [c for c in specific if c not in ["Layer 1 (L1)", "Layer 2 (L2)", "Smart Contract Platform"]]
            if detailed:
                return detailed[0]
            return specific[0]
            
        return categories[0]

    def get_metadata(self, symbol: str, coin_id: str) -> Dict:
        """Get narrative and filter status, checking cache first."""
        symbol = symbol.upper()
        cache_row = self.df[self.df['symbol'] == symbol]
        
        if not cache_row.empty:
            row = cache_row.iloc[0]
            return {"narrative": row['narrative'], "is_filtered": int(row['is_filtered'])}
            
        # Not in cache, fetch from CoinGecko
        print(f"  [CG] Fetching detail for {symbol} ({coin_id})...")
        url = f"{COINGECKO_BASE}/coins/{coin_id}"
        try:
            resp = requests.get(url, params={"localization": "false", "tickers": "false", "market_data": "false", "community_data": "false", "developer_data": "false", "sparkline": "false"})
            if resp.status_code == 429:
                print("    Rate limit. Waiting 60s...")
                time.sleep(60)
                return self.get_metadata(symbol, coin_id)
            
            resp.raise_for_status()
            detail = resp.json()
            categories = detail.get("categories", [])
            cat_ids = [c.lower().replace(" ", "-") for c in categories]
            
            # Check for excluded categories
            is_filtered = 0
            narrative = "Unknown"
            
            # Use original category names for the narrative column
            excluded_cats_indices = [i for i, cid in enumerate(cat_ids) if any(s_cat in cid for s_cat in STABLES_CATEGORIES)]
            if excluded_cats_indices:
                is_filtered = 1
                narrative = categories[excluded_cats_indices[0]]
            else:
                narrative = self._select_best_narrative(categories)

            # Update cache
            new_row = pd.DataFrame([{'symbol': symbol, 'narrative': narrative, 'is_filtered': is_filtered}])
            self.df = pd.concat([self.df, new_row], ignore_index=True).drop_duplicates('symbol')
            
            # Persist to DB immediately
            if self.db_manager and self.db_manager.enabled:
                self.db_manager.upsert_asset_metadata(symbol, narrative, is_filtered)
            
            # Persist to CSV if allowed
            if self.allow_csv:
                self.df.to_csv(self.file_path, index=False)
            
            return {"narrative": narrative, "is_filtered": is_filtered}
            
        except Exception as e:
            print(f"    [ERROR] CG Fetch failed for {symbol}: {e}")
            return {"narrative": "Unknown", "is_filtered": 0}

def coingecko_get_top_candidates(n: int = 50, specific_symbols: Optional[List[str]] = None) -> List[Dict]:
    """Fetch top tokens or specific symbols from CoinGecko markets."""
    print(f"[INFO] Fetching market data from CoinGecko (specific={bool(specific_symbols)})...")
    out = []
    
    url = f"{COINGECKO_BASE}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": "false",
    }
    
    if specific_symbols:
        # Sort and join symbols for the API call
        params["symbols"] = ",".join(specific_symbols).lower()
        params["per_page"] = 100
    
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        
        for coin in data:
            out.append({
                "symbol": coin.get("symbol", "").upper(),
                "id": coin.get("id")
            })
            
        # If we asked for specific symbols, we might want to preserve the order or ensure all found
        if specific_symbols:
            # Simple re-sorting if needed, though usually not critical
            pass
            
    except Exception as e:
        print(f"[ERROR] CG Markets API failed: {e}")
            
    return out

class CoinalyzeClient:
    """Minimized client for Coinalyze Spot data."""
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"api-key": api_key}

    def fetch_ohlcv(self, symbol: str, start_ts: int, end_ts: int) -> pd.DataFrame:
        url = f"{COINALYZE_BASE}/ohlcv-history"
        params = {
            "symbols": symbol,
            "interval": "daily",
            "from": start_ts // 1000,
            "to": end_ts // 1000
        }
        try:
            resp = requests.get(url, params=params, headers=self.headers)
            resp.raise_for_status()
            data = resp.json()
            if not data or not data[0].get('history'): return pd.DataFrame()
            
            history = data[0]['history']
            df = pd.DataFrame(history)
            # t: time, o: open, h: high, l: low, c: close, v: vol_base, bv: buy_vol_base, tx: trades, btx: buy_trades
            df.rename(columns={
                't': 'timestamp', 'o': 'price_open', 'h': 'price_high', 
                'l': 'price_low', 'c': 'price_close', 'v': 'volume_base', 
                'bv': 'buy_volume_base', 'tx': 'txn_count', 'btx': 'buy_txn_count'
            }, inplace=True)
            
            df['date'] = pd.to_datetime(df['timestamp'], unit='s', utc=True).dt.strftime('%Y-%m-%d')
            # Volume USD estimate if not provided (Coinalyze usually gives it for some, but let's be safe)
            df['volume_usd'] = df['volume_base'] * df['price_close']
            df['sell_volume_base'] = df['volume_base'] - df['buy_volume_base']
            df['volume_delta'] = df['buy_volume_base'] - df['sell_volume_base']
            
            # Calculate sell transactions
            if 'txn_count' in df.columns and 'buy_txn_count' in df.columns:
                df['sell_txn_count'] = df['txn_count'].astype(float) - df['buy_txn_count'].astype(float)
            
            # Numeric conversion for new columns
            for col in ['txn_count', 'buy_txn_count', 'sell_txn_count']:
                if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
                
            return df
        except Exception as e:
            print(f"    [Coinalyze Error] {e}")
            return pd.DataFrame()

class SpotScraper:
    def __init__(self, output_dir: str = "data/spot"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def get_incremental_start(self, path: str, default_start_ts: int, symbol: str, exchange: str, db_manager: Optional[DatabaseManager] = None) -> int:
        """
        Determine start date based on:
        1. Database last record (if available)
        2. Local CSV file (if available)
        3. Default start date
        """
        last_date = None
        
        # 1. Check Database
        if db_manager and db_manager.enabled:
            last_date_db = db_manager.get_last_data_date(symbol, exchange)
            if last_date_db:
                # Convert date to datetime at midnight UTC
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
            # Re-fetch the last 2 days to ensure completeness
            start_dt = last_date - timedelta(days=2)
            return max(default_start_ts, to_unix_ms(start_dt))
            
        return default_start_ts

    def fetch_binance(self, symbol: str, start_ts: int, end_ts: int) -> pd.DataFrame:
        """ Binance Klines """
        print(f"  [Binance] Fetching {symbol}...")
        all_data = []
        current_start = start_ts
        limit = 1000
        
        while current_start < end_ts:
            params = {"symbol": f"{symbol}USDT", "interval": "1d", "startTime": current_start, "endTime": end_ts, "limit": limit}
            try:
                resp = requests.get(BINANCE_SPOT_API, params=params)
                if resp.status_code == 400: return pd.DataFrame()
                resp.raise_for_status()
                data = resp.json()
                if not data: break
                all_data.extend(data)
                current_start = data[-1][0] + 86400000
                time.sleep(0.5)
            except Exception as e:
                print(f"    Error: {e}"); break
                
        if not all_data: return pd.DataFrame()
        df = pd.DataFrame(all_data)
        cols = ['timestamp', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'close_time', 'volume_usd', 'txn_count', 'buy_volume_base', 'buy_volume_usd', 'ignore']
        df.columns = cols[:len(df.columns)]
        df['date'] = pd.to_datetime(pd.to_numeric(df['timestamp']), unit='ms', utc=True).dt.strftime('%Y-%m-%d')
        for col in ['price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'volume_usd', 'buy_volume_base']:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
        df['sell_volume_base'] = df['volume_base'] - df['buy_volume_base']
        df['volume_delta'] = df['buy_volume_base'] - df['sell_volume_base']
        df['exchange'], df['symbol'] = 'binance', f"{symbol}USDT"
        
        # Try to patch with Coinalyze buy_txn_count if available
        api_key = os.getenv("COINALYZE_API_KEY")
        if api_key:
            try:
                client = CoinalyzeClient(api_key)
                # Binance Spot on Coinalyze is s{BASE}USDT.A
                cz_df = client.fetch_ohlcv(f"s{symbol}USDT.A", start_ts, end_ts)
                if not cz_df.empty:
                    # Merge on date to get buy_txn_count
                    df = df.merge(cz_df[['date', 'buy_txn_count', 'sell_txn_count']], on='date', how='left')
            except: pass

        # Ensure all required columns exist
        final_cols = ['date', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'volume_usd', 'buy_volume_base', 'sell_volume_base', 'volume_delta', 'txn_count', 'buy_txn_count', 'sell_txn_count', 'symbol', 'exchange']
        for col in final_cols:
            if col not in df.columns:
                df[col] = float('nan')

        return df[final_cols]

    def fetch_bybit(self, symbol: str, start_ts: int, end_ts: int) -> pd.DataFrame:
        """ Bybit Spot via Coinalyze (for Delta support) """
        print(f"  [Bybit/Coinalyze] Fetching {symbol}...")
        api_key = os.getenv("COINALYZE_API_KEY")
        if not api_key:
            print("    [ERROR] COINALYZE_API_KEY not found. Skipping Bybit.")
            return pd.DataFrame()
            
        client = CoinalyzeClient(api_key)
        # Bybit Spot on Coinalyze is s{BASE}USDT.6
        cz_symbol = f"s{symbol}USDT.6"
        df = client.fetch_ohlcv(cz_symbol, start_ts, end_ts)
        
        if df.empty: return pd.DataFrame()
        
        df['exchange'], df['symbol'] = 'bybit', f"{symbol}USDT"
        cols = ['date', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'volume_usd', 'buy_volume_base', 'sell_volume_base', 'volume_delta', 'txn_count', 'buy_txn_count', 'sell_txn_count', 'symbol', 'exchange']
        valid_cols = [c for c in cols if c in df.columns]
        return df[valid_cols].sort_values('date')

    def fetch_okx_delta(self, symbol: str) -> pd.DataFrame:
        """ Fetch Delta from OKX Rubik API (last 180 days) """
        try:
            params = {"ccy": symbol, "period": "1D", "instType": "SPOT"}
            resp = requests.get(OKX_RUBIK_API, params=params)
            resp.raise_for_status()
            data = resp.json().get("data", [])
            if not data: return pd.DataFrame()
            
            # OKX Rubik returns [ts, buy_vol, sell_vol]
            df = pd.DataFrame(data, columns=['timestamp', 'buy_volume_base', 'sell_volume_base'])
            df['date'] = pd.to_datetime(pd.to_numeric(df['timestamp']), unit='ms', utc=True).dt.strftime('%Y-%m-%d')
            df['buy_volume_base'] = pd.to_numeric(df['buy_volume_base'])
            df['sell_volume_base'] = pd.to_numeric(df['sell_volume_base'])
            df['volume_delta'] = df['buy_volume_base'] - df['sell_volume_base']
            return df[['date', 'buy_volume_base', 'sell_volume_base', 'volume_delta']]
        except Exception as e:
            print(f"    [OKX Delta Error] {e}")
            return pd.DataFrame()

    def fetch_okx(self, symbol: str, start_ts: int, end_ts: int) -> pd.DataFrame:
        """ OKX V5 Pagination + Rubik Delta Patch """
        print(f"  [OKX] Fetching {symbol} Prices...")
        all_data = []
        current_after = end_ts + 86400000
        while True:
            params = {"instId": f"{symbol}-USDT", "bar": "1D", "after": current_after, "before": start_ts - 1, "limit": 100}
            try:
                resp = requests.get(OKX_SPOT_API, params=params)
                resp.raise_for_status()
                data = resp.json().get("data", [])
                if not data: break
                all_data.extend(data)
                current_after = data[-1][0]
                if int(current_after) <= start_ts: break
                time.sleep(0.2)
            except Exception as e:
                print(f"    Error: {e}"); break
        
        if not all_data: return pd.DataFrame()
        
        df = pd.DataFrame(all_data, columns=['timestamp', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'volume_usd', 'volCcyQuote', 'confirm'])
        df['date'] = pd.to_datetime(pd.to_numeric(df['timestamp']), unit='ms', utc=True).dt.strftime('%Y-%m-%d')
        for col in ['price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'volume_usd']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Patch with Delta from Rubik
        delta_df = self.fetch_okx_delta(symbol)
        if not delta_df.empty:
            df = df.merge(delta_df, on='date', how='left')
        else:
            df['buy_volume_base'] = df['sell_volume_base'] = df['volume_delta'] = float('nan')
            
        # Try to patch with Coinalyze for transaction counts
        api_key = os.getenv("COINALYZE_API_KEY")
        if api_key:
            try:
                client = CoinalyzeClient(api_key)
                # OKX Spot on Coinalyze is s{BASE}USDT.3
                cz_df = client.fetch_ohlcv(f"s{symbol}USDT.3", start_ts, end_ts)
                if not cz_df.empty:
                    # Merge to get txn_count, buy_txn_count, sell_txn_count
                    df = df.merge(cz_df[['date', 'txn_count', 'buy_txn_count', 'sell_txn_count']], on='date', how='left')
            except: pass

        df['exchange'], df['symbol'] = 'okx', f"{symbol}-USDT"
        
        # Ensure all required columns exist
        final_cols = ['date', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'volume_usd', 'buy_volume_base', 'sell_volume_base', 'volume_delta', 'txn_count', 'buy_txn_count', 'sell_txn_count', 'symbol', 'exchange']
        for col in final_cols:
            if col not in df.columns:
                df[col] = float('nan')

        return df[final_cols].sort_values('date')

def main():
    parser = argparse.ArgumentParser(description="Fetch historical Spot OHLCV data from major exchanges")
    parser.add_argument("--limit", type=int, default=100, help="Number of top tokens to fetch (default: 100)")
    parser.add_argument("--top", type=int, dest="limit", help="Alias for --limit (backwards compatibility)")
    parser.add_argument("--csv", action="store_true", help="Save results to local CSV files (default: False)")
    parser.add_argument("--top-range", type=str, default=None, help="Rank range (e.g. 1-50)")
    parser.add_argument("--symbols", type=str, default=None, help="Specific symbols (e.g. BTC,ETH)")
    parser.add_argument("--exchanges", type=str, default="binance,bybit,okx", help="Exchanges to fetch")
    parser.add_argument("--start", type=str, default="2017-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--output-dir", type=str, default="data/spot", help="Output directory")
    args = parser.parse_args()
    
    scraper = SpotScraper(args.output_dir)
    db_manager = DatabaseManager()
    
    # Initialize Metadata Manager with DB support
    meta = AssetMetadataManager(db_manager=db_manager, allow_csv=args.csv)
    target_bases = []
    
    if args.symbols:
        raw_symbols = [s.strip().upper() for s in args.symbols.split(",")]
        candidates = coingecko_get_top_candidates(specific_symbols=raw_symbols)
        for c in candidates:
            res = meta.get_metadata(c['symbol'], c['id'])
            if res.get("is_filtered") == 0:
                target_bases.append(c['symbol'])
    else:
        limit = 50
        if args.top_range:
            _, end_rank = map(int, args.top_range.split("-"))
            limit = end_rank
        else:
            limit = args.limit
            
        candidates = coingecko_get_top_candidates(n=limit)
        valid_candidates = []
        for c in candidates:
            res = meta.get_metadata(c['symbol'], c['id'])
            if res['is_filtered'] == 0:
                valid_candidates.append(c['symbol'])
            if len(valid_candidates) >= limit: break
            
        if args.top_range:
            start_rank, end_rank = map(int, args.top_range.split("-"))
            target_bases = valid_candidates[start_rank-1:end_rank]
        else:
            target_bases = valid_candidates[:args.limit]

    exchanges = [e.strip().lower() for e in args.exchanges.split(",")]
    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.now(timezone.utc)
    start_ts, end_ts = to_unix_ms(start_dt), to_unix_ms(end_dt)
    
    print("=" * 60)
    print(f"Exchange Spot OHLCV Backfill (Cache: {len(meta.df)} assets)")
    print("=" * 60)
    print(f"Date Range: {start_dt.date()} to {end_dt.date()}")
    print(f"Exchanges:  {exchanges}")
    print(f"Targeting:  {len(target_bases)} tokens")
    print(f"Targeting:  {len(target_bases)} tokens")
    print("=" * 60)
    
    # Sync Metadata to Database
    if db_manager.enabled and not meta.df.empty:
        print(f"[DB] Syncing {len(meta.df)} cached assets metadata...")
        for _, row in meta.df.iterrows():
            db_manager.upsert_asset_metadata(row['symbol'], row['narrative'], row['is_filtered'])
    
    for exchange in exchanges:
        print(f"\nProcessing EXCHANGE: {exchange.upper()}")
        exch_dir = os.path.join(args.output_dir, exchange)
        os.makedirs(exch_dir, exist_ok=True)
        for base in target_bases:
            try:
                # Determine save path and incremental start
                fname = f"{base}USDT_spot_1d.csv"
                if exchange == 'okx': fname = f"{base}-USDT_spot_1d.csv"
                path = os.path.join(exch_dir, f"{base}USDT_spot_1d.csv") # Default format
                
                # Check for exchange-specific formats if needed, but keeping it consistent
                if exchange == 'okx': path = os.path.join(exch_dir, f"{base}USDT_spot_1d.csv")
                # Wait, earlier verification showed okx saved as BTCUSDT_spot_1d.csv in its dir
                # Let's check what I did in the previous tool calls
                
                path = os.path.join(exch_dir, f"{base}USDT_spot_1d.csv")
                
                # Dynamic start for incremental fetch
                # Standardize symbol/exchange format for DB lookup
                db_symbol = f"{base}USDT"
                if exchange == 'okx': db_symbol = f"{base}-USDT"
                
                dynamic_start = scraper.get_incremental_start(path, start_ts, db_symbol, exchange, db_manager)
                
                if exchange == 'binance': df_new = scraper.fetch_binance(base, dynamic_start, end_ts)
                elif exchange == 'bybit': df_new = scraper.fetch_bybit(base, dynamic_start, end_ts)
                elif exchange == 'okx': df_new = scraper.fetch_okx(base, dynamic_start, end_ts)
                else: break
                
                if not df_new.empty:
                    # Save to Database (Supabase)
                    if db_manager.enabled:
                        db_manager.upsert_spot_ohlcv(df_new)
                    
                    # Save metrics file (Optional CSV)
                    if args.csv:
                        if os.path.exists(path):
                            df_old = pd.read_csv(path)
                            df_final = pd.concat([df_old, df_new], ignore_index=True)
                            df_final.drop_duplicates(subset=['date'], keep='last', inplace=True)
                            df_final.sort_values('date', inplace=True)
                        else:
                            df_final = df_new
                            
                        df_final.to_csv(path, index=False)
                        print(f"    [CSV] Saved {base} -> {len(df_final)} total rows (New: {len(df_new)})")
                    else:
                        print(f"    [CSV] Skipping local save (use --csv to enable)")
                else: print(f"    [SKIPPED] {base} (no new data)")
            except Exception as e: print(f"    [FAILED] {base}: {e}")
            time.sleep(0.5)

if __name__ == "__main__":
    main()
