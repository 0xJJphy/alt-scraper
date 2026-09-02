import os
import time
import socket
import json
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
# data-api.binance.vision works from GitHub Actions (not geo-blocked)
BINANCE_SPOT_MIRRORS = [
    "https://data-api.binance.vision",  # Primary - works from GHA
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
]
BINANCE_SPOT_API = f"{BINANCE_SPOT_MIRRORS[0]}/api/v3/klines"
BYBIT_SPOT_API = "https://api.bybit.com/v5/market/kline"
OKX_SPOT_API = "https://www.okx.com/api/v5/market/history-candles"
OKX_RUBIK_API = "https://www.okx.com/api/v5/rubik/stat/taker-volume"
COINBASE_SPOT_API = "https://api.exchange.coinbase.com"
COINALYZE_BASE = "https://api.coinalyze.net/v1"
SPOT_SYMBOL_CACHE = os.path.join("data", "cache", "spot_exchange_symbols.json")

def to_unix_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _load_json_cache(path: str, ttl_hours: float) -> Optional[dict]:
    if ttl_hours <= 0 or not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        cached_at = float(payload.get("cached_at", 0))
        age_hours = (time.time() - cached_at) / 3600
        if age_hours <= ttl_hours:
            print(f"[INFO] Loaded spot exchange symbol cache ({age_hours:.1f}h old)")
            return payload
    except Exception as e:
        print(f"[INFO] Could not read spot symbol cache: {e}")
    return None


def _save_json_cache(path: str, payload: dict) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
    except Exception as e:
        print(f"[INFO] Could not write spot symbol cache: {e}")


def _fetch_binance_spot_symbols() -> Dict[str, str]:
    resp = _tor.direct.get(
        f"{BINANCE_SPOT_MIRRORS[0]}/api/v3/exchangeInfo",
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return {
        s["baseAsset"].upper(): s["symbol"]
        for s in data.get("symbols", [])
        if s.get("quoteAsset") == "USDT" and s.get("status") == "TRADING"
    }


def _fetch_bybit_spot_symbols() -> Dict[str, str]:
    out = {}
    cursor = None
    while True:
        params = {"category": "spot", "limit": 1000}
        if cursor:
            params["cursor"] = cursor
        resp = _tor.session.get(
            "https://api.bybit.com/v5/market/instruments-info",
            params=params,
            timeout=30,
        )
        resp.raise_for_status()
        result = resp.json().get("result", {})
        for s in result.get("list", []):
            if s.get("quoteCoin") == "USDT" and s.get("status") == "Trading":
                out[s.get("baseCoin", "").upper()] = s.get("symbol", "")
        cursor = result.get("nextPageCursor")
        if not cursor:
            break
    return out


def _fetch_okx_spot_symbols() -> Dict[str, str]:
    resp = _tor.session.get(
        "https://www.okx.com/api/v5/public/instruments",
        params={"instType": "SPOT"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return {
        s.get("baseCcy", "").upper(): s.get("instId", "")
        for s in data.get("data", [])
        if s.get("quoteCcy") == "USDT" and s.get("state") == "live"
    }


def _fetch_coinbase_spot_symbols() -> Dict[str, str]:
    """Productos USD de Coinbase.

    Coinbase es el único venue de la tabla cuyo quote es USD y no USDT: solo tiene 21
    pares USDT online frente a 401 en USD, así que restringirlo a USDT dejaría fuera el
    89% del mercado. `volume_base` va en unidades del activo base, que es lo que se agrega
    entre exchanges, y el desvío USD/USDT es el del peg (<0.1%).
    """
    resp = _tor.session.get(
        f"{COINBASE_SPOT_API}/products",
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    return {
        p.get("base_currency", "").upper(): p.get("id", "")
        for p in resp.json()
        if p.get("quote_currency") == "USD"
        and p.get("status") == "online"
        and not p.get("trading_disabled")
    }


# Formato del símbolo por exchange: (separador, quote).
SPOT_SYMBOL_FORMATS = {
    "binance":  ("",  "USDT"),   # BTCUSDT
    "bybit":    ("",  "USDT"),   # BTCUSDT
    "okx":      ("-", "USDT"),   # BTC-USDT
    "coinbase": ("-", "USD"),    # BTC-USD
}


def spot_symbol_for(exchange: str, base: str) -> str:
    """Símbolo nativo tal y como se guarda en spot_daily_ohlcv.symbol."""
    sep, quote = SPOT_SYMBOL_FORMATS.get(exchange.lower(), ("", "USDT"))
    return f"{base}{sep}{quote}"


def load_spot_exchange_symbols(exchanges: List[str], ttl_hours: float = 24.0) -> Dict[str, Dict[str, str]]:
    """
    Load native spot listings per exchange (quote USDT, o USD en coinbase).

    This prevents wasting historical requests on assets that are in the global
    universe but do not trade on a given spot venue. El quote de cada venue está en
    SPOT_SYMBOL_FORMATS.
    """
    cached = _load_json_cache(SPOT_SYMBOL_CACHE, ttl_hours)
    symbols_by_exchange = cached.get("exchanges", {}) if cached else {}
    loaded = dict(symbols_by_exchange)

    fetchers = {
        "binance": _fetch_binance_spot_symbols,
        "bybit": _fetch_bybit_spot_symbols,
        "okx": _fetch_okx_spot_symbols,
        "coinbase": _fetch_coinbase_spot_symbols,
    }

    missing = [ex for ex in exchanges if ex in fetchers and ex not in loaded]
    for exchange in missing:
        try:
            loaded[exchange] = fetchers[exchange]()
            print(f"[INFO] Loaded {len(loaded[exchange])} native spot {SPOT_SYMBOL_FORMATS.get(exchange, ('', 'USDT'))[1]} symbols for {exchange}")
        except Exception as e:
            print(f"[WARN] Could not load native spot symbols for {exchange}: {e}")
            loaded[exchange] = {}

    if missing:
        _save_json_cache(SPOT_SYMBOL_CACHE, {"cached_at": time.time(), "exchanges": loaded})

    return {ex: loaded.get(ex, {}) for ex in exchanges}

# ==============================================================================
# Tor Proxy Manager (opt-in via TOR_PROXY env var)
# ==============================================================================

class TorProxyManager:
    """Wraps a requests.Session with optional SOCKS5 Tor proxy and circuit rotation.
    Activated only when TOR_PROXY is set in the environment. Falls back to a
    plain session otherwise — zero behavior change when Tor is not configured.

    Two sessions are always available:
      self.session  — routed via Tor (OKX, Bybit: geo-blocked without Tor)
      self.direct   — plain connection, NO proxy (Binance: blocks ALL Tor exit nodes)
    """
    def __init__(self):
        proxy = os.getenv("TOR_PROXY", "").strip()
        self._control_port = int(os.getenv("TOR_CONTROL_PORT", 9051))
        self.active = bool(proxy)
        self.direct = requests.Session()   # always plain — for Binance
        self.session = requests.Session()  # Tor-proxied when active
        if self.active:
            self.session.proxies = {"http": proxy, "https": proxy}
            print(f"[Tor] Proxy active ({proxy}) — OKX/Bybit via Tor, Binance via direct", flush=True)

    def rotate_circuit(self) -> bool:
        """Request a new Tor exit node via SIGNAL NEWNYM on the ControlPort."""
        if not self.active:
            return False
        try:
            with socket.create_connection(("127.0.0.1", self._control_port), timeout=5) as s:
                s.sendall(b'AUTHENTICATE ""\r\nSIGNAL NEWNYM\r\nQUIT\r\n')
                s.recv(1024)
            time.sleep(3)  # allow circuit to establish
            print("[Tor] Circuit rotated — new exit node assigned", flush=True)
            return True
        except Exception as e:
            print(f"[Tor] Circuit rotation failed: {e}", flush=True)
            return False


_tor = TorProxyManager()

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

    def upsert_spot_ohlcv(self, df: pd.DataFrame):
        """Batch upsert spot OHLCV data using execute_values (50-100x faster)."""
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
                    self._sanitize_float(row.get('price_open')),
                    self._sanitize_float(row.get('price_high')),
                    self._sanitize_float(row.get('price_low')),
                    self._sanitize_float(row.get('price_close')),
                    self._sanitize_float(row.get('volume_base')),
                    self._sanitize_float(row.get('volume_usd')),
                    self._sanitize_float(row.get('buy_volume_base')),
                    self._sanitize_float(row.get('sell_volume_base')),
                    self._sanitize_float(row.get('volume_delta')),
                    self._sanitize_int(row.get('txn_count')),
                    self._sanitize_int(row.get('buy_txn_count')),
                    self._sanitize_int(row.get('sell_txn_count'))
                ))

            # Batch INSERT with ON CONFLICT (upsert)
            sql = """
                INSERT INTO spot_daily_ohlcv (
                    date, symbol, exchange,
                    price_open, price_high, price_low, price_close,
                    volume_base, volume_usd,
                    buy_volume_base, sell_volume_base, volume_delta,
                    txn_count, buy_txn_count, sell_txn_count,
                    updated_at
                ) VALUES %s
                ON CONFLICT (date, symbol, exchange) DO UPDATE SET
                    price_open = COALESCE(EXCLUDED.price_open, spot_daily_ohlcv.price_open),
                    price_high = COALESCE(EXCLUDED.price_high, spot_daily_ohlcv.price_high),
                    price_low = COALESCE(EXCLUDED.price_low, spot_daily_ohlcv.price_low),
                    price_close = COALESCE(EXCLUDED.price_close, spot_daily_ohlcv.price_close),
                    volume_base = COALESCE(EXCLUDED.volume_base, spot_daily_ohlcv.volume_base),
                    volume_usd = COALESCE(EXCLUDED.volume_usd, spot_daily_ohlcv.volume_usd),
                    buy_volume_base = COALESCE(EXCLUDED.buy_volume_base, spot_daily_ohlcv.buy_volume_base),
                    sell_volume_base = COALESCE(EXCLUDED.sell_volume_base, spot_daily_ohlcv.sell_volume_base),
                    volume_delta = COALESCE(EXCLUDED.volume_delta, spot_daily_ohlcv.volume_delta),
                    txn_count = COALESCE(EXCLUDED.txn_count, spot_daily_ohlcv.txn_count),
                    buy_txn_count = COALESCE(EXCLUDED.buy_txn_count, spot_daily_ohlcv.buy_txn_count),
                    sell_txn_count = COALESCE(EXCLUDED.sell_txn_count, spot_daily_ohlcv.sell_txn_count),
                    updated_at = NOW()
            """

            # Template adds NOW() for updated_at
            template = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())"

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
            cur.close()
        except Exception as e:
            print(f"    [DB ERROR] Metadata upsert failed for {symbol}: {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()

    def bulk_upsert_asset_metadata(self, df: pd.DataFrame):
        """Efficiently upsert multiple metadata records in a single connection."""
        if not self.enabled or df.empty: return
        
        conn = None
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            
            records = []
            for _, row in df.iterrows():
                records.append((
                    self._to_python(row['symbol']),
                    self._to_python(row['narrative']),
                    bool(row['is_filtered']),
                    self._sanitize_float(row.get('market_cap')),
                    self._sanitize_int(row.get('market_cap_rank'))
                ))
            
            sql = """
                SELECT upsert_asset_metadata(
                    v.symbol, v.narrative, v.is_filtered, v.market_cap, v.market_cap_rank
                )
                FROM (VALUES %s) AS v(symbol, narrative, is_filtered, market_cap, market_cap_rank)
            """
            execute_values(cur, sql, records, page_size=100)
            conn.commit()
            cur.close()
            print(f"    [DB] Bulk synced {len(records)} metadata records.")
        except Exception as e:
            print(f"    [DB ERROR] Bulk metadata sync failed: {e}")
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
            query = "SELECT symbol, narrative, is_filtered, market_cap, market_cap_rank FROM asset_metadata"
            df = pd.read_sql(query, conn)
            conn.close()
            return df
        except Exception as e:
            print(f"    [DB INFO] Could not fetch metadata: {e}")
            return pd.DataFrame()
        finally:
            if conn: conn.close()

    def get_full_tracked_symbols(self) -> List[str]:
        """Return all non-filtered assets that have ever been in the historical top universe."""
        if not self.enabled:
            return []
        conn = None
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            cur.execute("""
                SELECT symbol
                FROM asset_metadata
                WHERE (is_filtered = false OR is_filtered IS NULL)
                  AND ever_in_top_50 = true
                ORDER BY market_cap_rank ASC NULLS LAST, symbol ASC
            """)
            return [r[0] for r in cur.fetchall()]
        except Exception as e:
            print(f"    [DB INFO] Could not fetch full tracked universe: {e}")
            return []
        finally:
            if conn:
                conn.close()

class AssetMetadataManager:
    def __init__(self, file_path: str = "data/asset_metadata.csv", db_manager: Optional[DatabaseManager] = None, allow_csv: bool = True):
        self.file_path = file_path
        self.db_manager = db_manager
        self.allow_csv = allow_csv
        self.df = pd.DataFrame(columns=['symbol', 'narrative', 'is_filtered', 'market_cap', 'market_cap_rank'])
        
        # 1. Load from CSV first (if exists)
        csv_df = pd.DataFrame()
        if os.path.exists(file_path):
            try:
                csv_df = pd.read_csv(file_path)
                print(f"  [Meta] Loaded {len(csv_df)} assets from CSV")
            except Exception as e:
                print(f"  [Meta Check] Could not read CSV: {e}")

        # 2. Load from DB (if enabled)
        db_df = pd.DataFrame()
        if self.db_manager and self.db_manager.enabled:
            print("  [Meta] Loading from Database...")
            db_df = self.db_manager.get_all_asset_metadata()
            if not db_df.empty:
                print(f"  [Meta] Loaded {len(db_df)} assets from DB")

        # 3. Merge (CSV takes priority for narrative/filter if both exist)
        if not csv_df.empty and not db_df.empty:
            # Union of symbols
            self.df = pd.concat([csv_df, db_df], ignore_index=True).drop_duplicates('symbol', keep='first')
        elif not csv_df.empty:
            self.df = csv_df
        elif not db_df.empty:
            self.df = db_df
        
        # Ensure correct types
        if not self.df.empty:
            if 'is_filtered' in self.df.columns:
                self.df['is_filtered'] = pd.to_numeric(self.df['is_filtered'], errors='coerce').fillna(0).astype(int)
            self.df['symbol'] = self.df['symbol'].str.upper()

        # Create/Touch CSV if allowed and not exists
        if self.allow_csv and not os.path.exists(file_path):
             os.makedirs(os.path.dirname(file_path), exist_ok=True)
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

    def get_metadata(self, symbol: str, coin_id: str, market_cap: Optional[float] = None, market_cap_rank: Optional[int] = None) -> Dict:
        """Get narrative and filter status, checking cache first.

        Optimized to avoid slow CoinGecko API calls:
        1. Check cache (DB/CSV) first
        2. Use KNOWN_FILTERED_SYMBOLS for instant stablecoin/wrapped detection
        3. Assign default narrative based on market_cap_rank if not in cache
        4. Only call CoinGecko API as last resort (with rate limit handling)
        """
        symbol = symbol.upper()
        cache_row = self.df[self.df['symbol'] == symbol]

        # 1. Check cache first
        if not cache_row.empty:
            row = cache_row.iloc[0]
            # Update market_cap if provided
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
        # This is fast and avoids rate limits. Narratives can be enriched later.
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

    def enrich_narratives_from_api(self, coin_id: str, symbol: str) -> Optional[str]:
        """Optional: Fetch detailed narrative from CoinGecko API (slow, use sparingly)."""
        try:
            url = f"{COINGECKO_BASE}/coins/{coin_id}"
            resp = requests.get(url, params={"localization": "false", "tickers": "false", "market_data": "false", "community_data": "false", "developer_data": "false", "sparkline": "false"}, timeout=30)
            if resp.status_code == 429:
                print(f"    [CG] Rate limit hit for {symbol}, skipping enrichment")
                return None

            resp.raise_for_status()
            detail = resp.json()
            categories = detail.get("categories", [])
            cat_ids = [c.lower().replace(" ", "-") for c in categories]

            # Check for excluded categories
            excluded_cats_indices = [i for i, cid in enumerate(cat_ids) if any(s_cat in cid for s_cat in STABLES_CATEGORIES)]
            if excluded_cats_indices:
                return categories[excluded_cats_indices[0]]
            else:
                return self._select_best_narrative(categories)

        except Exception as e:
            print(f"    [CG] Enrichment failed for {symbol}: {e}")
            return None

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


def coingecko_get_top_candidates(n: int = 50, specific_symbols: Optional[List[str]] = None, max_retries: int = 3) -> List[Dict]:
    """Fetch top tokens from CoinGecko markets with retry for null market_cap.

    ONE API call = 250 tokens with market_cap.
    Retries if too many market_caps are null (CoinGecko sometimes returns incomplete data).
    """
    print(f"[INFO] Fetching market data from CoinGecko (specific={bool(specific_symbols)})...")

    url = f"{COINGECKO_BASE}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": "false",
    }

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
                print(f"[CG] Note: {null_count} tokens have null market_cap (will use 0)")

            return out

        except requests.exceptions.Timeout:
            print(f"[CG] Timeout, attempt {attempt+1}/{max_retries}")
            time.sleep(2 ** attempt)
        except Exception as e:
            print(f"[ERROR] CG Markets API failed: {e}")
            time.sleep(2 ** attempt)

    print("[CG] All retries failed, returning empty list")
    return []

class CoinalyzeClient:
    """Minimized client for Coinalyze Spot data.
    Uses COINALYZE_API_KEY_SPOT if set, otherwise falls back to COINALYZE_API_KEY.
    Keeping spot calls on a dedicated key avoids contention with alt_scraper's
    futures batch calls when both run in parallel inside run_pipeline.py.
    """
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {"api-key": api_key}
        self._last_call = 0.0
        self._min_interval = 1.6  # stay under 40 req/min

    def _throttle(self):
        """Proactive rate limiting — avoids 429s instead of reacting to them."""
        now = time.time()
        gap = self._min_interval - (now - self._last_call)
        if gap > 0:
            time.sleep(gap)
        self._last_call = time.time()

    def fetch_ohlcv(self, symbol: str, start_ts: int, end_ts: int, max_retries: int = 3) -> pd.DataFrame:
        url = f"{COINALYZE_BASE}/ohlcv-history"
        params = {
            "symbols": symbol,
            "interval": "daily",
            "from": start_ts // 1000,
            "to": end_ts // 1000
        }
        
        for attempt in range(max_retries):
            try:
                self._throttle()
                resp = requests.get(url, params=params, headers=self.headers, timeout=30)

                if resp.status_code == 429:
                    retry_after = int(float(resp.headers.get("Retry-After", "10")))
                    print(f"    [Coinalyze Retry] Rate limited for {symbol}, waiting {retry_after}s (attempt {attempt+1}/{max_retries})...")
                    time.sleep(retry_after)
                    continue
                
                resp.raise_for_status()
                data = resp.json()
                if not data or not data[0].get('history'): return pd.DataFrame()
                
                history = data[0]['history']
                df = pd.DataFrame(history)
                df.rename(columns={
                    't': 'timestamp', 'o': 'price_open', 'h': 'price_high', 
                    'l': 'price_low', 'c': 'price_close', 'v': 'volume_base', 
                    'bv': 'buy_volume_base', 'tx': 'txn_count', 'btx': 'buy_txn_count'
                }, inplace=True)
                
                df['date'] = pd.to_datetime(df['timestamp'], unit='s', utc=True).dt.strftime('%Y-%m-%d')
                df['volume_usd'] = df['volume_base'] * df['price_close']
                
                # Derived metrics
                if 'buy_volume_base' in df.columns:
                    df['sell_volume_base'] = df['volume_base'] - df['buy_volume_base']
                    df['volume_delta'] = df['buy_volume_base'] - df['sell_volume_base']
                
                if 'txn_count' in df.columns and 'buy_txn_count' in df.columns:
                    df['sell_txn_count'] = df['txn_count'] - df['buy_txn_count']
                
                for col in ['txn_count', 'buy_txn_count', 'sell_txn_count']:
                    if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                return df
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    print(f"    [Coinalyze Retry] Request failed for {symbol}: {e}. Retrying in 10s...")
                    time.sleep(10)
                else:
                    print(f"    [Coinalyze Error] {e}")
                    return pd.DataFrame()
            except Exception as e:
                print(f"    [Coinalyze Error] Unexpected error: {e}")
                return pd.DataFrame()
                
        return pd.DataFrame()

# ==============================================================================
# Exchange Fetchers (Standardized)
# ==============================================================================

class BinanceSpotFetcher:
    """Fetcher for Binance Spot V3 API."""
    BASE_URL = "https://data-api.binance.vision/api/v3"  # Works from GHA
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }

    def fetch_current_day_data(self, symbol: str, max_retries: int = 3) -> Optional[Dict]:
        """Fetch today's open candle data."""
        for attempt in range(max_retries):
            try:
                params = {"symbol": f"{symbol}USDT", "interval": "1d", "limit": 1}
                resp = _tor.direct.get(f"{self.BASE_URL}/klines", params=params, headers=self.HEADERS, timeout=15)  # Binance blocks all Tor exit nodes — use direct
                if resp.status_code == 429:
                    wait_time = int(float(resp.headers.get("Retry-After", 2 ** attempt)))
                    print(f"    [Binance Spot] Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                elif resp.status_code in (403, 418, 451):
                    print(f"    [Binance Spot] IP blocked (HTTP {resp.status_code}), attempt {attempt+1}/{max_retries}")
                    _tor.rotate_circuit()
                    time.sleep(2 ** attempt)
                    continue
                data = resp.json()
                if not data: return None
                k = data[0]
                # [ts, o, h, l, c, v, cts, qv, n, tbv, tqv, ignore]
                return {
                    "price_open": float(k[1]), "price_high": float(k[2]), "price_low": float(k[3]), "price_close": float(k[4]),
                    "volume_base": float(k[5]), "volume_usd": float(k[7]), "txn_count": int(k[8]),
                    "buy_volume_base": float(k[9])
                }
            except requests.exceptions.Timeout:
                print(f"    [Binance Spot] Timeout, attempt {attempt+1}/{max_retries}")
                time.sleep(2 ** attempt)
            except Exception as e:
                print(f"    [Binance Spot Error] {e}")
                return None
        return None

class BybitSpotFetcher:
    """Fetcher for Bybit V5 Spot API."""
    BASE_URL = "https://api.bybit.com/v5/market"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }

    def fetch_current_day_data(self, symbol: str, max_retries: int = 3) -> Optional[Dict]:
        for attempt in range(max_retries):
            try:
                params = {"category": "spot", "symbol": f"{symbol}USDT", "interval": "D", "limit": 1}
                resp = _tor.session.get(f"{self.BASE_URL}/kline", params=params, headers=self.HEADERS, timeout=15)
                if resp.status_code == 429:
                    print(f"    [Bybit Spot] Rate limited, waiting {2 ** attempt}s...")
                    time.sleep(2 ** attempt)
                    continue
                data = resp.json().get("result", {}).get("list", [])
                if not data: return None
                k = data[0]
                # [ts, o, h, l, c, v, qv]
                return {
                    "price_open": float(k[1]), "price_high": float(k[2]), "price_low": float(k[3]), "price_close": float(k[4]),
                    "volume_base": float(k[5]), "volume_usd": float(k[6])
                }
            except requests.exceptions.Timeout:
                print(f"    [Bybit Spot] Timeout, attempt {attempt+1}/{max_retries}")
                time.sleep(2 ** attempt)
            except Exception as e:
                print(f"    [Bybit Spot Error] {e}")
                return None
        return None

class OKXSpotFetcher:
    """Fetcher for OKX V5 Spot API."""
    BASE_URL = "https://www.okx.com/api/v5"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }

    def fetch_current_day_data(self, symbol: str, max_retries: int = 3) -> Optional[Dict]:
        for attempt in range(max_retries):
            try:
                # "1Dutc", no "1D": ver fetch_okx. Con "1D" antes de las 16:00 UTC esto
                # devolvia la vela abierta AYER a las 16:00 y la guardabamos como la de hoy.
                params = {"instId": f"{symbol}-USDT", "bar": "1Dutc", "limit": 1}
                resp = _tor.session.get(f"{self.BASE_URL}/market/candles", params=params, headers=self.HEADERS, timeout=15)
                if resp.status_code == 429:
                    print(f"    [OKX Spot] Rate limited, waiting {2 ** attempt}s...")
                    time.sleep(2 ** attempt)
                    continue
                elif resp.status_code in (403, 418):
                    print(f"    [OKX Spot] IP blocked (HTTP {resp.status_code}), attempt {attempt+1}/{max_retries}")
                    _tor.rotate_circuit()
                    continue
                data = resp.json().get("data", [])
                if not data: return None
                k = data[0]
                return {
                    "price_open": float(k[1]), "price_high": float(k[2]), "price_low": float(k[3]), "price_close": float(k[4]),
                    "volume_base": float(k[5]), "volume_usd": float(k[6])
                }
            except requests.exceptions.Timeout:
                print(f"    [OKX Spot] Timeout, attempt {attempt+1}/{max_retries}")
                time.sleep(2 ** attempt)
            except Exception as e:
                print(f"    [OKX Spot Error] {e}")
                return None
        return None

    def fetch_bulk_rubik_delta(self, symbol: str, max_retries: int = 3) -> pd.DataFrame:
        """Fetch Taker Buy/Sell ratio from Rubik (last ~180 days).

        OJO CON DOS COSAS, las dos nos han corrompido el delta de OKX:

        1) OKX devuelve cada fila como [ts, sellVol, buyVol] — el SELL va primero. Leerlas
           al revés invertía el signo del delta de todo el histórico de OKX (se veía en que
           sign(volume_delta) anticorrelaba con el retorno del día: -0.20 en ETH, -0.14 en XRP,
           mientras Binance/Bybit daban +0.23/+0.26).

        2) Rubik se consulta por `ccy`, así que agrega TODOS los pares spot del activo
           (USDT, USDC, BTC...), pero la fila que parcheamos es solo el par -USDT. Las
           magnitudes no son comparables: buy+sell nunca cuadraba con volume_base.
           Por eso devolvemos únicamente `buy_ratio` — la proporción sí es representativa,
           la magnitud no. Quien llama la reescala con su propio volume_base.
        """
        for attempt in range(max_retries):
            try:
                params = {"ccy": symbol, "period": "1D", "instType": "SPOT"}
                resp = _tor.session.get(f"{self.BASE_URL}/rubik/stat/taker-volume", params=params, headers=self.HEADERS, timeout=15)
                if resp.status_code == 429:
                    print(f"    [OKX Rubik] Rate limited, waiting {2 ** attempt}s...")
                    time.sleep(2 ** attempt)
                    continue
                json_data = resp.json()
                if json_data.get('code') != '0':
                    print(f"    [Rubik Info] Code {json_data.get('code')}: {json_data.get('msg')}")
                    return pd.DataFrame()
                data = json_data.get("data", [])
                if not data: return pd.DataFrame()
                df = pd.DataFrame(data, columns=['timestamp', 'sell_volume_base', 'buy_volume_base'])
                df['date'] = pd.to_datetime(df['timestamp'].astype('int64'), unit='ms', utc=True).dt.strftime('%Y-%m-%d')
                buy = pd.to_numeric(df['buy_volume_base'], errors='coerce')
                sell = pd.to_numeric(df['sell_volume_base'], errors='coerce')
                total = buy + sell
                df['buy_ratio'] = buy / total.where(total > 0)
                return df[['date', 'buy_ratio']].dropna(subset=['buy_ratio'])
            except requests.exceptions.Timeout:
                print(f"    [OKX Rubik] Timeout, attempt {attempt+1}/{max_retries}")
                time.sleep(2 ** attempt)
            except Exception as e:
                print(f"    [Rubik Error] {e}")
                return pd.DataFrame()
        return pd.DataFrame()

class CoinbaseSpotFetcher:
    """Fetcher for the Coinbase Exchange public API (USD pairs)."""
    BASE_URL = COINBASE_SPOT_API
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }

    def fetch_current_day_data(self, symbol: str, max_retries: int = 3) -> Optional[Dict]:
        """Vela abierta de hoy. El histórico NO pasa por aquí: lo sirve Coinalyze entero
        en una sola llamada (ver SpotScraper.fetch_coinbase), así que a Coinbase solo le
        pedimos 1 petición por símbolo y corrida — muy por debajo de sus 10 req/s por IP.
        """
        for attempt in range(max_retries):
            try:
                resp = _tor.session.get(
                    f"{self.BASE_URL}/products/{symbol}-USD/candles",
                    params={"granularity": 86400}, headers=self.HEADERS, timeout=15,
                )
                if resp.status_code == 429:
                    wait_time = int(float(resp.headers.get("Retry-After", 2 ** attempt)))
                    print(f"    [Coinbase Spot] Rate limited, waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                data = resp.json()
                if not isinstance(data, list) or not data: return None
                k = data[0]  # más reciente primero
                # OJO: Coinbase devuelve [time, LOW, HIGH, OPEN, close, volume] — low y high
                # van ANTES que open/close, al revés de lo habitual. Leerlo en el orden
                # típico OHLC intercambia apertura con mínimo y máximo con cierre.
                return {
                    "price_open": float(k[3]), "price_high": float(k[2]),
                    "price_low": float(k[1]), "price_close": float(k[4]),
                    "volume_base": float(k[5]),
                    # El endpoint no da volumen en quote: se deriva, igual que en
                    # CoinalyzeClient.fetch_ohlcv.
                    "volume_usd": float(k[5]) * float(k[4]),
                }
            except requests.exceptions.Timeout:
                print(f"    [Coinbase Spot] Timeout, attempt {attempt+1}/{max_retries}")
                time.sleep(2 ** attempt)
            except Exception as e:
                print(f"    [Coinbase Spot Error] {e}")
                return None
        return None

GLOBAL_FETCHERS = {
    "binance": BinanceSpotFetcher(),
    "bybit": BybitSpotFetcher(),
    "okx": OKXSpotFetcher(),
    "coinbase": CoinbaseSpotFetcher()
}

# Tolerancia relativa al comparar el volumen de una fuente externa con el volumen propio
# de la fila. Por debajo de esto asumimos que ambas describen el MISMO mercado.
RECONCILE_TOL = 0.02


def _reconciles(external_total: pd.Series, own_total: pd.Series) -> pd.Series:
    """True donde el total de la fuente externa cuadra con el propio dentro de RECONCILE_TOL.

    Es la comprobación que faltaba: sin ella mezclábamos el buy_volume de las klines de
    Binance (par USDT) con el sell_volume de Coinalyze (que podía venir del par FDUSD o USDC
    por la cadena de fallback), y el delta resultante medía la diferencia de escala entre dos
    mercados distintos, no la presión compradora.
    """
    own = pd.to_numeric(own_total, errors='coerce')
    ext = pd.to_numeric(external_total, errors='coerce')
    rel = (ext - own).abs() / own.where(own > 0)
    return rel <= RECONCILE_TOL


def _needs_fill(series: pd.Series, volume_base: pd.Series) -> pd.Series:
    """True donde el valor falta. Un 0 con volumen > 0 también cuenta como hueco."""
    s = pd.to_numeric(series, errors='coerce')
    v = pd.to_numeric(volume_base, errors='coerce')
    return s.isna() | ((s == 0) & (v > 0))


def patch_missing_metrics(df: pd.DataFrame, base: str, exchange: str, symbol: str) -> pd.DataFrame:
    """Hybrid patching for Spot data.

    Regla única, aplicada igual a los tres exchanges: de una fuente externa solo aceptamos la
    MAGNITUD si su volumen total reconcilia con el volume_base de la fila. Si no reconcilia,
    la usamos solo como RATIO y reescalamos. Si no hay ni ratio, se queda a NULL.

    Y solo rellenamos `buy_volume_base`: `sell_volume_base` y `volume_delta` se DERIVAN
    siempre de volume_base - buy, así que el invariante buy + sell == volume_base se cumple
    por construcción y el delta nunca puede volver a mezclar dos fuentes.
    """
    if exchange.lower() not in GLOBAL_FETCHERS: return df
    fetcher = GLOBAL_FETCHERS[exchange.lower()]

    # 1. Ensure Today's Data
    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    # Initialize metric columns if they don't exist
    target_cols = ['buy_volume_base', 'sell_volume_base', 'volume_delta', 'txn_count', 'buy_txn_count', 'sell_txn_count']
    for col in target_cols:
        if col not in df.columns:
            df[col] = None

    if df.empty or not (df['date'] == today_str).any():
        print(f"    [Hybrid] Fetching current day open candle from {exchange.upper()}...")
        today_data = fetcher.fetch_current_day_data(base)
        if today_data:
            today_row = pd.DataFrame([today_data])
            today_row['date'] = today_str
            today_row['symbol'] = symbol
            today_row['exchange'] = exchange
            df = pd.concat([df, today_row], ignore_index=True)

    if df.empty: return df

    for col in ['volume_base', 'buy_volume_base', 'sell_volume_base', 'txn_count', 'buy_txn_count']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Procedencia del par buy/sell, solo para el log. Permite auditar una corrida sin tener
    # que deducir a posteriori de dónde salió cada número.
    sources = []
    if df['buy_volume_base'].notna().any():
        sources.append(f"nativo:{exchange}")

    # 2. Patch missing Metrics (Taker Buy Volume & Txn Counts)
    # Use dedicated spot key to avoid quota contention with alt_scraper futures batch.
    # Confirmado con la API real: cada key de Coinalyze tiene su propio cupo de
    # 40 req/min, independiente — no es "la misma cuenta reparte cupo entre keys".
    # El fallback de abajo existía para no romper si no había key de spot, pero
    # es JUSTO el fallo que este comentario dice evitar: si COINALYZE_API_KEY_SPOT
    # falta (p.ej. un .env que no la trae), este proceso empieza a compartir cupo
    # con alt_scraper.py sin que nada lo diga — igual pasa corriendo en paralelo o
    # secuencial, sólo que en paralelo revienta antes. El aviso de abajo lo hace
    # ruidoso en vez de silencioso.
    spot_key = os.getenv("COINALYZE_API_KEY_SPOT")
    api_key = spot_key or os.getenv("COINALYZE_API_KEY")
    if api_key and not spot_key:
        print("    [WARN] COINALYZE_API_KEY_SPOT no está configurada: usando la key de "
              "futures. Si esto corre junto a alt_scraper.py comparten cupo de 40 req/min "
              "y uno de los dos puede acabar en 429.")
    if api_key and not df.empty:
        # Determine patch window: from the start of the current dataframe to today
        df_sorted = df.sort_values('date')
        first_date_str = df_sorted.iloc[0]['date']
        patch_start_dt = datetime.strptime(first_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        patch_start_ts = int(patch_start_dt.timestamp() * 1000)

        print(f"    [Hybrid] Patching metrics via Coinalyze bulk history (from {first_date_str})...")
        client = CoinalyzeClient(api_key)
        # Mapping for Coinalyze Spot symbols: (prefijo, quote, sufijo de exchange).
        # Bybit lleva prefijo 's'; coinbase cotiza contra USD, no USDT.
        cz_map = {
            "binance":  ("",  "USDT", ".A"),
            "bybit":    ("s", "USDT", ".6"),
            "okx":      ("",  "USDT", ".3"),
            "coinbase": ("",  "USD",  ".C"),
        }
        prefix, quote, suffix = cz_map.get(exchange.lower(), ("", "USDT", ""))

        # Binance Spot mapping is tricky on Coinalyze. Fallback sequence: USDT -> FDUSD -> USDC
        # Cuidado: FDUSD/USDC son OTRO mercado. Sirven como ratio, nunca como magnitud — de ahí
        # la reconciliación de más abajo.
        syms_to_try = [f"{prefix}{base}{quote}{suffix}"]
        if exchange.lower() == "binance":
            syms_to_try.extend([f"{base}FDUSD.A", f"{base}USDC.A"])

        df_cz = pd.DataFrame()
        cz_symbol_used = None
        for cz_sym in syms_to_try:
            temp_df = client.fetch_ohlcv(cz_sym, patch_start_ts, to_unix_ms(datetime.now(timezone.utc)))
            if not temp_df.empty:
                # Check if we got any of the critical metrics (more permissive)
                has_tx = 'txn_count' in temp_df.columns and temp_df['txn_count'].notna().any() and (temp_df['txn_count'] > 0).any()
                has_btv = 'buy_volume_base' in temp_df.columns and temp_df['buy_volume_base'].notna().any()

                if has_tx or has_btv:
                    df_cz = temp_df
                    cz_symbol_used = cz_sym
                    print(f"    [Hybrid] Using Coinalyze symbol: {cz_sym} (tx={has_tx}, btv={has_btv})")
                    break

        if not df_cz.empty:
            cz_cols = {c: f"cz_{c}" for c in ['volume_base', 'buy_volume_base', 'txn_count', 'buy_txn_count'] if c in df_cz.columns}
            cz = df_cz[['date'] + list(cz_cols)].rename(columns=cz_cols)
            cz = cz.drop_duplicates(subset=['date'], keep='last')
            df = df.merge(cz, on='date', how='left')

            if 'cz_buy_volume_base' in df.columns and 'cz_volume_base' in df.columns:
                vol_ok = _reconciles(df['cz_volume_base'], df['volume_base'])
                cz_vol = pd.to_numeric(df['cz_volume_base'], errors='coerce')
                cz_ratio = pd.to_numeric(df['cz_buy_volume_base'], errors='coerce') / cz_vol.where(cz_vol > 0)
                # Magnitud si el mercado es el mismo; si no, solo la proporción reescalada.
                cz_buy = pd.to_numeric(df['cz_buy_volume_base'], errors='coerce').where(vol_ok, cz_ratio * df['volume_base'])
                fill = _needs_fill(df['buy_volume_base'], df['volume_base']) & cz_buy.notna()
                if fill.any():
                    df.loc[fill, 'buy_volume_base'] = cz_buy[fill]
                    scaled = int((fill & ~vol_ok).sum())
                    sources.append(f"coinalyze:{cz_symbol_used}" + (f"(ratio x{scaled})" if scaled else ""))
                    if scaled:
                        print(f"    [Hybrid] {scaled} filas de {cz_symbol_used} no reconcilian con "
                              f"volume_base: usadas como ratio, no como magnitud.")

            # Los contadores van en pareja (txn_count, buy_txn_count): o vienen los dos de la
            # misma fuente, o no valen. Mezclarlos daba buy_txn_count > txn_count (63 filas solo
            # en BTC/binance) y por tanto sell_txn_count negativo.
            if 'cz_txn_count' in df.columns and 'cz_buy_txn_count' in df.columns:
                own_tx = pd.to_numeric(df['txn_count'], errors='coerce')
                cz_btx = pd.to_numeric(df['cz_buy_txn_count'], errors='coerce')
                tx_ok = _reconciles(df['cz_txn_count'], own_tx) | own_tx.isna()
                take = tx_ok & cz_btx.notna() & _needs_fill(df['buy_txn_count'], df['volume_base'])
                if take.any():
                    df.loc[take, 'buy_txn_count'] = cz_btx[take]
                    no_own = take & own_tx.isna()
                    if no_own.any():
                        df.loc[no_own, 'txn_count'] = pd.to_numeric(df['cz_txn_count'], errors='coerce')[no_own]
                dropped = int((~tx_ok & cz_btx.notna()).sum())
                if dropped:
                    print(f"    [Hybrid] {dropped} filas con buy_txn_count descartadas: el "
                          f"txn_count de Coinalyze no cuadra con el propio.")

            df.drop(columns=[c for c in df.columns if c.startswith('cz_')], inplace=True)
            print(f"    [Hybrid] Patched {base} with Coinalyze depth data.")

    # 3. Special Case: OKX Rubik — solo como RATIO (agrega todos los pares del activo).
    if exchange.lower() == "okx":
        print(f"    [Hybrid] Patching OKX Taker Volume via Rubik (ratio)...")
        df_rubik = fetcher.fetch_bulk_rubik_delta(base)
        if not df_rubik.empty:
            df = df.merge(df_rubik.drop_duplicates(subset=['date'], keep='last'), on='date', how='left')
            rubik_buy = df['buy_ratio'] * df['volume_base']
            fill = _needs_fill(df['buy_volume_base'], df['volume_base']) & rubik_buy.notna()
            if fill.any():
                df.loc[fill, 'buy_volume_base'] = rubik_buy[fill]
                sources.append(f"rubik-ratio(x{int(fill.sum())})")
            df.drop(columns=['buy_ratio'], inplace=True)

    # 4. Derivados. sell y delta SIEMPRE salen de volume_base - buy: así el invariante
    #    buy + sell == volume_base se cumple por construcción.
    if 'volume_base' in df.columns and 'buy_volume_base' in df.columns:
        df['sell_volume_base'] = df['volume_base'] - df['buy_volume_base']
        df['volume_delta'] = df['buy_volume_base'] - df['sell_volume_base']

    if 'txn_count' in df.columns and 'buy_txn_count' in df.columns:
        df['sell_txn_count'] = pd.to_numeric(df['txn_count'], errors='coerce') - pd.to_numeric(df['buy_txn_count'], errors='coerce')

    # 5. Guarda de invariante. Es la red que impide que esto vuelva a corromperse en silencio:
    #    antes, un sell contaminado se escribía en la DB sin que nada lo dijera.
    if not df.empty:
        vol = pd.to_numeric(df['volume_base'], errors='coerce')
        buy = pd.to_numeric(df['buy_volume_base'], errors='coerce')
        sell = pd.to_numeric(df['sell_volume_base'], errors='coerce')
        bad_vol = buy.notna() & (
            (buy < 0) | (sell < 0)
            | (((buy + sell - vol).abs() / vol.where(vol > 0)) > RECONCILE_TOL)
        )
        if bad_vol.any():
            print(f"    [WARN] {symbol}/{exchange}: {int(bad_vol.sum())} filas violan "
                  f"buy+sell==volume_base — se anulan buy/sell/delta.")
            df.loc[bad_vol, ['buy_volume_base', 'sell_volume_base', 'volume_delta']] = None

        tx = pd.to_numeric(df['txn_count'], errors='coerce')
        btx = pd.to_numeric(df['buy_txn_count'], errors='coerce')
        bad_tx = btx.notna() & ((btx < 0) | (tx.notna() & (btx > tx)))
        if bad_tx.any():
            print(f"    [WARN] {symbol}/{exchange}: {int(bad_tx.sum())} filas con "
                  f"buy_txn_count > txn_count — se anulan los contadores buy/sell.")
            df.loc[bad_tx, ['buy_txn_count', 'sell_txn_count']] = None

        print(f"    [Hybrid] {symbol}/{exchange} buy/sell desde: {', '.join(sources) or 'sin datos'}")

    # 6. Final Cleanup (Drop duplicates and ensure consistency)
    if not df.empty:
        df.drop_duplicates(subset=['date'], keep='last', inplace=True)

    return df

class SpotScraper:
    def __init__(self, output_dir: str = "data/spot"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def get_incremental_start(self, path: str, default_start_ts: int, symbol: str, exchange: str, db_manager: Optional[DatabaseManager] = None) -> int:
        """Determines start date with 7-day overlap and user-override priority."""
        last_date = None
        if db_manager and db_manager.enabled:
            last_date_db = db_manager.get_last_data_date(symbol, exchange)
            if last_date_db:
                last_date = datetime.combine(last_date_db, datetime.min.time(), tzinfo=timezone.utc)
                print(f"    [Start] Found DB record: {last_date.date()}")

        if not last_date and os.path.exists(path):
            try:
                df = pd.read_csv(path)
                if not df.empty and 'date' in df.columns:
                    last_date_str = df['date'].max()
                    last_date = datetime.strptime(last_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    print(f"    [Start] Found CSV record: {last_date.date()}")
            except: pass

        if last_date:
            # Re-fetch the last 14 days to ensure completeness (Coinalyze/OKX lag fix)
            fast_forward_ts = to_unix_ms(last_date - timedelta(days=14))
            
            # If the user didn't specify a start date (it's the 2017 default), 
            # or if the requested start is newer than our fast_forward, we use incremental.
            if default_start_ts <= 1483228800000:
                print(f"    [Start] Incremental mode (14d overlap): {datetime.fromtimestamp(fast_forward_ts/1000, tz=timezone.utc).date()}")
                return fast_forward_ts
                
            # If user provided a custom start date newer than 2017, respect it
            return default_start_ts
            
        return default_start_ts

    def fetch_binance(self, base: str, start_ts: int, end_ts: int) -> pd.DataFrame:
        """Binance Spot with retry logic and headers."""
        print(f"  [Binance] Fetching {base}...")
        all_data = []
        current_start = start_ts
        limit = 1000
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.binance.com/"
        }

        # Track current mirror index — rotate on 403/418 regardless of Tor status
        current_mirror_idx = 0
        current_api = BINANCE_SPOT_API  # starts with data-api.binance.vision

        while current_start < end_ts:
            params = {"symbol": f"{base}USDT", "interval": "1d", "startTime": current_start, "endTime": end_ts, "limit": limit}

            for attempt in range(3):
                try:
                    resp = _tor.direct.get(current_api, params=params, headers=headers, timeout=30)  # Binance blocks all Tor exit nodes — use direct
                    if resp.status_code == 200:
                        data = resp.json()
                        if not data:
                            break
                        all_data.extend(data)
                        current_start = data[-1][0] + 86400000
                        time.sleep(0.3)
                        break
                    elif resp.status_code == 429:
                        wait_time = int(float(resp.headers.get("Retry-After", 2 ** attempt)))
                        print(f"    [Binance] Rate limited, waiting {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    elif resp.status_code in (400, 403, 418, 451):
                        print(f"    [Binance] HTTP {resp.status_code} for {base} (attempt {attempt+1}/3)")
                        # Rotate circuit AND switch mirror — data-api.binance.vision blocks Tor exit nodes
                        current_mirror_idx = (current_mirror_idx + 1) % len(BINANCE_SPOT_MIRRORS)
                        current_api = f"{BINANCE_SPOT_MIRRORS[current_mirror_idx]}/api/v3/klines"
                        print(f"    [Binance] Switching to mirror: {BINANCE_SPOT_MIRRORS[current_mirror_idx]}")
                        if _tor.active:
                            _tor.rotate_circuit()  # new IP + new mirror
                        else:
                            time.sleep(5 ** attempt + 5)
                        continue
                    else:
                        print(f"    [Binance] Unexpected HTTP {resp.status_code}")
                        break
                except requests.exceptions.Timeout:
                    print(f"    [Binance] Timeout for {base}, attempt {attempt+1}/3")
                    time.sleep(2 ** attempt)
                except Exception as e:
                    print(f"    [Binance] Error: {e}")
                    break
            else:
                break  # All retries failed

            if not all_data or (resp.status_code == 200 and not data):
                break

        if not all_data:
            print(f"    [Binance] No data returned for {base}")
            return pd.DataFrame()

        print(f"    [Binance] Fetched {len(all_data)} candles from data-api.binance.vision")
        df = pd.DataFrame(all_data)
        cols = ['timestamp', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'close_time', 'volume_usd', 'txn_count', 'buy_volume_base', 'buy_volume_usd', 'ignore']
        df.columns = cols[:len(df.columns)]
        df['date'] = pd.to_datetime(pd.to_numeric(df['timestamp']), unit='ms', utc=True).dt.strftime('%Y-%m-%d')
        for col in ['price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'volume_usd', 'buy_volume_base']:
            if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce')

        df['exchange'], df['symbol'] = 'binance', f"{base}USDT"
        df = patch_missing_metrics(df, base, 'binance', f"{base}USDT")

        final_cols = ['date', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'volume_usd', 'buy_volume_base', 'sell_volume_base', 'volume_delta', 'txn_count', 'buy_txn_count', 'sell_txn_count', 'symbol', 'exchange']
        return df[[c for c in final_cols if c in df.columns]]

    def fetch_bybit(self, base: str, start_ts: int, end_ts: int) -> pd.DataFrame:
        """ Bybit Spot Hybrid (Coinalyze Primary) """
        print(f"  [Bybit] Sourcing from Coinalyze...")
        api_key = os.getenv("COINALYZE_API_KEY_SPOT") or os.getenv("COINALYZE_API_KEY")
        if not api_key: return pd.DataFrame()
        client = CoinalyzeClient(api_key)
        df = client.fetch_ohlcv(f"s{base}USDT.6", start_ts, end_ts)
        if df.empty: return pd.DataFrame()
        df['exchange'], df['symbol'] = 'bybit', f"{base}USDT"
        return patch_missing_metrics(df, base, 'bybit', f"{base}USDT")

    def fetch_coinbase(self, base: str, start_ts: int, end_ts: int) -> pd.DataFrame:
        """ Coinbase Spot Hybrid (Coinalyze Primary) — mismo patrón que Bybit.

        Coinalyze sirve Coinbase con símbolo {BASE}USD.C, con buy volume y contadores de
        operaciones desde 2017 y cierres idénticos a la API de Coinbase, así que el
        histórico completo entra en una sola llamada y el delta cumple el invariante sin
        necesidad de reescalar nada.
        """
        print(f"  [Coinbase] Sourcing from Coinalyze...")
        api_key = os.getenv("COINALYZE_API_KEY_SPOT") or os.getenv("COINALYZE_API_KEY")
        if not api_key: return pd.DataFrame()
        client = CoinalyzeClient(api_key)
        df = client.fetch_ohlcv(f"{base}USD.C", start_ts, end_ts)
        if df.empty: return pd.DataFrame()
        symbol = spot_symbol_for('coinbase', base)
        df['exchange'], df['symbol'] = 'coinbase', symbol
        df = patch_missing_metrics(df, base, 'coinbase', symbol)

        final_cols = ['date', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'volume_usd', 'buy_volume_base', 'sell_volume_base', 'volume_delta', 'txn_count', 'buy_txn_count', 'sell_txn_count', 'symbol', 'exchange']
        return df[[c for c in final_cols if c in df.columns]]

    def fetch_okx(self, base: str, start_ts: int, end_ts: int) -> pd.DataFrame:
        """ OKX Spot Hybrid """
        print(f"  [OKX] Fetching {base}...")
        all_data = []
        current_after = end_ts + 86400000
        while True:
            # "1D" en OKX cierra en el corte de UTC+8 (16:00 UTC), no en medianoche UTC:
            # la fila okx describia otras 24h que la de binance/bybit con la MISMA `date`
            # (308 bps de diferencia media en price_close, frente a 23 bps de bybit).
            # "1Dutc" es la misma vela alineada a UTC.
            params = {"instId": f"{base}-USDT", "bar": "1Dutc", "after": current_after, "limit": 100}
            try:
                resp = _tor.session.get(OKX_SPOT_API, params=params, timeout=15)
                if resp.status_code in (403, 418, 429):
                    print(f"    [OKX] HTTP {resp.status_code}, rotating circuit...")
                    _tor.rotate_circuit()
                    continue
                data = resp.json().get("data", [])
                if not data: break
                all_data.extend(data)
                current_after = data[-1][0]
                if int(current_after) <= start_ts: break
                time.sleep(0.2)
            except: break
        
        if not all_data: return pd.DataFrame()
        df = pd.DataFrame(all_data, columns=['timestamp', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'volume_usd', 'volCcyQuote', 'confirm'])
        df['date'] = pd.to_datetime(pd.to_numeric(df['timestamp']), unit='ms', utc=True).dt.strftime('%Y-%m-%d')
        for col in ['price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'volume_usd']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['exchange'], df['symbol'] = 'okx', f"{base}-USDT"
        df = patch_missing_metrics(df, base, 'okx', f"{base}-USDT")

        final_cols = ['date', 'price_open', 'price_high', 'price_low', 'price_close', 'volume_base', 'volume_usd', 'buy_volume_base', 'sell_volume_base', 'volume_delta', 'txn_count', 'buy_txn_count', 'sell_txn_count', 'symbol', 'exchange']
        return df[[c for c in final_cols if c in df.columns]]

def main():
    parser = argparse.ArgumentParser(description="Fetch historical Spot OHLCV data from major exchanges")
    parser.add_argument("--limit", type=int, default=100, help="Number of top tokens to fetch (default: 100)")
    parser.add_argument("--top", type=int, dest="limit", help="Alias for --limit (backwards compatibility)")
    parser.add_argument("--csv", action="store_true", help="Save results to local CSV files (default: False)")
    parser.add_argument("--top-range", type=str, default=None, help="Rank range (e.g. 1-50)")
    parser.add_argument("--symbols", type=str, default=None, help="Specific symbols (e.g. BTC,ETH)")
    parser.add_argument("--exchanges", type=str, default="binance,bybit,okx",
                        help="Exchanges to fetch. Disponibles: binance,bybit,okx,coinbase "
                             "(coinbase aún no entra en el default ni en run_pipeline.py)")
    parser.add_argument("--start", type=str, default="2017-01-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--output-dir", type=str, default="data/spot", help="Output directory")
    parser.add_argument("--metadata-only", action="store_true", help="Only sync metadata and exit")
    parser.add_argument("--full-universe", action="store_true",
                       help="Use all non-filtered assets marked ever_in_top_50 in asset_metadata. "
                            "Avoids selecting assets from the current CoinGecko top list.")
    parser.add_argument("--skip-exchange-symbol-filter", action="store_true",
                       help="Do not prefilter assets by native exchange spot listings")
    parser.add_argument("--spot-symbol-cache-ttl-hours", type=float,
                       default=float(os.environ.get("SPOT_SYMBOL_CACHE_TTL_HOURS", "24")),
                       help="Hours to reuse cached native spot symbol lists (default: env SPOT_SYMBOL_CACHE_TTL_HOURS or 24)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Resolve target symbols and exchange support, then exit without fetching candles")
    args = parser.parse_args()
    
    scraper = SpotScraper(args.output_dir)
    db_manager = DatabaseManager()
    
    # Initialize Metadata Manager with DB support
    meta = AssetMetadataManager(db_manager=db_manager, allow_csv=args.csv)
    
    target_bases = []
    
    if args.full_universe and not args.symbols:
        target_bases = db_manager.get_full_tracked_symbols()
        if not target_bases:
            raise SystemExit("[ERROR] --full-universe requested but no ever_in_top_50 assets found in asset_metadata.")
    elif args.symbols:
        raw_symbols = [s.strip().upper() for s in args.symbols.split(",")]
        candidates = coingecko_get_top_candidates(specific_symbols=raw_symbols)
        for c in candidates:
            res = meta.get_metadata(c['symbol'], c['id'], c.get('market_cap'), c.get('market_cap_rank'))
            if res.get("is_filtered") == 0:
                target_bases.append(c['symbol'])
    else:
        limit = args.limit
        if args.top_range:
            _, end_rank = map(int, args.top_range.split("-"))
            limit = end_rank

        # A. Start with all non-filtered tokens already in our metadata (DB-first continuity)
        # This keeps updating tokens that drop out of the top 50
        tracked_active = meta.df[meta.df['is_filtered'] == 0]['symbol'].tolist()
        target_bases = [s for s in tracked_active if s not in ['BTC', 'ETH']] # Filter main pairs if desired, though usually kept
        
        # B. Add current top candidates from CoinGecko
        candidates = coingecko_get_top_candidates(n=limit)
        new_top_symbols = []
        for c in candidates:
            res = meta.get_metadata(c['symbol'], c['id'], c.get('market_cap'), c.get('market_cap_rank'))
            if res['is_filtered'] == 0:
                new_top_symbols.append(c['symbol'])
            if len(new_top_symbols) >= limit: break
            
        # Combine lists
        target_bases = list(dict.fromkeys(target_bases + new_top_symbols)) # Preserve uniqueness and order
        
        if args.top_range:
            start_rank, end_rank = map(int, args.top_range.split("-"))
            target_bases = target_bases[start_rank-1:end_rank]
        else:
            target_bases = target_bases[:max(len(target_bases), args.limit)] # Ensure we at least cover the limit

    if args.metadata_only:
        # 1. Update existing metadata first
        if db_manager.enabled and not meta.df.empty:
            print(f"[DB] Bulk syncing {len(meta.df)} cached assets metadata...")
            db_manager.bulk_upsert_asset_metadata(meta.df)
        print("[INFO] Metadata sync complete. Exiting (--metadata-only).")
        return

    exchanges = [e.strip().lower() for e in args.exchanges.split(",")]
    start_dt = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.now(timezone.utc)
    start_ts, end_ts = to_unix_ms(start_dt), to_unix_ms(end_dt)

    spot_symbols = {}
    if not args.skip_exchange_symbol_filter:
        spot_symbols = load_spot_exchange_symbols(exchanges, ttl_hours=args.spot_symbol_cache_ttl_hours)
    
    print("=" * 60)
    print(f"Exchange Spot OHLCV Backfill (Cache: {len(meta.df)} assets)")
    print("=" * 60)
    print(f"Date Range: {start_dt.date()} to {end_dt.date()}")
    print(f"Exchanges:  {exchanges}")
    print(f"Targeting:  {len(target_bases)} tokens")
    if spot_symbols:
        for exchange in exchanges:
            supported = spot_symbols.get(exchange, {})
            supported_count = sum(1 for base in target_bases if base in supported)
            print(f"Supported on {exchange}: {supported_count}/{len(target_bases)} tokens")
    print("=" * 60)

    if args.dry_run:
        print("[INFO] Dry run complete. No candles fetched.")
        return
    
    # Sync Metadata to Database (Efficient bulk sync)
    if db_manager.enabled and not meta.df.empty:
        print(f"[DB] Syncing {len(meta.df)} cached assets metadata...")
        db_manager.bulk_upsert_asset_metadata(meta.df)
    
    for exchange in exchanges:
        print(f"\nProcessing EXCHANGE: {exchange.upper()}")
        exch_dir = os.path.join(args.output_dir, exchange)
        os.makedirs(exch_dir, exist_ok=True)
        exchange_symbols = spot_symbols.get(exchange, {}) if not args.skip_exchange_symbol_filter else {}
        if not args.skip_exchange_symbol_filter:
            bases_for_exchange = [base for base in target_bases if base in exchange_symbols]
            skipped = len(target_bases) - len(bases_for_exchange)
            print(f"  [Filter] {len(bases_for_exchange)} tradable {SPOT_SYMBOL_FORMATS.get(exchange, ('', 'USDT'))[1]} spot assets on {exchange}; skipping {skipped} unsupported assets.")
        else:
            bases_for_exchange = target_bases

        for base in bases_for_exchange:
            try:
                # Símbolo nativo del venue (BTCUSDT / BTC-USDT / BTC-USD) y ruta del CSV.
                # El nombre de fichero va sin separador, que es como se guardaron siempre.
                db_symbol = spot_symbol_for(exchange, base)
                path = os.path.join(exch_dir, f"{db_symbol.replace('-', '')}_spot_1d.csv")
                
                dynamic_start = scraper.get_incremental_start(path, start_ts, db_symbol, exchange, db_manager)
                
                if exchange == 'binance':
                    df_new = scraper.fetch_binance(base, dynamic_start, end_ts)
                    time.sleep(0.3 if _tor.active else 1.0)  # Tor rotates IP → lower 429 risk
                elif exchange == 'bybit': df_new = scraper.fetch_bybit(base, dynamic_start, end_ts)
                elif exchange == 'okx': df_new = scraper.fetch_okx(base, dynamic_start, end_ts)
                elif exchange == 'coinbase': df_new = scraper.fetch_coinbase(base, dynamic_start, end_ts)
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
            time.sleep(0.1 if _tor.active else 0.5)  # Tor active → tighter inter-token gap

if __name__ == "__main__":
    main()
